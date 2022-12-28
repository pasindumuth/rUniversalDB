mod monitor;

use crate::monitor::{col_type_str, MetadataMonitor};
use clap::{arg, App};
use env_logger::Builder;
use log::LevelFilter;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::cast;
use runiversal::common::{
  mk_rid, rand_string, ColName, ColType, ColVal, GossipData, InternalMode, PaxosGroupId,
  QueryResult, TablePath, TableView, Timestamp,
};
use runiversal::common::{EndpointId, RequestId};
use runiversal::message as msg;
use runiversal::net::{send_msg, start_acceptor_thread, GenericInputTrait, SendAction};
use runiversal::sql_parser::is_ddl;
use runiversal::test_utils::mk_seed;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::collections::BTreeMap;
use std::io::SeekFrom::End;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::SystemTime;

// The Threading Model is the same as the Transact Server.

fn main() {
  // Setup CLI parsing
  let matches = App::new("rUniversalDB")
    .version("1.0")
    .author("Pasindu M. <pasindumuth@gmail.com>")
    .arg(arg!(-i --ip <VALUE>).required(true).help("The IP address of the current host."))
    .arg(
      arg!(-m --mips <VALUE>)
        .required(false)
        .help("A space separate list (in quotes) of Master IPs."),
    )
    .arg(arg!(-e --entry_mip <VALUE>).required(false).help(
      "If '-m' is not specified, this Master IP address is used to connect to \
       the system for communication by the command prompt.",
    ))
    .get_matches();

  // Setup logging
  Builder::new().filter_level(LevelFilter::Off).init();

  // Get required arguments
  let this_ip = matches.value_of("ip").unwrap().to_string();
  let mut state = ClientState::new(this_ip);

  // If `mips` is specified, we interpret this as a one-off execution of the program where
  // we merely need to start the Masters.
  if let Some(master_ips_str) = matches.value_of("mips") {
    let master_eids: Vec<_> = master_ips_str
      .split(" ")
      .map(|ip| EndpointId::new(ip.to_string(), InternalMode::Internal))
      .collect();

    // Send out the `StartMaster` message
    let (to_server_sender, to_server_receiver) = mpsc::channel::<()>();
    for eid in &master_eids {
      state.send(
        eid,
        SendAction::new(
          msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::StartMaster(msg::StartMaster {
            master_eids: master_eids.clone(),
          })),
          Some(to_server_sender.clone()),
        ),
      );
    }

    // Wait until all messages have been sent. After this, we exit.
    for _ in 0..master_eids.len() {
      to_server_receiver.recv().unwrap();
    }
  } else if let Some(master_ip) = matches.value_of("entry_mip") {
    // Otherwise, enter the read loop.
    state.initialize_connection(master_ip.to_string());
    state.start_loop();
  } else {
    println!("Please specify either '-m' or '-e'.");
  }
}

// -----------------------------------------------------------------------------------------------
//  ClientState
// -----------------------------------------------------------------------------------------------

/// An input containing incoming network data.
pub struct NetworkInput {
  eid: EndpointId,
  message: msg::NetworkMessage,
}

/// `GenericInput` for the client CLI.
pub enum GenericInput {
  NetworkInput(NetworkInput),
  /// This signals for the thread currently listening to stop listening.
  None,
}

impl GenericInputTrait for GenericInput {
  fn from_network(eid: EndpointId, message: msg::NetworkMessage) -> GenericInput {
    GenericInput::NetworkInput(NetworkInput { eid, message })
  }
}

/// Actions returned by `handle_input` to be performed by `start_loop`.
enum LoopAction {
  Print(String),
  DoNothing,
  Exit,
}

/// Contains the client state (e.g. connection state).
struct ClientState {
  to_server_sender: Sender<GenericInput>,
  to_server_receiver: Option<Receiver<GenericInput>>,
  out_conn_map: Arc<Mutex<BTreeMap<EndpointId, Sender<SendAction>>>>,
  /// Rng
  rand: XorShiftRng,
  /// The EndpointId of this node
  this_eid: EndpointId,
  /// The Master EndpointIds we tried starting the Master with
  master_eids: Vec<EndpointId>,
  /// The EndpointId that most communication should use.
  opt_target_master_eid: Option<EndpointId>,
  opt_target_slave_eid: Option<EndpointId>,
  /// The CLI command prompt (which maintaining command history, cursor state, etc).
  read_loop: Editor<()>,
}

impl ClientState {
  fn new(this_ip: String) -> ClientState {
    // The mpsc channel for passing data to the Server Thread from all FromNetwork Threads.
    let (to_server_sender, to_server_receiver) = mpsc::channel::<GenericInput>();
    // Maps the IP addresses to a FromServer Queue, used to send data to Outgoing Connections.
    let out_conn_map = Arc::new(Mutex::new(BTreeMap::<EndpointId, Sender<SendAction>>::new()));
    // Create an RNG for ID generation
    let mut rand = XorShiftRng::from_entropy();

    // Start the Accepting Thread
    start_acceptor_thread(&to_server_sender, this_ip.clone());

    // The EndpointId of this node
    let this_internal_mode = InternalMode::External { salt: rand_string(&mut rand) };
    let this_eid = EndpointId::new(this_ip, this_internal_mode.clone());

    ClientState {
      to_server_sender,
      to_server_receiver: Some(to_server_receiver),
      out_conn_map,
      rand,
      this_eid,
      master_eids: vec![],
      opt_target_master_eid: None,
      opt_target_slave_eid: None,
      read_loop: Editor::new(),
    }
  }

  fn start_loop(&mut self) {
    // Setup the CLI read loop.
    loop {
      // Read the next line from the command prompt.
      let readline = self.read_loop.readline(">> ");
      let input = match readline {
        Ok(line) => {
          self.read_loop.add_history_entry(line.as_str());
          line
        }
        Err(ReadlineError::Interrupted) => {
          println!("CTRL-C");
          break;
        }
        Err(ReadlineError::Eof) => {
          println!("CTRL-D");
          break;
        }
        Err(err) => {
          println!("Error: {:?}", err);
          break;
        }
      };

      match self.handle_input(input) {
        Ok(action) => match action {
          LoopAction::Print(message) => println!("{}", message),
          LoopAction::DoNothing => {}
          LoopAction::Exit => break,
        },
        Err(error_message) => println!("{}", error_message),
      }
    }
  }

  /// Contacts `master_ip` to solicit the `self.master_eids`,
  /// `self.opt_target_master_eid`, and `self.opt_traget_slave_eid`.
  fn initialize_connection(&mut self, master_ip: String) {
    let request_id = mk_rid(&mut self.rand);
    let master_eid = EndpointId::new(master_ip.to_string(), InternalMode::Internal);
    self.send(
      &master_eid,
      SendAction::new(
        msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
          msg::MasterExternalReq::ExternalMetadataRequest(msg::ExternalMetadataRequest {
            sender_eid: self.this_eid.clone(),
            request_id: request_id.clone(),
          }),
        )),
        None,
      ),
    );

    // Send and wait for a response
    let message =
      block_until_network_response(self.to_server_receiver.as_ref(), &request_id).message;
    let external_msg = cast!(msg::NetworkMessage::External, message).unwrap();
    let resp = cast!(msg::ExternalMessage::ExternalMetadataResponse, external_msg).unwrap();

    // Populate ClientState
    self.master_eids = resp.gossip_data.get().master_address_config.clone();
    for (gid, lid) in resp.leader_map {
      if let PaxosGroupId::Slave(_) = gid {
        // For simplicity, choose some random Slave Leadership.
        self.opt_target_slave_eid = Some(lid.eid);
      } else {
        // And choose the only Master Leadership.
        self.opt_target_master_eid = Some(lid.eid);
      }
    }
  }

  /// Processes a line submitted by the user.
  fn handle_input(&mut self, input: String) -> Result<LoopAction, String> {
    // Quit
    if input == "\\q" {
      Ok(LoopAction::Exit)
    }
    // Set the remote MasterNode to communicate with.
    else if input.starts_with("master_target ") {
      let mut it = input.split(" ");
      it.next();
      self.opt_target_master_eid =
        Some(EndpointId::new(it.next().unwrap().to_string(), InternalMode::Internal));
      Ok(LoopAction::DoNothing)
    }
    // Set the remote Slave Node to communicate with.
    else if input.starts_with("slave_target ") {
      let mut it = input.split(" ");
      it.next();
      self.opt_target_slave_eid =
        Some(EndpointId::new(it.next().unwrap().to_string(), InternalMode::Internal));
      Ok(LoopAction::DoNothing)
    }
    // Display metadata that we pull continuous from the Master Group.
    else if input.starts_with("live") {
      // Temporarily take the `to_server_receiver` and construct the MetadataMonitor
      let to_server_receiver = std::mem::take(&mut self.to_server_receiver).unwrap();
      let seed = mk_seed(&mut self.rand);
      let monitor = MetadataMonitor::new(
        self.to_server_sender.clone(),
        to_server_receiver,
        seed,
        self.out_conn_map.clone(),
        self.this_eid.clone(),
        self.opt_target_master_eid.clone().unwrap(),
      )
      .unwrap();

      // Display the metadata monitor. When it is done, it will return back
      // the `to_server_receiver`.
      self.to_server_receiver = Some(monitor.show_screen().unwrap());
      Ok(LoopAction::DoNothing)
    }
    // Query and display metadata from the system
    else if input.starts_with("\\dt") {
      let request_id = mk_rid(&mut self.rand);
      let network_msg = msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
        msg::MasterExternalReq::ExternalMetadataRequest(msg::ExternalMetadataRequest {
          sender_eid: self.this_eid.clone(),
          request_id: request_id.clone(),
        }),
      ));

      // Send and wait for a response
      self.send(self.get_master()?, SendAction::new(network_msg, None));
      let message =
        block_until_network_response(self.to_server_receiver.as_ref(), &request_id).message;
      let external_msg = cast!(msg::NetworkMessage::External, message).unwrap();
      let resp = cast!(msg::ExternalMessage::ExternalMetadataResponse, external_msg).unwrap();
      let gossip_data = resp.gossip_data;
      let timestamp = gossip_data.get().table_generation.get_latest_lat();

      // Display the output
      let display = if input.starts_with("\\dt ") {
        // Here, a table name should also be specified, which we display in detail.
        let mut it = input.split(" ");
        it.next();
        let path = it.next().unwrap().to_string();

        // Format and display the table metadata, if it exists.
        if let Some(display_table) = format_dt_table(gossip_data, path.clone(), timestamp) {
          display_table
        } else {
          format!("Table '{}' does not exist.", path)
        }
      } else {
        // Here, we want to print only the database metadata
        format_dt_overview(gossip_data, timestamp)
      };
      Ok(LoopAction::Print(display))
    }
    // Send a normal DQL or DQL Query (based on what the `opt_target_eid` is).
    else {
      let mut next_loop_action = LoopAction::DoNothing;

      // The user can enter multiple queries in at once such that they are all
      // executed one-at-a-time. This is convenient for quickly setting up multiple
      // tables with some initial data.
      for input in input.split("-- Separate") {
        let request_id = mk_rid(&mut self.rand);
        if is_ddl(&input) {
          // Send this message as a DDL Query to the Master.
          let network_msg = msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
            msg::MasterExternalReq::PerformExternalDDLQuery(msg::PerformExternalDDLQuery {
              sender_eid: self.this_eid.clone(),
              request_id: request_id.clone(),
              query: input.to_string(),
            }),
          ));
          self.send(self.get_master()?, SendAction::new(network_msg, None));
        } else {
          // Otherwise, send this message as a DQL Query to the Slave.
          let network_msg = msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
            msg::SlaveExternalReq::PerformExternalQuery(msg::PerformExternalQuery {
              sender_eid: self.this_eid.clone(),
              request_id: request_id.clone(),
              query: input.to_string(),
            }),
          ));
          self.send(self.get_slave()?, SendAction::new(network_msg, None));
        };

        // Send and wait for a response
        let message =
          block_until_network_response(self.to_server_receiver.as_ref(), &request_id).message;

        // Display the output
        if let Some(display) = match message {
          msg::NetworkMessage::External(external) => match external {
            msg::ExternalMessage::ExternalQuerySuccess(success) => {
              Some(format!("{}", format_table(success.result)))
            }
            msg::ExternalMessage::ExternalQueryAborted(aborted) => {
              Some(format!("Failed with error: {:#?}", aborted.payload))
            }
            msg::ExternalMessage::ExternalDDLQuerySuccess(_) => None,
            msg::ExternalMessage::ExternalDDLQueryAborted(aborted) => {
              Some(format!("Failed with error: {:#?}", aborted.payload))
            }
            _ => Some(format!("{:#?}", external)),
          },
          message => Some(format!("{:#?}", message)),
        } {
          // We are only interested in printing the results of the final query.
          next_loop_action = LoopAction::Print(display);
        }
      }

      Ok(next_loop_action)
    }
  }

  fn get_master(&self) -> Result<&EndpointId, String> {
    get_eid(&self.opt_target_master_eid)
  }

  fn get_slave(&self) -> Result<&EndpointId, String> {
    get_eid(&self.opt_target_slave_eid)
  }

  /// A convenience function for sending data to `eid`.
  fn send(&self, eid: &EndpointId, action: SendAction) {
    send_msg(&self.out_conn_map, eid, action, &self.this_eid.mode);
  }
}

/// Unwraps `opt_target`, returning an error if not present.
fn get_eid(opt_target: &Option<EndpointId>) -> Result<&EndpointId, String> {
  if let Some(target) = opt_target {
    Ok(target)
  } else {
    Err("A target address is not set. Do that by typing 'target <hostname>'.".to_string())
  }
}

// -----------------------------------------------------------------------------------------------
//  Network Utils
// -----------------------------------------------------------------------------------------------

/// Read messages from `receiver` until one arrives with a matching `rid`, or `None` arrives.
pub fn block_until_generic_response(
  receiver: Option<&Receiver<GenericInput>>,
  rid: &RequestId,
) -> GenericInput {
  loop {
    let input = receiver.unwrap().recv().unwrap();
    if let GenericInput::NetworkInput(network_input) = &input {
      if match &network_input.message {
        msg::NetworkMessage::External(message) => match message {
          msg::ExternalMessage::ExternalQuerySuccess(res) => &res.request_id == rid,
          msg::ExternalMessage::ExternalQueryAborted(res) => &res.request_id == rid,
          msg::ExternalMessage::ExternalDDLQuerySuccess(res) => &res.request_id == rid,
          msg::ExternalMessage::ExternalDDLQueryAborted(res) => &res.request_id == rid,
          msg::ExternalMessage::ExternalShardingSuccess(res) => &res.request_id == rid,
          msg::ExternalMessage::ExternalShardingAborted(res) => &res.request_id == rid,
          msg::ExternalMessage::ExternalMetadataResponse(res) => &res.request_id == rid,
        },
        _ => {
          debug_assert!(false);
          false
        }
      } {
        return input;
      }
    } else {
      return input;
    }
  }
}

/// Read messages from `receiver` until one arrives with a matching `rid`.
pub fn block_until_network_response(
  receiver: Option<&Receiver<GenericInput>>,
  rid: &RequestId,
) -> NetworkInput {
  loop {
    let input = block_until_generic_response(receiver.clone(), rid);
    if let GenericInput::NetworkInput(network_input) = input {
      return network_input;
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Print Utils
// -----------------------------------------------------------------------------------------------

/// How to justify content within a cell.
enum Justification {
  Left,
  Center,
  Right,
}

/// Takes every element in `elems` and places it within a cell, using the given
/// `justification`, and returns the line.
fn format_even_spaces(elems: Vec<String>, justification: Justification) -> String {
  const CELL_WIDTH: usize = 16;
  const MAX_CELL_CONTENT_LEN: usize = 14;

  // Format `elems`
  let mut formatted_elems = Vec::<String>::new();
  formatted_elems.push("|".to_string());
  for elem in elems {
    let resolved_elem = if elem.len() > MAX_CELL_CONTENT_LEN {
      format!("{}...", &elem[..(MAX_CELL_CONTENT_LEN - 3)])
    } else {
      elem
    };

    let white_space = CELL_WIDTH - resolved_elem.len();
    let (l_padding, r_padding) = match justification {
      Justification::Left => (1, white_space - 1),
      Justification::Center => (white_space / 2 + white_space % 2, white_space / 2),
      Justification::Right => (white_space - 1, 1),
    };

    formatted_elems.push(format!(
      "{}{}{}",
      " ".repeat(l_padding),
      resolved_elem,
      " ".repeat(r_padding)
    ));
    formatted_elems.push("|".to_string());
  }

  formatted_elems.join("")
}

/// Format `result` into a printable string.
fn format_table(result: QueryResult) -> String {
  let mut lines = Vec::<String>::new();

  // Construct Display Columns for the Display Table
  let mut display_cols = Vec::<String>::new();
  display_cols.push("index".to_string());
  for maybe_col_name in result.schema {
    if let Some(ColName(col_name)) = maybe_col_name {
      display_cols.push(col_name);
    } else {
      display_cols.push("<nameless>".to_string());
    }
  }
  display_cols.push("count".to_string());
  let display_cols_line = format_even_spaces(display_cols, Justification::Center);
  let display_width = display_cols_line.len();

  // Populate the first few lines
  lines.push("-".repeat(display_width));
  lines.push(display_cols_line);
  lines.push("-".repeat(display_width));

  // Construct Display Rows
  for (index, (cols, count)) in result.data.rows.into_iter().enumerate() {
    let mut display_row = Vec::<String>::new();
    display_row.push(index.to_string());
    for col in cols {
      let col_val_str = match col {
        Some(ColVal::Int(val)) => val.to_string(),
        Some(ColVal::Bool(val)) => val.to_string(),
        Some(ColVal::String(val)) => format!("\"{}\"", val),
        None => "NULL".to_string(),
      };
      display_row.push(col_val_str);
    }
    display_row.push(count.to_string());

    lines.push(format_even_spaces(display_row, Justification::Right));
    lines.push("-".repeat(display_width));
  }

  lines.join("\n")
}

/// Format database schema into a printable string.
fn format_dt_overview(gossip: GossipData, timestamp: Timestamp) -> String {
  let mut lines = Vec::<String>::new();

  // Construct Display Table
  let mut display_cols = vec!["Schema".to_string(), "Name".to_string(), "Type".to_string()];
  let display_cols_line = format_even_spaces(display_cols, Justification::Center);
  let display_width = display_cols_line.len();

  // Populate the first few lines
  lines.push("-".repeat(display_width));
  lines.push(display_cols_line);
  lines.push("-".repeat(display_width));

  // Construct Display Rows
  for (table_path, _) in gossip.get().table_generation.static_snapshot_read(&timestamp) {
    let display_row = vec!["public".to_string(), table_path.0, "table".to_string()];
    lines.push(format_even_spaces(display_row, Justification::Right));
    lines.push("-".repeat(display_width));
  }

  lines.join("\n")
}

/// Format table schema into a printable string.
fn format_dt_table(gossip: GossipData, path: String, timestamp: Timestamp) -> Option<String> {
  let mut lines = Vec::<String>::new();

  // Construct Display Columns for the Display Table
  let mut display_cols = vec!["Column".to_string(), "Type".to_string()];
  let display_cols_line = format_even_spaces(display_cols, Justification::Center);
  let display_width = display_cols_line.len();

  // Populate the first few lines
  lines.push("-".repeat(display_width));
  lines.push(display_cols_line);
  lines.push("-".repeat(display_width));

  // Construct Display Rows
  let table_path = TablePath(path);
  let (gen, _) = gossip.get().table_generation.static_read(&table_path, &timestamp)?;
  let table_path_gen = (table_path.clone(), gen.clone());
  let schema = gossip.get().db_schema.get(&table_path_gen).unwrap();

  let mut all_cols = schema.key_cols.clone();
  all_cols.extend(schema.val_cols.static_snapshot_read(&timestamp).into_iter());
  for (col_name, col_type) in all_cols {
    let display_row = vec![col_name.0, col_type_str(&col_type)];
    lines.push(format_even_spaces(display_row, Justification::Right));
    lines.push("-".repeat(display_width));
  }

  Some(lines.join("\n"))
}
