use clap::{arg, App};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::cast;
use runiversal::common::{
  mk_rid, rand_string, ColName, ColType, ColVal, GossipData, InternalMode, TablePath, TableView,
  Timestamp,
};
use runiversal::common::{EndpointId, RequestId};
use runiversal::message as msg;
use runiversal::net::{send_msg, start_acceptor_thread, GenericInputTrait};
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
    .get_matches();

  // Get required arguments
  let this_ip = matches.value_of("ip").unwrap().to_string();
  let mut state = ClientState::new(this_ip);
  state.start_loop();
}

// -----------------------------------------------------------------------------------------------
//  ClientState
// -----------------------------------------------------------------------------------------------

/// `GenericInput` for the client CLI.
struct GenericInput {
  eid: EndpointId,
  message: msg::NetworkMessage,
}

impl GenericInputTrait for GenericInput {
  fn from_network(eid: EndpointId, message: msg::NetworkMessage) -> GenericInput {
    GenericInput { eid, message }
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
  to_server_receiver: Receiver<GenericInput>,
  out_conn_map: Arc<Mutex<BTreeMap<EndpointId, Sender<Vec<u8>>>>>,
  /// Rng
  rand: XorShiftRng,
  /// The EndpointId of this node
  this_eid: EndpointId,
  /// The Master EndpointIds we tried starting the Master with
  master_eids: Vec<EndpointId>,
  /// The EndpointId that most communication should use.
  opt_target_eid: Option<EndpointId>,
  /// The CLI command prompt (which maintaining command history, cursor state, etc).
  read_loop: Editor<()>,
}

impl ClientState {
  fn new(this_ip: String) -> ClientState {
    // The mpsc channel for passing data to the Server Thread from all FromNetwork Threads.
    let (to_server_sender, to_server_receiver) = mpsc::channel::<GenericInput>();
    // Maps the IP addresses to a FromServer Queue, used to send data to Outgoing Connections.
    let out_conn_map = Arc::new(Mutex::new(BTreeMap::<EndpointId, Sender<Vec<u8>>>::new()));
    // Create an RNG for ID generation
    let mut rand = XorShiftRng::from_entropy();

    // Start the Accepting Thread
    start_acceptor_thread(&to_server_sender, this_ip.clone());

    // The EndpointId of this node
    let this_internal_mode = InternalMode::External { salt: rand_string(&mut rand) };
    let this_eid = EndpointId::new(this_ip, this_internal_mode.clone());

    ClientState {
      to_server_receiver,
      out_conn_map,
      rand,
      this_eid,
      master_eids: vec![],
      opt_target_eid: None,
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

  /// Processes a line submitted by the user.
  fn handle_input(&mut self, input: String) -> Result<LoopAction, String> {
    // Quit
    if input == "\\q" {
      Ok(LoopAction::Exit)
    }
    // Start the Master Group
    else if input.starts_with("startmaster ") {
      // Parse the `master_eids`
      let mut it = input.split(" ");
      it.next();
      self.master_eids =
        it.map(|ip| EndpointId::new(ip.to_string(), InternalMode::Internal)).collect();

      // Instruct the nodes to become part of the Master Group.
      for eid in &self.master_eids {
        send_msg(
          &self.out_conn_map,
          eid,
          msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::StartMaster(msg::StartMaster {
            master_eids: self.master_eids.clone(),
          })),
          &self.this_eid.mode,
        );
      }
      Ok(LoopAction::DoNothing)
    }
    // Set the remote Node to communicate with.
    else if input.starts_with("target ") {
      let mut it = input.split(" ");
      it.next();
      self.opt_target_eid =
        Some(EndpointId::new(it.next().unwrap().to_string(), InternalMode::Internal));
      Ok(LoopAction::DoNothing)
    }
    // Query and display metadata from the system
    else if input.starts_with("\\dt") {
      let request_id = mk_rid(&mut self.rand);
      let network_msg = msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
        msg::MasterExternalReq::ExternalMetadataRequest(msg::ExternalMetadataRequest {
          sender_eid: self.this_eid.clone(),
          request_id,
        }),
      ));

      // Send and wait for a response
      send_msg(&self.out_conn_map, self.get_target()?, network_msg, &self.this_eid.mode);
      let message = self.to_server_receiver.recv().unwrap().message;

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
      let request_id = mk_rid(&mut self.rand);
      let network_msg = if self.master_eids.contains(self.get_target()?) {
        // Send this message as a  DDL Query, since the target is set for the Master.
        msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
          msg::MasterExternalReq::PerformExternalDDLQuery(msg::PerformExternalDDLQuery {
            sender_eid: self.this_eid.clone(),
            request_id,
            query: input,
          }),
        ))
      } else {
        // Otherwise, send this message as a DQL Query, since the Target is a Slave.
        msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
          msg::SlaveExternalReq::PerformExternalQuery(msg::PerformExternalQuery {
            sender_eid: self.this_eid.clone(),
            request_id,
            query: input,
          }),
        ))
      };

      // Send and wait for a response
      send_msg(&self.out_conn_map, self.get_target()?, network_msg, &self.this_eid.mode);
      let message = self.to_server_receiver.recv().unwrap().message;

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
        Ok(LoopAction::Print(display))
      } else {
        Ok(LoopAction::DoNothing)
      }
    }
  }

  fn get_target(&self) -> Result<&EndpointId, String> {
    if let Some(target) = &self.opt_target_eid {
      Ok(target)
    } else {
      Err("A target address is not set. Do that by typing 'target <hostname>'.".to_string())
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

/// Format `table_view` into a printable string.
fn format_table(table_view: TableView) -> String {
  let mut lines = Vec::<String>::new();

  // Construct Display Columns for the Display Table
  let mut display_cols = Vec::<String>::new();
  display_cols.push("index".to_string());
  for maybe_col_name in table_view.col_names {
    if let Some(ColName(col_name)) = maybe_col_name {
      display_cols.push(col_name);
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
  for (index, (cols, count)) in table_view.rows.into_iter().enumerate() {
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
    let display_row = vec![
      col_name.0,
      match col_type {
        ColType::Int => "integer".to_string(),
        ColType::Bool => "bool".to_string(),
        ColType::String => "string".to_string(),
      },
    ];
    lines.push(format_even_spaces(display_row, Justification::Right));
    lines.push("-".repeat(display_width));
  }

  Some(lines.join("\n"))
}
