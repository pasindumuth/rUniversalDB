mod prompt;

use crate::prompt::prompt_wrapper;
use clap::{arg, App};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::cast;
use runiversal::common::{mk_rid, rand_string, ColName, ColVal, InternalMode, TableView};
use runiversal::common::{EndpointId, RequestId};
use runiversal::message as msg;
use runiversal::net::{send_msg, start_acceptor_thread, GenericInputTrait};
use std::collections::BTreeMap;
use std::io::SeekFrom::End;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::SystemTime;

// The Threading Model is the same as the Transact Server.

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

fn main() -> crossterm::Result<()> {
  // Setup CLI parsing
  let matches = App::new("rUniversalDB")
    .version("1.0")
    .author("Pasindu M. <pasindumuth@gmail.com>")
    .arg(arg!(-i --ip <VALUE>).required(true).help("The IP address of the current host."))
    .get_matches();

  // Get required arguments
  let this_ip = matches.value_of("ip").unwrap().to_string();

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
  // The Master EndpointIds we tried starting the Master with
  let mut master_eids = Vec::<EndpointId>::new();
  // The EndpointId that most communication should use.
  let mut opt_target_eid = Option::<EndpointId>::None;

  // Setup the CLI read loop.
  loop {
    let input = if let Some(input) = prompt_wrapper()? { input } else { "exit".to_string() };
    match input.split_once(" ") {
      Some(("startmaster", rest)) => {
        // Start the masters
        master_eids = rest
          .split(" ")
          .into_iter()
          .map(|ip| EndpointId::new(ip.to_string(), InternalMode::Internal))
          .collect();
        for eid in &master_eids {
          send_msg(
            &out_conn_map,
            eid,
            msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::StartMaster(msg::StartMaster {
              master_eids: master_eids.clone(),
            })),
            &this_internal_mode,
          );
        }
      }
      Some(("target", rest)) => {
        opt_target_eid = Some(EndpointId::new(rest.to_string(), InternalMode::Internal));
      }
      _ => {
        if input == "exit" {
          break;
        } else {
          if let Some(target_eid) = &opt_target_eid {
            // Check if this is a debug request, making sure to print using {} (not {:#?}).
            if input == "debug" {
              let request_id = mk_rid(&mut rand);
              let network_msg = msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
                msg::MasterExternalReq::ExternalDebugRequest(msg::ExternalDebugRequest {
                  sender_eid: this_eid.clone(),
                  request_id,
                }),
              ));

              // Send and wait for a response
              send_msg(&out_conn_map, &target_eid, network_msg, &this_internal_mode);
              let message = to_server_receiver.recv().unwrap().message;

              // Print the response
              let external_msg = cast!(msg::NetworkMessage::External, message).unwrap();
              let resp = cast!(msg::ExternalMessage::ExternalDebugResponse, external_msg).unwrap();
              println!("{}", resp.debug_str);
            } else {
              let request_id = mk_rid(&mut rand);
              let network_msg = if master_eids.contains(&target_eid) {
                // Send this message as a  DDL Query, since the target is set for the Master.
                msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
                  msg::MasterExternalReq::PerformExternalDDLQuery(msg::PerformExternalDDLQuery {
                    sender_eid: this_eid.clone(),
                    request_id,
                    query: input,
                  }),
                ))
              } else {
                // Otherwise, send this message as a DQL Query, since the Target is a Slave.
                msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
                  msg::SlaveExternalReq::PerformExternalQuery(msg::PerformExternalQuery {
                    sender_eid: this_eid.clone(),
                    request_id,
                    query: input,
                  }),
                ))
              };

              // Send and wait for a response
              send_msg(&out_conn_map, &target_eid, network_msg, &this_internal_mode);
              let message = to_server_receiver.recv().unwrap().message;
              match message {
                msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(
                  success,
                )) => {
                  println!("{}", format_table(success.result));
                }
                message => {
                  // Print the respnse
                  println!("{:#?}", message);
                }
              }
            }
          } else {
            println!("A target address is not set. Do that by typing 'target <hostname>'.\n");
          }
        }
      }
    }
  }

  Ok(())
}

/// Format `table_view` into a printable string.
fn format_table(table_view: TableView) -> String {
  const CELL_WIDTH: usize = 16;
  const MAX_CELL_CONTENT_LEN: usize = 14;

  let mut lines = Vec::<String>::new();

  // Construct the elements in the schema string
  let mut schema_row_elems = Vec::<String>::new();
  schema_row_elems.push("index".to_string());
  for maybe_col_name in table_view.col_names {
    if let Some(ColName(col_name)) = maybe_col_name {
      schema_row_elems.push(col_name);
    }
  }
  schema_row_elems.push("count".to_string());

  // Format the schema string
  let mut formatted_schema_row_elems = Vec::<String>::new();
  formatted_schema_row_elems.push("|".to_string());
  for str in schema_row_elems {
    let resolved_str = if str.len() > MAX_CELL_CONTENT_LEN {
      format!("{}...", &str[..(MAX_CELL_CONTENT_LEN - 3)])
    } else {
      str
    };

    let white_space = CELL_WIDTH - resolved_str.len();
    let l_padding = white_space / 2 + white_space % 2;
    let r_padding = white_space / 2;

    formatted_schema_row_elems.push(format!(
      "{}{}{}",
      " ".repeat(l_padding),
      resolved_str,
      " ".repeat(r_padding)
    ));
    formatted_schema_row_elems.push("|".to_string());
  }

  let schema_row = formatted_schema_row_elems.join("");
  let length = schema_row.len();

  lines.push("-".repeat(length));
  lines.push(schema_row);
  lines.push("-".repeat(length));

  // Construct the row strings
  for (index, (cols, count)) in table_view.rows.into_iter().enumerate() {
    let mut row_elems = Vec::<String>::new();
    row_elems.push(index.to_string());
    for col in cols {
      let col_val_str = match col {
        Some(ColVal::Int(val)) => val.to_string(),
        Some(ColVal::Bool(val)) => val.to_string(),
        Some(ColVal::String(val)) => format!("\"{}\"", val),
        None => "NULL".to_string(),
      };
      row_elems.push(col_val_str);
    }
    row_elems.push(count.to_string());

    let mut formatted_row_elems = Vec::<String>::new();
    formatted_row_elems.push("|".to_string());
    for str in row_elems {
      let resolved_str = if str.len() > MAX_CELL_CONTENT_LEN {
        format!("{}...", &str[..(MAX_CELL_CONTENT_LEN - 3)])
      } else {
        str
      };

      let white_space = CELL_WIDTH - resolved_str.len();
      let l_padding = white_space - 1;
      let r_padding = 1;

      formatted_row_elems.push(format!(
        "{}{}{}",
        " ".repeat(l_padding),
        resolved_str,
        " ".repeat(r_padding)
      ));
      formatted_row_elems.push("|".to_string());
    }

    lines.push(formatted_row_elems.join(""));
    lines.push("-".repeat(length));
  }

  lines.join("\n")
}
