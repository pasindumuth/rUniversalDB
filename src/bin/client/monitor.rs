use crate::{
  block_until_generic_response, block_until_network_response, GenericInput, NetworkInput,
};
use crossterm::event::{
  DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, MouseEventKind,
};
use crossterm::terminal::{
  disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::{event, execute};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  mk_rid, ColType, ColVal, EndpointId, GossipData, LeaderMap, PaxosGroupId, PaxosGroupIdTrait,
};
use runiversal::message as msg;
use runiversal::net::{send_msg, SendAction};
use std::collections::BTreeMap;
use std::io;
use std::io::Stdout;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{sleep, JoinHandle};
use std::time::Duration;
use tabled::{Table, Tabled};
use tui::backend::CrosstermBackend;
use tui::layout::{Constraint, Direction, Layout};
use tui::widgets::Paragraph;
use tui::Terminal;

/// This is returned from the 2 background jobs to the main thread (which
/// updates the metadata screen accordingly).
enum Signal {
  NetworkMessage(NetworkInput),
  Event(Event),
}

/// This is constructed whenever the user switches to a live view
/// of the system metadata.
pub struct MetadataMonitor {
  terminal: Terminal<CrosstermBackend<Stdout>>,
  sender: Sender<GenericInput>,
  network_thread_jh: JoinHandle<Receiver<GenericInput>>,
  user_io_thread_jh: JoinHandle<()>,
  to_main_receiver: Receiver<Signal>,

  /// Scrolling
  scroll: (u16, u16),

  /// Text to show
  content: String,
}

impl MetadataMonitor {
  /// Here, we switch to the metadata screen and construct the background jobs
  /// to receive network data and keyboard and mouse inputs.
  ///
  /// Here, `receiver` is the main receiver for the Main thread.
  pub fn new(
    sender: Sender<GenericInput>,
    receiver: Receiver<GenericInput>,
    // Network-related arguments
    seed: [u8; 16],
    out_conn_map: Arc<Mutex<BTreeMap<EndpointId, Sender<SendAction>>>>,
    this_eid: EndpointId,
    master_eid: EndpointId,
  ) -> Result<MetadataMonitor, io::Error> {
    // Setup alternate screen.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let terminal = Terminal::new(CrosstermBackend::new(stdout))?;

    // A sender/receiver for the background threads to send data to the main thread.
    let (to_main_sender, to_main_receiver) = mpsc::channel::<Signal>();

    // Create a background job to receive network events.
    let network_thread_jh = {
      let mut rand = XorShiftRng::from_seed(seed);
      let to_main_sender = to_main_sender.clone();
      std::thread::spawn(move || {
        loop {
          // Sleep for 200ms before re-requesting updated metadata.
          sleep(Duration::new(0, 200_000_000));

          // Send a metadata request.
          let request_id = mk_rid(&mut rand);
          let network_msg = msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
            msg::MasterExternalReq::ExternalMetadataRequest(msg::ExternalMetadataRequest {
              sender_eid: this_eid.clone(),
              request_id: request_id.clone(),
            }),
          ));
          send_msg(&out_conn_map, &master_eid, SendAction::new(network_msg, None), &this_eid.mode);

          // Anticipate the response.
          if let GenericInput::NetworkInput(network_input) =
            block_until_generic_response(Some(&receiver), &request_id)
          {
            to_main_sender.send(Signal::NetworkMessage(network_input));
          } else {
            break receiver;
          }
        }
      })
    };

    // Create a background job to receive network events.
    let user_io_thread_jh = {
      let to_main_sender = to_main_sender.clone();
      std::thread::spawn(move || {
        while let Ok(term_event) = event::read() {
          // Check if we need to terminate this thread.
          let should_quit = match &term_event {
            Event::Key(key) => should_quit(key),
            _ => false,
          };

          to_main_sender.send(Signal::Event(term_event.clone()));
          if should_quit {
            break;
          }
        }
      })
    };

    Ok(MetadataMonitor {
      terminal,
      sender,
      network_thread_jh,
      user_io_thread_jh,
      to_main_receiver,
      scroll: (0, 0),
      content: "".to_string(),
    })
  }

  /// Start receiving keyboard and mouse and network inputs until the user decides to
  /// quit. At this point, we switch back to the original screen
  pub fn show_screen(mut self) -> Result<Receiver<GenericInput>, io::Error> {
    loop {
      match self.to_main_receiver.recv().unwrap() {
        Signal::NetworkMessage(generic_input) => {
          if let msg::NetworkMessage::External(msg::ExternalMessage::ExternalMetadataResponse(
            resp,
          )) = generic_input.message
          {
            let gossip_data = resp.gossip_data;
            let leader_map = resp.leader_map;

            // Construct the Display tables and assign the tables to be rendered on the screen.
            self.content = vec![
              paxos_group_table(&gossip_data, &leader_map),
              free_nodes(resp.free_node_eids),
              sharding_table(&gossip_data),
              table_schemas_table(&gossip_data),
            ]
            .join("\n");
          }
        }
        Signal::Event(event) => match event {
          Event::Key(key) => match &key.code {
            KeyCode::Up => {
              // Scroll up
              if self.scroll.0 > 0 {
                self.scroll.0 -= 1;
              }
            }
            KeyCode::Down => {
              // Scroll down
              self.scroll.0 += 1;
            }
            KeyCode::Left => {
              // Scroll left
              if self.scroll.1 > 0 {
                self.scroll.1 -= 2;
              }
            }
            KeyCode::Right => {
              // Scroll right
              self.scroll.1 += 2;
            }
            _ => {
              if should_quit(&key) {
                break;
              }
            }
          },
          // Handle mouse events to scroll the view.
          Event::Mouse(event) => match event.kind {
            MouseEventKind::ScrollDown => {
              self.scroll.0 += 1;
            }
            MouseEventKind::ScrollUp => {
              if self.scroll.0 > 0 {
                self.scroll.0 -= 1;
              }
            }
            _ => {}
          },
          _ => {}
        },
      }

      // Re-render the view.
      let content = self.content.clone();
      let scroll = self.scroll.clone();
      self.terminal.draw(|f| {
        let chunks = Layout::default()
          .direction(Direction::Vertical)
          .constraints([Constraint::Percentage(100)].as_ref())
          .split(f.size());

        f.render_widget(Paragraph::new(content).scroll(scroll), chunks[0]);
      })?;
    }

    // Switch back to the normal screen and turn on the cursor.
    execute!(self.terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    self.terminal.show_cursor()?;
    disable_raw_mode()?;

    self.user_io_thread_jh.join().unwrap(); // This thread should already have exit.
    self.sender.send(GenericInput::None); // Signal the network background to stop.
    Ok(self.network_thread_jh.join().unwrap())
  }
}

/// Check if the quitting KeyCode was entered.
fn should_quit(key: &KeyEvent) -> bool {
  match key.code {
    KeyCode::Esc | KeyCode::Char('q') => true,
    _ => false,
  }
}

// -----------------------------------------------------------------------------------------------
//  Print Utils
// -----------------------------------------------------------------------------------------------

#[derive(Tabled)]
struct PaxosGroupRow {
  replicated_group: String,
  leader: String,
  members: String,
}

#[derive(Tabled)]
struct ShardingRow {
  table_name: String,
  tablet_group_id: String,
  slave_group_id: String,
  range_start: String,
  range_end: String,
}

#[derive(Tabled)]
struct TableSchemaRow {
  table_name: String,
  key: String,
  columns: String,
}

/// Create a Table containing the
fn paxos_group_table(gossip_data: &GossipData, leader_map: &LeaderMap) -> String {
  let mut paxos_group_rows = Vec::<PaxosGroupRow>::new();

  // Add in Master data.
  let eids = gossip_data.get().master_address_config;
  let ips: Vec<_> = eids.iter().map(|eid| eid.ip.clone()).collect();
  paxos_group_rows.push(PaxosGroupRow {
    replicated_group: "Master".to_string(),
    leader: leader_map.get(&PaxosGroupId::Master).unwrap().eid.ip.clone(),
    members: ips.join(", "),
  });

  // Add in Slaves data.
  for (sid, eids) in gossip_data.get().slave_address_config {
    let ips: Vec<_> = eids.iter().map(|eid| eid.ip.clone()).collect();
    paxos_group_rows.push(PaxosGroupRow {
      replicated_group: format!("Slave {}", sid.0),
      leader: leader_map.get(&sid.to_gid()).unwrap().eid.ip.clone(),
      members: ips.join(", "),
    });
  }

  format!("Table 1: Paxos Configurations\n{}", Table::new(paxos_group_rows).to_string())
}

/// Convert the `start` or `end` of `TabletKeyRange` into a string.
fn range_bound_str(range_bound: &Option<ColVal>) -> String {
  match range_bound {
    None => "Unbounded".to_string(),
    Some(ColVal::Int(val)) => val.to_string(),
    Some(ColVal::Bool(val)) => val.to_string(),
    Some(ColVal::String(val)) => val.to_string(),
  }
}

/// Create a Display Table containing the sharding data.
fn sharding_table(gossip_data: &GossipData) -> String {
  let timestamp = gossip_data.get().table_generation.get_latest_lat();

  // Construct the sharding rows to display. There is a row for each Tablet.
  let mut sharding_rows = Vec::<ShardingRow>::new();
  let table_path_gens = gossip_data.get().table_generation.static_snapshot_read(&timestamp);
  for table_path_gen in table_path_gens {
    let shards = gossip_data.get().sharding_config.get(&table_path_gen).unwrap();
    let (table_path, _) = table_path_gen;
    for (range, tid) in shards {
      let sid = gossip_data.get().tablet_address_config.get(tid).unwrap();
      sharding_rows.push(ShardingRow {
        table_name: table_path.0.clone(),
        tablet_group_id: tid.0.clone(),
        slave_group_id: sid.0.clone(),
        range_start: range_bound_str(&range.start),
        range_end: range_bound_str(&range.end),
      });
    }
  }

  format!("Table 2: Sharding\n{}", Table::new(sharding_rows).to_string())
}

/// Create a display string for the set of FreeNodes in the system.
fn free_nodes(free_node_eids: Vec<EndpointId>) -> String {
  let ips: Vec<_> = free_node_eids.into_iter().map(|eid| eid.ip).collect();
  format!("FreeNodes: {}\n", ips.join(", "))
}

/// Convert `col_type` to a `String`.
pub fn col_type_str(col_type: &ColType) -> String {
  match col_type {
    ColType::Int => "Int".to_string(),
    ColType::Bool => "Bool".to_string(),
    ColType::String => "String".to_string(),
  }
}

/// Create a Table containing the
fn table_schemas_table(gossip_data: &GossipData) -> String {
  let timestamp = gossip_data.get().table_generation.get_latest_lat();

  // Construct the sharding rows to display. There is a row for each Tablet.
  let mut schema_rows = Vec::<TableSchemaRow>::new();
  let table_path_gens = gossip_data.get().table_generation.static_snapshot_read(&timestamp);
  for (table_path, (gen, _)) in table_path_gens {
    let table_path_gen = (table_path.clone(), gen);
    let schema = gossip_data.get().db_schema.get(&table_path_gen).unwrap();

    // Stringify the key columns.
    let mut key_elems = Vec::<String>::new();
    for (col_name, col_type) in &schema.key_cols {
      key_elems.push(format!("({}: {})", col_name.0, col_type_str(col_type)));
    }
    let key = key_elems.join(", ");

    // Stringify the value columns.
    let mut val_elems = Vec::<String>::new();
    for (col_name, col_type) in &schema.val_cols.static_snapshot_read(&timestamp) {
      val_elems.push(format!("({}: {})", col_name.0, col_type_str(col_type)));
    }
    let columns = val_elems.join(", ");

    schema_rows.push(TableSchemaRow { table_name: table_path.0.clone(), key, columns });
  }

  format!("Table 3: Schemas\n{}", Table::new(schema_rows).to_string())
}
