#![feature(map_first_last)]

mod server;

#[macro_use]
extern crate runiversal;

use crate::server::{
  handle_conn, handle_self_conn, send_msg, ProdCoreIOCtx, ProdMasterIOCtx, ProdSlaveIOCtx,
  TIMER_INCREMENT,
};
use clap::{arg, App};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{mk_t, BasicIOCtx, GossipData};
use runiversal::coord::{CoordConfig, CoordContext, CoordForwardMsg, CoordState};
use runiversal::free_node_manager::FreeNodeType;
use runiversal::master::{
  FullMasterInput, MasterConfig, MasterContext, MasterState, MasterTimerInput,
};
use runiversal::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, SlaveGroupId,
};
use runiversal::model::message as msg;
use runiversal::model::message::FreeNodeMessage;
use runiversal::net::{recv, send_bytes};
use runiversal::paxos::PaxosConfig;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveConfig, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::test_utils as tu;
use runiversal::test_utils::mk_seed;
use std::collections::{BTreeMap, LinkedList};
use std::env;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

/// The threading architecture we use is as follows. Every network
/// connection has 2 threads, one for receiving data, called the
/// FromNetwork Thread (which spends most of its time blocking on reading
/// the socket), and one thread for sending data, called the ToNetwork
/// Thread (which spends most of its time blocking on a FromServer Queue,
/// which we create every time a new socket is created). Once a FromNetwork
/// Thread receives a packet, it puts it into a Multi-Producer-Single-Consumer
/// Queue, called the ToServer MPSC. Here, each FromNetwork Thread is a Producer,
/// and the only Consumer is the Server Thread. Once the Server Thread wants
/// to send a packet out of a socket, it places it in the socket's FromServer
/// Queue. The ToNetwork Thread picks this up and sends it out of the socket.
/// The FromServer Queue is a Single-Producer-Single-Consumer queue.
///
/// The Server Thread also needs to connect to itself. We don't use
/// a network socket for this, and we don't have two auxiliary threads.
/// Instead, we have one auxiliary thread, called the Self Connection
/// Thread, which takes packets that are sent out of Server Thread and
/// immediately feeds it back in.
///
/// We also have an Accepting Thread that listens for new connections
/// and constructs the FromNetwork Thread, ToNetwork Thread, and connects
/// them up.

// TODO:
//  1. document above the now only use uni-direction queues.
//  2. Figure out what happens if the other side disconnects. The `stream.read_u32`
//    function probably returns an error, and we can remove the EndpointId from
//    `out_conn_map`. However, we need to figure out if this TCP API is a long-term,
//    persisted connection (or if it will randomly disconnect with the other side, e.g.
//    due to inactivity or temporary network partitions).

// -----------------------------------------------------------------------------------------------
//  GenericInput
// -----------------------------------------------------------------------------------------------

const SERVER_PORT: u32 = 1610;

#[derive(Debug)]
pub enum GenericInput {
  Message(EndpointId, msg::NetworkMessage),
  SlaveTimerInput(SlaveTimerInput),
  SlaveBackMessage(SlaveBackMessage),
  MasterTimerInput(MasterTimerInput),
  FreeNodeTimerInput,
}

// -----------------------------------------------------------------------------------------------
//  Buffer Helpers
// -----------------------------------------------------------------------------------------------

/// Used to add elements form a BTree MultiMap
pub fn amend_buffer<MessageT>(
  buffered_messages: &mut BTreeMap<EndpointId, Vec<MessageT>>,
  eid: &EndpointId,
  message: MessageT,
) {
  if let Some(buffer) = buffered_messages.get_mut(eid) {
    buffer.push(message);
  } else {
    buffered_messages.insert(eid.clone(), vec![message]);
  }
}

// -----------------------------------------------------------------------------------------------
//  NominalSlaveState
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct NominalSlaveState {
  state: SlaveState,
  io_ctx: ProdSlaveIOCtx,
  /// Messages (which are not client messages) that came from `EndpointId`s that is not present
  /// in `state.get_eids`.
  buffered_messages: BTreeMap<EndpointId, Vec<msg::SlaveMessage>>,
  /// The `Gen` of the set of `EndpointId`s that `state` currently allows
  /// messages to pass through from.
  cur_gen: Gen,
}

impl NominalSlaveState {
  fn init(
    state: SlaveState,
    io_ctx: ProdSlaveIOCtx,
    buffered_messages: BTreeMap<EndpointId, Vec<msg::SlaveMessage>>,
  ) -> NominalSlaveState {
    // Record and maintain the current version of `get_eids`, which we use to
    // detect if `get_eids` changes and ensure that all buffered messages that
    // should be delivered actually are.
    let mut cur_gen = state.get_eids().get_gen().clone();

    // Construct NominalSlaveState
    let mut nominal_state = NominalSlaveState { state, io_ctx, buffered_messages, cur_gen };

    // Deliver all buffered messages, leaving buffered only what should be
    // buffered. (Importantly, there might be Slave messages, like Paxos,
    // already present from the other Slave nodes being bootstrapped).
    nominal_state.deliver_all_once();

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    nominal_state.deliver_all();
    nominal_state
  }

  fn handle_msg(&mut self, eid: &EndpointId, slave_msg: msg::SlaveMessage) {
    // Pass through normally or buffer.
    self.deliver_single_once(eid, slave_msg);

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    self.deliver_all();
  }

  /// Deliver the `slave_message` to `slave_state`, or buffer it.
  fn deliver_single_once(&mut self, eid: &EndpointId, slave_msg: msg::SlaveMessage) {
    // If the message is from the External, we deliver it.
    if let msg::SlaveMessage::SlaveExternalReq(_) = &slave_msg {
      self.state.handle_input(&mut self.io_ctx, FullSlaveInput::SlaveMessage(slave_msg));
    }
    // Otherwise, if it is from an EndpointId from `get_eids`, we deliver it.
    else if self.state.get_eids().get_value().contains(eid) {
      self.state.handle_input(&mut self.io_ctx, FullSlaveInput::SlaveMessage(slave_msg));
    }
    // Otherwise, if the message is a tier 1 message, we deliver it
    else if slave_msg.is_tier_1() {
      self.state.handle_input(&mut self.io_ctx, FullSlaveInput::SlaveMessage(slave_msg));
    } else {
      // Otherwise, we buffer the message.
      amend_buffer(&mut self.buffered_messages, eid, slave_msg);
    }
  }

  fn deliver_all_once(&mut self) {
    // Deliver all messages that were buffered so far.
    let mut old_buffered_messages = std::mem::take(&mut self.buffered_messages);
    for (eid, buffer) in old_buffered_messages {
      for slave_msg in buffer {
        self.deliver_single_once(&eid, slave_msg);
      }
    }
  }

  fn deliver_all(&mut self) {
    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    let mut gen_did_change = &self.cur_gen != self.state.get_eids().get_gen();
    while gen_did_change {
      let cur_gen = self.state.get_eids().get_gen().clone();
      self.deliver_all_once();
      // Check again whether the `get_eids` changed.
      gen_did_change = &cur_gen != self.state.get_eids().get_gen();
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  NominalMasterState
// -----------------------------------------------------------------------------------------------

// TODO: add a tier to the network message to help reasoning about the
//  below easier.

#[derive(Debug)]
struct NominalMasterState {
  state: MasterState,
  io_ctx: ProdMasterIOCtx,
  /// Messages (which are not client messages) that came from `EndpointId`s that is not present
  /// in `state.get_eids`.
  buffered_messages: BTreeMap<EndpointId, Vec<msg::MasterMessage>>,
  /// The `Gen` of the set of `EndpointId`s that `state` currently allows
  /// messages to pass through from.
  cur_gen: Gen,
}

impl NominalMasterState {
  fn init(
    state: MasterState,
    io_ctx: ProdMasterIOCtx,
    buffered_messages: BTreeMap<EndpointId, Vec<msg::MasterMessage>>,
  ) -> NominalMasterState {
    // Record and maintain the current version of `get_eids`, which we use to
    // detect if `get_eids` changes and ensure that all buffered messages that
    // should be delivered actually are.
    let mut cur_gen = state.get_eids().get_gen().clone();

    // Construct NominalMasterState
    let mut nominal_state = NominalMasterState { state, io_ctx, buffered_messages, cur_gen };

    // Deliver all buffered messages, leaving buffered only what should be
    // buffered. (Importantly, there might be Master messages, like Paxos,
    // already present from the other Master nodes being bootstrapped).
    nominal_state.deliver_all_once();

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    nominal_state.deliver_all();
    nominal_state
  }

  fn handle_msg(&mut self, eid: &EndpointId, master_msg: msg::MasterMessage) {
    // Pass through normally or buffer.
    self.deliver_single_once(eid, master_msg);

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    self.deliver_all();
  }

  /// Deliver the `master_message` to `master_state`, or buffer it.
  fn deliver_single_once(&mut self, eid: &EndpointId, master_msg: msg::MasterMessage) {
    // If the message is from the External, we deliver it.
    if let msg::MasterMessage::MasterExternalReq(_) = &master_msg {
      self.state.handle_input(&mut self.io_ctx, FullMasterInput::MasterMessage(master_msg));
    }
    // Otherwise, if it is from an EndpointId from `get_eids`, we deliver it.
    else if self.state.get_eids().get_value().contains(eid) {
      self.state.handle_input(&mut self.io_ctx, FullMasterInput::MasterMessage(master_msg));
    }
    // Otherwise, if the message is a tier 1 message, we deliver it
    else if master_msg.is_tier_1() {
      self.state.handle_input(&mut self.io_ctx, FullMasterInput::MasterMessage(master_msg));
    } else {
      // Otherwise, we buffer the message.
      amend_buffer(&mut self.buffered_messages, eid, master_msg);
    }
  }

  fn deliver_all_once(&mut self) {
    // Deliver all messages that were buffered so far.
    let mut old_buffered_messages = std::mem::take(&mut self.buffered_messages);
    for (eid, buffer) in old_buffered_messages {
      for master_msg in buffer {
        self.deliver_single_once(&eid, master_msg);
      }
    }
  }

  fn deliver_all(&mut self) {
    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    let mut gen_did_change = &self.cur_gen != self.state.get_eids().get_gen();
    while gen_did_change {
      let cur_gen = self.state.get_eids().get_gen().clone();
      self.deliver_all_once();
      // Check again whether the `get_eids` changed.
      gen_did_change = &cur_gen != self.state.get_eids().get_gen();
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  NodeState
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum NodeState {
  DNEState(BTreeMap<EndpointId, Vec<msg::NetworkMessage>>),
  FreeNodeState(LeadershipId, BTreeMap<EndpointId, Vec<msg::NetworkMessage>>),
  NominalSlaveState(NominalSlaveState),
  NominalMasterState(NominalMasterState),
  PostExistence,
}

// -----------------------------------------------------------------------------------------------
//  General
// -----------------------------------------------------------------------------------------------

fn mk_master_config() -> MasterConfig {
  MasterConfig { timestamp_suffix_divisor: 100 }
}

// -----------------------------------------------------------------------------------------------
//  Main
// -----------------------------------------------------------------------------------------------

fn main() {
  // Setup CLI parsing
  let matches = App::new("rUniversalDB")
    .version("1.0")
    .author("Pasindu M. <pasindumuth@gmail.com>")
    .arg(
      arg!(-t --startup_type <VALUE>)
        .required(true)
        .help("Indicates if this is an initial Master node ('masterbootup') or not ('freenode').'")
        .possible_values(["masterbootup", "freenode"]),
    )
    .arg(arg!(-i --ip <VALUE>).required(true).help("The IP address of the current host."))
    .arg(
      arg!(-f --freenode_type <VALUE>)
        .required(false)
        .help("The type of freenode this is.")
        .possible_values(["newslave", "reconfig"]),
    )
    .arg(arg!(-e --entry_ip <VALUE>).required(false).help(
      "The IP address of the current Master \
       Leader. (This is unused if the startup_type is 'masterbootup').",
    ))
    .get_matches();

  // Get required arguments
  let startup_type = matches.value_of("startup_type").unwrap().to_string();
  let this_ip = matches.value_of("ip").unwrap().to_string();

  // The mpsc channel for passing data to the Server Thread from all FromNetwork Threads.
  let (to_server_sender, to_server_receiver) = mpsc::channel::<GenericInput>();
  // Maps the IP addresses to a FromServer Queue, used to send data to Outgoing Connections.
  let out_conn_map = Arc::new(Mutex::new(BTreeMap::<EndpointId, Sender<Vec<u8>>>::new()));

  // Start the Accepting Thread
  {
    let to_server_sender = to_server_sender.clone();
    let this_ip = this_ip.clone();
    thread::spawn(move || {
      let listener = TcpListener::bind(format!("{}:{}", &this_ip, SERVER_PORT)).unwrap();
      for stream in listener.incoming() {
        let stream = stream.unwrap();
        let endpoint_id = handle_conn(&to_server_sender, stream);
        println!("Connected from: {:?}", endpoint_id);
      }
    });
  }

  // Create the self-connection
  let this_eid = EndpointId(this_ip);
  handle_self_conn(&this_eid, &out_conn_map, &to_server_sender);

  // Run startup_type specific code.
  match &startup_type[..] {
    "masterbootup" => {}
    "freenode" => {
      // Parse entry_ip
      let master_ip = matches
        .value_of("entry_ip")
        .expect("entry_ip is requred if startup_type is 'freenode'")
        .to_string();
      let master_eid = EndpointId(master_ip);

      // Parse freenode_type
      let freenode_type = matches
        .value_of("freenode_type")
        .expect("entry_ip is requred if startup_type is 'freenode'");

      let node_type = match freenode_type {
        "newslave" => FreeNodeType::NewSlaveFreeNode,
        "reconfig" => FreeNodeType::ReconfigFreeNode,
        _ => unreachable!(),
      };

      // Send RegisterFreeNode
      send_msg(
        &out_conn_map,
        &master_eid,
        msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
          msg::FreeNodeAssoc::RegisterFreeNode(msg::RegisterFreeNode {
            sender_eid: this_eid.clone(),
            node_type,
          }),
        )),
      );
    }
    _ => unreachable!(),
  }

  // Setup FreeNode timer
  {
    // We use a simple approach for now, where we just generate
    // FreeNodeTimerInput periodically forever.
    let to_server_sender = to_server_sender.clone();
    thread::spawn(move || loop {
      // Sleep and then dispatch
      let increment = std::time::Duration::from_micros(TIMER_INCREMENT);
      thread::sleep(increment);
      to_server_sender.send(GenericInput::FreeNodeTimerInput);
    });
  }

  let mut node_state = NodeState::DNEState(BTreeMap::default());
  loop {
    let generic_input = to_server_receiver.recv().unwrap();
    match &mut node_state {
      NodeState::DNEState(buffered_messages) => match generic_input {
        GenericInput::Message(eid, message) => {
          // Handle FreeNode messages
          if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
            match free_node_msg {
              msg::FreeNodeMessage::FreeNodeRegistered(registered) => {
                node_state =
                  NodeState::FreeNodeState(registered.cur_lid, std::mem::take(buffered_messages));
              }
              msg::FreeNodeMessage::ShutdownNode => {
                node_state = NodeState::PostExistence;
              }
              msg::FreeNodeMessage::StartMaster(start) => {
                // Create the ProdMasterIOCtx
                let mut rand = XorShiftRng::from_entropy();
                let mut io_ctx = ProdMasterIOCtx {
                  rand,
                  out_conn_map: out_conn_map.clone(),
                  exited: false,
                  to_master: to_server_sender.clone(),
                  tasks: Arc::new(Mutex::new(BTreeMap::default())),
                };

                // Create the MasterState
                let leader = start.master_eids.get(0).unwrap();
                let master_lid = LeadershipId { gen: Gen(0), eid: leader.clone() };
                let mut leader_map = BTreeMap::<PaxosGroupId, LeadershipId>::new();
                leader_map.insert(PaxosGroupId::Master, master_lid);
                let mut master_state = MasterState::new(MasterContext::create_initial(
                  mk_master_config(),
                  this_eid.clone(),
                  BTreeMap::default(),
                  start.master_eids,
                  leader_map.clone(),
                  PaxosConfig::prod(),
                ));

                // Bootstrap the Master
                master_state.bootstrap(&mut io_ctx);

                // Convert all buffered messages to MasterMessage, since those should be all
                // that is present.
                let mut master_buffered_msgs =
                  BTreeMap::<EndpointId, Vec<msg::MasterMessage>>::new();
                for (eid, buffer) in std::mem::take(buffered_messages) {
                  for message in buffer {
                    let master_msg = cast!(msg::NetworkMessage::Master, message).unwrap();
                    amend_buffer(&mut master_buffered_msgs, &eid, master_msg);
                  }
                }

                // Advance
                node_state = NodeState::NominalMasterState(NominalMasterState::init(
                  master_state,
                  io_ctx,
                  master_buffered_msgs,
                ));
              }
              _ => {}
            }
          } else {
            // Otherwise, buffer the message
            amend_buffer(buffered_messages, &eid, message);
          }
        }
        _ => {}
      },
      NodeState::FreeNodeState(lid, buffered_messages) => match generic_input {
        GenericInput::Message(eid, message) => {
          // Handle FreeNode messages
          if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
            match free_node_msg {
              FreeNodeMessage::MasterLeadershipId(new_lid) => {
                // Update the Leadership Id if it is more recent.
                if new_lid.gen > lid.gen {
                  *lid = new_lid;
                }
              }
              FreeNodeMessage::CreateSlaveGroup(create) => {
                // Create Slave RNG.
                let mut rand = XorShiftRng::from_entropy();

                // Create GossipData
                let gossip = Arc::new(create.gossip);

                // Create the Coord
                let mut coord_map = BTreeMap::<CoordGroupId, Sender<CoordForwardMsg>>::new();
                let mut coord_positions: Vec<CoordGroupId> = Vec::new();
                for coord_group_id in create.coord_ids {
                  coord_positions.push(coord_group_id.clone());
                  // Create the seed for the Tablet's RNG. We use the Slave's
                  // RNG to create a random seed.
                  let rand = XorShiftRng::from_seed(mk_seed(&mut rand));

                  // Create mpsc queue for Slave-Coord communication.
                  let (to_coord_sender, to_coord_receiver) = mpsc::channel();
                  coord_map.insert(coord_group_id.clone(), to_coord_sender);

                  // Create the Tablet
                  let coord_context = CoordContext::new(
                    CoordConfig::default(),
                    create.sid.clone(),
                    coord_group_id,
                    this_eid.clone(),
                    gossip.clone(),
                    create.leader_map.clone(),
                  );
                  let mut io_ctx = ProdCoreIOCtx {
                    out_conn_map: out_conn_map.clone(),
                    exited: false,
                    rand,
                    to_slave: to_server_sender.clone(),
                  };
                  thread::spawn(move || {
                    let mut coord = CoordState::new(coord_context);
                    loop {
                      let coord_msg = to_coord_receiver.recv().unwrap();
                      coord.handle_input(&mut io_ctx, coord_msg);
                    }
                  });
                }

                // Construct the SlaveState
                let mut io_ctx = ProdSlaveIOCtx {
                  rand,
                  out_conn_map: out_conn_map.clone(),
                  exited: false,
                  to_slave: to_server_sender.clone(),
                  tablet_map: Default::default(),
                  coord_map,
                  tasks: Arc::new(Mutex::new(Default::default())),
                };
                io_ctx.start();
                let slave_context = SlaveContext::new(
                  coord_positions,
                  SlaveConfig::default(),
                  create.sid.clone(),
                  this_eid.clone(),
                  gossip,
                  create.leader_map,
                  PaxosConfig::prod(),
                );
                let mut slave_state = SlaveState::new(slave_context);

                // Bootstrap the slave
                slave_state.bootstrap(&mut io_ctx);

                // Convert all buffered messages to SlaveMessages, since those should be all
                // that is present.
                let mut slave_buffered_msgs = BTreeMap::<EndpointId, Vec<msg::SlaveMessage>>::new();
                for (eid, buffer) in std::mem::take(buffered_messages) {
                  for message in buffer {
                    let slave_msg = cast!(msg::NetworkMessage::Slave, message).unwrap();
                    amend_buffer(&mut slave_buffered_msgs, &eid, slave_msg);
                  }
                }

                // Advance
                node_state = NodeState::NominalSlaveState(NominalSlaveState::init(
                  slave_state,
                  io_ctx,
                  slave_buffered_msgs,
                ));

                // Respond with a `ConfirmSlaveCreation`.
                send_msg(
                  &out_conn_map,
                  &eid,
                  msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
                    msg::FreeNodeAssoc::ConfirmSlaveCreation(msg::ConfirmSlaveCreation {
                      sid: create.sid,
                      sender_eid: this_eid.clone(),
                    }),
                  )),
                );
              }
              FreeNodeMessage::SlaveSnapshot(_) => {
                // TODO: do
              }
              FreeNodeMessage::MasterSnapshot(snapshot) => {
                // Create the ProdMasterIOCtx
                let mut rand = XorShiftRng::from_entropy();
                let mut io_ctx = ProdMasterIOCtx {
                  rand,
                  out_conn_map: out_conn_map.clone(),
                  exited: false,
                  to_master: to_server_sender.clone(),
                  tasks: Arc::new(Mutex::new(BTreeMap::default())),
                };

                // Create the MasterState
                let mut master_state = MasterState::new(MasterContext::create_reconfig(
                  &mut io_ctx,
                  mk_master_config(),
                  this_eid.clone(),
                  snapshot,
                  PaxosConfig::prod(),
                ));

                // Bootstrap the Master
                master_state.bootstrap(&mut io_ctx);

                // Convert all buffered messages to MasterMessage, since those should be all
                // that is present.
                let mut master_buffered_msgs =
                  BTreeMap::<EndpointId, Vec<msg::MasterMessage>>::new();
                for (eid, buffer) in std::mem::take(buffered_messages) {
                  for message in buffer {
                    let master_msg = cast!(msg::NetworkMessage::Master, message).unwrap();
                    amend_buffer(&mut master_buffered_msgs, &eid, master_msg);
                  }
                }

                // Advance
                node_state = NodeState::NominalMasterState(NominalMasterState::init(
                  master_state,
                  io_ctx,
                  master_buffered_msgs,
                ));
              }
              FreeNodeMessage::ShutdownNode => {
                node_state = NodeState::PostExistence;
              }
              _ => {}
            }
          } else {
            // Otherwise, buffer the message
            amend_buffer(buffered_messages, &eid, message);
          }
        }
        GenericInput::FreeNodeTimerInput => {
          // Send out `FreeNodeHeartbeat`
          send_msg(
            &out_conn_map,
            &lid.eid,
            msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
              msg::FreeNodeAssoc::FreeNodeHeartbeat(msg::FreeNodeHeartbeat {
                sender_eid: this_eid.clone(),
                cur_lid: lid.clone(),
              }),
            )),
          );
        }
        _ => {}
      },
      NodeState::NominalSlaveState(nominal_state) => {
        match generic_input {
          GenericInput::Message(eid, message) => {
            // Handle FreeNode messages
            if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
              match free_node_msg {
                FreeNodeMessage::CreateSlaveGroup(_) => {
                  // Respond with a `ConfirmSlaveCreation`.
                  send_msg(
                    &out_conn_map,
                    &eid,
                    msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
                      msg::FreeNodeAssoc::ConfirmSlaveCreation(msg::ConfirmSlaveCreation {
                        sid: nominal_state.state.ctx.this_sid.clone(),
                        sender_eid: this_eid.clone(),
                      }),
                    )),
                  );
                }
                FreeNodeMessage::SlaveSnapshot(_) => {
                  // Respond with a `NewNodeStarted`.
                  send_msg(
                    &out_conn_map,
                    &eid,
                    msg::NetworkMessage::Slave(msg::SlaveMessage::PaxosDriverMessage(
                      msg::PaxosDriverMessage::NewNodeStarted(msg::NewNodeStarted {
                        paxos_node: this_eid.clone(),
                      }),
                    )),
                  );
                }
                FreeNodeMessage::ShutdownNode => {
                  nominal_state.io_ctx.mark_exit();
                }
                _ => {}
              }
            } else if let msg::NetworkMessage::Slave(slave_msg) = message {
              // Forward the message.
              nominal_state.handle_msg(&eid, slave_msg);
            }
          }
          GenericInput::SlaveTimerInput(timer_input) => {
            // Forward the `SlaveTimerInput`
            nominal_state.state.handle_input(
              &mut nominal_state.io_ctx,
              FullSlaveInput::SlaveTimerInput(timer_input),
            );
          }
          GenericInput::SlaveBackMessage(back_msg) => {
            // Forward the `SlaveBackMessage`
            nominal_state
              .state
              .handle_input(&mut nominal_state.io_ctx, FullSlaveInput::SlaveBackMessage(back_msg));
          }
          _ => {}
        }

        // Check the IOCtx and see if we should go to post existence.
        if nominal_state.io_ctx.did_exit() {
          node_state = NodeState::PostExistence;
        }
      }
      NodeState::NominalMasterState(nominal_state) => {
        match generic_input {
          GenericInput::Message(eid, message) => {
            // Handle FreeNode messages
            if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
              match free_node_msg {
                FreeNodeMessage::MasterSnapshot(_) => {
                  // Respond with a `NewNodeStarted`.
                  send_msg(
                    &out_conn_map,
                    &eid,
                    msg::NetworkMessage::Master(msg::MasterMessage::PaxosDriverMessage(
                      msg::PaxosDriverMessage::NewNodeStarted(msg::NewNodeStarted {
                        paxos_node: this_eid.clone(),
                      }),
                    )),
                  );
                }
                FreeNodeMessage::ShutdownNode => {
                  nominal_state.io_ctx.mark_exit();
                }
                _ => {}
              }
            } else if let msg::NetworkMessage::Master(master_msg) = message {
              // Forward the message.
              nominal_state.handle_msg(&eid, master_msg);
            }
          }
          GenericInput::MasterTimerInput(timer_input) => {
            // Forward the `MasterTimerInput`
            nominal_state.state.handle_input(
              &mut nominal_state.io_ctx,
              FullMasterInput::MasterTimerInput(timer_input),
            );
          }
          _ => {}
        }

        // Check the IOCtx and see if we should go to post existence.
        if nominal_state.io_ctx.did_exit() {
          node_state = NodeState::PostExistence;
        }
      }
      NodeState::PostExistence => {}
    }
  }
}
