#![feature(map_first_last)]

mod server;

#[macro_use]
extern crate runiversal;

use crate::server::{ProdCoreIOCtx, ProdIOCtx, TIMER_INCREMENT};
use clap::{arg, App};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  mk_t, BasicIOCtx, FreeNodeIOCtx, GossipData, InternalMode, MasterIOCtx, NodeIOCtx, SlaveIOCtx,
};
use runiversal::common::{CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, SlaveGroupId};
use runiversal::coord::{CoordConfig, CoordContext, CoordForwardMsg, CoordState};
use runiversal::free_node_manager::FreeNodeType;
use runiversal::master::{
  FullMasterInput, MasterConfig, MasterContext, MasterState, MasterTimerInput,
};
use runiversal::message as msg;
use runiversal::message::FreeNodeMessage;
use runiversal::net::{handle_self_conn, send_msg, start_acceptor_thread, SendAction};
use runiversal::node::{get_prod_configs, GenericInput, NodeConfig, NodeState};
use runiversal::paxos::PaxosConfig;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveConfig, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::tablet::TabletConfig;
use runiversal::test_utils as tu;
use runiversal::test_utils::mk_seed;
use std::collections::{BTreeMap, LinkedList};
use std::env;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

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
  let out_conn_map = Arc::new(Mutex::new(BTreeMap::<EndpointId, Sender<SendAction>>::new()));

  // Start the Accepting Thread
  start_acceptor_thread(&to_server_sender, this_ip.clone());

  // Create the self-connection
  let this_internal_mode = InternalMode::Internal;
  let this_eid = EndpointId::new(this_ip, this_internal_mode.clone());
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
      let master_eid = EndpointId::new(master_ip, InternalMode::Internal);

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
        SendAction::new(
          msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
            msg::FreeNodeAssoc::RegisterFreeNode(msg::RegisterFreeNode {
              sender_eid: this_eid.clone(),
              node_type,
            }),
          )),
          None,
        ),
        &this_internal_mode,
      );
    }
    _ => unreachable!(),
  }

  let mut io_ctx = ProdIOCtx {
    rand: XorShiftRng::from_entropy(),
    out_conn_map,
    exited: false,
    to_top: to_server_sender,
    tablet_map: Default::default(),
    coord_map: Default::default(),
    tasks: Arc::new(Mutex::new(Default::default())),
  };
  io_ctx.start();

  let mut node = NodeState::new(this_eid, get_prod_configs());
  node.bootstrap(&mut io_ctx);

  // Enter the main loop forever.
  loop {
    let generic_input = to_server_receiver.recv().unwrap();
    node.process_input(&mut io_ctx, generic_input);
  }
}
