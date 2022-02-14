#![feature(map_first_last)]

mod server;

#[macro_use]
extern crate runiversal;

use crate::server::{handle_conn, handle_self_conn, ProdCoreIOCtx, ProdIOCtx, TIMER_INCREMENT};
use clap::{arg, App};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  mk_t, BasicIOCtx, FreeNodeIOCtx, GossipData, MasterIOCtx, NodeIOCtx, SlaveIOCtx,
};
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
use runiversal::net::{recv, send_bytes, send_msg, SERVER_PORT};
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

  let mut io_ctx = ProdIOCtx {
    rand: XorShiftRng::from_entropy(),
    out_conn_map,
    exited: false,
    to_top: to_server_sender,
    tablet_map: Default::default(),
    coord_map: Default::default(),
    tasks: Arc::new(Mutex::new(Default::default())),
  };

  let mut node = NodeState::new(this_eid, get_prod_configs());
  node.bootstrap(&mut io_ctx);

  // Enter the main loop forever.
  loop {
    let generic_input = to_server_receiver.recv().unwrap();
    node.process_input(&mut io_ctx, generic_input);
  }
}
