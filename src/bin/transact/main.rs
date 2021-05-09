mod server;

use crate::server::start_server;
use runiversal::model::common::EndpointId;
use runiversal::net::{recv, send};
use std::collections::{HashMap, LinkedList};
use std::env;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

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

const SERVER_PORT: u32 = 1610;

fn handle_conn(
  net_conn_map: &Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
  to_server_sender: &Sender<(EndpointId, Vec<u8>)>,
  stream: TcpStream,
) -> EndpointId {
  let endpoint_id = EndpointId(stream.peer_addr().unwrap().ip().to_string());

  // Setup FromNetwork Thread
  {
    let to_server_sender = to_server_sender.clone();
    let endpoint_id = endpoint_id.clone();
    let stream = stream.try_clone().unwrap();
    thread::spawn(move || loop {
      let val_in = recv(&stream);
      to_server_sender.send((endpoint_id.clone(), val_in)).unwrap();
    });
  }

  // This is the FromServer Queue.
  let (from_server_sender, from_server_receiver) = mpsc::channel();
  // Add from_server_sender to the net_conn_map so the Server Thread can access it.
  let mut net_conn_map = net_conn_map.lock().unwrap();
  net_conn_map.insert(endpoint_id.clone(), from_server_sender);

  // Setup ToNetwork Thread
  thread::spawn(move || loop {
    let data_out = from_server_receiver.recv().unwrap();
    send(&data_out, &stream);
  });

  return endpoint_id;
}

fn handle_self_conn(
  endpoint_id: &EndpointId,
  net_conn_map: &Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
  to_server_sender: &Sender<(EndpointId, Vec<u8>)>,
) {
  // This is the FromServer Queue.
  let (from_server_sender, from_server_receiver) = mpsc::channel();
  // Add sender of the SPSC to the net_conn_map so the Server Thread can access it.
  let mut net_conn_map = net_conn_map.lock().unwrap();
  net_conn_map.insert(endpoint_id.clone(), from_server_sender);

  // Setup Self Connection Thread
  let endpoint_id = endpoint_id.clone();
  let to_server_sender = to_server_sender.clone();
  thread::spawn(move || loop {
    let data = from_server_receiver.recv().unwrap();
    to_server_sender.send((endpoint_id.clone(), data)).unwrap();
  });
}

fn main() {
  let mut args: LinkedList<String> = env::args().collect();

  // Removes the program name argument.
  args.pop_front();
  // Pop the seed
  let slave_index = args
    .pop_front()
    .expect("A slave index should be provided.")
    .parse::<u32>()
    .expect("The slave index couldn't be parsed as a string.");
  // Pop the IP address
  let cur_ip = args.pop_front().expect("The endpoint_id of the current slave should be provided.");

  // The mpsc channel for sending data to the Server Thread from all FromNetwork Threads.
  let (to_server_sender, to_server_receiver) = mpsc::channel();
  // The map mapping the IP addresses to an FromServer Queue, used to
  // communicate with the ToNetwork Threads to send data out.
  let net_conn_map = Arc::new(Mutex::new(HashMap::<EndpointId, Sender<Vec<u8>>>::new()));

  // Start the Accepting Thread
  {
    let to_server_sender = to_server_sender.clone();
    let net_conn_map = net_conn_map.clone();
    let cur_ip = cur_ip.clone();
    thread::spawn(move || {
      let listener = TcpListener::bind(format!("{}:{}", &cur_ip, SERVER_PORT)).unwrap();
      for stream in listener.incoming() {
        let stream = stream.unwrap();
        let endpoint_id = handle_conn(&net_conn_map, &to_server_sender, stream);
        println!("Connected from: {:?}", endpoint_id);
      }
    });
  }

  // Connect to other IPs
  for ip in args {
    let stream = TcpStream::connect(format!("{}:{}", ip, SERVER_PORT));
    let endpoint_id = handle_conn(&net_conn_map, &to_server_sender, stream.unwrap());
    println!("Connected to: {:?}", endpoint_id);
  }

  // Handle self-connection
  let endpoint_id = EndpointId(cur_ip);
  handle_self_conn(&endpoint_id, &net_conn_map, &to_server_sender);

  // Start the server
  start_server(to_server_receiver, &net_conn_map, slave_index);
}
