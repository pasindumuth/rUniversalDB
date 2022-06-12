use crate::common::{EndpointId, InternalMode};
use crate::message as msg;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::info;
use std::collections::BTreeMap;
use std::io::{Read, Write};
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

// -----------------------------------------------------------------------------------------------
//  Network Output Connections
// -----------------------------------------------------------------------------------------------
// We use simple 4 byte header that holds the length of the real message.

fn send_bytes(data: &[u8], mut stream: &TcpStream) -> std::io::Result<()> {
  // Write out the fixed-size header
  stream.write_u32::<BigEndian>(data.len() as u32)?;
  // Write out the full message
  stream.write(data)?;
  Ok(())
}

fn recv(mut stream: &TcpStream) -> std::io::Result<Vec<u8>> {
  // Read in the fixed-size header
  let size = stream.read_u32::<BigEndian>()?;
  // Read in the full message
  let mut buf = vec![0; size as usize];
  let mut i = 0;
  while i < size {
    let num_read = stream.read(&mut buf[i as usize..])?;
    i += num_read as u32;
  }
  Ok(buf)
}

// -----------------------------------------------------------------------------------------------
//  Network Output Connections
// -----------------------------------------------------------------------------------------------
// In the below scheme, network connections are only ever constructed lazily
// (i.e. when doing `send_msg`), and once a connection is dropped, it is never re-created
// again. Thus, we have the FIFO behavior of TCP, even after the connection drops.

const SERVER_PORT: u32 = 1610;

pub trait GenericInputTrait {
  fn from_network(eid: EndpointId, message: msg::NetworkMessage) -> Self;
}

/// Starts the Acceptor Thread, which accepts connections at `SERVER_PORT` and creates
/// a `FromNetwork` Thread for each new connection.
pub fn start_acceptor_thread<GenericInputT: 'static + GenericInputTrait + Send>(
  to_server_sender: &Sender<GenericInputT>,
  this_ip: String,
) {
  let to_server_sender = to_server_sender.clone();
  let this_ip = this_ip.clone();
  thread::spawn(move || {
    let listener = TcpListener::bind(format!("{}:{}", &this_ip, SERVER_PORT)).unwrap();
    for stream in listener.incoming() {
      let stream = stream.unwrap();
      handle_conn(&to_server_sender, stream);
    }
  });
}

/// Creates the FromNetwork threads for this new Incoming Connection, `stream`.
fn handle_conn<GenericInputT: 'static + GenericInputTrait + Send>(
  to_server_sender: &Sender<GenericInputT>,
  stream: TcpStream,
) {
  let ip = stream.peer_addr().unwrap().ip().to_string();

  // Configure the stream to block indefinitely for reads and writes.
  stream.set_read_timeout(None).unwrap();
  stream.set_write_timeout(None).unwrap();

  // Setup FromNetwork Thread
  {
    let ip = ip.clone();
    let to_server_sender = to_server_sender.clone();
    let stream = stream.try_clone().unwrap();
    thread::Builder::new()
      .name(format!("FromNetwork {}", ip))
      .spawn(move || {
        let error = match recv(&stream) {
          Ok(data) => {
            // Read the Initialization message and construct the EndpointId accordingly.
            let init_msg: msg::InitMessage = rmp_serde::from_read_ref(&data).unwrap();
            let eid = EndpointId::new(ip.clone(), init_msg.is_internal);

            // Read data until the connection closes.
            loop {
              match recv(&stream) {
                Ok(data) => {
                  let network_msg: msg::NetworkMessage = rmp_serde::from_read_ref(&data).unwrap();
                  to_server_sender
                    .send(GenericInputT::from_network(eid.clone(), network_msg))
                    .unwrap();
                }
                Err(error) => {
                  // This means that the connection effectively closed.
                  break error;
                }
              };
            }
          }
          Err(error) => {
            // This means that the connection effectively closed.
            error
          }
        };

        info!(
          "Thread 'FromNetwork {}' shutting down. \
         Connection closed with error: {}",
          ip, error
        );
      })
      .unwrap();
  }
}

/// Creates a thread that acts as both the FromNetwork and ToNetwork Threads,
/// setting up both the Incoming Connection as well as Outgoing Connection at once.
pub fn handle_self_conn<GenericInputT: 'static + GenericInputTrait + Send>(
  this_eid: &EndpointId,
  out_conn_map: &Arc<Mutex<BTreeMap<EndpointId, Sender<SendAction>>>>,
  to_server_sender: &Sender<GenericInputT>,
) {
  let mut out_conn_map = out_conn_map.lock().unwrap();
  let (sender, receiver) = mpsc::channel();
  out_conn_map.insert(this_eid.clone(), sender);

  // Setup Self Connection Thread
  let to_server_sender = to_server_sender.clone();
  let this_eid = this_eid.clone();
  thread::Builder::new()
    .name(format!("Self Connection"))
    .spawn(move || loop {
      let send_action = receiver.recv().unwrap();
      to_server_sender
        .send(GenericInputT::from_network(this_eid.clone(), send_action.msg))
        .unwrap();
      if let Some(confirm_target) = send_action.maybe_confirm_target {
        // If a `confirm_target` is specified, then send a confirmation of the send.
        let _ = confirm_target.send(());
      }
    })
    .unwrap();
}

/// The object that is passed to `send_msg` with instruction on what to send and
/// whether or not that sending needs confirmation.
pub struct SendAction {
  msg: msg::NetworkMessage,
  /// Once the message is sent, the ToNetwork Thread sends
  /// a `()` to this `confirm_target`.
  maybe_confirm_target: Option<Sender<()>>,
}

impl SendAction {
  pub fn new(msg: msg::NetworkMessage, maybe_confirm_target: Option<Sender<()>>) -> SendAction {
    SendAction { msg, maybe_confirm_target }
  }
}

/// Consider the case where `eid.is_internal` is `true`.
///
/// This functions sends `msg` to the given `eid`. If the connection does not exist, we
/// instantiate a connection accordingly. If the connection gets dropped, then any messages
/// we try sending it also get dropped (i.e. we do not try reconnecting in order to preserve
/// FIFO network behavior). This is because `out_conn_map` still holds that `EndpointId`
/// as a key and sinks any messages sent to it.
///
/// For the case where `eid.is_internal` is false, we clean up `out_conn_map` if we detect
/// the connection to drop. This allows a user from the same IP address to reconnect to this
/// node (most of the time, if this node actually detects that the previous connection had closed).
pub fn send_msg(
  locked_out_conn_map: &Arc<Mutex<BTreeMap<EndpointId, Sender<SendAction>>>>,
  eid: &EndpointId,
  action: SendAction,
  this_eid_internal: &InternalMode,
) {
  let mut out_conn_map = locked_out_conn_map.lock().unwrap();

  // If there is not an out-going connection to `eid`, then make one.
  if !out_conn_map.contains_key(eid) {
    // We create the ToNetwork thread.
    let (sender, receiver) = mpsc::channel();
    out_conn_map.insert(eid.clone(), sender);
    let locked_out_conn_map = locked_out_conn_map.clone();
    let eid = eid.clone();
    let this_eid_internal = this_eid_internal.clone();
    thread::Builder::new()
      .name(format!("ToNetwork {}", eid.ip))
      .spawn(move || {
        let stream = TcpStream::connect(format!("{}:{}", eid.ip, SERVER_PORT)).unwrap();
        // Configure the stream to block indefinitely for reads and writes.
        stream.set_read_timeout(None).unwrap();
        stream.set_write_timeout(None).unwrap();

        // Send Initialization message
        let init_msg = msg::InitMessage { is_internal: this_eid_internal.clone() };
        let data_out = rmp_serde::to_vec(&init_msg).unwrap();
        let _ = send_bytes(&data_out, &stream);

        // Send data until the connection closes.
        let error = loop {
          let send_action = receiver.recv().unwrap();
          let data_out = rmp_serde::to_vec(&send_action.msg).unwrap();
          if let Err(error) = send_bytes(&data_out, &stream) {
            // This means that the connection effectively closed.
            break error;
          } else if let Some(confirm_target) = send_action.maybe_confirm_target {
            // If a `confirm_target` is specified, then send a confirmation of the send.
            let _ = confirm_target.send(());
          }
        };

        // If the other `EndpointId` is not an Internal, then we clean up `out_conn_map`
        // so that next time we try sending a message to that `EndpointId`, we will establish
        // a new connection. Importantly, observe that this is the only code that will ever
        // remove an element from `out_conn_map`.
        if let InternalMode::External { .. } = &eid.mode {
          let mut out_conn_map = locked_out_conn_map.lock().unwrap();
          out_conn_map.remove(&eid).unwrap();
        }

        info!(
          "Thread 'ToNetwork {:?}' shutting down. Connection closed with error: {}",
          eid, error
        );
      })
      .unwrap();
  }

  // Send the `msg` to the ToNetwork thread. If the `receiver` was deallocated because the
  // connection closed, recall that `sender` will still exist, but `send` would fail. This
  // behavior continues to adhere to FIFO network behavior, where network messages are never
  // received on the other side.
  let sender = out_conn_map.get(eid).unwrap();
  let _ = sender.send(action);
}
