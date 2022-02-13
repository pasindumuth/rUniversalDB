use crate::model::common::EndpointId;
use crate::model::message as msg;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

// -----------------------------------------------------------------------------------------------
//  Network Output Connections
// -----------------------------------------------------------------------------------------------
// We use simple 4 byte header that holds the length of the real message.

pub fn send_bytes(data: &[u8], mut stream: &TcpStream) {
  // Write out the fixed-size header
  stream.write_u32::<BigEndian>(data.len() as u32).unwrap();
  // Write out the full message
  stream.write(data).unwrap();
}

pub fn recv(mut stream: &TcpStream) -> Vec<u8> {
  // Read in the fixed-size header
  let size = stream.read_u32::<BigEndian>().unwrap();
  // Read in the full message
  let mut buf = vec![0; size as usize];
  let mut i = 0;
  while i < size {
    let num_read = stream.read(&mut buf[i as usize..]).unwrap();
    i += num_read as u32;
  }
  return buf;
}

// -----------------------------------------------------------------------------------------------
//  Network Output Connections
// -----------------------------------------------------------------------------------------------

pub const SERVER_PORT: u32 = 1610;

/// Send `msg` to the given `eid`. If the connection does not exist, we instantiate
/// a connection accordingly.
pub fn send_msg(
  out_conn_map: &Arc<Mutex<BTreeMap<EndpointId, Sender<Vec<u8>>>>>,
  eid: &EndpointId,
  msg: msg::NetworkMessage,
) {
  let mut out_conn_map = out_conn_map.lock().unwrap();

  // If there is not an out-going connection to `eid`, then make one.
  if !out_conn_map.contains_key(eid) {
    // We create the ToNetwork thread.
    let (sender, receiver) = mpsc::channel();
    out_conn_map.insert(eid.clone(), sender);
    let EndpointId(ip) = eid.clone();
    thread::spawn(move || {
      let stream = TcpStream::connect(format!("{}:{}", ip, SERVER_PORT)).unwrap();
      loop {
        let data_out = receiver.recv().unwrap();
        send_bytes(&data_out, &stream);
      }
    });
  }

  // Send the `msg` to the ToNetwork thread.
  let sender = out_conn_map.get(eid).unwrap();
  sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
}
