use crate::common::rand::RandGen;
use crate::model::common::{EndpointId, TabletShape};
use crate::model::message::{SlaveAction, SlaveMessage, TabletMessage};
use crate::slave::slave::{SlaveSideEffects, SlaveState};
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

pub fn start_slave_thread(
  this_eid: EndpointId,
  rand_gen: RandGen,
  receiver: Receiver<(EndpointId, Vec<u8>)>,
  net_conn_map: Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
  tablet_map: HashMap<TabletShape, Sender<TabletMessage>>,
  tablet_config: HashMap<EndpointId, Vec<TabletShape>>,
) {
  let mut state = SlaveState::new(rand_gen, this_eid.clone(), tablet_config);
  println!("Starting Slave Thread {:?}", this_eid);
  loop {
    // Receive the data.
    let (endpoint_id, data) = receiver.recv().unwrap();
    let msg: SlaveMessage = rmp_serde::from_read_ref(&data).unwrap();
    println!("Recieved message: {:?}", msg);

    // Handle the incoming message.
    let mut side_effects = SlaveSideEffects::new();
    state.handle_incoming_message(&mut side_effects, &endpoint_id, msg);

    // Interpret the actions and perform the necessary
    // side effectful operations.
    for action in side_effects.actions {
      match action {
        SlaveAction::Send { eid, msg } => {
          let net_conn_map = net_conn_map.lock().unwrap();
          let sender = net_conn_map.get(&eid).unwrap();
          sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
        }
        SlaveAction::Forward { tablet, msg } => {
          tablet_map.get(&tablet).unwrap().send(msg).unwrap();
        }
      }
    }
  }
}
