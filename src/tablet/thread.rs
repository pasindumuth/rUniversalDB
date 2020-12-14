use crate::common::rand::RandGen;
use crate::model::common::{EndpointId, Schema, TabletShape};
use crate::model::message::{TabletAction, TabletMessage};
use crate::tablet::tablet::{TabletSideEffects, TabletState};
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

pub fn start_tablet_thread(
  this_shape: TabletShape,
  this_slave_eid: EndpointId,
  static_schema: Schema,
  rand_gen: RandGen,
  receiver: Receiver<TabletMessage>,
  net_conn_map: Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
) {
  println!("Starting Tablet Thread {:?}", this_shape);
  let mut state = TabletState::new(rand_gen, this_shape, this_slave_eid, static_schema);
  loop {
    // Receive the Tablet message.
    let msg = receiver.recv().unwrap();
    println!("Recieved message: {:?}", msg);

    // Handle the incoming message.
    let mut side_effects = TabletSideEffects::new();
    state.handle_incoming_message(&mut side_effects, msg);

    // Interpret the actions and perform the necessary
    // side effectful operations.
    for action in side_effects.actions {
      match action {
        TabletAction::Send { eid, msg } => {
          let net_conn_map = net_conn_map.lock().unwrap();
          let sender = net_conn_map.get(&eid).unwrap();
          sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
        }
      }
    }
  }
}
