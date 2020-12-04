use crate::common::rand::RandGen;
use crate::model::common::{EndpointId, TabletShape};
use crate::model::message::SlaveMessage::Client;
use crate::model::message::{SlaveMessage, TabletMessage};
use crate::slave::slave::{handle_incoming_message, SideEffects, SlaveState};
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

pub fn start_slave_thread(
    cur_ip: EndpointId,
    rand_gen: RandGen,
    receiver: Receiver<(EndpointId, Vec<u8>)>,
    net_conn_map: Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
    _tablet_map: HashMap<TabletShape, Sender<TabletMessage>>,
) {
    let state = SlaveState { rand_gen };

    // Start Server Thread
    println!("Starting Server {:?}", cur_ip);
    loop {
        // Receive the data.
        let (endpoint_id, data) = receiver.recv().unwrap();
        let msg: SlaveMessage = rmp_serde::from_read_ref(&data).unwrap();
        println!("Recieved message: {:?}", msg);

        // Create the response.
        let res = Client {
            msg: String::from("hi"),
        };

        // Send the response.
        let net_conn_map = net_conn_map.lock().unwrap();
        let sender = net_conn_map.get(&endpoint_id).unwrap();
        sender.send(rmp_serde::to_vec(&res).unwrap()).unwrap();

        let side_effects = SideEffects {
            actions: Vec::new(),
        };

        let msg = Client {
            msg: String::from("hi"),
        };

        handle_incoming_message(&side_effects, &state, (endpoint_id.clone(), msg));
    }
}
