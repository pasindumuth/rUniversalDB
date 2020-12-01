use crate::common::rand::RandGen;
use crate::model::common::EndpointId;
use crate::model::message::{SlaveActions, SlaveMessage};
use rand::RngCore;

#[derive(Debug)]
pub struct SideEffects {
    pub actions: Vec<SlaveActions>,
}

#[derive(Debug)]
pub struct ServerState {
    pub rand_gen: RandGen,
}

pub fn handle_incoming_message(
    _side_effects: &SideEffects,
    _state: &ServerState,
    input: (EndpointId, SlaveMessage),
) {
    let (_endpoint_id, _msg) = input;
    println!("hi");
}
