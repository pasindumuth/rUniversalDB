use crate::model::common::EndpointId;
use crate::model::message::{SlaveActions, SlaveMessage};
use rand::RngCore;

pub struct SideEffects {
    pub actions: Vec<SlaveActions>,
}

pub struct ServerState {
    pub rand_gen: Box<dyn RngCore>,
}

pub fn handle_incoming_message(
    _side_effects: &SideEffects,
    _state: &ServerState,
    input: (EndpointId, SlaveMessage),
) {
    let (_endpoint_id, _msg) = input;
    println!("hi");
}
