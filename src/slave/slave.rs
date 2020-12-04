use crate::common::rand::RandGen;
use crate::model::common::EndpointId;
use crate::model::message::{SlaveActions, SlaveMessage};
use rand::RngCore;

#[derive(Debug)]
pub struct SideEffects {
    pub actions: Vec<SlaveActions>,
}

#[derive(Debug)]
pub struct SlaveState {
    pub rand_gen: RandGen,
}

impl SlaveState {
    pub fn new(rand_gen: RandGen) -> SlaveState {
        SlaveState { rand_gen: rand_gen }
    }
}

pub fn handle_incoming_message(
    _side_effects: &SideEffects,
    _state: &SlaveState,
    input: (EndpointId, SlaveMessage),
) {
    let (_endpoint_id, _msg) = input;
    println!("hi");
}
