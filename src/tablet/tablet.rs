use crate::common::rand::RandGen;
use crate::model::common::EndpointId;
use crate::model::message::{TabletActions, TabletMessage};
use rand::RngCore;

#[derive(Debug)]
pub struct SideEffects {
    pub actions: Vec<TabletActions>,
}

#[derive(Debug)]
pub struct TabletState {
    pub rand_gen: RandGen,
}

pub fn handle_incoming_message(
    _side_effects: &SideEffects,
    _state: &TabletState,
    input: (EndpointId, TabletMessage),
) {
    let (_endpoint_id, _msg) = input;
    println!("hi");
}
