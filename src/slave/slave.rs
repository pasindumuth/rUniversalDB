use crate::common::rand::RandGen;
use crate::model::common::EndpointId;
use crate::model::message::{SlaveAction, SlaveMessage};

#[derive(Debug)]
pub struct SlaveSideEffects {
    pub actions: Vec<SlaveAction>,
}

impl SlaveSideEffects {
    pub fn new() -> SlaveSideEffects {
        SlaveSideEffects {
            actions: Vec::new(),
        }
    }

    pub fn add(&mut self, action: SlaveAction) {
        self.actions.push(action);
    }
}

#[derive(Debug)]
pub struct SlaveState {
    pub rand_gen: RandGen,
    pub this_eid: EndpointId,
}

impl SlaveState {
    pub fn new(rand_gen: RandGen, this_eid: EndpointId) -> SlaveState {
        SlaveState { rand_gen, this_eid }
    }

    /// Top-level network message handling function. It muttates
    /// the SlaveState and Populates `side_effects` with any IO
    /// operations that need to be done as a consequence.
    pub fn handle_incoming_message(
        &mut self,
        side_effects: &mut SlaveSideEffects,
        from_eid: &EndpointId,
        msg: SlaveMessage,
    ) {
        println!("eid: {:?}, msg: {:?}", from_eid, msg);
        side_effects.add(SlaveAction::Send {
            eid: from_eid.clone(),
            msg,
        });
    }
}
