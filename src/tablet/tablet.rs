use crate::common::rand::RandGen;
use crate::model::common::{TabletShape};
use crate::model::message::{SlaveMessage, TabletAction, TabletMessage};

#[derive(Debug)]
pub struct TabletSideEffects {
    pub actions: Vec<TabletAction>,
}

impl TabletSideEffects {
    pub fn new() -> TabletSideEffects {
        TabletSideEffects {
            actions: Vec::new(),
        }
    }

    pub fn add(&mut self, action: TabletAction) {
        self.actions.push(action);
    }
}

#[derive(Debug)]
pub struct TabletState {
    pub rand_gen: RandGen,
    pub this_shape: TabletShape,
}

impl TabletState {
    pub fn new(rand_gen: RandGen, this_shape: TabletShape) -> TabletState {
        TabletState {
            rand_gen,
            this_shape,
        }
    }

    pub fn handle_incoming_message(
        &mut self,
        side_effects: &mut TabletSideEffects,
        msg: TabletMessage,
    ) {
        println!("msg: {:?}", msg);
        match msg {
            TabletMessage::Input { eid, msg } => {
                side_effects.add(TabletAction::Send {
                    eid: eid.clone(),
                    msg: SlaveMessage::Client { msg },
                });
            }
        }
    }
}
