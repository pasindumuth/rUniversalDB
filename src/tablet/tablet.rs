use crate::common::rand::RandGen;
use crate::model::common::{Schema, TabletShape};
use crate::model::message::{SlaveMessage, TabletAction, TabletMessage};
use crate::storage::relational_tablet::RelationalTablet;

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
    pub relational_tablet: RelationalTablet,
}

impl TabletState {
    pub fn new(rand_gen: RandGen, this_shape: TabletShape) -> TabletState {
        let schema = Schema {
            key_cols: Vec::new(),
            val_cols: Vec::new(),
        };
        TabletState {
            rand_gen,
            this_shape,
            relational_tablet: RelationalTablet::new(schema),
        }
    }

    pub fn handle_incoming_message(
        &mut self,
        _side_effects: &mut TabletSideEffects,
        msg: TabletMessage,
    ) {
        println!("msg: {:?}", msg);
    }
}
