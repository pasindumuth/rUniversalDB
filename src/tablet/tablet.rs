use crate::common::rand::RandGen;
use crate::model::common::{Row, Schema, TabletShape};
use crate::model::message::{
  AdminMessage, AdminPayload, AdminRequest, AdminResponse, SlaveMessage, TabletAction,
  TabletMessage,
};
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
  pub fn new(rand_gen: RandGen, this_shape: TabletShape, schema: Schema) -> TabletState {
    TabletState {
      rand_gen,
      this_shape,
      relational_tablet: RelationalTablet::new(schema),
    }
  }

  pub fn handle_incoming_message(
    &mut self,
    side_effects: &mut TabletSideEffects,
    msg: TabletMessage,
  ) {
    match msg {
      TabletMessage::Input {
        eid,
        msg: slave_msg,
      } => match slave_msg {
        SlaveMessage::Admin(AdminMessage { meta, payload }) => match payload {
          AdminPayload::Request(admin_request) => {
            match admin_request {
              AdminRequest::Insert {
                key,
                value,
                timestamp,
                ..
              } => {
                let row = Row { key, val: value };
                self.relational_tablet.insert_row(&row, timestamp).unwrap();
              }
              AdminRequest::Read { key, timestamp, .. } => {
                let result = self.relational_tablet.read_row(&key, timestamp);
                side_effects.add(TabletAction::Send {
                  eid,
                  msg: SlaveMessage::Admin(AdminMessage {
                    meta,
                    payload: AdminPayload::Response(AdminResponse::Read { result }),
                  }),
                });
              }
            };
          }
          AdminPayload::Response(_) => panic!("Admin should never send a response here."),
        },
        SlaveMessage::Client(_) => panic!("Can't handle client messages yet."),
      },
    }
  }
}
