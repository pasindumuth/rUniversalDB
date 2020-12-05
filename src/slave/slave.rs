use crate::common::rand::RandGen;
use crate::model::common::{EndpointId, Schema, TabletKeyRange, TabletPath, TabletShape};
use crate::model::message::{AdminMessage, AdminRequest, SlaveAction, SlaveMessage, TabletMessage};
use crate::storage::relational_tablet::RelationalTablet;

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
    println!("eid: {:?}, msg: {:?}", from_eid, &msg);
    match &msg {
      SlaveMessage::Admin(admin_msg) => match admin_msg {
        AdminMessage::Request(admin_request) => {
          let path = match admin_request {
            AdminRequest::Insert { path, .. } => path,
            AdminRequest::Read { path, .. } => path,
          };
          side_effects.add(SlaveAction::Forward {
            // For now, we just assume that if we get an AdminMessage
            // with some `path`, then this Slave has the Tablet for it
            // and that Tablet contains the whole key space.
            shape: TabletShape {
              path: path.clone(),
              range: TabletKeyRange {
                start: None,
                end: None,
              },
            },
            msg: TabletMessage::Input {
              eid: from_eid.clone(),
              msg: msg,
            },
          });
        }
        _ => panic!("Admin Response not supported yet."),
      },
      SlaveMessage::Client(_) => panic!("Client messages not supported yet."),
    }
  }
}
