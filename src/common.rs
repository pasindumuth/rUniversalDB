use crate::model::common::{EndpointId, QueryId, TabletGroupId};
use crate::model::message::{NetworkMessage, TabletMessage};
use rand::{Rng, RngCore};

pub trait NetworkOut {
  fn send(&mut self, eid: &EndpointId, msg: NetworkMessage);
}

pub trait TabletForwardOut {
  fn forward(&mut self, tablet_group_id: &TabletGroupId, msg: TabletMessage);
}

pub trait IOTypes {
  type RngCoreT: RngCore;
  type NetworkOutT: NetworkOut;
  type TabletForwardOutT: TabletForwardOut;
}

/// These are very low-level utilities where I consider
/// it a shortcoming of the language that there isn't something
/// I can already use.

pub fn rvec(i: i32, j: i32) -> Vec<i32> {
  (i..j).collect()
}

pub fn mk_qid<R: Rng>(rng: &mut R) -> QueryId {
  let mut bytes: [u8; 8] = [0; 8];
  rng.fill(&mut bytes);
  QueryId(bytes)
}
