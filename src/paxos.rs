use crate::model::common::{EndpointId, LeadershipId};
use crate::model::message as msg;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LeaderChanged {
  pub lid: LeadershipId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PLEntry<BundleT> {
  Bundle(BundleT),
  LeaderChanged(LeaderChanged),
}

// -----------------------------------------------------------------------------------------------
//  Paxos Driver
// -----------------------------------------------------------------------------------------------
// TODO: implement

#[derive(Debug)]
pub struct PaxosDriver<BundleT> {
  pub bundle: BundleT,
}

impl<BundleT> PaxosDriver<BundleT> {
  pub fn handle_paxos_message(&mut self, _: msg::PaxosMessage) -> Option<PLEntry<BundleT>> {
    None
  }

  pub fn insert_bundle(&mut self, _: BundleT) {}
}
