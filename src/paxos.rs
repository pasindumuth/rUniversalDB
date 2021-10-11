use crate::model::common::{EndpointId, LeadershipId};
use crate::model::message as msg;
use serde::{Deserialize, Serialize};

// -----------------------------------------------------------------------------------------------
//  Paxos Driver
// -----------------------------------------------------------------------------------------------
// TODO: implement

#[derive(Debug)]
pub struct PaxosDriver<BundleT> {
  pub bundle: BundleT,
}

impl<BundleT> PaxosDriver<BundleT> {
  pub fn handle_paxos_message(
    &mut self,
    _: msg::PaxosDriverMessage<BundleT>,
  ) -> Vec<msg::PLEntry<BundleT>> {
    Vec::new()
  }

  pub fn insert_bundle(&mut self, _: BundleT) {}
}
