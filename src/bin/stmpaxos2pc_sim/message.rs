use crate::stm_simple_tm_es::STMSimplePayloadTypes;
use runiversal::model::common::{QueryId, SlaveGroupId};
use runiversal::model::message as msg;
use runiversal::stmpaxos2pc_tm as stmpaxos2pc;
use serde::{Deserialize, Serialize};

// -------------------------------------------------------------------------------------------------
//  NetworkMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum NetworkMessage {
  Slave(SlaveMessage),
}

// -------------------------------------------------------------------------------------------------
//  SlaveMessage
// -------------------------------------------------------------------------------------------------

pub type RemoteMessage<PayloadT> = msg::RemoteMessage<PayloadT>;
pub type RemoteLeaderChangedGossip = msg::RemoteLeaderChangedGossip;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveMessage {
  ExternalMessage(ExternalMessage),
  RemoteMessage(msg::RemoteMessage<SlaveRemotePayload>),
  RemoteLeaderChangedGossip(msg::RemoteLeaderChangedGossip),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveRemotePayload {
  // Simple STMPaxos2PC
  STMRMMessage(stmpaxos2pc::RMMessage<STMSimplePayloadTypes>),
  STMTMMessage(stmpaxos2pc::TMMessage<STMSimplePayloadTypes>),
}

// -------------------------------------------------------------------------------------------------
//  ExternalMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalMessage {
  STMSimpleRequest(STMSimpleRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleRequest {
  pub query_id: QueryId,
  pub rms: Vec<SlaveGroupId>,
}

// -------------------------------------------------------------------------------------------------
//  Paxos
// -------------------------------------------------------------------------------------------------

pub type LeaderChanged = msg::LeaderChanged;
pub type PLEntry<BundleT> = msg::PLEntry<BundleT>;
