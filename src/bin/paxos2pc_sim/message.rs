use crate::simple_tm_es::SimplePayloadTypes;
use crate::stm_simple_tm_es::STMSimpleTMPayloadTypes;
use runiversal::common::{QueryId, SlaveGroupId};
use runiversal::message as msg;
use runiversal::paxos2pc_tm as paxos2pc;
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
  STMRMMessage(stmpaxos2pc::RMMessage<STMSimpleTMPayloadTypes>),
  STMTMMessage(stmpaxos2pc::TMMessage<STMSimpleTMPayloadTypes>),

  // Simple Paxos2PC
  RMMessage(paxos2pc::RMMessage<SimplePayloadTypes>),
  TMMessage(paxos2pc::TMMessage<SimplePayloadTypes>),
}

// -------------------------------------------------------------------------------------------------
//  ExternalMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalMessage {
  STMSimpleRequest(STMSimpleRequest),
  SimpleRequest(SimpleRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleRequest {
  pub query_id: QueryId,
  pub rms: Vec<SlaveGroupId>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleRequest {
  pub query_id: QueryId,
  pub rms: Vec<SlaveGroupId>,
}

// -------------------------------------------------------------------------------------------------
//  Paxos
// -------------------------------------------------------------------------------------------------

pub type LeaderChanged = msg::LeaderChanged;
pub type PLEntry<BundleT> = msg::PLEntry<BundleT>;
