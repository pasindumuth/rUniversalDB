use crate::simple_tm_es::SimplePayloadTypes;
use runiversal::model::common::{QueryId, SlaveGroupId};
use runiversal::model::message as msg;
use runiversal::stmpaxos2pc_tm::{RMMessage, TMMessage};
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
  RMMessage(RMMessage<SimplePayloadTypes>),
  TMMessage(TMMessage<SimplePayloadTypes>),
}

// -------------------------------------------------------------------------------------------------
//  ExternalMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalMessage {
  SimpleRequest(SimpleRequest),
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
