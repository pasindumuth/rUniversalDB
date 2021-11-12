use crate::message as msg;
use crate::slave::{SlaveContext, SlavePLm};
use runiversal::common::BasicIOCtx;
use runiversal::model::common::{EndpointId, RequestId, SlaveGroupId};
use runiversal::paxos2pc_tm::{
  Paxos2PCTMInner, Paxos2PCTMOuter, PayloadTypes, RMMessage, RMPLm, TMMessage,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

// RM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleRMPrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleRMCommitted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleRMAborted {}

// TM-to-RM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimplePrepare {}

// SimplePayloadTypes

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimplePayloadTypes {}

impl PayloadTypes for SimplePayloadTypes {
  // Master
  type RMPLm = SlavePLm;
  type RMPath = SlaveGroupId;
  type TMPath = SlaveGroupId;
  type RMMessage = msg::SlaveRemotePayload;
  type TMMessage = msg::SlaveRemotePayload;
  type NetworkMessageT = msg::NetworkMessage;
  type RMContext = SlaveContext;
  type RMExtraData = ();
  type TMContext = SlaveContext;

  // RM PLm
  type RMPreparedPLm = SimpleRMPrepared;
  type RMCommittedPLm = SimpleRMCommitted;
  type RMAbortedPLm = SimpleRMAborted;

  fn rm_plm(plm: RMPLm<Self>) -> Self::RMPLm {
    SlavePLm::SimpleRM(plm)
  }

  type Prepare = SimplePrepare;

  fn rm_msg(msg: RMMessage<Self>) -> Self::RMMessage {
    msg::SlaveRemotePayload::RMMessage(msg)
  }

  fn tm_msg(msg: TMMessage<Self>) -> Self::TMMessage {
    msg::SlaveRemotePayload::TMMessage(msg)
  }
}

// -----------------------------------------------------------------------------------------------
//  Simple Implementation
// -----------------------------------------------------------------------------------------------

pub type SimpleTMES = Paxos2PCTMOuter<SimplePayloadTypes, SimpleTMInner>;

#[derive(Debug)]
pub struct SimpleTMInner {}

impl Paxos2PCTMInner<SimplePayloadTypes> for SimpleTMInner {
  fn new_rec<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> SimpleTMInner {
    SimpleTMInner {}
  }

  fn committed<IO: BasicIOCtx<msg::NetworkMessage>>(&mut self, _: &mut SlaveContext, _: &mut IO) {}

  fn aborted<IO: BasicIOCtx<msg::NetworkMessage>>(&mut self, _: &mut SlaveContext, _: &mut IO) {}
}
