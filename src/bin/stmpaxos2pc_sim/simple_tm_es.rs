use crate::message as msg;
use crate::slave::{SlaveContext, SlavePLm};
use runiversal::common::BasicIOCtx;
use runiversal::model::common::{EndpointId, QueryId, RequestId, SlaveGroupId, Timestamp};
use runiversal::stmpaxos2pc_tm::{
  Abort, Aborted, Closed, Commit, PayloadTypes, Prepare, Prepared, RMAbortedPLm, RMCommittedPLm,
  RMMessage, RMPLm, RMPreparedPLm, STMPaxos2PCTMInner, STMPaxos2PCTMOuter, TMAbortedPLm,
  TMClosedPLm, TMCommittedPLm, TMMessage, TMPLm, TMPreparedPLm,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

// TM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleTMPrepared {
  pub rms: Vec<SlaveGroupId>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleTMCommitted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleTMAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleTMClosed {}

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleAbort {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleCommit {}

// RM-to-TM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimplePrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleClosed {}

// SimplePayloadTypes

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimplePayloadTypes {}

impl PayloadTypes for SimplePayloadTypes {
  // Master
  type RMPLm = SlavePLm;
  type TMPLm = SlavePLm;
  type RMPath = SlaveGroupId;
  type TMPath = SlaveGroupId;
  type RMMessage = msg::SlaveRemotePayload;
  type TMMessage = msg::SlaveRemotePayload;
  type NetworkMessageT = msg::NetworkMessage;
  type RMContext = SlaveContext;
  type TMContext = SlaveContext;

  // TM PLm
  type TMPreparedPLm = SimpleTMPrepared;
  type TMCommittedPLm = SimpleTMCommitted;
  type TMAbortedPLm = SimpleTMAborted;
  type TMClosedPLm = SimpleTMClosed;

  fn tm_plm(plm: TMPLm<Self>) -> Self::TMPLm {
    SlavePLm::SimpleTM(plm)
  }

  // RM PLm
  type RMPreparedPLm = SimpleRMPrepared;
  type RMCommittedPLm = SimpleRMCommitted;
  type RMAbortedPLm = SimpleRMAborted;

  fn rm_plm(plm: RMPLm<Self>) -> Self::RMPLm {
    SlavePLm::SimpleRM(plm)
  }

  // TM-to-RM Messages
  type Prepare = SimplePrepare;
  type Abort = SimpleAbort;
  type Commit = SimpleCommit;

  fn rm_msg(msg: RMMessage<Self>) -> Self::RMMessage {
    msg::SlaveRemotePayload::RMMessage(msg)
  }

  // RM-to-TM Messages
  type Prepared = SimplePrepared;
  type Aborted = SimpleAborted;
  type Closed = SimpleClosed;

  fn tm_msg(msg: TMMessage<Self>) -> Self::TMMessage {
    msg::SlaveRemotePayload::TMMessage(msg)
  }
}

// -----------------------------------------------------------------------------------------------
//  General STMPaxos2PC TM Types
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct ResponseData {
  pub request_id: RequestId,
  pub sender_eid: EndpointId,
}

// -----------------------------------------------------------------------------------------------
//  Simple Implementation
// -----------------------------------------------------------------------------------------------

pub type SimpleTMES = STMPaxos2PCTMOuter<SimplePayloadTypes, SimpleTMInner>;

#[derive(Debug)]
pub struct SimpleTMInner {
  // RMs to use
  pub rms: Vec<SlaveGroupId>,
}

impl STMPaxos2PCTMInner<SimplePayloadTypes> for SimpleTMInner {
  fn new_follower<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    _: &mut IO,
    payload: SimpleTMPrepared,
  ) -> SimpleTMInner {
    SimpleTMInner { rms: payload.rms }
  }

  fn mk_prepared_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> SimpleTMPrepared {
    SimpleTMPrepared { rms: self.rms.clone() }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> BTreeMap<SlaveGroupId, SimplePrepare> {
    let mut prepares = BTreeMap::<SlaveGroupId, SimplePrepare>::new();
    for rm in &self.rms {
      prepares.insert(rm.clone(), SimplePrepare {});
    }
    prepares
  }

  fn mk_committed_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &BTreeMap<SlaveGroupId, SimplePrepared>,
  ) -> SimpleTMCommitted {
    SimpleTMCommitted {}
  }

  fn committed_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &TMCommittedPLm<SimplePayloadTypes>,
  ) -> BTreeMap<SlaveGroupId, SimpleCommit> {
    let mut commits = BTreeMap::<SlaveGroupId, SimpleCommit>::new();
    for rm in &self.rms {
      commits.insert(rm.clone(), SimpleCommit {});
    }
    commits
  }

  fn mk_aborted_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> SimpleTMAborted {
    SimpleTMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> BTreeMap<SlaveGroupId, SimpleAbort> {
    let mut aborts = BTreeMap::<SlaveGroupId, SimpleAbort>::new();
    for rm in &self.rms {
      aborts.insert(rm.clone(), SimpleAbort {});
    }
    aborts
  }

  fn mk_closed_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> SimpleTMClosed {
    SimpleTMClosed {}
  }

  fn closed_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &TMClosedPLm<SimplePayloadTypes>,
  ) {
  }

  fn node_died<IO: BasicIOCtx<msg::NetworkMessage>>(&mut self, _: &mut SlaveContext, _: &mut IO) {}
}
