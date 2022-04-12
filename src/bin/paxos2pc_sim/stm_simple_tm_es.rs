use crate::message as msg;
use crate::slave::{SlaveContext, SlavePLm};
use runiversal::common::BasicIOCtx;
use runiversal::model::common::{EndpointId, RequestId, SlaveGroupId};
use runiversal::stmpaxos2pc_tm::{
  PayloadTypes, RMMessage, RMPLm, STMPaxos2PCTMInner, STMPaxos2PCTMOuter, TMClosedPLm,
  TMCommittedPLm, TMMessage, TMPLm,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

// TM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleTMPrepared {
  pub rms: Vec<SlaveGroupId>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleTMCommitted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleTMAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleTMClosed {}

// RM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleRMPrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleRMCommitted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleRMAborted {}

// TM-to-RM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimplePrepare {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleAbort {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleCommit {}

// RM-to-TM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimplePrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimpleClosed {}

// STMSimplePayloadTypes

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMSimplePayloadTypes {}

impl PayloadTypes for STMSimplePayloadTypes {
  // Master
  type RMPath = SlaveGroupId;
  type TMPath = SlaveGroupId;
  type NetworkMessageT = msg::NetworkMessage;
  type RMContext = SlaveContext;
  type TMContext = SlaveContext;

  // Actions
  type RMCommitActionData = ();

  // TM PLm
  type TMPreparedPLm = STMSimpleTMPrepared;
  type TMCommittedPLm = STMSimpleTMCommitted;
  type TMAbortedPLm = STMSimpleTMAborted;
  type TMClosedPLm = STMSimpleTMClosed;

  // RM PLm
  type RMPreparedPLm = STMSimpleRMPrepared;
  type RMCommittedPLm = STMSimpleRMCommitted;
  type RMAbortedPLm = STMSimpleRMAborted;

  // TM-to-RM Messages
  type Prepare = STMSimplePrepare;
  type Abort = STMSimpleAbort;
  type Commit = STMSimpleCommit;

  // RM-to-TM Messages
  type Prepared = STMSimplePrepared;
  type Aborted = STMSimpleAborted;
  type Closed = STMSimpleClosed;
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

pub type STMSimpleTMES = STMPaxos2PCTMOuter<STMSimplePayloadTypes, STMSimpleTMInner>;

#[derive(Debug)]
pub struct STMSimpleTMInner {
  // RMs to use
  pub rms: Vec<SlaveGroupId>,
}

impl STMPaxos2PCTMInner<STMSimplePayloadTypes> for STMSimpleTMInner {
  fn new_follower<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    _: &mut IO,
    payload: STMSimpleTMPrepared,
  ) -> STMSimpleTMInner {
    STMSimpleTMInner { rms: payload.rms }
  }

  fn mk_prepared_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> STMSimpleTMPrepared {
    STMSimpleTMPrepared { rms: self.rms.clone() }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> BTreeMap<SlaveGroupId, STMSimplePrepare> {
    let mut prepares = BTreeMap::<SlaveGroupId, STMSimplePrepare>::new();
    for rm in &self.rms {
      prepares.insert(rm.clone(), STMSimplePrepare {});
    }
    prepares
  }

  fn mk_committed_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &BTreeMap<SlaveGroupId, STMSimplePrepared>,
  ) -> STMSimpleTMCommitted {
    STMSimpleTMCommitted {}
  }

  fn committed_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &TMCommittedPLm<STMSimplePayloadTypes>,
  ) -> BTreeMap<SlaveGroupId, STMSimpleCommit> {
    let mut commits = BTreeMap::<SlaveGroupId, STMSimpleCommit>::new();
    for rm in &self.rms {
      commits.insert(rm.clone(), STMSimpleCommit {});
    }
    commits
  }

  fn mk_aborted_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> STMSimpleTMAborted {
    STMSimpleTMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> BTreeMap<SlaveGroupId, STMSimpleAbort> {
    let mut aborts = BTreeMap::<SlaveGroupId, STMSimpleAbort>::new();
    for rm in &self.rms {
      aborts.insert(rm.clone(), STMSimpleAbort {});
    }
    aborts
  }

  fn mk_closed_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> STMSimpleTMClosed {
    STMSimpleTMClosed {}
  }

  fn closed_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &TMClosedPLm<STMSimplePayloadTypes>,
  ) {
  }

  fn leader_changed<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) {
  }

  fn reconfig_snapshot(&self) -> Self {
    unimplemented!()
  }
}
