use crate::common::{cur_timestamp, Timestamp};
use crate::common::{mk_t, BasicIOCtx};
use crate::model::common::{TNodePath, TabletGroupId};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::shard_split_tm_es::{
  ShardNodePath, ShardSplitClosed, ShardSplitCommit, ShardSplitPrepare, ShardSplitPrepared,
  ShardSplitTMPayloadTypes,
};
use crate::slave::{SlaveContext, SlavePLm};
use crate::stmpaxos2pc_rm::{
  RMCommittedPLm, RMPLm, RMPayloadTypes, RMServerContext, STMPaxos2PCRMAction, STMPaxos2PCRMInner,
  STMPaxos2PCRMOuter,
};
use crate::stmpaxos2pc_tm::TMMessage;
use serde::{Deserialize, Serialize};
use std::cmp::max;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitSlaveRMPayloadTypes {}

impl RMPayloadTypes for ShardSplitSlaveRMPayloadTypes {
  type TM = ShardSplitTMPayloadTypes;
  type RMContext = SlaveContext;

  // Actions
  type RMCommitActionData = ();

  // RM PLm
  type RMPreparedPLm = ShardSplitSlaveRMPrepared;
  type RMCommittedPLm = ShardSplitSlaveRMCommitted;
  type RMAbortedPLm = ShardSplitSlaveRMAborted;
}

// RM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitSlaveRMPrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitSlaveRMCommitted {
  /// The `TabletGroupId` for the new Tablet that will be created.
  pub tid: TabletGroupId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitSlaveRMAborted {}

// -----------------------------------------------------------------------------------------------
//  RMServerContext ShardSplitSlave
// -----------------------------------------------------------------------------------------------

impl RMServerContext<ShardSplitSlaveRMPayloadTypes> for SlaveContext {
  fn push_plm(&mut self, plm: RMPLm<ShardSplitSlaveRMPayloadTypes>) {
    self.slave_bundle.plms.push(SlavePLm::ShardSplit(plm));
  }

  fn send_to_tm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    _: &(),
    msg: TMMessage<ShardSplitTMPayloadTypes>,
  ) {
    self.send_to_master(io_ctx, msg::MasterRemotePayload::ShardSplit(msg));
  }

  fn mk_node_path(&self) -> ShardNodePath {
    ShardNodePath::Slave(self.this_sid.clone())
  }

  fn is_leader(&self) -> bool {
    SlaveContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  ShardSplitSlaveES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitSlaveRMInner {}

pub type ShardSplitSlaveRMES =
  STMPaxos2PCRMOuter<ShardSplitSlaveRMPayloadTypes, ShardSplitSlaveRMInner>;
pub type ShardSplitSlaveRMAction = STMPaxos2PCRMAction<ShardSplitSlaveRMPayloadTypes>;

impl STMPaxos2PCRMInner<ShardSplitSlaveRMPayloadTypes> for ShardSplitSlaveRMInner {
  fn new<IO: BasicIOCtx>(
    _: &mut SlaveContext,
    _: &mut IO,
    _: ShardSplitPrepare,
  ) -> ShardSplitSlaveRMInner {
    ShardSplitSlaveRMInner {}
  }

  fn new_follower<IO: BasicIOCtx>(
    _: &mut SlaveContext,
    _: &mut IO,
    _: ShardSplitSlaveRMPrepared,
  ) -> ShardSplitSlaveRMInner {
    ShardSplitSlaveRMInner {}
  }

  fn mk_closed() -> ShardSplitClosed {
    ShardSplitClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> Option<ShardSplitSlaveRMPrepared> {
    Some(ShardSplitSlaveRMPrepared {})
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> ShardSplitPrepared {
    ShardSplitPrepared {}
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    commit: &ShardSplitCommit,
  ) -> ShardSplitSlaveRMCommitted {
    ShardSplitSlaveRMCommitted { tid: commit.new_tablet.1.clone() }
  }

  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &RMCommittedPLm<ShardSplitSlaveRMPayloadTypes>,
  ) -> () {
    ()
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> ShardSplitSlaveRMAborted {
    ShardSplitSlaveRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(&mut self, _: &mut SlaveContext, _: &mut IO) {}

  fn reconfig_snapshot(&self) -> ShardSplitSlaveRMInner {
    self.clone()
  }
}
