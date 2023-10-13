use crate::common::{cur_timestamp, QueryId, Timestamp};
use crate::common::{mk_t, BasicIOCtx};
use crate::common::{
  ShardingGen, SlaveGroupId, TNodePath, TablePath, TabletGroupId, TabletKeyRange,
};
use crate::message as msg;
use crate::server::ServerContextBase;
use crate::shard_split_tm_es::{
  STRange, ShardNodePath, ShardSplitClosed, ShardSplitCommit, ShardSplitPrepare,
  ShardSplitPrepared, ShardSplitTMPayloadTypes,
};
use crate::stmpaxos2pc_rm::{
  RMCommittedPLm, RMPLm, RMPayloadTypes, RMServerContext, STMPaxos2PCRMAction, STMPaxos2PCRMInner,
  STMPaxos2PCRMOuter,
};
use crate::stmpaxos2pc_tm::TMMessage;
use crate::tablet::{TabletContext, TabletPLm};
use serde::{Deserialize, Serialize};
use std::cmp::max;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTabletRMPayloadTypes {}

impl RMPayloadTypes for ShardSplitTabletRMPayloadTypes {
  type TM = ShardSplitTMPayloadTypes;
  type RMContext = TabletContext;

  // Actions. This contains the target Tablet to create.
  type RMCommitActionData = STRange;

  // RM PLm
  type RMPreparedPLm = ShardSplitTabletRMPrepared;
  type RMCommittedPLm = ShardSplitTabletRMCommitted;
  type RMAbortedPLm = ShardSplitTabletRMAborted;
}

// RM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTabletRMPrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTabletRMCommitted {
  pub sharding_gen: ShardingGen,
  pub target_old: STRange,
  pub target_new: STRange,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTabletRMAborted {}

// -----------------------------------------------------------------------------------------------
//  RMServerContext ShardSplitTablet
// -----------------------------------------------------------------------------------------------

impl RMServerContext<ShardSplitTabletRMPayloadTypes> for TabletContext {
  fn push_plm(&mut self, plm: RMPLm<ShardSplitTabletRMPayloadTypes>) {
    self.tablet_bundle.push(TabletPLm::ShardSplit(plm));
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
    ShardNodePath::Tablet(TabletContext::mk_node_path(self))
  }

  fn is_leader(&self) -> bool {
    TabletContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  ShardSplitTabletRMES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTabletRMInner {}

pub type ShardSplitTabletRMES =
  STMPaxos2PCRMOuter<ShardSplitTabletRMPayloadTypes, ShardSplitTabletRMInner>;
pub type ShardSplitTabletRMAction = STMPaxos2PCRMAction<ShardSplitTabletRMPayloadTypes>;

impl STMPaxos2PCRMInner<ShardSplitTabletRMPayloadTypes> for ShardSplitTabletRMInner {
  fn new<IO: BasicIOCtx>(
    _: &mut TabletContext,
    _: &mut IO,
    _: ShardSplitPrepare,
  ) -> ShardSplitTabletRMInner {
    ShardSplitTabletRMInner {}
  }

  fn new_follower<IO: BasicIOCtx>(
    _: &mut TabletContext,
    _: &mut IO,
    _: ShardSplitTabletRMPrepared,
  ) -> ShardSplitTabletRMInner {
    ShardSplitTabletRMInner {}
  }

  fn mk_closed() -> ShardSplitClosed {
    ShardSplitClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
  ) -> Option<ShardSplitTabletRMPrepared> {
    if ctx.pause_ddl() {
      None
    } else {
      Some(ShardSplitTabletRMPrepared {})
    }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> ShardSplitPrepared {
    ShardSplitPrepared {}
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    commit: &ShardSplitCommit,
  ) -> ShardSplitTabletRMCommitted {
    ShardSplitTabletRMCommitted {
      sharding_gen: commit.sharding_gen.clone(),
      target_old: commit.target_old.clone(),
      target_new: commit.target_new.clone(),
    }
  }

  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    plm: &RMCommittedPLm<ShardSplitTabletRMPayloadTypes>,
  ) -> STRange {
    // Update Sharding data.
    ctx.this_sharding_gen = plm.payload.sharding_gen.clone();
    ctx.this_tablet_key_range = plm.payload.target_old.range.clone();
    plm.payload.target_new.clone()
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> ShardSplitTabletRMAborted {
    ShardSplitTabletRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {}

  fn reconfig_snapshot(&self) -> ShardSplitTabletRMInner {
    self.clone()
  }
}
