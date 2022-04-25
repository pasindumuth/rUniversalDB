use crate::common::TNodePath;
use crate::common::{cur_timestamp, Timestamp};
use crate::common::{mk_t, BasicIOCtx};
use crate::drop_table_tm_es::{
  DropTableClosed, DropTableCommit, DropTablePrepare, DropTablePrepared, DropTableTMPayloadTypes,
};
use crate::message as msg;
use crate::server::ServerContextBase;
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
pub struct DropTableRMPayloadTypes {}

impl RMPayloadTypes for DropTableRMPayloadTypes {
  type TM = DropTableTMPayloadTypes;
  type RMContext = TabletContext;

  // Actions
  type RMCommitActionData = Timestamp;

  // RM PLm
  type RMPreparedPLm = DropTableRMPrepared;
  type RMCommittedPLm = DropTableRMCommitted;
  type RMAbortedPLm = DropTableRMAborted;
}

// RM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableRMPrepared {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableRMCommitted {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableRMAborted {}

// -----------------------------------------------------------------------------------------------
//  RMServerContext DropTable
// -----------------------------------------------------------------------------------------------

impl RMServerContext<DropTableRMPayloadTypes> for TabletContext {
  fn push_plm(&mut self, plm: RMPLm<DropTableRMPayloadTypes>) {
    self.tablet_bundle.push(TabletPLm::DropTable(plm));
  }

  fn send_to_tm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    _: &(),
    msg: TMMessage<DropTableTMPayloadTypes>,
  ) {
    self.send_to_master(io_ctx, msg::MasterRemotePayload::DropTable(msg));
  }

  fn mk_node_path(&self) -> TNodePath {
    TabletContext::mk_node_path(self)
  }

  fn is_leader(&self) -> bool {
    TabletContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  DropTableES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableRMInner {
  pub prepared_timestamp: Timestamp,
}

pub type DropTableRMES = STMPaxos2PCRMOuter<DropTableRMPayloadTypes, DropTableRMInner>;
pub type DropTableRMAction = STMPaxos2PCRMAction<DropTableRMPayloadTypes>;

impl STMPaxos2PCRMInner<DropTableRMPayloadTypes> for DropTableRMInner {
  fn new<IO: BasicIOCtx>(
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    _: DropTablePrepare,
  ) -> DropTableRMInner {
    // Construct the `preparing_timestamp`
    let mut timestamp = cur_timestamp(io_ctx, ctx.tablet_config.timestamp_suffix_divisor);
    timestamp = max(timestamp, ctx.table_schema.val_cols.get_latest_lat());
    timestamp = max(timestamp, ctx.presence_timestamp.clone());
    for (_, req) in ctx.waiting_locked_cols.iter().chain(ctx.inserting_locked_cols.iter()) {
      timestamp = max(timestamp, req.timestamp.clone());
    }
    timestamp = timestamp.add(mk_t(1));

    DropTableRMInner { prepared_timestamp: timestamp }
  }

  fn new_follower<IO: BasicIOCtx>(
    _: &mut TabletContext,
    _: &mut IO,
    payload: DropTableRMPrepared,
  ) -> DropTableRMInner {
    DropTableRMInner { prepared_timestamp: payload.timestamp }
  }

  fn mk_closed() -> DropTableClosed {
    DropTableClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
  ) -> Option<DropTableRMPrepared> {
    if ctx.pause_ddl() {
      None
    } else {
      Some(DropTableRMPrepared { timestamp: self.prepared_timestamp.clone() })
    }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> DropTablePrepared {
    DropTablePrepared { timestamp: self.prepared_timestamp.clone() }
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    commit: &DropTableCommit,
  ) -> DropTableRMCommitted {
    DropTableRMCommitted { timestamp: commit.timestamp.clone() }
  }

  /// Apply the `alter_op` to this Tablet's `table_schema`.
  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    committed_plm: &RMCommittedPLm<DropTableRMPayloadTypes>,
  ) -> Timestamp {
    committed_plm.payload.timestamp.clone()
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> DropTableRMAborted {
    DropTableRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {}

  fn reconfig_snapshot(&self) -> DropTableRMInner {
    self.clone()
  }
}
