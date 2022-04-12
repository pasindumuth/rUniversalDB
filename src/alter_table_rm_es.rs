use crate::alter_table_tm_es::{
  AlterTableClosed, AlterTableCommit, AlterTablePayloadTypes, AlterTablePrepare,
  AlterTablePrepared, AlterTableRMAborted, AlterTableRMCommitted, AlterTableRMPrepared,
};
use crate::common::{cur_timestamp, mk_t, BasicIOCtx, Timestamp};
use crate::model::common::{proc, TNodePath};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::stmpaxos2pc_rm::{
  RMCommittedPLm, RMPLm, RMServerContext, STMPaxos2PCRMAction, STMPaxos2PCRMInner,
  STMPaxos2PCRMOuter,
};
use crate::stmpaxos2pc_tm::TMMessage;
use crate::tablet::{TabletContext, TabletPLm};
use serde::{Deserialize, Serialize};
use std::cmp::max;

// -----------------------------------------------------------------------------------------------
//  RMServerContext AlterTable
// -----------------------------------------------------------------------------------------------

impl RMServerContext<AlterTablePayloadTypes> for TabletContext {
  fn push_plm(&mut self, plm: RMPLm<AlterTablePayloadTypes>) {
    self.tablet_bundle.push(TabletPLm::AlterTable(plm));
  }

  fn send_to_tm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    _: &(),
    msg: TMMessage<AlterTablePayloadTypes>,
  ) {
    self.send_to_master(io_ctx, msg::MasterRemotePayload::AlterTable(msg));
  }

  fn mk_node_path(&self) -> TNodePath {
    TabletContext::mk_node_path(self)
  }

  fn is_leader(&self) -> bool {
    TabletContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  AlterTableES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRMInner {
  pub alter_op: proc::AlterOp,
  pub prepared_timestamp: Timestamp,
}

pub type AlterTableRMES = STMPaxos2PCRMOuter<AlterTablePayloadTypes, AlterTableRMInner>;
pub type AlterTableRMAction = STMPaxos2PCRMAction<AlterTablePayloadTypes>;

impl STMPaxos2PCRMInner<AlterTablePayloadTypes> for AlterTableRMInner {
  fn new<IO: BasicIOCtx>(
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    payload: AlterTablePrepare,
  ) -> AlterTableRMInner {
    // Construct the `preparing_timestamp`
    let mut timestamp = cur_timestamp(io_ctx, ctx.tablet_config.timestamp_suffix_divisor);
    let col_name = &payload.alter_op.col_name;
    timestamp = max(timestamp, ctx.table_schema.val_cols.get_lat(col_name));
    for (_, req) in ctx.waiting_locked_cols.iter().chain(ctx.inserting_locked_cols.iter()) {
      if req.cols.contains(col_name) {
        timestamp = max(timestamp, req.timestamp.clone());
      }
    }
    timestamp = timestamp.add(mk_t(1));

    AlterTableRMInner { alter_op: payload.alter_op, prepared_timestamp: timestamp }
  }

  fn new_follower<IO: BasicIOCtx>(
    _: &mut TabletContext,
    _: &mut IO,
    payload: AlterTableRMPrepared,
  ) -> AlterTableRMInner {
    AlterTableRMInner { alter_op: payload.alter_op, prepared_timestamp: payload.timestamp }
  }

  fn mk_closed() -> AlterTableClosed {
    AlterTableClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> AlterTableRMPrepared {
    AlterTableRMPrepared {
      alter_op: self.alter_op.clone(),
      timestamp: self.prepared_timestamp.clone(),
    }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> AlterTablePrepared {
    AlterTablePrepared { timestamp: self.prepared_timestamp.clone() }
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    commit: &AlterTableCommit,
  ) -> AlterTableRMCommitted {
    AlterTableRMCommitted { timestamp: commit.timestamp.clone() }
  }

  /// Apply the `alter_op` to this Tablet's `table_schema`.
  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    committed_plm: &RMCommittedPLm<AlterTablePayloadTypes>,
  ) {
    ctx.table_schema.val_cols.write(
      &self.alter_op.col_name,
      self.alter_op.maybe_col_type.clone(),
      committed_plm.payload.timestamp.clone(),
    );
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> AlterTableRMAborted {
    AlterTableRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {}

  fn reconfig_snapshot(&self) -> AlterTableRMInner {
    self.clone()
  }
}
