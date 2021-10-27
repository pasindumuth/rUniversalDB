use crate::alter_table_tm_es::{
  AlterTableClosed, AlterTableCommit, AlterTablePayloadTypes, AlterTablePrepare,
  AlterTablePrepared, AlterTableRMAborted, AlterTableRMCommitted, AlterTableRMPrepared,
};
use crate::common::BasicIOCtx;
use crate::model::common::{proc, Timestamp};
use crate::stmpaxos2pc_rm::{STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use crate::stmpaxos2pc_tm::RMCommittedPLm;
use crate::tablet::{plm, TabletContext, TabletPLm};
use std::cmp::max;

// -----------------------------------------------------------------------------------------------
//  AlterTableES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct AlterTableRMInner {
  pub alter_op: proc::AlterOp,
  pub prepared_timestamp: Timestamp,
}

pub type AlterTableRMES = STMPaxos2PCRMOuter<AlterTablePayloadTypes, AlterTableRMInner>;

impl STMPaxos2PCRMInner<AlterTablePayloadTypes> for AlterTableRMInner {
  fn new<IO: BasicIOCtx>(
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    payload: AlterTablePrepare,
  ) -> AlterTableRMInner {
    // Construct the `preparing_timestamp`
    let mut timestamp = io_ctx.now();
    let col_name = &payload.alter_op.col_name;
    timestamp = max(timestamp, ctx.table_schema.val_cols.get_lat(col_name));
    for (_, req) in ctx.waiting_locked_cols.iter().chain(ctx.inserting_locked_cols.iter()) {
      if req.cols.contains(col_name) {
        timestamp = max(timestamp, req.timestamp);
      }
    }
    timestamp += 1;

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
    AlterTableRMPrepared { alter_op: self.alter_op.clone(), timestamp: self.prepared_timestamp }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> AlterTablePrepared {
    AlterTablePrepared { timestamp: self.prepared_timestamp }
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    commit: &AlterTableCommit,
  ) -> AlterTableRMCommitted {
    AlterTableRMCommitted { timestamp: commit.timestamp }
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
      committed_plm.payload.timestamp,
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
}
