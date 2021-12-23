use crate::common::{mk_t, BasicIOCtx};
use crate::drop_table_tm_es::{
  DropTableClosed, DropTableCommit, DropTablePayloadTypes, DropTablePrepare, DropTablePrepared,
  DropTableRMAborted, DropTableRMCommitted, DropTableRMPrepared,
};
use crate::model::common::Timestamp;
use crate::stmpaxos2pc_rm::{STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use crate::stmpaxos2pc_tm::RMCommittedPLm;
use crate::tablet::TabletContext;
use std::cmp::max;

// -----------------------------------------------------------------------------------------------
//  DropTableES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct DropTableRMInner {
  pub prepared_timestamp: Timestamp,

  /// This is populated if this ES completes with a Success. The Tablet will mark itself as
  /// deleted when this ES is Exits.
  pub committed_timestamp: Option<Timestamp>,
}

pub type DropTableRMES = STMPaxos2PCRMOuter<DropTablePayloadTypes, DropTableRMInner>;

impl STMPaxos2PCRMInner<DropTablePayloadTypes> for DropTableRMInner {
  fn new<IO: BasicIOCtx>(
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    _: DropTablePrepare,
  ) -> DropTableRMInner {
    // Construct the `preparing_timestamp`
    let mut timestamp = io_ctx.now();
    timestamp = max(timestamp, ctx.table_schema.val_cols.get_latest_lat());
    for (_, req) in ctx.waiting_locked_cols.iter().chain(ctx.inserting_locked_cols.iter()) {
      timestamp = max(timestamp, req.timestamp.clone());
    }
    timestamp = timestamp.add(mk_t(1));

    DropTableRMInner { prepared_timestamp: timestamp, committed_timestamp: None }
  }

  fn new_follower<IO: BasicIOCtx>(
    _: &mut TabletContext,
    _: &mut IO,
    payload: DropTableRMPrepared,
  ) -> DropTableRMInner {
    DropTableRMInner { prepared_timestamp: payload.timestamp, committed_timestamp: None }
  }

  fn mk_closed() -> DropTableClosed {
    DropTableClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> DropTableRMPrepared {
    DropTableRMPrepared { timestamp: self.prepared_timestamp.clone() }
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
    committed_plm: &RMCommittedPLm<DropTablePayloadTypes>,
  ) {
    self.committed_timestamp = Some(committed_plm.payload.timestamp.clone());
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> DropTableRMAborted {
    DropTableRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {}
}
