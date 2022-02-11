use crate::common::{cur_timestamp, Timestamp};
use crate::common::{mk_t, BasicIOCtx};
use crate::drop_table_tm_es::{
  DropTableClosed, DropTableCommit, DropTablePayloadTypes, DropTablePrepare, DropTablePrepared,
  DropTableRMAborted, DropTableRMCommitted, DropTableRMPrepared,
};
use crate::stmpaxos2pc_rm::{STMPaxos2PCRMAction, STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use crate::stmpaxos2pc_tm::RMCommittedPLm;
use crate::tablet::TabletContext;
use serde::{Deserialize, Serialize};
use std::cmp::max;

// -----------------------------------------------------------------------------------------------
//  DropTableES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableRMInner {
  pub prepared_timestamp: Timestamp,
}

pub type DropTableRMES = STMPaxos2PCRMOuter<DropTablePayloadTypes, DropTableRMInner>;
pub type DropTableRMAction = STMPaxos2PCRMAction<DropTablePayloadTypes>;

impl STMPaxos2PCRMInner<DropTablePayloadTypes> for DropTableRMInner {
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
