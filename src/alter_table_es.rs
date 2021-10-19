use crate::alter_table_tm_es::{
  AlterTableClosed, AlterTableCommit, AlterTablePayloadTypes, AlterTablePrepared,
  AlterTableRMAborted, AlterTableRMCommitted, AlterTableRMPrepared,
};
use crate::common::CoreIOCtx;
use crate::model::common::{proc, QueryId, Timestamp};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::stmpaxos2pc_rm::{STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use crate::stmpaxos2pc_tm::RMCommittedPLm;
use crate::tablet::{plm, TabletContext, TabletPLm};

// -----------------------------------------------------------------------------------------------
//  Old
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum State {
  Follower,
  WaitingInsertingPrepared,
  InsertingPrepared,
  Prepared,
  InsertingCommitted,
  InsertingPreparedAborted,
  InsertingAborted,
}

// -----------------------------------------------------------------------------------------------
//  AlterTableES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct AlterTableRMInner {
  pub query_id: QueryId,
  pub alter_op: proc::AlterOp,
  pub prepared_timestamp: Timestamp,
}

pub(crate) type AlterTableES = STMPaxos2PCRMOuter<AlterTablePayloadTypes, AlterTableRMInner>;

impl STMPaxos2PCRMInner<AlterTablePayloadTypes> for AlterTableRMInner {
  fn mk_closed<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) -> AlterTableClosed {
    AlterTableClosed {}
  }

  fn mk_prepared_plm<IO: CoreIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> AlterTableRMPrepared {
    AlterTableRMPrepared { alter_op: self.alter_op.clone(), timestamp: self.prepared_timestamp }
  }

  fn prepared_plm_inserted<IO: CoreIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> AlterTablePrepared {
    AlterTablePrepared { timestamp: self.prepared_timestamp }
  }

  fn mk_committed_plm<IO: CoreIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    commit: &AlterTableCommit,
  ) -> AlterTableRMCommitted {
    AlterTableRMCommitted { timestamp: commit.timestamp }
  }
  /// Apply the `alter_op` to this Tablet's `table_schema`.
  fn committed_plm_inserted<IO: CoreIOCtx>(
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

  fn mk_aborted_plm<IO: CoreIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> AlterTableRMAborted {
    AlterTableRMAborted {}
  }

  fn aborted_plm_inserted<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {}
}
