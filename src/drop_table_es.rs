use crate::common::BasicIOCtx;
use crate::drop_table_tm_es::{
  DropTableClosed, DropTableCommit, DropTablePayloadTypes, DropTablePrepared, DropTableRMAborted,
  DropTableRMCommitted, DropTableRMPrepared,
};
use crate::model::common::{proc, QueryId, Timestamp};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::stmpaxos2pc_rm::{STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use crate::stmpaxos2pc_tm::RMCommittedPLm;
use crate::tablet::{plm, TabletContext, TabletPLm};

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

pub type DropTableES = STMPaxos2PCRMOuter<DropTablePayloadTypes, DropTableRMInner>;

impl STMPaxos2PCRMInner<DropTablePayloadTypes> for DropTableRMInner {
  fn mk_closed<IO: BasicIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) -> DropTableClosed {
    DropTableClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> DropTableRMPrepared {
    DropTableRMPrepared { timestamp: self.prepared_timestamp }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> DropTablePrepared {
    DropTablePrepared { timestamp: self.prepared_timestamp }
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    commit: &DropTableCommit,
  ) -> DropTableRMCommitted {
    DropTableRMCommitted { timestamp: commit.timestamp }
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
