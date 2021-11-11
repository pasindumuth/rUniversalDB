use crate::common::BasicIOCtx;
use crate::finish_query_tm_es::{
  FinishQueryPayloadTypes, FinishQueryRMAborted, FinishQueryRMCommitted, FinishQueryRMPrepared,
};
use crate::model::common::{proc, Timestamp};
use crate::paxos2pc_rm::{Paxos2PCRMInner, Paxos2PCRMOuter};
use crate::paxos2pc_tm::PayloadTypes;
use crate::storage::{commit_to_storage, GenericTable};
use crate::tablet::{ReadWriteRegion, TabletContext};

#[derive(Debug)]
pub struct FinishQueryRMInner {
  pub region_lock: ReadWriteRegion,
  pub timestamp: Timestamp,
  pub update_view: GenericTable,
}

pub type FinishQueryRMES = Paxos2PCRMOuter<FinishQueryPayloadTypes, FinishQueryRMInner>;

impl Paxos2PCRMInner<FinishQueryPayloadTypes> for FinishQueryRMInner {
  fn new<IO: BasicIOCtx>(ctx: &mut TabletContext, io_ctx: &mut IO) -> Self {
    unimplemented!()
  }

  fn new_follower<IO: BasicIOCtx>(
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    payload: FinishQueryRMPrepared,
  ) -> Self {
    unimplemented!()
  }

  fn early_aborted<IO: BasicIOCtx>(&mut self, ctx: &mut TabletContext, _: &mut IO) {
    ctx.inserting_prepared_writes.remove(&self.timestamp);
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> FinishQueryRMPrepared {
    FinishQueryRMPrepared {
      region_lock: self.region_lock.clone(),
      timestamp: self.timestamp.clone(),
      update_view: self.update_view.clone(),
    }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(&mut self, ctx: &mut TabletContext, _: &mut IO) {
    let region_lock = ctx.inserting_prepared_writes.remove(&self.timestamp).unwrap();
    ctx.prepared_writes.insert(self.timestamp.clone(), region_lock);
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> FinishQueryRMCommitted {
    FinishQueryRMCommitted {}
  }

  fn committed_plm_inserted<IO: BasicIOCtx>(&mut self, ctx: &mut TabletContext, _: &mut IO) {
    commit_to_storage(&mut ctx.storage, &self.timestamp, self.update_view.clone());
    let region_lock = ctx.prepared_writes.remove(&self.timestamp).unwrap();
    ctx.committed_writes.insert(self.timestamp.clone(), region_lock);
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
  ) -> FinishQueryRMAborted {
    FinishQueryRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(&mut self, ctx: &mut TabletContext, _: &mut IO) {
    ctx.prepared_writes.remove(&self.timestamp).unwrap();
  }
}
