use crate::common::BasicIOCtx;
use crate::finish_query_tm_es::{
  FinishQueryPayloadTypes, FinishQueryPrepare, FinishQueryRMAborted, FinishQueryRMCommitted,
  FinishQueryRMPrepared,
};
use crate::model::common::{proc, QueryId, Timestamp};
use crate::paxos2pc_rm::{Paxos2PCRMInner, Paxos2PCRMOuter};
use crate::paxos2pc_tm::PayloadTypes;
use crate::storage::{commit_to_storage, compress_updates_views, GenericTable};
use crate::tablet::{MSQueryES, ReadWriteRegion, TabletContext};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  FinishQueryRMES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct FinishQueryRMInner {
  pub region_lock: ReadWriteRegion,
  pub timestamp: Timestamp,
  pub update_view: GenericTable,
}

pub type FinishQueryRMES = Paxos2PCRMOuter<FinishQueryPayloadTypes, FinishQueryRMInner>;

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl Paxos2PCRMInner<FinishQueryPayloadTypes> for FinishQueryRMInner {
  fn new<IO: BasicIOCtx>(
    ctx: &mut TabletContext,
    _: &mut IO,
    payload: FinishQueryPrepare,
    extra_data: &mut BTreeMap<QueryId, MSQueryES>,
  ) -> Option<FinishQueryRMInner> {
    if let Some(ms_query_es) = extra_data.remove(&payload.query_id) {
      ctx.ms_root_query_map.remove(&ms_query_es.root_query_path.query_id);
      debug_assert!(ms_query_es.pending_queries.is_empty());

      let timestamp = ms_query_es.timestamp;

      // Move the VerifyingReadWrite to inserting.
      let verifying = ctx.verifying_writes.remove(&timestamp).unwrap();
      debug_assert!(verifying.m_waiting_read_protected.is_empty());
      let region_lock = ReadWriteRegion {
        orig_p: verifying.orig_p,
        m_read_protected: verifying.m_read_protected,
        m_write_protected: verifying.m_write_protected,
      };
      ctx.inserting_prepared_writes.insert(timestamp.clone(), region_lock.clone());

      Some(FinishQueryRMInner {
        region_lock,
        timestamp,
        update_view: compress_updates_views(ms_query_es.update_views),
      })
    } else {
      // The MSQueryES might not be present because of a DeadlockSafetyWriteAbort.
      None
    }
  }

  fn new_follower<IO: BasicIOCtx>(
    _: &mut TabletContext,
    _: &mut IO,
    payload: FinishQueryRMPrepared,
  ) -> FinishQueryRMInner {
    FinishQueryRMInner {
      region_lock: payload.region_lock,
      timestamp: payload.timestamp,
      update_view: payload.update_view,
    }
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
    ctx.inserting_prepared_writes.remove(&self.timestamp);
    ctx.prepared_writes.insert(self.timestamp.clone(), self.region_lock.clone());
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
