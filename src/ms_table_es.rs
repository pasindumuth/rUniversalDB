use crate::common::{CoreIOCtx, QueryESResult, QueryPlan, Timestamp};
use crate::gr_query_es::GRQueryES;
use crate::model::common::{
  proc, CQueryPath, CTQueryPath, ColName, Context, PaxosGroupId, PaxosGroupIdTrait, QueryId,
  TQueryPath, TableView, TransTableName,
};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::table_read_es::{check_gossip, does_query_plan_align, request_lock_columns};
use crate::tablet::{ColumnsLocking, Executing, MSQueryES, Pending, TabletContext};
use std::collections::BTreeSet;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  SqlQueryInner
// -----------------------------------------------------------------------------------------------

pub trait SqlQueryInner {
  fn request_region_locks<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es: &GeneralQueryES,
  ) -> Result<QueryId, msg::QueryError>;

  fn compute_subqueries<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es: &GeneralQueryES,
    ms_query_es: &mut MSQueryES,
  ) -> Vec<GRQueryES>;

  fn finish<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es: &GeneralQueryES,
    subquery_results: (Vec<(Vec<proc::ColumnRef>, Vec<TransTableName>)>, Vec<Vec<TableView>>),
    ms_query_es: &mut MSQueryES,
  ) -> MSTableAction;
}

// -----------------------------------------------------------------------------------------------
//  MSTableES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum MSTableExecutionS {
  Start,
  ColumnsLocking(ColumnsLocking),
  GossipDataWaiting,
  Pending(Pending),
  Executing(Executing),
  Done,
}

#[derive(Debug)]
pub struct GeneralQueryES {
  pub root_query_path: CQueryPath,
  pub timestamp: Timestamp,
  pub tier: u32,
  pub context: Rc<Context>,
  pub query_id: QueryId,

  /// QueryPlan
  pub query_plan: QueryPlan,

  /// The `QueryId` of the `MSQueryES` that this ES belongs to.
  /// We make sure that it exists as long as this ES exists.
  pub ms_query_id: QueryId,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
}

#[derive(Debug)]
pub struct MSTableES<SqlQueryInnerT: SqlQueryInner> {
  pub sender_path: CTQueryPath,
  pub child_queries: Vec<QueryId>,
  pub general: GeneralQueryES,
  pub inner: SqlQueryInnerT,
  pub state: MSTableExecutionS,
}

pub enum MSTableAction {
  /// This tells the parent Server to wait.
  Wait,
  /// This tells the parent Server to perform subqueries.
  SendSubqueries(Vec<GRQueryES>),
  /// Indicates the ES succeeded with the given result.
  Success(QueryESResult),
  /// Indicates the ES failed with a QueryError.
  QueryError(msg::QueryError),
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl<SqlQueryInnerT: SqlQueryInner> MSTableES<SqlQueryInnerT> {
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableAction {
    // First, we lock the columns that the QueryPlan requires certain properties of.
    assert!(matches!(self.state, MSTableExecutionS::Start));
    let locked_cols_qid = request_lock_columns(
      ctx,
      io_ctx,
      &self.general.query_id,
      &self.general.timestamp,
      &self.general.query_plan,
    );
    self.state = MSTableExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid });

    MSTableAction::Wait
  }

  /// Handle Columns being locked
  pub fn local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableAction {
    match &self.state {
      MSTableExecutionS::ColumnsLocking(locking) => {
        if locking.locked_cols_qid == locked_cols_qid {
          // Now, we check whether the TableSchema aligns with the QueryPlan.
          if !does_query_plan_align(ctx, &self.general.timestamp, &self.general.query_plan) {
            self.state = MSTableExecutionS::Done;
            MSTableAction::QueryError(msg::QueryError::InvalidQueryPlan)
          } else {
            // If it aligns, we verify is GossipData is recent enough.
            self.check_gossip_data(ctx, io_ctx)
          }
        } else {
          debug_assert!(false);
          MSTableAction::Wait
        }
      }
      _ => MSTableAction::Wait,
    }
  }

  /// Handle this just as `local_locked_cols`
  pub fn global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableAction {
    self.local_locked_cols(ctx, io_ctx, locked_cols_qid)
  }

  /// Here, the column locking request results in us realizing the table has been dropped.
  pub fn table_dropped(&mut self, _: &mut TabletContext) -> MSTableAction {
    match &self.state {
      MSTableExecutionS::ColumnsLocking(_) => {
        self.state = MSTableExecutionS::Done;
        MSTableAction::QueryError(msg::QueryError::InvalidQueryPlan)
      }
      _ => {
        debug_assert!(false);
        MSTableAction::Wait
      }
    }
  }

  // Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableAction {
    // If the GossipData is valid, then act accordingly.
    if check_gossip(&ctx.gossip.get(), &self.general.query_plan) {
      // We start locking the regions.
      match self.inner.request_region_locks(ctx, io_ctx, &self.general) {
        Ok(protect_qid) => {
          self.state = MSTableExecutionS::Pending(Pending { query_id: protect_qid });
          MSTableAction::Wait
        }
        Err(query_error) => {
          self.state = MSTableExecutionS::Done;
          MSTableAction::QueryError(query_error)
        }
      }
    } else {
      // If not, we go to GossipDataWaiting
      self.state = MSTableExecutionS::GossipDataWaiting;

      // Request a GossipData from the Master to help stimulate progress.
      let sender_path = ctx.this_sid.clone();
      ctx.ctx().send_to_master(
        io_ctx,
        msg::MasterRemotePayload::MasterGossipRequest(msg::MasterGossipRequest { sender_path }),
      );

      MSTableAction::Wait
    }
  }

  /// Here, we GossipData gets delivered.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableAction {
    if let MSTableExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, io_ctx)
    } else {
      // Do nothing
      MSTableAction::Wait
    }
  }

  /// Handle ReadRegion protection
  pub fn local_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    ms_query_es: &mut MSQueryES,
    protect_qid: QueryId,
  ) -> MSTableAction {
    match &self.state {
      MSTableExecutionS::Pending(pending) if protect_qid == pending.query_id => {
        let gr_query_ess = self.inner.compute_subqueries(ctx, io_ctx, &self.general, ms_query_es);

        // Move the ES to the Executing state.
        self.state = MSTableExecutionS::Executing(Executing::create(&gr_query_ess));
        let exec = cast!(MSTableExecutionS::Executing, &mut self.state).unwrap();

        // See if we are already finished (due to having no subqueries).
        if exec.is_complete() {
          let result = std::mem::take(exec).get_results();
          self.state = MSTableExecutionS::Done;
          self.inner.finish(ctx, io_ctx, &self.general, result, ms_query_es)
        } else {
          // Otherwise, return the subqueries.
          MSTableAction::SendSubqueries(gr_query_ess)
        }
      }
      _ => {
        debug_assert!(false);
        MSTableAction::Wait
      }
    }
  }

  /// This is called if a subquery fails.
  pub fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> MSTableAction {
    self.exit_and_clean_up(ctx, io_ctx);
    MSTableAction::QueryError(query_error)
  }

  /// Handles a Subquery completing
  pub fn handle_subquery_done<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    ms_query_es: &mut MSQueryES,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    (_, table_views): (Vec<Option<ColName>>, Vec<TableView>),
  ) -> MSTableAction {
    // Add the subquery results into the MSTableES.
    self.general.new_rms.extend(subquery_new_rms);
    let exec = cast!(MSTableExecutionS::Executing, &mut self.state).unwrap();
    exec.add_subquery_result(subquery_id, table_views);

    // See if we are finished (due to computing all subqueries).
    if exec.is_complete() {
      let result = std::mem::take(exec).get_results();
      self.state = MSTableExecutionS::Done;
      self.inner.finish(ctx, io_ctx, &self.general, result, ms_query_es)
    } else {
      // Otherwise, we wait.
      MSTableAction::Wait
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {
    self.state = MSTableExecutionS::Done;
  }

  pub fn sender_gid(&self) -> PaxosGroupId {
    self.sender_path.node_path.sid.to_gid()
  }
}
