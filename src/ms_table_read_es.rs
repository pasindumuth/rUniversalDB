use crate::col_usage::{collect_top_level_cols, compute_select_schema, free_external_cols};
use crate::common::{
  lookup, mk_qid, to_table_path, CoreIOCtx, KeyBound, OrigP, QueryESResult, QueryPlan, ReadRegion,
  Timestamp,
};
use crate::expression::{compress_row_region, compute_key_region, is_true};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, CQueryPath, ColName, ColValN, Context, ContextRow, QueryId, TQueryPath, TableView,
  TransTableName,
};
use crate::model::message as msg;
use crate::server::{
  contains_col, evaluate_super_simple_select, mk_eval_error, ContextConstructor, ServerContextBase,
};
use crate::storage::MSStorageView;
use crate::table_read_es::{
  check_gossip, compute_read_region, does_query_plan_align, fully_evaluate_select,
  request_lock_columns,
};
use crate::tablet::{
  compute_col_map, compute_subqueries, ColumnsLocking, Executing, MSQueryES, Pending,
  RequestedReadProtected, StorageLocalTable, TabletContext,
};
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::ops::Deref;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  MSTableReadES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum MSReadExecutionS {
  Start,
  ColumnsLocking(ColumnsLocking),
  GossipDataWaiting,
  Pending(Pending),
  Executing(Executing),
  Done,
}

#[derive(Debug)]
pub struct MSTableReadES {
  pub root_query_path: CQueryPath,
  pub timestamp: Timestamp,
  pub tier: u32,
  pub context: Rc<Context>,

  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,

  /// The `QueryId` of the `MSQueryES` that this ES belongs to.
  /// We make sure that it exists as long as this ES exists.
  pub ms_query_id: QueryId,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub state: MSReadExecutionS,
}

pub enum MSTableReadAction {
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

impl MSTableReadES {
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableReadAction {
    // First, we lock the columns that the QueryPlan requires certain properties of.
    assert!(matches!(self.state, MSReadExecutionS::Start));
    let qid = request_lock_columns(ctx, io_ctx, &self.query_id, &self.timestamp, &self.query_plan);
    self.state = MSReadExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid: qid });

    MSTableReadAction::Wait
  }

  // Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableReadAction {
    // If the GossipData is valid, then act accordingly.
    if check_gossip(&ctx.gossip.get(), &self.query_plan) {
      // We start locking the regions.
      self.start_ms_table_read_es(ctx, io_ctx)
    } else {
      // If not, we go to GossipDataWaiting
      self.state = MSReadExecutionS::GossipDataWaiting;

      // Request a GossipData from the Master to help stimulate progress.
      let sender_path = ctx.this_sid.clone();
      ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::MasterGossipRequest(
        msg::MasterGossipRequest { sender_path },
      ));

      return MSTableReadAction::Wait;
    }
  }

  /// Handle Columns being locked
  pub fn local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableReadAction {
    match &self.state {
      MSReadExecutionS::ColumnsLocking(locking) => {
        if locking.locked_cols_qid == locked_cols_qid {
          // Now, we check whether the TableSchema aligns with the QueryPlan.
          if !does_query_plan_align(ctx, &self.timestamp, &self.query_plan) {
            self.state = MSReadExecutionS::Done;
            MSTableReadAction::QueryError(msg::QueryError::InvalidQueryPlan)
          } else {
            // If it aligns, we verify is GossipData is recent enough.
            self.check_gossip_data(ctx, io_ctx)
          }
        } else {
          debug_assert!(false);
          MSTableReadAction::Wait
        }
      }
      _ => MSTableReadAction::Wait,
    }
  }

  /// Handle this just as `local_locked_cols`
  pub fn global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableReadAction {
    self.local_locked_cols(ctx, io_ctx, locked_cols_qid)
  }

  /// Here, the column locking request results in us realizing the table has been dropped.
  pub fn table_dropped(&mut self, _: &mut TabletContext) -> MSTableReadAction {
    match &self.state {
      MSReadExecutionS::ColumnsLocking(_) => {
        self.state = MSReadExecutionS::Done;
        MSTableReadAction::QueryError(msg::QueryError::InvalidQueryPlan)
      }
      _ => {
        debug_assert!(false);
        MSTableReadAction::Wait
      }
    }
  }

  /// Here, we GossipData gets delivered.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableReadAction {
    if let MSReadExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, io_ctx)
    } else {
      // Do nothing
      MSTableReadAction::Wait
    }
  }

  /// Starts the Execution state
  fn start_ms_table_read_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableReadAction {
    // Compute the ReadRegion
    let read_region = compute_read_region(
      &ctx.table_schema.key_cols,
      &self.query_plan,
      &self.context,
      &self.sql_query.selection,
    );

    // Move the MSTableReadES to the Pending state with the given ReadRegion.
    let protect_qid = mk_qid(io_ctx.rand());
    self.state = MSReadExecutionS::Pending(Pending { query_id: protect_qid.clone() });

    // Add a ReadRegion to the m_waiting_read_protected.
    let verifying = ctx.verifying_writes.get_mut(&self.timestamp).unwrap();
    verifying.m_waiting_read_protected.insert(RequestedReadProtected {
      orig_p: OrigP::new(self.query_id.clone()),
      query_id: protect_qid,
      read_region,
    });
    MSTableReadAction::Wait
  }

  /// Handle ReadRegion protection
  pub fn local_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    ms_query_es: &MSQueryES,
    protect_qid: QueryId,
  ) -> MSTableReadAction {
    match &self.state {
      MSReadExecutionS::Pending(pending) if protect_qid == pending.query_id => {
        let gr_query_ess = compute_subqueries(
          GRQueryConstructorView {
            root_query_path: &self.root_query_path,
            timestamp: &self.timestamp,
            sql_query: &self.sql_query,
            query_plan: &self.query_plan,
            query_id: &self.query_id,
            context: &self.context,
          },
          io_ctx.rand(),
          StorageLocalTable::new(
            &ctx.table_schema,
            &self.timestamp,
            &self.query_plan.col_usage_node.source,
            &self.sql_query.selection,
            MSStorageView::new(
              &ctx.storage,
              &ctx.table_schema,
              &ms_query_es.update_views,
              self.tier.clone(),
            ),
          ),
        );

        // Move the ES to the Executing state.
        self.state = MSReadExecutionS::Executing(Executing::create(&gr_query_ess));
        let exec = cast!(MSReadExecutionS::Executing, &mut self.state).unwrap();

        // See if we are already finished (due to having no subqueries).
        if exec.is_complete() {
          self.finish_ms_table_read_es(ctx, io_ctx, ms_query_es)
        } else {
          // Otherwise, return the subqueries.
          MSTableReadAction::SendSubqueries(gr_query_ess)
        }
      }
      _ => {
        debug_assert!(false);
        MSTableReadAction::Wait
      }
    }
  }

  /// This is called if a subquery fails.
  pub fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> MSTableReadAction {
    self.exit_and_clean_up(ctx, io_ctx);
    MSTableReadAction::QueryError(query_error)
  }

  /// Handles a Subquery completing
  pub fn handle_subquery_done<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    ms_query_es: &MSQueryES,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    (_, table_views): (Vec<Option<ColName>>, Vec<TableView>),
  ) -> MSTableReadAction {
    // Add the subquery results into the MSTableReadES.
    self.new_rms.extend(subquery_new_rms);
    let exec = cast!(MSReadExecutionS::Executing, &mut self.state).unwrap();
    exec.add_subquery_result(subquery_id, table_views);

    // See if we are finished (due to computing all subqueries).
    if exec.is_complete() {
      self.finish_ms_table_read_es(ctx, io_ctx, ms_query_es)
    } else {
      // Otherwise, we wait.
      MSTableReadAction::Wait
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  fn finish_ms_table_read_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    ms_query_es: &MSQueryES,
  ) -> MSTableReadAction {
    let exec = cast!(MSReadExecutionS::Executing, &mut self.state).unwrap();

    // Compute children.
    let (children, subquery_results) = std::mem::take(exec).get_results();

    // Create the ContextConstructor.
    let context_constructor = ContextConstructor::new(
      self.context.context_schema.clone(),
      StorageLocalTable::new(
        &ctx.table_schema,
        &self.timestamp,
        &self.query_plan.col_usage_node.source,
        &self.sql_query.selection,
        MSStorageView::new(
          &ctx.storage,
          &ctx.table_schema,
          &ms_query_es.update_views,
          self.tier.clone(),
        ),
      ),
      children,
    );

    // Evaluate
    let eval_res = fully_evaluate_select(
      context_constructor,
      &self.context.deref(),
      subquery_results,
      &self.sql_query,
    );

    match eval_res {
      Ok((select_schema, res_table_views)) => {
        // Signal Success and return the data.
        self.state = MSReadExecutionS::Done;
        MSTableReadAction::Success(QueryESResult {
          result: (select_schema, res_table_views),
          new_rms: self.new_rms.iter().cloned().collect(),
        })
      }
      Err(eval_error) => {
        self.state = MSReadExecutionS::Done;
        MSTableReadAction::QueryError(mk_eval_error(eval_error))
      }
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {
    self.state = MSReadExecutionS::Done;
  }
}
