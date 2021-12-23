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
use crate::table_read_es::fully_evaluate_select;
use crate::tablet::{
  compute_col_map, compute_subqueries, ColumnsLocking, Executing, MSQueryES, Pending,
  RequestedReadProtected, SingleSubqueryStatus, StorageLocalTable, SubqueryFinished,
  SubqueryPending, TabletContext,
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

    let mut all_cols = BTreeSet::<ColName>::new();
    all_cols.extend(free_external_cols(&self.query_plan.col_usage_node.external_cols));
    all_cols.extend(self.query_plan.col_usage_node.safe_present_cols.clone());

    // If there are extra required cols, we add them in.
    if let Some(extra_cols) =
      self.query_plan.extra_req_cols.get(to_table_path(&self.sql_query.from))
    {
      all_cols.extend(extra_cols.clone());
    }

    let locked_cols_qid = ctx.add_requested_locked_columns(
      io_ctx,
      OrigP::new(self.query_id.clone()),
      self.timestamp.clone(),
      all_cols.into_iter().collect(),
    );
    self.state = MSReadExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid });

    MSTableReadAction::Wait
  }

  /// This checks that `external_cols` are not present, and `safe_present_cols` and
  /// `extra_req_cols` are preset.
  ///
  /// Note: this does *not* required columns to be locked.
  fn does_query_plan_align(&self, ctx: &TabletContext) -> bool {
    // First, check that `external_cols are absent.
    for col in free_external_cols(&self.query_plan.col_usage_node.external_cols) {
      // Since the `key_cols` are static, no query plan should have one of
      // these as an External Column.
      assert!(lookup(&ctx.table_schema.key_cols, &col).is_none());
      if ctx.table_schema.val_cols.static_read(&col, &self.timestamp).is_some() {
        return false;
      }
    }

    // Next, check that `safe_present_cols` are present.
    for col in &self.query_plan.col_usage_node.safe_present_cols {
      if !contains_col(&ctx.table_schema, col, &self.timestamp) {
        return false;
      }
    }

    // Next, check that `extra_req_cols` are present.
    if let Some(extra_cols) =
      self.query_plan.extra_req_cols.get(to_table_path(&self.sql_query.from))
    {
      for col in extra_cols {
        if !contains_col(&ctx.table_schema, col, &self.timestamp) {
          return false;
        }
      }
    }

    return true;
  }

  // Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableReadAction {
    for (table_path, gen) in &self.query_plan.table_location_map {
      if !ctx.gossip.sharding_config.contains_key(&(table_path.clone(), gen.clone())) {
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

    // We start locking the regions.
    self.start_ms_table_read_es(ctx, io_ctx)
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
          if !self.does_query_plan_align(ctx) {
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
    // Compute the Row Region by taking the union across all ContextRows
    let mut row_region = Vec::<KeyBound>::new();
    for context_row in &self.context.context_rows {
      let key_bounds = compute_key_region(
        &self.sql_query.selection,
        compute_col_map(&self.context.context_schema, context_row),
        &self.query_plan.col_usage_node.source,
        &ctx.table_schema.key_cols,
      );
      for key_bound in key_bounds {
        row_region.push(key_bound);
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Read Column Region.
    let mut val_col_region = BTreeSet::<ColName>::new();
    val_col_region.extend(self.query_plan.col_usage_node.safe_present_cols.clone());
    for (key_col, _) in &ctx.table_schema.key_cols {
      val_col_region.remove(key_col);
    }

    // Move the MSTableReadES to the Pending state with the given ReadRegion.
    let protect_qid = mk_qid(io_ctx.rand());
    let val_col_region = Vec::from_iter(val_col_region.into_iter());
    let read_region = ReadRegion { val_col_region, row_region };
    self.state = MSReadExecutionS::Pending(Pending {
      read_region: read_region.clone(),
      query_id: protect_qid.clone(),
    });

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
    _: QueryId,
  ) -> MSTableReadAction {
    match &self.state {
      MSReadExecutionS::Pending(pending) => {
        let gr_query_statuses = match compute_subqueries(
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
        ) {
          Ok(gr_query_statuses) => gr_query_statuses,
          Err(eval_error) => {
            self.state = MSReadExecutionS::Done;
            return MSTableReadAction::QueryError(mk_eval_error(eval_error));
          }
        };

        // Here, we have computed all GRQueryESs, and we can now add them to Executing.
        let mut subqueries = Vec::<SingleSubqueryStatus>::new();
        for gr_query_es in &gr_query_statuses {
          subqueries.push(SingleSubqueryStatus::Pending(SubqueryPending {
            context: gr_query_es.context.clone(),
            query_id: gr_query_es.query_id.clone(),
          }));
        }

        // Move the ES to the Executing state.
        self.state = MSReadExecutionS::Executing(Executing {
          completed: 0,
          subqueries,
          row_region: pending.read_region.row_region.clone(),
        });

        if gr_query_statuses.is_empty() {
          // Since there are no subqueries, we can go straight to finishing the ES.
          self.finish_ms_table_read_es(ctx, io_ctx, ms_query_es)
        } else {
          // Return the subqueries
          MSTableReadAction::SendSubqueries(gr_query_statuses)
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
    let executing_state = cast!(MSReadExecutionS::Executing, &mut self.state).unwrap();
    let subquery_idx = executing_state.find_subquery(&subquery_id).unwrap();
    let single_status = executing_state.subqueries.get_mut(subquery_idx).unwrap();
    let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
    *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
      context: context.clone(),
      result: table_views,
    });
    executing_state.completed += 1;

    // If there are still subqueries to compute, then we wait. Otherwise, we finish
    // up the MSTableReadES and respond to the client.
    if executing_state.completed < executing_state.subqueries.len() {
      MSTableReadAction::Wait
    } else {
      self.finish_ms_table_read_es(ctx, io_ctx, ms_query_es)
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  pub fn finish_ms_table_read_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    ms_query_es: &MSQueryES,
  ) -> MSTableReadAction {
    let executing_state = cast!(MSReadExecutionS::Executing, &mut self.state).unwrap();

    // Compute children.
    let mut children = Vec::<(Vec<proc::ColumnRef>, Vec<TransTableName>)>::new();
    let mut subquery_results = Vec::<Vec<TableView>>::new();
    for single_status in &executing_state.subqueries {
      let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
      let context_schema = &result.context.context_schema;
      children
        .push((context_schema.column_context_schema.clone(), context_schema.trans_table_names()));
      subquery_results.push(result.result.clone());
    }

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
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, ctx: &mut TabletContext, _: &mut IO) {
    match &self.state {
      MSReadExecutionS::Start => {}
      MSReadExecutionS::ColumnsLocking(_) => {}
      MSReadExecutionS::GossipDataWaiting => {}
      MSReadExecutionS::Pending(pending) => {
        // Here, we remove the ReadRegion from `m_waiting_read_protected`, if it still
        // exists. Note that we leave `m_read_protected` and `m_write_protected` intact
        // since they are inconvenient to change (since they don't have `query_id`.
        ctx.remove_m_read_protected_request(&self.timestamp, &pending.query_id);
      }
      MSReadExecutionS::Executing(executing) => {
        // Here, we need to cancel every Subquery. Depending on the state of the
        // SingleSubqueryStatus, we either need to either clean up the column locking
        // request or the ReadRegion from m_waiting_read_protected.

        // Note that we leave `m_read_protected` and `m_write_protected` in-tact
        // since they are inconvenient to change (since they don't have `query_id`).
        for single_status in &executing.subqueries {
          match single_status {
            SingleSubqueryStatus::Pending(_) => {}
            SingleSubqueryStatus::Finished(_) => {}
          }
        }
      }
      MSReadExecutionS::Done => {}
    }
    self.state = MSReadExecutionS::Done;
  }
}
