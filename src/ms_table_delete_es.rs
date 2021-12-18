use crate::col_usage::{
  collect_top_level_cols, compute_delete_schema, compute_update_schema, free_external_cols,
};
use crate::common::{
  lookup, mk_qid, CoreIOCtx, KeyBound, OrigP, QueryESResult, QueryPlan, ReadRegion, WriteRegion,
  WriteRegionType,
};
use crate::expression::{compress_row_region, compute_key_region, is_true, EvalError};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, CQueryPath, ColName, ColType, ColVal, ColValN, Context, ContextRow, PrimaryKey, QueryId,
  TQueryPath, TableView, Timestamp, TransTableName,
};
use crate::model::message as msg;
use crate::server::{
  contains_col, evaluate_delete, evaluate_update, mk_eval_error, ContextConstructor,
  ServerContextBase,
};
use crate::storage::{GenericTable, MSStorageView};
use crate::tablet::{
  compute_col_map, compute_subqueries, ColumnsLocking, Executing, MSQueryES, Pending,
  RequestedReadProtected, SingleSubqueryStatus, StorageLocalTable, SubqueryFinished,
  SubqueryPending, TabletContext,
};
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  MSTableDeleteES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum MSDeleteExecutionS {
  Start,
  ColumnsLocking(ColumnsLocking),
  GossipDataWaiting,
  Pending(Pending),
  Executing(Executing),
  Done,
}

#[derive(Debug)]
pub struct MSTableDeleteES {
  pub root_query_path: CQueryPath,
  pub timestamp: Timestamp,
  /// This is the Tier that the new UpdateView should be added at.
  pub tier: u32,
  pub context: Rc<Context>,

  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::Delete,
  pub query_plan: QueryPlan,

  /// The `QueryId` of the `MSQueryES` that this ES belongs to.
  /// We make sure that it exists as long as this ES exists.
  pub ms_query_id: QueryId,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub state: MSDeleteExecutionS,
}

pub enum MSTableDeleteAction {
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

impl MSTableDeleteES {
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableDeleteAction {
    // First, we lock the columns that the QueryPlan requires certain properties of.
    assert!(matches!(self.state, MSDeleteExecutionS::Start));

    let mut all_cols = BTreeSet::<ColName>::new();
    all_cols.extend(free_external_cols(&self.query_plan.col_usage_node.external_cols));
    all_cols.extend(self.query_plan.col_usage_node.safe_present_cols.clone());

    // If there are extra required cols, we add them in.
    if let Some(extra_cols) = self.query_plan.extra_req_cols.get(&self.sql_query.table.source_ref) {
      all_cols.extend(extra_cols.clone());
    }

    let locked_cols_qid = ctx.add_requested_locked_columns(
      io_ctx,
      OrigP::new(self.query_id.clone()),
      self.timestamp.clone(),
      all_cols.into_iter().collect(),
    );
    self.state = MSDeleteExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid });

    MSTableDeleteAction::Wait
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
      if ctx.table_schema.val_cols.static_read(&col, self.timestamp).is_some() {
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
    if let Some(extra_cols) = self.query_plan.extra_req_cols.get(&self.sql_query.table.source_ref) {
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
  ) -> MSTableDeleteAction {
    for (table_path, gen) in &self.query_plan.table_location_map {
      if !ctx.gossip.sharding_config.contains_key(&(table_path.clone(), gen.clone())) {
        // If not, we go to GossipDataWaiting
        self.state = MSDeleteExecutionS::GossipDataWaiting;

        // Request a GossipData from the Master to help stimulate progress.
        let sender_path = ctx.this_sid.clone();
        ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::MasterGossipRequest(
          msg::MasterGossipRequest { sender_path },
        ));

        return MSTableDeleteAction::Wait;
      }
    }

    // We start locking the regions.
    self.start_ms_table_delete_es(ctx, io_ctx)
  }

  /// Handle Columns being locked
  pub fn local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableDeleteAction {
    match &self.state {
      MSDeleteExecutionS::ColumnsLocking(locking) => {
        if locking.locked_cols_qid == locked_cols_qid {
          // Now, we check whether the TableSchema aligns with the QueryPlan.
          if !self.does_query_plan_align(ctx) {
            self.state = MSDeleteExecutionS::Done;
            MSTableDeleteAction::QueryError(msg::QueryError::InvalidQueryPlan)
          } else {
            // If it aligns, we verify is GossipData is recent enough.
            self.check_gossip_data(ctx, io_ctx)
          }
        } else {
          debug_assert!(false);
          MSTableDeleteAction::Wait
        }
      }
      _ => MSTableDeleteAction::Wait,
    }
  }

  /// Handle this just as `local_locked_cols`
  pub fn global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableDeleteAction {
    self.local_locked_cols(ctx, io_ctx, locked_cols_qid)
  }

  /// Here, the column locking request results in us realizing the table has been dropped.
  pub fn table_dropped(&mut self, _: &mut TabletContext) -> MSTableDeleteAction {
    match &self.state {
      MSDeleteExecutionS::ColumnsLocking(_) => {
        self.state = MSDeleteExecutionS::Done;
        MSTableDeleteAction::QueryError(msg::QueryError::InvalidQueryPlan)
      }
      _ => {
        debug_assert!(false);
        MSTableDeleteAction::Wait
      }
    }
  }

  /// Here, we GossipData gets delivered.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableDeleteAction {
    if let MSDeleteExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, io_ctx)
    } else {
      // Do nothing
      MSTableDeleteAction::Wait
    }
  }

  /// Starts the Execution state
  fn start_ms_table_delete_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableDeleteAction {
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

    // Compute the Write Region
    let write_region =
      WriteRegion { row_region: row_region.clone(), presence: true, val_col_region: vec![] };

    // Verify that we have WriteRegion Isolation with Subsequent Reads. We abort
    // if we don't, and we amend this MSQuery's VerifyingReadWriteRegions if we do.
    if !ctx.check_write_region_isolation(&write_region, &self.timestamp) {
      self.state = MSDeleteExecutionS::Done;
      MSTableDeleteAction::QueryError(msg::QueryError::WriteRegionConflictWithSubsequentRead)
    } else {
      // Compute the Read Column Region.
      let mut val_col_region = BTreeSet::<ColName>::new();
      val_col_region.extend(self.query_plan.col_usage_node.safe_present_cols.clone());
      for (key_col, _) in &ctx.table_schema.key_cols {
        val_col_region.remove(key_col);
      }

      // Move the MSTableDeleteES to the Pending state with the given ReadRegion.
      let protect_qid = mk_qid(io_ctx.rand());
      let val_col_region = Vec::from_iter(val_col_region.into_iter());
      let read_region = ReadRegion { val_col_region, row_region };
      self.state = MSDeleteExecutionS::Pending(Pending {
        read_region: read_region.clone(),
        query_id: protect_qid.clone(),
      });

      // Add a ReadRegion to the `m_waiting_read_protected` and the
      // WriteRegion into `m_write_protected`.
      let verifying = ctx.verifying_writes.get_mut(&self.timestamp).unwrap();
      verifying.m_waiting_read_protected.insert(RequestedReadProtected {
        orig_p: OrigP::new(self.query_id.clone()),
        query_id: protect_qid,
        read_region,
      });
      verifying.m_write_protected.insert(write_region);
      MSTableDeleteAction::Wait
    }
  }

  /// Handle ReadRegion protection
  pub fn local_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    ms_query_es: &mut MSQueryES,
    _: QueryId,
  ) -> MSTableDeleteAction {
    match &self.state {
      MSDeleteExecutionS::Pending(pending) => {
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
              self.tier.clone() + 1, // Remember that `tier` is the Tier to write to, which is
                                     // one lower than which to read from.
            ),
          ),
        ) {
          Ok(gr_query_statuses) => gr_query_statuses,
          Err(eval_error) => {
            self.state = MSDeleteExecutionS::Done;
            return MSTableDeleteAction::QueryError(mk_eval_error(eval_error));
          }
        };

        // Here, we have to evaluate subqueries. Thus, we go to Executing and return
        // SendSubqueries to the parent server.
        let mut subqueries = Vec::<SingleSubqueryStatus>::new();
        for gr_query_es in &gr_query_statuses {
          subqueries.push(SingleSubqueryStatus::Pending(SubqueryPending {
            context: gr_query_es.context.clone(),
            query_id: gr_query_es.query_id.clone(),
          }));
        }

        // Move the ES to the Executing state.
        self.state = MSDeleteExecutionS::Executing(Executing {
          completed: 0,
          subqueries,
          row_region: pending.read_region.row_region.clone(),
        });

        if gr_query_statuses.is_empty() {
          // Since there are no subqueries, we can go straight to finishing the ES.
          self.finish_ms_table_delete_es(ctx, io_ctx, ms_query_es)
        } else {
          // Return the subqueries
          MSTableDeleteAction::SendSubqueries(gr_query_statuses)
        }
      }
      _ => {
        debug_assert!(false);
        MSTableDeleteAction::Wait
      }
    }
  }

  /// This is called if a subquery fails.
  pub fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> MSTableDeleteAction {
    self.exit_and_clean_up(ctx, io_ctx);
    MSTableDeleteAction::QueryError(query_error)
  }

  /// Handles a Subquery completing.
  pub fn handle_subquery_done<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    ms_query_es: &mut MSQueryES,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    (_, table_views): (Vec<Option<ColName>>, Vec<TableView>),
  ) -> MSTableDeleteAction {
    // Add the subquery results into the MSTableDeleteES.
    self.new_rms.extend(subquery_new_rms);
    let executing_state = cast!(MSDeleteExecutionS::Executing, &mut self.state).unwrap();
    let subquery_idx = executing_state.find_subquery(&subquery_id).unwrap();
    let single_status = executing_state.subqueries.get_mut(subquery_idx).unwrap();
    let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
    *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
      context: context.clone(),
      result: table_views,
    });
    executing_state.completed += 1;

    // If there are still subqueries to compute, then we wait. Otherwise, we finish
    // up the MSTableDeleteES and respond to the client.
    if executing_state.completed < executing_state.subqueries.len() {
      MSTableDeleteAction::Wait
    } else {
      self.finish_ms_table_delete_es(ctx, io_ctx, ms_query_es)
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  pub fn finish_ms_table_delete_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    ms_query_es: &mut MSQueryES,
  ) -> MSTableDeleteAction {
    let executing_state = cast!(MSDeleteExecutionS::Executing, &mut self.state).unwrap();

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
          self.tier.clone() + 1,
        ),
      ),
      children,
    );

    // These are all of the `ColNames` that we need in order to evaluate the Delete.
    // This consists of all Top-Level Columns for every expression, as well as all Key
    // Columns (since they are included in the resulting table).
    let mut top_level_cols_set = BTreeSet::<proc::ColumnRef>::new();
    top_level_cols_set.extend(ctx.table_schema.get_key_col_refs());
    top_level_cols_set.extend(collect_top_level_cols(&self.sql_query.selection));
    let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

    // Setup the TableView that we are going to return and the UpdateView that we're going
    // to hold in the MSQueryES.
    let res_col_names = compute_delete_schema(&self.sql_query);
    let mut res_table_view = TableView::new(res_col_names.clone());
    let mut update_view = GenericTable::new();

    // Finally, iterate over the Context Rows of the subqueries and compute the final values.
    let eval_res = context_constructor.run(
      &self.context.context_rows,
      top_level_col_names.clone(),
      &mut |context_row_idx: usize,
            top_level_col_vals: Vec<ColValN>,
            contexts: Vec<(ContextRow, usize)>,
            count: u64| {
        assert_eq!(context_row_idx, 0); // Recall there is only one ContextRow for Updates.

        // First, we extract the subquery values using the child Context indices.
        let mut subquery_vals = Vec::<TableView>::new();
        for (subquery_idx, (_, child_context_idx)) in contexts.iter().enumerate() {
          let val = subquery_results.get(subquery_idx).unwrap().get(*child_context_idx).unwrap();
          subquery_vals.push(val.clone());
        }

        // Now, we evaluate all expressions in the SQL query and amend the
        // result to this TableView (if the WHERE clause evaluates to true).
        let evaluated_delete = evaluate_delete(
          &self.sql_query,
          &top_level_col_names,
          &top_level_col_vals,
          &subquery_vals,
        )?;
        if is_true(&evaluated_delete.selection)? {
          // This means that the current row should be selected for the result.
          let mut res_row = Vec::<ColValN>::new();

          // We reconstruct the PrimaryKey
          let mut primary_key = PrimaryKey { cols: vec![] };
          for key_col in &ctx.table_schema.get_key_col_refs() {
            let idx = top_level_col_names.iter().position(|col| key_col == col).unwrap();
            let col_val = top_level_col_vals.get(idx).unwrap().clone();
            res_row.push(col_val.clone());
            primary_key.cols.push(col_val.unwrap());
          }

          // Amend the UpdateView to delete the PrimaryKey
          update_view.insert((primary_key, None), None);
        };
        Ok(())
      },
    );

    if let Err(eval_error) = eval_res {
      self.state = MSDeleteExecutionS::Done;
      return MSTableDeleteAction::QueryError(mk_eval_error(eval_error));
    }

    // Amend the `update_view` in the MSQueryES.
    ms_query_es.update_views.insert(self.tier.clone(), update_view);

    // Signal Success and return the data.
    self.state = MSDeleteExecutionS::Done;
    MSTableDeleteAction::Success(QueryESResult {
      result: (res_col_names, vec![res_table_view]),
      new_rms: self.new_rms.iter().cloned().collect(),
    })
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {
    self.state = MSDeleteExecutionS::Done;
  }
}