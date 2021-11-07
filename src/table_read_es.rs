use crate::col_usage::collect_top_level_cols;
use crate::common::{
  btree_multimap_insert, lookup, mk_qid, CoreIOCtx, KeyBound, OrigP, QueryESResult, QueryPlan,
  TableRegion,
};
use crate::expression::{compress_row_region, is_true};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, CQueryPath, ColName, ColValN, Context, ContextRow, QueryId, TQueryPath, TableView,
  Timestamp, TransTableName,
};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::server::{
  evaluate_super_simple_select, mk_eval_error, weak_contains_col, ContextConstructor,
};
use crate::storage::SimpleStorageView;
use crate::tablet::{
  compute_subqueries, ColumnsLocking, ContextKeyboundComputer, Executing, Pending,
  QueryReplanningSqlView, RequestedReadProtected, SingleSubqueryStatus, StorageLocalTable,
  SubqueryFinished, SubqueryPending, TabletContext,
};
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  TableReadES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum ExecutionS {
  Start,
  ColumnsLocking(ColumnsLocking),
  GossipDataWaiting,
  Pending(Pending),
  Executing(Executing),
  WaitingGlobalLockedCols(QueryESResult),
  Done,
}

#[derive(Debug)]
pub struct TableReadES {
  pub root_query_path: CQueryPath,
  pub timestamp: Timestamp,
  pub context: Rc<Context>,

  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub waiting_global_locks: BTreeSet<QueryId>,
  pub state: ExecutionS,
}

pub enum TableAction {
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

impl TableReadES {
  pub fn start<IO: CoreIOCtx>(&mut self, ctx: &mut TabletContext, io_ctx: &mut IO) -> TableAction {
    // First, we lock the columns that the QueryPlan requires certain properties of.
    assert!(matches!(self.state, ExecutionS::Start));

    let mut all_cols = BTreeSet::<ColName>::new();
    all_cols.extend(self.query_plan.col_usage_node.external_cols.clone());
    all_cols.extend(self.query_plan.col_usage_node.safe_present_cols.clone());

    // If there are extra required cols, we add them in.
    if let Some(extra_cols) = self.query_plan.extra_req_cols.get(self.sql_query.table()) {
      all_cols.extend(extra_cols.clone());
    }

    let locked_cols_qid = ctx.add_requested_locked_columns(
      io_ctx,
      OrigP::new(self.query_id.clone()),
      self.timestamp.clone(),
      all_cols.into_iter().collect(),
    );
    self.state = ExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid });

    TableAction::Wait
  }

  /// This checks that `external_cols` are not present, and `safe_present_cols` and
  /// `extra_req_cols` are preset.
  ///
  /// Note: this does *not* required columns to be locked.
  fn does_query_plan_align(&self, ctx: &TabletContext) -> bool {
    // First, check that `external_cols are absent.
    for col in &self.query_plan.col_usage_node.external_cols {
      // Since the `key_cols` are static, no query plan should have one of
      // these as an External Column.
      assert!(lookup(&ctx.table_schema.key_cols, col).is_none());
      if ctx.table_schema.val_cols.static_read(&col, self.timestamp).is_some() {
        return false;
      }
    }

    // Next, check that `safe_present_cols` are present.
    for col in &self.query_plan.col_usage_node.safe_present_cols {
      if !weak_contains_col(&ctx.table_schema, col, &self.timestamp) {
        return false;
      }
    }

    // Next, check that `extra_req_cols` are present.
    if let Some(extra_cols) = self.query_plan.extra_req_cols.get(self.sql_query.table()) {
      for col in extra_cols {
        if !weak_contains_col(&ctx.table_schema, col, &self.timestamp) {
          return false;
        }
      }
    }

    return true;
  }

  /// Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> TableAction {
    for (table_path, gen) in &self.query_plan.table_location_map {
      if !ctx.gossip.sharding_config.contains_key(&(table_path.clone(), gen.clone())) {
        // If not, we go to GossipDataWaiting
        self.state = ExecutionS::GossipDataWaiting;

        // Request a GossipData from the Master to help stimulate progress.
        let sender_path = ctx.this_sid.clone();
        ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::MasterGossipRequest(
          msg::MasterGossipRequest { sender_path },
        ));

        return TableAction::Wait;
      }
    }

    // We start locking the regions.
    self.start_table_read_es(ctx, io_ctx)
  }

  fn common_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> TableAction {
    // Now, we check whether the TableSchema aligns with the QueryPlan.
    if !self.does_query_plan_align(ctx) {
      self.state = ExecutionS::Done;
      TableAction::QueryError(msg::QueryError::InvalidQueryPlan)
    } else {
      // If it aligns, we verify is GossipData is recent enough.
      self.check_gossip_data(ctx, io_ctx)
    }
  }

  /// Handle Columns being locked
  pub fn local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> TableAction {
    let locking = cast!(ExecutionS::ColumnsLocking, &self.state).unwrap();
    assert_eq!(locking.locked_cols_qid, locked_cols_qid);

    // Since this is only a LockedLockedCols, we amend `waiting_global_locks`.
    self.waiting_global_locks.insert(locked_cols_qid);
    self.common_locked_cols(ctx, io_ctx)
  }

  fn remote_waiting_global_lock<IO: CoreIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    query_id: &QueryId,
  ) -> TableAction {
    self.waiting_global_locks.remove(query_id);
    if let ExecutionS::WaitingGlobalLockedCols(res) = &self.state {
      // Signal Success and return the data.
      let res = res.clone();
      self.state = ExecutionS::Done;
      TableAction::Success(res)
    } else {
      TableAction::Wait
    }
  }

  /// Handle GlobalLockedCols
  pub fn global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> TableAction {
    if let ExecutionS::ColumnsLocking(locking) = &self.state {
      assert_eq!(locking.locked_cols_qid, locked_cols_qid);
      self.common_locked_cols(ctx, io_ctx)
    } else {
      // Here, note that LocalLockedCols must have previously been provided because
      // GlobalLockedCols required a PL insertion.
      self.remote_waiting_global_lock(ctx, io_ctx, &locked_cols_qid)
    }
  }

  /// Here, the column locking request results in us realizing the table has been dropped.
  pub fn table_dropped<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) -> TableAction {
    assert!(cast!(ExecutionS::ColumnsLocking, &self.state).is_ok());
    self.state = ExecutionS::Done;
    TableAction::QueryError(msg::QueryError::InvalidQueryPlan)
  }

  /// Here, we GossipData gets delivered.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> TableAction {
    if let ExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, io_ctx)
    } else {
      // Do nothing
      TableAction::Wait
    }
  }

  /// Processes the Start state of TableReadES.
  fn start_table_read_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> TableAction {
    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &self.sql_query.selection,
      &ctx.table_schema,
      &self.timestamp,
      &self.context.context_schema,
    );

    // Compute the Row Region by taking the union across all ContextRows
    let mut row_region = Vec::<KeyBound>::new();
    for context_row in &self.context.context_rows {
      match keybound_computer.compute_keybounds(&context_row) {
        Ok(key_bounds) => {
          for key_bound in key_bounds {
            row_region.push(key_bound);
          }
        }
        Err(eval_error) => {
          self.state = ExecutionS::Done;
          return TableAction::QueryError(mk_eval_error(eval_error));
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Column Region.
    let mut col_region = BTreeSet::<ColName>::new();
    col_region.extend(self.sql_query.projection.clone());
    col_region.extend(self.query_plan.col_usage_node.safe_present_cols.clone());

    // Move the TableReadES to the Pending state with the given ReadRegion.
    let protect_qid = mk_qid(io_ctx.rand());
    let col_region = Vec::from_iter(col_region.into_iter());
    let read_region = TableRegion { col_region, row_region };
    self.state = ExecutionS::Pending(Pending {
      read_region: read_region.clone(),
      query_id: protect_qid.clone(),
    });

    // Add a read protection requested
    btree_multimap_insert(
      &mut ctx.waiting_read_protected,
      &self.timestamp,
      RequestedReadProtected {
        orig_p: OrigP::new(self.query_id.clone()),
        query_id: protect_qid,
        read_region,
      },
    );

    TableAction::Wait
  }

  /// Handle ReadRegion protection
  pub fn local_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    protect_qid: QueryId,
  ) -> TableAction {
    self.waiting_global_locks.insert(protect_qid);
    let pending = cast!(ExecutionS::Pending, &self.state).unwrap();
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
        &self.sql_query.selection,
        SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
      ),
    ) {
      Ok(gr_query_statuses) => gr_query_statuses,
      Err(eval_error) => {
        self.state = ExecutionS::Done;
        return TableAction::QueryError(mk_eval_error(eval_error));
      }
    };

    if gr_query_statuses.is_empty() {
      // Since there are no subqueries, we can go straight to finishing the ES.
      self.finish_table_read_es(ctx, io_ctx)
    } else {
      // Here, we have computed all GRQueryESs, and we can now add them to Executing.
      let mut subqueries = Vec::<SingleSubqueryStatus>::new();
      for gr_query_es in &gr_query_statuses {
        subqueries.push(SingleSubqueryStatus::Pending(SubqueryPending {
          context: gr_query_es.context.clone(),
          query_id: gr_query_es.query_id.clone(),
        }));
      }

      // Move the ES to the Executing state.
      self.state = ExecutionS::Executing(Executing {
        completed: 0,
        subqueries,
        row_region: pending.read_region.row_region.clone(),
      });

      // Return the subqueries
      TableAction::SendSubqueries(gr_query_statuses)
    }
  }

  /// Handle getting GlobalReadProtected
  pub fn global_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_id: QueryId,
  ) -> TableAction {
    self.remote_waiting_global_lock(ctx, io_ctx, &query_id)
  }

  /// This is called if a subquery fails. This simply responds to the sender
  /// and Exits and Clean Ups this ES. This is also called when a Deadlock Safety
  /// Abortion happens.
  pub fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> TableAction {
    self.exit_and_clean_up(ctx, io_ctx);
    TableAction::QueryError(query_error)
  }

  /// Handles a Subquery completing
  pub fn handle_subquery_done<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    (_, table_views): (Vec<ColName>, Vec<TableView>),
  ) -> TableAction {
    // Add the subquery results into the TableReadES.
    self.new_rms.extend(subquery_new_rms);
    let executing_state = cast!(ExecutionS::Executing, &mut self.state).unwrap();
    let subquery_idx = executing_state.find_subquery(&subquery_id).unwrap();
    let single_status = executing_state.subqueries.get_mut(subquery_idx).unwrap();
    let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
    *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
      context: context.clone(),
      result: table_views,
    });
    executing_state.completed += 1;

    // If there are still subqueries to compute, then we wait. Otherwise, we finish
    // up the TableReadES and respond to the client.
    if executing_state.completed < executing_state.subqueries.len() {
      TableAction::Wait
    } else {
      self.finish_table_read_es(ctx, io_ctx)
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  fn finish_table_read_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
  ) -> TableAction {
    let executing_state = cast!(ExecutionS::Executing, &mut self.state).unwrap();

    // Compute children.
    let mut children = Vec::<(Vec<ColName>, Vec<TransTableName>)>::new();
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
        &self.sql_query.selection,
        SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
      ),
      children,
    );

    // These are all of the `ColNames` we need in order to evaluate the Select.
    let mut top_level_cols_set = BTreeSet::<ColName>::new();
    top_level_cols_set.extend(collect_top_level_cols(&self.sql_query.selection));
    top_level_cols_set.extend(self.sql_query.projection.clone());
    let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

    // Finally, iterate over the Context Rows of the subqueries and compute the final values.
    let mut res_table_views = Vec::<TableView>::new();
    for _ in 0..self.context.context_rows.len() {
      res_table_views.push(TableView::new(self.sql_query.projection.clone()));
    }

    let eval_res = context_constructor.run(
      &self.context.context_rows,
      top_level_col_names.clone(),
      &mut |context_row_idx: usize,
            top_level_col_vals: Vec<ColValN>,
            contexts: Vec<(ContextRow, usize)>,
            count: u64| {
        // First, we extract the subquery values using the child Context indices.
        let mut subquery_vals = Vec::<TableView>::new();
        for (subquery_idx, (_, child_context_idx)) in contexts.iter().enumerate() {
          let val = subquery_results.get(subquery_idx).unwrap().get(*child_context_idx).unwrap();
          subquery_vals.push(val.clone());
        }

        // Now, we evaluate all expressions in the SQL query and amend the
        // result to this TableView (if the WHERE clause evaluates to true).
        let evaluated_select = evaluate_super_simple_select(
          &self.sql_query,
          &top_level_col_names,
          &top_level_col_vals,
          &subquery_vals,
        )?;
        if is_true(&evaluated_select.selection)? {
          // This means that the current row should be selected for the result. Thus, we take
          // the values of the project columns and insert it into the appropriate TableView.
          let mut res_row = Vec::<ColValN>::new();
          for res_col_name in &self.sql_query.projection {
            let idx = top_level_col_names.iter().position(|k| res_col_name == k).unwrap();
            res_row.push(top_level_col_vals.get(idx).unwrap().clone());
          }

          res_table_views[context_row_idx].add_row_multi(res_row, count);
        };
        Ok(())
      },
    );

    if let Err(eval_error) = eval_res {
      self.state = ExecutionS::Done;
      return TableAction::QueryError(mk_eval_error(eval_error));
    }

    // Signal Success and return the data.
    self.state = ExecutionS::Done;
    TableAction::Success(QueryESResult {
      result: (self.sql_query.projection.clone(), res_table_views),
      new_rms: self.new_rms.iter().cloned().collect(),
    })
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {
    self.state = ExecutionS::Done;
  }
}
