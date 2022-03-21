use crate::col_usage::{collect_top_level_cols, compute_select_schema, free_external_cols};
use crate::common::{
  btree_multimap_insert, lookup, mk_qid, to_table_path, CoreIOCtx, GossipData, GossipDataView,
  KeyBound, OrigP, QueryESResult, QueryPlan, ReadRegion, Timestamp,
};
use crate::expression::{
  compress_row_region, compute_key_region, evaluate_c_expr, is_true, CExpr, EvalError,
};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  iast, proc, CQueryPath, ColName, ColVal, ColValN, Context, ContextRow, QueryId, TQueryPath,
  TablePath, TableView, TransTableName,
};
use crate::model::message as msg;
use crate::server::{
  contains_col, evaluate_super_simple_select, mk_eval_error, ContextConstructor,
};
use crate::server::{LocalTable, ServerContextBase};
use crate::storage::SimpleStorageView;
use crate::tablet::{
  compute_col_map, compute_subqueries, ColumnsLocking, Executing, Pending, RequestedReadProtected,
  SingleSubqueryStatus, StorageLocalTable, SubqueryFinished, SubqueryPending, TabletContext,
};
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::ops::Deref;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  Utilities
// -----------------------------------------------------------------------------------------------

/// This checks if the `GossipData` provided is not too old and contains everything
/// necessary to handle the `QueryPlan`.
pub fn check_gossip<'a>(gossip: &GossipDataView<'a>, query_plan: &QueryPlan) -> bool {
  // Check that all tables in the `table_location_map` are present.
  for (table_path, gen) in &query_plan.table_location_map {
    if !gossip.sharding_config.contains_key(&(table_path.clone(), gen.clone())) {
      return false;
    };
  }

  // Check that the PaxosGroupIds in `query_leader_map` in the local `leader_map`,
  // since the GRQueryES will rely on this.
  for (sid, _) in &query_plan.query_leader_map {
    if !gossip.slave_address_config.contains_key(sid) {
      return false;
    }
  }

  return true;
}

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
    self.state = ExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid });

    TableAction::Wait
  }

  /// This checks that free `external_cols` are not present, and `safe_present_cols` and
  /// `extra_req_cols` are preset.
  ///
  /// Note: this does *not* required columns to be globally locked, only locally.
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

  /// Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> TableAction {
    // If the GossipData is valid, then act accordingly.
    if check_gossip(&ctx.gossip.get(), &self.query_plan) {
      // We start locking the regions.
      self.start_table_read_es(ctx, io_ctx)
    } else {
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

  fn remove_waiting_global_lock<IO: CoreIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    query_id: &QueryId,
  ) -> TableAction {
    self.waiting_global_locks.remove(query_id);
    if self.waiting_global_locks.is_empty() {
      if let ExecutionS::WaitingGlobalLockedCols(res) = &self.state {
        // Signal Success and return the data.
        let res = res.clone();
        self.state = ExecutionS::Done;
        TableAction::Success(res)
      } else {
        TableAction::Wait
      }
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
      self.remove_waiting_global_lock(ctx, io_ctx, &locked_cols_qid)
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

    // Move the TableReadES to the Pending state with the given ReadRegion.
    let protect_qid = mk_qid(io_ctx.rand());
    let val_col_region = Vec::from_iter(val_col_region.into_iter());
    let read_region = ReadRegion { val_col_region, row_region };
    self.state = ExecutionS::Pending(Pending { query_id: protect_qid.clone() });

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
    match &self.state {
      ExecutionS::Pending(pending) if protect_qid == pending.query_id => {
        self.waiting_global_locks.insert(protect_qid);
        let gr_query_statuses = compute_subqueries(
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
            SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
          ),
        );

        // Here, we have computed all GRQueryESs, and we can now add them to Executing.
        let mut subqueries = Vec::<SingleSubqueryStatus>::new();
        for gr_query_es in &gr_query_statuses {
          subqueries.push(SingleSubqueryStatus::Pending(SubqueryPending {
            context: gr_query_es.context.clone(),
            query_id: gr_query_es.query_id.clone(),
          }));
        }

        // Move the ES to the Executing state.
        self.state = ExecutionS::Executing(Executing { completed: 0, subqueries });

        if gr_query_statuses.is_empty() {
          // Since there are no subqueries, we can go straight to finishing the ES.
          self.finish_table_read_es(ctx, io_ctx)
        } else {
          // Return the subqueries
          TableAction::SendSubqueries(gr_query_statuses)
        }
      }
      _ => {
        debug_assert!(false);
        TableAction::Wait
      }
    }
  }

  /// Handle getting GlobalReadProtected
  pub fn global_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_id: QueryId,
  ) -> TableAction {
    self.remove_waiting_global_lock(ctx, io_ctx, &query_id)
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
    (_, table_views): (Vec<Option<ColName>>, Vec<TableView>),
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
        SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
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
        let res = QueryESResult {
          result: (select_schema, res_table_views),
          new_rms: self.new_rms.iter().cloned().collect(),
        };

        if self.waiting_global_locks.is_empty() {
          // Signal Success and return the data.
          self.state = ExecutionS::Done;
          TableAction::Success(res)
        } else {
          self.state = ExecutionS::WaitingGlobalLockedCols(res);
          TableAction::Wait
        }
      }
      Err(eval_error) => {
        self.state = ExecutionS::Done;
        TableAction::QueryError(mk_eval_error(eval_error))
      }
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {
    self.state = ExecutionS::Done;
  }
}

/// Fully evaluate a `Select` query, including aggregation.
pub fn fully_evaluate_select<LocalTableT: LocalTable>(
  context_constructor: ContextConstructor<LocalTableT>,
  context: &Context,
  subquery_results: Vec<Vec<TableView>>,
  sql_query: &proc::SuperSimpleSelect,
) -> Result<(Vec<Option<ColName>>, Vec<TableView>), EvalError> {
  // These are all of the `ColNames` we need in order to evaluate the Select.
  let mut top_level_cols_set = BTreeSet::<proc::ColumnRef>::new();
  top_level_cols_set.extend(collect_top_level_cols(&sql_query.selection));
  for (select_item, _) in &sql_query.projection {
    top_level_cols_set.extend(collect_top_level_cols(match select_item {
      proc::SelectItem::ValExpr(expr) => expr,
      proc::SelectItem::UnaryAggregate(unary_agg) => &unary_agg.expr,
    }));
  }
  let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

  // Finally, iterate over the Context Rows of the subqueries and compute the final values.
  let mut pre_agg_table_views = Vec::<TableView>::new();
  for _ in 0..context.context_rows.len() {
    // TODO: we shouldn't need to pass in a bogus schema for intermediary tables.
    pre_agg_table_views.push(TableView::new(Vec::new()));
  }

  context_constructor.run(
    &context.context_rows,
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
        &sql_query,
        &top_level_col_names,
        &top_level_col_vals,
        &subquery_vals,
      )?;
      if is_true(&evaluated_select.selection)? {
        // This means that the current row should be selected for the result.
        pre_agg_table_views[context_row_idx].add_row_multi(evaluated_select.projection, count);
      };
      Ok(())
    },
  )?;

  Ok((compute_select_schema(sql_query), pre_agg_table_views))
}

pub fn perform_aggregation(
  sql_query: &proc::SuperSimpleSelect,
  pre_agg_table_views: Vec<TableView>,
) -> Result<(Vec<Option<ColName>>, Vec<TableView>), EvalError> {
  // Produce the result table, handling aggregates and DISTINCT accordingly.
  let mut res_table_views = Vec::<TableView>::new();
  let select_schema = compute_select_schema(sql_query);
  for pre_agg_table_view in pre_agg_table_views {
    let mut res_table_view = TableView::new(select_schema.clone());

    // Handle aggregation
    if is_agg(sql_query) {
      // Invert `pre_agg_table_view.rows` having indexes in the outer vector correspond
      // to the columns. Recall that there are as many columns in `pre_agg_table_view` as
      // there are in the final result table (due to how all SelectItems must be aggregates,
      // and how all aggregates take only one argument).
      // TODO perhaps introduce a SingleColumn type.
      let mut columns = Vec::<TableView>::new();
      for _ in 0..sql_query.projection.len() {
        columns.push(TableView::new(Vec::new()));
      }
      for (row, count) in pre_agg_table_view.rows {
        for (i, val) in row.into_iter().enumerate() {
          columns[i].add_row_multi(vec![val], count);
        }
      }

      // Perform the aggregation
      let mut res_row = Vec::<ColValN>::new();
      for ((select_item, _), mut column) in sql_query.projection.iter().zip(columns.into_iter()) {
        let unary_agg = cast!(proc::SelectItem::UnaryAggregate, select_item).unwrap();

        // Handle inner DISTICT
        if unary_agg.distinct {
          for (_, count) in &mut column.rows {
            *count = 1;
          }
        }

        fn count_op(column: &TableView) -> Result<ColValN, EvalError> {
          let mut total_count: i32 = 0;
          for (val_row, count) in &column.rows {
            let val = val_row.iter().next().unwrap();
            match val {
              None => {}
              Some(_) => {
                total_count += (*count) as i32;
              }
            }
          }
          Ok(Some(ColVal::Int(total_count)))
        }

        fn sum_op(column: &TableView) -> Result<ColValN, EvalError> {
          let mut all_null = true; // Keeps track of if all ColVals are all NULL.
          let mut total_sum = 0;
          for (val_row, count) in &column.rows {
            let val = val_row.iter().next().unwrap();
            match val {
              None => {}
              Some(ColVal::Int(int_val)) => {
                total_sum += int_val * (*count) as i32;
                all_null = false;
              }
              Some(_) => return Err(EvalError::GenericError),
            }
          }

          // In SQL, there are no non-NULL ColVals, then the SUM evaluate to NULL. This
          // includes the case of an empty table.
          Ok(if all_null { None } else { Some(ColVal::Int(total_sum)) })
        }

        // TODO: This should actually be returning a float
        fn avg_op(column: &TableView) -> Result<ColValN, EvalError> {
          let sum_val = sum_op(column)?;
          let count_val = count_op(column)?;
          let avg_expr = CExpr::BinaryExpr {
            op: iast::BinaryOp::Divide,
            left: Box::new(CExpr::Value { val: sum_val }),
            right: Box::new(CExpr::Value { val: count_val }),
          };
          evaluate_c_expr(&avg_expr)
        }

        // Compute the result row
        let res_col_val = match &unary_agg.op {
          iast::UnaryAggregateOp::Count => count_op(&column)?,
          iast::UnaryAggregateOp::Sum => sum_op(&column)?,
          iast::UnaryAggregateOp::Avg => avg_op(&column)?,
        };
        res_row.push(res_col_val);
      }

      res_table_view.add_row(res_row);
    } else {
      res_table_view.rows = pre_agg_table_view.rows;
    }

    // Handle outer DISTINCT
    if sql_query.distinct {
      for (_, count) in &mut res_table_view.rows {
        *count = 1;
      }
    }

    res_table_views.push(res_table_view);
  }

  Ok((select_schema, res_table_views))
}

/// Checks if the `SuperSimpleSelect` has aggregates in its projection.
/// NOTE: Recall that in this case, all elements in the projection are aggregates
/// for now for simplicity.
pub fn is_agg(sql_query: &proc::SuperSimpleSelect) -> bool {
  if let Some((proc::SelectItem::UnaryAggregate(_), _)) = sql_query.projection.get(0) {
    true
  } else {
    false
  }
}
