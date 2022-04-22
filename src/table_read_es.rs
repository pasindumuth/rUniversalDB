use crate::col_usage::{collect_top_level_cols, free_external_cols};
use crate::common::{
  btree_multimap_insert, lookup, mk_qid, to_table_path, CoreIOCtx, GossipData, GossipDataView,
  KeyBound, OrigP, QueryESResult, QueryPlan, ReadRegion, Timestamp,
};
use crate::common::{
  CQueryPath, CTQueryPath, ColName, ColType, ColVal, ColValN, Context, ContextRow, PaxosGroupId,
  PaxosGroupIdTrait, QueryId, SlaveGroupId, TQueryPath, TablePath, TableView, TransTableName,
};
use crate::expression::{
  compress_row_region, compute_key_region, evaluate_c_expr, is_true, CExpr, EvalError,
};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::master_query_planning_es::ColPresenceReq;
use crate::message as msg;
use crate::server::{
  contains_col, contains_val_col, evaluate_super_simple_select, mk_eval_error, ContextConstructor,
  ExtraColumnRef, LocalColumnRef,
};
use crate::server::{LocalTable, ServerContextBase};
use crate::sql_ast::proc::SelectClause;
use crate::sql_ast::{iast, proc};
use crate::storage::SimpleStorageView;
use crate::tablet::{
  compute_col_map, compute_subqueries, ColSet, ColumnsLocking, Executing, Pending,
  RequestedReadProtected, StorageLocalTable, TPESAction, TPESBase, TabletContext,
};
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::ops::Deref;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  Utilities
// -----------------------------------------------------------------------------------------------

pub fn request_lock_columns<IO: CoreIOCtx>(
  ctx: &mut TabletContext,
  io_ctx: &mut IO,
  query_id: &QueryId,
  timestamp: &Timestamp,
  query_plan: &QueryPlan,
) -> QueryId {
  let mut col_set =
    if let Some(col_presence_req) = query_plan.col_presence_req.get(&ctx.this_table_path) {
      match col_presence_req {
        ColPresenceReq::ReqPresentAbsent(req) => {
          let mut all_cols = Vec::<ColName>::new();
          all_cols.extend(req.present_cols.clone());
          all_cols.extend(req.absent_cols.clone());
          ColSet::Cols(all_cols)
        }
        ColPresenceReq::ReqPresentExclusive(_) => ColSet::All,
      }
    } else {
      ColSet::Cols(vec![])
    };

  // Even if `col_set` is empty, we need to at least update the `presence_timestamp`.
  ctx.add_requested_locked_columns(io_ctx, OrigP::new(query_id.clone()), timestamp.clone(), col_set)
}

/// Check that `col_presence_req` aligns with the local `TabletSchema`.
pub fn does_query_plan_align(
  ctx: &TabletContext,
  timestamp: &Timestamp,
  query_plan: &QueryPlan,
) -> bool {
  if let Some(col_presence_req) = query_plan.col_presence_req.get(&ctx.this_table_path) {
    match col_presence_req {
      ColPresenceReq::ReqPresentAbsent(req) => {
        // Check the `present_cols`
        for col in &req.present_cols {
          if !contains_val_col(&ctx.table_schema, col, timestamp) {
            return false;
          }
        }

        // Check the `absent_cols`
        for col in &req.absent_cols {
          if contains_val_col(&ctx.table_schema, col, timestamp) {
            return false;
          }
        }
      }
      ColPresenceReq::ReqPresentExclusive(req) => {
        // Check that `req.cols` contains the exact same columns as this TableSchema.
        let all_val_cols = ctx.table_schema.val_cols.static_snapshot_read(timestamp);
        if req.cols.len() != all_val_cols.len() {
          return false;
        }
        for col in &req.cols {
          if !all_val_cols.contains_key(col) {
            return false;
          }
        }
        return true;
      }
    }
  }

  return true;
}

/// This checks if the `GossipData` provided is not too old and contains everything
/// necessary to handle the `QueryPlan`.
pub fn check_gossip<'a>(gossip: &GossipDataView<'a>, query_plan: &QueryPlan) -> bool {
  // Check that all tables in the `table_location_map` are present.
  for (table_path, full_gen) in &query_plan.table_location_map {
    if !gossip.sharding_config.contains_key(&(table_path.clone(), full_gen.clone())) {
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

/// Compute the `ReadRegion` with the given `query_plan` and `selection`. The
/// `extra_local_cols` are extra `ColName`s that need to be locked that are
/// specific to the query.
pub fn compute_read_region(
  key_cols: &Vec<(ColName, ColType)>,
  query_plan: &QueryPlan,
  context: &Context,
  selection: &proc::ValExpr,
  extra_local_cols: Vec<ColName>,
) -> ReadRegion {
  // Compute the Row Region by taking the union across all ContextRows
  let mut row_region = Vec::<KeyBound>::new();
  for context_row in &context.context_rows {
    let key_bounds = compute_key_region(
      selection,
      compute_col_map(&context.context_schema, context_row),
      &query_plan.col_usage_node.source,
      key_cols,
    );
    for key_bound in key_bounds {
      row_region.push(key_bound);
    }
  }
  row_region = compress_row_region(row_region);

  // Compute the Column Region.
  let mut val_col_region = BTreeSet::<ColName>::new();
  val_col_region.extend(query_plan.col_usage_node.safe_present_cols.clone());
  val_col_region.extend(extra_local_cols);
  for (key_col, _) in key_cols {
    val_col_region.remove(key_col);
  }
  let val_col_region = Vec::from_iter(val_col_region.into_iter());

  // Compute the ReadRegion
  ReadRegion { val_col_region, row_region }
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

  // Fields needed for responding.
  pub sender_path: CTQueryPath,
  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub waiting_global_locks: BTreeSet<QueryId>,
  pub state: ExecutionS,
  pub child_queries: Vec<QueryId>,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl TableReadES {
  /// Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> TPESAction {
    // If the GossipData is valid, then act accordingly.
    if check_gossip(&ctx.gossip.get(), &self.query_plan) {
      // We start locking the regions.
      self.start_table_read_es(ctx, io_ctx)
    } else {
      // If not, we go to GossipDataWaiting
      self.state = ExecutionS::GossipDataWaiting;

      // Request a GossipData from the Master to help stimulate progress.
      let sender_path = ctx.this_sid.clone();
      ctx.send_to_master(
        io_ctx,
        msg::MasterRemotePayload::MasterGossipRequest(msg::MasterGossipRequest { sender_path }),
      );

      return TPESAction::Wait;
    }
  }

  fn common_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> TPESAction {
    // Now, we check whether the TableSchema aligns with the QueryPlan.
    if !does_query_plan_align(ctx, &self.timestamp, &self.query_plan) {
      self.state = ExecutionS::Done;
      TPESAction::QueryError(msg::QueryError::InvalidQueryPlan)
    } else {
      // If it aligns, we verify is GossipData is recent enough.
      self.check_gossip_data(ctx, io_ctx)
    }
  }

  fn remove_waiting_global_lock<IO: CoreIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    query_id: &QueryId,
  ) -> TPESAction {
    self.waiting_global_locks.remove(query_id);
    if self.waiting_global_locks.is_empty() {
      if let ExecutionS::WaitingGlobalLockedCols(res) = &self.state {
        // Signal Success and return the data.
        let res = res.clone();
        self.state = ExecutionS::Done;
        TPESAction::Success(res)
      } else {
        TPESAction::Wait
      }
    } else {
      TPESAction::Wait
    }
  }

  /// Processes the Start state of TableReadES.
  fn start_table_read_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> TPESAction {
    // Get extra columns that must be in the region due to SELECT * .
    let mut extra_cols = match &self.sql_query.projection {
      SelectClause::SelectList(_) => vec![],
      SelectClause::Wildcard => ctx.table_schema.get_schema_val_cols_static(&self.timestamp),
    };

    // Compute the ReadRegion
    let read_region = compute_read_region(
      &ctx.table_schema.key_cols,
      &self.query_plan,
      &self.context,
      &self.sql_query.selection,
      extra_cols,
    );

    // Move the TableReadES to the Pending state
    let protect_qid = mk_qid(io_ctx.rand());
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

    TPESAction::Wait
  }

  /// Handles a ES finishing with all subqueries results in.
  fn finish_table_read_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
  ) -> TPESAction {
    let exec = cast!(ExecutionS::Executing, &mut self.state).unwrap();

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
        SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
      ),
      children,
    );

    // Evaluate
    let schema = self.query_plan.col_usage_node.schema.clone();
    let eval_res = fully_evaluate_select(
      context_constructor,
      &self.context.deref(),
      subquery_results,
      &self.sql_query,
      &schema,
    );

    match eval_res {
      Ok(res_table_views) => {
        let res = QueryESResult {
          result: (schema, res_table_views),
          new_rms: self.new_rms.iter().cloned().collect(),
        };

        if self.waiting_global_locks.is_empty() {
          // Signal Success and return the data.
          self.state = ExecutionS::Done;
          TPESAction::Success(res)
        } else {
          self.state = ExecutionS::WaitingGlobalLockedCols(res);
          TPESAction::Wait
        }
      }
      Err(eval_error) => {
        self.state = ExecutionS::Done;
        TPESAction::QueryError(mk_eval_error(eval_error))
      }
    }
  }
}

impl TPESBase for TableReadES {
  type ESContext = ();

  fn sender_sid(&self) -> &SlaveGroupId {
    &self.sender_path.node_path.sid
  }
  fn query_id(&self) -> &QueryId {
    &self.query_id
  }
  fn ctx_query_id(&self) -> Option<&QueryId> {
    None
  }

  fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    _: &mut (),
  ) -> TPESAction {
    // First, we lock the columns that the QueryPlan requires certain properties of.
    assert!(matches!(self.state, ExecutionS::Start));
    let qid = request_lock_columns(ctx, io_ctx, &self.query_id, &self.timestamp, &self.query_plan);
    self.state = ExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid: qid });

    TPESAction::Wait
  }

  /// Handle Columns being locked
  fn local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> TPESAction {
    let locking = cast!(ExecutionS::ColumnsLocking, &self.state).unwrap();
    assert_eq!(locking.locked_cols_qid, locked_cols_qid);

    // Since this is only a LockedLockedCols, we amend `waiting_global_locks`.
    self.waiting_global_locks.insert(locked_cols_qid);
    self.common_locked_cols(ctx, io_ctx)
  }

  /// Handle GlobalLockedCols
  fn global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> TPESAction {
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
  fn table_dropped(&mut self, _: &mut TabletContext) -> TPESAction {
    assert!(cast!(ExecutionS::ColumnsLocking, &self.state).is_ok());
    self.state = ExecutionS::Done;
    TPESAction::QueryError(msg::QueryError::InvalidQueryPlan)
  }

  /// Here, we GossipData gets delivered.
  fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    _: &mut (),
  ) -> TPESAction {
    if let ExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, io_ctx)
    } else {
      // Do nothing
      TPESAction::Wait
    }
  }

  /// Handle ReadRegion protection
  fn local_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    _: &mut (),
    protect_qid: QueryId,
  ) -> TPESAction {
    match &self.state {
      ExecutionS::Pending(pending) if protect_qid == pending.query_id => {
        self.waiting_global_locks.insert(protect_qid);
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
            SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
          ),
        );

        // Move the ES to the Executing state.
        self.state = ExecutionS::Executing(Executing::create(&gr_query_ess));
        let exec = cast!(ExecutionS::Executing, &mut self.state).unwrap();

        // See if we are already finished (due to having no subqueries).
        if exec.is_complete() {
          self.finish_table_read_es(ctx, io_ctx)
        } else {
          // Otherwise, return the subqueries.
          TPESAction::SendSubqueries(gr_query_ess)
        }
      }
      _ => {
        debug_assert!(false);
        TPESAction::Wait
      }
    }
  }

  /// Handle getting GlobalReadProtected
  fn global_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_id: QueryId,
  ) -> TPESAction {
    self.remove_waiting_global_lock(ctx, io_ctx, &query_id)
  }

  /// This is called if a subquery fails. This simply responds to the sender
  /// and Exits and Clean Ups this ES. This is also called when a Deadlock Safety
  /// Abortion happens.
  fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> TPESAction {
    self.exit_and_clean_up(ctx, io_ctx);
    TPESAction::QueryError(query_error)
  }

  /// Handles a Subquery completing
  fn handle_subquery_done<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    _: &mut (),
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    (_, table_views): (Vec<Option<ColName>>, Vec<TableView>),
  ) -> TPESAction {
    // Add the subquery results into the TableReadES.
    self.new_rms.extend(subquery_new_rms);
    let exec = cast!(ExecutionS::Executing, &mut self.state).unwrap();
    exec.add_subquery_result(subquery_id, table_views);

    // See if we are finished (due to computing all subqueries).
    if exec.is_complete() {
      self.finish_table_read_es(ctx, io_ctx)
    } else {
      // Otherwise, we wait.
      TPESAction::Wait
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {
    self.state = ExecutionS::Done;
  }

  fn deregister(self, _: &mut ()) -> (QueryId, CTQueryPath, Vec<QueryId>) {
    (self.query_id, self.sender_path, self.child_queries)
  }
}

/// Fully evaluate a `Select` query, including aggregation.
pub fn fully_evaluate_select<LocalTableT: LocalTable>(
  context_constructor: ContextConstructor<LocalTableT>,
  context: &Context,
  subquery_results: Vec<Vec<TableView>>,
  sql_query: &proc::SuperSimpleSelect,
  schema: &Vec<Option<ColName>>,
) -> Result<Vec<TableView>, EvalError> {
  // These are all of the `ExtraColumnRef`s we need in order to evaluate the Select.
  let mut top_level_extra_cols_set = BTreeSet::<ExtraColumnRef>::new();
  top_level_extra_cols_set.extend(
    collect_top_level_cols(&sql_query.selection).into_iter().map(|c| ExtraColumnRef::Named(c)),
  );
  match &sql_query.projection {
    SelectClause::SelectList(select_list) => {
      for (select_item, _) in select_list {
        top_level_extra_cols_set.extend(
          collect_top_level_cols(match select_item {
            proc::SelectItem::ValExpr(expr) => expr,
            proc::SelectItem::UnaryAggregate(unary_agg) => &unary_agg.expr,
          })
          .into_iter()
          .map(|c| ExtraColumnRef::Named(c)),
        );
      }
    }
    SelectClause::Wildcard => {
      // For `ColName`s that are present in `schema`, read the column.
      // Otherwise, read the index.
      for (index, maybe_col_name) in schema.iter().enumerate() {
        if let Some(col_name) = maybe_col_name {
          top_level_extra_cols_set.insert(ExtraColumnRef::Named(proc::ColumnRef {
            table_name: None,
            col_name: col_name.clone(),
          }));
        } else {
          top_level_extra_cols_set.insert(ExtraColumnRef::Unnamed(index));
        }
      }
    }
  }
  let top_level_extra_col_refs = Vec::from_iter(top_level_extra_cols_set.into_iter());

  // Finally, iterate over the Context Rows of the subqueries and compute the final values.
  let mut pre_agg_table_views = Vec::<TableView>::new();
  for _ in 0..context.context_rows.len() {
    pre_agg_table_views.push(TableView::new(schema.clone()));
  }

  context_constructor.run(
    &context.context_rows,
    top_level_extra_col_refs.clone(),
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
        schema,
        &top_level_extra_col_refs,
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

  Ok(pre_agg_table_views)
}

pub fn perform_aggregation(
  sql_query: &proc::SuperSimpleSelect,
  pre_agg_table_views: Vec<TableView>,
) -> Result<Vec<TableView>, EvalError> {
  // Produce the result table, handling aggregates and DISTINCT accordingly.
  let mut res_table_views = Vec::<TableView>::new();
  for pre_agg_table_view in pre_agg_table_views {
    let mut res_table_view = TableView::new(pre_agg_table_view.col_names);

    // Handle aggregation
    if is_agg(sql_query) {
      // Invert `pre_agg_table_view.rows` having indexes in the outer vector correspond
      // to the columns. Recall that there are as many columns in `pre_agg_table_view` as
      // there are in the final result table (due to how all SelectItems must be aggregates,
      // and how all aggregates take only one argument).
      // TODO perhaps introduce a SingleColumn type.
      let select_list = cast!(proc::SelectClause::SelectList, &sql_query.projection).unwrap();
      let mut columns = Vec::<TableView>::new();
      for _ in 0..select_list.len() {
        columns.push(TableView::new(Vec::new()));
      }
      for (row, count) in pre_agg_table_view.rows {
        for (i, val) in row.into_iter().enumerate() {
          columns[i].add_row_multi(vec![val], count);
        }
      }

      // Perform the aggregation
      let mut res_row = Vec::<ColValN>::new();
      for ((select_item, _), mut column) in select_list.iter().zip(columns.into_iter()) {
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

  Ok(res_table_views)
}

/// Checks if the `SuperSimpleSelect` has aggregates in its projection.
/// NOTE: Recall that in this case, all elements in the projection are aggregates
/// for now for simplicity.
pub fn is_agg(sql_query: &proc::SuperSimpleSelect) -> bool {
  match &sql_query.projection {
    SelectClause::SelectList(select_list) => {
      if let Some((proc::SelectItem::UnaryAggregate(_), _)) = select_list.first() {
        true
      } else {
        false
      }
    }
    SelectClause::Wildcard => false,
  }
}
