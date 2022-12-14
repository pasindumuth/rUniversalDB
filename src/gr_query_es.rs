use crate::col_usage::{
  alias_collecting_cb, external_col_collecting_cb, external_trans_table_collecting_cb,
  gr_query_collecting_cb, trans_table_collecting_cb, QueryElement, QueryIterator,
};
use crate::common::{
  lookup, lookup_pos, merge_table_views, mk_qid, rand_string, CTQueryPath, ColVal, ColValN,
  CoreIOCtx, FullGen, OrigP, QueryPlan, ReadOnlySet, Timestamp,
};
use crate::common::{
  CQueryPath, ColName, Context, ContextRow, ContextSchema, Gen, LeadershipId, PaxosGroupIdTrait,
  QueryId, SlaveGroupId, TQueryPath, TablePath, TableView, TierMap, TransTableLocationPrefix,
  TransTableName,
};
use crate::expression::{construct_cexpr, evaluate_c_expr, is_true, EvalError};
use crate::join_util::{
  add_vals, add_vals_general, compute_children_general, extract_subqueries, initialize_contexts,
  initialize_contexts_general, initialize_contexts_general_once, make_parent_row, mk_context_row,
  Location, Locations,
};
use crate::master_query_planning_es::{ColPresenceReq, ErrorTrait};
use crate::message as msg;
use crate::query_converter::collect_jlns;
use crate::server::{
  extract_subquery_vals, CTServerContext, CommonQuery, EvaluatedSuperSimpleSelect,
  GeneralColumnRef, ServerContextBase, UnnamedColumnRef,
};
use crate::sql_ast::{iast, proc};
use crate::table_read_es::perform_aggregation;
use crate::tablet::Executing;
use crate::tm_status::{SendHelper, TMStatus};
use crate::trans_table_read_es::TransTableSource;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  GRQuery Actions
// -----------------------------------------------------------------------------------------------
pub enum InternalError {
  InternalColumnsDNE(Vec<ColName>),
  QueryError(msg::QueryError),
}

pub struct GRQueryResult {
  pub new_rms: BTreeSet<TQueryPath>,
  pub result: Vec<TableView>,
}

pub enum GRQueryAction {
  /// Do Nothing
  Wait,
  /// This tells the parent Server to perform subqueries.
  SendSubqueries(Vec<GRQueryES>),
  /// This tells the parent Server to execute the given TMStatus.
  ExecuteTMStatus(TMStatus),
  /// This tells the parent Server that this GRQueryES has completed
  /// successfully (having already responded, etc).
  Success(GRQueryResult),
  /// This indicates that the GRQueryES failed, where a child query responded with a QueryError.
  /// Here, the GRQueryES can just be trivially erased from the parent.
  QueryError(msg::QueryError),
}

// -----------------------------------------------------------------------------------------------
//  GRQueryES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
struct ReadStage {
  stage_idx: usize,
  /// This fields maps the indices of the GRQueryES Context to that of the Context
  /// in this TMStatus. We cache this here since it is computed when the child
  /// context is computed.
  parent_to_child_context_map: Vec<usize>,
  stage_query_id: QueryId,
}

/// This is string like `"LRLL"`, which points to a descendent `JoinNode` in the Join Tree.
type JoinNodeId = String;

#[derive(Debug)]
enum ResultState {
  Waiting,
  // We only get here if there are weak conjunctions whose subqueries we are evaluating,
  // and there are strong conjunctions that we will have to evaluate next.
  WaitingSubqueriesForWeak(Executing),
  /// Here, the `Executing` could contain either weak conjunctions, or strong conjunctions
  /// (if the JoinType is not Inner and there were only Strong Conjunctions present).
  WaitingSubqueriesFinal(Vec<Vec<TableView>>, Executing),
  Finished(Vec<TableView>),
}

#[derive(Debug)]
struct JoinNodeEvalData {
  /// There are as many elements here as there are `ContextRow`s in the parent `Context`,
  /// and the values will span all `ContextRow`s in this `context`.
  parent_to_child_context_map: Vec<usize>,
  /// The `Context` for this child node.
  context: Rc<Context>,
  result_state: ResultState,
}

#[derive(Debug)]
struct JoinStage {
  stage_idx: usize,
  maybe_subqueries_executing: Option<Executing>,
  result_map: BTreeMap<JoinNodeId, JoinNodeEvalData>,
  join_node_schema: BTreeMap<JoinNodeId, Vec<GeneralColumnRef>>,

  /// There are as many elements here as there are `ContextRow`s in the parent `Context`,
  /// and the values will span all `ContextRow`s in this `context`.
  parent_to_child_context_map: Vec<usize>,
  /// The `Context` for this stage.
  context: Rc<Context>,
}

#[derive(Debug)]
enum GRExecutionS {
  Start,
  ReadStage(ReadStage),
  JoinStage(JoinStage),
  Done,
}

#[derive(Debug)]
pub struct GRQueryES {
  /// This is only here so it can be forwarded to child queries.
  pub root_query_path: CQueryPath,
  /// This is only here so it can be forwarded to child queries.
  pub timestamp: Timestamp,
  pub context: Rc<Context>,

  /// The elements of the outer Vec corresponds to every ContextRow in the
  /// `context`. The elements of the inner vec corresponds to the elements in
  /// `trans_table_views`. The `usize` indexes into an element in the corresponding
  /// `Vec<TableView>` inside the `trans_table_views`.
  pub new_trans_table_context: Vec<Vec<usize>>,

  /// The QueryId of this `GRQueryES`.
  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::GRQuery,
  pub query_plan: QueryPlan,

  // The dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub trans_table_views: Vec<(TransTableName, Vec<TableView>)>,
  state: GRExecutionS,

  /// This holds the path to the parent ES.
  pub orig_p: OrigP,
}

/// Ideally, we should create a create an auxiliary struct to cache `schema` and `instances`
/// so that they don't constantly have to be looked up. However, we would need to access `prefix`
/// here at the Server level, so we avoid doing this for now.
impl TransTableSource for GRQueryES {
  fn get_instance(&self, trans_table_name: &TransTableName, idx: usize) -> &TableView {
    lookup(&self.trans_table_views, trans_table_name).unwrap().get(idx).unwrap()
  }

  fn get_schema(&self, trans_table_name: &TransTableName) -> &Vec<Option<ColName>> {
    lookup(&self.sql_query.trans_tables, trans_table_name).unwrap().schema()
  }
}

// -----------------------------------------------------------------------------------------------
//  GRQuery Constructor
// -----------------------------------------------------------------------------------------------
// This is a convenient View Container we use to help create GRQuerys.

/// This is a trait for getting all of the Subqueries in a SQL
/// query (e.g. SuperSimpleSelect, Update).
pub trait SubqueryComputableSql {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery>;
}

impl SubqueryComputableSql for proc::TableSelect {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    let mut top_level_cols_set = Vec::<proc::GRQuery>::new();
    QueryIterator::new_top_level()
      .iterate_table_select(&mut gr_query_collecting_cb(&mut top_level_cols_set), self);
    top_level_cols_set
  }
}

impl SubqueryComputableSql for proc::TransTableSelect {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    let mut top_level_cols_set = Vec::<proc::GRQuery>::new();
    QueryIterator::new_top_level()
      .iterate_trans_table_select(&mut gr_query_collecting_cb(&mut top_level_cols_set), self);
    top_level_cols_set
  }
}

impl SubqueryComputableSql for proc::Update {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    let mut top_level_cols_set = Vec::<proc::GRQuery>::new();
    QueryIterator::new_top_level()
      .iterate_update(&mut gr_query_collecting_cb(&mut top_level_cols_set), self);
    top_level_cols_set
  }
}

impl SubqueryComputableSql for proc::Delete {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    let mut top_level_cols_set = Vec::<proc::GRQuery>::new();
    QueryIterator::new_top_level()
      .iterate_delete(&mut gr_query_collecting_cb(&mut top_level_cols_set), self);
    top_level_cols_set
  }
}

pub struct GRQueryConstructorView<'a, SqlQueryT: SubqueryComputableSql> {
  pub root_query_path: &'a CQueryPath,
  pub timestamp: &'a Timestamp,
  /// SQL query containing by the parent ES.
  pub sql_query: &'a SqlQueryT,
  /// QueryPlan of the parent ES
  pub query_plan: &'a QueryPlan,
  /// QueryId of the parent ES
  pub query_id: &'a QueryId,
  /// The parent ES's Context
  pub context: &'a Context,
}

impl<'a, SqlQueryT: SubqueryComputableSql> GRQueryConstructorView<'a, SqlQueryT> {
  pub fn mk_gr_query_es(
    &self,
    gr_query_id: QueryId,
    context: Rc<Context>,
    subquery_idx: usize,
  ) -> GRQueryES {
    // Filter the TransTables in the QueryPlan based on the TransTables available for this subquery.
    let new_trans_table_context = (0..context.context_rows.len()).map(|_| Vec::new()).collect();
    // Finally, construct the GRQueryES.
    GRQueryES {
      root_query_path: self.root_query_path.clone(),
      timestamp: self.timestamp.clone(),
      context,
      new_trans_table_context,
      query_id: gr_query_id,
      sql_query: self.sql_query.collect_subqueries().remove(subquery_idx),
      query_plan: self.query_plan.clone(),
      new_rms: Default::default(),
      trans_table_views: vec![],
      state: GRExecutionS::Start,
      orig_p: OrigP::new(self.query_id.clone()),
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

pub enum TransTableIdx {
  External(usize),
  Local(usize),
}

impl GRQueryES {
  /// Starts the GRQueryES from its initial state.
  pub fn start<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) -> GRQueryAction {
    self.advance(ctx, io_ctx)
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    tm_qid: QueryId,
    new_rms: BTreeSet<TQueryPath>,
    pre_agg_table_views: Vec<TableView>,
  ) -> GRQueryAction {
    let read_stage = cast!(GRExecutionS::ReadStage, &mut self.state).unwrap();
    assert_eq!(&read_stage.stage_query_id, &tm_qid);

    // Combine the results into a single one
    let (trans_table_name, stage) = self.sql_query.trans_tables.get(read_stage.stage_idx).unwrap();
    let table_views = match match stage {
      proc::GRQueryStage::TableSelect(sql_query) => {
        perform_aggregation(sql_query, pre_agg_table_views)
      }
      proc::GRQueryStage::TransTableSelect(sql_query) => {
        perform_aggregation(sql_query, pre_agg_table_views)
      }
      proc::GRQueryStage::JoinSelect(_) => {
        debug_assert!(false);
        return GRQueryAction::Wait;
      }
    } {
      Ok(result) => result,
      Err(eval_error) => {
        return GRQueryAction::QueryError(msg::QueryError::RuntimeError {
          msg: format!("Aggregation of GRQueryES failed with error {:?}", eval_error),
        });
      }
    };

    // Amend the `new_trans_table_context`
    for i in 0..self.context.context_rows.len() {
      let idx = read_stage.parent_to_child_context_map.get(i).unwrap();
      self.new_trans_table_context.get_mut(i).unwrap().push(*idx);
    }

    // Accumulate in the `new_rms`.
    self.new_rms.extend(new_rms);

    // Add the `table_views` to the GRQueryES and advance it.
    self.trans_table_views.push((trans_table_name.clone(), table_views));
    self.advance(ctx, io_ctx)
  }

  /// This is called when the TMStatus has completed unsuccessfully.
  pub fn handle_tm_aborted<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    _: &mut Ctx,
    _: &mut IO,
    aborted_data: msg::AbortedData,
  ) -> GRQueryAction {
    match aborted_data {
      msg::AbortedData::QueryError(query_error) => {
        // In the case of a QueryError, we just propagate it up.
        self.state = GRExecutionS::Done;
        GRQueryAction::QueryError(query_error)
      }
    }
  }

  /// This is called when one of the remote node's Leadership changes beyond the
  /// LeadershipId that we had sent a PerformQuery to.
  pub fn handle_tm_remote_leadership_changed<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) -> GRQueryAction {
    let read_stage = cast!(GRExecutionS::ReadStage, &self.state).unwrap();
    self.process_gr_query_stage(ctx, io_ctx, read_stage.stage_idx)
  }

  /// This Exits and Cleans up this GRQueryES.
  pub fn exit_and_clean_up<Ctx: CTServerContext>(&mut self, _: &mut Ctx) {
    self.state = GRExecutionS::Done;
  }

  /// We call this when a JoinNode enters the `Finished` stage.
  fn join_node_finished<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    this_id: JoinNodeId,
  ) -> Result<GRQueryAction, EvalError> {
    let join_stage = cast!(GRExecutionS::JoinStage, &mut self.state).unwrap();
    let (_, stage) = self.sql_query.trans_tables.get(join_stage.stage_idx).unwrap();
    let join_select = cast!(proc::GRQueryStage::JoinSelect, stage).unwrap();

    // Handle the case it is not the root.
    if !this_id.is_empty() {
      let (parent_id, last_char) = {
        let mut this_id = this_id.clone();
        let mut chars = this_id.chars();
        let last_char = chars.next_back().unwrap();
        let parent_id = chars.as_str().to_string();
        (parent_id, last_char)
      };

      let sibling_id = {
        let mut sibling_id = parent_id.clone();
        if last_char == 'L' {
          sibling_id.push('R');
        } else {
          sibling_id.push('L');
        }
        sibling_id
      };

      if let Some(eval_data) = join_stage.result_map.get_mut(&sibling_id) {
        // We only take action if the sibling is also finished.
        if let ResultState::Finished(_) = &eval_data.result_state {
          // Lookup the parent JoinNode
          let parent_node = lookup_join_node(&join_select.from, parent_id.clone());
          let parent_inner = cast!(proc::JoinNode::JoinInnerNode, parent_node).unwrap();

          // Collect all GRQuerys in the conjunctions.
          let (weak_gr_queries, strong_gr_queries) = extract_subqueries(&parent_inner);

          let left_id = parent_id.clone() + "L";
          let right_id = parent_id.clone() + "R";

          if weak_gr_queries.is_empty() && strong_gr_queries.is_empty() {
            // Get the EvalData of this, the sibling, and the parent.
            let left_eval_data = join_stage.result_map.remove(&left_id).unwrap();
            let right_eval_data = join_stage.result_map.remove(&right_id).unwrap();
            let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();

            // Get the schemas of the children and the target schema of the parent.
            let left_schema = join_stage.join_node_schema.get(&left_id).unwrap();
            let right_schema = join_stage.join_node_schema.get(&right_id).unwrap();
            let parent_schema = join_stage.join_node_schema.get(&parent_id).unwrap();

            // Consider the case that there are no dependencies.
            if !join_select.dependency_graph.contains_key(&left_id)
              && !join_select.dependency_graph.contains_key(&right_id)
            {
              // Here, there are no dependencies.
              fn do_independent_join(
                first_eval_data: JoinNodeEvalData,
                second_eval_data: JoinNodeEvalData,
                parent_eval_data: &mut JoinNodeEvalData,
                first_schema: &Vec<GeneralColumnRef>,
                second_schema: &Vec<GeneralColumnRef>,
                parent_schema: &Vec<GeneralColumnRef>,
                parent_inner: &proc::JoinInnerNode,
                first_outer: bool,
                second_outer: bool,
              ) -> Result<(), EvalError> {
                let make_parent_row = |first_row: &Vec<ColValN>,
                                       second_row: &Vec<ColValN>|
                 -> Vec<ColValN> {
                  make_parent_row(first_schema, first_row, second_schema, second_row, parent_schema)
                };

                let first_table_views =
                  cast!(ResultState::Finished, first_eval_data.result_state).unwrap();
                let second_table_views =
                  cast!(ResultState::Finished, second_eval_data.result_state).unwrap();

                let mut finished_table_views = Vec::<TableView>::new();

                // Iterate over parent context rows.
                let column_context_schema =
                  &parent_eval_data.context.context_schema.column_context_schema;

                for (i, context_row) in parent_eval_data.context.context_rows.iter().enumerate() {
                  let column_context_row = &context_row.column_context_row;

                  // Get the child `TableView`s corresponding to the context_row.
                  let first_table_view = first_table_views
                    .get(*first_eval_data.parent_to_child_context_map.get(i).unwrap())
                    .unwrap();
                  let second_table_view = second_table_views
                    .get(*second_eval_data.parent_to_child_context_map.get(i).unwrap())
                    .unwrap();

                  // Start constructing the `col_map` (needed for evaluating the conjunctions).
                  let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
                  add_vals(&mut col_map, column_context_schema, column_context_row);

                  // Initialiate the TableView we would add to the final joined result.
                  let mut table_view = TableView::new();

                  // Used hold all rows on the Second Side that were unrejected by weak conjunctions.
                  let mut second_side_weak_unrejected = BTreeSet::<Vec<ColValN>>::new();

                  // Iterate over `this` table
                  for (first_row, first_count) in &first_table_view.rows {
                    let first_count = *first_count;
                    add_vals_general(&mut col_map, first_schema, first_row);

                    let mut all_weak_rejected = true;

                    // Iterate over `sibling` table.
                    'second: for (second_row, second_count) in &second_table_view.rows {
                      let second_count = *second_count;
                      add_vals_general(&mut col_map, second_schema, second_row);

                      // Evaluate the weak conjunctions. If one of them evaluates to false, we skip
                      // this pair of rows.
                      for expr in &parent_inner.weak_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &vec![],
                          &mut 0,
                        )?)?)? {
                          continue 'second;
                        }
                      }

                      // Here, we mark that at least one Right side succeeded was not
                      // rejected due to weak conjunctions.
                      all_weak_rejected = false;
                      second_side_weak_unrejected.insert(second_row.clone());

                      // Evaluate the strong conjunctions. If one of them evaluates to false, we skip
                      // this pair of rows.
                      for expr in &parent_inner.strong_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &vec![],
                          &mut 0,
                        )?)?)? {
                          continue 'second;
                        }
                      }

                      // Add the row to the TableView.
                      let joined_row = make_parent_row(first_row, second_row);
                      table_view.add_row_multi(joined_row, first_count * second_count);
                    }

                    // Here, we check whether we need to manufacture a row
                    if all_weak_rejected && first_outer {
                      // construct an artificial right row
                      let mut second_row = Vec::<ColValN>::new();
                      second_row.resize(second_schema.len(), None);
                      let mut second_count = 1;

                      // Most of the below is now copy-pasted from the above, except that we do
                      // not filter with weak conjunctions.
                      add_vals_general(&mut col_map, second_schema, &second_row);

                      // Evaluate the strong conjunctions. If one of them evaluates to false, we skip
                      // this pair of rows.
                      for expr in &parent_inner.strong_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &vec![],
                          &mut 0,
                        )?)?)? {
                          break;
                        }
                      }

                      // Add the row to the TableView.
                      let joined_row = make_parent_row(first_row, &second_row);
                      table_view.add_row_multi(joined_row, first_count * second_count);
                    }
                  }

                  // Re-add rows on the Second Side if need be
                  if second_outer {
                    // construct an artificial right row
                    let mut first_row = Vec::<ColValN>::new();
                    first_row.resize(first_schema.len(), None);
                    let mut first_count = 1;

                    add_vals_general(&mut col_map, first_schema, &first_row);

                    'second: for (second_row, second_count) in &second_table_view.rows {
                      // Check that the `second_row` was always rejected by weak conjunctions.
                      if !second_side_weak_unrejected.contains(second_row) {
                        // Most of the below is now copy-pasted from the above, except that we do
                        // not filter with weak conjunctions.
                        let second_count = *second_count;
                        add_vals_general(&mut col_map, second_schema, &second_row);

                        // Evaluate the strong conjunctions. If one of them evaluates to false, we skip
                        // this pair of rows.
                        for expr in &parent_inner.strong_conjunctions {
                          if !is_true(&evaluate_c_expr(&construct_cexpr(
                            expr,
                            &col_map,
                            &vec![],
                            &mut 0,
                          )?)?)? {
                            continue 'second;
                          }
                        }

                        // Add the row to the TableView.
                        let joined_row = make_parent_row(&first_row, second_row);
                        table_view.add_row_multi(joined_row, first_count * second_count);
                      }
                    }
                  }

                  finished_table_views.push(table_view);
                }

                // Move the parent stage to the finished stage
                parent_eval_data.result_state = ResultState::Finished(finished_table_views);
                Ok(())
              }

              match parent_inner.join_type {
                iast::JoinType::Inner => {
                  do_independent_join(
                    left_eval_data,
                    right_eval_data,
                    parent_eval_data,
                    left_schema,
                    right_schema,
                    parent_schema,
                    parent_inner,
                    false,
                    false,
                  )?;
                }
                iast::JoinType::Left => {
                  do_independent_join(
                    left_eval_data,
                    right_eval_data,
                    parent_eval_data,
                    left_schema,
                    right_schema,
                    parent_schema,
                    parent_inner,
                    true,
                    false,
                  )?;
                }
                iast::JoinType::Right => {
                  do_independent_join(
                    right_eval_data,
                    left_eval_data,
                    parent_eval_data,
                    right_schema,
                    left_schema,
                    parent_schema,
                    parent_inner,
                    true,
                    false,
                  )?;
                }
                iast::JoinType::Outer => {
                  do_independent_join(
                    left_eval_data,
                    right_eval_data,
                    parent_eval_data,
                    left_schema,
                    right_schema,
                    parent_schema,
                    parent_inner,
                    true,
                    true,
                  )?;
                }
              };
            } else {
              // There is a dependency.
              fn do_dependent_join(
                first_eval_data: JoinNodeEvalData,
                second_eval_data: JoinNodeEvalData,
                parent_eval_data: &mut JoinNodeEvalData,
                first_schema: &Vec<GeneralColumnRef>,
                second_schema: &Vec<GeneralColumnRef>,
                parent_schema: &Vec<GeneralColumnRef>,
                parent_inner: &proc::JoinInnerNode,
              ) -> Result<(), EvalError> {
                let make_parent_row = |first_row: &Vec<ColValN>,
                                       second_row: &Vec<ColValN>|
                 -> Vec<ColValN> {
                  make_parent_row(first_schema, first_row, second_schema, second_row, parent_schema)
                };

                let first_table_views =
                  cast!(ResultState::Finished, first_eval_data.result_state).unwrap();
                let second_table_views =
                  cast!(ResultState::Finished, second_eval_data.result_state).unwrap();

                let mut finished_table_views = Vec::<TableView>::new();

                // Iterate over parent context rows.
                let column_context_schema =
                  &parent_eval_data.context.context_schema.column_context_schema;

                let mut second_side_count = 0;
                for (i, context_row) in parent_eval_data.context.context_rows.iter().enumerate() {
                  let column_context_row = &context_row.column_context_row;

                  // Get the child `TableView`s corresponding to the context_row.
                  let first_table_view = first_table_views
                    .get(*first_eval_data.parent_to_child_context_map.get(i).unwrap())
                    .unwrap();

                  // Start constructing the `col_map` (needed for evaluating the conjunctions).
                  let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
                  add_vals(&mut col_map, column_context_schema, column_context_row);

                  // Initialiate the TableView we would add to the final joined result.
                  let mut table_view = TableView::new();

                  // Iterate over `this` table
                  for (first_row, first_count) in &first_table_view.rows {
                    let first_count = *first_count;
                    add_vals_general(&mut col_map, first_schema, first_row);

                    let mut all_weak_rejected = true;

                    // Get the second side;
                    let second_table_view = second_table_views
                      .get(
                        *second_eval_data
                          .parent_to_child_context_map
                          .get(second_side_count)
                          .unwrap(),
                      )
                      .unwrap();
                    second_side_count += 1;

                    // Iterate over `sibling` table.
                    'second: for (second_row, second_count) in &second_table_view.rows {
                      let second_count = *second_count;
                      add_vals_general(&mut col_map, second_schema, second_row);

                      // Evaluate the weak conjunctions. If one of them evaluates to false, we skip
                      // this pair of rows.
                      for expr in &parent_inner.weak_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &vec![],
                          &mut 0,
                        )?)?)? {
                          continue 'second;
                        }
                      }

                      // Here, we mark that at least one Right side succeeded was not
                      // rejected due to weak conjunctions.
                      all_weak_rejected = false;

                      // Evaluate the strong conjunctions. If one of them evaluates to false, we skip
                      // this pair of rows.
                      for expr in &parent_inner.strong_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &vec![],
                          &mut 0,
                        )?)?)? {
                          continue 'second;
                        }
                      }

                      // Add the row to the TableView.
                      let joined_row = make_parent_row(first_row, second_row);
                      table_view.add_row_multi(joined_row, first_count * second_count);
                    }

                    // Here, we check whether we need to manufacture a row
                    if all_weak_rejected {
                      // construct an artificial right row
                      let mut second_row = Vec::<ColValN>::new();
                      second_row.resize(second_schema.len(), None);
                      let mut second_count = 1;

                      // Most of the below is now copy-pasted from the above, except that we do
                      // not filter with weak conjunctions.
                      add_vals_general(&mut col_map, second_schema, &second_row);

                      // Evaluate the strong conjunctions. If one of them evaluates to false, we skip
                      // this pair of rows.
                      for expr in &parent_inner.strong_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &vec![],
                          &mut 0,
                        )?)?)? {
                          break;
                        }
                      }

                      // Add the row to the TableView.
                      let joined_row = make_parent_row(first_row, &second_row);
                      table_view.add_row_multi(joined_row, first_count * second_count);
                    }
                  }

                  finished_table_views.push(table_view);
                }

                // Move the parent stage to the finished stage
                parent_eval_data.result_state = ResultState::Finished(finished_table_views);
                Ok(())
              }

              match parent_inner.join_type {
                iast::JoinType::Inner => {
                  // Handle the the direction of the dependency.
                  if join_select.dependency_graph.contains(&right_id) {
                    do_dependent_join(
                      left_eval_data,
                      right_eval_data,
                      parent_eval_data,
                      left_schema,
                      right_schema,
                      parent_schema,
                      parent_inner,
                    )?;
                  } else {
                    do_dependent_join(
                      right_eval_data,
                      left_eval_data,
                      parent_eval_data,
                      right_schema,
                      left_schema,
                      parent_schema,
                      parent_inner,
                    )?;
                  }
                }
                iast::JoinType::Left => {
                  do_dependent_join(
                    left_eval_data,
                    right_eval_data,
                    parent_eval_data,
                    left_schema,
                    right_schema,
                    parent_schema,
                    parent_inner,
                  )?;
                }
                iast::JoinType::Right => {
                  do_dependent_join(
                    right_eval_data,
                    left_eval_data,
                    parent_eval_data,
                    right_schema,
                    left_schema,
                    parent_schema,
                    parent_inner,
                  )?;
                }
                iast::JoinType::Outer => {
                  // An OUTER JOIN should never have a dependency between its children.
                  debug_assert!(false);
                }
              };
            }

            // No matter what, this node must be finished.
            self.join_node_finished(ctx, io_ctx, parent_id)
          } else {
            // Get the EvalData of this, the sibling, and the parent. Note that we remove
            // the parent completely in order to satisfy the borrow checker.
            let left_eval_data = join_stage.result_map.get(&left_id).unwrap();
            let right_eval_data = join_stage.result_map.get(&right_id).unwrap();
            let parent_eval_data = join_stage.result_map.get(&parent_id).unwrap();

            // Get the schemas of the children and the target schema of the parent.
            let left_schema = join_stage.join_node_schema.get(&left_id).unwrap();
            let right_schema = join_stage.join_node_schema.get(&right_id).unwrap();

            // Define a function to help initialize contexts and construct locations.
            let parent_context = &parent_eval_data.context;

            // Copy some variables to help make GRQueryES construction easier.
            let root_query_path = self.root_query_path.clone();
            let timestamp = self.timestamp.clone();
            let query_plan = self.query_plan.clone();
            let orig_p = OrigP::new(self.query_id.clone());
            let mk_gr_query_ess = |io_ctx: &mut IO,
                                   query_and_context: Vec<(proc::GRQuery, Context)>|
             -> Vec<GRQueryES> {
              // Construct the GRQuerESs.
              let mut gr_query_ess = Vec::<GRQueryES>::new();
              for (sql_query, context) in query_and_context {
                let context = Rc::new(context);
                let query_id = mk_qid(io_ctx.rand());

                // Filter the TransTables in the QueryPlan based on the TransTables
                // available for this subquery.
                let new_trans_table_context =
                  (0..context.context_rows.len()).map(|_| Vec::new()).collect();
                // Finally, construct the GRQueryES.
                gr_query_ess.push(GRQueryES {
                  root_query_path: root_query_path.clone(),
                  timestamp: timestamp.clone(),
                  context,
                  new_trans_table_context,
                  query_id,
                  sql_query,
                  query_plan: query_plan.clone(),
                  new_rms: Default::default(),
                  trans_table_views: vec![],
                  state: GRExecutionS::Start,
                  orig_p: orig_p.clone(),
                });
              }

              // Return that GRQueryESs.
              gr_query_ess
            };

            // Create a helper function.
            let mut send_gr_queries_indep = |io_ctx: &mut IO,
                                             first_eval_data: &JoinNodeEvalData,
                                             second_eval_data: &JoinNodeEvalData,
                                             parent_eval_data: &JoinNodeEvalData,
                                             first_schema: &Vec<GeneralColumnRef>,
                                             second_schema: &Vec<GeneralColumnRef>,
                                             gr_queries: Vec<proc::GRQuery>|
             -> Vec<GRQueryES> {
              let (mut contexts, all_locations) = initialize_contexts(
                &parent_context.context_schema,
                &first_schema,
                &second_schema,
                &gr_queries,
              );

              // Get TableViews
              let first_table_views =
                cast!(ResultState::Finished, &first_eval_data.result_state).unwrap();
              let second_table_views =
                cast!(ResultState::Finished, &second_eval_data.result_state).unwrap();

              // Sets to keep track of `ContextRow`s constructed for every corresponding
              // `Context` so that we do not add duplicate `ContextRow`s.
              let mut context_row_sets = Vec::<BTreeSet<ContextRow>>::new();
              context_row_sets.resize(contexts.len(), BTreeSet::new());

              // Iterate over parent context rows, then the child rows, and build out the
              // all GRQuery contexts.
              for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
                // Get the child `TableView`s corresponding to the context_row.
                let first_table_view = first_table_views
                  .get(*first_eval_data.parent_to_child_context_map.get(i).unwrap())
                  .unwrap();

                // Next, we iterate over rows of Left and Right sides, and we construct
                // a `ContextRow` for each GRQuery.
                for (first_row, _) in &first_table_view.rows {
                  let second_table_view = second_table_views
                    .get(*second_eval_data.parent_to_child_context_map.get(i).unwrap())
                    .unwrap();
                  for (second_row, _) in &second_table_view.rows {
                    for i in 0..gr_queries.len() {
                      let context = contexts.get_mut(i).unwrap();
                      let context_row_set = context_row_sets.get_mut(i).unwrap();
                      let context_row = mk_context_row(
                        all_locations.get(i).unwrap(),
                        parent_context_row,
                        first_row,
                        second_row,
                      );

                      // Add in the ContextRow if it has not been added yet.
                      if !context_row_set.contains(&context_row) {
                        context_row_set.insert(context_row.clone());
                        context.context_rows.push(context_row);
                      }
                    }
                  }
                }
              }

              // Construct the GRQuerESs.
              mk_gr_query_ess(io_ctx, gr_queries.into_iter().zip(contexts.into_iter()).collect())
            };

            // Handle dependencies.
            let mut send_gr_queries_dep = |io_ctx: &mut IO,
                                           first_eval_data: &JoinNodeEvalData,
                                           second_eval_data: &JoinNodeEvalData,
                                           parent_eval_data: &JoinNodeEvalData,
                                           first_schema: &Vec<GeneralColumnRef>,
                                           second_schema: &Vec<GeneralColumnRef>,
                                           gr_queries: Vec<proc::GRQuery>|
             -> Vec<GRQueryES> {
              let (mut contexts, all_locations) = initialize_contexts(
                &parent_context.context_schema,
                &first_schema,
                &second_schema,
                &gr_queries,
              );

              // Get TableViews
              let first_table_views =
                cast!(ResultState::Finished, &first_eval_data.result_state).unwrap();
              let second_table_views =
                cast!(ResultState::Finished, &second_eval_data.result_state).unwrap();

              // Sets to keep track of `ContextRow`s constructed for every corresponding
              // `Context` so that we do not add duplicate `ContextRow`s.
              let mut context_row_sets = Vec::<BTreeSet<ContextRow>>::new();
              context_row_sets.resize(contexts.len(), BTreeSet::new());

              // Iterate over parent context rows, then the child rows, and build out the
              // all GRQuery contexts.
              let mut second_side_count = 0;
              for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
                // Get the child `TableView`s corresponding to the context_row.
                let first_table_view = first_table_views
                  .get(*first_eval_data.parent_to_child_context_map.get(i).unwrap())
                  .unwrap();

                // Next, we iterate over rows of Left and Right sides, and we construct
                // a `ContextRow` for each GRQuery.
                for (first_row, _) in &first_table_view.rows {
                  let second_table_view = second_table_views
                    .get(
                      *second_eval_data.parent_to_child_context_map.get(second_side_count).unwrap(),
                    )
                    .unwrap();
                  second_side_count += 1;
                  for (second_row, _) in &second_table_view.rows {
                    for i in 0..gr_queries.len() {
                      let context = contexts.get_mut(i).unwrap();
                      let context_row_set = context_row_sets.get_mut(i).unwrap();
                      let context_row = mk_context_row(
                        all_locations.get(i).unwrap(),
                        parent_context_row,
                        first_row,
                        second_row,
                      );

                      // Add in the ContextRow if it has not been added yet.
                      if !context_row_set.contains(&context_row) {
                        context_row_set.insert(context_row.clone());
                        context.context_rows.push(context_row);
                      }
                    }
                  }
                }
              }

              // Construct the GRQuerESs.
              mk_gr_query_ess(io_ctx, gr_queries.into_iter().zip(contexts.into_iter()).collect())
            };

            if !weak_gr_queries.is_empty() {
              // Here there are some weak conjunctions. Consider the case
              // where there are no dependencies.
              if !join_select.dependency_graph.contains_key(&left_id)
                && !join_select.dependency_graph.contains_key(&right_id)
              {
                // Here, there are no dependencies.
                match parent_inner.join_type {
                  iast::JoinType::Inner => {
                    // For INNER Joins, we can start both weak and strong conjunctions immediately.
                    let gr_query_ess = send_gr_queries_indep(
                      io_ctx,
                      left_eval_data,
                      right_eval_data,
                      parent_eval_data,
                      left_schema,
                      right_schema,
                      weak_gr_queries.iter().chain(strong_gr_queries.iter()).cloned().collect(),
                    );

                    // Since both Weak conjunctions and Strong conjunctions are handled, we
                    // update the state to `WaitingSubqueriesFinal`.
                    let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
                    parent_eval_data.result_state = ResultState::WaitingSubqueriesFinal(
                      Vec::new(),
                      Executing::create(&gr_query_ess),
                    );

                    // Return that GRQueryESs.
                    Ok(GRQueryAction::SendSubqueries(gr_query_ess))
                  }
                  iast::JoinType::Left | iast::JoinType::Right | iast::JoinType::Outer => {
                    // For other types of Joins, we can start both weak and strong conjunctions
                    // we need to evaluate the weak conjunctions first.
                    let gr_query_ess = send_gr_queries_indep(
                      io_ctx,
                      left_eval_data,
                      right_eval_data,
                      parent_eval_data,
                      left_schema,
                      right_schema,
                      weak_gr_queries,
                    );

                    let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
                    if !strong_gr_queries.is_empty() {
                      // Since only Weak conjunctions are handled, we move to
                      // `WaitingSubqueriesForWeak` because there are strong conjunctions too.
                      parent_eval_data.result_state =
                        ResultState::WaitingSubqueriesForWeak(Executing::create(&gr_query_ess));
                    } else {
                      // Here, we can move it to the end.
                      parent_eval_data.result_state = ResultState::WaitingSubqueriesFinal(
                        Vec::new(),
                        Executing::create(&gr_query_ess),
                      );
                    }

                    // Return that GRQueryESs.
                    Ok(GRQueryAction::SendSubqueries(gr_query_ess))
                  }
                }
              } else {
                // Here, there are dependencies.
                match parent_inner.join_type {
                  iast::JoinType::Inner => {
                    let gr_query_ess = if join_select.dependency_graph.contains(&right_id) {
                      send_gr_queries_dep(
                        io_ctx,
                        left_eval_data,
                        right_eval_data,
                        parent_eval_data,
                        left_schema,
                        right_schema,
                        weak_gr_queries.iter().chain(strong_gr_queries.iter()).cloned().collect(),
                      )
                    } else {
                      send_gr_queries_dep(
                        io_ctx,
                        right_eval_data,
                        left_eval_data,
                        parent_eval_data,
                        right_schema,
                        left_schema,
                        weak_gr_queries.iter().chain(strong_gr_queries.iter()).cloned().collect(),
                      )
                    };

                    // Since both Weak conjunctions and Strong conjunctions are handled, we
                    // update the state to `WaitingSubqueriesFinal`.
                    let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
                    parent_eval_data.result_state = ResultState::WaitingSubqueriesFinal(
                      Vec::new(),
                      Executing::create(&gr_query_ess),
                    );

                    // Return that GRQueryESs.
                    Ok(GRQueryAction::SendSubqueries(gr_query_ess))
                  }
                  iast::JoinType::Left => {
                    let gr_query_ess = send_gr_queries_dep(
                      io_ctx,
                      left_eval_data,
                      right_eval_data,
                      parent_eval_data,
                      left_schema,
                      right_schema,
                      weak_gr_queries,
                    );

                    let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
                    if !strong_gr_queries.is_empty() {
                      // Since only Weak conjunctions are handled, we move to
                      // `WaitingSubqueriesForWeak` because there are strong conjunctions too.
                      parent_eval_data.result_state =
                        ResultState::WaitingSubqueriesForWeak(Executing::create(&gr_query_ess));
                    } else {
                      // Here, we can move it to the end.
                      parent_eval_data.result_state = ResultState::WaitingSubqueriesFinal(
                        Vec::new(),
                        Executing::create(&gr_query_ess),
                      );
                    }

                    // Return that GRQueryESs.
                    Ok(GRQueryAction::SendSubqueries(gr_query_ess))
                  }
                  iast::JoinType::Right => {
                    let gr_query_ess = send_gr_queries_dep(
                      io_ctx,
                      right_eval_data,
                      left_eval_data,
                      parent_eval_data,
                      right_schema,
                      left_schema,
                      weak_gr_queries,
                    );

                    let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
                    if !strong_gr_queries.is_empty() {
                      // Since only Weak conjunctions are handled, we move to
                      // `WaitingSubqueriesForWeak` because there are strong conjunctions too.
                      parent_eval_data.result_state =
                        ResultState::WaitingSubqueriesForWeak(Executing::create(&gr_query_ess));
                    } else {
                      // Here, we can move it to the end.
                      parent_eval_data.result_state = ResultState::WaitingSubqueriesFinal(
                        Vec::new(),
                        Executing::create(&gr_query_ess),
                      );
                    }

                    // Return that GRQueryESs.
                    Ok(GRQueryAction::SendSubqueries(gr_query_ess))
                  }
                  iast::JoinType::Outer => {
                    // An OUTER JOIN should never have a dependency between its children.
                    debug_assert!(false);
                    Ok(GRQueryAction::Wait)
                  }
                }
              }
            } else {
              // If there are no weak conjunctions, then we can act as if they
              // fully evaluated and move forward from there.
              self.advance_to_final_subqueries(ctx, io_ctx, parent_id, vec![])
            }
          }
        } else {
          // If the sibling is not finished, then we do nothing.
          Ok(GRQueryAction::Wait)
        }
      } else {
        // Here, the JoinNode corresponding to the `sibling_id` was waiting to for
        // `this_id` to finish because the former depends on the latter. Here, we
        // construct the context for the latter and then start evaluating.

        // Get the EvalData of this, the sibling, and the parent. Note that we remove
        // the parent completely in order to satisfy the borrow checker.
        let this_eval_data = join_stage.result_map.get(&this_id).unwrap();
        let parent_eval_data = join_stage.result_map.get(&parent_id).unwrap();

        // Get the schemas of the children and the target schema of the parent.
        let this_schema = join_stage.join_node_schema.get(&this_id).unwrap();

        // Get TableViews
        let this_table_views = cast!(ResultState::Finished, &this_eval_data.result_state).unwrap();

        // Construct stuff to use to compute context.
        let join_node = lookup_join_node(&join_select.from, sibling_id.clone());
        let elem = QueryElement::JoinNode(join_node);
        let (mut context, locations) = initialize_contexts_general_once(
          &parent_eval_data.context.context_schema,
          this_schema,
          &vec![],
          elem,
        );
        let mut context_row_map = BTreeMap::<ContextRow, usize>::new();
        let mut parent_to_child_context_map = Vec::<usize>::new();

        // Iterate over parent context rows, then the child rows, and build out the
        // all GRQuery contexts.
        for (i, parent_context_row) in parent_eval_data.context.context_rows.iter().enumerate() {
          // Get the child `TableView`s corresponding to the context_row.
          let this_table_view = this_table_views
            .get(*this_eval_data.parent_to_child_context_map.get(i).unwrap())
            .unwrap();

          // Next, we iterate over rows of Left and Right sides, and we construct
          // a `ContextRow` for each GRQuery.
          for (this_row, _) in &this_table_view.rows {
            let context_row = mk_context_row(&locations, parent_context_row, this_row, &vec![]);

            // Add in the ContextRow if it has not been added yet.
            if !context_row_map.contains(&context_row) {
              let index = context_row_map.len();
              context_row_map.insert(context_row.clone(), index);
              context.context_rows.push(context_row.clone());
            }

            parent_to_child_context_map.push(*context_row_map.get(&context_row).unwrap());
          }
        }

        // Build the EvalData for the sibling table
        let sibling_eval_data = JoinNodeEvalData {
          context: Rc::new(context),
          parent_to_child_context_map,
          result_state: ResultState::Waiting,
        };

        let mut queries = Vec::<(QueryId, Rc<Context>, proc::GRQuery)>::new();
        start_evaluating_join(
          ctx,
          io_ctx,
          &mut join_stage.result_map,
          &mut queries,
          &join_select.dependency_graph,
          sibling_id,
          &join_select.from,
          sibling_eval_data,
        );

        let mut gr_query_ess = Vec::<GRQueryES>::new();
        for (query_id, context, sql_query) in queries {
          // Filter the TransTables in the QueryPlan based on the TransTables
          // available for this subquery.
          let new_trans_table_context =
            (0..context.context_rows.len()).map(|_| Vec::new()).collect();
          // Finally, construct the GRQueryES.
          gr_query_ess.push(GRQueryES {
            root_query_path: self.root_query_path.clone(),
            timestamp: self.timestamp.clone(),
            context,
            new_trans_table_context,
            query_id,
            sql_query,
            query_plan: self.query_plan.clone(),
            new_rms: Default::default(),
            trans_table_views: vec![],
            state: GRExecutionS::Start,
            orig_p: OrigP::new(self.query_id.clone()),
          });
        }

        // This will not be empty because the leafs under `sibling_id`
        // will definitely produce GRQuerESs.
        debug_assert!(!gr_query_ess.is_empty());

        Ok(GRQueryAction::SendSubqueries(gr_query_ess))
      }
    } else {
      // Collect all subqueries for the SelectItems. Recall that JoinSelects
      // do not have a WHERE clause (since it is pushed into JoinNode).
      let mut it = QueryIterator::new_top_level();
      let mut gr_queries = Vec::<proc::GRQuery>::new();
      it.iterate_select_items(
        &mut gr_query_collecting_cb(&mut gr_queries),
        &join_select.projection,
      );

      if gr_queries.is_empty() {
        // Here, there are no subqueries to evaluate, so we can finish.
        self.finish_join_select(ctx, io_ctx, vec![])
      } else {
        // When the root is EvalData is finished, it should be the only element
        // in the `result_map`.
        debug_assert!(join_stage.result_map.len() == 1);
        let (_, eval_data) = join_stage.result_map.iter().next().unwrap();
        let schema = join_stage.join_node_schema.get("").unwrap();

        // Build children
        let (mut contexts, all_locations) =
          initialize_contexts(&join_stage.context.context_schema, schema, &vec![], &gr_queries);

        // Get TableViews
        let table_views = cast!(ResultState::Finished, &eval_data.result_state).unwrap();

        // Sets to keep track of `ContextRow`s constructed for every corresponding
        // `Context` so that we do not add duplicate `ContextRow`s.
        let mut context_row_sets = Vec::<BTreeSet<ContextRow>>::new();
        context_row_sets.resize(contexts.len(), BTreeSet::new());

        // Iterate over parent context rows, then the child rows, and build out the
        // all GRQuery contexts.
        for (i, parent_context_row) in self.context.context_rows.iter().enumerate() {
          // Get the child `TableView`s corresponding to the context_row.
          let table_view =
            table_views.get(*eval_data.parent_to_child_context_map.get(i).unwrap()).unwrap();

          // Next, we iterate over rows of Left and Right sides, and we construct
          // a `ContextRow` for each GRQuery.
          for (row, _) in &table_view.rows {
            for i in 0..gr_queries.len() {
              let context = contexts.get_mut(i).unwrap();
              let context_row_set = context_row_sets.get_mut(i).unwrap();
              let context_row =
                mk_context_row(all_locations.get(i).unwrap(), parent_context_row, row, &vec![]);

              // Add in the ContextRow if it has not been added yet.
              if !context_row_set.contains(&context_row) {
                context_row_set.insert(context_row.clone());
                context.context_rows.push(context_row);
              }
            }
          }
        }

        // Build subqueries.

        // Copy some variables to help make GRQueryES construction easier.
        let root_query_path = self.root_query_path.clone();
        let timestamp = self.timestamp.clone();
        let query_plan = self.query_plan.clone();
        let orig_p = OrigP::new(self.query_id.clone());
        let mk_gr_query_ess =
          |io_ctx: &mut IO, query_and_context: Vec<(proc::GRQuery, Context)>| -> Vec<GRQueryES> {
            // Construct the GRQuerESs.
            let mut gr_query_ess = Vec::<GRQueryES>::new();
            for (sql_query, context) in query_and_context {
              let context = Rc::new(context);
              let query_id = mk_qid(io_ctx.rand());

              // Filter the TransTables in the QueryPlan based on the TransTables
              // available for this subquery.
              let new_trans_table_context =
                (0..context.context_rows.len()).map(|_| Vec::new()).collect();
              // Finally, construct the GRQueryES.
              gr_query_ess.push(GRQueryES {
                root_query_path: root_query_path.clone(),
                timestamp: timestamp.clone(),
                context,
                new_trans_table_context,
                query_id,
                sql_query,
                query_plan: query_plan.clone(),
                new_rms: Default::default(),
                trans_table_views: vec![],
                state: GRExecutionS::Start,
                orig_p: orig_p.clone(),
              });
            }

            // Return that GRQueryESs.
            gr_query_ess
          };

        // Build GRQuerESs
        let gr_query_ess =
          mk_gr_query_ess(io_ctx, gr_queries.into_iter().zip(contexts.into_iter()).collect());

        // Update state
        join_stage.maybe_subqueries_executing = Some(Executing::create(&gr_query_ess));

        // Return the subqueries
        Ok(GRQueryAction::SendSubqueries(gr_query_ess))
      }
    }
  }

  fn advance_to_final_subqueries<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    _: &mut Ctx,
    io_ctx: &mut IO,
    parent_id: JoinNodeId,
    result: Vec<Vec<TableView>>,
  ) -> Result<GRQueryAction, EvalError> {
    // Dig into the state and get relevant data.
    let join_stage = cast!(GRExecutionS::JoinStage, &mut self.state).unwrap();
    let (_, stage) = self.sql_query.trans_tables.get(join_stage.stage_idx).unwrap();
    let join_select = cast!(proc::GRQueryStage::JoinSelect, stage).unwrap();
    let parent_node = lookup_join_node(&join_select.from, parent_id.clone());
    let parent_inner = cast!(proc::JoinNode::JoinInnerNode, parent_node).unwrap();
    let (weak_gr_queries, strong_gr_queries) = extract_subqueries(&parent_inner);

    // Construct the stuff, let is go.
    let left_id = parent_id.clone() + "L";
    let right_id = parent_id.clone() + "R";

    // Get the EvalData of this, the sibling, and the parent. Note that we remove
    // the parent completely in order to satisfy the borrow checker.
    let left_eval_data = join_stage.result_map.get(&left_id).unwrap();
    let right_eval_data = join_stage.result_map.get(&right_id).unwrap();
    let parent_eval_data = join_stage.result_map.get(&parent_id).unwrap();

    // Get the schemas of the children and the target schema of the parent.
    let left_schema = join_stage.join_node_schema.get(&left_id).unwrap();
    let right_schema = join_stage.join_node_schema.get(&right_id).unwrap();

    // Define a function to help initialize contexts and construct locations.
    let parent_context = &parent_eval_data.context;

    // Copy some variables to help make GRQueryES construction easier.
    let root_query_path = self.root_query_path.clone();
    let timestamp = self.timestamp.clone();
    let query_plan = self.query_plan.clone();
    let orig_p = OrigP::new(self.query_id.clone());
    let mk_gr_query_ess = |io_ctx: &mut IO,
                           query_and_context: Vec<(proc::GRQuery, Context)>|
     -> Vec<GRQueryES> {
      // Construct the GRQuerESs.
      let mut gr_query_ess = Vec::<GRQueryES>::new();
      for (sql_query, context) in query_and_context {
        let context = Rc::new(context);
        let query_id = mk_qid(io_ctx.rand());

        // Filter the TransTables in the QueryPlan based on the TransTables
        // available for this subquery.
        let new_trans_table_context = (0..context.context_rows.len()).map(|_| Vec::new()).collect();
        // Finally, construct the GRQueryES.
        gr_query_ess.push(GRQueryES {
          root_query_path: root_query_path.clone(),
          timestamp: timestamp.clone(),
          context,
          new_trans_table_context,
          query_id,
          sql_query,
          query_plan: query_plan.clone(),
          new_rms: Default::default(),
          trans_table_views: vec![],
          state: GRExecutionS::Start,
          orig_p: orig_p.clone(),
        });
      }

      // Return that GRQueryESs.
      gr_query_ess
    };

    // Handle the case that there is a dependency.
    if !join_select.dependency_graph.contains_key(&left_id)
      && !join_select.dependency_graph.contains_key(&right_id)
    {
      // There is a dependency.

      // Create a helper function.
      let mut send_gr_queries_indep = |io_ctx: &mut IO,
                                       first_eval_data: &JoinNodeEvalData,
                                       second_eval_data: &JoinNodeEvalData,
                                       parent_eval_data: &JoinNodeEvalData,
                                       first_schema: &Vec<GeneralColumnRef>,
                                       second_schema: &Vec<GeneralColumnRef>,
                                       weak_gr_queries: Vec<proc::GRQuery>,
                                       strong_gr_queries: Vec<proc::GRQuery>,
                                       result: &Vec<Vec<TableView>>,
                                       first_outer: bool,
                                       second_outer: bool|
       -> Result<Vec<GRQueryES>, EvalError> {
        // Get TableViews
        let first_table_views =
          cast!(ResultState::Finished, &first_eval_data.result_state).unwrap();
        let second_table_views =
          cast!(ResultState::Finished, &second_eval_data.result_state).unwrap();

        // We need to construct ContextRows for both the weak conjunctions (to look
        // up the ContextRow values, as well as
        let all_gr_queries: Vec<_> =
          weak_gr_queries.iter().chain(strong_gr_queries.iter()).cloned().collect();
        let (mut contexts, all_locations) = initialize_contexts(
          &parent_context.context_schema,
          &first_schema,
          &second_schema,
          &all_gr_queries,
        );

        // Sets to keep track of `ContextRow`s constructed for every corresponding
        // `Context` so that we do not add duplicate `ContextRow`s.
        let mut context_row_maps = Vec::<BTreeMap<ContextRow, usize>>::new();
        context_row_maps.resize(contexts.len(), BTreeMap::new());

        // Iterate over parent context rows.
        let parent_column_context_schema =
          &parent_eval_data.context.context_schema.column_context_schema;

        // Iterate over parent context rows, then the child rows, and build out the
        // all GRQuery contexts.
        for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
          let parent_column_context_row = &parent_context_row.column_context_row;

          // Get the child `TableView`s corresponding to the context_row.
          let first_table_view = first_table_views
            .get(*first_eval_data.parent_to_child_context_map.get(i).unwrap())
            .unwrap();
          let second_table_view = second_table_views
            .get(*second_eval_data.parent_to_child_context_map.get(i).unwrap())
            .unwrap();

          // Start constructing the `col_map` (needed for evaluating the conjunctions).
          let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
          add_vals(&mut col_map, parent_column_context_schema, parent_column_context_row);

          // Used hold all rows on the Second Side that were unrejected by weak conjunctions.
          let mut second_side_weak_unrejected = BTreeSet::<Vec<ColValN>>::new();

          // Next, we iterate over rows of Left and Right sides, and we construct
          // a `ContextRow` for each GRQuery.
          for (first_row, _) in &first_table_view.rows {
            add_vals_general(&mut col_map, first_schema, first_row);

            let mut all_weak_rejected = true;

            'second: for (second_row, _) in &second_table_view.rows {
              add_vals_general(&mut col_map, second_schema, second_row);

              let mut raw_subquery_vals = Vec::<TableView>::new();
              for i in 0..weak_gr_queries.len() {
                let context_row_map = context_row_maps.get_mut(i).unwrap();
                let context_row = mk_context_row(
                  all_locations.get(i).unwrap(),
                  parent_context_row,
                  first_row,
                  second_row,
                );

                // Add in the ContextRow if it has not been added yet.
                if !context_row_map.contains(&context_row) {
                  let index = context_row_map.len();
                  context_row_map.insert(context_row.clone(), index);
                }

                // Lookup the raw subquery value for this weak conjunction and
                // add it to raw_subquery_vals
                let index = context_row_map.get(&context_row).unwrap();
                let cur_result = result.get(i).unwrap();
                let table_view = cur_result.get(*index).unwrap();
                raw_subquery_vals.push(table_view.clone());
              }

              // Evaluate the weak conjunctions. If one of them evaluates to false,
              // we skip this pair of rows.
              let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
              let mut next_subquery_idx = 0;
              for expr in &parent_inner.weak_conjunctions {
                if !is_true(&evaluate_c_expr(&construct_cexpr(
                  expr,
                  &col_map,
                  &subquery_vals,
                  &mut next_subquery_idx,
                )?)?)? {
                  // This means that the weak join rejects
                  continue 'second;
                }
              }

              // Here, we mark that at least one Right side succeeded was not
              // rejected due to weak conjunctions.
              all_weak_rejected = false;
              second_side_weak_unrejected.insert(second_row.clone());

              // Here, we go ahead and construct a ContextRow for all strong
              // conjunctions.
              for i in weak_gr_queries.len()..all_gr_queries.len() {
                let context = contexts.get_mut(i).unwrap();
                let context_row_map = context_row_maps.get_mut(i).unwrap();
                let context_row = mk_context_row(
                  all_locations.get(i).unwrap(),
                  parent_context_row,
                  first_row,
                  second_row,
                );

                // Add in the ContextRow if it has not been added yet.
                if !context_row_map.contains(&context_row) {
                  let index = context_row_map.len();
                  context_row_map.insert(context_row.clone(), index);
                  context.context_rows.push(context_row);
                }
              }
            }

            // Here, we check whether we need to manufacture a row
            if all_weak_rejected && first_outer {
              // construct an artificial right row
              let mut second_row = Vec::<ColValN>::new();
              second_row.resize(second_schema.len(), None);

              // Most of the below is now copy-pasted from the above, except that
              // we do not filter with weak conjunctions.
              add_vals_general(&mut col_map, second_schema, &second_row);

              // Add in the manufactuored row for every Strong Conjunction.
              for i in weak_gr_queries.len()..all_gr_queries.len() {
                let context = contexts.get_mut(i).unwrap();
                let context_row_map = context_row_maps.get_mut(i).unwrap();
                let context_row = mk_context_row(
                  all_locations.get(i).unwrap(),
                  parent_context_row,
                  first_row,
                  &second_row,
                );

                // Add in the ContextRow if it has not been added yet.
                if !context_row_map.contains(&context_row) {
                  let index = context_row_map.len();
                  context_row_map.insert(context_row.clone(), index);
                  context.context_rows.push(context_row);
                }
              }
            }
          }

          // Re-add rows on the Second Side if need be
          if second_outer {
            // construct an artificial right row
            let mut first_row = Vec::<ColValN>::new();
            first_row.resize(first_schema.len(), None);

            add_vals_general(&mut col_map, first_schema, &first_row);

            for (second_row, _) in &second_table_view.rows {
              // Check that the `second_row` was always rejected by weak conjunctions.
              if !second_side_weak_unrejected.contains(second_row) {
                // Most of the below is now copy-pasted from the above, except that we do
                // not filter with weak conjunctions.
                add_vals_general(&mut col_map, second_schema, &second_row);

                // Add in the manufactuored row for every Strong Conjunction.
                for i in weak_gr_queries.len()..all_gr_queries.len() {
                  let context = contexts.get_mut(i).unwrap();
                  let context_row_map = context_row_maps.get_mut(i).unwrap();
                  let context_row = mk_context_row(
                    all_locations.get(i).unwrap(),
                    parent_context_row,
                    &first_row,
                    second_row,
                  );

                  // Add in the ContextRow if it has not been added yet.
                  if !context_row_map.contains(&context_row) {
                    let index = context_row_map.len();
                    context_row_map.insert(context_row.clone(), index);
                    context.context_rows.push(context_row);
                  }
                }
              }
            }
          }
        }

        // Trim off the first part of `contexts` because those are irrelevant.
        let contexts = contexts.into_iter().skip(weak_gr_queries.len()).into_iter();

        // Construct the GRQuerESs.
        Ok(mk_gr_query_ess(
          io_ctx,
          strong_gr_queries.into_iter().zip(contexts.into_iter().into_iter()).collect(),
        ))
      };

      match &parent_inner.join_type {
        iast::JoinType::Inner => {
          // Inner JOINs should never be in the `WaitingSubqueriesForWeak` state.
          debug_assert!(false);
          Ok(GRQueryAction::Wait)
        }
        iast::JoinType::Left => {
          // Make GRQuerESs
          let gr_query_ess = send_gr_queries_indep(
            io_ctx,
            left_eval_data,
            right_eval_data,
            parent_eval_data,
            left_schema,
            right_schema,
            weak_gr_queries,
            strong_gr_queries,
            &result,
            true,
            false,
          )?;

          // We move to `WaitingSubqueriesFinal`.
          let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
          parent_eval_data.result_state =
            ResultState::WaitingSubqueriesFinal(result, Executing::create(&gr_query_ess));

          // Return that GRQueryESs.
          Ok(GRQueryAction::SendSubqueries(gr_query_ess))
        }
        iast::JoinType::Right => {
          // Make GRQuerESs. We make the Right side to be the first, since this
          // is needed for row re-addition.
          let gr_query_ess = send_gr_queries_indep(
            io_ctx,
            right_eval_data,
            left_eval_data,
            parent_eval_data,
            right_schema,
            left_schema,
            weak_gr_queries,
            strong_gr_queries,
            &result,
            true,
            false,
          )?;

          // We move to `WaitingSubqueriesFinal`.
          let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
          parent_eval_data.result_state =
            ResultState::WaitingSubqueriesFinal(result, Executing::create(&gr_query_ess));

          // Return that GRQueryESs.
          Ok(GRQueryAction::SendSubqueries(gr_query_ess))
        }
        iast::JoinType::Outer => {
          // Make GRQuerESs
          let gr_query_ess = send_gr_queries_indep(
            io_ctx,
            left_eval_data,
            right_eval_data,
            parent_eval_data,
            left_schema,
            right_schema,
            weak_gr_queries,
            strong_gr_queries,
            &result,
            true,
            true,
          )?;

          // We move to `WaitingSubqueriesFinal`.
          let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
          parent_eval_data.result_state =
            ResultState::WaitingSubqueriesFinal(result, Executing::create(&gr_query_ess));

          // Return that GRQueryESs.
          Ok(GRQueryAction::SendSubqueries(gr_query_ess))
        }
      }
    } else {
      // Handle the case of dependencies existing.

      // Create a helper function.
      let mut send_gr_queries_dep = |io_ctx: &mut IO,
                                     first_eval_data: &JoinNodeEvalData,
                                     second_eval_data: &JoinNodeEvalData,
                                     parent_eval_data: &JoinNodeEvalData,
                                     first_schema: &Vec<GeneralColumnRef>,
                                     second_schema: &Vec<GeneralColumnRef>,
                                     weak_gr_queries: Vec<proc::GRQuery>,
                                     strong_gr_queries: Vec<proc::GRQuery>,
                                     result: &Vec<Vec<TableView>>|
       -> Result<Vec<GRQueryES>, EvalError> {
        // Get TableViews
        let first_table_views =
          cast!(ResultState::Finished, &first_eval_data.result_state).unwrap();
        let second_table_views =
          cast!(ResultState::Finished, &second_eval_data.result_state).unwrap();

        // We need to construct ContextRows for both the weak conjunctions (to look
        // up the ContextRow values, as well as
        let all_gr_queries: Vec<_> =
          weak_gr_queries.iter().chain(strong_gr_queries.iter()).cloned().collect();
        let (mut contexts, all_locations) = initialize_contexts(
          &parent_context.context_schema,
          &first_schema,
          &second_schema,
          &all_gr_queries,
        );

        // Sets to keep track of `ContextRow`s constructed for every corresponding
        // `Context` so that we do not add duplicate `ContextRow`s.
        let mut context_row_maps = Vec::<BTreeMap<ContextRow, usize>>::new();
        context_row_maps.resize(contexts.len(), BTreeMap::new());

        // Iterate over parent context rows.
        let parent_column_context_schema =
          &parent_eval_data.context.context_schema.column_context_schema;

        // Iterate over parent context rows, then the child rows, and build out the
        // all GRQuery contexts.
        let mut second_side_count = 0;
        for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
          let parent_column_context_row = &parent_context_row.column_context_row;

          // Get the child `TableView`s corresponding to the context_row.
          let first_table_view = first_table_views
            .get(*first_eval_data.parent_to_child_context_map.get(i).unwrap())
            .unwrap();

          // Start constructing the `col_map` (needed for evaluating the conjunctions).
          let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
          add_vals(&mut col_map, parent_column_context_schema, parent_column_context_row);

          // Next, we iterate over rows of Left and Right sides, and we construct
          // a `ContextRow` for each GRQuery.
          for (first_row, _) in &first_table_view.rows {
            add_vals_general(&mut col_map, first_schema, first_row);

            let mut all_weak_rejected = true;

            // Get the second side;
            let second_table_view = second_table_views
              .get(*second_eval_data.parent_to_child_context_map.get(second_side_count).unwrap())
              .unwrap();
            second_side_count += 1;

            'second: for (second_row, _) in &second_table_view.rows {
              add_vals_general(&mut col_map, second_schema, second_row);

              let mut raw_subquery_vals = Vec::<TableView>::new();
              for i in 0..weak_gr_queries.len() {
                let context_row_map = context_row_maps.get_mut(i).unwrap();
                let context_row = mk_context_row(
                  all_locations.get(i).unwrap(),
                  parent_context_row,
                  first_row,
                  second_row,
                );

                // Add in the ContextRow if it has not been added yet.
                if !context_row_map.contains(&context_row) {
                  let index = context_row_map.len();
                  context_row_map.insert(context_row.clone(), index);
                }

                // Lookup the raw subquery value for this weak conjunction and
                // add it to raw_subquery_vals
                let index = context_row_map.get(&context_row).unwrap();
                let cur_result = result.get(i).unwrap();
                let table_view = cur_result.get(*index).unwrap();
                raw_subquery_vals.push(table_view.clone());
              }

              // Evaluate the weak conjunctions. If one of them evaluates to false,
              // we skip this pair of rows.
              let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
              let mut next_subquery_idx = 0;
              for expr in &parent_inner.weak_conjunctions {
                if !is_true(&evaluate_c_expr(&construct_cexpr(
                  expr,
                  &col_map,
                  &subquery_vals,
                  &mut next_subquery_idx,
                )?)?)? {
                  // This means that the weak join rejects
                  continue 'second;
                }
              }

              // Here, we mark that at least one Right side succeeded was not
              // rejected due to weak conjunctions.
              all_weak_rejected = false;

              // Here, we go ahead and construct a ContextRow for all strong
              // conjunctions.
              for i in weak_gr_queries.len()..all_gr_queries.len() {
                let context = contexts.get_mut(i).unwrap();
                let context_row_map = context_row_maps.get_mut(i).unwrap();
                let context_row = mk_context_row(
                  all_locations.get(i).unwrap(),
                  parent_context_row,
                  first_row,
                  second_row,
                );

                // Add in the ContextRow if it has not been added yet.
                if !context_row_map.contains(&context_row) {
                  let index = context_row_map.len();
                  context_row_map.insert(context_row.clone(), index);
                  context.context_rows.push(context_row);
                }
              }
            }

            // Here, we check whether we need to manufacture a row
            if all_weak_rejected {
              // construct an artificial right row
              let mut second_row = Vec::<ColValN>::new();
              second_row.resize(second_schema.len(), None);

              // Most of the below is now copy-pasted from the above, except that
              // we do not filter with weak conjunctions.
              add_vals_general(&mut col_map, second_schema, &second_row);

              // Add in the manufactuored row for every Strong Conjunction.
              for i in weak_gr_queries.len()..all_gr_queries.len() {
                let context = contexts.get_mut(i).unwrap();
                let context_row_map = context_row_maps.get_mut(i).unwrap();
                let context_row = mk_context_row(
                  all_locations.get(i).unwrap(),
                  parent_context_row,
                  first_row,
                  &second_row,
                );

                // Add in the ContextRow if it has not been added yet.
                if !context_row_map.contains(&context_row) {
                  let index = context_row_map.len();
                  context_row_map.insert(context_row.clone(), index);
                  context.context_rows.push(context_row);
                }
              }
            }
          }
        }

        // Trim off the first part of `contexts` because those are irrelevant.
        let contexts = contexts.into_iter().skip(weak_gr_queries.len()).into_iter();

        // Construct the GRQuerESs.
        Ok(mk_gr_query_ess(
          io_ctx,
          strong_gr_queries.into_iter().zip(contexts.into_iter().into_iter()).collect(),
        ))
      };

      match &parent_inner.join_type {
        iast::JoinType::Inner => {
          // Inner JOINs should never be in the `WaitingSubqueriesForWeak` state.
          debug_assert!(false);
          Ok(GRQueryAction::Wait)
        }
        iast::JoinType::Left => {
          // Make GRQuerESs
          let gr_query_ess = send_gr_queries_dep(
            io_ctx,
            left_eval_data,
            right_eval_data,
            parent_eval_data,
            left_schema,
            right_schema,
            weak_gr_queries,
            strong_gr_queries,
            &result,
          )?;

          // We move to `WaitingSubqueriesFinal`.
          let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
          parent_eval_data.result_state =
            ResultState::WaitingSubqueriesFinal(result, Executing::create(&gr_query_ess));

          // Return that GRQueryESs.
          Ok(GRQueryAction::SendSubqueries(gr_query_ess))
        }
        iast::JoinType::Right => {
          // Make GRQuerESs. We make the Right side to be the first, since this
          // is needed for row re-addition.
          let gr_query_ess = send_gr_queries_dep(
            io_ctx,
            right_eval_data,
            left_eval_data,
            parent_eval_data,
            right_schema,
            left_schema,
            weak_gr_queries,
            strong_gr_queries,
            &result,
          )?;

          // We move to `WaitingSubqueriesFinal`.
          let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
          parent_eval_data.result_state =
            ResultState::WaitingSubqueriesFinal(result, Executing::create(&gr_query_ess));

          // Return that GRQueryESs.
          Ok(GRQueryAction::SendSubqueries(gr_query_ess))
        }
        iast::JoinType::Outer => {
          // Inner JOINs should never be in the `WaitingSubqueriesForWeak` state.
          debug_assert!(false);
          Ok(GRQueryAction::Wait)
        }
      }
    }
  }

  pub fn handle_gr_query_success<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    qid: QueryId,
    result: Vec<TableView>,
    rms: BTreeSet<TQueryPath>,
  ) -> GRQueryAction {
    match self.handle_gr_query_success_internal(ctx, io_ctx, qid, result, rms) {
      Ok(action) => action,
      Err(eval_error) => GRQueryAction::QueryError(msg::QueryError::RuntimeError {
        msg: format!("{:?}", eval_error),
      }),
    }
  }

  fn handle_gr_query_success_internal<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    qid: QueryId,
    result: Vec<TableView>,
    rms: BTreeSet<TQueryPath>,
  ) -> Result<GRQueryAction, EvalError> {
    self.new_rms.extend(rms);

    let QueryId(qid_str) = &qid;
    let mut it = Option::<usize>::None;
    for (index, cur_char) in qid_str.chars().enumerate() {
      if cur_char == '_' {
        it = Some(index);
      }
    }

    // This should be a JoinStage if we are getting GRQueryESs results.
    let join_stage = cast!(GRExecutionS::JoinStage, &mut self.state).unwrap();
    // Determine what to do based on the state of the sibling
    let (_, stage) = self.sql_query.trans_tables.get(join_stage.stage_idx).unwrap();
    let join_select = cast!(proc::GRQueryStage::JoinSelect, stage).unwrap();

    // This means the `qid` is destined for somewhere in the Join Tree
    if let Some(i) = it {
      let parent_id: String = qid_str.chars().take(i).collect();

      // Lookup this JoinNode
      let parent_node = lookup_join_node(&join_select.from, parent_id.clone());
      let parent_inner = cast!(proc::JoinNode::JoinInnerNode, parent_node).unwrap();

      // Collect all GRQuerys in the conjunctions.
      let (weak_gr_queries, strong_gr_queries) = extract_subqueries(&parent_inner);

      // Handle the case
      let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
      match &mut parent_eval_data.result_state {
        ResultState::Waiting => {
          // This means `parent_id` must correspond to a JoinLeaf, and we just got the
          // result of the JoinLeaf's GRQueryES.
          parent_eval_data.result_state = ResultState::Finished(result);
          self.join_node_finished(ctx, io_ctx, parent_id)
        }
        ResultState::WaitingSubqueriesForWeak(executing) => {
          executing.add_subquery_result(qid, result);
          // If this is complete, then we continue
          if executing.is_complete() {
            let (_, result) = std::mem::take(executing).get_results();
            self.advance_to_final_subqueries(ctx, io_ctx, parent_id, result)
          } else {
            // Here, we are waiting for more subquery results to come.
            Ok(GRQueryAction::Wait)
          }
        }
        ResultState::WaitingSubqueriesFinal(results, executing) => {
          executing.add_subquery_result(qid, result);
          if executing.is_complete() {
            let (_, second_results) = std::mem::take(executing).get_results();
            results.extend(second_results.into_iter());
            let result = std::mem::take(results);

            // Construct the stuff, let is go.
            let left_id = parent_id.clone() + "L";
            let right_id = parent_id.clone() + "R";

            // Get the EvalData of this, the sibling, and the parent. Note that we remove
            // the parent completely in order to satisfy the borrow checker.
            let left_eval_data = join_stage.result_map.remove(&left_id).unwrap();
            let right_eval_data = join_stage.result_map.remove(&right_id).unwrap();
            let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();

            // Get the schemas of the children and the target schema of the parent.
            let left_schema = join_stage.join_node_schema.get(&left_id).unwrap();
            let right_schema = join_stage.join_node_schema.get(&right_id).unwrap();
            let parent_schema = join_stage.join_node_schema.get(&parent_id).unwrap();

            // Handle the case that there is a dependency.
            if !join_select.dependency_graph.contains_key(&left_id)
              && !join_select.dependency_graph.contains_key(&right_id)
            {
              // There is a dependency.

              // Create a helper function.
              let mut send_gr_queries_indep = |io_ctx: &mut IO,
                                               first_eval_data: JoinNodeEvalData,
                                               second_eval_data: JoinNodeEvalData,
                                               parent_eval_data: &mut JoinNodeEvalData,
                                               first_schema: &Vec<GeneralColumnRef>,
                                               second_schema: &Vec<GeneralColumnRef>,
                                               parent_schema: &Vec<GeneralColumnRef>,
                                               weak_gr_queries: Vec<proc::GRQuery>,
                                               strong_gr_queries: Vec<proc::GRQuery>,
                                               result: Vec<Vec<TableView>>,
                                               first_outer: bool,
                                               second_outer: bool|
               -> Result<(), EvalError> {
                let make_parent_row = |first_row: &Vec<ColValN>,
                                       second_row: &Vec<ColValN>|
                 -> Vec<ColValN> {
                  make_parent_row(first_schema, first_row, second_schema, second_row, parent_schema)
                };

                // Define a function to help initialize contexts and construct locations.
                let parent_context = &parent_eval_data.context;

                // Get TableViews
                let first_table_views =
                  cast!(ResultState::Finished, first_eval_data.result_state).unwrap();
                let second_table_views =
                  cast!(ResultState::Finished, second_eval_data.result_state).unwrap();

                let mut finished_table_views = Vec::<TableView>::new();

                // We need to construct ContextRows for both the weak conjunctions (to look
                // up the ContextRow values, as well as
                let all_gr_queries: Vec<_> =
                  weak_gr_queries.iter().chain(strong_gr_queries.iter()).cloned().collect();
                let (mut contexts, all_locations) = initialize_contexts(
                  &parent_context.context_schema,
                  &first_schema,
                  &second_schema,
                  &all_gr_queries,
                );

                // Sets to keep track of `ContextRow`s constructed for every corresponding
                // `Context` so that we do not add duplicate `ContextRow`s.
                let mut context_row_maps = Vec::<BTreeMap<ContextRow, usize>>::new();
                context_row_maps.resize(contexts.len(), BTreeMap::new());

                // Iterate over parent context rows.
                let parent_column_context_schema =
                  &parent_eval_data.context.context_schema.column_context_schema;

                // Iterate over parent context rows, then the child rows, and build out the
                // all GRQuery contexts.
                for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
                  let parent_column_context_row = &parent_context_row.column_context_row;

                  // Get the child `TableView`s corresponding to the context_row.
                  let first_table_view = first_table_views
                    .get(*first_eval_data.parent_to_child_context_map.get(i).unwrap())
                    .unwrap();
                  let second_table_view = second_table_views
                    .get(*second_eval_data.parent_to_child_context_map.get(i).unwrap())
                    .unwrap();

                  // Start constructing the `col_map` (needed for evaluating the conjunctions).
                  let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
                  add_vals(&mut col_map, parent_column_context_schema, parent_column_context_row);

                  // Initialiate the TableView we would add to the final joined result.
                  let mut table_view = TableView::new();

                  // Used hold all rows on the Second Side that were unrejected by weak conjunctions.
                  let mut second_side_weak_unrejected = BTreeSet::<Vec<ColValN>>::new();

                  // Next, we iterate over rows of Left and Right sides, and we construct
                  // a `ContextRow` for each GRQuery.
                  for (first_row, first_count) in &first_table_view.rows {
                    let first_count = *first_count;
                    add_vals_general(&mut col_map, first_schema, first_row);

                    let mut all_weak_rejected = true;

                    'second: for (second_row, second_count) in &second_table_view.rows {
                      let second_count = *second_count;
                      add_vals_general(&mut col_map, second_schema, second_row);

                      let mut raw_subquery_vals = Vec::<TableView>::new();
                      for i in 0..weak_gr_queries.len() {
                        let context_row_map = context_row_maps.get_mut(i).unwrap();
                        let context_row = mk_context_row(
                          all_locations.get(i).unwrap(),
                          parent_context_row,
                          first_row,
                          second_row,
                        );

                        // Add in the ContextRow if it has not been added yet.
                        if !context_row_map.contains(&context_row) {
                          let index = context_row_map.len();
                          context_row_map.insert(context_row.clone(), index);
                        }

                        // Lookup the raw subquery value for this weak conjunction and
                        // add it to raw_subquery_vals
                        let index = context_row_map.get(&context_row).unwrap();
                        let cur_result = result.get(i).unwrap();
                        let table_view = cur_result.get(*index).unwrap();
                        raw_subquery_vals.push(table_view.clone());
                      }

                      // Evaluate the weak conjunctions. If one of them evaluates to false,
                      // we skip this pair of rows.
                      let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
                      let mut next_subquery_idx = 0;
                      for expr in &parent_inner.weak_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &subquery_vals,
                          &mut next_subquery_idx,
                        )?)?)? {
                          // This means that the weak join rejects
                          continue 'second;
                        }
                      }

                      // Here, we mark that at least one Right side succeeded was not
                      // rejected due to weak conjunctions.
                      all_weak_rejected = false;
                      second_side_weak_unrejected.insert(second_row.clone());

                      // Here, we go ahead and construct a ContextRow for all strong
                      // conjunctions.
                      let mut raw_subquery_vals = Vec::<TableView>::new();
                      for i in weak_gr_queries.len()..all_gr_queries.len() {
                        let context_row_map = context_row_maps.get_mut(i).unwrap();
                        let context_row = mk_context_row(
                          all_locations.get(i).unwrap(),
                          parent_context_row,
                          first_row,
                          second_row,
                        );

                        // Add in the ContextRow if it has not been added yet.
                        if !context_row_map.contains(&context_row) {
                          let index = context_row_map.len();
                          context_row_map.insert(context_row.clone(), index);
                        }

                        // add it to raw_subquery_vals
                        let index = context_row_map.get(&context_row).unwrap();
                        let cur_result = result.get(i).unwrap();
                        let table_view = cur_result.get(*index).unwrap();
                        raw_subquery_vals.push(table_view.clone());
                      }

                      // Evaluate the strong conjunctions. If one of them evaluates to false,
                      // we skip this pair of rows.
                      let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
                      let mut next_subquery_idx = 0;
                      for expr in &parent_inner.strong_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &subquery_vals,
                          &mut next_subquery_idx,
                        )?)?)? {
                          // This means that the weak join rejects
                          continue 'second;
                        }
                      }

                      // Add the row to the TableView.
                      let joined_row = make_parent_row(first_row, &second_row);
                      table_view.add_row_multi(joined_row, first_count * second_count);
                    }

                    // Here, we check whether we need to manufacture a row
                    if all_weak_rejected && first_outer {
                      // construct an artificial right row
                      let mut second_row = Vec::<ColValN>::new();
                      second_row.resize(second_schema.len(), None);
                      let mut second_count = 1;

                      // Most of the below is now copy-pasted from the above, except that
                      // we do not filter with weak conjunctions.
                      add_vals_general(&mut col_map, second_schema, &second_row);

                      // Add in the manufactuored row for every Strong Conjunction.
                      let mut raw_subquery_vals = Vec::<TableView>::new();
                      for i in weak_gr_queries.len()..all_gr_queries.len() {
                        let context_row_map = context_row_maps.get_mut(i).unwrap();
                        let context_row = mk_context_row(
                          all_locations.get(i).unwrap(),
                          parent_context_row,
                          first_row,
                          &second_row,
                        );

                        // Add in the ContextRow if it has not been added yet.
                        if !context_row_map.contains(&context_row) {
                          let index = context_row_map.len();
                          context_row_map.insert(context_row.clone(), index);
                        }

                        // add it to raw_subquery_vals
                        let index = context_row_map.get(&context_row).unwrap();
                        let cur_result = result.get(i).unwrap();
                        let table_view = cur_result.get(*index).unwrap();
                        raw_subquery_vals.push(table_view.clone());
                      }

                      // Evaluate the strong conjunctions. If one of them evaluates to false,
                      // we skip this pair of rows.
                      let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
                      let mut next_subquery_idx = 0;
                      for expr in &parent_inner.strong_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &subquery_vals,
                          &mut next_subquery_idx,
                        )?)?)? {
                          break;
                        }
                      }

                      // Add the row to the TableView.
                      let joined_row = make_parent_row(first_row, &second_row);
                      table_view.add_row_multi(joined_row, first_count * second_count);
                    }
                  }

                  // Re-add rows on the Second Side if need be
                  if second_outer {
                    // construct an artificial right row
                    let mut first_row = Vec::<ColValN>::new();
                    first_row.resize(first_schema.len(), None);
                    let first_count = 1;

                    add_vals_general(&mut col_map, first_schema, &first_row);

                    for (second_row, second_count) in &second_table_view.rows {
                      let second_count = *second_count;
                      // Check that the `second_row` was always rejected by weak conjunctions.
                      if !second_side_weak_unrejected.contains(second_row) {
                        // Most of the below is now copy-pasted from the above, except that we do
                        // not filter with weak conjunctions.
                        add_vals_general(&mut col_map, second_schema, &second_row);

                        // Add in the manufactuored row for every Strong Conjunction.
                        let mut raw_subquery_vals = Vec::<TableView>::new();
                        for i in weak_gr_queries.len()..all_gr_queries.len() {
                          let context_row_map = context_row_maps.get_mut(i).unwrap();
                          let context_row = mk_context_row(
                            all_locations.get(i).unwrap(),
                            parent_context_row,
                            &first_row,
                            second_row,
                          );

                          // Add in the ContextRow if it has not been added yet.
                          if !context_row_map.contains(&context_row) {
                            let index = context_row_map.len();
                            context_row_map.insert(context_row.clone(), index);
                          }

                          // add it to raw_subquery_vals
                          let index = context_row_map.get(&context_row).unwrap();
                          let cur_result = result.get(i).unwrap();
                          let table_view = cur_result.get(*index).unwrap();
                          raw_subquery_vals.push(table_view.clone());
                        }

                        // Evaluate the strong conjunctions. If one of them evaluates to false,
                        // we skip this pair of rows.
                        let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
                        let mut next_subquery_idx = 0;
                        for expr in &parent_inner.strong_conjunctions {
                          if !is_true(&evaluate_c_expr(&construct_cexpr(
                            expr,
                            &col_map,
                            &subquery_vals,
                            &mut next_subquery_idx,
                          )?)?)? {
                            break;
                          }
                        }

                        // Add the row to the TableView.
                        let joined_row = make_parent_row(&first_row, second_row);
                        table_view.add_row_multi(joined_row, first_count * second_count);
                      }
                    }
                  }

                  finished_table_views.push(table_view);
                }

                // Move the parent stage to the finished stage
                parent_eval_data.result_state = ResultState::Finished(finished_table_views);
                Ok(())
              };

              match &parent_inner.join_type {
                iast::JoinType::Inner => {
                  send_gr_queries_indep(
                    io_ctx,
                    left_eval_data,
                    right_eval_data,
                    parent_eval_data,
                    left_schema,
                    right_schema,
                    parent_schema,
                    weak_gr_queries,
                    strong_gr_queries,
                    result,
                    false,
                    false,
                  )?;
                }
                iast::JoinType::Left => {
                  send_gr_queries_indep(
                    io_ctx,
                    left_eval_data,
                    right_eval_data,
                    parent_eval_data,
                    left_schema,
                    right_schema,
                    parent_schema,
                    weak_gr_queries,
                    strong_gr_queries,
                    result,
                    true,
                    false,
                  )?;
                }
                iast::JoinType::Right => {
                  send_gr_queries_indep(
                    io_ctx,
                    right_eval_data,
                    left_eval_data,
                    parent_eval_data,
                    right_schema,
                    left_schema,
                    parent_schema,
                    weak_gr_queries,
                    strong_gr_queries,
                    result,
                    true,
                    false,
                  )?;
                }
                iast::JoinType::Outer => {
                  send_gr_queries_indep(
                    io_ctx,
                    left_eval_data,
                    right_eval_data,
                    parent_eval_data,
                    left_schema,
                    right_schema,
                    parent_schema,
                    weak_gr_queries,
                    strong_gr_queries,
                    result,
                    true,
                    true,
                  )?;
                }
              }
            } else {
              // Handle the case of dependencies existing.

              // Create a helper function.
              let mut send_gr_queries_dep = |io_ctx: &mut IO,
                                             first_eval_data: JoinNodeEvalData,
                                             second_eval_data: JoinNodeEvalData,
                                             parent_eval_data: &mut JoinNodeEvalData,
                                             first_schema: &Vec<GeneralColumnRef>,
                                             second_schema: &Vec<GeneralColumnRef>,
                                             parent_schema: &Vec<GeneralColumnRef>,
                                             weak_gr_queries: Vec<proc::GRQuery>,
                                             strong_gr_queries: Vec<proc::GRQuery>,
                                             result: Vec<Vec<TableView>>|
               -> Result<(), EvalError> {
                let make_parent_row = |first_row: &Vec<ColValN>,
                                       second_row: &Vec<ColValN>|
                 -> Vec<ColValN> {
                  make_parent_row(first_schema, first_row, second_schema, second_row, parent_schema)
                };

                // Define a function to help initialize contexts and construct locations.
                let parent_context = &parent_eval_data.context;

                // Get TableViews
                let first_table_views =
                  cast!(ResultState::Finished, first_eval_data.result_state).unwrap();
                let second_table_views =
                  cast!(ResultState::Finished, second_eval_data.result_state).unwrap();

                let mut finished_table_views = Vec::<TableView>::new();

                // We need to construct ContextRows for both the weak conjunctions (to look
                // up the ContextRow values, as well as
                let all_gr_queries: Vec<_> =
                  weak_gr_queries.iter().chain(strong_gr_queries.iter()).cloned().collect();
                let (mut contexts, all_locations) = initialize_contexts(
                  &parent_context.context_schema,
                  &first_schema,
                  &second_schema,
                  &all_gr_queries,
                );

                // Sets to keep track of `ContextRow`s constructed for every corresponding
                // `Context` so that we do not add duplicate `ContextRow`s.
                let mut context_row_maps = Vec::<BTreeMap<ContextRow, usize>>::new();
                context_row_maps.resize(contexts.len(), BTreeMap::new());

                // Iterate over parent context rows.
                let parent_column_context_schema =
                  &parent_eval_data.context.context_schema.column_context_schema;

                // Iterate over parent context rows, then the child rows, and build out the
                // all GRQuery contexts.
                let mut second_side_count = 0;
                for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
                  let parent_column_context_row = &parent_context_row.column_context_row;

                  // Get the child `TableView`s corresponding to the context_row.
                  let first_table_view = first_table_views
                    .get(*first_eval_data.parent_to_child_context_map.get(i).unwrap())
                    .unwrap();

                  // Start constructing the `col_map` (needed for evaluating the conjunctions).
                  let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
                  add_vals(&mut col_map, parent_column_context_schema, parent_column_context_row);

                  // Initialiate the TableView we would add to the final joined result.
                  let mut table_view = TableView::new();

                  // Next, we iterate over rows of Left and Right sides, and we construct
                  // a `ContextRow` for each GRQuery.
                  for (first_row, first_count) in &first_table_view.rows {
                    let first_count = *first_count;
                    add_vals_general(&mut col_map, first_schema, first_row);

                    let mut all_weak_rejected = true;

                    // Get the second side;
                    let second_table_view = second_table_views
                      .get(
                        *second_eval_data
                          .parent_to_child_context_map
                          .get(second_side_count)
                          .unwrap(),
                      )
                      .unwrap();
                    second_side_count += 1;

                    'second: for (second_row, second_count) in &second_table_view.rows {
                      let second_count = *second_count;
                      add_vals_general(&mut col_map, second_schema, second_row);

                      let mut raw_subquery_vals = Vec::<TableView>::new();
                      for i in 0..weak_gr_queries.len() {
                        let context_row_map = context_row_maps.get_mut(i).unwrap();
                        let context_row = mk_context_row(
                          all_locations.get(i).unwrap(),
                          parent_context_row,
                          first_row,
                          second_row,
                        );

                        // Add in the ContextRow if it has not been added yet.
                        if !context_row_map.contains(&context_row) {
                          let index = context_row_map.len();
                          context_row_map.insert(context_row.clone(), index);
                        }

                        // Lookup the raw subquery value for this weak conjunction and
                        // add it to raw_subquery_vals
                        let index = context_row_map.get(&context_row).unwrap();
                        let cur_result = result.get(i).unwrap();
                        let table_view = cur_result.get(*index).unwrap();
                        raw_subquery_vals.push(table_view.clone());
                      }

                      // Evaluate the weak conjunctions. If one of them evaluates to false,
                      // we skip this pair of rows.
                      let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
                      let mut next_subquery_idx = 0;
                      for expr in &parent_inner.weak_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &subquery_vals,
                          &mut next_subquery_idx,
                        )?)?)? {
                          // This means that the weak join rejects
                          continue 'second;
                        }
                      }

                      // Here, we mark that at least one Right side succeeded was not
                      // rejected due to weak conjunctions.
                      all_weak_rejected = false;

                      // Here, we go ahead and construct a ContextRow for all strong
                      // conjunctions.
                      let mut raw_subquery_vals = Vec::<TableView>::new();
                      for i in weak_gr_queries.len()..all_gr_queries.len() {
                        let context_row_map = context_row_maps.get_mut(i).unwrap();
                        let context_row = mk_context_row(
                          all_locations.get(i).unwrap(),
                          parent_context_row,
                          first_row,
                          second_row,
                        );

                        // Add in the ContextRow if it has not been added yet.
                        if !context_row_map.contains(&context_row) {
                          let index = context_row_map.len();
                          context_row_map.insert(context_row.clone(), index);
                        }

                        // add it to raw_subquery_vals
                        let index = context_row_map.get(&context_row).unwrap();
                        let cur_result = result.get(i).unwrap();
                        let table_view = cur_result.get(*index).unwrap();
                        raw_subquery_vals.push(table_view.clone());
                      }

                      // Evaluate the strong conjunctions. If one of them evaluates to false,
                      // we skip this pair of rows.
                      let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
                      let mut next_subquery_idx = 0;
                      for expr in &parent_inner.strong_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &subquery_vals,
                          &mut next_subquery_idx,
                        )?)?)? {
                          // This means that the weak join rejects
                          continue 'second;
                        }
                      }

                      // Add the row to the TableView.
                      let joined_row = make_parent_row(first_row, &second_row);
                      table_view.add_row_multi(joined_row, first_count * second_count);
                    }

                    // Here, we check whether we need to manufacture a row
                    if all_weak_rejected {
                      // construct an artificial right row
                      let mut second_row = Vec::<ColValN>::new();
                      second_row.resize(second_schema.len(), None);
                      let mut second_count = 1;

                      // Most of the below is now copy-pasted from the above, except that
                      // we do not filter with weak conjunctions.
                      add_vals_general(&mut col_map, second_schema, &second_row);

                      // Add in the manufactuored row for every Strong Conjunction.
                      let mut raw_subquery_vals = Vec::<TableView>::new();
                      for i in weak_gr_queries.len()..all_gr_queries.len() {
                        let context_row_map = context_row_maps.get_mut(i).unwrap();
                        let context_row = mk_context_row(
                          all_locations.get(i).unwrap(),
                          parent_context_row,
                          first_row,
                          &second_row,
                        );

                        // Add in the ContextRow if it has not been added yet.
                        if !context_row_map.contains(&context_row) {
                          let index = context_row_map.len();
                          context_row_map.insert(context_row.clone(), index);
                        }

                        // add it to raw_subquery_vals
                        let index = context_row_map.get(&context_row).unwrap();
                        let cur_result = result.get(i).unwrap();
                        let table_view = cur_result.get(*index).unwrap();
                        raw_subquery_vals.push(table_view.clone());
                      }

                      // Evaluate the strong conjunctions. If one of them evaluates to false,
                      // we skip this pair of rows.
                      let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
                      let mut next_subquery_idx = 0;
                      for expr in &parent_inner.strong_conjunctions {
                        if !is_true(&evaluate_c_expr(&construct_cexpr(
                          expr,
                          &col_map,
                          &subquery_vals,
                          &mut next_subquery_idx,
                        )?)?)? {
                          break;
                        }
                      }

                      // Add the row to the TableView.
                      let joined_row = make_parent_row(first_row, &second_row);
                      table_view.add_row_multi(joined_row, first_count * second_count);
                    }
                  }

                  finished_table_views.push(table_view);
                }

                // Move the parent stage to the finished stage
                parent_eval_data.result_state = ResultState::Finished(finished_table_views);
                Ok(())
              };

              match &parent_inner.join_type {
                iast::JoinType::Inner => {
                  if join_select.dependency_graph.contains(&right_id) {
                    send_gr_queries_dep(
                      io_ctx,
                      left_eval_data,
                      right_eval_data,
                      parent_eval_data,
                      left_schema,
                      right_schema,
                      parent_schema,
                      weak_gr_queries,
                      strong_gr_queries,
                      result,
                    )?;
                  } else {
                    send_gr_queries_dep(
                      io_ctx,
                      right_eval_data,
                      left_eval_data,
                      parent_eval_data,
                      right_schema,
                      left_schema,
                      parent_schema,
                      weak_gr_queries,
                      strong_gr_queries,
                      result,
                    )?;
                  }
                }
                iast::JoinType::Left => {
                  send_gr_queries_dep(
                    io_ctx,
                    left_eval_data,
                    right_eval_data,
                    parent_eval_data,
                    left_schema,
                    right_schema,
                    parent_schema,
                    weak_gr_queries,
                    strong_gr_queries,
                    result,
                  )?;
                }
                iast::JoinType::Right => {
                  send_gr_queries_dep(
                    io_ctx,
                    right_eval_data,
                    left_eval_data,
                    parent_eval_data,
                    right_schema,
                    left_schema,
                    parent_schema,
                    weak_gr_queries,
                    strong_gr_queries,
                    result,
                  )?;
                }
                iast::JoinType::Outer => {
                  // Inner JOINs should never be in the `WaitingSubqueriesFinal` state.
                  debug_assert!(false);
                }
              }
            }

            // No matter what, this node must be finished.
            self.join_node_finished(ctx, io_ctx, parent_id)
          } else {
            // Here, we are waiting for more subquery results to come.
            Ok(GRQueryAction::Wait)
          }
        }
        ResultState::Finished(_) => {
          // We should not get a subquery result if the EvalData is already finished.
          debug_assert!(false);
          Ok(GRQueryAction::Wait)
        }
      }
    } else {
      // Here, forward the subquery results to the `maybe_subqueries_executing` at the top
      // level, and finish the query if it is done.
      let executing = join_stage.maybe_subqueries_executing.as_mut().unwrap();
      executing.add_subquery_result(qid, result);
      if executing.is_complete() {
        let (_, result) = std::mem::take(executing).get_results();
        self.finish_join_select(ctx, io_ctx, result)
      } else {
        // Otherwise, keep waiting
        Ok(GRQueryAction::Wait)
      }
    }
  }

  fn finish_join_select<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    result: Vec<Vec<TableView>>,
  ) -> Result<GRQueryAction, EvalError> {
    // This should be a JoinStage if we are getting GRQueryESs results.
    let join_stage = cast!(GRExecutionS::JoinStage, &mut self.state).unwrap();
    // Determine what to do based on the state of the sibling
    let (trans_table_name, stage) = self.sql_query.trans_tables.get(join_stage.stage_idx).unwrap();
    let join_select = cast!(proc::GRQueryStage::JoinSelect, stage).unwrap();

    // Same as that. Rebuild context, but use a map instead, and in the outer parent
    // iteration, initialize TableView for result, and in inner iteration, evaluate
    // the join_selects select_items and amend the TableView accorigly.

    // Collect GRQueries for top level
    let mut it = QueryIterator::new_top_level();
    let mut gr_queries = Vec::<proc::GRQuery>::new();
    it.iterate_select_items(&mut gr_query_collecting_cb(&mut gr_queries), &join_select.projection);

    // When the root is EvalData is finished, it should be the only element
    // in the `result_map`.
    debug_assert!(join_stage.result_map.len() == 1);
    let (_, eval_data) = join_stage.result_map.iter().next().unwrap();
    let schema = join_stage.join_node_schema.get("").unwrap();

    // Build children
    let (mut contexts, all_locations) =
      initialize_contexts(&join_stage.context.context_schema, schema, &vec![], &gr_queries);

    // Get TableViews
    let table_views = cast!(ResultState::Finished, &eval_data.result_state).unwrap();

    // The final resulting TableViews.
    let mut pre_agg_table_views = Vec::<TableView>::new();

    // Sets to keep track of `ContextRow`s constructed for every corresponding
    // `Context` so that we do not add duplicate `ContextRow`s.
    let mut context_row_maps = Vec::<BTreeMap<ContextRow, usize>>::new();
    context_row_maps.resize(contexts.len(), BTreeMap::new());

    // Iterate over parent context rows, then the child rows, and build out the
    // all GRQuery contexts.
    let parent_context_schema = &join_stage.context.context_schema;
    for (i, parent_context_row) in join_stage.context.context_rows.iter().enumerate() {
      // Get the child `TableView`s corresponding to the context_row.
      let joined_table_view =
        table_views.get(*eval_data.parent_to_child_context_map.get(i).unwrap()).unwrap();

      let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
      add_vals(
        &mut col_map,
        &parent_context_schema.column_context_schema,
        &parent_context_row.column_context_row,
      );

      // Initialiate the TableView we would add to the final joined result.
      let mut finished_table_view = TableView::new();

      // Next, we iterate over rows of Left and Right sides, and we construct
      // a `ContextRow` for each GRQuery.
      for (row, _) in &joined_table_view.rows {
        add_vals_general(&mut col_map, schema, row);

        let mut raw_subquery_vals = Vec::<TableView>::new();
        for i in 0..gr_queries.len() {
          let context_row_map = context_row_maps.get_mut(i).unwrap();
          let context_row =
            mk_context_row(all_locations.get(i).unwrap(), parent_context_row, row, &vec![]);

          // Add in the ContextRow if it has not been added yet.
          if !context_row_map.contains(&context_row) {
            let index = context_row_map.len();
            context_row_map.insert(context_row.clone(), index);
          }

          // Lookup the raw subquery value for this weak conjunction and
          // add it to raw_subquery_vals
          let index = context_row_map.get(&context_row).unwrap();
          let cur_result = result.get(i).unwrap();
          let table_view = cur_result.get(*index).unwrap();
          raw_subquery_vals.push(table_view.clone());
        }

        // The projection result.
        let mut projection = Vec::<ColValN>::new();

        let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
        let mut next_subquery_idx = 0;
        for item in &join_select.projection {
          match item {
            proc::SelectItem::ExprWithAlias { item, .. } => {
              let expr = match item {
                proc::SelectExprItem::ValExpr(expr) => expr,
                proc::SelectExprItem::UnaryAggregate(unary_agg) => &unary_agg.expr,
              };
              let c_expr = construct_cexpr(expr, &col_map, &subquery_vals, &mut next_subquery_idx)?;
              projection.push(evaluate_c_expr(&c_expr)?);
            }
            proc::SelectItem::Wildcard { table_name } => {
              if let Some(table_name) = table_name {
                for (col_val, general_col_ref) in row.iter().zip(schema.iter()) {
                  if table_name == general_col_ref.table_name() {
                    projection.push(col_val.clone());
                  }
                }
              } else {
                projection.extend(row.clone().into_iter());
              }
            }
          }
        }

        finished_table_view.add_row(projection);
      }

      pre_agg_table_views.push(finished_table_view);
    }

    // Apply aggregation logic.
    let table_views = perform_aggregation(join_select, pre_agg_table_views)?;

    // Amend the `new_trans_table_context`
    for i in 0..self.context.context_rows.len() {
      let idx = join_stage.parent_to_child_context_map.get(i).unwrap();
      self.new_trans_table_context.get_mut(i).unwrap().push(*idx);
    }

    // Add the `table_views` to the GRQueryES and advance it.
    self.trans_table_views.push((trans_table_name.clone(), table_views));
    Ok(self.advance(ctx, io_ctx))
  }

  /// This advanced the Stage of the GRQueryES. If there is no next Stage, then we
  /// return Done, containing the result and signaling that the GRQueryES is complete.
  fn advance<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) -> GRQueryAction {
    // Compute the next stage
    let next_stage_idx = match &self.state {
      GRExecutionS::Start => 0,
      GRExecutionS::ReadStage(read_stage) => read_stage.stage_idx + 1,
      _ => panic!(),
    };

    if next_stage_idx < self.sql_query.trans_tables.len() {
      // This means that we have still have stages to evaluate, so we move on.
      self.process_gr_query_stage(ctx, io_ctx, next_stage_idx)
    } else {
      // This means the GRQueryES is done, so we send the desired result
      // back to the originator.
      let return_trans_table_pos =
        lookup_pos(&self.trans_table_views, &self.sql_query.returning).unwrap();
      let (_, table_views) = self.trans_table_views.get(return_trans_table_pos).unwrap();

      // To compute the result, recall that we need to associate the Context to each TableView.
      let mut result = Vec::<TableView>::new();
      for extended_trans_tables_row in &self.new_trans_table_context {
        let idx = extended_trans_tables_row.get(return_trans_table_pos).unwrap();
        result.push(table_views.get(*idx).unwrap().clone());
      }

      // Finally, we signal that the GRQueryES is done and send back the results.
      self.state = GRExecutionS::Done;
      GRQueryAction::Success(GRQueryResult { new_rms: self.new_rms.clone(), result })
    }
  }

  /// This function moves the GRQueryES to the Stage indicated by `stage_idx`.
  /// (Note the index must be valid (i.e. be an actual stage)).
  fn process_gr_query_stage<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    stage_idx: usize,
  ) -> GRQueryAction {
    let (_, stage) = self.sql_query.trans_tables.get(stage_idx).unwrap();

    let context_computer = ChildContextComputer {
      query_id: &self.query_id,
      new_trans_table_context: &self.new_trans_table_context,
      trans_table_views: &self.trans_table_views,
    };

    let (context, parent_to_child_context_map) = context_computer.compute_child_context(
      ctx,
      io_ctx,
      &self.context,
      QueryElement::GRQueryStage(stage),
    );

    // Construct the QueryPlan. We amend this Slave to the `query_leader_map`.
    let mut query_leader_map = self.query_plan.query_leader_map.clone();
    query_leader_map
      .insert(ctx.this_sid().clone(), ctx.leader_map().get(ctx.this_gid()).unwrap().clone());
    let query_plan = QueryPlan {
      tier_map: self.query_plan.tier_map.clone(),
      query_leader_map: query_leader_map.clone(),
      table_location_map: self.query_plan.table_location_map.clone(),
      col_presence_req: self.query_plan.col_presence_req.clone(),
    };

    // Construct the TMStatus
    let mut tm_status =
      TMStatus::new(io_ctx, self.root_query_path.clone(), OrigP::new(self.query_id.clone()));

    // Send out the PerformQuery and populate TMStatus accordingly.
    match stage {
      proc::GRQueryStage::TableSelect(select) => {
        // Here, we must do a TableSelectQuery.
        let general_query = msg::GeneralQuery::TableSelectQuery(msg::TableSelectQuery {
          timestamp: self.timestamp.clone(),
          context,
          sql_query: select.clone(),
          query_plan,
        });
        let full_gen = self.query_plan.table_location_map.get(&select.from.table_path).unwrap();
        let tids = ctx.get_min_tablets(&select.from, full_gen, &select.selection);

        let helper = SendHelper::TableQuery(general_query, tids);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx);
          return GRQueryAction::QueryError(msg::QueryError::InvalidLeadershipId);
        }

        // Move the GRQueryES to the next Stage.
        self.state = GRExecutionS::ReadStage(ReadStage {
          stage_idx,
          parent_to_child_context_map,
          stage_query_id: tm_status.query_id().clone(),
        });

        // Return the TMStatus for the parent Server to execute.
        GRQueryAction::ExecuteTMStatus(tm_status)
      }
      proc::GRQueryStage::TransTableSelect(select) => {
        // Here, we must do a TransTableSelectQuery. Recall there is only one RM.
        let location_prefix = context
          .context_schema
          .trans_table_context_schema
          .iter()
          .find(|prefix| &prefix.trans_table_name == &select.from.trans_table_name)
          .unwrap()
          .clone();
        let general_query = msg::GeneralQuery::TransTableSelectQuery(msg::TransTableSelectQuery {
          location_prefix: location_prefix.clone(),
          context,
          sql_query: select.clone(),
          query_plan,
        });

        let helper = SendHelper::TransTableQuery(general_query, location_prefix);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx);
          return GRQueryAction::QueryError(msg::QueryError::InvalidLeadershipId);
        }

        // Move the GRQueryES to the next Stage.
        self.state = GRExecutionS::ReadStage(ReadStage {
          stage_idx,
          parent_to_child_context_map,
          stage_query_id: tm_status.query_id().clone(),
        });

        // Return the TMStatus for the parent Server to execute.
        GRQueryAction::ExecuteTMStatus(tm_status)
      }
      proc::GRQueryStage::JoinSelect(select) => {
        let mut join_stage = JoinStage {
          stage_idx,
          maybe_subqueries_executing: None,
          result_map: Default::default(),
          join_node_schema: Default::default(),
          parent_to_child_context_map,
          context: Rc::new(context),
        };

        // Build children
        let mut parent_to_child_context_map = Vec::<usize>::new();
        let (mut context, locations) = initialize_contexts_general_once(
          &self.context.context_schema,
          &vec![],
          &vec![],
          QueryElement::JoinNode(&select.from),
        );

        // Build the context
        {
          // Sets to keep track of `ContextRow`s constructed for every corresponding
          // `Context` so that we do not add duplicate `ContextRow`s.
          let mut context_row_set = BTreeSet::<ContextRow>::new();

          // Iterate over parent context rows, then the child rows, and build out the
          // all GRQuery contexts.
          let parent_context = &join_stage.context;
          for (_, parent_context_row) in parent_context.context_rows.iter().enumerate() {
            // Next, we iterate over rows of Left and Right sides, and we construct
            // a `ContextRow` for each GRQuery.
            let context_row = mk_context_row(&locations, parent_context_row, &vec![], &vec![]);

            // Add in the ContextRow if it has not been added yet.
            if !context_row_set.contains(&context_row) {
              context_row_set.insert(context_row.clone());
              context.context_rows.push(context_row);
            }

            parent_to_child_context_map.push(context.context_rows.len());
          }
        }

        let col_refs = add_col_refs_for_projection(select);
        let mut join_node_schema = BTreeMap::<JoinNodeId, BTreeSet<GeneralColumnRef>>::new();
        build_join_node_schema(String::new(), col_refs, &select.from, &mut join_node_schema);
        for (join_node_id, col_refs) in join_node_schema {
          join_stage.join_node_schema.insert(join_node_id, col_refs.into_iter().collect());
        }

        let mut queries = Vec::<(QueryId, Rc<Context>, proc::GRQuery)>::new();
        start_evaluating_join(
          ctx,
          io_ctx,
          &mut join_stage.result_map,
          &mut queries,
          &select.dependency_graph,
          String::new(),
          &select.from,
          JoinNodeEvalData {
            context: Rc::new(context),
            parent_to_child_context_map,
            result_state: ResultState::Waiting,
          },
        );

        let mut gr_query_ess = Vec::<GRQueryES>::new();
        for (query_id, context, sql_query) in queries {
          // Filter the TransTables in the QueryPlan based on the TransTables
          // available for this subquery.
          let new_trans_table_context =
            (0..context.context_rows.len()).map(|_| Vec::new()).collect();
          // Finally, construct the GRQueryES.
          gr_query_ess.push(GRQueryES {
            root_query_path: self.root_query_path.clone(),
            timestamp: self.timestamp.clone(),
            context,
            new_trans_table_context,
            query_id,
            sql_query,
            query_plan: self.query_plan.clone(),
            new_rms: Default::default(),
            trans_table_views: vec![],
            state: GRExecutionS::Start,
            orig_p: OrigP::new(self.query_id.clone()),
          });
        }

        // Move the GRQueryES to the next Stage.
        self.state = GRExecutionS::JoinStage(join_stage);

        // Return the subqueries for the parent server to execute.
        GRQueryAction::SendSubqueries(gr_query_ess)
      }
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  ChildContextComputer
// -----------------------------------------------------------------------------------------------

fn start_evaluating_join<IO: CoreIOCtx, Ctx: CTServerContext>(
  ctx: &mut Ctx,
  io_ctx: &mut IO,
  result_map: &mut BTreeMap<JoinNodeId, JoinNodeEvalData>,
  child_queries: &mut Vec<(QueryId, Rc<Context>, proc::GRQuery)>,
  dependency_graph: &proc::DependencyGraph,
  jni: JoinNodeId,
  node: &proc::JoinNode,
  eval_data: JoinNodeEvalData,
) {
  match node {
    proc::JoinNode::JoinInnerNode(inner) => {
      let left_side = jni.clone() + "L";
      let right_side = jni.clone() + "R";
      if !dependency_graph.contains_key(&left_side) {
        // Build the context
        let parent_context = &eval_data.context;
        let mut parent_to_child_context_map = Vec::<usize>::new();
        let (mut context, locations) = initialize_contexts_general_once(
          &parent_context.context_schema,
          &vec![],
          &vec![],
          QueryElement::JoinNode(inner.left.deref()),
        );

        {
          // Sets to keep track of `ContextRow`s constructed for every corresponding
          // `Context` so that we do not add duplicate `ContextRow`s.
          let mut context_row_set = BTreeSet::<ContextRow>::new();

          // Iterate over parent context rows, then the child rows, and build out the
          // all GRQuery contexts.
          for (_, parent_context_row) in parent_context.context_rows.iter().enumerate() {
            // Next, we iterate over rows of Left and Right sides, and we construct
            // a `ContextRow` for each GRQuery.
            let context_row = mk_context_row(&locations, parent_context_row, &vec![], &vec![]);

            // Add in the ContextRow if it has not been added yet.
            if !context_row_set.contains(&context_row) {
              context_row_set.insert(context_row.clone());
              context.context_rows.push(context_row);
            }

            parent_to_child_context_map.push(context.context_rows.len());
          }
        }

        start_evaluating_join(
          ctx,
          io_ctx,
          result_map,
          child_queries,
          dependency_graph,
          left_side,
          inner.left.deref(),
          JoinNodeEvalData {
            context: Rc::new(context),
            parent_to_child_context_map,
            result_state: ResultState::Waiting,
          },
        );
      }
      if !dependency_graph.contains_key(&right_side) {
        // Build the context
        let parent_context = &eval_data.context;
        let mut parent_to_child_context_map = Vec::<usize>::new();
        let (mut context, locations) = initialize_contexts_general_once(
          &parent_context.context_schema,
          &vec![],
          &vec![],
          QueryElement::JoinNode(inner.right.deref()),
        );

        {
          // Sets to keep track of `ContextRow`s constructed for every corresponding
          // `Context` so that we do not add duplicate `ContextRow`s.
          let mut context_row_set = BTreeSet::<ContextRow>::new();

          // Iterate over parent context rows, then the child rows, and build out the
          // all GRQuery contexts.
          for (_, parent_context_row) in parent_context.context_rows.iter().enumerate() {
            // Next, we iterate over rows of Left and Right sides, and we construct
            // a `ContextRow` for each GRQuery.
            let context_row = mk_context_row(&locations, parent_context_row, &vec![], &vec![]);

            // Add in the ContextRow if it has not been added yet.
            if !context_row_set.contains(&context_row) {
              context_row_set.insert(context_row.clone());
              context.context_rows.push(context_row);
            }

            parent_to_child_context_map.push(context.context_rows.len());
          }
        }

        start_evaluating_join(
          ctx,
          io_ctx,
          result_map,
          child_queries,
          dependency_graph,
          right_side,
          inner.right.deref(),
          JoinNodeEvalData {
            context: Rc::new(context),
            parent_to_child_context_map,
            result_state: ResultState::Waiting,
          },
        );
      }
    }
    proc::JoinNode::JoinLeaf(leaf) => {
      // The QueryId here indicates the originating path of the JoinLeaf in the
      // Join Tree, which is useful for routing the result to the right location.
      let query_id = QueryId(format!("{}_{}", jni, rand_string(io_ctx.rand())));
      child_queries.push((query_id, eval_data.context.clone(), leaf.query.clone()));
    }
  };

  result_map.insert(jni.clone(), eval_data);
}

struct ChildContextComputer<'a> {
  pub query_id: &'a QueryId,
  pub new_trans_table_context: &'a Vec<Vec<usize>>,
  pub trans_table_views: &'a Vec<(TransTableName, Vec<TableView>)>,
}

impl<'a> ChildContextComputer<'a> {
  /// This takes an arbitrary `Context`, which we consider as the parent context, and then
  /// produces a child context for an arbitrary `elem`.
  fn compute_child_context<IO: CoreIOCtx, Ctx: CTServerContext>(
    &self,
    ctx: &mut Ctx,
    _: &mut IO,
    parent_context: &Context,
    elem: QueryElement,
  ) -> (Context, Vec<usize>) {
    let (col_names, trans_table_names) = compute_children_general(elem);

    // We first compute the Context
    let mut new_context_schema = ContextSchema::default();

    // Elements here correspond to `col_names`, where the `usize` points to the corresponding
    // ColName in the parent ColumnContextSchema.
    let mut col_indices = Vec::<usize>::new();
    for col_name in col_names {
      col_indices.push(
        parent_context
          .context_schema
          .column_context_schema
          .iter()
          .position(|name| &col_name == name)
          .unwrap(),
      );
    }

    // Similarly, elements here correspond to `trans_table_names`, except the `usize` depends on
    // if the TransTableName is an external TransTable (in TransTableContextSchema) or a local one.
    let mut trans_table_name_indicies = Vec::<TransTableIdx>::new();
    for trans_table_name in trans_table_names {
      if let Some(idx) = parent_context
        .context_schema
        .trans_table_context_schema
        .iter()
        .position(|prefix| prefix.trans_table_name == trans_table_name)
      {
        trans_table_name_indicies.push(TransTableIdx::External(idx));
      } else {
        trans_table_name_indicies.push(TransTableIdx::Local(
          self.trans_table_views.iter().position(|(name, _)| name == &trans_table_name).unwrap(),
        ));
      }
    }

    // Compute the child ContextSchema
    for idx in &col_indices {
      let col_name = parent_context.context_schema.column_context_schema.get(*idx).unwrap();
      new_context_schema.column_context_schema.push(col_name.clone());
    }

    for trans_table_idx in &trans_table_name_indicies {
      match trans_table_idx {
        TransTableIdx::External(idx) => {
          let prefix = parent_context.context_schema.trans_table_context_schema.get(*idx).unwrap();
          new_context_schema.trans_table_context_schema.push(prefix.clone());
        }
        TransTableIdx::Local(idx) => {
          let (trans_table_name, _) = self.trans_table_views.get(*idx).unwrap();
          new_context_schema.trans_table_context_schema.push(TransTableLocationPrefix {
            source: ctx.mk_this_query_path(self.query_id.clone()),
            trans_table_name: trans_table_name.clone(),
          });
        }
      }
    }

    // This contains the ContextRows of the child Context we're creating.
    let mut new_context_rows = Vec::<ContextRow>::new();
    // This maps the above ContextRows back to the index in which they appear.
    let mut reverse_map = BTreeMap::<ContextRow, usize>::new();
    // Elements here correspond to the parent ContextRows that have been processed, which
    // index that the corresponding child ContextRow takes on above.
    let mut parent_to_child_context_map = Vec::<usize>::new();

    // Iterate through the parent ContextRows and construct the child ContextRows.
    for (row_idx, context_row) in parent_context.context_rows.iter().enumerate() {
      let mut new_context_row = ContextRow::default();

      // Populate the ColumnContextRow.
      for idx in &col_indices {
        let col_val = context_row.column_context_row.get(*idx).unwrap();
        new_context_row.column_context_row.push(col_val.clone());
      }

      // Populate the TransTableContextRow
      for trans_table_idx in &trans_table_name_indicies {
        match trans_table_idx {
          TransTableIdx::External(idx) => {
            let trans_val = context_row.trans_table_context_row.get(*idx).unwrap();
            new_context_row.trans_table_context_row.push(trans_val.clone());
          }
          TransTableIdx::Local(idx) => {
            let trans_val = self.new_trans_table_context.get(row_idx).unwrap().get(*idx).unwrap();
            new_context_row.trans_table_context_row.push(trans_val.clone());
          }
        }
      }

      // Populate the next Context and associated metadata containers.
      if !reverse_map.contains_key(&new_context_row) {
        reverse_map.insert(new_context_row.clone(), new_context_rows.len());
        new_context_rows.push(new_context_row.clone());
      }
      parent_to_child_context_map.push(reverse_map.get(&new_context_row).unwrap().clone());
    }

    // Compute the context.
    let context = Context { context_schema: new_context_schema, context_rows: new_context_rows };

    (context, parent_to_child_context_map)
  }
}

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

fn build_join_node_schema(
  path: JoinNodeId,
  mut col_refs: BTreeSet<GeneralColumnRef>,
  join_node: &proc::JoinNode,
  join_node_schema: &mut BTreeMap<JoinNodeId, BTreeSet<GeneralColumnRef>>,
) {
  join_node_schema.insert(path.clone(), col_refs.clone());
  if let proc::JoinNode::JoinInnerNode(inner) = join_node {
    // Add the column references in the conjunctions to col_refs
    let jlns = collect_jlns(join_node);
    for expr in &inner.weak_conjunctions {
      add_col_refs_with_expr(&jlns, expr, &mut col_refs);
    }
    for expr in &inner.strong_conjunctions {
      add_col_refs_with_expr(&jlns, expr, &mut col_refs);
    }

    // Split the `ColumnRef`s into the left and right sides.
    let left_jlns = collect_jlns(&inner.left);
    let right_jlns = collect_jlns(&inner.right);
    let mut left_col_refs = BTreeSet::<GeneralColumnRef>::new();
    let mut right_col_refs = BTreeSet::<GeneralColumnRef>::new();
    for col_ref in col_refs {
      if left_jlns.contains(col_ref.table_name()) {
        left_col_refs.insert(col_ref);
      } else {
        debug_assert!(right_jlns.contains(col_ref.table_name()));
        right_col_refs.insert(col_ref);
      }
    }

    // Recurse.
    let left_path = path.clone() + "L";
    let right_path = path + "R";
    build_join_node_schema(left_path, left_col_refs, &inner.left, join_node_schema);
    build_join_node_schema(right_path, right_col_refs, &inner.right, join_node_schema);
  }
}

fn add_col_refs_with_expr(
  jlns: &Vec<String>,
  expr: &proc::ValExpr,
  col_refs: &mut BTreeSet<GeneralColumnRef>,
) {
  QueryIterator::new().iterate_expr(
    &mut |elem| {
      if let QueryElement::ValExpr(proc::ValExpr::ColumnRef(col_ref)) = elem {
        if jlns.contains(&col_ref.table_name) {
          col_refs.insert(GeneralColumnRef::Named(col_ref.clone()));
        }
      }
    },
    expr,
  )
}

/// Compute all `ColumnRefs` that need to be read from the `JoinLeafs` that
/// are needed by the the projection of the `JoinSelect.
fn add_col_refs_for_projection(select: &proc::JoinSelect) -> BTreeSet<GeneralColumnRef> {
  let mut col_refs = BTreeSet::<GeneralColumnRef>::new();
  let jlns = collect_jlns(&select.from);
  for select_item in &select.projection {
    match select_item {
      proc::SelectItem::ExprWithAlias { item, .. } => match item {
        proc::SelectExprItem::ValExpr(expr)
        | proc::SelectExprItem::UnaryAggregate(proc::UnaryAggregate { expr, .. }) => {
          // Add columns revealed returns by the JoinLeafs that are needed by `expr`.
          add_col_refs_with_expr(&jlns, expr, &mut col_refs);
        }
      },
      proc::SelectItem::Wildcard { table_name } => {
        // Add all columns returned by the JoinLeafs.
        add_col_refs_for_wildcard(table_name, &select.from, &mut col_refs);
      }
    }
  }
  col_refs
}

/// Given a `JoinSelect` with a (with a possible `table_name` qualification), we recurse down
/// the `from` and add all `ColumnRef`s that would need to be returned from the SELECT.
fn add_col_refs_for_wildcard(
  table_name: &Option<String>,
  from: &proc::JoinNode,
  col_refs: &mut BTreeSet<GeneralColumnRef>,
) {
  match from {
    proc::JoinNode::JoinInnerNode(inner) => {
      add_col_refs_for_wildcard(table_name, &inner.left, col_refs);
      add_col_refs_for_wildcard(table_name, &inner.right, col_refs);
    }
    proc::JoinNode::JoinLeaf(leaf) => {
      if let Some(name) = table_name {
        if name != &leaf.alias {
          return;
        }
      }

      // Recall that from `flatten_join_leaf` that the GRQuerys in the JoinLeaf
      // only has one stage. Recall that there can be `None` ColNames here (in the
      // case that the stage's SelectItem is a Wildcard).
      let (_, stage) = leaf.query.trans_tables.last().unwrap();
      for (index, maybe_col_name) in stage.schema().iter().enumerate() {
        if let Some(col_name) = maybe_col_name {
          col_refs.insert(GeneralColumnRef::Named(proc::ColumnRef {
            table_name: leaf.alias.clone(),
            col_name: col_name.clone(),
          }));
        } else {
          col_refs.insert(GeneralColumnRef::Unnamed(UnnamedColumnRef {
            table_name: leaf.alias.clone(),
            index,
          }));
        }
      }
    }
  }
}

/// Dig into the `join_node` tree using the path specified by `jni`, and the
/// return the resulting `JoinNode`.
fn lookup_join_node(join_node: &proc::JoinNode, jni: JoinNodeId) -> &proc::JoinNode {
  let mut cur_node = join_node;
  for cur_char in jni.chars() {
    let inner = cast!(proc::JoinNode::JoinInnerNode, cur_node).unwrap();
    if cur_char == 'L' {
      cur_node = &inner.left;
    } else {
      debug_assert!(cur_char == 'R');
      cur_node = &inner.right;
    }
  }
  cur_node
}
