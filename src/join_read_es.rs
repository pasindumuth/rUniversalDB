use crate::col_usage::{gr_query_collecting_cb, QueryElement, QueryIterator};
use crate::common::{
  mk_qid, rand_string, unexpected_branch, CQueryPath, CTQueryPath, ColValN, Context, ContextRow,
  ContextSchema, CoreIOCtx, OrigP, QueryESResult, QueryId, QueryPlan, ReadOnlySet, TQueryPath,
  TableView, Timestamp,
};
use crate::expression::{construct_cexpr, evaluate_c_expr, is_true, EvalError};
use crate::gr_query_es::{GRExecutionS, GRQueryES};
use crate::join_util::{
  add_vals, add_vals_general, extract_subqueries, initialize_contexts,
  initialize_contexts_general_once, make_parent_row, mk_context_row, Locations,
};
use crate::message as msg;
use crate::query_converter::collect_jlns;
use crate::server::{
  extract_subquery_vals, mk_eval_error, CTServerContext, GeneralColumnRef, UnnamedColumnRef,
};
use crate::sql_ast::{iast, proc};
use crate::table_read_es::perform_aggregation;
use crate::tablet::{Executing, TPESAction};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  JoinReadES
// -----------------------------------------------------------------------------------------------

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

impl Default for ResultState {
  fn default() -> Self {
    ResultState::Waiting
  }
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

impl JoinNodeEvalData {
  fn cast_finished(&self) -> FinishedEvalData {
    let table_views = cast!(ResultState::Finished, &self.result_state).unwrap();
    FinishedEvalData { parent_to_child_context_map: &self.parent_to_child_context_map, table_views }
  }
}

struct FinishedEvalData<'a> {
  parent_to_child_context_map: &'a Vec<usize>,
  table_views: &'a Vec<TableView>,
}

impl<'a> FinishedEvalData<'a> {
  fn get_for_parent_context_row(&self, i: usize) -> &TableView {
    self.table_views.get(*self.parent_to_child_context_map.get(i).unwrap()).unwrap()
  }
}

#[derive(Debug, Default)]
struct JoinEvaluating {
  result_map: BTreeMap<JoinNodeId, JoinNodeEvalData>,
  join_node_schema: BTreeMap<JoinNodeId, Vec<GeneralColumnRef>>,
}

#[derive(Debug)]
struct ProjectionEvaluating {
  /// Copied from the root JoinNodeEvalData
  parent_to_child_context_map: Vec<usize>,
  context: Rc<Context>,
  table_views: Vec<TableView>,

  /// Subquery executing state
  subqueries_executing: Executing,
  /// Copied from `JoinEvlauating`
  schema: Vec<GeneralColumnRef>,
}

impl ProjectionEvaluating {
  fn get_finished(&self) -> FinishedEvalData {
    FinishedEvalData {
      parent_to_child_context_map: &self.parent_to_child_context_map,
      table_views: &self.table_views,
    }
  }
}

#[derive(Debug)]
enum ExecutionS {
  Start,
  JoinEvaluating(JoinEvaluating),
  ProjectionEvaluating(ProjectionEvaluating),
  Done,
}

#[derive(Debug)]
pub struct JoinReadES {
  root_query_path: CQueryPath,
  timestamp: Timestamp,
  context: Rc<Context>,

  // Fields needed for responding.
  query_id: QueryId,

  // Query-related fields.
  sql_query: proc::JoinSelect,
  query_plan: QueryPlan,

  // Dynamically evolving fields.
  new_rms: BTreeSet<TQueryPath>,
  state: ExecutionS,
  orig_p: OrigP,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl JoinReadES {
  pub fn create<IO: CoreIOCtx>(
    io_ctx: &mut IO,
    root_query_path: CQueryPath,
    timestamp: Timestamp,
    context: Rc<Context>,
    sql_query: proc::JoinSelect,
    query_plan: QueryPlan,
    orig_p: OrigP,
  ) -> JoinReadES {
    JoinReadES {
      root_query_path,
      timestamp,
      context,
      query_id: mk_qid(io_ctx.rand()),
      sql_query,
      query_plan,
      new_rms: BTreeSet::new(),
      state: ExecutionS::Start,
      orig_p,
    }
  }

  pub fn query_id(&self) -> &QueryId {
    &self.query_id
  }

  pub fn start<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) -> Option<TPESAction> {
    // Build children
    let (context, parent_to_child_context_map) =
      build_join_node_context(&self.context, &self.sql_query.from);

    let mut state =
      JoinEvaluating { result_map: Default::default(), join_node_schema: Default::default() };

    let col_refs = add_col_refs_for_projection(&self.sql_query);
    let mut join_node_schema = BTreeMap::<JoinNodeId, BTreeSet<GeneralColumnRef>>::new();
    build_join_node_schema(String::new(), col_refs, &self.sql_query.from, &mut join_node_schema);
    for (join_node_id, col_refs) in join_node_schema {
      state.join_node_schema.insert(join_node_id, col_refs.into_iter().collect());
    }

    let mut queries = Vec::<(QueryId, Rc<Context>, proc::GRQuery)>::new();
    start_evaluating_join(
      ctx,
      io_ctx,
      &mut state.result_map,
      &mut queries,
      &self.sql_query.dependency_graph,
      String::new(),
      &self.sql_query.from,
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
      let new_trans_table_context = (0..context.context_rows.len()).map(|_| Vec::new()).collect();
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
    self.state = ExecutionS::JoinEvaluating(state);

    // Return subqueries.
    Some(TPESAction::SendSubqueries(gr_query_ess))
  }

  /// We call this when a JoinNode enters the `Finished` stage.
  fn join_node_finished<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    this_id: JoinNodeId,
  ) -> Option<TPESAction> {
    let join_stage = cast!(ExecutionS::JoinEvaluating, &mut self.state)?;
    let join_select = &self.sql_query;

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
          let parent_node = lookup_join_node(&self.sql_query.from, parent_id.clone());
          let parent_inner = cast!(proc::JoinNode::JoinInnerNode, parent_node).unwrap();
          let (weak_gr_queries, strong_gr_queries) = extract_subqueries(&parent_inner);

          if weak_gr_queries.is_empty() && strong_gr_queries.is_empty() {
            // Here, we may finish the JoinNode immediately.
            self.finish_join_node(ctx, io_ctx, parent_id, vec![], vec![])
          } else {
            // Case in `join_node_finished` where there is a subquery in either the
            // Weak conjunctions or Strong conjunctions.

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
              // Construct the GRQueryESs.
              let mut gr_query_ess = Vec::<GRQueryES>::new();
              for (sql_query, context) in query_and_context {
                let context = Rc::new(context);
                let query_id = QueryId(format!("{}_{}", parent_id, rand_string(io_ctx.rand())));

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
              let mut simple = ContextConstructorSimple::new(
                &parent_context.context_schema,
                &first_schema,
                &second_schema,
                &gr_queries,
              );

              // Get TableViews
              let first_finished = first_eval_data.cast_finished();
              let second_finished = second_eval_data.cast_finished();

              // Iterate over the Parent rows.
              for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
                // Get both Sides.
                let first_table_view = first_finished.get_for_parent_context_row(i);
                let second_table_view = second_finished.get_for_parent_context_row(i);

                // Iterate over First Side rows.
                for (first_row, _) in &first_table_view.rows {
                  // Iterate over Second Side rows.
                  for (second_row, _) in &second_table_view.rows {
                    simple.add_row(parent_context_row, first_row, second_row);
                  }
                }
              }

              // Construct the GRQueryESs.
              mk_gr_query_ess(
                io_ctx,
                gr_queries.into_iter().zip(simple.to_contexts().into_iter()).collect(),
              )
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
              let mut simple = ContextConstructorSimple::new(
                &parent_context.context_schema,
                &first_schema,
                &second_schema,
                &gr_queries,
              );

              // Get TableViews
              let first_finished = first_eval_data.cast_finished();
              let second_finished = second_eval_data.cast_finished();

              // Iterate over the Parent rows.
              let mut second_side_count = 0;
              for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
                // Get the First Side
                let first_table_view = first_finished.get_for_parent_context_row(i);

                // Iterate over First Side rows.
                for (first_row, _) in &first_table_view.rows {
                  // Get the Second Side.
                  let second_table_view =
                    second_finished.get_for_parent_context_row(second_side_count);
                  second_side_count += 1;

                  // Iterate over Second Side rows.
                  for (second_row, _) in &second_table_view.rows {
                    simple.add_row(parent_context_row, first_row, second_row);
                  }
                }
              }

              // Construct the GRQueryESs.
              mk_gr_query_ess(
                io_ctx,
                gr_queries.into_iter().zip(simple.to_contexts().into_iter()).collect(),
              )
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
                    Some(TPESAction::SendSubqueries(gr_query_ess))
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
                    // We move to `WaitingSubqueriesForWeak`.
                    parent_eval_data.result_state =
                      ResultState::WaitingSubqueriesForWeak(Executing::create(&gr_query_ess));

                    // Return that GRQueryESs.
                    Some(TPESAction::SendSubqueries(gr_query_ess))
                  }
                }
              } else {
                // Here, there are dependencies.
                match parent_inner.join_type {
                  iast::JoinType::Inner => {
                    let gr_query_ess = if join_select.dependency_graph.contains_key(&right_id) {
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
                    Some(TPESAction::SendSubqueries(gr_query_ess))
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
                    // We move to `WaitingSubqueriesForWeak`.
                    parent_eval_data.result_state =
                      ResultState::WaitingSubqueriesForWeak(Executing::create(&gr_query_ess));

                    // Return that GRQueryESs.
                    Some(TPESAction::SendSubqueries(gr_query_ess))
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
                    // We move to `WaitingSubqueriesForWeak`.
                    parent_eval_data.result_state =
                      ResultState::WaitingSubqueriesForWeak(Executing::create(&gr_query_ess));

                    // Return that GRQueryESs.
                    Some(TPESAction::SendSubqueries(gr_query_ess))
                  }
                  // An OUTER JOIN should never have a dependency between its children.
                  iast::JoinType::Outer => return unexpected_branch(),
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
          None
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
        let this_finished = this_eval_data.cast_finished();

        // Construct stuff to use to compute context.
        let join_node = lookup_join_node(&join_select.from, sibling_id.clone());

        let mut parent_to_child_context_map = Vec::<usize>::new();
        let mut simple = ContextConstructorSimpleOnce::new(
          &parent_eval_data.context.context_schema,
          this_schema,
          &vec![],
          QueryElement::JoinNode(join_node),
        );

        // Iterate over parent context rows, then the child rows, and build out context.
        for (i, parent_context_row) in parent_eval_data.context.context_rows.iter().enumerate() {
          for (this_row, _) in &this_finished.get_for_parent_context_row(i).rows {
            simple.add_row(parent_context_row, this_row, &vec![]);
            parent_to_child_context_map.push(simple.context.context_rows.len());
          }
        }

        // Build the EvalData for the sibling table
        let sibling_eval_data = JoinNodeEvalData {
          context: Rc::new(simple.context),
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
        // will definitely produce GRQueryESs.
        debug_assert!(!gr_query_ess.is_empty());

        Some(TPESAction::SendSubqueries(gr_query_ess))
      }
    } else {
      // Case in `join_node_finished` where the root `JoinNode` is finished evaluating.

      // Collect all subqueries for the SelectItems. Recall that JoinSelects
      // do not have a WHERE clause (since it is pushed into JoinNode).
      let mut it = QueryIterator::new_top_level();
      let mut gr_queries = Vec::<proc::GRQuery>::new();
      it.iterate_select_items(
        &mut gr_query_collecting_cb(&mut gr_queries),
        &join_select.projection,
      );

      let mut join_stage = std::mem::take(join_stage);
      debug_assert!(join_stage.result_map.len() == 1);
      let (_, mut eval_data) = join_stage.result_map.into_iter().next().unwrap();
      let schema = join_stage.join_node_schema.get("").unwrap().clone();

      // Get TableViews
      let table_views =
        cast!(ResultState::Finished, std::mem::take(&mut eval_data.result_state)).unwrap();

      if gr_queries.is_empty() {
        // Update state
        self.state = ExecutionS::ProjectionEvaluating(ProjectionEvaluating {
          parent_to_child_context_map: eval_data.parent_to_child_context_map,
          context: eval_data.context,
          table_views,
          subqueries_executing: Executing::default(),
          schema,
        });

        // Here, there are no subqueries to evaluate, so we can finish.
        self.finish_join_select(ctx, io_ctx, vec![])
      } else {
        // When the root is EvalData is finished, it should be the only element
        // in the `result_map`.

        // Get TableViews
        let finished = eval_data.cast_finished();

        let mut simple = ContextConstructorSimple::new(
          &self.context.context_schema,
          &schema,
          &vec![],
          &gr_queries,
        );

        // Iterate over the Parent rows.
        for (i, parent_context_row) in self.context.context_rows.iter().enumerate() {
          // Get the joined table.
          let joined_table_view = finished.get_for_parent_context_row(i);

          // Iterate over rows.
          for (row, _) in &joined_table_view.rows {
            simple.add_row(parent_context_row, row, &vec![]);
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
            // Construct the GRQueryESs.
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

        // Build GRQueryESs
        let gr_query_ess = mk_gr_query_ess(
          io_ctx,
          gr_queries.into_iter().zip(simple.to_contexts().into_iter()).collect(),
        );

        // Update state
        self.state = ExecutionS::ProjectionEvaluating(ProjectionEvaluating {
          parent_to_child_context_map: eval_data.parent_to_child_context_map,
          context: eval_data.context,
          table_views,
          subqueries_executing: Executing::create(&gr_query_ess),
          schema,
        });

        // Return the subqueries
        Some(TPESAction::SendSubqueries(gr_query_ess))
      }
    }
  }

  /// Here, the `parent_id` will point to a `JoinInnerNode`, and we wish to
  /// move it's EvalData to `WaitingSubqueriesFinal`. The `result` has the same
  /// length as the number of subqueries in the Weak Conjunctions. Also importantly,
  /// there should be at least one Strong Conjunction (otherwise, we could just call
  /// `finish_join_node` instead).
  fn advance_to_final_subqueries<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    _: &mut Ctx,
    io_ctx: &mut IO,
    parent_id: JoinNodeId,
    weak_results: Vec<Vec<TableView>>,
  ) -> Option<TPESAction> {
    // Dig into the state and get relevant data.
    let join_stage = cast!(ExecutionS::JoinEvaluating, &mut self.state)?;
    let join_select = &self.sql_query;
    let parent_node = lookup_join_node(&join_select.from, parent_id.clone());
    let parent_inner = cast!(proc::JoinNode::JoinInnerNode, parent_node).unwrap();
    let (weak_gr_queries, strong_gr_queries) = extract_subqueries(&parent_inner);
    debug_assert_eq!(weak_gr_queries.len(), weak_results.len());

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
      // Construct the GRQueryESs.
      let mut gr_query_ess = Vec::<GRQueryES>::new();
      for (sql_query, context) in query_and_context {
        let context = Rc::new(context);
        let query_id = QueryId(format!("{}_{}", parent_id, rand_string(io_ctx.rand())));

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
        let first_finished = first_eval_data.cast_finished();
        let second_finished = second_eval_data.cast_finished();

        let mut weak_simple = SubqueryExtractorSimple::new(
          &parent_context.context_schema,
          &first_schema,
          &second_schema,
          &weak_gr_queries,
          result,
        );

        let mut strong_simple = ContextConstructorSimple::new(
          &parent_context.context_schema,
          &first_schema,
          &second_schema,
          &strong_gr_queries,
        );

        // Iterate over the Parent rows.
        for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
          // Start constructing the `col_map` (needed for evaluating the conjunctions).
          let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
          add_vals(
            &mut col_map,
            &parent_eval_data.context.context_schema.column_context_schema,
            &parent_context_row.column_context_row,
          );

          // Get both Sides.
          let first_table_view = first_finished.get_for_parent_context_row(i);
          let second_table_view = second_finished.get_for_parent_context_row(i);

          // Holds all rows on the Second Side that were unrejected by weak conjunctions.
          let mut second_side_weak_unrejected = BTreeSet::<Vec<ColValN>>::new();

          // Iterate over First Side rows.
          for (first_row, _) in &first_table_view.rows {
            add_vals_general(&mut col_map, first_schema, first_row);

            let mut all_weak_rejected = true;

            // Iterate over Second Side rows.
            for (second_row, _) in &second_table_view.rows {
              add_vals_general(&mut col_map, second_schema, second_row);

              // Evaluate the weak conjunctions.
              let vals = weak_simple.get_results(parent_context_row, first_row, second_row);
              if evaluate_conjunctions(&parent_inner.weak_conjunctions, &col_map, &vals)? {
                continue;
              }

              // Mark that at least one pair of `first_row` and `second_row` was not rejected.
              all_weak_rejected = false;
              second_side_weak_unrejected.insert(second_row.clone());

              // Add in the artificial row to the Strong Conjunctions' context.
              strong_simple.add_row(parent_context_row, first_row, second_row);
            }

            // Here, we check whether we need to manufacture a row
            if all_weak_rejected && first_outer {
              // Construct an artificial row
              let (second_row, _) = make_empty_row(second_schema.len());
              add_vals_general(&mut col_map, second_schema, &second_row);

              // Add in the artificial row to the Strong Conjunctions' context.
              strong_simple.add_row(parent_context_row, first_row, &second_row);
            }
          }

          // Perform row-readdition on the Second Side if need be.
          if second_outer {
            // Construct an artificial row
            let (first_row, _) = make_empty_row(first_schema.len());
            add_vals_general(&mut col_map, first_schema, &first_row);

            for (second_row, _) in &second_table_view.rows {
              // Check that the `second_row` was always rejected by weak conjunctions.
              if !second_side_weak_unrejected.contains(second_row) {
                add_vals_general(&mut col_map, second_schema, &second_row);

                // Add in the artificial row to the Strong Conjunctions' context.
                strong_simple.add_row(parent_context_row, &first_row, &second_row);
              }
            }
          }
        }

        // Construct the GRQueryESs.
        Ok(mk_gr_query_ess(
          io_ctx,
          strong_gr_queries.into_iter().zip(strong_simple.to_contexts().into_iter()).collect(),
        ))
      };

      match &parent_inner.join_type {
        // Inner JOINs should never be in the `WaitingSubqueriesForWeak` state.
        iast::JoinType::Inner => unexpected_branch(),
        iast::JoinType::Left => {
          // Make GRQueryESs
          match send_gr_queries_indep(
            io_ctx,
            left_eval_data,
            right_eval_data,
            parent_eval_data,
            left_schema,
            right_schema,
            weak_gr_queries,
            strong_gr_queries,
            &weak_results,
            true,
            false,
          ) {
            Ok(gr_query_ess) => {
              // We move to `WaitingSubqueriesFinal`.
              let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
              parent_eval_data.result_state =
                ResultState::WaitingSubqueriesFinal(weak_results, Executing::create(&gr_query_ess));

              // Return that GRQueryESs.
              Some(TPESAction::SendSubqueries(gr_query_ess))
            }
            Err(eval_error) => Some(TPESAction::QueryError(mk_eval_error(eval_error))),
          }
        }
        iast::JoinType::Right => {
          // Make GRQueryESs. We make the Right side to be the first, since this
          // is needed for row re-addition.
          match send_gr_queries_indep(
            io_ctx,
            right_eval_data,
            left_eval_data,
            parent_eval_data,
            right_schema,
            left_schema,
            weak_gr_queries,
            strong_gr_queries,
            &weak_results,
            true,
            false,
          ) {
            Ok(gr_query_ess) => {
              // We move to `WaitingSubqueriesFinal`.
              let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
              parent_eval_data.result_state =
                ResultState::WaitingSubqueriesFinal(weak_results, Executing::create(&gr_query_ess));

              // Return that GRQueryESs.
              Some(TPESAction::SendSubqueries(gr_query_ess))
            }
            Err(eval_error) => Some(TPESAction::QueryError(mk_eval_error(eval_error))),
          }
        }
        iast::JoinType::Outer => {
          // Make GRQueryESs
          match send_gr_queries_indep(
            io_ctx,
            left_eval_data,
            right_eval_data,
            parent_eval_data,
            left_schema,
            right_schema,
            weak_gr_queries,
            strong_gr_queries,
            &weak_results,
            true,
            true,
          ) {
            Ok(gr_query_ess) => {
              // We move to `WaitingSubqueriesFinal`.
              let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
              parent_eval_data.result_state =
                ResultState::WaitingSubqueriesFinal(weak_results, Executing::create(&gr_query_ess));

              // Return that GRQueryESs.
              Some(TPESAction::SendSubqueries(gr_query_ess))
            }
            Err(eval_error) => Some(TPESAction::QueryError(mk_eval_error(eval_error))),
          }
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
        let first_finished = first_eval_data.cast_finished();
        let second_finished = second_eval_data.cast_finished();

        let mut weak_simple = SubqueryExtractorSimple::new(
          &parent_context.context_schema,
          &first_schema,
          &second_schema,
          &weak_gr_queries,
          result,
        );

        let mut strong_simple = ContextConstructorSimple::new(
          &parent_context.context_schema,
          &first_schema,
          &second_schema,
          &strong_gr_queries,
        );

        // Iterate over the Parent rows.
        let mut second_side_count = 0;
        for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
          // Start constructing the `col_map` (needed for evaluating the conjunctions).
          let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
          add_vals(
            &mut col_map,
            &parent_eval_data.context.context_schema.column_context_schema,
            &parent_context_row.column_context_row,
          );

          // Get the First Side
          let first_table_view = first_finished.get_for_parent_context_row(i);

          // Iterate over First Side rows.
          for (first_row, _) in &first_table_view.rows {
            add_vals_general(&mut col_map, first_schema, first_row);

            let mut all_weak_rejected = true;

            // Get the second side
            let second_table_view = second_finished.get_for_parent_context_row(i);
            second_side_count += 1;

            // Iterate over Second Side rows.
            for (second_row, _) in &second_table_view.rows {
              add_vals_general(&mut col_map, second_schema, second_row);

              // Evaluate the weak conjunctions.
              let vals = weak_simple.get_results(parent_context_row, first_row, second_row);
              if evaluate_conjunctions(&parent_inner.weak_conjunctions, &col_map, &vals)? {
                continue;
              }

              // Mark that at least one pair of `first_row` and `second_row` was not rejected.
              all_weak_rejected = false;

              // Add in the artificial row to the Strong Conjunctions' context.
              strong_simple.add_row(parent_context_row, first_row, second_row);
            }

            // Here, we check whether we need to manufacture a row
            if all_weak_rejected {
              // Construct an artificial row.
              let (second_row, _) = make_empty_row(second_schema.len());
              add_vals_general(&mut col_map, second_schema, &second_row);

              // Add in the artificial row to the Strong Conjunctions' context.
              strong_simple.add_row(parent_context_row, first_row, &second_row);
            }
          }
        }

        // Construct the GRQueryESs.
        Ok(mk_gr_query_ess(
          io_ctx,
          strong_gr_queries.into_iter().zip(strong_simple.to_contexts().into_iter()).collect(),
        ))
      };

      match &parent_inner.join_type {
        // Inner JOINs should never be in the `WaitingSubqueriesForWeak` state.
        iast::JoinType::Inner => unexpected_branch(),
        iast::JoinType::Left => {
          // Make GRQueryESs
          match send_gr_queries_dep(
            io_ctx,
            left_eval_data,
            right_eval_data,
            parent_eval_data,
            left_schema,
            right_schema,
            weak_gr_queries,
            strong_gr_queries,
            &weak_results,
          ) {
            Ok(gr_query_ess) => {
              // We move to `WaitingSubqueriesFinal`.
              let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
              parent_eval_data.result_state =
                ResultState::WaitingSubqueriesFinal(weak_results, Executing::create(&gr_query_ess));

              // Return that GRQueryESs.
              Some(TPESAction::SendSubqueries(gr_query_ess))
            }
            Err(eval_error) => Some(TPESAction::QueryError(mk_eval_error(eval_error))),
          }
        }
        iast::JoinType::Right => {
          // Make GRQueryESs. We make the Right side to be the first, since this
          // is needed for row re-addition.
          match send_gr_queries_dep(
            io_ctx,
            right_eval_data,
            left_eval_data,
            parent_eval_data,
            right_schema,
            left_schema,
            weak_gr_queries,
            strong_gr_queries,
            &weak_results,
          ) {
            Ok(gr_query_ess) => {
              // We move to `WaitingSubqueriesFinal`.
              let parent_eval_data = join_stage.result_map.get_mut(&parent_id).unwrap();
              parent_eval_data.result_state =
                ResultState::WaitingSubqueriesFinal(weak_results, Executing::create(&gr_query_ess));

              // Return that GRQueryESs.
              Some(TPESAction::SendSubqueries(gr_query_ess))
            }
            Err(eval_error) => Some(TPESAction::QueryError(mk_eval_error(eval_error))),
          }
        }
        // Inner JOINs should never be in the `WaitingSubqueriesForWeak` state.
        iast::JoinType::Outer => unexpected_branch(),
      }
    }
  }

  fn finish_join_node<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    parent_id: JoinNodeId,
    weak_result: Vec<Vec<TableView>>,
    strong_result: Vec<Vec<TableView>>,
  ) -> Option<TPESAction> {
    // Lookup the JoinInnerNode.
    let join_select = &self.sql_query;
    let join_stage = cast!(ExecutionS::JoinEvaluating, &mut self.state)?;
    let parent_node = lookup_join_node(&join_select.from, parent_id.clone());
    let parent_inner = cast!(proc::JoinNode::JoinInnerNode, parent_node).unwrap();
    let (weak_gr_queries, strong_gr_queries) = extract_subqueries(&parent_inner);

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
    let result = if !join_select.dependency_graph.contains_key(&left_id)
      && !join_select.dependency_graph.contains_key(&right_id)
    {
      // There is a dependency.
      match &parent_inner.join_type {
        iast::JoinType::Inner => finish_independent_join(
          left_eval_data,
          right_eval_data,
          parent_eval_data,
          left_schema,
          right_schema,
          parent_schema,
          parent_inner,
          weak_gr_queries,
          strong_gr_queries,
          weak_result,
          strong_result,
          false,
          false,
        ),
        iast::JoinType::Left => finish_independent_join(
          left_eval_data,
          right_eval_data,
          parent_eval_data,
          left_schema,
          right_schema,
          parent_schema,
          parent_inner,
          weak_gr_queries,
          strong_gr_queries,
          weak_result,
          strong_result,
          true,
          false,
        ),
        iast::JoinType::Right => finish_independent_join(
          right_eval_data,
          left_eval_data,
          parent_eval_data,
          right_schema,
          left_schema,
          parent_schema,
          parent_inner,
          weak_gr_queries,
          strong_gr_queries,
          weak_result,
          strong_result,
          true,
          false,
        ),
        iast::JoinType::Outer => finish_independent_join(
          left_eval_data,
          right_eval_data,
          parent_eval_data,
          left_schema,
          right_schema,
          parent_schema,
          parent_inner,
          weak_gr_queries,
          strong_gr_queries,
          weak_result,
          strong_result,
          true,
          true,
        ),
      }
    } else {
      // Handle the case of dependencies existing.
      match &parent_inner.join_type {
        iast::JoinType::Inner => {
          if join_select.dependency_graph.contains_key(&right_id) {
            finish_dependent_join(
              left_eval_data,
              right_eval_data,
              parent_eval_data,
              left_schema,
              right_schema,
              parent_schema,
              parent_inner,
              weak_gr_queries,
              strong_gr_queries,
              weak_result,
              strong_result,
            )
          } else {
            finish_dependent_join(
              right_eval_data,
              left_eval_data,
              parent_eval_data,
              right_schema,
              left_schema,
              parent_schema,
              parent_inner,
              weak_gr_queries,
              strong_gr_queries,
              weak_result,
              strong_result,
            )
          }
        }
        iast::JoinType::Left => finish_dependent_join(
          left_eval_data,
          right_eval_data,
          parent_eval_data,
          left_schema,
          right_schema,
          parent_schema,
          parent_inner,
          weak_gr_queries,
          strong_gr_queries,
          weak_result,
          strong_result,
        ),
        iast::JoinType::Right => finish_dependent_join(
          right_eval_data,
          left_eval_data,
          parent_eval_data,
          right_schema,
          left_schema,
          parent_schema,
          parent_inner,
          weak_gr_queries,
          strong_gr_queries,
          weak_result,
          strong_result,
        ),
        // Inner JOINs should never be in the `WaitingSubqueriesFinal` state.
        iast::JoinType::Outer => return unexpected_branch(),
      }
    };

    match result {
      Ok(()) => self.join_node_finished(ctx, io_ctx, parent_id),
      Err(eval_error) => Some(TPESAction::QueryError(mk_eval_error(eval_error))),
    }
  }

  /// This is called if a subquery fails.
  pub fn handle_internal_query_error<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> Option<TPESAction> {
    self.exit_and_clean_up(ctx, io_ctx);
    Some(TPESAction::QueryError(query_error))
  }

  pub fn handle_subquery_done<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    qid: QueryId,
    rms: BTreeSet<TQueryPath>,
    result: Vec<TableView>,
  ) -> Option<TPESAction> {
    self.new_rms.extend(rms);

    let QueryId(qid_str) = &qid;
    let mut it = Option::<usize>::None;
    for (index, cur_char) in qid_str.chars().enumerate() {
      if cur_char == '_' {
        it = Some(index);
      }
    }

    // This means the `qid` is destined for somewhere in the Join Tree
    if let Some(i) = it {
      // This should be a JoinStage if we are getting GRQueryESs results.
      let join_stage = cast!(ExecutionS::JoinEvaluating, &mut self.state)?;
      let this_id: String = qid_str.chars().take(i).collect();

      // Handle the case
      let eval_data = join_stage.result_map.get_mut(&this_id).unwrap();
      match &mut eval_data.result_state {
        ResultState::Waiting => {
          // This means `parent_id` must correspond to a JoinLeaf, and we just got the
          // result of the JoinLeaf's GRQueryES.
          eval_data.result_state = ResultState::Finished(result);
          self.join_node_finished(ctx, io_ctx, this_id)
        }
        ResultState::WaitingSubqueriesForWeak(executing) => {
          // This means that `this_id` is a JoinInnerNode. Rename it to `parent_id`.
          let parent_id = this_id;
          executing.add_subquery_result(qid, result);

          // If this is complete, then we continue
          if executing.is_complete() {
            let (_, results) = std::mem::take(executing).get_results();

            // Computer whether there are strong conjunctions here.
            let join_select = &self.sql_query;
            let parent_node = lookup_join_node(&join_select.from, parent_id.clone());
            let parent_inner = cast!(proc::JoinNode::JoinInnerNode, parent_node).unwrap();
            let (_, strong_gr_queries) = extract_subqueries(&parent_inner);

            // If there are strong conjunctions then advance to `WaitingSubqueriesFinal`
            if !strong_gr_queries.is_empty() {
              self.advance_to_final_subqueries(ctx, io_ctx, parent_id, results)
            } else {
              // Otherwise, we can simply finish.
              self.finish_join_node(ctx, io_ctx, parent_id, results, vec![])
            }
          } else {
            // Here, we are waiting for more subquery results to come.
            None
          }
        }
        ResultState::WaitingSubqueriesFinal(results, executing) => {
          // This means that `this_id` is a JoinInnerNode. Rename it to `parent_id`.
          let parent_id = this_id;
          executing.add_subquery_result(qid, result);

          // If this is complete, then we continue
          if executing.is_complete() {
            let (_, strong_results) = std::mem::take(executing).get_results();
            let results = std::mem::take(results);

            // Finish the JoinNode
            self.finish_join_node(ctx, io_ctx, parent_id, results, strong_results)
          } else {
            // Here, we are waiting for more subquery results to come.
            None
          }
        }
        // We should not get a subquery result if the EvalData is already finished.
        ResultState::Finished(_) => unexpected_branch(),
      }
    } else {
      let projection_evaluation = cast!(ExecutionS::ProjectionEvaluating, &mut self.state)?;

      // Here, forward the subquery results to the `maybe_subqueries_executing` at the top
      // level, and finish the query if it is done.
      let executing = &mut projection_evaluation.subqueries_executing;
      executing.add_subquery_result(qid, result);
      if executing.is_complete() {
        let (_, result) = std::mem::take(executing).get_results();
        self.finish_join_select(ctx, io_ctx, result)
      } else {
        // Otherwise, keep waiting
        None
      }
    }
  }

  /// This is the last function to be called. Here, the `results` corresponds to the
  /// results of any subqueries in the `SelectItem`s of the `JoinSelect`
  fn finish_join_select<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    _: &mut Ctx,
    _: &mut IO,
    results: Vec<Vec<TableView>>,
  ) -> Option<TPESAction> {
    // This should be a JoinStage if we are getting GRQueryESs results.
    let join_stage = cast!(ExecutionS::ProjectionEvaluating, &mut self.state)?;
    let schema = &join_stage.schema;
    let join_select = &self.sql_query;

    // Collect GRQueries for top level
    let mut it = QueryIterator::new_top_level();
    let mut gr_queries = Vec::<proc::GRQuery>::new();
    it.iterate_select_items(&mut gr_query_collecting_cb(&mut gr_queries), &join_select.projection);

    // Get TableViews
    let finished = join_stage.get_finished();

    let mut simple = SubqueryExtractorSimple::new(
      &self.context.context_schema,
      schema,
      &vec![],
      &gr_queries,
      &results,
    );

    let mut pre_agg_table_views = Vec::<TableView>::new();

    // Iterate over the Parent rows.
    for (i, parent_context_row) in self.context.context_rows.iter().enumerate() {
      // Start constructing the `col_map` (needed for evaluating the conjunctions).
      let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
      add_vals(
        &mut col_map,
        &&self.context.context_schema.column_context_schema,
        &parent_context_row.column_context_row,
      );

      // Initialize the TableView we would add to the final joined result.
      let mut finished_table_view = TableView::new();

      // Get the joined table.
      let joined_table_view = finished.get_for_parent_context_row(i);

      // Iterate over rows.
      for (row, _) in &joined_table_view.rows {
        add_vals_general(&mut col_map, schema, row);

        let vals = simple.get_results(parent_context_row, row, &vec![]);

        // The projection result.
        // TODO share with `evaluate_super_simple_select`?
        let exec = || -> Result<_, EvalError> {
          let mut projection = Vec::<ColValN>::new();
          let subquery_vals = extract_subquery_vals(&vals)?;
          let mut next_subquery_idx = 0;
          for item in &join_select.projection {
            match item {
              proc::SelectItem::ExprWithAlias { item, .. } => {
                let expr = match item {
                  proc::SelectExprItem::ValExpr(expr) => expr,
                  proc::SelectExprItem::UnaryAggregate(unary_agg) => &unary_agg.expr,
                };
                let c_expr =
                  construct_cexpr(expr, &col_map, &subquery_vals, &mut next_subquery_idx)?;
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

          Ok(projection)
        };

        let projection = match exec() {
          Ok(projection) => projection,
          Err(eval_error) => return Some(TPESAction::QueryError(mk_eval_error(eval_error))),
        };

        finished_table_view.add_row(projection);
      }

      pre_agg_table_views.push(finished_table_view);
    }

    // Apply aggregation logic.
    match perform_aggregation(join_select, pre_agg_table_views) {
      Ok(table_views) => Some(TPESAction::Success(QueryESResult {
        result: table_views,
        new_rms: self.new_rms.iter().cloned().collect(),
      })),
      Err(eval_error) => return Some(TPESAction::QueryError(mk_eval_error(eval_error))),
    }
  }

  /// This Exits and Cleans up this GRQueryES.
  pub fn exit_and_clean_up<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    _: &mut Ctx,
    _: &mut IO,
  ) {
    self.state = ExecutionS::Done;
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
        let (context, parent_to_child_context_map) =
          build_join_node_context(&eval_data.context, inner.left.deref());

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
        let (context, parent_to_child_context_map) =
          build_join_node_context(&eval_data.context, inner.right.deref());

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

/// Build the Context and `parent_to_child_map` that should be used by a JoinNode.
fn build_join_node_context(
  parent_context: &Context,
  join_node: &proc::JoinNode,
) -> (Context, Vec<usize>) {
  // Build Child Context
  let mut parent_to_child_context_map = Vec::<usize>::new();
  let mut simple = ContextConstructorSimpleOnce::new(
    &parent_context.context_schema,
    &vec![],
    &vec![],
    QueryElement::JoinNode(join_node),
  );

  // Iterate over parent context rows and build out the context for the root JoinNode.
  for (_, parent_context_row) in parent_context.context_rows.iter().enumerate() {
    simple.add_row(parent_context_row, &vec![], &vec![]);
    parent_to_child_context_map.push(simple.context.context_rows.len());
  }

  (simple.context, parent_to_child_context_map)
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

/// Create an empty row of `ColValN`s of the given length.
fn make_empty_row(length: usize) -> (Vec<ColValN>, u64) {
  let mut row = Vec::<ColValN>::new();
  row.resize(length, None);
  (row, 1)
}

struct ContextConstructorSimpleOnce {
  context: Context,
  locations: Locations,
  context_row_set: BTreeSet<ContextRow>,
}

impl ContextConstructorSimpleOnce {
  fn new(
    parent_context_schema: &ContextSchema,
    first_schema: &Vec<GeneralColumnRef>,
    second_schema: &Vec<GeneralColumnRef>,
    elem: QueryElement,
  ) -> ContextConstructorSimpleOnce {
    let (mut context, locations) = initialize_contexts_general_once(
      &parent_context_schema,
      &first_schema,
      &second_schema,
      elem.clone(),
    );
    let mut context_row_set = BTreeSet::<ContextRow>::new();
    ContextConstructorSimpleOnce { context, locations, context_row_set }
  }

  fn add_row(
    &mut self,
    parent_context_row: &ContextRow,
    first_row: &Vec<ColValN>,
    second_row: &Vec<ColValN>,
  ) {
    let context_row = mk_context_row(&self.locations, parent_context_row, first_row, second_row);

    // Add in the ContextRow if it has not been added yet.
    if !self.context_row_set.contains(&context_row) {
      self.context_row_set.insert(context_row.clone());
      self.context.context_rows.push(context_row);
    }
  }
}

struct ContextConstructorSimple {
  simples: Vec<ContextConstructorSimpleOnce>,
}

impl ContextConstructorSimple {
  fn new(
    parent_context_schema: &ContextSchema,
    first_schema: &Vec<GeneralColumnRef>,
    second_schema: &Vec<GeneralColumnRef>,
    gr_queries: &Vec<proc::GRQuery>,
  ) -> ContextConstructorSimple {
    let mut simples = Vec::<ContextConstructorSimpleOnce>::new();
    for gr_query in gr_queries {
      simples.push(ContextConstructorSimpleOnce::new(
        parent_context_schema,
        first_schema,
        second_schema,
        QueryElement::GRQuery(gr_query),
      ));
    }
    ContextConstructorSimple { simples }
  }

  fn add_row(
    &mut self,
    parent_context_row: &ContextRow,
    first_row: &Vec<ColValN>,
    second_row: &Vec<ColValN>,
  ) {
    for simple in &mut self.simples {
      simple.add_row(parent_context_row, first_row, second_row);
    }
  }

  fn to_contexts(self) -> Vec<Context> {
    let mut contexts = Vec::<Context>::new();
    for simple in self.simples {
      contexts.push(simple.context);
    }
    contexts
  }
}

struct SubqueryExtractorSimple<'a> {
  contexts: Vec<Context>,
  all_locations: Vec<Locations>,
  context_row_maps: Vec<BTreeMap<ContextRow, usize>>,
  result: &'a Vec<Vec<TableView>>,
}

impl<'a> SubqueryExtractorSimple<'a> {
  fn new(
    parent_context_schema: &ContextSchema,
    first_schema: &Vec<GeneralColumnRef>,
    second_schema: &Vec<GeneralColumnRef>,
    gr_queries: &Vec<proc::GRQuery>,
    result: &'a Vec<Vec<TableView>>,
  ) -> SubqueryExtractorSimple<'a> {
    let (mut contexts, all_locations) =
      initialize_contexts(&parent_context_schema, &first_schema, &second_schema, gr_queries);
    let mut context_row_maps = Vec::<BTreeMap<ContextRow, usize>>::new();
    context_row_maps.resize(contexts.len(), BTreeMap::new());
    SubqueryExtractorSimple { contexts, all_locations, context_row_maps, result }
  }

  fn get_results(
    &mut self,
    parent_context_row: &ContextRow,
    first_row: &Vec<ColValN>,
    second_row: &Vec<ColValN>,
  ) -> Vec<TableView> {
    let mut raw_subquery_vals = Vec::<TableView>::new();
    for i in 0..self.context_row_maps.len() {
      let context_row_map = self.context_row_maps.get_mut(i).unwrap();
      let context_row = mk_context_row(
        self.all_locations.get(i).unwrap(),
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
      let cur_result = self.result.get(i).unwrap();
      let table_view = cur_result.get(*index).unwrap();
      raw_subquery_vals.push(table_view.clone());
    }

    raw_subquery_vals
  }
}

// Evaluate Conjunctions
fn evaluate_conjunctions(
  conjunctions: &Vec<proc::ValExpr>,
  col_map: &BTreeMap<proc::ColumnRef, ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<bool, EvalError> {
  let subquery_vals = extract_subquery_vals(&raw_subquery_vals)?;
  let mut next_subquery_idx = 0;
  for expr in conjunctions {
    if !is_true(&evaluate_c_expr(&construct_cexpr(
      expr,
      &col_map,
      &subquery_vals,
      &mut next_subquery_idx,
    )?)?)? {
      return Ok(false);
    }
  }
  Ok(true)
}

// Create a helper function.
fn finish_independent_join(
  first_eval_data: JoinNodeEvalData,
  second_eval_data: JoinNodeEvalData,
  parent_eval_data: &mut JoinNodeEvalData,
  first_schema: &Vec<GeneralColumnRef>,
  second_schema: &Vec<GeneralColumnRef>,
  parent_schema: &Vec<GeneralColumnRef>,
  parent_inner: &proc::JoinInnerNode,
  weak_gr_queries: Vec<proc::GRQuery>,
  strong_gr_queries: Vec<proc::GRQuery>,
  weak_result: Vec<Vec<TableView>>,
  strong_result: Vec<Vec<TableView>>,
  first_outer: bool,
  second_outer: bool,
) -> Result<(), EvalError> {
  let make_parent_row = |first_row: &Vec<ColValN>, second_row: &Vec<ColValN>| -> Vec<ColValN> {
    make_parent_row(first_schema, first_row, second_schema, second_row, parent_schema)
  };

  // Get TableViews
  let first_finished = first_eval_data.cast_finished();
  let second_finished = second_eval_data.cast_finished();

  // Construct Contextu construction utilities.
  let parent_context = &parent_eval_data.context;

  let mut weak_simple = SubqueryExtractorSimple::new(
    &parent_context.context_schema,
    &first_schema,
    &second_schema,
    &weak_gr_queries,
    &weak_result,
  );

  let mut strong_simple = SubqueryExtractorSimple::new(
    &parent_context.context_schema,
    &first_schema,
    &second_schema,
    &strong_gr_queries,
    &strong_result,
  );

  let parent_context = &parent_eval_data.context;
  let mut finished_table_views = Vec::<TableView>::new();

  // Iterate over the Parent rows.
  for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
    // Start constructing the `col_map` (needed for evaluating the conjunctions).
    let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
    add_vals(
      &mut col_map,
      &parent_eval_data.context.context_schema.column_context_schema,
      &parent_context_row.column_context_row,
    );

    // Initialize the TableView we would add to the final joined result.
    let mut table_view = TableView::new();

    // Get both Sides.
    let first_table_view = first_finished.get_for_parent_context_row(i);
    let second_table_view = second_finished.get_for_parent_context_row(i);

    // Holds all rows on the Second Side that were unrejected by weak conjunctions.
    let mut second_side_weak_unrejected = BTreeSet::<Vec<ColValN>>::new();

    // Iterate over First Side rows.
    for (first_row, first_count) in &first_table_view.rows {
      let first_count = *first_count;
      add_vals_general(&mut col_map, first_schema, first_row);

      let mut all_weak_rejected = true;

      // Iterate over Second Side rows.
      for (second_row, second_count) in &second_table_view.rows {
        let second_count = *second_count;
        add_vals_general(&mut col_map, second_schema, second_row);

        // Evaluate the weak conjunctions.
        let vals = weak_simple.get_results(parent_context_row, first_row, second_row);
        if evaluate_conjunctions(&parent_inner.weak_conjunctions, &col_map, &vals)? {
          continue;
        }

        // Mark that at least one pair of `first_row` and `second_row` was not rejected.
        all_weak_rejected = false;
        second_side_weak_unrejected.insert(second_row.clone());

        // Evaluate the strong conjunctions.
        let vals = strong_simple.get_results(parent_context_row, first_row, second_row);
        if evaluate_conjunctions(&parent_inner.strong_conjunctions, &col_map, &vals)? {
          continue;
        }

        // Add the row to the TableView.
        let joined_row = make_parent_row(first_row, &second_row);
        table_view.add_row_multi(joined_row, first_count * second_count);
      }

      // Here, we check whether we need to manufacture a row
      if all_weak_rejected && first_outer {
        // Construct an artificial row
        let (second_row, second_count) = make_empty_row(second_schema.len());
        add_vals_general(&mut col_map, second_schema, &second_row);

        // Evaluate the strong conjunctions.
        let vals = strong_simple.get_results(parent_context_row, first_row, &second_row);
        if evaluate_conjunctions(&parent_inner.strong_conjunctions, &col_map, &vals)? {
          // Add the row to the TableView.
          let joined_row = make_parent_row(first_row, &second_row);
          table_view.add_row_multi(joined_row, first_count * second_count);
        }
      }
    }

    // Perform row-readdition on the Second Side if need be.
    if second_outer {
      // Construct an artificial row
      let (first_row, first_count) = make_empty_row(first_schema.len());
      add_vals_general(&mut col_map, first_schema, &first_row);

      // Iterate over the Second Side.
      for (second_row, second_count) in &second_table_view.rows {
        let second_count = *second_count;

        // Check that the `second_row` was always rejected by weak conjunctions.
        if !second_side_weak_unrejected.contains(second_row) {
          add_vals_general(&mut col_map, second_schema, &second_row);

          // Evaluate the strong conjunctions.
          let vals = strong_simple.get_results(parent_context_row, &first_row, second_row);
          if evaluate_conjunctions(&parent_inner.strong_conjunctions, &col_map, &vals)? {
            // Add the row to the TableView.
            let joined_row = make_parent_row(&first_row, second_row);
            table_view.add_row_multi(joined_row, first_count * second_count);
          }
        }
      }
    }

    finished_table_views.push(table_view);
  }

  // Move the parent stage to the finished stage
  parent_eval_data.result_state = ResultState::Finished(finished_table_views);
  Ok(())
}

// Create a helper function.
fn finish_dependent_join(
  first_eval_data: JoinNodeEvalData,
  second_eval_data: JoinNodeEvalData,
  parent_eval_data: &mut JoinNodeEvalData,
  first_schema: &Vec<GeneralColumnRef>,
  second_schema: &Vec<GeneralColumnRef>,
  parent_schema: &Vec<GeneralColumnRef>,
  parent_inner: &proc::JoinInnerNode,
  weak_gr_queries: Vec<proc::GRQuery>,
  strong_gr_queries: Vec<proc::GRQuery>,
  weak_result: Vec<Vec<TableView>>,
  strong_result: Vec<Vec<TableView>>,
) -> Result<(), EvalError> {
  let make_parent_row = |first_row: &Vec<ColValN>, second_row: &Vec<ColValN>| -> Vec<ColValN> {
    make_parent_row(first_schema, first_row, second_schema, second_row, parent_schema)
  };

  // Get TableViews
  let first_finished = first_eval_data.cast_finished();
  let second_finished = second_eval_data.cast_finished();

  // Construct Contextu construction utilities.
  let parent_context = &parent_eval_data.context;

  let mut weak_simple = SubqueryExtractorSimple::new(
    &parent_context.context_schema,
    &first_schema,
    &second_schema,
    &weak_gr_queries,
    &weak_result,
  );

  let mut strong_simple = SubqueryExtractorSimple::new(
    &parent_context.context_schema,
    &first_schema,
    &second_schema,
    &strong_gr_queries,
    &strong_result,
  );

  let mut finished_table_views = Vec::<TableView>::new();

  // Iterate over the Parent rows.
  let mut second_side_count = 0;
  for (i, parent_context_row) in parent_context.context_rows.iter().enumerate() {
    // Start constructing the `col_map` (needed for evaluating the conjunctions).
    let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
    add_vals(
      &mut col_map,
      &parent_eval_data.context.context_schema.column_context_schema,
      &parent_context_row.column_context_row,
    );

    // Initialize the TableView we would add to the final joined result.
    let mut table_view = TableView::new();

    // Get the First Side
    let first_table_view = first_finished.get_for_parent_context_row(i);

    // Iterate over First Side rows.
    for (first_row, first_count) in &first_table_view.rows {
      let first_count = *first_count;
      add_vals_general(&mut col_map, first_schema, first_row);

      let mut all_weak_rejected = true;

      // Get the Second Side.
      let second_table_view = second_finished.get_for_parent_context_row(second_side_count);
      second_side_count += 1;

      // Iterate over Second Side rows.
      for (second_row, second_count) in &second_table_view.rows {
        let second_count = *second_count;
        add_vals_general(&mut col_map, second_schema, second_row);

        // Evaluate the weak conjunctions.
        let vals = weak_simple.get_results(parent_context_row, first_row, second_row);
        if evaluate_conjunctions(&parent_inner.weak_conjunctions, &col_map, &vals)? {
          continue;
        }

        // Mark that at least one pair of `first_row` and `second_row` was not rejected.
        all_weak_rejected = false;

        // Evaluate the strong conjunctions.
        let vals = strong_simple.get_results(parent_context_row, first_row, second_row);
        if evaluate_conjunctions(&parent_inner.strong_conjunctions, &col_map, &vals)? {
          continue;
        }

        // Add the row to the TableView.
        let joined_row = make_parent_row(first_row, &second_row);
        table_view.add_row_multi(joined_row, first_count * second_count);
      }

      // Here, we check whether we need to manufacture a row
      if all_weak_rejected {
        // Construct an artificial row.
        let (second_row, second_count) = make_empty_row(second_schema.len());
        add_vals_general(&mut col_map, second_schema, &second_row);

        // Evaluate the strong conjunctions.
        let vals = strong_simple.get_results(parent_context_row, first_row, &second_row);
        if evaluate_conjunctions(&parent_inner.strong_conjunctions, &col_map, &vals)? {
          // Add the row to the TableView.
          let joined_row = make_parent_row(first_row, &second_row);
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
