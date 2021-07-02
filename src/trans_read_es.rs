use crate::col_usage::{collect_select_subqueries, ColUsagePlanner};
use crate::common::{lookup_pos, mk_qid, IOTypes, NetworkOut, OrigP, QueryPlan};
use crate::expression::{is_true, EvalError};
use crate::model::common::{
  proc, ColName, ColType, ColValN, ContextRow, TableView, Timestamp, TransTableName,
};
use crate::model::common::{Context, QueryId, QueryPath, TierMap, TransTableLocationPrefix};
use crate::model::message as msg;
use crate::server::{evaluate_super_simple_select, mk_eval_error, CommonQuery, ServerContext};
use crate::tablet::{
  ContextConverter, Executing, GRExecutionS, GRQueryES, GRQueryPlan, QueryReplanningSqlView,
  SingleSubqueryStatus, SubqueryFinished, SubqueryPending, SubqueryStatus,
};
use crate::trans_read_es::TransTableAction::Wait;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

trait TransTableSource {
  fn get_instance(&mut self, prefix: &TransTableLocationPrefix, idx: usize) -> &TableView;
  fn get_schema(&self, prefix: &TransTableLocationPrefix) -> Vec<ColName>;
}

#[derive(Debug)]
enum TransExecutionS {
  Start,
  Executing(Executing),
}

#[derive(Debug)]
struct TransTableReadES {
  root_query_path: QueryPath,
  tier_map: TierMap,
  location_prefix: TransTableLocationPrefix,
  context: Rc<Context>,

  // Fields needed for responding.
  sender_path: QueryPath,
  query_id: QueryId,

  // Query-related fields.
  sql_query: proc::SuperSimpleSelect,
  query_plan: QueryPlan,

  // Dynamically evolving fields.
  new_rms: HashSet<QueryPath>,
  state: TransExecutionS,

  // Convenience fields
  timestamp: Timestamp, // The timestamp read from the GRQueryES
}

#[derive(Debug)]
enum TransQueryReplanningS {
  Start,
  /// Used to wait on the master
  MasterQueryReplanning {
    master_query_id: QueryId,
  },
  Done(bool),
}

#[derive(Debug)]
struct TransQueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to TableReadES.
  root_query_path: QueryPath,
  tier_map: TierMap,
  query_id: QueryId,

  // These members are parallel to the messages in `msg::GeneralQuery`.
  location_prefix: TransTableLocationPrefix,
  context: Rc<Context>,
  sql_query: proc::SuperSimpleSelect,
  query_plan: QueryPlan,

  /// Path of the original sender (needed for responding with errors).
  sender_path: QueryPath,
  /// The OrigP of the Task holding this CommonQueryReplanningES
  orig_p: OrigP,
  /// The state of the CommonQueryReplanningES
  state: TransQueryReplanningS,

  // Convenience fields
  timestamp: Timestamp, // The timestamp read from the GRQueryES
}

enum TransTableAction {
  /// This tells the parent Server to wait. This is used after this ES sends
  /// out a MasterQueryReplanning, while it's waiting for subqueries, etc.
  Wait,
  /// This tells the parent Server to perform subqueries.
  SendSubqueries(Vec<GRQueryES>),
  /// This tells the parent Server that this TransTableReadES is done (having
  /// sent back responses, etc).
  Done,
}

#[derive(Debug)]
enum FullTransTableReadES {
  QueryReplanning(TransQueryReplanningES),
  Executing(TransTableReadES),
}

impl FullTransTableReadES {
  fn start<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &mut SourceT,
  ) -> TransTableAction {
    let plan_es = cast!(FullTransTableReadES::QueryReplanning, self).unwrap();
    plan_es.start::<T, SourceT>(ctx, trans_table_source);
    if let TransQueryReplanningS::Done(success) = &plan_es.state {
      if *success {
        // If the QueryReplanning was successful, we move the FullTransTableReadES
        // to Executing in the Start state, and immediately start executing it.
        *self = FullTransTableReadES::Executing(TransTableReadES {
          root_query_path: plan_es.root_query_path.clone(),
          tier_map: plan_es.tier_map.clone(),
          location_prefix: plan_es.location_prefix.clone(),
          context: plan_es.context.clone(),
          sender_path: plan_es.sender_path.clone(),
          query_id: plan_es.query_id.clone(),
          sql_query: plan_es.sql_query.clone(),
          query_plan: plan_es.query_plan.clone(),
          new_rms: Default::default(),
          state: TransExecutionS::Start,
          timestamp: plan_es.timestamp,
        });
        self.start_trans_table_read_es(ctx, trans_table_source)
      } else {
        // If the replanning was unsuccessful, the Abort message should already have
        // been sent, and we may exit.
        TransTableAction::Done
      }
    } else {
      TransTableAction::Wait
    }
  }

  fn start_trans_table_read_es<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &mut SourceT,
  ) -> TransTableAction {
    let es = cast!(FullTransTableReadES::Executing, self).unwrap();
    assert!(matches!(&es.state, &TransExecutionS::Start));
    let gr_query_statuses = match compute_trans_table_subqueries::<T, SourceT>(
      ctx.rand,
      &es.location_prefix,
      trans_table_source,
      &es.query_id,
      &es.root_query_path,
      &es.tier_map,
      &collect_select_subqueries(&es.sql_query),
      &es.timestamp,
      &es.context,
      &es.query_plan,
    ) {
      Ok(gr_query_statuses) => gr_query_statuses,
      Err(eval_error) => {
        self.send_query_error(ctx, mk_eval_error(eval_error));
        return TransTableAction::Done;
      }
    };

    // Here, we have computed all GRQueryESs, and we can now add them to
    // `subquery_status`.
    let mut gr_query_ids = Vec::<QueryId>::new();
    let mut subquery_status = SubqueryStatus { subqueries: Default::default() };
    for gr_query_es in &gr_query_statuses {
      let query_id = gr_query_es.query_id.clone();
      gr_query_ids.push(query_id.clone());
      subquery_status.subqueries.insert(
        query_id.clone(),
        SingleSubqueryStatus::Pending(SubqueryPending { context: gr_query_es.context.clone() }),
      );
    }

    // Move the ES to the Executing state.
    es.state = TransExecutionS::Executing(Executing {
      completed: 0,
      subquery_pos: gr_query_ids.clone(),
      subquery_status,
      row_region: vec![], // This doesn't make sense for TransTables...
    });

    // Return the subqueries so that they can be driven from the parent Server.
    return TransTableAction::SendSubqueries(gr_query_statuses);
  }

  /// Handles a Subquery completing
  fn handle_subquery_done<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &mut SourceT,
    subquery_id: QueryId,
    subquery_new_rms: HashSet<QueryPath>,
    (_, table_views): (Vec<ColName>, Vec<TableView>),
  ) -> TransTableAction {
    let es = cast!(FullTransTableReadES::Executing, self).unwrap();

    // Add the subquery results into the TableReadES.
    es.new_rms.extend(subquery_new_rms);
    let executing_state = cast!(TransExecutionS::Executing, &mut es.state).unwrap();
    let subquery_status = &mut executing_state.subquery_status;
    let single_status = subquery_status.subqueries.get_mut(&subquery_id).unwrap();
    let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context.clone();
    *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
      context: context.clone(),
      result: table_views,
    });
    executing_state.completed += 1;

    // If all subqueries have been evaluated, finish the TransTableReadES
    // and respond to the client.
    if executing_state.completed == subquery_status.subqueries.len() {
      // Get the GRQueryES so that
      let num_subqueries = executing_state.subquery_pos.len();

      // Compute the position in the TransTableContextRow that we can use to
      // get the index of current TransTableInstance.
      let trans_table_name = es.location_prefix.trans_table_name.clone();
      let trans_table_name_pos = context
        .context_schema
        .trans_table_context_schema
        .iter()
        .position(|prefix| &prefix.trans_table_name == &trans_table_name)
        .unwrap();

      // Get all TransTableInstances.
      let trans_table_schema = trans_table_source.get_schema(&es.location_prefix);

      // Construct the ContextConverters for all subqueries
      let mut converters = Vec::<ContextConverter>::new();
      for subquery_id in &executing_state.subquery_pos {
        let single_status = subquery_status.subqueries.get(subquery_id).unwrap();
        let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
        let context_schema = &result.context.context_schema;
        converters.push(ContextConverter::trans_general_create(
          &es.context.context_schema,
          &trans_table_schema,
          context_schema.column_context_schema.clone(),
          context_schema.trans_table_names(),
        ));
      }

      // Setup the child_context_row_maps that will be populated over time.
      let mut child_context_row_maps = Vec::<HashMap<ContextRow, usize>>::new();
      for _ in 0..num_subqueries {
        child_context_row_maps.push(HashMap::new());
      }

      // Compute the Schema of the TableView that will be returned by this TransTableReadES.
      let mut res_col_names = es.sql_query.projection.clone();
      let mut res_table_views = Vec::<TableView>::new();
      for context_row in &es.context.context_rows {
        // Lookup the relevent TransTableInstance at the given ContextRow
        let trans_table_instance_pos =
          context_row.trans_table_context_row.get(trans_table_name_pos).unwrap();
        let trans_table_instance =
          trans_table_source.get_instance(&es.location_prefix, *trans_table_instance_pos);

        // Next, we initialize the TableView that we are trying to construct
        // for this `context_row`.
        let mut res_table_view =
          TableView { col_names: res_col_names.clone(), rows: Default::default() };

        for (trans_table_row, count) in &trans_table_instance.rows {
          // Now, we compute the subquery result for all subqueries for this
          // `context_row` + `trans_table_row`.
          let mut subquery_vals = Vec::<TableView>::new();
          for index in 0..num_subqueries {
            // Compute the child ContextRow for this subquery, and populate
            // `child_context_row_map` accordingly.
            let conv = converters.get(index).unwrap();
            let child_context_row_map = child_context_row_maps.get_mut(index).unwrap();
            let row = conv.extract_child_relevent_cols(&trans_table_schema, trans_table_row);
            let new_context_row = conv.compute_child_context_row(context_row, row);
            if !child_context_row_map.contains_key(&new_context_row) {
              let idx = child_context_row_map.len();
              child_context_row_map.insert(new_context_row.clone(), idx);
            }

            // Get the child_context_idx to get the relevent TableView from the subquery
            // results, and populate `subquery_vals`.
            let child_context_idx = child_context_row_map.get(&new_context_row).unwrap();
            let subquery_id = executing_state.subquery_pos.get(index).unwrap();
            let single_status = subquery_status.subqueries.get(subquery_id).unwrap();
            let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
            subquery_vals.push(result.result.get(*child_context_idx).unwrap().clone());
          }

          /// Now, we evaluate all expressions in the SQL query and amend the
          /// result to this TableView (if the WHERE clause evaluates to true).
          let query = &es.sql_query;
          let context = &es.context;
          let eval_res = (|| {
            let evaluated_select = evaluate_super_simple_select(
              query,
              &context.context_schema.column_context_schema,
              &context_row.column_context_row,
              &trans_table_schema,
              &trans_table_row,
              &subquery_vals,
            )?;
            if is_true(&evaluated_select.selection)? {
              // This means that the current row should be selected for the result.
              // First, we take the projected columns.
              let mut res_row = Vec::<ColValN>::new();
              for res_col_name in &res_col_names {
                let idx = trans_table_schema.iter().position(|k| res_col_name == k).unwrap();
                res_row.push(trans_table_row.get(idx).unwrap().clone());
              }

              // Then, we add the `res_row` into the TableView. Note that since every row in
              // a TransTableInstance has a count associated, we need to add that many rows
              // to `res_table_view` as well.
              res_table_view.add_row_multi(res_row, *count);
            };
            Ok(())
          })();

          if let Err(eval_error) = eval_res {
            self.send_query_error(ctx, mk_eval_error(eval_error));
            return TransTableAction::Done;
          }
        }

        // Finally, accumulate the resulting TableView.
        res_table_views.push(res_table_view);
      }

      // Build the success message and respond.
      let success_msg = msg::QuerySuccess {
        return_path: es.sender_path.query_id.clone(),
        query_id: es.query_id.clone(),
        result: (es.sql_query.projection.clone(), res_table_views),
        new_rms: es.new_rms.iter().cloned().collect(),
      };
      let sender_path = es.sender_path.clone();
      ctx.send_to_path(sender_path, CommonQuery::QuerySuccess(success_msg));
      TransTableAction::Done
    } else {
      TransTableAction::Wait
    }
  }

  /// Sends a QueryError back to location of the `sender_path` in this ES.
  fn send_query_error<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    query_error: msg::QueryError,
  ) {
    let sender_path = match &self {
      FullTransTableReadES::QueryReplanning(es) => es.sender_path.clone(),
      FullTransTableReadES::Executing(es) => es.sender_path.clone(),
    };
    let aborted = msg::QueryAborted {
      return_path: sender_path.query_id.clone(),
      query_id: match &self {
        FullTransTableReadES::QueryReplanning(es) => es.query_id.clone(),
        FullTransTableReadES::Executing(es) => es.query_id.clone(),
      },
      payload: msg::AbortedData::QueryError(query_error),
    };
    ctx.send_to_path(sender_path, CommonQuery::QueryAborted(aborted));
  }
}

/// This computes GRQueryESs corresponding to every element in `subqueries` for TransTableReadESs.
fn compute_trans_table_subqueries<T: IOTypes, SourceT: TransTableSource>(
  // TabletContext params
  rand: &mut T::RngCoreT,
  location_prefix: &TransTableLocationPrefix,
  trans_table_source: &mut SourceT,
  // TabletStatus params
  query_id: &QueryId,
  root_query_path: &QueryPath,
  tier_map: &TierMap,
  subqueries: &Vec<proc::GRQuery>,
  timestamp: &Timestamp,
  context: &Context,
  query_plan: &QueryPlan,
) -> Result<Vec<GRQueryES>, EvalError> {
  // We first compute all GRQueryESs before adding them to `tablet_status`, in case
  // an error occurs here and we need to abort.

  // Compute the position in the TransTableContextRow that we can use to
  // get the index of current TransTableInstance.
  let trans_table_name_pos = context
    .context_schema
    .trans_table_context_schema
    .iter()
    .position(|prefix| &prefix.trans_table_name == &location_prefix.trans_table_name)
    .unwrap();

  let mut gr_query_statuses = Vec::<GRQueryES>::new();
  for subquery_index in 0..subqueries.len() {
    let subquery = subqueries.get(subquery_index).unwrap();
    let child = query_plan.col_usage_node.children.get(subquery_index).unwrap();

    // This computes a ContextSchema for the subquery, as well as exposes a conversion
    // utility to compute ContextRows.
    let conv = ContextConverter::create_from_query_plan(
      &context.context_schema,
      &query_plan.col_usage_node,
      subquery_index,
    );

    // Construct the `ContextRow`s. To do this, we iterate over main Query's
    // `ContextRow`s and then the corresponding `ContextRow`s for the subquery.
    // We hold the child `ContextRow`s in Vec, and we use a HashSet to avoid duplicates.
    let mut new_context_rows = Vec::<ContextRow>::new();
    let mut new_row_set = HashSet::<ContextRow>::new();
    for context_row in &context.context_rows {
      // Lookup the relevent TransTableInstance at the given ContextRow
      let trans_table_instance_pos =
        context_row.trans_table_context_row.get(trans_table_name_pos).unwrap();
      let trans_table_instance =
        trans_table_source.get_instance(location_prefix, *trans_table_instance_pos);
      // We take each row in the TransTableInstance, couple it with the main ContextRow, and
      // then get the child ContextRow from `conv`.
      for (row, _) in &trans_table_instance.rows {
        let new_context_row = conv.compute_child_context_row(context_row, row.clone());
        if !new_row_set.contains(&new_context_row) {
          new_row_set.insert(new_context_row.clone());
          new_context_rows.push(new_context_row);
        }
      }
    }

    // Finally, compute the context.
    let context =
      Rc::new(Context { context_schema: conv.context_schema, context_rows: new_context_rows });

    // Construct the GRQueryES
    let gr_query_id = mk_qid(rand);
    let gr_query_es = GRQueryES {
      root_query_path: root_query_path.clone(),
      tier_map: tier_map.clone(),
      timestamp: timestamp.clone(),
      context,
      new_trans_table_context: vec![],
      query_id: gr_query_id.clone(),
      sql_query: subquery.clone(),
      query_plan: GRQueryPlan {
        gossip_gen: query_plan.gossip_gen.clone(),
        trans_table_schemas: query_plan.trans_table_schemas.clone(),
        col_usage_nodes: child.clone(),
      },
      new_rms: Default::default(),
      trans_table_view: vec![],
      state: GRExecutionS::Start,
      orig_p: OrigP::new(query_id.clone()),
    };
    gr_query_statuses.push(gr_query_es)
  }

  Ok(gr_query_statuses)
}

impl TransQueryReplanningES {
  fn start<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &SourceT,
  ) {
    matches!(self.state, TransQueryReplanningS::Start);
    // First, verify that the select columns are in the TransTable.
    let schema_cols = trans_table_source.get_schema(&self.location_prefix);
    for col in &self.sql_query.projection {
      if !schema_cols.contains(col) {
        // One of the projected columns aren't in the schema, indicating that the
        // SuperSimpleTransTableSelect is invalid. Thus, we abort.
        let sender_path = self.sender_path.clone();
        let aborted_msg = msg::QueryAborted {
          return_path: sender_path.query_id.clone(),
          query_id: self.query_id.clone(),
          payload: msg::AbortedData::QueryError(msg::QueryError::LateralError),
        };
        ctx.send_to_path(sender_path, CommonQuery::QueryAborted(aborted_msg));
        self.state = TransQueryReplanningS::Done(true);
        return;
      }
    }

    // Next, we check the GossipGen of the QueryPlan.
    if ctx.gossip.gossip_gen <= self.query_plan.gossip_gen {
      // This means that the sender knew everything this Node knew and more when it made the
      // QueryPlan, so we can use it directly. Note that since there is no column locking
      // required, we can move to the Execting state immediately.
      self.state = TransQueryReplanningS::Done(true);
    } else {
      // This means we must recompute the QueryPlan.
      let mut planner = ColUsagePlanner {
        gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
        timestamp: self.timestamp,
      };
      let (_, col_usage_node) = planner.plan_stage_query_with_schema(
        &mut self.query_plan.trans_table_schemas.clone(),
        &self.sql_query.projection,
        &self.sql_query.from,
        schema_cols.clone(),
        &self.sql_query.exprs(),
      );

      // Update the QueryPlan
      let external_cols = col_usage_node.external_cols.clone();
      self.query_plan.gossip_gen = ctx.gossip.gossip_gen;
      self.query_plan.col_usage_node = col_usage_node;

      // Next, we check to see if all ColNames in `external_cols` is contaiend
      // in the Context. If not, we have to consult the Master.
      for col in external_cols {
        if !self.context.context_schema.column_context_schema.contains(&col) {
          // This means we need to consult the Master.
          let master_query_id = mk_qid(ctx.rand);

          ctx.network_output.send(
            &ctx.master_eid,
            msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
              msg::PerformMasterFrozenColUsage {
                query_id: master_query_id.clone(),
                timestamp: self.timestamp,
                trans_table_schemas: self.query_plan.trans_table_schemas.clone(),
                col_usage_tree: msg::ColUsageTree::MSQueryStage(self.sql_query.ms_query_stage()),
              },
            )),
          );
          ctx.master_query_map.insert(master_query_id.clone(), self.orig_p.clone());

          // Advance Replanning State.
          self.state = TransQueryReplanningS::MasterQueryReplanning { master_query_id };
          return;
        }
      }

      // If we get here that means we have successfully computed a valid QueryPlan,
      // and we are done.
      self.state = TransQueryReplanningS::Done(true);
    }
  }
}
