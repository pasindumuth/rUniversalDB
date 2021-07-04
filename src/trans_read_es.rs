use crate::col_usage::{collect_select_subqueries, ColUsagePlanner};
use crate::common::{lookup_pos, mk_qid, IOTypes, NetworkOut, OrigP, QueryPlan};
use crate::expression::{is_true, EvalError};
use crate::gr_query_es::{GRExecutionS, GRQueryES, GRQueryPlan};
use crate::model::common::{
  proc, ColName, ColType, ColValN, ContextRow, TableView, Timestamp, TransTableName,
};
use crate::model::common::{Context, QueryId, QueryPath, TierMap, TransTableLocationPrefix};
use crate::model::message as msg;
use crate::server::{
  evaluate_super_simple_select, mk_eval_error, CommonQuery, ContextConverter, ServerContext,
};
use crate::tablet::{
  Executing, QueryReplanningSqlView, SingleSubqueryStatus, SubqueryFinished, SubqueryPending,
  SubqueryStatus,
};
use crate::trans_read_es::TransTableAction::Wait;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

pub trait TransTableSource {
  fn get_instance(&self, prefix: &TransTableLocationPrefix, idx: usize) -> &TableView;
  fn get_schema(&self, prefix: &TransTableLocationPrefix) -> Vec<ColName>;
}

#[derive(Debug)]
pub enum TransExecutionS {
  Start,
  Executing(Executing),
}

#[derive(Debug)]
pub struct TransTableReadES {
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  pub location_prefix: TransTableLocationPrefix,
  pub context: Rc<Context>,

  // Fields needed for responding.
  pub sender_path: QueryPath,
  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,

  // Dynamically evolving fields.
  pub new_rms: HashSet<QueryPath>,
  pub state: TransExecutionS,

  // Convenience fields
  pub timestamp: Timestamp, // The timestamp read from the GRQueryES
}

#[derive(Debug)]
pub enum TransQueryReplanningS {
  Start,
  /// Used to wait on the master
  MasterQueryReplanning {
    master_query_id: QueryId,
  },
  Done(bool),
}

#[derive(Debug)]
pub struct TransQueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to TableReadES.
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  pub query_id: QueryId,

  // These members are parallel to the messages in `msg::GeneralQuery`.
  pub location_prefix: TransTableLocationPrefix,
  pub context: Rc<Context>,
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,

  /// Path of the original sender (needed for responding with errors).
  pub sender_path: QueryPath,
  /// The OrigP of the Task holding this CommonQueryReplanningES
  pub orig_p: OrigP,
  /// The state of the CommonQueryReplanningES
  pub state: TransQueryReplanningS,

  // Convenience fields
  pub timestamp: Timestamp, // The timestamp read from the GRQueryES
}

pub enum TransTableAction {
  /// This tells the parent Server to wait. This is used after this ES sends
  /// out a MasterQueryReplanning, while it's waiting for subqueries, etc.
  Wait,
  /// This tells the parent Server to perform subqueries.
  SendSubqueries(Vec<GRQueryES>),
  /// This tells the parent Server that this TransTableReadES has completed
  /// successfully (having already responded, etc).
  Done,
  /// This tells the parent Server that this TransTableReadES has completed
  /// unsuccessfully, and that the given `QueryId`s (subqueries) should be
  /// Exit and Cleaned Up, along with this one.
  ExitAndCleanUp(Vec<QueryId>),
}

#[derive(Debug)]
pub enum FullTransTableReadES {
  QueryReplanning(TransQueryReplanningES),
  Executing(TransTableReadES),
}

impl FullTransTableReadES {
  pub fn location_prefix(&self) -> &TransTableLocationPrefix {
    match self {
      FullTransTableReadES::QueryReplanning(es) => &es.location_prefix,
      FullTransTableReadES::Executing(es) => &es.location_prefix,
    }
  }

  pub fn start<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &SourceT,
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
        TransTableAction::ExitAndCleanUp(Vec::new())
      }
    } else {
      TransTableAction::Wait
    }
  }

  pub fn start_trans_table_read_es<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    let es = cast!(FullTransTableReadES::Executing, self).unwrap();
    assert!(matches!(&es.state, &TransExecutionS::Start));

    // We first compute all GRQueryESs before adding them to `tablet_status`, in case
    // an error occurs here and we need to abort.

    // Compute the position in the TransTableContextRow that we can use to
    // get the index of current TransTableInstance.
    let trans_table_name_pos = es
      .context
      .context_schema
      .trans_table_context_schema
      .iter()
      .position(|prefix| &prefix.trans_table_name == &es.location_prefix.trans_table_name)
      .unwrap();

    let subqueries = collect_select_subqueries(&es.sql_query);

    let mut gr_query_statuses = Vec::<GRQueryES>::new();
    for subquery_index in 0..subqueries.len() {
      let subquery = subqueries.get(subquery_index).unwrap();
      let child = es.query_plan.col_usage_node.children.get(subquery_index).unwrap();

      // This computes a ContextSchema for the subquery, as well as exposes a conversion
      // utility to compute ContextRows.
      let conv = ContextConverter::create_from_query_plan(
        &es.context.context_schema,
        &es.query_plan.col_usage_node,
        subquery_index,
      );

      // Construct the `ContextRow`s. To do this, we iterate over main Query's
      // `ContextRow`s and then the corresponding `ContextRow`s for the subquery.
      // We hold the child `ContextRow`s in Vec, and we use a HashSet to avoid duplicates.
      let mut new_context_rows = Vec::<ContextRow>::new();
      let mut new_row_set = HashSet::<ContextRow>::new();
      for context_row in &es.context.context_rows {
        // Lookup the relevent TransTableInstance at the given ContextRow
        let trans_table_instance_pos =
          context_row.trans_table_context_row.get(trans_table_name_pos).unwrap();
        let trans_table_instance =
          trans_table_source.get_instance(&es.location_prefix, *trans_table_instance_pos);
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
      let gr_query_id = mk_qid(ctx.rand);
      let gr_query_es = GRQueryES {
        root_query_path: es.root_query_path.clone(),
        tier_map: es.tier_map.clone(),
        timestamp: es.timestamp.clone(),
        context,
        new_trans_table_context: vec![],
        query_id: gr_query_id.clone(),
        sql_query: subquery.clone(),
        query_plan: GRQueryPlan {
          gossip_gen: es.query_plan.gossip_gen.clone(),
          trans_table_schemas: es.query_plan.trans_table_schemas.clone(),
          col_usage_nodes: child.clone(),
        },
        new_rms: Default::default(),
        trans_table_views: vec![],
        state: GRExecutionS::Start,
        orig_p: OrigP::new(es.query_id.clone()),
      };
      gr_query_statuses.push(gr_query_es)
    }

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

  /// Handles InternalColumnsDNE
  pub fn handle_internal_columns_dne<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &SourceT,
    subquery_id: QueryId,
    rem_cols: Vec<ColName>,
  ) -> TransTableAction {
    let es = cast!(FullTransTableReadES::Executing, self).unwrap();
    let executing = cast!(TransExecutionS::Executing, &mut es.state).unwrap();

    let trans_table_name = es.location_prefix.trans_table_name.clone();
    let trans_table_schema = trans_table_source.get_schema(&es.location_prefix);

    // First, we need to see if the missing columns are in the context. If not, then
    // we have to exit and clean up, and send an abort. If so, we need to lookup up
    // the right subquery, create a new subqueryId, construct a new context with a
    // recomputesubquery function, and then start executing it.

    // First, we check if all columns in `rem_cols` are at least present in the context.
    let mut missing_cols = Vec::<ColName>::new();
    for col in &rem_cols {
      if !trans_table_schema.contains(col) {
        if !es.context.context_schema.column_context_schema.contains(col) {
          missing_cols.push(col.clone());
        }
      }
    }

    if !missing_cols.is_empty() {
      // If there are missing columns, we construct a ColumnsDNE containing
      // `missing_cols`, send it back to the originator, and finish the ES.
      self.send_query_aborted(ctx, msg::AbortedData::ColumnsDNE { missing_cols });
      self.exit_and_clean_up(ctx)
    } else {
      // This means there are no missing columns, and so we can try the
      // GRQueryES again by extending its Context.

      // Find the subquery that just aborted. There should always be such a Subquery.
      let single_status = (|| {
        for (id, state) in &executing.subquery_status.subqueries {
          if id == &subquery_id {
            return Some(state);
          }
        }
        return None;
      })()
      .unwrap();
      let pending_status = cast!(SingleSubqueryStatus::Pending, single_status).unwrap();

      // We extend the ColumnContextSchema. Recall that we take the ContextSchema
      // of the previous subquery, rather than recomputing it from the QueryPlan
      // (since that might be out-of-date).
      let old_context_schema = &pending_status.context.context_schema;
      let mut new_columns = old_context_schema.column_context_schema.clone();
      new_columns.extend(rem_cols);

      let conv = ContextConverter::trans_general_create(
        &es.context.context_schema,
        &trans_table_schema,
        new_columns,
        old_context_schema.trans_table_names(),
      );

      // Compute the position in the TransTableContextRow that we can use to
      // get the index of current TransTableInstance.
      let trans_table_name_pos = es
        .context
        .context_schema
        .trans_table_context_schema
        .iter()
        .position(|prefix| &prefix.trans_table_name == &trans_table_name)
        .unwrap();

      // Construct the `ContextRow`s. To do this, we iterate over main Query's
      // `ContextRow`s and, then the rows of the corresponding TransTableInstance.
      // We hold the child `ContextRow`s in Vec, and we use a HashSet to avoid duplicates.
      let mut new_context_rows = Vec::<ContextRow>::new();
      let mut new_row_set = HashSet::<ContextRow>::new();
      for context_row in &es.context.context_rows {
        // Lookup the relevent TransTableInstance at the given ContextRow
        let trans_table_instance_pos =
          context_row.trans_table_context_row.get(trans_table_name_pos).unwrap();
        let trans_table_instance =
          trans_table_source.get_instance(&es.location_prefix, *trans_table_instance_pos);
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

      // We generate a new subquery ID to assign the new SingleSubqueryStatus,
      // as well as the corresponding GRQueryES.
      let new_subquery_id = mk_qid(ctx.rand);

      // Construct the GRQueryES
      let subqueries = collect_select_subqueries(&es.sql_query);
      let subquery_idx = executing.subquery_pos.iter().position(|id| &subquery_id == id).unwrap();
      let sql_subquery = subqueries.get(subquery_idx).unwrap().clone();
      let child = es.query_plan.col_usage_node.children.get(subquery_idx).unwrap();
      let gr_query_es = GRQueryES {
        root_query_path: es.root_query_path.clone(),
        tier_map: es.tier_map.clone(),
        timestamp: es.timestamp.clone(),
        context: context.clone(),
        new_trans_table_context: vec![],
        query_id: new_subquery_id.clone(),
        sql_query: sql_subquery,
        query_plan: GRQueryPlan {
          gossip_gen: es.query_plan.gossip_gen.clone(),
          trans_table_schemas: es.query_plan.trans_table_schemas.clone(),
          col_usage_nodes: child.clone(),
        },
        new_rms: Default::default(),
        trans_table_views: vec![],
        state: GRExecutionS::Start,
        orig_p: OrigP::new(es.query_id.clone()),
      };

      // Update the `executing` state to contain the new subquery_id.
      executing.subquery_status.subqueries.remove(&subquery_id);
      executing.subquery_pos[subquery_idx] = new_subquery_id.clone();
      executing.subquery_status.subqueries.insert(
        new_subquery_id.clone(),
        SingleSubqueryStatus::Pending(SubqueryPending { context }),
      );

      // Return the GRQueryESs for execution.
      TransTableAction::SendSubqueries(vec![gr_query_es])
    }
  }

  /// Handles a Subquery completing
  pub fn handle_subquery_done<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &SourceT,
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
            return self.exit_and_clean_up(ctx);
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

  /// This is can be called both for if a subquery fails, or if there is a LateralError
  /// due to the ES owning the TransTable disappears. This simply responds to the sender
  /// and Exits and Clean Ups this ES.
  /// TODO: I believe that this and `handle_internal_columns_dne` should be unified into
  /// one function that is essentially called when a child GRQueryES fails.
  pub fn handle_internal_query_error<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    query_error: msg::QueryError,
  ) -> TransTableAction {
    self.send_query_error(ctx, query_error);
    self.exit_and_clean_up(ctx)
  }

  /// This Cleans up any Master queries we launched and it returns instructions for the
  /// parent Server to follow to clean up subqueries.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut ServerContext<T>) -> TransTableAction {
    match self {
      FullTransTableReadES::QueryReplanning(es) => {
        match &es.state {
          TransQueryReplanningS::Start => {}
          TransQueryReplanningS::MasterQueryReplanning { master_query_id } => {
            // Remove if present
            if ctx.master_query_map.remove(&master_query_id).is_some() {
              // If the removal was successful, we should also send a Cancellation
              // message to the Master.
              ctx.network_output.send(
                &ctx.master_eid,
                msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
                  msg::CancelMasterFrozenColUsage { query_id: master_query_id.clone() },
                )),
              );
            }
          }
          TransQueryReplanningS::Done(_) => {}
        }
        TransTableAction::ExitAndCleanUp(Vec::new())
      }
      FullTransTableReadES::Executing(es) => {
        let mut subquery_ids = Vec::<QueryId>::new();
        match &es.state {
          TransExecutionS::Start => {}
          TransExecutionS::Executing(executing) => {
            // Here, we need to cancel every Subquery.
            for (query_id, single_query) in &executing.subquery_status.subqueries {
              match single_query {
                SingleSubqueryStatus::LockingSchemas(_) => panic!(),
                SingleSubqueryStatus::PendingReadRegion(_) => panic!(),
                SingleSubqueryStatus::Pending(_) => {
                  subquery_ids.push(query_id.clone());
                }
                SingleSubqueryStatus::Finished(_) => {}
              }
            }
          }
        }
        TransTableAction::ExitAndCleanUp(subquery_ids)
      }
    }
  }

  /// Get the `QueryPath` of the sender of this ES.
  pub fn get_sender_path(&self) -> QueryPath {
    match &self {
      FullTransTableReadES::QueryReplanning(es) => es.sender_path.clone(),
      FullTransTableReadES::Executing(es) => es.sender_path.clone(),
    }
  }

  /// Get the `QueryId` of the sender of this ES.
  pub fn get_query_id(&self) -> QueryId {
    match &self {
      FullTransTableReadES::QueryReplanning(es) => es.query_id.clone(),
      FullTransTableReadES::Executing(es) => es.query_id.clone(),
    }
  }

  /// Sends a QueryAborted with `payload` back to location of the `sender_path` in this ES.
  pub fn send_query_aborted<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    payload: msg::AbortedData,
  ) {
    let sender_path = self.get_sender_path();
    let aborted = msg::QueryAborted {
      return_path: sender_path.query_id.clone(),
      query_id: self.get_query_id(),
      payload,
    };
    ctx.send_to_path(sender_path, CommonQuery::QueryAborted(aborted));
  }

  /// Sends a QueryError back to location of the `sender_path` in this ES.
  pub fn send_query_error<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    query_error: msg::QueryError,
  ) {
    self.send_query_aborted(ctx, msg::AbortedData::QueryError(query_error));
  }
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
