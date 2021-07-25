use crate::col_usage::collect_top_level_cols;
use crate::common::{
  map_insert, mk_qid, IOTypes, KeyBound, OrigP, QueryESResult, QueryPlan, TableRegion,
};
use crate::expression::{compress_row_region, is_true, EvalError};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, ColName, ColType, ColVal, ColValN, Context, ContextRow, Gen, PrimaryKey, QueryId,
  QueryPath, TableView, TierMap, Timestamp, TransTableName,
};
use crate::model::message as msg;
use crate::ms_table_read_es::FullMSTableReadES;
use crate::query_replanning_es::{
  CommonQueryReplanningES, CommonQueryReplanningS, QueryReplanningAction, QueryReplanningSqlView,
};
use crate::server::{
  contains_col, evaluate_update, mk_eval_error, CommonQuery, ContextConstructor,
};
use crate::storage::{GenericTable, MSStorageView};
use crate::tablet::{
  compute_subqueries, recompute_subquery, ContextKeyboundComputer, Executing, MSQueryES, Pending,
  ProtectRequest, SingleSubqueryStatus, StorageLocalTable, SubqueryFinished,
  SubqueryLockingSchemas, SubqueryPending, SubqueryPendingReadRegion, TabletContext,
};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  MSTableWriteES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
enum MSWriteExecutionS {
  Start,
  Pending(Pending),
  Executing(Executing),
  Done,
}

#[derive(Debug)]
pub struct MSTableWriteES {
  root_query_path: QueryPath,
  tier_map: TierMap,
  timestamp: Timestamp,
  tier: u32,
  context: Rc<Context>,

  query_id: QueryId,

  // Query-related fields.
  sql_query: proc::Update,
  query_plan: QueryPlan,

  // MSQuery fields
  ms_query_id: QueryId,

  // Dynamically evolving fields.
  new_rms: HashSet<QueryPath>,
  state: MSWriteExecutionS,
}

#[derive(Debug)]
pub struct MSWriteQueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to MSTableWriteES.
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  pub ms_query_id: QueryId,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES<proc::Update>,
}

pub enum MSTableWriteAction {
  /// This tells the parent Server to wait.
  Wait,
  /// This tells the parent Server to perform subqueries.
  SendSubqueries(Vec<GRQueryES>),
  /// Indicates the ES succeeded with the given result.
  Success(QueryESResult),
  /// Indicates the ES failed with a QueryError.
  QueryError(msg::QueryError),
  /// Indicates the ES failed with insufficient columns in the Context.
  ColumnsDNE(Vec<ColName>),
}

#[derive(Debug)]
pub enum FullMSTableWriteES {
  QueryReplanning(MSWriteQueryReplanningES),
  Executing(MSTableWriteES),
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl FullMSTableWriteES {
  pub fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> MSTableWriteAction {
    let plan_es = cast!(Self::QueryReplanning, self).unwrap();
    let action = plan_es.status.start(ctx);
    self.handle_replanning_action(ctx, action)
  }

  /// Handle Columns being locked
  pub fn columns_locked<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    locked_cols_qid: QueryId,
  ) -> MSTableWriteAction {
    match self {
      FullMSTableWriteES::QueryReplanning(plan_es) => {
        let action = plan_es.status.columns_locked::<T>(ctx);
        self.handle_replanning_action(ctx, action)
      }
      FullMSTableWriteES::Executing(es) => {
        let executing = cast!(MSWriteExecutionS::Executing, &mut es.state).unwrap();

        // Find the SingleSubqueryStatus that sent out this requested_locked_columns.
        // There should always be such a Subquery.
        let subquery_idx = executing.find_subquery(&locked_cols_qid).unwrap();
        let single_status = executing.subqueries.get_mut(subquery_idx).unwrap();
        let locking_status = cast!(SingleSubqueryStatus::LockingSchemas, single_status).unwrap();

        // None of the `new_cols` should already exist in the old subquery Context schema
        // (since they didn't exist when the GRQueryES complained).
        for col in &locking_status.new_cols {
          assert!(!locking_status.old_columns.contains(col));
        }

        // Next, we compute the subset of `new_cols` that aren't in the Table
        // Schema or the Context.
        let mut missing_cols = Vec::<ColName>::new();
        for col in &locking_status.new_cols {
          if !contains_col(&ctx.table_schema, col, &es.timestamp) {
            if !es.context.context_schema.column_context_schema.contains(col) {
              missing_cols.push(col.clone());
            }
          }
        }

        if !missing_cols.is_empty() {
          // If there are missing columns, we ECU and propagate up the ColumnsDNE.
          self.exit_and_clean_up(ctx);
          MSTableWriteAction::ColumnsDNE(missing_cols)
        } else {
          // Here, we know all `new_cols` are in this Context, and so we can continue
          // trying to evaluate the subquery.

          // Now, add the `new_cols` to the schema
          let mut new_columns = locking_status.old_columns.clone();
          new_columns.extend(locking_status.new_cols.clone());

          // For ColNames in `new_cols` that belong to this Table, we need to
          // lock the region, so we compute a TableRegion accordingly.
          let mut new_col_region = Vec::<ColName>::new();
          for col in &locking_status.new_cols {
            if contains_col(&ctx.table_schema, col, &es.timestamp) {
              new_col_region.push(col.clone());
            }
          }
          let new_read_region =
            TableRegion { col_region: new_col_region, row_region: executing.row_region.clone() };

          // Add a read protection requested
          let protect_qid = mk_qid(&mut ctx.rand);
          let verifying_write = ctx.verifying_writes.get_mut(&es.timestamp).unwrap();
          verifying_write.m_waiting_read_protected.insert(ProtectRequest {
            orig_p: OrigP::new(es.query_id.clone()),
            protect_qid: protect_qid.clone(),
            read_region: new_read_region.clone(),
          });
          // Finally, update the SingleSubqueryStatus to wait for the Region Protection.
          *single_status = SingleSubqueryStatus::PendingReadRegion(SubqueryPendingReadRegion {
            new_columns,
            trans_table_names: locking_status.trans_table_names.clone(),
            query_id: protect_qid,
          });
          MSTableWriteAction::Wait
        }
      }
    }
  }

  /// Handle Master Response, routing it to the QueryReplanning.
  pub fn handle_master_response<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    gossip_gen: Gen,
    tree: msg::FrozenColUsageTree,
  ) -> MSTableWriteAction {
    let plan_es = cast!(FullMSTableWriteES::QueryReplanning, self).unwrap();
    let action = plan_es.status.handle_master_response(ctx, gossip_gen, tree);
    self.handle_replanning_action(ctx, action)
  }

  /// Handle the `action` sent back by the `CommonQueryReplanningES`.
  fn handle_replanning_action<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    action: QueryReplanningAction,
  ) -> MSTableWriteAction {
    match action {
      QueryReplanningAction::Wait => MSTableWriteAction::Wait,
      QueryReplanningAction::Success => {
        // If the QueryReplanning was successful, we move the FullMSTableWriteES
        // to Executing in the Start state, and immediately start executing it.
        let plan_es = cast!(FullMSTableWriteES::QueryReplanning, self).unwrap();
        let comm_plan_es = &mut plan_es.status;

        // First, we look up the `tier` of this Table being
        // written, update the `tier_map`.
        let sql_query = comm_plan_es.sql_view.clone();
        let mut tier_map = plan_es.tier_map.clone();
        let tier = tier_map.map.get(sql_query.table()).unwrap().clone();
        *tier_map.map.get_mut(sql_query.table()).unwrap() += 1;

        // Then, we construct the MSTableWriteES.
        *self = FullMSTableWriteES::Executing(MSTableWriteES {
          root_query_path: plan_es.root_query_path.clone(),
          tier_map,
          timestamp: comm_plan_es.timestamp,
          tier,
          context: comm_plan_es.context.clone(),
          query_id: comm_plan_es.query_id.clone(),
          sql_query,
          query_plan: comm_plan_es.query_plan.clone(),
          ms_query_id: plan_es.ms_query_id.clone(),
          new_rms: Default::default(),
          state: MSWriteExecutionS::Start,
        });

        // Finally, we start the MSWriteTableES.
        self.start_ms_table_write_es(ctx)
      }
      QueryReplanningAction::QueryError(query_error) => {
        // Forward up the QueryError. We do not need to ECU because
        // QueryReplanning will be in Done
        MSTableWriteAction::QueryError(query_error)
      }
      QueryReplanningAction::ColumnsDNE(missing_cols) => {
        // Forward up the QueryError. We do not need to ECU because
        // QueryReplanning will be in Done
        MSTableWriteAction::ColumnsDNE(missing_cols)
      }
    }
  }

  /// Handle ReadRegion protection
  pub fn read_protected<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    ms_query_es: &mut MSQueryES,
    protect_query_id: QueryId,
  ) -> MSTableWriteAction {
    let es = cast!(FullMSTableWriteES::Executing, self).unwrap();
    match &mut es.state {
      MSWriteExecutionS::Start => panic!(),
      MSWriteExecutionS::Pending(pending) => {
        let gr_query_statuses = match compute_subqueries::<T, _, _>(
          GRQueryConstructorView {
            root_query_path: &es.root_query_path,
            tier_map: &es.tier_map,
            timestamp: &es.timestamp,
            sql_query: &es.sql_query,
            query_plan: &es.query_plan,
            query_id: &es.query_id,
            context: &es.context,
          },
          &mut ctx.rand,
          StorageLocalTable::new(
            &ctx.table_schema,
            &es.timestamp,
            &es.sql_query.selection,
            MSStorageView::new(
              &ctx.storage,
              &ctx.table_schema,
              &ms_query_es.update_views,
              es.tier.clone(),
            ),
          ),
        ) {
          Ok(gr_query_statuses) => gr_query_statuses,
          Err(eval_error) => {
            es.state = MSWriteExecutionS::Done;
            return MSTableWriteAction::QueryError(mk_eval_error(eval_error));
          }
        };

        if gr_query_statuses.is_empty() {
          // Since there are no subqueries, we can go straight to finishing the ES.
          self.finish_ms_table_write_es(ctx, ms_query_es)
        } else {
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
          es.state = MSWriteExecutionS::Executing(Executing {
            completed: 0,
            subqueries,
            row_region: pending.read_region.row_region.clone(),
          });

          // Return the subqueries
          MSTableWriteAction::SendSubqueries(gr_query_statuses)
        }
      }
      MSWriteExecutionS::Executing(executing) => {
        let (_, gr_query_es) = match recompute_subquery::<T, _, _>(
          GRQueryConstructorView {
            root_query_path: &es.root_query_path,
            tier_map: &es.tier_map,
            timestamp: &es.timestamp,
            sql_query: &es.sql_query,
            query_plan: &es.query_plan,
            query_id: &es.query_id,
            context: &es.context,
          },
          &mut ctx.rand,
          StorageLocalTable::new(
            &ctx.table_schema,
            &es.timestamp,
            &es.sql_query.selection,
            MSStorageView::new(
              &ctx.storage,
              &ctx.table_schema,
              &ms_query_es.update_views,
              es.tier.clone(),
            ),
          ),
          executing,
          &protect_query_id,
        ) {
          Ok(gr_query_statuses) => gr_query_statuses,
          Err(eval_error) => {
            self.exit_and_clean_up(ctx);
            return MSTableWriteAction::QueryError(mk_eval_error(eval_error));
          }
        };

        // Return the subquery
        MSTableWriteAction::SendSubqueries(vec![gr_query_es])
      }
      MSWriteExecutionS::Done => panic!(),
    }
  }

  /// Handles InternalColumnsDNE
  pub fn handle_internal_columns_dne<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    subquery_id: QueryId,
    rem_cols: Vec<ColName>,
  ) -> MSTableWriteAction {
    let es = cast!(FullMSTableWriteES::Executing, self).unwrap();
    let executing = cast!(MSWriteExecutionS::Executing, &mut es.state).unwrap();

    // Insert a requested_locked_columns for the missing columns.
    let locked_cols_qid = ctx.add_requested_locked_columns(
      OrigP::new(es.query_id.clone()),
      es.timestamp,
      rem_cols.clone(),
    );

    // Get the SingleStatus
    let pos = executing.find_subquery(&subquery_id).unwrap();
    let single_status = executing.subqueries.get_mut(pos).unwrap();

    // Replace the the new SingleSubqueryStatus.
    let old_pending = cast!(SingleSubqueryStatus::Pending, single_status).unwrap();
    let old_context_schema = old_pending.context.context_schema.clone();
    *single_status = SingleSubqueryStatus::LockingSchemas(SubqueryLockingSchemas {
      old_columns: old_context_schema.column_context_schema.clone(),
      trans_table_names: old_context_schema.trans_table_names(),
      new_cols: rem_cols,
      query_id: locked_cols_qid,
    });

    MSTableWriteAction::Wait
  }

  /// This is called if a subquery fails.
  pub fn handle_internal_query_error<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    query_error: msg::QueryError,
  ) -> MSTableWriteAction {
    self.exit_and_clean_up(ctx);
    MSTableWriteAction::QueryError(query_error)
  }

  /// Handles a Subquery completing.
  pub fn handle_subquery_done<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    ms_query_es: &mut MSQueryES,
    subquery_id: QueryId,
    subquery_new_rms: HashSet<QueryPath>,
    (_, table_views): (Vec<ColName>, Vec<TableView>),
  ) -> MSTableWriteAction {
    let es = cast!(FullMSTableWriteES::Executing, self).unwrap();

    // Add the subquery results into the MSTableWriteES.
    es.new_rms.extend(subquery_new_rms);
    let executing_state = cast!(MSWriteExecutionS::Executing, &mut es.state).unwrap();
    let subquery_idx = executing_state.find_subquery(&subquery_id).unwrap();
    let single_status = executing_state.subqueries.get_mut(subquery_idx).unwrap();
    let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
    *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
      context: context.clone(),
      result: table_views,
    });
    executing_state.completed += 1;

    // If there are still subqueries to compute, then we wait. Otherwise, we finish
    // up the MSTableWriteES and respond to the client.
    if executing_state.completed < executing_state.subqueries.len() {
      MSTableWriteAction::Wait
    } else {
      self.finish_ms_table_write_es(ctx, ms_query_es)
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  pub fn finish_ms_table_write_es<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    ms_query_es: &mut MSQueryES,
  ) -> MSTableWriteAction {
    let es = cast!(FullMSTableWriteES::Executing, self).unwrap();
    let executing_state = cast!(MSWriteExecutionS::Executing, &mut es.state).unwrap();

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
    let storage_view = MSStorageView::new(
      &ctx.storage,
      &ctx.table_schema,
      &ms_query_es.update_views,
      es.tier.clone() - 1,
    );
    let context_constructor = ContextConstructor::new(
      es.context.context_schema.clone(),
      StorageLocalTable::new(
        &ctx.table_schema,
        &es.timestamp,
        &es.sql_query.selection,
        storage_view,
      ),
      children,
    );

    // These are all of the `ColNames` that we need in order to evaluate the Update.
    // This consists of all Top-Level Columns for every expression, as well as all Key
    // Columns (since they are included in the resulting table).
    let mut top_level_cols_set = HashSet::<ColName>::new();
    top_level_cols_set.extend(ctx.table_schema.get_key_cols());
    top_level_cols_set.extend(collect_top_level_cols(&es.sql_query.selection));
    for (_, expr) in &es.sql_query.assignment {
      top_level_cols_set.extend(collect_top_level_cols(expr));
    }
    let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

    // Setup the TableView that we are going to return and the UpdateView that we're going
    // to hold in the MSQueryES.
    let mut res_col_names = Vec::<ColName>::new();
    res_col_names.extend(ctx.table_schema.get_key_cols());
    res_col_names.extend(es.sql_query.assignment.iter().map(|(name, _)| name.clone()));
    let mut res_table_view = TableView::new(res_col_names.clone());
    let mut update_view = GenericTable::new();

    // Finally, iterate over the Context Rows of the subqueries and compute the final values.
    let eval_res = context_constructor.run(
      &es.context.context_rows,
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
        let evaluated_update = evaluate_update(
          &es.sql_query,
          &top_level_col_names,
          &top_level_col_vals,
          &subquery_vals,
        )?;
        if is_true(&evaluated_update.selection)? {
          // This means that the current row should be selected for the result.
          let mut res_row = Vec::<ColValN>::new();

          // First, we add in the Key Columns
          let mut primary_key = PrimaryKey { cols: vec![] };
          for (key_col, _) in &ctx.table_schema.key_cols {
            let idx = top_level_col_names.iter().position(|col| key_col == col).unwrap();
            let col_val = top_level_col_vals.get(idx).unwrap().clone();
            res_row.push(col_val.clone());
            primary_key.cols.push(col_val.unwrap());
          }

          // Then, iterate through the assignment, updating `res_row` and `update_view`.
          for (col_name, col_val) in evaluated_update.assignment {
            // We need to check that the Type of `col_val` conforms to the Table Schema.
            // Note that we only do this if `col_val` is non-NULL.
            if let Some(val) = &col_val {
              let col_type =
                ctx.table_schema.val_cols.strong_static_read(&col_name, es.timestamp).unwrap();
              let does_match = match (val, col_type) {
                (ColVal::Bool(_), ColType::Bool) => true,
                (ColVal::Int(_), ColType::Int) => true,
                (ColVal::String(_), ColType::String) => true,
                _ => false,
              };
              if !does_match {
                return Err(EvalError::TypeError);
              }
            }
            // Add in the `col_val`.
            res_row.push(col_val.clone());
            update_view.insert((primary_key.clone(), Some(col_name)), col_val);
          }

          // Finally, we add the `res_row` into the TableView.
          res_table_view.add_row_multi(res_row, count);
        };
        Ok(())
      },
    );

    if let Err(eval_error) = eval_res {
      es.state = MSWriteExecutionS::Done;
      return MSTableWriteAction::QueryError(mk_eval_error(eval_error));
    }

    // Amend the `update_view` in the MSQueryES.
    ms_query_es.update_views.insert(es.tier.clone(), update_view);

    // Signal Success and return the data.
    es.state = MSWriteExecutionS::Done;
    MSTableWriteAction::Success(QueryESResult {
      result: (res_col_names, vec![res_table_view]),
      new_rms: es.new_rms.iter().cloned().collect(),
    })
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    match self {
      FullMSTableWriteES::QueryReplanning(es) => es.status.exit_and_clean_up(ctx),
      FullMSTableWriteES::Executing(es) => {
        match &es.state {
          MSWriteExecutionS::Start => {}
          MSWriteExecutionS::Pending(pending) => {
            // Here, we remove the ReadRegion from `m_waiting_read_protected`, if it still
            // exists. Note that we leave `m_read_protected` and `m_write_protected` intact
            // since they are inconvenient to change (since they don't have `query_id`.
            ctx.remove_m_read_protected_request(&es.timestamp, &pending.query_id);
          }
          MSWriteExecutionS::Executing(executing) => {
            // Here, we need to cancel every Subquery. Depending on the state of the
            // SingleSubqueryStatus, we either need to either clean up the column locking
            // request or the ReadRegion from m_waiting_read_protected.

            // Note that we leave `m_read_protected` and `m_write_protected` in-tact
            // since they are inconvenient to change (since they don't have `query_id`).
            for single_status in &executing.subqueries {
              match single_status {
                SingleSubqueryStatus::LockingSchemas(locking_status) => {
                  ctx.remove_col_locking_request(locking_status.query_id.clone());
                }
                SingleSubqueryStatus::PendingReadRegion(protect_status) => {
                  ctx.remove_m_read_protected_request(&es.timestamp, &protect_status.query_id);
                }
                SingleSubqueryStatus::Pending(_) => {}
                SingleSubqueryStatus::Finished(_) => {}
              }
            }
          }
          MSWriteExecutionS::Done => {}
        }
        es.state = MSWriteExecutionS::Done;
      }
    }
  }

  /// Starts the Execution state
  fn start_ms_table_write_es<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
  ) -> MSTableWriteAction {
    let es = cast!(FullMSTableWriteES::Executing, self).unwrap();

    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &es.sql_query.selection,
      &ctx.table_schema,
      &es.timestamp,
      &es.context.context_schema,
    );

    // Compute the Row Region by taking the union across all ContextRows
    let mut row_region = Vec::<KeyBound>::new();
    for context_row in &es.context.context_rows {
      match keybound_computer.compute_keybounds(&context_row) {
        Ok(key_bounds) => {
          for key_bound in key_bounds {
            row_region.push(key_bound);
          }
        }
        Err(eval_error) => {
          es.state = MSWriteExecutionS::Done;
          return MSTableWriteAction::QueryError(mk_eval_error(eval_error));
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Write Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(es.sql_query.assignment.iter().map(|(col, _)| col.clone()));

    // Compute the Write Region
    let col_region = Vec::from_iter(col_region.into_iter());
    let write_region = TableRegion { col_region, row_region: row_region.clone() };

    // Verify that we have WriteRegion Isolation with Subsequent Reads. We abort
    // if we don't, and we amend this MSQuery's VerifyingReadWriteRegions if we do.
    if !ctx.check_write_region_isolation(&write_region, &es.timestamp) {
      es.state = MSWriteExecutionS::Done;
      MSTableWriteAction::QueryError(msg::QueryError::WriteRegionConflictWithSubsequentRead)
    } else {
      // Compute the Read Column Region.
      let col_region = es.query_plan.col_usage_node.safe_present_cols.clone();
      let read_region = TableRegion { col_region, row_region };

      // Move the MSTableWriteES to the Pending state with the given ReadRegion.
      let protect_qid = mk_qid(&mut ctx.rand);
      es.state = MSWriteExecutionS::Pending(Pending {
        read_region: read_region.clone(),
        query_id: protect_qid.clone(),
      });

      // Add a ReadRegion to the `m_waiting_read_protected` and the
      // WriteRegion into `m_write_protected`.
      let verifying = ctx.verifying_writes.get_mut(&es.timestamp).unwrap();
      verifying.m_waiting_read_protected.insert(ProtectRequest {
        orig_p: OrigP::new(es.query_id.clone()),
        protect_qid,
        read_region,
      });
      verifying.m_write_protected.insert(write_region);
      MSTableWriteAction::Wait
    }
  }

  /// Get the `QueryId` of the owning originating MSQueryES.
  pub fn ms_query_id(&self) -> &QueryId {
    match &self {
      FullMSTableWriteES::QueryReplanning(es) => &es.ms_query_id,
      FullMSTableWriteES::Executing(es) => &es.ms_query_id,
    }
  }
}
