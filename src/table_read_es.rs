use crate::col_usage::collect_top_level_cols;
use crate::common::{
  btree_multimap_insert, mk_qid, IOTypes, KeyBound, OrigP, QueryESResult, QueryPlan, TableRegion,
};
use crate::expression::{compress_row_region, is_true};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, ColName, ColValN, Context, ContextRow, Gen, QueryId, QueryPath, TableView, TierMap,
  Timestamp, TransTableName,
};
use crate::model::message as msg;
use crate::query_replanning_es::{
  CommonQueryReplanningES, CommonQueryReplanningS, QueryReplanningAction,
};
use crate::server::{
  contains_col, evaluate_super_simple_select, mk_eval_error, CommonQuery, ContextConstructor,
  ServerContext,
};
use crate::storage::SimpleStorageView;
use crate::tablet::{
  compute_subqueries, recompute_subquery, ContextKeyboundComputer, Executing, Pending,
  ProtectRequest, SingleSubqueryStatus, StorageLocalTable, SubqueryFinished,
  SubqueryLockingSchemas, SubqueryPending, SubqueryPendingReadRegion, TabletContext,
};
use std::collections::HashSet;
use std::iter::FromIterator;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  TableReadES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
enum ExecutionS {
  Start,
  Pending(Pending),
  Executing(Executing),
  Done,
}

#[derive(Debug)]
pub struct TableReadES {
  root_query_path: QueryPath,
  tier_map: TierMap,
  timestamp: Timestamp,
  context: Rc<Context>,

  query_id: QueryId,

  // Query-related fields.
  sql_query: proc::SuperSimpleSelect,
  query_plan: QueryPlan,

  // Dynamically evolving fields.
  new_rms: HashSet<QueryPath>,
  state: ExecutionS,
}

#[derive(Debug)]
pub struct QueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to TableReadES.
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES<proc::SuperSimpleSelect>,
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
  /// Indicates the ES failed with insufficient columns in the Context.
  ColumnsDNE(Vec<ColName>),
}

#[derive(Debug)]
pub enum FullTableReadES {
  QueryReplanning(QueryReplanningES),
  Executing(TableReadES),
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl FullTableReadES {
  pub fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> TableAction {
    let plan_es = cast!(Self::QueryReplanning, self).unwrap();
    let action = plan_es.status.start(ctx);
    self.handle_replanning_action(ctx, action)
  }

  /// Handle Columns being locked
  pub fn columns_locked<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    locked_cols_qid: QueryId,
  ) -> TableAction {
    match self {
      FullTableReadES::QueryReplanning(plan_es) => {
        let action = plan_es.status.columns_locked::<T>(ctx);
        self.handle_replanning_action(ctx, action)
      }
      FullTableReadES::Executing(es) => {
        let executing = cast!(ExecutionS::Executing, &mut es.state).unwrap();

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
          TableAction::ColumnsDNE(missing_cols)
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
          btree_multimap_insert(
            &mut ctx.waiting_read_protected,
            &es.timestamp,
            ProtectRequest {
              orig_p: OrigP::new(es.query_id.clone()),
              protect_qid: protect_qid.clone(),
              read_region: new_read_region.clone(),
            },
          );

          // Finally, update the SingleSubqueryStatus to wait for the Region Protection.
          *single_status = SingleSubqueryStatus::PendingReadRegion(SubqueryPendingReadRegion {
            new_columns,
            trans_table_names: locking_status.trans_table_names.clone(),
            query_id: protect_qid,
          });

          TableAction::Wait
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
  ) -> TableAction {
    let plan_es = cast!(FullTableReadES::QueryReplanning, self).unwrap();
    let action = plan_es.status.handle_master_response(ctx, gossip_gen, tree);
    self.handle_replanning_action(ctx, action)
  }

  /// Handle the `action` sent back by the `CommonQueryReplanningES`.
  fn handle_replanning_action<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    action: QueryReplanningAction,
  ) -> TableAction {
    match action {
      QueryReplanningAction::Wait => TableAction::Wait,
      QueryReplanningAction::Success => {
        // If the QueryReplanning was successful, we move the FullTableReadES
        // to Executing in the Start state, and immediately start executing it.
        let plan_es = cast!(FullTableReadES::QueryReplanning, self).unwrap();
        let comm_plan_es = &mut plan_es.status;
        *self = FullTableReadES::Executing(TableReadES {
          root_query_path: plan_es.root_query_path.clone(),
          tier_map: plan_es.tier_map.clone(),
          timestamp: comm_plan_es.timestamp,
          context: comm_plan_es.context.clone(),
          query_id: comm_plan_es.query_id.clone(),
          sql_query: comm_plan_es.sql_view.clone(),
          query_plan: comm_plan_es.query_plan.clone(),
          new_rms: Default::default(),
          state: ExecutionS::Start,
        });
        self.start_table_read_es(ctx)
      }
      QueryReplanningAction::QueryError(query_error) => {
        // Forward up the QueryError. We do not need to ECU because
        // QueryReplanning will be in Done
        TableAction::QueryError(query_error)
      }
      QueryReplanningAction::ColumnsDNE(missing_cols) => {
        // Forward up the QueryError. We do not need to ECU because
        // QueryReplanning will be in Done
        TableAction::ColumnsDNE(missing_cols)
      }
    }
  }

  /// Handle ReadRegion protection
  pub fn read_protected<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    protect_query_id: QueryId,
  ) -> TableAction {
    let es = cast!(FullTableReadES::Executing, self).unwrap();
    match &mut es.state {
      ExecutionS::Start => panic!(),
      ExecutionS::Pending(pending) => {
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
            SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
          ),
        ) {
          Ok(gr_query_statuses) => gr_query_statuses,
          Err(eval_error) => {
            es.state = ExecutionS::Done;
            return TableAction::QueryError(mk_eval_error(eval_error));
          }
        };

        if gr_query_statuses.is_empty() {
          // Since there are no subqueries, we can go straight to finishing the ES.
          self.finish_table_read_es(ctx)
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
          es.state = ExecutionS::Executing(Executing {
            completed: 0,
            subqueries,
            row_region: pending.read_region.row_region.clone(),
          });

          // Return the subqueries
          TableAction::SendSubqueries(gr_query_statuses)
        }
      }
      ExecutionS::Executing(executing) => {
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
            SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
          ),
          executing,
          &protect_query_id,
        ) {
          Ok(gr_query_statuses) => gr_query_statuses,
          Err(eval_error) => {
            self.exit_and_clean_up(ctx);
            return TableAction::QueryError(mk_eval_error(eval_error));
          }
        };

        // Return the subquery
        TableAction::SendSubqueries(vec![gr_query_es])
      }
      ExecutionS::Done => panic!(),
    }
  }

  /// Handles InternalColumnsDNE
  pub fn handle_internal_columns_dne<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    subquery_id: QueryId,
    rem_cols: Vec<ColName>,
  ) -> TableAction {
    let es = cast!(FullTableReadES::Executing, self).unwrap();
    let executing = cast!(ExecutionS::Executing, &mut es.state).unwrap();

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

    TableAction::Wait
  }

  /// This is called if a subquery fails. This simply responds to the sender
  /// and Exits and Clean Ups this ES. This is also called when a Deadlock Safety
  /// Abortion happens.
  pub fn handle_internal_query_error<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    query_error: msg::QueryError,
  ) -> TableAction {
    self.exit_and_clean_up(ctx);
    TableAction::QueryError(query_error)
  }

  /// Handles a Subquery completing
  pub fn handle_subquery_done<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    subquery_id: QueryId,
    subquery_new_rms: HashSet<QueryPath>,
    (_, table_views): (Vec<ColName>, Vec<TableView>),
  ) -> TableAction {
    let es = cast!(FullTableReadES::Executing, self).unwrap();

    // Add the subquery results into the TableReadES.
    es.new_rms.extend(subquery_new_rms);
    let executing_state = cast!(ExecutionS::Executing, &mut es.state).unwrap();
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
      self.finish_table_read_es(ctx)
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  fn finish_table_read_es<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> TableAction {
    let es = cast!(FullTableReadES::Executing, self).unwrap();
    let executing_state = cast!(ExecutionS::Executing, &mut es.state).unwrap();

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
      es.context.context_schema.clone(),
      StorageLocalTable::new(
        &ctx.table_schema,
        &es.timestamp,
        &es.sql_query.selection,
        SimpleStorageView::new(&ctx.storage, &ctx.table_schema),
      ),
      children,
    );

    // These are all of the `ColNames` we need in order to evaluate the Select.
    let mut top_level_cols_set = HashSet::<ColName>::new();
    top_level_cols_set.extend(collect_top_level_cols(&es.sql_query.selection));
    top_level_cols_set.extend(es.sql_query.projection.clone());
    let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

    // Finally, iterate over the Context Rows of the subqueries and compute the final values.
    let mut res_table_views = Vec::<TableView>::new();
    for _ in 0..es.context.context_rows.len() {
      res_table_views.push(TableView::new(es.sql_query.projection.clone()));
    }

    let eval_res = context_constructor.run(
      &es.context.context_rows,
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
          &es.sql_query,
          &top_level_col_names,
          &top_level_col_vals,
          &subquery_vals,
        )?;
        if is_true(&evaluated_select.selection)? {
          // This means that the current row should be selected for the result. Thus, we take
          // the values of the project columns and insert it into the appropriate TableView.
          let mut res_row = Vec::<ColValN>::new();
          for res_col_name in &es.sql_query.projection {
            let idx = top_level_col_names.iter().position(|k| res_col_name == k).unwrap();
            res_row.push(top_level_col_vals.get(idx).unwrap().clone());
          }

          res_table_views[context_row_idx].add_row_multi(res_row, count);
        };
        Ok(())
      },
    );

    if let Err(eval_error) = eval_res {
      es.state = ExecutionS::Done;
      return TableAction::QueryError(mk_eval_error(eval_error));
    }

    // Signal Success and return the data.
    es.state = ExecutionS::Done;
    TableAction::Success(QueryESResult {
      result: (es.sql_query.projection.clone(), res_table_views),
      new_rms: es.new_rms.iter().cloned().collect(),
    })
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    match self {
      FullTableReadES::QueryReplanning(es) => es.status.exit_and_clean_up(ctx),
      FullTableReadES::Executing(es) => {
        match &es.state {
          ExecutionS::Start => {}
          ExecutionS::Pending(pending) => {
            // Here, we remove the ReadRegion from `waiting_read_protected`, if it still exists.
            ctx.remove_read_protected_request(&es.timestamp, &pending.query_id);
          }
          ExecutionS::Executing(executing) => {
            // Here, we need to cancel every Subquery. Depending on the state of the
            // SingleSubqueryStatus, we either need to either clean up the column locking
            // request or the ReadRegion from read protection.
            for single_status in &executing.subqueries {
              match single_status {
                SingleSubqueryStatus::LockingSchemas(locking_status) => {
                  ctx.remove_col_locking_request(locking_status.query_id.clone());
                }
                SingleSubqueryStatus::PendingReadRegion(protect_status) => {
                  ctx.remove_read_protected_request(&es.timestamp, &protect_status.query_id);
                }
                SingleSubqueryStatus::Pending(_) => {}
                SingleSubqueryStatus::Finished(_) => {}
              }
            }
          }
          ExecutionS::Done => {}
        }
        es.state = ExecutionS::Done;
      }
    }
  }

  /// Processes the Start state of TableReadES.
  fn start_table_read_es<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> TableAction {
    let es = cast!(FullTableReadES::Executing, self).unwrap();

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
          es.state = ExecutionS::Done;
          return TableAction::QueryError(mk_eval_error(eval_error));
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(es.sql_query.projection.clone());
    col_region.extend(es.query_plan.col_usage_node.safe_present_cols.clone());

    // Move the TableReadES to the Pending state with the given ReadRegion.
    let protect_qid = mk_qid(&mut ctx.rand);
    let col_region = Vec::from_iter(col_region.into_iter());
    let read_region = TableRegion { col_region, row_region };
    es.state = ExecutionS::Pending(Pending {
      read_region: read_region.clone(),
      query_id: protect_qid.clone(),
    });

    // Add a read protection requested
    btree_multimap_insert(
      &mut ctx.waiting_read_protected,
      &es.timestamp,
      ProtectRequest { orig_p: OrigP::new(es.query_id.clone()), protect_qid, read_region },
    );

    TableAction::Wait
  }

  /// Get the `QueryId` of the sender of this ES.
  pub fn query_id(&self) -> QueryId {
    match &self {
      FullTableReadES::QueryReplanning(es) => es.status.query_id.clone(),
      FullTableReadES::Executing(es) => es.query_id.clone(),
    }
  }
}
