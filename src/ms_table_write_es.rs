use crate::col_usage::collect_top_level_cols;
use crate::common::{
  lookup, map_insert, mk_qid, IOTypes, KeyBound, OrigP, QueryESResult, QueryPlan, TableRegion,
};
use crate::expression::{compress_row_region, is_true, EvalError};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, ColName, ColType, ColVal, ColValN, Context, ContextRow, Gen, PrimaryKey, QueryId,
  QueryPath, TableView, TierMap, Timestamp, TransTableName,
};
use crate::model::message as msg;
use crate::ms_table_read_es::MSTableReadES;
use crate::server::{
  contains_col, evaluate_update, mk_eval_error, weak_contains_col, CommonQuery, ContextConstructor,
};
use crate::storage::{GenericTable, MSStorageView};
use crate::tablet::{
  compute_subqueries, recompute_subquery, ColumnsLocking, ContextKeyboundComputer, Executing,
  MSQueryES, Pending, ProtectRequest, QueryReplanningSqlView, SingleSubqueryStatus,
  StorageLocalTable, SubqueryFinished, SubqueryLockingSchemas, SubqueryPending,
  SubqueryPendingReadRegion, TabletContext,
};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  MSTableWriteES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum MSWriteExecutionS {
  Start,
  ColumnsLocking(ColumnsLocking),
  GossipDataWaiting,
  Pending(Pending),
  Executing(Executing),
  Done,
}

#[derive(Debug)]
pub struct MSTableWriteES {
  pub root_query_path: QueryPath,
  pub timestamp: Timestamp,
  pub tier: u32,
  pub context: Rc<Context>,

  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::Update,
  pub query_plan: QueryPlan,

  // MSQuery fields
  pub ms_query_id: QueryId,

  // Dynamically evolving fields.
  pub new_rms: HashSet<QueryPath>,
  pub state: MSWriteExecutionS,
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
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl MSTableWriteES {
  pub fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> MSTableWriteAction {
    // First, we lock the columns that the QueryPlan requires certain properties of.
    assert!(matches!(self.state, MSWriteExecutionS::Start));

    let mut all_cols = HashSet::<ColName>::new();
    all_cols.extend(self.query_plan.col_usage_node.external_cols.clone());
    all_cols.extend(self.query_plan.col_usage_node.safe_present_cols.clone());

    // If there are extra required cols, we add them in.
    if let Some(extra_cols) = self.query_plan.extra_req_cols.get(self.sql_query.table()) {
      all_cols.extend(extra_cols.clone());
    }

    let locked_cols_qid = ctx.add_requested_locked_columns(
      OrigP::new(self.query_id.clone()),
      self.timestamp.clone(),
      all_cols.into_iter().collect(),
    );
    self.state = MSWriteExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid });

    MSTableWriteAction::Wait
  }

  /// This checks that `external_cols` aren't present, and `safe_present_cols` are present.
  /// Note: this does *not* required columns to be locked.
  fn does_query_plan_align<T: IOTypes>(&self, ctx: &TabletContext<T>) -> bool {
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

  /// Handle Columns being locked
  pub fn columns_locked<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    locked_cols_qid: QueryId,
  ) -> MSTableWriteAction {
    let locking = cast!(MSWriteExecutionS::ColumnsLocking, &self.state).unwrap();
    assert_eq!(locking.locked_cols_qid, locked_cols_qid);

    // Now, we check whether the TableSchema aligns with the QueryPlan.
    if self.does_query_plan_align(ctx) {
      MSTableWriteAction::QueryError(msg::QueryError::InvalidQueryPlan)
    } else {
      // If it aligns, we start locking the regions.
      self.start_ms_table_write_es(ctx)
    }
  }

  /// Starts the Execution state
  fn start_ms_table_write_es<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
  ) -> MSTableWriteAction {
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
          self.state = MSWriteExecutionS::Done;
          return MSTableWriteAction::QueryError(mk_eval_error(eval_error));
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Write Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(self.sql_query.assignment.iter().map(|(col, _)| col.clone()));

    // Compute the Write Region
    let col_region = Vec::from_iter(col_region.into_iter());
    let write_region = TableRegion { col_region, row_region: row_region.clone() };

    // Verify that we have WriteRegion Isolation with Subsequent Reads. We abort
    // if we don't, and we amend this MSQuery's VerifyingReadWriteRegions if we do.
    if !ctx.check_write_region_isolation(&write_region, &self.timestamp) {
      self.state = MSWriteExecutionS::Done;
      MSTableWriteAction::QueryError(msg::QueryError::WriteRegionConflictWithSubsequentRead)
    } else {
      // Compute the Read Column Region.
      let col_region = self.query_plan.col_usage_node.safe_present_cols.clone();
      let read_region = TableRegion { col_region, row_region };

      // Move the MSTableWriteES to the Pending state with the given ReadRegion.
      let protect_qid = mk_qid(&mut ctx.rand);
      self.state = MSWriteExecutionS::Pending(Pending {
        read_region: read_region.clone(),
        query_id: protect_qid.clone(),
      });

      // Add a ReadRegion to the `m_waiting_read_protected` and the
      // WriteRegion into `m_write_protected`.
      let verifying = ctx.verifying_writes.get_mut(&self.timestamp).unwrap();
      verifying.m_waiting_read_protected.insert(ProtectRequest {
        orig_p: OrigP::new(self.query_id.clone()),
        protect_qid,
        read_region,
      });
      verifying.m_write_protected.insert(write_region);
      MSTableWriteAction::Wait
    }
  }

  /// Handle ReadRegion protection
  pub fn read_protected<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    ms_query_es: &mut MSQueryES,
    _: QueryId,
  ) -> MSTableWriteAction {
    let pending = cast!(MSWriteExecutionS::Pending, &self.state).unwrap();
    let gr_query_statuses = match compute_subqueries::<T, _, _>(
      GRQueryConstructorView {
        root_query_path: &self.root_query_path,
        timestamp: &self.timestamp,
        sql_query: &self.sql_query,
        query_plan: &self.query_plan,
        query_id: &self.query_id,
        context: &self.context,
      },
      &mut ctx.rand,
      StorageLocalTable::new(
        &ctx.table_schema,
        &self.timestamp,
        &self.sql_query.selection,
        MSStorageView::new(
          &ctx.storage,
          &ctx.table_schema,
          &ms_query_es.update_views,
          self.tier.clone() - 1, // Remember that `tier` is the Tier to write to, which is
                                 // One ahead of that which to read from.
        ),
      ),
    ) {
      Ok(gr_query_statuses) => gr_query_statuses,
      Err(eval_error) => {
        self.state = MSWriteExecutionS::Done;
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
      self.state = MSWriteExecutionS::Executing(Executing {
        completed: 0,
        subqueries,
        row_region: pending.read_region.row_region.clone(),
      });

      // Return the subqueries
      MSTableWriteAction::SendSubqueries(gr_query_statuses)
    }
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
    // Add the subquery results into the MSTableWriteES.
    self.new_rms.extend(subquery_new_rms);
    let executing_state = cast!(MSWriteExecutionS::Executing, &mut self.state).unwrap();
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
    let executing_state = cast!(MSWriteExecutionS::Executing, &mut self.state).unwrap();

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
        MSStorageView::new(
          &ctx.storage,
          &ctx.table_schema,
          &ms_query_es.update_views,
          self.tier.clone() - 1,
        ),
      ),
      children,
    );

    // These are all of the `ColNames` that we need in order to evaluate the Update.
    // This consists of all Top-Level Columns for every expression, as well as all Key
    // Columns (since they are included in the resulting table).
    let mut top_level_cols_set = HashSet::<ColName>::new();
    top_level_cols_set.extend(ctx.table_schema.get_key_cols());
    top_level_cols_set.extend(collect_top_level_cols(&self.sql_query.selection));
    for (_, expr) in &self.sql_query.assignment {
      top_level_cols_set.extend(collect_top_level_cols(expr));
    }
    let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

    // Setup the TableView that we are going to return and the UpdateView that we're going
    // to hold in the MSQueryES.
    let mut res_col_names = Vec::<ColName>::new();
    res_col_names.extend(ctx.table_schema.get_key_cols());
    res_col_names.extend(self.sql_query.assignment.iter().map(|(name, _)| name.clone()));
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
        let evaluated_update = evaluate_update(
          &self.sql_query,
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
                ctx.table_schema.val_cols.strong_static_read(&col_name, self.timestamp).unwrap();
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
      self.state = MSWriteExecutionS::Done;
      return MSTableWriteAction::QueryError(mk_eval_error(eval_error));
    }

    // Amend the `update_view` in the MSQueryES.
    ms_query_es.update_views.insert(self.tier.clone(), update_view);

    // Signal Success and return the data.
    self.state = MSWriteExecutionS::Done;
    MSTableWriteAction::Success(QueryESResult {
      result: (res_col_names, vec![res_table_view]),
      new_rms: self.new_rms.iter().cloned().collect(),
    })
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    match &self.state {
      MSWriteExecutionS::Start => {}
      MSWriteExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid }) => {
        ctx.remove_col_locking_request(locked_cols_qid.clone());
      }
      MSWriteExecutionS::GossipDataWaiting => {}
      MSWriteExecutionS::Pending(pending) => {
        // Here, we remove the ReadRegion from `m_waiting_read_protected`, if it still
        // exists. Note that we leave `m_read_protected` and `m_write_protected` intact
        // since they are inconvenient to change (since they don't have `query_id`.
        ctx.remove_m_read_protected_request(&self.timestamp, &pending.query_id);
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
              ctx.remove_m_read_protected_request(&self.timestamp, &protect_status.query_id);
            }
            SingleSubqueryStatus::Pending(_) => {}
            SingleSubqueryStatus::Finished(_) => {}
          }
        }
      }
      MSWriteExecutionS::Done => {}
    }
    self.state = MSWriteExecutionS::Done;
  }

  /// Get the `QueryId` of the owning originating MSQueryES.
  pub fn ms_query_id(&self) -> &QueryId {
    &self.query_id
  }
}
