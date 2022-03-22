use crate::col_usage::{collect_top_level_cols, compute_update_schema, free_external_cols};
use crate::common::{
  lookup, mk_qid, CoreIOCtx, KeyBound, OrigP, QueryESResult, QueryPlan, ReadRegion, Timestamp,
  WriteRegion, WriteRegionType,
};
use crate::expression::{compress_row_region, compute_key_region, is_true, EvalError};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, CQueryPath, ColName, ColType, ColVal, ColValN, Context, ContextRow, PaxosGroupId,
  PaxosGroupIdTrait, PrimaryKey, QueryId, TQueryPath, TableView, TransTableName,
};
use crate::model::message as msg;
use crate::server::{
  contains_col, evaluate_update, mk_eval_error, ContextConstructor, ServerContextBase,
};
use crate::storage::{GenericTable, MSStorageView};
use crate::table_read_es::{
  check_gossip, compute_read_region, does_query_plan_align, request_lock_columns,
};
use crate::tablet::{
  compute_col_map, compute_subqueries, ColumnsLocking, Executing, MSQueryES, Pending,
  RequestedReadProtected, StorageLocalTable, TabletContext,
};
use std::collections::BTreeSet;
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
  pub root_query_path: CQueryPath,
  pub timestamp: Timestamp,
  /// This is the Tier that the new UpdateView should be added at.
  pub tier: u32,
  pub context: Rc<Context>,

  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::Update,
  pub query_plan: QueryPlan,

  /// The `QueryId` of the `MSQueryES` that this ES belongs to.
  /// We make sure that it exists as long as this ES exists.
  pub ms_query_id: QueryId,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
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
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableWriteAction {
    // First, we lock the columns that the QueryPlan requires certain properties of.
    assert!(matches!(self.state, MSWriteExecutionS::Start));
    let qid = request_lock_columns(ctx, io_ctx, &self.query_id, &self.timestamp, &self.query_plan);
    self.state = MSWriteExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid: qid });

    MSTableWriteAction::Wait
  }

  // Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableWriteAction {
    // If the GossipData is valid, then act accordingly.
    if check_gossip(&ctx.gossip.get(), &self.query_plan) {
      // We start locking the regions.
      self.start_ms_table_write_es(ctx, io_ctx)
    } else {
      // If not, we go to GossipDataWaiting
      self.state = MSWriteExecutionS::GossipDataWaiting;

      // Request a GossipData from the Master to help stimulate progress.
      let sender_path = ctx.this_sid.clone();
      ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::MasterGossipRequest(
        msg::MasterGossipRequest { sender_path },
      ));

      return MSTableWriteAction::Wait;
    }
  }

  /// Handle Columns being locked
  pub fn local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableWriteAction {
    match &self.state {
      MSWriteExecutionS::ColumnsLocking(locking) => {
        if locking.locked_cols_qid == locked_cols_qid {
          // Now, we check whether the TableSchema aligns with the QueryPlan.
          if !does_query_plan_align(ctx, &self.timestamp, &self.query_plan) {
            self.state = MSWriteExecutionS::Done;
            MSTableWriteAction::QueryError(msg::QueryError::InvalidQueryPlan)
          } else {
            // If it aligns, we verify is GossipData is recent enough.
            self.check_gossip_data(ctx, io_ctx)
          }
        } else {
          debug_assert!(false);
          MSTableWriteAction::Wait
        }
      }
      _ => MSTableWriteAction::Wait,
    }
  }

  /// Handle this just as `local_locked_cols`
  pub fn global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableWriteAction {
    self.local_locked_cols(ctx, io_ctx, locked_cols_qid)
  }

  /// Here, the column locking request results in us realizing the table has been dropped.
  pub fn table_dropped(&mut self, _: &mut TabletContext) -> MSTableWriteAction {
    match &self.state {
      MSWriteExecutionS::ColumnsLocking(_) => {
        self.state = MSWriteExecutionS::Done;
        MSTableWriteAction::QueryError(msg::QueryError::InvalidQueryPlan)
      }
      _ => {
        debug_assert!(false);
        MSTableWriteAction::Wait
      }
    }
  }

  /// Here, we GossipData gets delivered.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableWriteAction {
    if let MSWriteExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, io_ctx)
    } else {
      // Do nothing
      MSTableWriteAction::Wait
    }
  }

  /// Starts the Execution state
  fn start_ms_table_write_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableWriteAction {
    // Compute the ReadRegion
    let read_region = compute_read_region(
      &ctx.table_schema.key_cols,
      &self.query_plan,
      &self.context,
      &self.sql_query.selection,
    );

    // Compute the WriteRegion
    let write_region = WriteRegion {
      row_region: read_region.row_region.clone(),
      presence: false,
      val_col_region: self.sql_query.assignment.iter().map(|(col, _)| col.clone()).collect(),
    };

    // Verify that we have WriteRegion Isolation with Subsequent Reads. We abort
    // if we don't, and we amend this MSQuery's VerifyingReadWriteRegions if we do.
    if !ctx.check_write_region_isolation(&write_region, &self.timestamp) {
      self.state = MSWriteExecutionS::Done;
      MSTableWriteAction::QueryError(msg::QueryError::WriteRegionConflictWithSubsequentRead)
    } else {
      // Move the MSTableWriteES to the Pending state with the given ReadRegion.
      let protect_qid = mk_qid(io_ctx.rand());
      self.state = MSWriteExecutionS::Pending(Pending { query_id: protect_qid.clone() });

      // Add a ReadRegion to the `m_waiting_read_protected` and the
      // WriteRegion into `m_write_protected`.
      let verifying = ctx.verifying_writes.get_mut(&self.timestamp).unwrap();
      verifying.m_waiting_read_protected.insert(RequestedReadProtected {
        orig_p: OrigP::new(self.query_id.clone()),
        query_id: protect_qid,
        read_region,
      });
      verifying.m_write_protected.insert(write_region);
      MSTableWriteAction::Wait
    }
  }

  /// Handle ReadRegion protection
  pub fn local_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    ms_query_es: &mut MSQueryES,
    protect_qid: QueryId,
  ) -> MSTableWriteAction {
    match &self.state {
      MSWriteExecutionS::Pending(pending) if protect_qid == pending.query_id => {
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
            MSStorageView::new(
              &ctx.storage,
              &ctx.table_schema,
              &ms_query_es.update_views,
              self.tier.clone() + 1, // Remember that `tier` is the Tier to write to, which is
                                     // one lower than which to read from.
            ),
          ),
        );

        // Move the ES to the Executing state.
        self.state = MSWriteExecutionS::Executing(Executing::create(&gr_query_ess));
        let exec = cast!(MSWriteExecutionS::Executing, &mut self.state).unwrap();

        // See if we are already finished (due to having no subqueries).
        if exec.is_complete() {
          self.finish_ms_table_write_es(ctx, io_ctx, ms_query_es)
        } else {
          // Otherwise, return the subqueries.
          MSTableWriteAction::SendSubqueries(gr_query_ess)
        }
      }
      _ => {
        debug_assert!(false);
        MSTableWriteAction::Wait
      }
    }
  }

  /// This is called if a subquery fails.
  pub fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> MSTableWriteAction {
    self.exit_and_clean_up(ctx, io_ctx);
    MSTableWriteAction::QueryError(query_error)
  }

  /// Handles a Subquery completing.
  pub fn handle_subquery_done<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    ms_query_es: &mut MSQueryES,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    (_, table_views): (Vec<Option<ColName>>, Vec<TableView>),
  ) -> MSTableWriteAction {
    // Add the subquery results into the MSTableWriteES.
    self.new_rms.extend(subquery_new_rms);
    let exec = cast!(MSWriteExecutionS::Executing, &mut self.state).unwrap();
    exec.add_subquery_result(subquery_id, table_views);

    // See if we are finished (due to computing all subqueries).
    if exec.is_complete() {
      self.finish_ms_table_write_es(ctx, io_ctx, ms_query_es)
    } else {
      // Otherwise, we wait.
      MSTableWriteAction::Wait
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  fn finish_ms_table_write_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    ms_query_es: &mut MSQueryES,
  ) -> MSTableWriteAction {
    let exec = cast!(MSWriteExecutionS::Executing, &mut self.state).unwrap();

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
        MSStorageView::new(
          &ctx.storage,
          &ctx.table_schema,
          &ms_query_es.update_views,
          self.tier.clone() + 1,
        ),
      ),
      children,
    );

    // These are all of the `ColNames` that we need in order to evaluate the Update.
    // This consists of all Top-Level Columns for every expression, as well as all Key
    // Columns (since they are included in the resulting table).
    let mut top_level_cols_set = BTreeSet::<proc::ColumnRef>::new();
    top_level_cols_set.extend(ctx.table_schema.get_key_col_refs());
    top_level_cols_set.extend(collect_top_level_cols(&self.sql_query.selection));
    for (_, expr) in &self.sql_query.assignment {
      top_level_cols_set.extend(collect_top_level_cols(expr));
    }
    let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

    // Setup the TableView that we are going to return and the UpdateView that we're going
    // to hold in the MSQueryES.
    let res_col_names = compute_update_schema(&self.sql_query, &ctx.table_schema.key_cols);
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
          for key_col in &ctx.table_schema.get_key_col_refs() {
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
                ctx.table_schema.val_cols.static_read(&col_name, &self.timestamp).unwrap();
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
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {
    self.state = MSWriteExecutionS::Done;
  }
}
