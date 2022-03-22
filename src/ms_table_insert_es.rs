use crate::col_usage::{compute_insert_schema, free_external_cols};
use crate::common::{
  lookup, lookup_pos, mk_qid, ColBound, CoreIOCtx, KeyBound, OrigP, PolyColBound, QueryESResult,
  QueryPlan, ReadRegion, SingleBound, Timestamp, WriteRegion, WriteRegionType,
};
use crate::expression::{
  compress_row_region, construct_colvaln, construct_simple_cexpr, evaluate_c_expr, is_true, CExpr,
  EvalError,
};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, CQueryPath, ColName, ColType, ColVal, ColValN, Context, ContextRow, PrimaryKey, QueryId,
  TQueryPath, TableView, TransTableName,
};
use crate::model::message as msg;
use crate::server::{
  contains_col, evaluate_update, mk_eval_error, ContextConstructor, ServerContextBase,
};
use crate::storage::{GenericTable, MSStorageView, StorageView, PRESENCE_VALN};
use crate::table_read_es::{check_gossip, does_query_plan_align, request_lock_columns};
use crate::tablet::{
  ColumnsLocking, Executing, MSQueryES, RequestedReadProtected, StorageLocalTable, TabletContext,
};
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  MSTableInsertES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct Pending {
  /// The keys of the row that is trying to be inserted to in terms of as `Vec<KeyBound>`.
  row_region: Vec<KeyBound>,
  update_view: GenericTable,
  res_table_view: TableView,
  query_id: QueryId,
}

#[derive(Debug)]
pub enum MSTableInsertExecutionS {
  Start,
  ColumnsLocking(ColumnsLocking),
  GossipDataWaiting,
  Pending(Pending),
  Done,
}

#[derive(Debug)]
pub struct MSTableInsertES {
  pub root_query_path: CQueryPath,
  pub timestamp: Timestamp,
  pub tier: u32,
  pub context: Rc<Context>,

  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::Insert,
  pub query_plan: QueryPlan,

  /// The `QueryId` of the `MSQueryES` that this ES belongs to.
  /// We make sure that it exists as long as this ES exists.
  pub ms_query_id: QueryId,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub state: MSTableInsertExecutionS,
}

pub enum MSTableInsertAction {
  /// This tells the parent Server to wait.
  Wait,
  /// Indicates the ES succeeded with the given result.
  Success(QueryESResult),
  /// Indicates the ES failed with a QueryError.
  QueryError(msg::QueryError),
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl MSTableInsertES {
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableInsertAction {
    // First, we lock the columns that the QueryPlan requires certain properties of.
    assert!(matches!(self.state, MSTableInsertExecutionS::Start));
    let qid = request_lock_columns(ctx, io_ctx, &self.query_id, &self.timestamp, &self.query_plan);
    self.state = MSTableInsertExecutionS::ColumnsLocking(ColumnsLocking { locked_cols_qid: qid });

    MSTableInsertAction::Wait
  }

  // Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableInsertAction {
    // If the GossipData is valid, then act accordingly.
    if check_gossip(&ctx.gossip.get(), &self.query_plan) {
      // We start locking the regions.
      self.start_ms_table_insert_es(ctx, io_ctx)
    } else {
      // If not, we go to GossipDataWaiting
      self.state = MSTableInsertExecutionS::GossipDataWaiting;

      // Request a GossipData from the Master to help stimulate progress.
      let sender_path = ctx.this_sid.clone();
      ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::MasterGossipRequest(
        msg::MasterGossipRequest { sender_path },
      ));

      return MSTableInsertAction::Wait;
    }
  }

  /// Handle Columns being locked
  pub fn local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableInsertAction {
    match &self.state {
      MSTableInsertExecutionS::ColumnsLocking(locking) => {
        if locking.locked_cols_qid == locked_cols_qid {
          // Now, we check whether the TableSchema aligns with the QueryPlan.
          if !does_query_plan_align(ctx, &self.timestamp, &self.query_plan) {
            self.state = MSTableInsertExecutionS::Done;
            MSTableInsertAction::QueryError(msg::QueryError::InvalidQueryPlan)
          } else {
            // If it aligns, we verify is GossipData is recent enough.
            self.check_gossip_data(ctx, io_ctx)
          }
        } else {
          debug_assert!(false);
          MSTableInsertAction::Wait
        }
      }
      _ => MSTableInsertAction::Wait,
    }
  }

  /// Handle this just as `local_locked_cols`
  pub fn global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    locked_cols_qid: QueryId,
  ) -> MSTableInsertAction {
    self.local_locked_cols(ctx, io_ctx, locked_cols_qid)
  }

  /// Here, the column locking request results in us realizing the table has been dropped.
  pub fn table_dropped(&mut self, _: &mut TabletContext) -> MSTableInsertAction {
    match &self.state {
      MSTableInsertExecutionS::ColumnsLocking(_) => {
        self.state = MSTableInsertExecutionS::Done;
        MSTableInsertAction::QueryError(msg::QueryError::InvalidQueryPlan)
      }
      _ => {
        debug_assert!(false);
        MSTableInsertAction::Wait
      }
    }
  }

  /// Here, we GossipData gets delivered.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableInsertAction {
    if let MSTableInsertExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, io_ctx)
    } else {
      // Do nothing
      MSTableInsertAction::Wait
    }
  }

  /// Starts the Execution state
  fn start_ms_table_insert_es<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> MSTableInsertAction {
    // By this point, we have done QueryVerification. Studying how the QueryPlan is made for
    // Insert queries, we see that by here, we will know for certain that all Key Columns
    // are present in `columns`, all other ColNames in `columns` will be present in the
    // Table Schema, and that each row in `values` will have as many elements as `columns`.

    // Evaluate the Values
    let mut eval_values = Vec::<Vec<ColValN>>::new();
    for row in &self.sql_query.values {
      let mut eval_row = Vec::<ColValN>::new();
      for val_expr in row {
        match (|| {
          // For now, Insert queries only support Simple ValExprs as a value (i.e. no subqueries).
          let c_expr = construct_simple_cexpr(val_expr)?;
          evaluate_c_expr(&c_expr)
        })() {
          Ok(val) => eval_row.push(val),
          Err(eval_error) => {
            self.state = MSTableInsertExecutionS::Done;
            return MSTableInsertAction::QueryError(mk_eval_error(eval_error));
          }
        }
      }
      eval_values.push(eval_row);
    }

    // Validate that the types of values align with the schema.
    for (i, col_name) in self.sql_query.columns.iter().enumerate() {
      let col_type = if let Some(col_type) = lookup(&ctx.table_schema.key_cols, col_name) {
        col_type
      } else {
        // The `col_name` must be a ValCol that is already locked at this timestamp.
        ctx.table_schema.val_cols.static_read(col_name, &self.timestamp).unwrap()
      };

      for row in &eval_values {
        let col_valn = row.get(i).unwrap();
        let type_matches = match (col_type, col_valn) {
          (ColType::Int, Some(ColVal::Int(_))) => true,
          (ColType::Int, None) => true,
          (ColType::Bool, Some(ColVal::Bool(_))) => true,
          (ColType::Bool, None) => true,
          (ColType::String, Some(ColVal::String(_))) => true,
          (ColType::String, None) => true,
          _ => false,
        };
        if !type_matches {
          // If types do not match for some row, we propagate up a TypeError.
          self.state = MSTableInsertExecutionS::Done;
          return MSTableInsertAction::QueryError(mk_eval_error(EvalError::TypeError));
        }
      }
    }

    // Compute the UpdateView where we insert all of these rows as new rows.
    let mut update_view = GenericTable::new();
    let mut res_table_view = TableView::new(compute_insert_schema(&self.sql_query));
    let mut pkeys = BTreeSet::<PrimaryKey>::new();
    for row in eval_values {
      // Construct PrimaryKey.
      let mut row_map = BTreeMap::<ColName, ColValN>::new();
      for i in 0..row.len() {
        row_map.insert(self.sql_query.columns.get(i).unwrap().clone(), row.get(i).unwrap().clone());
      }
      let mut pkey = PrimaryKey { cols: vec![] };
      for (key, _) in &ctx.table_schema.key_cols {
        let valn = row_map.remove(key).unwrap();
        if let Some(val) = valn {
          pkey.cols.push(val.clone());
        } else {
          // Since the value of a Key Col cannot be NULL, we return an error if this is the case.
          self.state = MSTableInsertExecutionS::Done;
          return MSTableInsertAction::QueryError(mk_eval_error(EvalError::TypeError));
        }
      }

      // Only add the row if it falls within the rage of this Tablet.
      if ctx.check_range_inclusion(&pkey) {
        // Add the ValCol values. These are the remaining elements of `row_map`.
        update_view.insert((pkey.clone(), None), PRESENCE_VALN);
        for (col_name, valn) in row_map {
          update_view.insert((pkey.clone(), Some(col_name)), valn);
        }

        pkeys.insert(pkey);

        // We also construct `res_table_view`, which is what we return to the sender
        res_table_view.add_row(row)
      }
    }

    // Construct a set of KeyBounds for each row that is added
    let mut row_region = Vec::<KeyBound>::new();
    for pkey in pkeys {
      let mut key_bound = KeyBound { col_bounds: vec![] };
      for val in pkey.cols {
        match val {
          ColVal::Int(v) => {
            key_bound.col_bounds.push(PolyColBound::Int(ColBound {
              start: SingleBound::Included(v.clone()),
              end: SingleBound::Included(v.clone()),
            }));
          }
          ColVal::Bool(v) => {
            key_bound.col_bounds.push(PolyColBound::Bool(ColBound {
              start: SingleBound::Included(v.clone()),
              end: SingleBound::Included(v.clone()),
            }));
          }
          ColVal::String(v) => {
            key_bound.col_bounds.push(PolyColBound::String(ColBound {
              start: SingleBound::Included(v.clone()),
              end: SingleBound::Included(v.clone()),
            }));
          }
        }
      }
      row_region.push(key_bound);
    }

    // Compute the WriteRegion
    let mut val_col_region = Vec::<ColName>::new();
    for col_name in self.sql_query.columns.iter() {
      if lookup(&ctx.table_schema.key_cols, col_name).is_none() {
        val_col_region.push(col_name.clone());
      };
    }

    let write_region =
      WriteRegion { row_region: row_region.clone(), presence: true, val_col_region };

    // Verify that we have Write Region Isolation with Subsequent Reads. We abort
    // if we do not, and we amend this MSQuery's VerifyingReadWriteRegions if we do.
    if !ctx.check_write_region_isolation(&write_region, &self.timestamp) {
      self.state = MSTableInsertExecutionS::Done;
      MSTableInsertAction::QueryError(msg::QueryError::WriteRegionConflictWithSubsequentRead)
    } else {
      let protect_qid = mk_qid(io_ctx.rand());
      // Move the MSTableInsertES to the Pending state with the computed update view.
      self.state = MSTableInsertExecutionS::Pending(Pending {
        row_region: row_region.clone(),
        update_view,
        res_table_view,
        query_id: protect_qid.clone(),
      });

      // Construct a ReadRegion for checking that none of the new rows already exist. Note that
      // we take `val_col_region` is empty, since we do not need it.
      let read_region = ReadRegion { val_col_region: vec![], row_region };

      // Add a ReadRegion to the `m_waiting_read_protected` and the
      // WriteRegion into `m_write_protected`.
      let verifying = ctx.verifying_writes.get_mut(&self.timestamp).unwrap();
      verifying.m_waiting_read_protected.insert(RequestedReadProtected {
        orig_p: OrigP::new(self.query_id.clone()),
        query_id: protect_qid,
        read_region,
      });
      verifying.m_write_protected.insert(write_region);
      MSTableInsertAction::Wait
    }
  }

  /// Handle ReadRegion protection
  pub fn local_read_protected<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    ms_query_es: &mut MSQueryES,
    protect_qid: QueryId,
  ) -> MSTableInsertAction {
    match &self.state {
      MSTableInsertExecutionS::Pending(pending) if protect_qid == pending.query_id => {
        // Verify that the keys are not in the storage. We create a PresenceSnapshot only
        // consisting of keys and verify that the Insert does not write to these keys.
        let storage_view = MSStorageView::new(
          &ctx.storage,
          &ctx.table_schema,
          &ms_query_es.update_views,
          self.tier.clone() + 1,
        );

        let snapshot =
          storage_view.compute_presence_snapshot(&pending.row_region, &vec![], &self.timestamp);

        // Iterate over the Insert keys.
        let update_view = &pending.update_view;
        for ((pkey, ci), _) in update_view {
          if ci == &None {
            if snapshot.contains_key(pkey) {
              // This already key exists, so we must respond with an abort.
              self.state = MSTableInsertExecutionS::Done;
              return MSTableInsertAction::QueryError(msg::QueryError::RuntimeError {
                msg: "Inserting a row that already exists.".to_string(),
              });
            }
          }
        }

        // Finally, apply the update to the MSQueryES's update_views
        ms_query_es.update_views.insert(self.tier.clone(), update_view.clone());

        // Signal Success and return the data.
        let res_table_view = pending.res_table_view.clone();
        self.state = MSTableInsertExecutionS::Done;
        MSTableInsertAction::Success(QueryESResult {
          result: (compute_insert_schema(&self.sql_query), vec![res_table_view]),
          new_rms: self.new_rms.iter().cloned().collect(),
        })
      }
      _ => {
        debug_assert!(false);
        MSTableInsertAction::Wait
      }
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut TabletContext, _: &mut IO) {
    self.state = MSTableInsertExecutionS::Done;
  }
}
