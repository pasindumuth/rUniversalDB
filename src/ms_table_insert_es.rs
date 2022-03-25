use crate::col_usage::compute_insert_schema;
use crate::common::{
  lookup, mk_qid, ColBound, CoreIOCtx, KeyBound, OrigP, PolyColBound, QueryESResult, ReadRegion,
  SingleBound, WriteRegion,
};
use crate::expression::{construct_simple_cexpr, evaluate_c_expr, EvalError};
use crate::gr_query_es::GRQueryES;
use crate::model::common::{
  proc, ColName, ColType, ColVal, ColValN, PrimaryKey, QueryId, TableView, TransTableName,
};
use crate::model::message as msg;
use crate::ms_table_es::{GeneralQueryES, MSTableES, SqlQueryInner};
use crate::server::mk_eval_error;
use crate::storage::{GenericTable, MSStorageView, StorageView, PRESENCE_VALN};
use crate::tablet::TableAction;
use crate::tablet::{MSQueryES, RequestedReadProtected, TabletContext};
use std::collections::{BTreeMap, BTreeSet};

// -----------------------------------------------------------------------------------------------
//  MSTableInsertES
// -----------------------------------------------------------------------------------------------

pub type MSTableInsertES = MSTableES<InsertInner>;

#[derive(Debug)]
struct ExtraPendingData {
  /// The keys of the row that is trying to be inserted to in terms of as `Vec<KeyBound>`.
  row_region: Vec<KeyBound>,
  update_view: GenericTable,
  res_table_view: TableView,
}

#[derive(Debug)]
pub struct InsertInner {
  sql_query: proc::Insert,
  extra_pending: Option<ExtraPendingData>,
}

impl InsertInner {
  pub fn new(sql_query: proc::Insert) -> Self {
    InsertInner { sql_query, extra_pending: None }
  }
}

impl SqlQueryInner for InsertInner {
  fn request_region_locks<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es: &GeneralQueryES,
  ) -> Result<QueryId, msg::QueryError> {
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
            return Err(mk_eval_error(eval_error));
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
        ctx.table_schema.val_cols.static_read(col_name, &es.timestamp).unwrap()
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
          return Err(mk_eval_error(EvalError::TypeError));
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
          return Err(mk_eval_error(EvalError::TypeError));
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
    if !ctx.check_write_region_isolation(&write_region, &es.timestamp) {
      Err(msg::QueryError::WriteRegionConflictWithSubsequentRead)
    } else {
      let protect_qid = mk_qid(io_ctx.rand());
      // Move the MSTableInsertES to the Pending state with the computed update view.
      self.extra_pending =
        Some(ExtraPendingData { row_region: row_region.clone(), update_view, res_table_view });

      // Construct a ReadRegion for checking that none of the new rows already exist. Note that
      // we take `val_col_region` is empty, since we do not need it.
      let read_region = ReadRegion { val_col_region: vec![], row_region };

      // Add a ReadRegion to the `m_waiting_read_protected` and the
      // WriteRegion into `m_write_protected`.
      let verifying = ctx.verifying_writes.get_mut(&es.timestamp).unwrap();
      verifying.m_waiting_read_protected.insert(RequestedReadProtected {
        orig_p: OrigP::new(es.query_id.clone()),
        query_id: protect_qid.clone(),
        read_region,
      });
      verifying.m_write_protected.insert(write_region);

      Ok(protect_qid)
    }
  }

  fn compute_subqueries<IO: CoreIOCtx>(
    &mut self,
    _: &mut TabletContext,
    _: &mut IO,
    _: &GeneralQueryES,
    _: &mut MSQueryES,
  ) -> Vec<GRQueryES> {
    vec![]
  }

  fn finish<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    es: &GeneralQueryES,
    _: (Vec<(Vec<proc::ColumnRef>, Vec<TransTableName>)>, Vec<Vec<TableView>>),
    ms_query_es: &mut MSQueryES,
  ) -> TableAction {
    // Verify that the keys are not in the storage. We create a PresenceSnapshot only
    // consisting of keys and verify that the Insert does not write to these keys.
    let storage_view = MSStorageView::new(
      &ctx.storage,
      &ctx.table_schema,
      &ms_query_es.update_views,
      es.tier.clone() + 1,
    );

    let pending = std::mem::take(&mut self.extra_pending).unwrap();
    let snapshot =
      storage_view.compute_presence_snapshot(&pending.row_region, &vec![], &es.timestamp);

    // Iterate over the Insert keys.
    let update_view = &pending.update_view;
    for ((pkey, ci), _) in update_view {
      if ci == &None {
        if snapshot.contains_key(pkey) {
          // This already key exists, so we must respond with an abort.
          return TableAction::QueryError(msg::QueryError::RuntimeError {
            msg: "Inserting a row that already exists.".to_string(),
          });
        }
      }
    }

    // Finally, apply the update to the MSQueryES's update_views
    ms_query_es.update_views.insert(es.tier.clone(), update_view.clone());

    // Signal Success and return the data.
    let res_table_view = pending.res_table_view.clone();
    TableAction::Success(QueryESResult {
      result: (compute_insert_schema(&self.sql_query), vec![res_table_view]),
      new_rms: es.new_rms.iter().cloned().collect(),
    })
  }
}
