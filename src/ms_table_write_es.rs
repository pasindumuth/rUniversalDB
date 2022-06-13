use crate::col_usage::{col_collecting_cb, col_ref_collecting_cb, QueryIterator};
use crate::common::{mk_qid, ColName, CoreIOCtx, OrigP, QueryESResult, Timestamp, WriteRegion};
use crate::common::{
  ColType, ColVal, ColValN, ContextRow, PrimaryKey, QueryId, TablePath, TableView, TransTableName,
};
use crate::expression::{does_types_match, is_true, EvalError};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::message as msg;
use crate::ms_table_es::{GeneralQueryES, MSTableES, SqlQueryInner};
use crate::server::{evaluate_update, mk_eval_error, ContextConstructor, ExtraColumnRef};
use crate::sql_ast::proc;
use crate::storage::{GenericTable, MSStorageView};
use crate::table_read_es::compute_read_region;
use crate::tablet::{
  compute_subqueries, MSQueryES, RequestedReadProtected, StorageLocalTable, TPESAction,
  TabletContext,
};
use std::collections::BTreeSet;
use std::iter::FromIterator;

// -----------------------------------------------------------------------------------------------
//  MSTableWriteES
// -----------------------------------------------------------------------------------------------

pub type MSTableWriteES = MSTableES<UpdateInner>;

#[derive(Debug)]
pub struct UpdateInner {
  sql_query: proc::Update,
}

impl UpdateInner {
  pub fn new(sql_query: proc::Update) -> Self {
    UpdateInner { sql_query }
  }
}

impl SqlQueryInner for UpdateInner {
  fn table_path(&self) -> &TablePath {
    &self.sql_query.table.source_ref
  }

  fn request_region_locks<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es: &GeneralQueryES,
  ) -> Result<QueryId, msg::QueryError> {
    // Collect all `ColNames` of this table that all `ColumnRefs` refer to.
    let mut safe_present_cols = Vec::<ColName>::new();
    QueryIterator::new().iterate_update(
      &mut col_collecting_cb(&self.sql_query.table.alias, &mut safe_present_cols),
      &self.sql_query,
    );

    // Compute the ReadRegion
    let source = self.sql_query.table.to_read_source();
    let read_region = compute_read_region(
      &ctx.table_schema.key_cols,
      &ctx.this_tablet_key_range,
      &es.context,
      &self.sql_query.selection,
      &source,
      safe_present_cols,
      vec![],
    );

    // Compute the WriteRegion
    let write_region = WriteRegion {
      row_region: read_region.row_region.clone(),
      presence: false,
      val_col_region: self.sql_query.assignment.iter().map(|(col, _)| col.clone()).collect(),
    };

    // Verify that we have WriteRegion Isolation with Subsequent Reads. We abort
    // if we don't, and we amend this MSQuery's VerifyingReadWriteRegions if we do.
    if !ctx.check_write_region_isolation(&write_region, &es.timestamp) {
      Err(msg::QueryError::WriteRegionConflictWithSubsequentRead)
    } else {
      // Move the MSTableWriteES to the Pending state with the given ReadRegion.
      let protect_qid = mk_qid(io_ctx.rand());

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
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es: &GeneralQueryES,
    ms_query_es: &mut MSQueryES,
  ) -> Vec<GRQueryES> {
    compute_subqueries(
      GRQueryConstructorView {
        root_query_path: &es.root_query_path,
        timestamp: &es.timestamp,
        sql_query: &self.sql_query,
        query_plan: &es.query_plan,
        query_id: &es.query_id,
        context: &es.context,
      },
      io_ctx.rand(),
      StorageLocalTable::new(
        &ctx.table_schema,
        &es.timestamp,
        &self.sql_query.table.to_read_source(),
        &ctx.this_tablet_key_range,
        &self.sql_query.selection,
        MSStorageView::new(
          &ctx.storage,
          &ctx.table_schema,
          &ms_query_es.update_views,
          es.tier.clone(),
        ),
      ),
    )
  }

  fn finish<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    _: &mut IO,
    es: &GeneralQueryES,
    (children, subquery_results): (
      Vec<(Vec<proc::ColumnRef>, Vec<TransTableName>)>,
      Vec<Vec<TableView>>,
    ),
    ms_query_es: &mut MSQueryES,
  ) -> TPESAction {
    // Create the ContextConstructor.
    let source = self.sql_query.table.to_read_source();
    let context_constructor = ContextConstructor::new(
      es.context.context_schema.clone(),
      StorageLocalTable::new(
        &ctx.table_schema,
        &es.timestamp,
        &source,
        &ctx.this_tablet_key_range,
        &self.sql_query.selection,
        MSStorageView::new(
          &ctx.storage,
          &ctx.table_schema,
          &ms_query_es.update_views,
          es.tier.clone(),
        ),
      ),
      children,
    );

    // These are all of the `ColNames` that we need in order to evaluate the Update.
    // This consists of all Top-Level Columns for every expression, as well as all Key
    // Columns (since they are included in the resulting table).
    let mut top_level_cols_set = BTreeSet::<proc::ColumnRef>::new();
    let cur_alias = &self.sql_query.table.alias;
    top_level_cols_set.extend(ctx.table_schema.get_key_col_refs(cur_alias));
    QueryIterator::new_top_level()
      .iterate_update(&mut col_ref_collecting_cb(&mut top_level_cols_set), &self.sql_query);
    let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());
    let top_level_extra_col_refs =
      Vec::from_iter(top_level_col_names.iter().map(|c| ExtraColumnRef::Named(c.clone())));

    // Setup the TableView that we are going to return and the UpdateView that we're going
    // to hold in the MSQueryES.
    let mut res_table_view = TableView::new();
    let mut update_view = GenericTable::new();

    // Finally, iterate over the Context Rows of the subqueries and compute the final values.
    let eval_res = context_constructor.run(
      &es.context.context_rows,
      top_level_extra_col_refs,
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
          let cur_alias = &self.sql_query.table.alias;
          for key_col in &ctx.table_schema.get_key_col_refs(cur_alias) {
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
                ctx.table_schema.val_cols.static_read(&col_name, &es.timestamp).unwrap();
              if !does_types_match(col_type, Some(val)) {
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

    match eval_res {
      Ok(()) => {
        // Amend the `update_view` in the MSQueryES.
        ms_query_es.update_views.insert(es.tier.clone() - 1, update_view);

        // Signal Success and return the data.
        TPESAction::Success(QueryESResult {
          result: vec![res_table_view],
          new_rms: es.new_rms.iter().cloned().collect(),
        })
      }
      Err(eval_error) => TPESAction::QueryError(mk_eval_error(eval_error)),
    }
  }
}
