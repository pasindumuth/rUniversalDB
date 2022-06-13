use crate::col_usage::{get_collecting_cb, iterate_select};
use crate::common::{mk_qid, ColName, CoreIOCtx, OrigP, QueryESResult, WriteRegion};
use crate::common::{
  ColType, ColVal, ColValN, ContextRow, PrimaryKey, QueryId, TablePath, TableView, TransTableName,
};
use crate::expression::{is_true, EvalError};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::message as msg;
use crate::ms_table_es::{GeneralQueryES, MSTableES, SqlQueryInner};
use crate::server::{mk_eval_error, ContextConstructor};
use crate::sql_ast::proc;
use crate::sql_ast::proc::SelectClause;
use crate::storage::{GenericTable, MSStorageView};
use crate::table_read_es::{compute_read_region, fully_evaluate_select};
use crate::tablet::{
  compute_subqueries, MSQueryES, RequestedReadProtected, StorageLocalTable, TPESAction,
  TabletContext,
};
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::ops::Deref;

// -----------------------------------------------------------------------------------------------
//  MSTableReadES
// -----------------------------------------------------------------------------------------------

pub type MSTableReadES = MSTableES<SelectInner>;

#[derive(Debug)]
pub struct SelectInner {
  sql_query: proc::SuperSimpleSelect,
}

impl SelectInner {
  pub fn new(sql_query: proc::SuperSimpleSelect) -> Self {
    SelectInner { sql_query }
  }
}

impl SqlQueryInner for SelectInner {
  /// This function shouly only be called if we know `from` is not a `JoinNode`.
  fn table_path(&self) -> &TablePath {
    self.sql_query.from.to_table_path()
  }

  fn request_region_locks<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es: &GeneralQueryES,
  ) -> Result<QueryId, msg::QueryError> {
    // Get extra columns that must be in the region due to SELECT * .
    let mut extra_cols = match &self.sql_query.projection {
      SelectClause::SelectList(_) => vec![],
      SelectClause::Wildcard => ctx.table_schema.get_schema_val_cols_static(&es.timestamp),
    };

    // Collect all `ColNames` of this table that all `ColumnRefs` refer to.
    let mut safe_present_cols = Vec::<ColName>::new();
    iterate_select(
      &mut get_collecting_cb(self.sql_query.from.name(), &mut safe_present_cols),
      &self.sql_query,
    );

    // Compute the ReadRegion
    let read_region = compute_read_region(
      &ctx.table_schema.key_cols,
      &ctx.this_tablet_key_range,
      &es.context,
      &self.sql_query.selection,
      &self.sql_query.from,
      safe_present_cols,
      extra_cols,
    );

    // Move the MSTableReadES to the Pending state with the given ReadRegion.
    let protect_qid = mk_qid(io_ctx.rand());

    // Add a ReadRegion to the m_waiting_read_protected.
    let verifying = ctx.verifying_writes.get_mut(&es.timestamp).unwrap();
    verifying.m_waiting_read_protected.insert(RequestedReadProtected {
      orig_p: OrigP::new(es.query_id.clone()),
      query_id: protect_qid.clone(),
      read_region,
    });

    Ok(protect_qid)
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
        &self.sql_query.from,
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
    let context_constructor = ContextConstructor::new(
      es.context.context_schema.clone(),
      StorageLocalTable::new(
        &ctx.table_schema,
        &es.timestamp,
        &self.sql_query.from,
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

    // Evaluate
    let schema = es.query_plan.col_usage_node.schema.clone();
    let eval_res = fully_evaluate_select(
      context_constructor,
      &es.context.deref(),
      subquery_results,
      &self.sql_query,
      &schema,
    );

    match eval_res {
      Ok(res_table_views) => {
        // Signal Success and return the data.
        TPESAction::Success(QueryESResult {
          result: res_table_views,
          new_rms: es.new_rms.iter().cloned().collect(),
        })
      }
      Err(eval_error) => TPESAction::QueryError(mk_eval_error(eval_error)),
    }
  }
}
