use crate::common::{add_item, lookup, TableSchema, Timestamp};
use crate::common::{ColName, ColType, Gen, TablePath, TierMap, TransTableName};
use crate::master_query_planning_es::{ColPresenceReq, DBSchemaView, ErrorTrait};
use crate::message as msg;
use crate::multiversion_map::MVM;
use crate::sql_ast::{iast, proc};
use serde::{Deserialize, Serialize};
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

// -----------------------------------------------------------------------------------------------
//  Collection functions
// -----------------------------------------------------------------------------------------------

/// This function returns all `ColName`s in the `expr` that don't fall
/// under a `Subquery`.
pub fn collect_top_level_cols(expr: &proc::ValExpr) -> Vec<proc::ColumnRef> {
  let mut cols = Vec::<proc::ColumnRef>::new();
  collect_top_level_cols_r(expr, &mut cols);
  cols
}

fn collect_top_level_cols_r(expr: &proc::ValExpr, cols: &mut Vec<proc::ColumnRef>) {
  match expr {
    proc::ValExpr::ColumnRef(col_ref) => cols.push(col_ref.clone()),
    proc::ValExpr::UnaryExpr { expr, .. } => collect_top_level_cols_r(&expr, cols),
    proc::ValExpr::BinaryExpr { left, right, .. } => {
      collect_top_level_cols_r(&left, cols);
      collect_top_level_cols_r(&right, cols);
    }
    proc::ValExpr::Value { .. } => {}
    proc::ValExpr::Subquery { .. } => {}
  }
}

/// This function collects and returns all GRQueries that belongs to a `Update`.
pub fn collect_update_subqueries(sql_query: &proc::Update) -> Vec<proc::GRQuery> {
  let mut subqueries = Vec::<proc::GRQuery>::new();
  for (_, expr) in &sql_query.assignment {
    collect_expr_subqueries_r(expr, &mut subqueries);
  }
  collect_expr_subqueries_r(&sql_query.selection, &mut subqueries);
  return subqueries;
}

/// This function collects and returns all GRQueries that belongs to a `SuperSimpleSelect`.
pub fn collect_select_subqueries(sql_query: &proc::SuperSimpleSelect) -> Vec<proc::GRQuery> {
  return collect_expr_subqueries(&sql_query.selection);
}

/// This function collects and returns all GRQueries that belongs to a `Delete`.
pub fn collect_delete_subqueries(sql_query: &proc::Delete) -> Vec<proc::GRQuery> {
  let mut subqueries = Vec::<proc::GRQuery>::new();
  collect_expr_subqueries_r(&sql_query.selection, &mut subqueries);
  return subqueries;
}

// Computes the set of all GRQuerys that appear as immediate children of `expr`.
fn collect_expr_subqueries(expr: &proc::ValExpr) -> Vec<proc::GRQuery> {
  let mut subqueries = Vec::<proc::GRQuery>::new();
  collect_expr_subqueries_r(expr, &mut subqueries);
  subqueries
}

fn collect_expr_subqueries_r(expr: &proc::ValExpr, subqueries: &mut Vec<proc::GRQuery>) {
  match expr {
    proc::ValExpr::ColumnRef { .. } => {}
    proc::ValExpr::UnaryExpr { expr, .. } => collect_expr_subqueries_r(expr, subqueries),
    proc::ValExpr::BinaryExpr { left, right, .. } => {
      collect_expr_subqueries_r(left, subqueries);
      collect_expr_subqueries_r(right, subqueries);
    }
    proc::ValExpr::Value { .. } => {}
    proc::ValExpr::Subquery { query, .. } => subqueries.push(query.deref().clone()),
  }
}

// -----------------------------------------------------------------------------------------------
//  Query Element Iteration
// -----------------------------------------------------------------------------------------------
pub enum QueryElement<'a> {
  MSQuery(&'a proc::MSQuery),
  GRQuery(&'a proc::GRQuery),
  SuperSimpleSelect(&'a proc::SuperSimpleSelect),
  Update(&'a proc::Update),
  Insert(&'a proc::Insert),
  Delete(&'a proc::Delete),
  ValExpr(&'a proc::ValExpr),
}

fn iterate_expr<'a, CbT: FnMut(QueryElement<'a>) -> ()>(cb: &mut CbT, expr: &'a proc::ValExpr) {
  cb(QueryElement::ValExpr(expr));
  match expr {
    proc::ValExpr::ColumnRef { .. } => {}
    proc::ValExpr::UnaryExpr { expr, .. } => iterate_expr(cb, expr),
    proc::ValExpr::BinaryExpr { left, right, .. } => {
      iterate_expr(cb, left);
      iterate_expr(cb, right);
    }
    proc::ValExpr::Value { .. } => {}
    proc::ValExpr::Subquery { query } => {
      iterate_gr_query(cb, query);
    }
  }
}

pub fn iterate_select<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::SuperSimpleSelect,
) {
  cb(QueryElement::SuperSimpleSelect(query));
  match &query.projection {
    proc::SelectClause::SelectList(select_list) => {
      for (item, _) in select_list {
        let expr = match item {
          proc::SelectItem::ValExpr(expr) => expr,
          proc::SelectItem::UnaryAggregate(agg) => &agg.expr,
        };
        iterate_expr(cb, expr);
      }
    }
    proc::SelectClause::Wildcard => {}
  }
  iterate_expr(cb, &query.selection)
}

pub fn iterate_update<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::Update,
) {
  cb(QueryElement::Update(query));
  for (_, expr) in &query.assignment {
    iterate_expr(cb, expr)
  }
  iterate_expr(cb, &query.selection)
}

pub fn iterate_insert<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::Insert,
) {
  cb(QueryElement::Insert(query));
  for row in &query.values {
    for expr in row {
      iterate_expr(cb, expr);
    }
  }
}

pub fn iterate_delete<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::Delete,
) {
  cb(QueryElement::Delete(query));
  iterate_expr(cb, &query.selection)
}

pub fn iterate_ms_query_stage<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  stage: &'a proc::MSQueryStage,
) {
  match stage {
    proc::MSQueryStage::SuperSimpleSelect(query) => {
      iterate_select(cb, query);
    }
    proc::MSQueryStage::Update(query) => {
      iterate_update(cb, query);
    }
    proc::MSQueryStage::Insert(query) => {
      iterate_insert(cb, query);
    }
    proc::MSQueryStage::Delete(query) => {
      iterate_delete(cb, query);
    }
  }
}

pub fn iterate_ms_query<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::MSQuery,
) {
  cb(QueryElement::MSQuery(query));
  for (_, stage) in &query.trans_tables {
    iterate_ms_query_stage(cb, stage);
  }
}

pub fn iterate_gr_query_stage<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  stage: &'a proc::GRQueryStage,
) {
  match stage {
    proc::GRQueryStage::SuperSimpleSelect(query) => {
      iterate_select(cb, query);
    }
  }
}

pub fn iterate_gr_query<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::GRQuery,
) {
  cb(QueryElement::GRQuery(query));
  for (_, stage) in &query.trans_tables {
    iterate_gr_query_stage(cb, stage);
  }
}

// -----------------------------------------------------------------------------------------------
//  Expression iteration
// -----------------------------------------------------------------------------------------------

pub fn col_collecting_cb<'a>(
  source: &'a String,
  col_container: &'a mut Vec<ColName>,
) -> impl FnMut(QueryElement) -> () + 'a {
  move |elem: QueryElement| match elem {
    QueryElement::ValExpr(expr) => {
      if let proc::ValExpr::ColumnRef(col_ref) = expr {
        if source == &col_ref.table_name {
          add_item(col_container, &col_ref.col_name);
        }
      }
    }
    _ => {}
  }
}

/// Construct a CB that collects every Table Alias underneath a `QueryElement`.
pub fn alias_collecting_cb<'a>(
  alias_container: &'a mut BTreeSet<String>,
) -> impl FnMut(QueryElement) -> () + 'a {
  move |elem: QueryElement| match elem {
    QueryElement::SuperSimpleSelect(select) => {
      alias_container.insert(select.from.name().clone());
    }
    QueryElement::Update(update) => {
      alias_container.insert(update.table.name().clone());
    }
    QueryElement::Insert(insert) => {
      alias_container.insert(insert.table.name().clone());
    }
    QueryElement::Delete(delete) => {
      alias_container.insert(delete.table.name().clone());
    }
    _ => {}
  }
}

/// Construct a CB that collects every `ColumnRef` under a `QueryElement`
/// not included in `alias_container`.
pub fn external_col_collecting_cb<'a>(
  alias_container: &'a BTreeSet<String>,
  external_cols: &'a mut BTreeSet<proc::ColumnRef>,
) -> impl FnMut(QueryElement) -> () + 'a {
  move |elem: QueryElement| match elem {
    QueryElement::ValExpr(expr) => {
      if let proc::ValExpr::ColumnRef(col_ref) = expr {
        if !alias_container.contains(&col_ref.table_name) {
          external_cols.insert(col_ref.clone());
        }
      }
    }
    _ => {}
  }
}

/// Construct a CB that collects every `TransTableName` underneath a `QueryElement`.
pub fn trans_table_collecting_cb<'a>(
  trans_table_container: &'a mut BTreeSet<TransTableName>,
) -> impl FnMut(QueryElement) -> () + 'a {
  move |elem: QueryElement| match elem {
    QueryElement::MSQuery(query) => {
      for (trans_table_name, _) in &query.trans_tables {
        trans_table_container.insert(trans_table_name.clone());
      }
    }
    QueryElement::GRQuery(query) => {
      for (trans_table_name, _) in &query.trans_tables {
        trans_table_container.insert(trans_table_name.clone());
      }
    }
    _ => {}
  }
}

/// Construct a CB that collects every `TransTableName` usage underneath a `QueryElement`
/// that is not included in `trans_table_container`.
pub fn external_trans_table_collecting_cb<'a>(
  trans_table_container: &'a BTreeSet<TransTableName>,
  external_trans_table: &'a mut BTreeSet<TransTableName>,
) -> impl FnMut(QueryElement) -> () + 'a {
  move |elem: QueryElement| match elem {
    QueryElement::SuperSimpleSelect(select) => {
      if let proc::GeneralSource::TransTableName { trans_table_name, .. } = &select.from {
        if !trans_table_container.contains(trans_table_name) {
          external_trans_table.insert(trans_table_name.clone());
        }
      }
    }
    _ => {}
  }
}
