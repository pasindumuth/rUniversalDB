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

pub struct QueryIterator {
  /// If this is true, we do not call `iterate_gr_query` for any elements underneath
  /// the query we passed in.
  top_level: bool,
}

impl QueryIterator {
  pub fn new() -> QueryIterator {
    QueryIterator { top_level: false }
  }

  pub fn new_top_level() -> QueryIterator {
    QueryIterator { top_level: true }
  }

  pub fn iterate_expr<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    expr: &'a proc::ValExpr,
  ) {
    cb(QueryElement::ValExpr(expr));
    match expr {
      proc::ValExpr::ColumnRef(_) => {}
      proc::ValExpr::UnaryExpr { expr, .. } => self.iterate_expr(cb, expr),
      proc::ValExpr::BinaryExpr { left, right, .. } => {
        self.iterate_expr(cb, left);
        self.iterate_expr(cb, right);
      }
      proc::ValExpr::Value { .. } => {}
      proc::ValExpr::Subquery { query } => {
        if !self.top_level {
          self.iterate_gr_query(cb, query);
        }
      }
    }
  }

  pub fn iterate_select<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::SuperSimpleSelect,
  ) {
    cb(QueryElement::SuperSimpleSelect(query));
    for item in &query.projection {
      match item {
        proc::SelectItem::ExprWithAlias { item, .. } => {
          let expr = match item {
            proc::SelectExprItem::ValExpr(expr) => expr,
            proc::SelectExprItem::UnaryAggregate(agg) => &agg.expr,
          };
          self.iterate_expr(cb, expr);
        }
        proc::SelectItem::Wildcard { .. } => {}
      }
    }
    self.iterate_expr(cb, &query.selection)
  }

  pub fn iterate_update<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::Update,
  ) {
    cb(QueryElement::Update(query));
    for (_, expr) in &query.assignment {
      self.iterate_expr(cb, expr)
    }
    self.iterate_expr(cb, &query.selection)
  }

  pub fn iterate_insert<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::Insert,
  ) {
    cb(QueryElement::Insert(query));
    for row in &query.values {
      for expr in row {
        self.iterate_expr(cb, expr);
      }
    }
  }

  pub fn iterate_delete<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::Delete,
  ) {
    cb(QueryElement::Delete(query));
    self.iterate_expr(cb, &query.selection)
  }

  pub fn iterate_ms_query_stage<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    stage: &'a proc::MSQueryStage,
  ) {
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(query) => {
        self.iterate_select(cb, query);
      }
      proc::MSQueryStage::Update(query) => {
        self.iterate_update(cb, query);
      }
      proc::MSQueryStage::Insert(query) => {
        self.iterate_insert(cb, query);
      }
      proc::MSQueryStage::Delete(query) => {
        self.iterate_delete(cb, query);
      }
    }
  }

  pub fn iterate_ms_query<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::MSQuery,
  ) {
    cb(QueryElement::MSQuery(query));
    for (_, stage) in &query.trans_tables {
      self.iterate_ms_query_stage(cb, stage);
    }
  }

  pub fn iterate_gr_query_stage<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    stage: &'a proc::GRQueryStage,
  ) {
    match stage {
      proc::GRQueryStage::SuperSimpleSelect(query) => {
        self.iterate_select(cb, query);
      }
    }
  }

  pub fn iterate_gr_query<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::GRQuery,
  ) {
    cb(QueryElement::GRQuery(query));
    for (_, stage) in &query.trans_tables {
      self.iterate_gr_query_stage(cb, stage);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Expression iteration
// -----------------------------------------------------------------------------------------------

/// Collects all `ColName`s that are paret of a `ColumnRef` that refers to `source`
/// underneath a `QueryElement`.
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

/// Collects all `ColumnRef`s underneath a `QueryElement`.
pub fn col_ref_collecting_cb<'a>(
  col_container: &'a mut BTreeSet<proc::ColumnRef>,
) -> impl FnMut(QueryElement) -> () + 'a {
  move |elem: QueryElement| match elem {
    QueryElement::ValExpr(expr) => {
      if let proc::ValExpr::ColumnRef(col_ref) = expr {
        col_container.insert(col_ref.clone());
      }
    }
    _ => {}
  }
}

/// Collects all `GRQuery`s underneath a `QueryElement`.
pub fn gr_query_collecting_cb<'a>(
  gr_query_container: &'a mut Vec<proc::GRQuery>,
) -> impl FnMut(QueryElement) -> () + 'a {
  move |elem: QueryElement| match elem {
    QueryElement::ValExpr(expr) => {
      if let proc::ValExpr::Subquery { query } = expr {
        gr_query_container.push(query.deref().clone());
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
