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

#[derive(Debug, Clone)]
pub enum QueryElement<'a> {
  MSQuery(&'a proc::MSQuery),
  GRQuery(&'a proc::GRQuery),
  GRQueryStage(&'a proc::GRQueryStage),
  TableSelect(&'a proc::TableSelect),
  TransTableSelect(&'a proc::TransTableSelect),
  JoinSelect(&'a proc::JoinSelect),
  JoinNode(&'a proc::JoinNode),
  JoinLeaf(&'a proc::JoinLeaf),
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

  pub fn iterate_select_items<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    projection: &'a Vec<proc::SelectItem>,
  ) {
    for item in projection {
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
  }

  pub fn iterate_join_leaf<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    leaf: &'a proc::JoinLeaf,
  ) {
    cb(QueryElement::JoinLeaf(leaf));
    self.iterate_gr_query(cb, &leaf.query);
  }

  pub fn iterate_join_node<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    node: &'a proc::JoinNode,
  ) {
    cb(QueryElement::JoinNode(node));
    match node {
      proc::JoinNode::JoinInnerNode(inner) => {
        self.iterate_join_node(cb, &inner.left);
        self.iterate_join_node(cb, &inner.right);
        for expr in &inner.strong_conjunctions {
          self.iterate_expr(cb, expr);
        }
        for expr in &inner.weak_conjunctions {
          self.iterate_expr(cb, expr);
        }
      }
      proc::JoinNode::JoinLeaf(leaf) => {
        self.iterate_join_leaf(cb, leaf);
      }
    }
  }

  pub fn iterate_table_select<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::TableSelect,
  ) {
    cb(QueryElement::TableSelect(query));
    self.iterate_select_items(cb, &query.projection);
    self.iterate_expr(cb, &query.selection)
  }

  pub fn iterate_trans_table_select<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::TransTableSelect,
  ) {
    cb(QueryElement::TransTableSelect(query));
    self.iterate_select_items(cb, &query.projection);
    self.iterate_expr(cb, &query.selection)
  }

  pub fn iterate_join_select<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query: &'a proc::JoinSelect,
  ) {
    cb(QueryElement::JoinSelect(query));
    self.iterate_select_items(cb, &query.projection);
    self.iterate_join_node(cb, &query.from);
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
      proc::MSQueryStage::TableSelect(query) => {
        self.iterate_table_select(cb, query);
      }
      proc::MSQueryStage::TransTableSelect(query) => {
        self.iterate_trans_table_select(cb, query);
      }
      proc::MSQueryStage::JoinSelect(query) => {
        self.iterate_join_select(cb, query);
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
    cb(QueryElement::GRQueryStage(stage));
    match stage {
      proc::GRQueryStage::TableSelect(query) => {
        self.iterate_table_select(cb, query);
      }
      proc::GRQueryStage::TransTableSelect(query) => {
        self.iterate_trans_table_select(cb, query);
      }
      proc::GRQueryStage::JoinSelect(query) => {
        self.iterate_join_select(cb, query);
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

  pub fn iterate_general<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
    &self,
    cb: &mut CbT,
    query_elem: QueryElement<'a>,
  ) {
    cb(query_elem.clone());
    match query_elem {
      QueryElement::MSQuery(ms_query) => {
        self.iterate_ms_query(cb, ms_query);
      }
      QueryElement::GRQuery(gr_query) => {
        self.iterate_gr_query(cb, gr_query);
      }
      QueryElement::GRQueryStage(gr_query_stage) => {
        self.iterate_gr_query_stage(cb, gr_query_stage);
      }
      QueryElement::TableSelect(table_select) => {
        self.iterate_table_select(cb, table_select);
      }
      QueryElement::TransTableSelect(trans_table_select) => {
        self.iterate_trans_table_select(cb, trans_table_select);
      }
      QueryElement::JoinSelect(join_select) => {
        self.iterate_join_select(cb, join_select);
      }
      QueryElement::JoinNode(join_node) => {
        self.iterate_join_node(cb, join_node);
      }
      QueryElement::JoinLeaf(join_leaf) => {
        self.iterate_join_leaf(cb, join_leaf);
      }
      QueryElement::Update(update) => {
        self.iterate_update(cb, update);
      }
      QueryElement::Insert(insert) => {
        self.iterate_insert(cb, insert);
      }
      QueryElement::Delete(delete) => {
        self.iterate_delete(cb, delete);
      }
      QueryElement::ValExpr(expr) => {
        self.iterate_expr(cb, expr);
      }
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Mutable Query Element Iteration
// -----------------------------------------------------------------------------------------------

pub enum QueryElementMut<'a> {
  MSQuery(&'a mut proc::MSQuery),
  GRQuery(&'a mut proc::GRQuery),
  TableSelect(&'a mut proc::TableSelect),
  TransTableSelect(&'a mut proc::TransTableSelect),
  JoinSelect(&'a mut proc::JoinSelect),
  JoinLeaf(&'a mut proc::JoinLeaf),
  Update(&'a mut proc::Update),
  Insert(&'a mut proc::Insert),
  Delete(&'a mut proc::Delete),
  ValExpr(&'a mut proc::ValExpr),
}

pub struct QueryIteratorMut {
  /// If this is true, we do not call `iterate_gr_query` for any elements underneath
  /// the query we passed in.
  top_level: bool,
}

impl QueryIteratorMut {
  pub fn new() -> QueryIteratorMut {
    QueryIteratorMut { top_level: false }
  }

  pub fn new_top_level() -> QueryIteratorMut {
    QueryIteratorMut { top_level: true }
  }

  pub fn iterate_expr<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    expr: &mut proc::ValExpr,
  ) {
    cb(QueryElementMut::ValExpr(expr));
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

  pub fn iterate_select_items<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    projection: &mut Vec<proc::SelectItem>,
  ) {
    for item in projection {
      match item {
        proc::SelectItem::ExprWithAlias { item, .. } => {
          let expr = match item {
            proc::SelectExprItem::ValExpr(expr) => expr,
            proc::SelectExprItem::UnaryAggregate(agg) => &mut agg.expr,
          };
          self.iterate_expr(cb, expr);
        }
        proc::SelectItem::Wildcard { .. } => {}
      }
    }
  }

  pub fn iterate_join_node<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    node: &mut proc::JoinNode,
  ) {
    match node {
      proc::JoinNode::JoinInnerNode(inner) => {
        self.iterate_join_node(cb, &mut inner.left);
        self.iterate_join_node(cb, &mut inner.right);
        for expr in &mut inner.strong_conjunctions {
          self.iterate_expr(cb, expr);
        }
        for expr in &mut inner.weak_conjunctions {
          self.iterate_expr(cb, expr);
        }
      }
      proc::JoinNode::JoinLeaf(leaf) => {
        cb(QueryElementMut::JoinLeaf(leaf));
        self.iterate_gr_query(cb, &mut leaf.query);
      }
    }
  }

  pub fn iterate_table_select<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    query: &mut proc::TableSelect,
  ) {
    cb(QueryElementMut::TableSelect(query));
    self.iterate_select_items(cb, &mut query.projection);
    self.iterate_expr(cb, &mut query.selection)
  }

  pub fn iterate_trans_table_select<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    query: &mut proc::TransTableSelect,
  ) {
    cb(QueryElementMut::TransTableSelect(query));
    self.iterate_select_items(cb, &mut query.projection);
    self.iterate_expr(cb, &mut query.selection)
  }

  pub fn iterate_join_select<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    query: &mut proc::JoinSelect,
  ) {
    cb(QueryElementMut::JoinSelect(query));
    self.iterate_join_node(cb, &mut query.from);
    self.iterate_select_items(cb, &mut query.projection);
  }

  pub fn iterate_update<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    query: &mut proc::Update,
  ) {
    cb(QueryElementMut::Update(query));
    for (_, expr) in &mut query.assignment {
      self.iterate_expr(cb, expr)
    }
    self.iterate_expr(cb, &mut query.selection)
  }

  pub fn iterate_insert<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    query: &mut proc::Insert,
  ) {
    cb(QueryElementMut::Insert(query));
    for row in &mut query.values {
      for expr in row {
        self.iterate_expr(cb, expr);
      }
    }
  }

  pub fn iterate_delete<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    query: &mut proc::Delete,
  ) {
    cb(QueryElementMut::Delete(query));
    self.iterate_expr(cb, &mut query.selection)
  }

  pub fn iterate_ms_query_stage<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    stage: &mut proc::MSQueryStage,
  ) {
    match stage {
      proc::MSQueryStage::TableSelect(query) => {
        self.iterate_table_select(cb, query);
      }
      proc::MSQueryStage::TransTableSelect(query) => {
        self.iterate_trans_table_select(cb, query);
      }
      proc::MSQueryStage::JoinSelect(query) => {
        self.iterate_join_select(cb, query);
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

  pub fn iterate_ms_query<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    query: &mut proc::MSQuery,
  ) {
    cb(QueryElementMut::MSQuery(query));
    for (_, stage) in &mut query.trans_tables {
      self.iterate_ms_query_stage(cb, stage);
    }
  }

  pub fn iterate_gr_query_stage<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    stage: &mut proc::GRQueryStage,
  ) {
    match stage {
      proc::GRQueryStage::TableSelect(query) => {
        self.iterate_table_select(cb, query);
      }
      proc::GRQueryStage::TransTableSelect(query) => {
        self.iterate_trans_table_select(cb, query);
      }
      proc::GRQueryStage::JoinSelect(query) => {
        self.iterate_join_select(cb, query);
      }
    }
  }

  pub fn iterate_gr_query<CbT: FnMut(QueryElementMut) -> ()>(
    &self,
    cb: &mut CbT,
    query: &mut proc::GRQuery,
  ) {
    cb(QueryElementMut::GRQuery(query));
    for (_, stage) in &mut query.trans_tables {
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
    QueryElement::TableSelect(select) => {
      alias_container.insert(select.from.alias.clone());
    }
    QueryElement::TransTableSelect(select) => {
      alias_container.insert(select.from.alias.clone());
    }
    QueryElement::JoinLeaf(leaf) => {
      alias_container.insert(leaf.alias.clone());
    }
    QueryElement::Update(update) => {
      alias_container.insert(update.table.alias.clone());
    }
    QueryElement::Insert(insert) => {
      alias_container.insert(insert.table.alias.clone());
    }
    QueryElement::Delete(delete) => {
      alias_container.insert(delete.table.alias.clone());
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
    QueryElement::TransTableSelect(select) => {
      let trans_table_name = &select.from.trans_table_name;
      if !trans_table_container.contains(trans_table_name) {
        external_trans_table.insert(trans_table_name.clone());
      }
    }
    _ => {}
  }
}
