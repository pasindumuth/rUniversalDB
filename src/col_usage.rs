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
//  ColUsagePlanner
// -----------------------------------------------------------------------------------------------

/// The main purpose of the `ColUsageNode` is for a parent ES to be able to compute the
/// `Context` that should be used for a child ES. That is why `requested_cols` only contains
/// `ColumnRef`s that can reference an ancestral `GeneralSource` if its query's table's schema
/// does not have it. (Hence why we do not include assigned columns in UPDATE queries or
/// inserted column in INSERT queries.)
///
/// Instead, if this is sent back with a MasterQueryPlanning, it will be hard facts that
/// `safe_present_cols` and `free_external_cols(external_cols)` *will* align with the table
/// schemas.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColUsageNode {
  /// The (Trans)Table used in the `proc::GeneralQuery` corresponding to this node.
  source: proc::GeneralSource,
  safe_present_cols: Vec<ColName>,

  /// The schema of the TransTable produced by this `ColUsageNode`.
  pub schema: Vec<Option<ColName>>,

  /// These are the ColNames used within all ValExprs outside of `Subquery` nodes.
  /// This is a convenience field used only in the ColUsagePlanner.
  requested_cols: Vec<proc::ColumnRef>,

  /// Take the union of `requested_cols` and all `external_cols` of all `ColUsageNodes`
  /// in `children`. Call this all_cols.
  ///
  /// Below, `safe_present_cols` is the subset of all_cols that are present in the (Trans)Table
  /// (according to the gossiped_db_schema). `external_cols` is the complement of that.
  pub children: Vec<Vec<(TransTableName, ColUsageNode)>>,

  /// The External Cols Property is where if a `table_name` is present in a `ColumnRef`, this
  /// will be different from `source.name()`. This can be seen since such a `ColumnRef` must
  /// either be placed into `safe_present_cols`, or the QueryPlanning should fail.
  pub external_cols: Vec<proc::ColumnRef>,
}

impl ColUsageNode {
  pub fn new(source: proc::GeneralSource) -> ColUsageNode {
    ColUsageNode {
      source,
      schema: vec![],
      requested_cols: vec![],
      children: vec![],
      safe_present_cols: vec![],
      external_cols: vec![],
    }
  }
}

/// This algorithm computes a `Vec<(TransTableName, ColUsageNode)>`
/// that is parallel to the provided `MSQuery`. This is a tree. Every `ColUsageNode`
/// corresponds to an `(MS/GR)QueryStage`, and every `Vec<(TransTableName, ColUsageNode)>`
/// corresponds to an `(MS/GR)Query`.
///
/// The validations in `perform_validations` needs to have been run through the `view`
/// before this `ColUsagePlanner` should be used.
pub struct ColUsagePlanner<ViewT: DBSchemaView> {
  pub view: ViewT,
}

impl<ErrorT: ErrorTrait, ViewT: DBSchemaView<ErrorT = ErrorT>> ColUsagePlanner<ViewT> {
  /// This is a generic function for computing a `ColUsageNode` for both Selects
  /// as well as Updates. Here, we use `trans_table_ctx` to check for column inclusions in
  /// the TransTables, and `self.gossip_db_schema` to check for column inclusion in the Tables.
  /// The `trans_table_ctx` is modified, accumulating all descendent TransTables. This is okay
  /// since TransTableNames are all uniques anyways. Finally, we amend the `schema` into the
  /// node as well.
  fn compute_frozen_col_usage_node(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    source: &proc::GeneralSource,
    exprs: &Vec<proc::ValExpr>,
    schema: Vec<Option<ColName>>,
  ) -> Result<ColUsageNode, ErrorT> {
    let mut node = ColUsageNode::new(source.clone());
    node.schema = schema;
    for expr in exprs {
      collect_top_level_cols_r(expr, &mut node.requested_cols);
      for query in collect_expr_subqueries(expr) {
        node.children.push(self.plan_gr_query(trans_table_ctx, &query)?);
      }
    }

    // Determine which columns are safe, and which are external.
    let mut all_cols = BTreeSet::<proc::ColumnRef>::new();
    for child_map in &node.children {
      for (_, child) in child_map {
        for col in &child.external_cols {
          all_cols.insert(col.clone());
        }
      }
    }

    for col in &node.requested_cols {
      all_cols.insert(col.clone());
    }

    // Split `all_cols` into `safe_present_cols` and `external_cols` based on the schema of
    // the (Trans)Table in question.
    let mut contains_col = |col_name: &ColName| -> Result<bool, ErrorT> {
      match source {
        proc::GeneralSource::TransTableName { trans_table_name, .. } => {
          // The Query converter will have made sure that all TransTableNames actually exist.
          let table_schema = trans_table_ctx.get(trans_table_name).unwrap();
          Ok(table_schema.contains(&Some(col_name.clone())))
        }
        proc::GeneralSource::TablePath { table_path, .. } => {
          // The Query converter will have made sure that all TablePaths actually exist.
          self.view.contains_col(table_path, col_name)
        }
        _ => panic!(),
      }
    };

    for col in all_cols {
      if source.name() == &col.table_name {
        if contains_col(&col.col_name)? {
          node.safe_present_cols.push(col.col_name);
        } else {
          return Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidColumnRef));
        }
      } else {
        node.external_cols.push(col);
      }
    }

    Ok(node)
  }

  /// Constructs a `ColUsageNode` and the schema of the returned TransTable.
  fn plan_select(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    select: &proc::SuperSimpleSelect,
  ) -> Result<ColUsageNode, ErrorT> {
    let mut projection = Vec::<Option<ColName>>::new();
    match &select.projection {
      proc::SelectClause::SelectList(select_list) => {
        for (select_item, alias) in select_list {
          if let Some(col) = alias {
            projection.push(Some(col.clone()));
          } else if let proc::SelectItem::ValExpr(proc::ValExpr::ColumnRef(col_ref)) = select_item {
            projection.push(Some(col_ref.col_name.clone()));
          } else {
            projection.push(None);
          }
        }
      }
      proc::SelectClause::Wildcard => match &select.from {
        proc::GeneralSource::TablePath { table_path, .. } => {
          for col in self.view.get_all_cols(table_path)? {
            projection.push(Some(col));
          }
        }
        proc::GeneralSource::TransTableName { trans_table_name, .. } => {
          projection = trans_table_ctx.get(trans_table_name).unwrap().clone();
        }
        _ => panic!(),
      },
    }

    let mut exprs = Vec::new();
    match &select.projection {
      proc::SelectClause::SelectList(select_list) => {
        let mut val_expr_count = 0;
        let mut unary_agg_count = 0;
        for (select_item, _) in select_list {
          match select_item {
            proc::SelectItem::ValExpr(expr) => {
              val_expr_count += 1;
              exprs.push(expr.clone());
            }
            proc::SelectItem::UnaryAggregate(unary_agg) => {
              unary_agg_count += 1;
              exprs.push(unary_agg.expr.clone());
            }
          }
        }
        if val_expr_count > 0 && unary_agg_count > 0 {
          return Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidSelectClause));
        }
      }
      proc::SelectClause::Wildcard => {}
    }

    exprs.push(select.selection.clone());

    Ok(self.compute_frozen_col_usage_node(trans_table_ctx, &select.from, &exprs, projection)?)
  }

  /// Constructs a `ColUsageNode` and the schema of the returned TransTable.
  fn plan_update(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    update: &proc::Update,
  ) -> Result<ColUsageNode, ErrorT> {
    let key_cols = self.view.key_cols(&update.table.source_ref)?;
    let mut projection = compute_update_schema(update, key_cols);

    let mut exprs = Vec::new();
    for (_, expr) in &update.assignment {
      exprs.push(expr.clone());
    }
    exprs.push(update.selection.clone());

    Ok(self.compute_frozen_col_usage_node(
      trans_table_ctx,
      &proc::GeneralSource::TablePath {
        table_path: update.table.source_ref.clone(),
        alias: update.table.alias.clone(),
      },
      &exprs,
      projection,
    )?)
  }

  /// Constructs a `ColUsageNode` and the schema of the returned TransTable.
  fn plan_insert(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    insert: &proc::Insert,
  ) -> Result<ColUsageNode, ErrorT> {
    let projection = compute_insert_schema(insert);

    let mut exprs = Vec::new();
    for row in &insert.values {
      for expr in row {
        exprs.push(expr.clone());
      }
    }

    Ok(self.compute_frozen_col_usage_node(
      trans_table_ctx,
      &proc::GeneralSource::TablePath {
        table_path: insert.table.source_ref.clone(),
        alias: insert.table.alias.clone(),
      },
      &exprs,
      projection,
    )?)
  }

  /// Constructs a `ColUsageNode` and the schema of the returned TransTable.
  fn plan_delete(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    delete: &proc::Delete,
  ) -> Result<ColUsageNode, ErrorT> {
    let mut projection = compute_delete_schema(delete);
    let mut exprs = vec![delete.selection.clone()];
    Ok(self.compute_frozen_col_usage_node(
      trans_table_ctx,
      &proc::GeneralSource::TablePath {
        table_path: delete.table.source_ref.clone(),
        alias: delete.table.alias.clone(),
      },
      &exprs,
      projection,
    )?)
  }

  /// Construct a `ColUsageNode` and the schema of the returned TransTable.
  fn plan_ms_query_stage(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    stage_query: &proc::MSQueryStage,
  ) -> Result<ColUsageNode, ErrorT> {
    match stage_query {
      proc::MSQueryStage::SuperSimpleSelect(select) => self.plan_select(trans_table_ctx, select),
      proc::MSQueryStage::Update(update) => self.plan_update(trans_table_ctx, update),
      proc::MSQueryStage::Insert(insert) => self.plan_insert(trans_table_ctx, insert),
      proc::MSQueryStage::Delete(delete) => self.plan_delete(trans_table_ctx, delete),
    }
  }

  /// Construct `ColUsageNode`s for the `gr_query`.
  fn plan_gr_query(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    gr_query: &proc::GRQuery,
  ) -> Result<Vec<(TransTableName, ColUsageNode)>, ErrorT> {
    let mut children = Vec::<(TransTableName, ColUsageNode)>::new();
    for (trans_table_name, (_, child_query)) in &gr_query.trans_tables {
      match child_query {
        proc::GRQueryStage::SuperSimpleSelect(select) => {
          let node = self.plan_select(trans_table_ctx, select)?;
          let cols = node.schema.clone();
          children.push((trans_table_name.clone(), node));
          trans_table_ctx.insert(trans_table_name.clone(), cols);
        }
      }
    }
    Ok(children)
  }

  /// Construct `ColUsageNode`s for the `ms_query`.
  pub fn plan_ms_query(
    &mut self,
    ms_query: &proc::MSQuery,
  ) -> Result<Vec<(TransTableName, ColUsageNode)>, ErrorT> {
    let mut trans_table_ctx = BTreeMap::<TransTableName, Vec<Option<ColName>>>::new();
    let mut children = Vec::<(TransTableName, ColUsageNode)>::new();
    for (trans_table_name, (_, child_query)) in &ms_query.trans_tables {
      let node = self.plan_ms_query_stage(&mut trans_table_ctx, child_query)?;
      let cols = node.schema.clone();
      children.push((trans_table_name.clone(), node));
      trans_table_ctx.insert(trans_table_name.clone(), cols);
    }

    // The top level `ColUsageNode`s cannot have external `ColumnRef`s.
    for (_, child) in &children {
      if !child.external_cols.is_empty() {
        return Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidColumnRef));
      }
    }

    // TODO: check that inserts don't have external_cols too.

    Ok(children)
  }

  pub fn finish(self) -> BTreeMap<TablePath, ColPresenceReq> {
    self.view.finish()
  }
}

pub fn compute_update_schema(
  update: &proc::Update,
  key_cols: &Vec<(ColName, ColType)>,
) -> Vec<Option<ColName>> {
  let mut projection = Vec::<Option<ColName>>::new();
  for (col, _) in key_cols {
    projection.push(Some(col.clone()));
  }
  for (col, _) in &update.assignment {
    projection.push(Some(col.clone()));
  }
  projection
}

fn compute_insert_schema(insert: &proc::Insert) -> Vec<Option<ColName>> {
  insert.columns.iter().cloned().map(|col| Some(col.clone())).collect()
}

fn compute_delete_schema(_: &proc::Delete) -> Vec<Option<ColName>> {
  vec![]
}

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
//  External Column and TransTable Computation
// -----------------------------------------------------------------------------------------------

/// Accumulates and returns all External `TransTableName`s that appear under `node`.
/// Recall that all `TransTablesName`s are unique. This should be determinisitic.
pub fn node_external_trans_tables(col_usage_node: &ColUsageNode) -> Vec<TransTableName> {
  let mut accum = Vec::<TransTableName>::new();
  node_external_trans_tables_r(col_usage_node, &mut BTreeSet::new(), &mut accum);
  accum
}

/// Same as above, except for multiple `nodes`. This should be determinisitic.
pub fn nodes_external_trans_tables(
  nodes: &Vec<(TransTableName, ColUsageNode)>,
) -> Vec<TransTableName> {
  let mut accum = Vec::<TransTableName>::new();
  nodes_external_trans_tables_r(nodes, &mut BTreeSet::new(), &mut accum);
  accum
}

/// Accumulates all External `TransTableName`s that appear under `node` into `accum`, where we
/// also exclude any that appear in `defined_trans_tables`.
///
/// Here, `defined_trans_tables` should remain unchanged, and `accum` should be determinisitic.
fn node_external_trans_tables_r(
  node: &ColUsageNode,
  defined_trans_tables: &mut BTreeSet<TransTableName>,
  accum: &mut Vec<TransTableName>,
) {
  if let proc::GeneralSource::TransTableName { trans_table_name, .. } = &node.source {
    if !defined_trans_tables.contains(trans_table_name) {
      accum.push(trans_table_name.clone())
    }
  }
  for nodes in &node.children {
    nodes_external_trans_tables_r(nodes, defined_trans_tables, accum);
  }
}

/// Same as the above function, except for multiple `nodes`.
fn nodes_external_trans_tables_r(
  nodes: &Vec<(TransTableName, ColUsageNode)>,
  defined_trans_tables: &mut BTreeSet<TransTableName>,
  accum: &mut Vec<TransTableName>,
) {
  for (trans_table_name, node) in nodes {
    defined_trans_tables.insert(trans_table_name.clone());
    node_external_trans_tables_r(node, defined_trans_tables, accum);
  }
  for (trans_table_name, _) in nodes {
    defined_trans_tables.remove(trans_table_name);
  }
}

/// Accumulates all External Columns in `nodes`.
pub fn nodes_external_cols(nodes: &Vec<(TransTableName, ColUsageNode)>) -> Vec<proc::ColumnRef> {
  let mut col_name_set = BTreeSet::<proc::ColumnRef>::new();
  for (_, node) in nodes {
    col_name_set.extend(node.external_cols.clone())
  }
  col_name_set.into_iter().collect()
}

// -----------------------------------------------------------------------------------------------
//  Query Element Iteration
// -----------------------------------------------------------------------------------------------
pub enum QueryElement<'a> {
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

pub fn iterate_ms_query<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::MSQuery,
) {
  for (_, (_, stage)) in &query.trans_tables {
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
}

pub fn iterate_gr_query<'a, CbT: FnMut(QueryElement<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::GRQuery,
) {
  for (_, (_, stage)) in &query.trans_tables {
    match stage {
      proc::GRQueryStage::SuperSimpleSelect(query) => {
        iterate_select(cb, query);
      }
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Expression iteration
// -----------------------------------------------------------------------------------------------

pub fn get_collecting_cb<'a>(
  source: &'a String,
  col_container: &'a mut Vec<ColName>,
) -> impl FnMut(QueryElement) -> () + 'a {
  move |elem: QueryElement| match elem {
    QueryElement::SuperSimpleSelect(_) => {}
    QueryElement::Update(_) => {}
    QueryElement::Insert(_) => {}
    QueryElement::Delete(_) => {}
    QueryElement::ValExpr(expr) => {
      if let proc::ValExpr::ColumnRef(col_ref) = expr {
        if source == &col_ref.table_name {
          add_item(col_container, &col_ref.col_name);
        }
      }
    }
  }
}
