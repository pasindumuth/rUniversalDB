use crate::common::{lookup, TableSchema};
use crate::model::common::proc::{GeneralSourceRef, MSQueryStage};
use crate::model::common::{
  iast, proc, ColName, Gen, TablePath, TierMap, Timestamp, TransTableName,
};
use crate::multiversion_map::MVM;
use crate::server::contains_col;
use serde::{Deserialize, Serialize};
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

// -----------------------------------------------------------------------------------------------
//  ColUsagePlanner
// -----------------------------------------------------------------------------------------------

/// There are several uses of this.
/// 1. The purpose of the `FrozenColUsageNode` is for a parent ES to be able to compute the
///    `Context` that should be used for a child ES. That is why `requested_cols` only contains
///    `ColumnRef`s that can reference an ancestral `GeneralSource` if its query's table's schema
///    does not have it. (Hence why we do not include assigned columns in UPDATE queries or
///    inserted column in INSERT queries.)
/// 2. When the MSCoordES creates this using its GossipData, `ColName`s in `safe_present_cols`
///    should be present, and `free_external_cols(external_cols)` should be absent in the schema.
///    The schemas should be checked and the Transaction aborted if the expectations fail.
/// 3. Instead, if this is sent back with a MasterQueryPlanning, it will be hard facts that
///    `safe_present_cols` and `free_external_cols(external_cols)` *will* align with the table
///    schemas.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrozenColUsageNode {
  /// The (Trans)Table used in the `proc::GeneralQuery` corresponding to this node.
  pub source: proc::GeneralSource,

  /// These are the ColNames used within all ValExprs outside of `Subquery` nodes.
  /// This is a convenience field used only in the ColUsagePlanner.
  pub requested_cols: Vec<proc::ColumnRef>,

  /// Take the union of `requested_cols` and all `external_cols` of all `FrozenColUsageNodes`
  /// in `children`. Call this all_cols.
  ///
  /// Below, `safe_present_cols` is the subset of all_cols that are present in the (Trans)Table
  /// (according to the gossiped_db_schema). `external_cols` is the complement of that.
  pub children: Vec<Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>>,
  pub safe_present_cols: Vec<ColName>,
  /// The External Cols Property is where if a `table_name` is present in a `ColumnRef`, this
  /// will be different from `source.name()`. This can be seen since such a `ColumnRef` must
  /// either be placed into `safe_present_cols`, or the QueryPlanning should fail.
  pub external_cols: Vec<proc::ColumnRef>,
}

impl FrozenColUsageNode {
  pub fn new(source: proc::GeneralSource) -> FrozenColUsageNode {
    FrozenColUsageNode {
      source,
      requested_cols: vec![],
      children: vec![],
      safe_present_cols: vec![],
      external_cols: vec![],
    }
  }
}

#[derive(Debug)]
pub enum ColUsageError {
  /// This is returned two ways:
  ///   1. If `ColumnRef` has a `source` that does not exist.
  ///   2. If `ColumnRef` has a `source` that exists, but the `ColName` is not in the
  ///      schema of that table.
  InvalidColumnRef,
  /// Returned if we detect that the Select clause is not right.
  InvalidSelectClause,
}

/// This algorithm computes a `Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>`
/// that is parallel to the provided `MSQuery`. This is a tree. Every `FrozenColUsageNode`
/// corresponds to an `(MS/GR)QueryStage`, and every `Vec<(TransTableName, (Vec<Option<ColName>>,
/// FrozenColUsageNode))>` corresponds to an `(MS/GR)Query`.
///
/// This algorithm contains the following assumptions:
///   1. All `TablePath`s referenced in the `MSQuery` exist in `table_generation` and `db_schema`
///      at the given `Timestamp` (by `static_read`).
///   2. All inserted columns in `INSERT` queries and all assigned columns in `UPDATE` queries
///      exist in the `db_schema`.
///   3. The assigned columns in an `UPDATE` are disjoint from the Key Columns. (This algorithm
///      does not support such queries).
///
/// In the above, (1) is critical (otherwise we might crash). The others might will not lead to
/// a crash, but since we use the inserted columns and assigned columns to compute the schema
/// of the resulting TransTable, we would like them to be present so that the resulting
/// `FrozenColUsageNode`s makes sense.
///
/// Users of this algorithm must verify these facts first.
pub struct ColUsagePlanner<'a> {
  pub db_schema: &'a BTreeMap<(TablePath, Gen), TableSchema>,
  pub table_generation: &'a MVM<TablePath, Gen>,
  pub timestamp: Timestamp,
}

impl<'a> ColUsagePlanner<'a> {
  /// This is a generic function for computing a `FrozenColUsageNode` for both Selects
  /// as well as Updates. Here, we use `trans_table_ctx` to check for column inclusions in
  /// the TransTables, and `self.gossip_db_schema` to check for column inclusion in the Tables.
  /// The `trans_table_ctx` is modified, accumulating all descendent TransTables. This is okay
  /// since TransTableNames are all uniques anyways.
  pub fn compute_frozen_col_usage_node(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    source: &proc::GeneralSource,
    exprs: &Vec<proc::ValExpr>,
  ) -> Result<FrozenColUsageNode, ColUsageError> {
    let mut node = FrozenColUsageNode::new(source.clone());
    for expr in exprs {
      collect_top_level_cols_r(expr, &mut node.requested_cols);
      for query in collect_expr_subqueries(expr) {
        node.children.push(self.plan_gr_query(trans_table_ctx, &query)?);
      }
    }

    // Determine which columns are safe, and which are external.
    let mut all_cols = BTreeSet::<proc::ColumnRef>::new();
    for child_map in &node.children {
      for (_, (_, child)) in child_map {
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
    let contains_col = |col_name: &ColName| {
      match &source.source_ref {
        GeneralSourceRef::TransTableName(trans_table_name) => {
          // The Query converter will have made sure that all TransTableNames actually exist.
          let table_schema = trans_table_ctx.get(trans_table_name).unwrap();
          table_schema.contains(&Some(col_name.clone()))
        }
        GeneralSourceRef::TablePath(table_path) => {
          // The Query converter will have made sure that all TablePaths actually exist.
          let gen = self.table_generation.static_read(table_path, self.timestamp).unwrap();
          let table_schema = self.db_schema.get(&(table_path.clone(), gen.clone())).unwrap();
          contains_col(&table_schema, col_name, &self.timestamp)
        }
      }
    };

    for col in all_cols {
      if let Some(table_name) = &col.table_name {
        if source.name() == table_name {
          if contains_col(&col.col_name) {
            node.safe_present_cols.push(col.col_name);
          } else {
            return Err(ColUsageError::InvalidColumnRef);
          }
        } else {
          node.external_cols.push(col);
        }
      } else {
        if contains_col(&col.col_name) {
          node.safe_present_cols.push(col.col_name);
        } else {
          node.external_cols.push(col);
        }
      }
    }

    Ok(node)
  }

  /// Constructs a `FrozenColUsageNode` and the schema of the returned TransTable.
  pub fn plan_select(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    select: &proc::SuperSimpleSelect,
  ) -> Result<(Vec<Option<ColName>>, FrozenColUsageNode), ColUsageError> {
    let mut projection = compute_select_schema(select);

    let mut exprs = Vec::new();
    let mut val_expr_count = 0;
    let mut unary_agg_count = 0;
    for (select_item, _) in &select.projection {
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
      return Err(ColUsageError::InvalidSelectClause);
    }

    exprs.push(select.selection.clone());

    Ok((projection, self.compute_frozen_col_usage_node(trans_table_ctx, &select.from, &exprs)?))
  }

  /// Constructs a `FrozenColUsageNode` and the schema of the returned TransTable.
  pub fn plan_update(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    update: &proc::Update,
  ) -> Result<(Vec<Option<ColName>>, FrozenColUsageNode), ColUsageError> {
    let gen = self.table_generation.static_read(&update.table.source_ref, self.timestamp).unwrap();
    let table_schema =
      &self.db_schema.get(&(update.table.source_ref.clone(), gen.clone())).unwrap();
    let mut projection = compute_update_schema(update, table_schema);

    let mut exprs = Vec::new();
    for (_, expr) in &update.assignment {
      exprs.push(expr.clone());
    }
    exprs.push(update.selection.clone());

    Ok((
      projection,
      self.compute_frozen_col_usage_node(
        trans_table_ctx,
        &proc::GeneralSource {
          source_ref: proc::GeneralSourceRef::TablePath(update.table.source_ref.clone()),
          alias: update.table.alias.clone(),
        },
        &exprs,
      )?,
    ))
  }

  /// Constructs a `FrozenColUsageNode` and the schema of the returned TransTable.
  pub fn plan_insert(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    insert: &proc::Insert,
  ) -> Result<(Vec<Option<ColName>>, FrozenColUsageNode), ColUsageError> {
    let projection = compute_insert_schema(insert);
    Ok((
      projection,
      self.compute_frozen_col_usage_node(
        trans_table_ctx,
        &proc::GeneralSource {
          source_ref: proc::GeneralSourceRef::TablePath(insert.table.source_ref.clone()),
          alias: insert.table.alias.clone(),
        },
        &Vec::new(), // No expressions
      )?,
    ))
  }

  /// Constructs a `FrozenColUsageNode` and the schema of the returned TransTable.
  pub fn plan_delete(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    delete: &proc::Delete,
  ) -> Result<(Vec<Option<ColName>>, FrozenColUsageNode), ColUsageError> {
    let mut projection = compute_delete_schema(delete);
    let mut exprs = vec![delete.selection.clone()];
    Ok((
      projection,
      self.compute_frozen_col_usage_node(
        trans_table_ctx,
        &proc::GeneralSource {
          source_ref: proc::GeneralSourceRef::TablePath(delete.table.source_ref.clone()),
          alias: delete.table.alias.clone(),
        },
        &exprs,
      )?,
    ))
  }

  /// Construct a `FrozenColUsageNode` and the schema of the returned TransTable.
  pub fn plan_ms_query_stage(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    stage_query: &proc::MSQueryStage,
  ) -> Result<(Vec<Option<ColName>>, FrozenColUsageNode), ColUsageError> {
    match stage_query {
      proc::MSQueryStage::SuperSimpleSelect(select) => self.plan_select(trans_table_ctx, select),
      proc::MSQueryStage::Update(update) => self.plan_update(trans_table_ctx, update),
      proc::MSQueryStage::Insert(insert) => self.plan_insert(trans_table_ctx, insert),
      proc::MSQueryStage::Delete(delete) => self.plan_delete(trans_table_ctx, delete),
    }
  }

  /// Construct `FrozenColUsageNode`s for the `gr_query`.
  pub fn plan_gr_query(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<Option<ColName>>>,
    gr_query: &proc::GRQuery,
  ) -> Result<Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>, ColUsageError> {
    let mut children = Vec::<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>::new();
    for (trans_table_name, child_query) in &gr_query.trans_tables {
      match child_query {
        proc::GRQueryStage::SuperSimpleSelect(select) => {
          let (cols, node) = self.plan_select(trans_table_ctx, select)?;
          children.push((trans_table_name.clone(), (cols.clone(), node)));
          trans_table_ctx.insert(trans_table_name.clone(), cols);
        }
      }
    }
    Ok(children)
  }

  /// Construct `FrozenColUsageNode`s for the `ms_query`.
  pub fn plan_ms_query(
    &mut self,
    ms_query: &proc::MSQuery,
  ) -> Result<Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>, ColUsageError> {
    let mut trans_table_ctx = BTreeMap::<TransTableName, Vec<Option<ColName>>>::new();
    let mut children = Vec::<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>::new();
    for (trans_table_name, child_query) in &ms_query.trans_tables {
      let (cols, node) = self.plan_ms_query_stage(&mut trans_table_ctx, child_query)?;
      children.push((trans_table_name.clone(), (cols.clone(), node)));
      trans_table_ctx.insert(trans_table_name.clone(), cols);
    }

    // The top level `FrozenColUsageNode`s cannot have external `ColumnRef`s.
    for (_, (_, child)) in &children {
      if !child.external_cols.is_empty() {
        return Err(ColUsageError::InvalidColumnRef);
      }
    }

    Ok(children)
  }
}

pub fn compute_select_schema(select: &proc::SuperSimpleSelect) -> Vec<Option<ColName>> {
  let mut projection = Vec::<Option<ColName>>::new();
  for (select_item, alias) in &select.projection {
    if let Some(col) = alias {
      projection.push(Some(col.clone()));
    } else if let proc::SelectItem::ValExpr(proc::ValExpr::ColumnRef(col_ref)) = select_item {
      projection.push(Some(col_ref.col_name.clone()));
    } else {
      projection.push(None);
    }
  }
  projection
}

pub fn compute_update_schema(
  update: &proc::Update,
  table_schema: &TableSchema,
) -> Vec<Option<ColName>> {
  let mut projection = Vec::<Option<ColName>>::new();
  for (col, _) in &table_schema.key_cols {
    projection.push(Some(col.clone()));
  }
  for (col, _) in &update.assignment {
    projection.push(Some(col.clone()));
  }
  projection
}

pub fn compute_insert_schema(insert: &proc::Insert) -> Vec<Option<ColName>> {
  insert.columns.iter().cloned().map(|col| Some(col.clone())).collect()
}

pub fn compute_delete_schema(_: &proc::Delete) -> Vec<Option<ColName>> {
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
pub fn node_external_trans_tables(col_usage_node: &FrozenColUsageNode) -> Vec<TransTableName> {
  let mut accum = Vec::<TransTableName>::new();
  node_external_trans_tables_r(col_usage_node, &mut BTreeSet::new(), &mut accum);
  accum
}

/// Same as above, except for multiple `nodes`. This should be determinisitic.
pub fn nodes_external_trans_tables(
  nodes: &Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>,
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
  node: &FrozenColUsageNode,
  defined_trans_tables: &mut BTreeSet<TransTableName>,
  accum: &mut Vec<TransTableName>,
) {
  if let proc::GeneralSourceRef::TransTableName(trans_table_name) = &node.source.source_ref {
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
  nodes: &Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>,
  defined_trans_tables: &mut BTreeSet<TransTableName>,
  accum: &mut Vec<TransTableName>,
) {
  for (trans_table_name, (_, node)) in nodes {
    defined_trans_tables.insert(trans_table_name.clone());
    node_external_trans_tables_r(node, defined_trans_tables, accum);
  }
  for (trans_table_name, _) in nodes {
    defined_trans_tables.remove(trans_table_name);
  }
}

/// Accumulates all External Columns in `nodes`.
pub fn nodes_external_cols(
  nodes: &Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>,
) -> Vec<proc::ColumnRef> {
  let mut col_name_set = BTreeSet::<proc::ColumnRef>::new();
  for (_, (_, node)) in nodes {
    col_name_set.extend(node.external_cols.clone())
  }
  col_name_set.into_iter().collect()
}

/// Filters for only the `ColumnRef`s without an alias. These are the `ColName`s that must not
/// exist in the Data Source of the `FrozenColUsageNode` that this `external_col` is present in.
/// This is because of the External Cols Property in `FrozenColUsageNode`.
pub fn free_external_cols(external_cols: &Vec<proc::ColumnRef>) -> Vec<ColName> {
  let mut free_external_cols = Vec::<ColName>::new();
  for col in external_cols {
    if col.table_name.is_none() {
      free_external_cols.push(col.col_name.clone())
    }
  }
  free_external_cols
}

// -----------------------------------------------------------------------------------------------
//  Stage Iteration
// -----------------------------------------------------------------------------------------------
pub enum GeneralStage<'a> {
  SuperSimpleSelect(&'a proc::SuperSimpleSelect),
  Update(&'a proc::Update),
  Insert(&'a proc::Insert),
  Delete(&'a proc::Delete),
}

fn iterate_stage_expr<'a, CbT: FnMut(GeneralStage<'a>) -> ()>(
  cb: &mut CbT,
  expr: &'a proc::ValExpr,
) {
  match expr {
    proc::ValExpr::ColumnRef { .. } => {}
    proc::ValExpr::UnaryExpr { expr, .. } => iterate_stage_expr(cb, expr),
    proc::ValExpr::BinaryExpr { left, right, .. } => {
      iterate_stage_expr(cb, left);
      iterate_stage_expr(cb, right);
    }
    proc::ValExpr::Value { .. } => {}
    proc::ValExpr::Subquery { query } => {
      iterate_stage_gr_query(cb, query);
    }
  }
}

fn iterate_stage_gr_query<'a, CbT: FnMut(GeneralStage<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::GRQuery,
) {
  for (_, stage) in &query.trans_tables {
    match stage {
      proc::GRQueryStage::SuperSimpleSelect(query) => {
        cb(GeneralStage::SuperSimpleSelect(query));
        iterate_stage_expr(cb, &query.selection)
      }
    }
  }
}

pub fn iterate_stage_ms_query<'a, CbT: FnMut(GeneralStage<'a>) -> ()>(
  cb: &mut CbT,
  query: &'a proc::MSQuery,
) {
  for (_, stage) in &query.trans_tables {
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(query) => {
        cb(GeneralStage::SuperSimpleSelect(query));
        iterate_stage_expr(cb, &query.selection)
      }
      proc::MSQueryStage::Update(query) => {
        cb(GeneralStage::Update(query));
        for (_, expr) in &query.assignment {
          iterate_stage_expr(cb, expr)
        }
        iterate_stage_expr(cb, &query.selection)
      }
      proc::MSQueryStage::Insert(query) => {
        cb(GeneralStage::Insert(query));
      }
      proc::MSQueryStage::Delete(query) => {
        cb(GeneralStage::Delete(query));
        iterate_stage_expr(cb, &query.selection)
      }
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Tests
// -----------------------------------------------------------------------------------------------

#[cfg(test)]
mod test {
  use super::*;
  use crate::model::common::ColType;
  use crate::test_utils::{cn, cno, mk_tab, mk_ttab};
  use std::collections::BTreeMap;

  #[test]
  fn basic_test() {
    let mut table_generation: MVM<TablePath, Gen> = MVM::new();
    table_generation.write(&mk_tab("t1"), Some(Gen(0)), 1);
    table_generation.write(&mk_tab("t2"), Some(Gen(0)), 1);
    table_generation.write(&mk_tab("t3"), Some(Gen(0)), 1);

    let db_schema: BTreeMap<(TablePath, Gen), TableSchema> = vec![
      (
        (mk_tab("t1"), Gen(0)),
        TableSchema::new(vec![(cn("c1"), ColType::String)], vec![(cn("c2"), ColType::Int)]),
      ),
      (
        (mk_tab("t2"), Gen(0)),
        TableSchema::new(
          vec![(cn("c1"), ColType::String), (cn("c3"), ColType::String)],
          vec![(cn("c4"), ColType::Int)],
        ),
      ),
      (
        (mk_tab("t3"), Gen(0)),
        TableSchema::new(
          vec![(cn("c5"), ColType::Int)],
          vec![(cn("c6"), ColType::String), (cn("c7"), ColType::Bool)],
        ),
      ),
    ]
    .into_iter()
    .collect();

    fn cref(s: &str) -> proc::ColumnRef {
      proc::ColumnRef { table_name: None, col_name: cn("c1") }
    }

    fn mk_read_src(s: &str) -> proc::GeneralSource {
      proc::GeneralSource { source_ref: proc::GeneralSourceRef::TablePath(mk_tab(s)), alias: None }
    }

    let ms_query = proc::MSQuery {
      trans_tables: vec![
        (
          mk_ttab("tt0"),
          proc::MSQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
            distinct: false,
            projection: vec![
              (proc::SelectItem::ValExpr(proc::ValExpr::ColumnRef(cref("c1"))), None),
              (proc::SelectItem::ValExpr(proc::ValExpr::ColumnRef(cref("c4"))), None),
            ],
            from: mk_read_src("t2"),
            selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
          }),
        ),
        (
          mk_ttab("tt1"),
          proc::MSQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
            distinct: false,
            projection: vec![(
              proc::SelectItem::ValExpr(proc::ValExpr::ColumnRef(cref("c1"))),
              None,
            )],
            from: mk_read_src("tt0"),
            selection: proc::ValExpr::Subquery {
              query: Box::new(proc::GRQuery {
                trans_tables: vec![(
                  mk_ttab("tt2"),
                  proc::GRQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
                    distinct: false,
                    projection: vec![(
                      proc::SelectItem::ValExpr(proc::ValExpr::ColumnRef(cref("c5"))),
                      None,
                    )],
                    from: mk_read_src("t3"),
                    selection: proc::ValExpr::BinaryExpr {
                      op: iast::BinaryOp::Plus,
                      left: Box::new(proc::ValExpr::ColumnRef(cref("c1"))),
                      right: Box::new(proc::ValExpr::ColumnRef(cref("c5"))),
                    },
                  }),
                )]
                .into_iter()
                .collect(),
                returning: mk_ttab("tt2"),
              }),
            },
          }),
        ),
        (
          mk_ttab("tt3"),
          proc::MSQueryStage::Update(proc::Update {
            table: proc::SimpleSource { source_ref: mk_tab("t1"), alias: None },
            assignment: vec![(cn("c2"), proc::ValExpr::ColumnRef(cref("c1")))],
            selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
          }),
        ),
      ]
      .into_iter()
      .collect(),
      returning: mk_ttab("tt1"),
    };

    let mut planner =
      ColUsagePlanner { db_schema: &db_schema, table_generation: &table_generation, timestamp: 1 };
    let col_usage_nodes = planner.plan_ms_query(&ms_query).unwrap();

    let exp_col_usage_nodes: Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))> = vec![
      (
        mk_ttab("tt0"),
        (
          vec![cno("c1"), cno("c4")],
          FrozenColUsageNode {
            source: proc::GeneralSource {
              source_ref: proc::GeneralSourceRef::TablePath(mk_tab("t2")),
              alias: None,
            },
            requested_cols: vec![],
            children: vec![],
            safe_present_cols: vec![],
            external_cols: vec![],
          },
        ),
      ),
      (
        mk_ttab("tt1"),
        (
          vec![cno("c1")],
          FrozenColUsageNode {
            source: proc::GeneralSource {
              source_ref: proc::GeneralSourceRef::TablePath(mk_tab("tt0")),
              alias: None,
            },
            children: vec![vec![(
              mk_ttab("tt2"),
              (
                vec![cno("c5")],
                FrozenColUsageNode {
                  source: proc::GeneralSource {
                    source_ref: proc::GeneralSourceRef::TablePath(mk_tab("t3")),
                    alias: None,
                  },

                  requested_cols: vec![cref("c1"), cref("c5")],
                  children: vec![],
                  safe_present_cols: vec![cn("c5")],
                  external_cols: vec![cref("c1")],
                },
              ),
            )]
            .into_iter()
            .collect()],
            requested_cols: vec![],
            safe_present_cols: vec![cn("c1")],
            external_cols: vec![],
          },
        ),
      ),
      (
        mk_ttab("tt3"),
        (
          vec![cno("c1"), cno("c2")],
          FrozenColUsageNode {
            source: proc::GeneralSource {
              source_ref: proc::GeneralSourceRef::TablePath(mk_tab("t1")),
              alias: None,
            },
            requested_cols: vec![cref("c1")],
            children: vec![],
            safe_present_cols: vec![cn("c1")],
            external_cols: vec![],
          },
        ),
      ),
    ];

    assert_eq!(col_usage_nodes, exp_col_usage_nodes);
  }
}
