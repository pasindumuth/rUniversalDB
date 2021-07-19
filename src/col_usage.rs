use crate::common::TableSchema;
use crate::model::common::{
  iast, proc, ColName, ColType, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
  TransTableName,
};
use crate::server::{contains_col, weak_contains_col};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::ops::Deref;

// -----------------------------------------------------------------------------------------------
//  ColUsagePlanner
// -----------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrozenColUsageNode {
  pub table_ref: proc::TableRef,
  pub requested_cols: Vec<ColName>,
  pub children: Vec<Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>>,
  pub safe_present_cols: Vec<ColName>,
  pub external_cols: Vec<ColName>,
}

impl FrozenColUsageNode {
  fn new(table_ref: proc::TableRef) -> FrozenColUsageNode {
    FrozenColUsageNode {
      table_ref: table_ref,
      requested_cols: vec![],
      children: vec![],
      safe_present_cols: vec![],
      external_cols: vec![],
    }
  }
}

pub struct ColUsagePlanner<'a> {
  pub gossiped_db_schema: &'a HashMap<TablePath, TableSchema>,
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
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    table_ref: &proc::TableRef,
    exprs: &Vec<proc::ValExpr>,
  ) -> FrozenColUsageNode {
    let mut node = FrozenColUsageNode::new(table_ref.clone());
    for expr in exprs {
      collect_top_level_cols_r(expr, &mut node.requested_cols);
      for query in collect_expr_subqueries(expr) {
        node.children.push(self.plan_gr_query(trans_table_ctx, &query));
      }
    }

    // Determine which columns are safe, and which are external.
    let mut all_cols = HashSet::<ColName>::new();
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

    // Split `all_cols` into `safe_present_cols` and `external_cols` based on
    // the (Trans)Table in question.
    match table_ref {
      proc::TableRef::TransTableName(trans_table_name) => {
        // The Query converter should make sure that all TransTableNames actually exist.
        let schema = trans_table_ctx.get(trans_table_name).unwrap();
        for col in all_cols {
          if schema.contains(&col) {
            node.safe_present_cols.push(col);
          } else {
            node.external_cols.push(col);
          }
        }
      }
      proc::TableRef::TablePath(table_path) => {
        // The Query converter should make sure that all TablePaths actually exist.
        let table_schema = self.gossiped_db_schema.get(table_path).unwrap();
        for col in all_cols {
          if weak_contains_col(&table_schema, &col, &self.timestamp) {
            node.safe_present_cols.push(col);
          } else {
            node.external_cols.push(col);
          }
        }
      }
    };

    return node;
  }

  /// Construct a `FrozenColUsageNode` for the `select`.
  pub fn plan_select(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    select: &proc::SuperSimpleSelect,
  ) -> (Vec<ColName>, FrozenColUsageNode) {
    (
      select.projection.clone(),
      self.compute_frozen_col_usage_node(
        trans_table_ctx,
        &select.from,
        &vec![select.selection.clone()],
      ),
    )
  }

  /// Construct a `FrozenColUsageNode` for the `update`.
  pub fn plan_update(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    update: &proc::Update,
  ) -> (Vec<ColName>, FrozenColUsageNode) {
    let mut projection = Vec::new();
    for (col, _) in &self.gossiped_db_schema.get(&update.table).unwrap().key_cols {
      projection.push(col.clone());
    }
    for (col, _) in &update.assignment {
      projection.push(col.clone());
    }

    let mut exprs = Vec::new();
    exprs.push(update.selection.clone());
    for (_, expr) in &update.assignment {
      exprs.push(expr.clone());
    }

    (
      projection,
      self.compute_frozen_col_usage_node(
        trans_table_ctx,
        &proc::TableRef::TablePath(update.table.clone()),
        &exprs,
      ),
    )
  }

  /// Construct a `FrozenColUsageNode` for the `stage_query`.
  pub fn plan_ms_query_stage(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    stage_query: &proc::MSQueryStage,
  ) -> (Vec<ColName>, FrozenColUsageNode) {
    match stage_query {
      proc::MSQueryStage::SuperSimpleSelect(select) => self.plan_select(trans_table_ctx, select),
      proc::MSQueryStage::Update(update) => self.plan_update(trans_table_ctx, update),
    }
  }

  /// Construct a `FrozenColUsageNode` for the `gr_query`.
  pub fn plan_gr_query(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    gr_query: &proc::GRQuery,
  ) -> Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))> {
    let mut children = Vec::<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>::new();
    for (trans_table_name, child_query) in &gr_query.trans_tables {
      match child_query {
        proc::GRQueryStage::SuperSimpleSelect(select) => {
          let (cols, node) = self.plan_select(trans_table_ctx, select);
          children.push((trans_table_name.clone(), (cols.clone(), node)));
          trans_table_ctx.insert(trans_table_name.clone(), cols);
        }
      }
    }
    return children;
  }

  /// Construct a `FrozenColUsageNode` for the `ms_query`.
  pub fn plan_ms_query(
    &mut self,
    ms_query: &proc::MSQuery,
  ) -> Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))> {
    let mut trans_table_ctx = HashMap::<TransTableName, Vec<ColName>>::new();
    let mut children = Vec::<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>::new();
    for (trans_table_name, child_query) in &ms_query.trans_tables {
      let (cols, node) = self.plan_ms_query_stage(&mut trans_table_ctx, child_query);
      children.push((trans_table_name.clone(), (cols.clone(), node)));
      trans_table_ctx.insert(trans_table_name.clone(), cols);
    }
    return children;
  }
}

// -----------------------------------------------------------------------------------------------
//  Collection functions
// -----------------------------------------------------------------------------------------------

/// This function returns all `ColName`s in the `expr` that don't fall
/// under a `Subquery`.
pub fn collect_top_level_cols(expr: &proc::ValExpr) -> Vec<ColName> {
  let mut cols = Vec::<ColName>::new();
  collect_top_level_cols_r(expr, &mut cols);
  cols
}

fn collect_top_level_cols_r(expr: &proc::ValExpr, cols: &mut Vec<ColName>) {
  match expr {
    proc::ValExpr::ColumnRef { col_ref } => cols.push(col_ref.clone()),
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
  node_external_trans_tables_r(col_usage_node, &mut HashSet::new(), &mut accum);
  accum
}

/// Same as above, except for multiple `nodes`. This should be determinisitic.
pub fn nodes_external_trans_tables(
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
) -> Vec<TransTableName> {
  let mut accum = Vec::<TransTableName>::new();
  nodes_external_trans_tables_r(nodes, &mut HashSet::new(), &mut accum);
  accum
}

/// Accumulates all External `TransTableName`s that appear under `node` into `accum`, where we
/// also exclude any that appear in `defined_trans_tables`.
///
/// Here, `defined_trans_tables` should remain unchanged, and `accum` should be determinisitic.
fn node_external_trans_tables_r(
  node: &FrozenColUsageNode,
  defined_trans_tables: &mut HashSet<TransTableName>,
  accum: &mut Vec<TransTableName>,
) {
  if let proc::TableRef::TransTableName(trans_table_name) = &node.table_ref {
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
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
  defined_trans_tables: &mut HashSet<TransTableName>,
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
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
) -> Vec<ColName> {
  let mut col_name_set = HashSet::<ColName>::new();
  for (_, (_, node)) in nodes {
    col_name_set.extend(node.external_cols.clone())
  }
  col_name_set.into_iter().collect()
}

// -----------------------------------------------------------------------------------------------
//  Tests
// -----------------------------------------------------------------------------------------------

#[cfg(test)]
mod test {
  use super::*;
  use crate::test_utils::{cn, mk_tab, mk_ttab};
  use std::collections::HashMap;

  #[test]
  fn basic_test() {
    let schema: HashMap<TablePath, TableSchema> = vec![
      (
        mk_tab("t1"),
        TableSchema::new(vec![(cn("c1"), ColType::String)], vec![(cn("c2"), ColType::Int)]),
      ),
      (
        mk_tab("t2"),
        TableSchema::new(
          vec![(cn("c1"), ColType::String), (cn("c3"), ColType::String)],
          vec![(cn("c4"), ColType::Int)],
        ),
      ),
      (
        mk_tab("t3"),
        TableSchema::new(
          vec![(cn("c5"), ColType::Int)],
          vec![(cn("c6"), ColType::String), (cn("c7"), ColType::Bool)],
        ),
      ),
    ]
    .into_iter()
    .collect();

    let ms_query = proc::MSQuery {
      trans_tables: vec![
        (
          mk_ttab("tt0"),
          proc::MSQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
            projection: vec![cn("c1"), cn("c4")],
            from: proc::TableRef::TablePath(mk_tab("t2")),
            selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
          }),
        ),
        (
          mk_ttab("tt1"),
          proc::MSQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
            projection: vec![cn("c1")],
            from: proc::TableRef::TransTableName(mk_ttab("tt0")),
            selection: proc::ValExpr::Subquery {
              query: Box::new(proc::GRQuery {
                trans_tables: vec![(
                  mk_ttab("tt2"),
                  proc::GRQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
                    projection: vec![cn("c5")],
                    from: proc::TableRef::TablePath(mk_tab("t3")),
                    selection: proc::ValExpr::BinaryExpr {
                      op: iast::BinaryOp::Plus,
                      left: Box::new(proc::ValExpr::ColumnRef { col_ref: cn("c1") }),
                      right: Box::new(proc::ValExpr::ColumnRef { col_ref: cn("c5") }),
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
            table: mk_tab("t1"),
            assignment: vec![(cn("c2"), proc::ValExpr::ColumnRef { col_ref: cn("c1") })],
            selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
          }),
        ),
      ]
      .into_iter()
      .collect(),
      returning: mk_ttab("tt1"),
    };

    let mut planner = ColUsagePlanner { gossiped_db_schema: &schema, timestamp: Timestamp(0) };
    let col_usage_nodes = planner.plan_ms_query(&ms_query);

    let exp_col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))> = vec![
      (
        mk_ttab("tt0"),
        (
          vec![cn("c1"), cn("c4")],
          FrozenColUsageNode {
            table_ref: proc::TableRef::TablePath(mk_tab("t2")),
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
          vec![cn("c1")],
          FrozenColUsageNode {
            table_ref: proc::TableRef::TransTableName(mk_ttab("tt0")),
            children: vec![vec![(
              mk_ttab("tt2"),
              (
                vec![cn("c5")],
                FrozenColUsageNode {
                  table_ref: proc::TableRef::TablePath(mk_tab("t3")),
                  requested_cols: vec![cn("c1"), cn("c5")],
                  children: vec![],
                  safe_present_cols: vec![cn("c5")],
                  external_cols: vec![cn("c1")],
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
          vec![cn("c1"), cn("c2")],
          FrozenColUsageNode {
            table_ref: proc::TableRef::TablePath(mk_tab("t1")),
            requested_cols: vec![cn("c1")],
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
