use crate::common::TableSchema;
use crate::model::common::{
  iast, proc, ColName, ColType, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
  TransTableName,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::ops::Deref;

// -----------------------------------------------------------------------------------------------
//  FrozenColUsageAlgorithm
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
  fn collect_subqueries(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    subqueries: &mut Vec<Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>>,
    expr: &proc::ValExpr,
  ) {
    for query in collect_expr_subqueries(expr) {
      subqueries.push(self.plan_gr_query(trans_table_ctx, &query));
    }
  }

  pub fn get_all_cols(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    exprs: &Vec<proc::ValExpr>,
  ) -> Vec<ColName> {
    let mut requested_cols = Vec::<ColName>::new();
    let mut children = Vec::<Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>>::new();
    for expr in exprs {
      collect_top_level_cols_R(expr, &mut requested_cols);
      self.collect_subqueries(trans_table_ctx, &mut children, expr);
    }

    // Determine all columns in use by the query.
    let mut all_cols = HashSet::<ColName>::new();
    for child_map in children {
      for (_, (_, child)) in child_map {
        for col in &child.external_cols {
          all_cols.insert(col.clone());
        }
      }
    }
    for col in requested_cols {
      all_cols.insert(col.clone());
    }

    return all_cols.into_iter().collect();
  }

  pub fn plan_stage_query_with_schema(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    projection: &Vec<ColName>,
    table_ref: &proc::TableRef,
    current_cols: Vec<ColName>,
    exprs: &Vec<proc::ValExpr>,
  ) -> (Vec<ColName>, FrozenColUsageNode) {
    let mut node = FrozenColUsageNode::new(table_ref.clone());
    for expr in exprs {
      collect_top_level_cols_R(expr, &mut node.requested_cols);
      self.collect_subqueries(trans_table_ctx, &mut node.children, expr);
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

    let current_cols_set = HashSet::<ColName>::from_iter(current_cols.into_iter());
    for col in all_cols {
      if current_cols_set.contains(&col) {
        node.safe_present_cols.push(col);
      } else {
        node.external_cols.push(col);
      }
    }

    return (projection.clone(), node);
  }

  fn plan_stage_query(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    projection: &Vec<ColName>,
    table_ref: &proc::TableRef,
    exprs: &Vec<proc::ValExpr>,
  ) -> (Vec<ColName>, FrozenColUsageNode) {
    // Determine the set of existing columns for this table_ref.
    let current_cols = match table_ref {
      proc::TableRef::TransTableName(trans_table_name) => {
        // The Query converter should make sure that all TransTableNames actually exist.
        trans_table_ctx.get(trans_table_name).unwrap().clone()
      }
      proc::TableRef::TablePath(table_path) => {
        // The Query converter should make sure that all TablePaths actually exist.
        let mut current_cols = Vec::new();
        let table_schema = self.gossiped_db_schema.get(table_path).unwrap();
        for (col_name, _) in &table_schema.key_cols {
          current_cols.push(col_name.clone());
        }
        for (col_name, _) in &table_schema.val_cols.static_snapshot_read(self.timestamp) {
          current_cols.push(col_name.clone());
        }
        current_cols
      }
    };

    return self.plan_stage_query_with_schema(
      trans_table_ctx,
      projection,
      table_ref,
      current_cols,
      exprs,
    );
  }

  pub fn plan_gr_query(
    &mut self,
    trans_table_ctx: &mut HashMap<TransTableName, Vec<ColName>>,
    query: &proc::GRQuery,
  ) -> Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))> {
    let mut children = Vec::<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>::new();
    for (trans_table_name, child_query) in &query.trans_tables {
      match child_query {
        proc::GRQueryStage::SuperSimpleSelect(select) => {
          let (cols, node) = self.plan_stage_query(
            trans_table_ctx,
            &select.projection,
            &select.from,
            &vec![select.selection.clone()],
          );
          children.push((trans_table_name.clone(), (cols.clone(), node)));
          trans_table_ctx.insert(trans_table_name.clone(), cols);
        }
      }
    }
    return children;
  }

  pub fn plan_ms_query(
    &mut self,
    query: &proc::MSQuery,
  ) -> HashMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)> {
    let mut trans_table_ctx = HashMap::<TransTableName, Vec<ColName>>::new();
    let mut children = HashMap::<TransTableName, (Vec<ColName>, FrozenColUsageNode)>::new();
    for (trans_table_name, child_query) in &query.trans_tables {
      match child_query {
        proc::MSQueryStage::SuperSimpleSelect(select) => {
          let (cols, node) = self.plan_stage_query(
            &mut trans_table_ctx,
            &select.projection,
            &select.from,
            &vec![select.selection.clone()],
          );
          children.insert(trans_table_name.clone(), (cols.clone(), node));
          trans_table_ctx.insert(trans_table_name.clone(), cols);
        }
        proc::MSQueryStage::Update(update) => {
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

          let (cols, node) = self.plan_stage_query(
            &mut trans_table_ctx,
            &projection,
            &proc::TableRef::TablePath(update.table.clone()),
            &exprs,
          );
          children.insert(trans_table_name.clone(), (cols.clone(), node));
          trans_table_ctx.insert(trans_table_name.clone(), cols);
        }
      }
    }
    return children;
  }
}

pub fn collect_top_level_cols(expr: &proc::ValExpr) -> Vec<ColName> {
  let mut cols = Vec::<ColName>::new();
  collect_top_level_cols_R(expr, &mut cols);
  cols
}

fn collect_top_level_cols_R(expr: &proc::ValExpr, cols: &mut Vec<ColName>) {
  match expr {
    proc::ValExpr::ColumnRef { col_ref } => cols.push(col_ref.clone()),
    proc::ValExpr::UnaryExpr { expr, .. } => collect_top_level_cols_R(&expr, cols),
    proc::ValExpr::BinaryExpr { left, right, .. } => {
      collect_top_level_cols_R(&left, cols);
      collect_top_level_cols_R(&right, cols);
    }
    proc::ValExpr::Value { .. } => {}
    proc::ValExpr::Subquery { .. } => {}
  }
}

/// This function collects and returns all GRQueries that belongs to a `SuperSimpleSelect`.
pub fn collect_select_subqueries(sql_query: &proc::SuperSimpleSelect) -> Vec<proc::GRQuery> {
  return collect_expr_subqueries(&sql_query.selection);
}

// Computes the set of all GRQuerys that appear as immediate children of `expr`.
fn collect_expr_subqueries(expr: &proc::ValExpr) -> Vec<proc::GRQuery> {
  fn collect_R(expr: &proc::ValExpr, subqueries: &mut Vec<proc::GRQuery>) {
    match expr {
      proc::ValExpr::ColumnRef { .. } => {}
      proc::ValExpr::UnaryExpr { expr, .. } => collect_R(expr, subqueries),
      proc::ValExpr::BinaryExpr { left, right, .. } => {
        collect_R(left, subqueries);
        collect_R(right, subqueries);
      }
      proc::ValExpr::Value { .. } => {}
      proc::ValExpr::Subquery { query, .. } => subqueries.push(query.deref().clone()),
    }
  }
  let mut subqueries = Vec::<proc::GRQuery>::new();
  collect_R(expr, &mut subqueries);
  subqueries
}

/// Recall that all TransTablesNames are unique. This function computes all external
/// TransTableNames in `col_usage_node` that's also not in `defined_trans_tables`
pub fn node_external_trans_tables(col_usage_node: &FrozenColUsageNode) -> Vec<TransTableName> {
  let mut accum = Vec::<TransTableName>::new();
  node_external_trans_tables_R(col_usage_node, &mut HashSet::new(), &mut accum);
  accum
}

/// Same as above, except we compute for all `nodes`.
pub fn nodes_external_trans_tables(
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
) -> Vec<TransTableName> {
  let mut accum = Vec::<TransTableName>::new();
  nodes_external_trans_tables_R(nodes, &mut HashSet::new(), &mut accum);
  accum
}

// Accumulates external TransTables in `node`.
fn node_external_trans_tables_R(
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
    nodes_external_trans_tables_R(nodes, defined_trans_tables, accum);
  }
}

// Accumulates external TransTables in `nodes`.
fn nodes_external_trans_tables_R(
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
  defined_trans_tables: &mut HashSet<TransTableName>,
  accum: &mut Vec<TransTableName>,
) {
  for (trans_table_name, (_, node)) in nodes {
    defined_trans_tables.insert(trans_table_name.clone());
    node_external_trans_tables_R(node, defined_trans_tables, accum);
  }
  for (trans_table_name, _) in nodes {
    defined_trans_tables.remove(trans_table_name);
  }
}

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

    let exp_col_usage_nodes: HashMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)> = vec![
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
    ]
    .into_iter()
    .collect();

    assert_eq!(col_usage_nodes, exp_col_usage_nodes);
  }
}
