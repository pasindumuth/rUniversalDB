use crate::common::TableSchema;
use crate::model::common::{
  iast, proc, ColName, ColType, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
  TransTableName,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::iter::FromIterator;

// -----------------------------------------------------------------------------------------------
//  FrozenColUsageAlgorithm
// -----------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct FrozenColUsageNode {
  table_ref: proc::TableRef,
  requested_cols: Vec<ColName>,
  children: Vec<BTreeMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)>>,
  safe_present_cols: Vec<ColName>,
  external_cols: Vec<ColName>,
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

struct ColUsagePlanner {
  gossiped_db_schema: BTreeMap<TablePath, TableSchema>,
  timestamp: Timestamp,
}

impl ColUsagePlanner {
  fn collect_subqueries(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<ColName>>,
    subqueries: &mut Vec<BTreeMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)>>,
    expr: &proc::ValExpr,
  ) {
    match expr {
      proc::ValExpr::ColumnRef { .. } => {}
      proc::ValExpr::UnaryExpr { expr, .. } => {
        self.collect_subqueries(trans_table_ctx, subqueries, &expr)
      }
      proc::ValExpr::BinaryExpr { left, right, .. } => {
        self.collect_subqueries(trans_table_ctx, subqueries, &left);
        self.collect_subqueries(trans_table_ctx, subqueries, &right);
      }
      proc::ValExpr::Value { .. } => {}
      proc::ValExpr::Subquery { query } => {
        subqueries.push(self.plan_gr_query(trans_table_ctx, query));
      }
    }
  }

  fn collect_top_level_cols(&mut self, cols: &mut Vec<ColName>, expr: &proc::ValExpr) {
    match expr {
      proc::ValExpr::ColumnRef { col_ref } => cols.push(col_ref.clone()),
      proc::ValExpr::UnaryExpr { expr, .. } => self.collect_top_level_cols(cols, &expr),
      proc::ValExpr::BinaryExpr { left, right, .. } => {
        self.collect_top_level_cols(cols, &left);
        self.collect_top_level_cols(cols, &right);
      }
      proc::ValExpr::Value { .. } => {}
      proc::ValExpr::Subquery { .. } => {}
    }
  }

  fn plan_stage_query(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<ColName>>,
    projection: &Vec<ColName>,
    table_ref: &proc::TableRef,
    exprs: &Vec<proc::ValExpr>,
  ) -> (Vec<ColName>, FrozenColUsageNode) {
    let mut node = FrozenColUsageNode::new(table_ref.clone());
    for expr in exprs {
      self.collect_top_level_cols(&mut node.requested_cols, expr);
      self.collect_subqueries(trans_table_ctx, &mut node.children, expr);
    }

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

  fn plan_gr_query(
    &mut self,
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<ColName>>,
    query: &proc::GRQuery,
  ) -> BTreeMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)> {
    let mut children = BTreeMap::<TransTableName, (Vec<ColName>, FrozenColUsageNode)>::new();
    for (trans_table_name, child_query) in &query.trans_tables {
      match child_query {
        proc::GRQueryStage::SuperSimpleSelect(select) => {
          let (cols, node) = self.plan_stage_query(
            trans_table_ctx,
            &select.projection,
            &select.from,
            &vec![select.selection.clone()],
          );
          children.insert(trans_table_name.clone(), (cols.clone(), node));
          trans_table_ctx.insert(trans_table_name.clone(), cols);
        }
      }
    }
    return children;
  }

  pub fn plan_ms_query(
    &mut self,
    query: &proc::MSQuery,
  ) -> BTreeMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)> {
    let mut trans_table_ctx = BTreeMap::<TransTableName, Vec<ColName>>::new();
    let mut children = BTreeMap::<TransTableName, (Vec<ColName>, FrozenColUsageNode)>::new();
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

#[cfg(test)]
mod test {
  use super::*;
  use crate::test_utils::{cn, mk_tab, mk_ttab};
  use std::collections::BTreeMap;

  #[test]
  fn basic_test() {
    let schema: BTreeMap<TablePath, TableSchema> = vec![
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

    let mut planner = ColUsagePlanner { gossiped_db_schema: schema, timestamp: Timestamp(0) };
    let col_usage_nodes = planner.plan_ms_query(&ms_query);

    let exp_col_usage_nodes: BTreeMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)> = vec![
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
