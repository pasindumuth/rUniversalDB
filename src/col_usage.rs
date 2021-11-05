use crate::common::TableSchema;
use crate::model::common::{proc, ColName, Gen, TablePath, TierMap, Timestamp, TransTableName};
use crate::multiversion_map::MVM;
use crate::server::weak_contains_col;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

// -----------------------------------------------------------------------------------------------
//  ColUsagePlanner
// -----------------------------------------------------------------------------------------------

/// There are several uses of this.
/// 1. The purpose of the `FrozenColUsageNode` is for a parent ES to be able to compute
///    the `Context` that it should send a child ES. That is why `requested_cols` only contains
///    `ColumnRef`s that can reference an ancestral `TableRef` if its query's table's schema
///    does not have it. (Hence why we do not include projected columns in SELECT queries,
///    or assigned columns in UPDATE queries.)
/// 2. When the MSCoordES creates this using it's GossipData, things like `safe_present_cols`
///    and `external_cols` should be considered expectations on what the schemas should be.
///    The schemas should be checked and the Transaction aborted if the expections fail.
/// 3. Instead, if this is sent back with a MasterQueryPlanning, it will be hard facts that
///    `safe_resent_cols` and `external_cols` *will* align with the table schemas.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FrozenColUsageNode {
  /// The (Trans)Table used in the `GeneralQuery` corresponding to this node.
  pub table_ref: proc::TableRef,

  /// These are the ColNames used within all ValExprs outside of `Subquery` nodes.
  /// This is a convenience field used only in the ColUsagePlanner.
  pub requested_cols: Vec<ColName>,

  /// Take the union of `requested_cols` and all `external_cols` of all `FrozenColUsageNodes`
  /// in `children`. Call this all_cols.
  ///
  /// Below, `safe_present_cols` is the subset of all_cols that are present in the (Trans)Table
  /// (according to the gossiped_db_schema). `external_cols` are the complement of that.
  pub children: Vec<Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>>,
  pub safe_present_cols: Vec<ColName>,
  pub external_cols: Vec<ColName>,
}

impl FrozenColUsageNode {
  pub fn new(table_ref: proc::TableRef) -> FrozenColUsageNode {
    FrozenColUsageNode {
      table_ref,
      requested_cols: vec![],
      children: vec![],
      safe_present_cols: vec![],
      external_cols: vec![],
    }
  }
}

/// This algorithm contains the following assumptions:
///   1. All `TablePath`s referenced the `MSQuery` exist in `table_generation` and `db_schema`
///      at the given `Timestamp`.
///   2. All projected columns in `SELECT` queries and all assigned columns in `UPDATE` queries
///      exist in the `db_schema`.
///   3. The assigned columns in an `UPDATE` are disjoint from the Key Columns. (This algorithm
///      does not support such queries).
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
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<ColName>>,
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
    let mut all_cols = BTreeSet::<ColName>::new();
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
        let table_schema = trans_table_ctx.get(trans_table_name).unwrap();
        for col in all_cols {
          if table_schema.contains(&col) {
            node.safe_present_cols.push(col);
          } else {
            node.external_cols.push(col);
          }
        }
      }
      proc::TableRef::TablePath(table_path) => {
        // The Query converter should make sure that all TablePaths actually exist.
        let gen = self.table_generation.static_read(table_path, self.timestamp).unwrap();
        let table_schema = self.db_schema.get(&(table_path.clone(), gen.clone())).unwrap();
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
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<ColName>>,
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
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<ColName>>,
    update: &proc::Update,
  ) -> (Vec<ColName>, FrozenColUsageNode) {
    let mut projection = Vec::new();
    let gen = self.table_generation.static_read(&update.table, self.timestamp).unwrap();
    for (col, _) in &self.db_schema.get(&(update.table.clone(), gen.clone())).unwrap().key_cols {
      projection.push(col.clone());
    }
    for (col, _) in &update.assignment {
      projection.push(col.clone());
    }

    let mut exprs = Vec::new();
    for (_, expr) in &update.assignment {
      exprs.push(expr.clone());
    }
    exprs.push(update.selection.clone());

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
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<ColName>>,
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
    trans_table_ctx: &mut BTreeMap<TransTableName, Vec<ColName>>,
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
    let mut trans_table_ctx = BTreeMap::<TransTableName, Vec<ColName>>::new();
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
  node_external_trans_tables_r(col_usage_node, &mut BTreeSet::new(), &mut accum);
  accum
}

/// Same as above, except for multiple `nodes`. This should be determinisitic.
pub fn nodes_external_trans_tables(
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
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
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
) -> Vec<ColName> {
  let mut col_name_set = BTreeSet::<ColName>::new();
  for (_, (_, node)) in nodes {
    col_name_set.extend(node.external_cols.clone())
  }
  col_name_set.into_iter().collect()
}

// -----------------------------------------------------------------------------------------------
//  Stage Iteration
// -----------------------------------------------------------------------------------------------
pub enum GeneralStage<'a> {
  SuperSimpleSelect(&'a proc::SuperSimpleSelect),
  Update(&'a proc::Update),
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
      }
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  QueryPlanning helpers
// -----------------------------------------------------------------------------------------------

/// Gather every reference to a `TablePath` found in the `query`.
pub fn collect_table_paths(query: &proc::MSQuery) -> BTreeSet<TablePath> {
  let mut table_paths = BTreeSet::<TablePath>::new();
  iterate_stage_ms_query(
    &mut |stage: GeneralStage| match stage {
      GeneralStage::SuperSimpleSelect(query) => {
        if let proc::TableRef::TablePath(table_path) = &query.from {
          table_paths.insert(table_path.clone());
        }
      }
      GeneralStage::Update(query) => {
        table_paths.insert(query.table.clone());
      }
    },
    query,
  );
  table_paths
}

/// Compute the all TierMaps for the `MSQueryES`.
///
/// The Tier should be where every Read query should be reading from, except
/// if the current stage is an Update, which should be one Tier ahead (i.e.
/// lower) for that TablePath.
pub fn compute_all_tier_maps(ms_query: &proc::MSQuery) -> BTreeMap<TransTableName, TierMap> {
  let mut all_tier_maps = BTreeMap::<TransTableName, TierMap>::new();
  let mut cur_tier_map = BTreeMap::<TablePath, u32>::new();
  for (_, stage) in &ms_query.trans_tables {
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(_) => {}
      proc::MSQueryStage::Update(update) => {
        cur_tier_map.insert(update.table.clone(), 0);
      }
    }
  }
  for (trans_table_name, stage) in ms_query.trans_tables.iter().rev() {
    all_tier_maps.insert(trans_table_name.clone(), TierMap { map: cur_tier_map.clone() });
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(_) => {}
      proc::MSQueryStage::Update(update) => {
        *cur_tier_map.get_mut(&update.table).unwrap() += 1;
      }
    }
  }
  all_tier_maps
}

/// Compute miscellaneoud data related to QueryPlanning. This can be shared between
/// the Master and MSCoordESs.
pub fn compute_query_plan_data(
  ms_query: &proc::MSQuery,
  table_generation: &MVM<TablePath, Gen>,
  timestamp: Timestamp,
) -> (BTreeMap<TablePath, Gen>, BTreeMap<TablePath, Vec<ColName>>) {
  let mut table_location_map = BTreeMap::<TablePath, Gen>::new();
  let mut extra_req_cols = BTreeMap::<TablePath, Vec<ColName>>::new();
  iterate_stage_ms_query(
    &mut |stage: GeneralStage| match stage {
      GeneralStage::SuperSimpleSelect(query) => {
        if let proc::TableRef::TablePath(table_path) = &query.from {
          let gen = table_generation.static_read(&table_path, timestamp).unwrap();
          table_location_map.insert(table_path.clone(), gen.clone());

          // Recall there might already be required columns for this TablePath.
          if !extra_req_cols.contains_key(&table_path) {
            extra_req_cols.insert(table_path.clone(), Vec::new());
          }
          let req_cols = extra_req_cols.get_mut(&table_path).unwrap();
          for col_name in &query.projection {
            if !req_cols.contains(col_name) {
              req_cols.push(col_name.clone());
            }
          }
        }
      }
      GeneralStage::Update(query) => {
        let gen = table_generation.static_read(&query.table, timestamp).unwrap();
        table_location_map.insert(query.table.clone(), gen.clone());

        // Recall there might already be required columns for this TablePath.
        if !extra_req_cols.contains_key(&query.table) {
          extra_req_cols.insert(query.table.clone(), Vec::new());
        }
        let req_cols = extra_req_cols.get_mut(&query.table).unwrap();
        for (col_name, _) in &query.assignment {
          if !req_cols.contains(col_name) {
            req_cols.push(col_name.clone());
          }
        }
      }
    },
    ms_query,
  );

  (table_location_map, extra_req_cols)
}

// -----------------------------------------------------------------------------------------------
//  Tests
// -----------------------------------------------------------------------------------------------

#[cfg(test)]
mod test {
  use super::*;
  use crate::model::common::ColType;
  use crate::test_utils::{cn, mk_tab, mk_ttab};
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

    let mut planner =
      ColUsagePlanner { db_schema: &db_schema, table_generation: &table_generation, timestamp: 1 };
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
