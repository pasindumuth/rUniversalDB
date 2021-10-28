use crate::col_usage::{
  collect_table_paths, compute_all_tier_maps, compute_query_plan_data, iterate_stage_ms_query,
  ColUsagePlanner, FrozenColUsageNode, GeneralStage,
};
use crate::common::{lookup, TableSchema};
use crate::master::{plm, MasterContext};
use crate::model::common::{proc, CQueryPath, ColName, QueryId, Timestamp, TransTableName};
use crate::model::message as msg;

// -----------------------------------------------------------------------------------------------
//  Master MasterQueryPlanningES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct MasterQueryPlanningES {
  pub sender_path: CQueryPath,
  pub query_id: QueryId,
  pub timestamp: Timestamp,
  pub ms_query: proc::MSQuery,
}

// -----------------------------------------------------------------------------------------------
//  MasterQueryPlanningES Pre-Insertion Implementation
// -----------------------------------------------------------------------------------------------

/// This is a helper that's used when checking the presence Required Columns in an `MSQuery`
enum PreReqColHelper {
  /// Returned if we can definitively say that at least one `ColName` is missing from `db_schema`.
  OneColMissing(ColName),
  /// Returned if we can definitively say that all `ColName`s are present `db_schema`.
  AllColsPresent,
  /// Returned if we cannot return either of the above.
  InsufficientLat,
}

/// Actions for the Master to execute
pub enum MasterQueryPlanningAction {
  Wait,
  Respond(msg::MasteryQueryPlanningResult),
}

/// Handle an incoming `PerformMasterQueryPlanning` message.
pub fn master_query_planning(
  ctx: &MasterContext,
  planning_msg: msg::PerformMasterQueryPlanning,
) -> MasterQueryPlanningAction {
  let table_paths = collect_table_paths(&planning_msg.ms_query);
  for table_path in &table_paths {
    if ctx.table_generation.get_lat(table_path) < planning_msg.timestamp {
      return MasterQueryPlanningAction::Wait;
    } else if ctx.table_generation.get_last_version(table_path).is_none() {
      // Otherwise, if the TablePath does not exist, we respond accordingly.
      return MasterQueryPlanningAction::Respond(msg::MasteryQueryPlanningResult::TablePathDNE(
        vec![table_path.clone()],
      ));
    }
  }

  // Next, we check that we are not trying to modify a Key Column in an Update.
  for (_, stage) in &planning_msg.ms_query.trans_tables {
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(_) => {}
      proc::MSQueryStage::Update(query) => {
        // The TablePath exists, from the above.
        let gen = ctx.table_generation.static_read(&query.table, planning_msg.timestamp).unwrap();
        let schema = ctx.db_schema.get(&(query.table.clone(), gen.clone())).unwrap();
        for (col_name, _) in &query.assignment {
          if lookup(&schema.key_cols, col_name).is_some() {
            // If so, we return an InvalidUpdate to the External.
            return MasterQueryPlanningAction::Respond(
              msg::MasteryQueryPlanningResult::InvalidUpdate,
            );
          }
        }
      }
    }
  }

  // Next, we see if all required columns in Select and Update queries are present.
  let mut helper = PreReqColHelper::AllColsPresent;
  iterate_stage_ms_query(
    &mut |stage: GeneralStage| {
      fn req_col_helper(
        schema: &TableSchema,
        col_name: &ColName,
        timestamp: Timestamp,
        helper: &mut PreReqColHelper,
      ) {
        if lookup(&schema.key_cols, col_name).is_none() {
          if schema.val_cols.get_lat(col_name) < timestamp {
            // Here, we realize we certain do not have AllColsPresent
            *helper = PreReqColHelper::InsufficientLat;
          } else {
            if schema.val_cols.static_read(col_name, timestamp).is_none() {
              // Here, we know for sure this `col_name` does not exist.
              *helper = PreReqColHelper::OneColMissing(col_name.clone());
            }
          }
        }
      }

      match helper {
        PreReqColHelper::OneColMissing(_) => {} // break out
        PreReqColHelper::AllColsPresent | PreReqColHelper::InsufficientLat => {
          let timestamp = planning_msg.timestamp;
          match stage {
            GeneralStage::SuperSimpleSelect(query) => {
              if let proc::TableRef::TablePath(table_path) = &query.from {
                // The TablePath exists, from the above.
                let gen = ctx.table_generation.static_read(&table_path, timestamp).unwrap();
                let schema = ctx.db_schema.get(&(table_path.clone(), gen.clone())).unwrap();
                for col_name in &query.projection {
                  req_col_helper(schema, col_name, timestamp, &mut helper);
                }
              }
            }
            GeneralStage::Update(query) => {
              // The TablePath exists, from the above.
              let gen = ctx.table_generation.static_read(&query.table, timestamp).unwrap();
              let schema = ctx.db_schema.get(&(query.table.clone(), gen.clone())).unwrap();
              for (col_name, _) in &query.assignment {
                req_col_helper(schema, col_name, timestamp, &mut helper);
              }
            }
          }
        }
      }
    },
    &planning_msg.ms_query,
  );

  match helper {
    PreReqColHelper::OneColMissing(col_name) => {
      // Here, we return an RequiredColumnDNE to the External.
      return MasterQueryPlanningAction::Respond(
        msg::MasteryQueryPlanningResult::RequiredColumnDNE(vec![col_name]),
      );
    }
    PreReqColHelper::AllColsPresent => {}
    PreReqColHelper::InsufficientLat => {
      // If the LAT is not high enough, we need to create an ES to persist a read.
      return MasterQueryPlanningAction::Wait;
    }
  }

  // Next, we run the FrozenColUsageAlgorithm.
  let mut planner = ColUsagePlanner {
    db_schema: &ctx.db_schema,
    table_generation: &ctx.table_generation,
    timestamp: planning_msg.timestamp,
  };
  let col_usage_nodes = planner.plan_ms_query(&planning_msg.ms_query);

  // Check that the LATs are high enough.
  if !check_nodes_lats(ctx, &col_usage_nodes, planning_msg.timestamp) {
    // If the LAT is not high enough, we need to create an ES to persist a read.
    return MasterQueryPlanningAction::Wait;
  }

  // Finally we construct a MasterQueryPlan and respond to the sender.
  let all_tier_maps = compute_all_tier_maps(&planning_msg.ms_query);
  let (table_location_map, extra_req_cols) =
    compute_query_plan_data(&planning_msg.ms_query, &ctx.table_generation, planning_msg.timestamp);
  return MasterQueryPlanningAction::Respond(msg::MasteryQueryPlanningResult::MasterQueryPlan(
    msg::MasterQueryPlan { all_tier_maps, table_location_map, extra_req_cols, col_usage_nodes },
  ));
}

/// Checks if the LATs of all `ColName`s in `safe_present_cols` and `external_cols` are higher
/// that `timestamp` for all `FrozenColUsageNode`s under `node`, where that node refers to a
/// Table (as opposed to a TransTable).
fn check_node_lats(ctx: &MasterContext, node: &FrozenColUsageNode, timestamp: Timestamp) -> bool {
  match &node.table_ref {
    proc::TableRef::TablePath(table_path) => {
      let gen = ctx.table_generation.static_read(table_path, timestamp).unwrap();
      let schema = ctx.db_schema.get(&(table_path.clone(), gen.clone())).unwrap();
      // Check `safe_present_cols` and `external_cols`.
      for col_name in node.safe_present_cols.iter().chain(node.external_cols.iter()) {
        if lookup(&schema.key_cols, col_name).is_none() {
          if schema.val_cols.get_lat(col_name) < timestamp {
            return false;
          }
        }
      }
      // Check children
      for child in &node.children {
        for (_, (_, child_node)) in child {
          if !check_node_lats(ctx, child_node, timestamp) {
            return false;
          }
        }
      }
    }
    proc::TableRef::TransTableName(_) => {}
  }
  true
}

/// Same as above, except we do it for every `FrozenColUsageNode` in `nodes`.
fn check_nodes_lats(
  ctx: &MasterContext,
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
  timestamp: Timestamp,
) -> bool {
  for (_, (_, node)) in nodes {
    if !check_node_lats(ctx, node, timestamp) {
      return false;
    }
  }
  true
}

// -----------------------------------------------------------------------------------------------
//  MasterQueryPlanningES Post-Insertion Implementation
// -----------------------------------------------------------------------------------------------

/// This is a helper that's used when checking the presence Required Columns in an `MSQuery`
enum PostReqColHelper {
  /// Returned if we can definitively say that at least one `ColName` is missing from `db_schema`.
  OneColMissing(ColName),
  /// Returned if we can definitively say that all `ColName`s are present `db_schema`.
  AllColsPresent,
}

/// Handle an incoming `PerformMasterQueryPlanning` message.
pub fn master_query_planning_post(
  ctx: &mut MasterContext,
  planning_plm: plm::MasterQueryPlanning,
) -> msg::MasteryQueryPlanningResult {
  let table_paths = collect_table_paths(&planning_plm.ms_query);
  for table_path in &table_paths {
    if ctx.table_generation.read(table_path, planning_plm.timestamp).is_none() {
      // If the TablePath does not exist, we respond accordingly.
      return msg::MasteryQueryPlanningResult::TablePathDNE(vec![table_path.clone()]);
    }
  }

  // Next, we check that we are not trying to modify a Key Column in an Update.
  for (_, stage) in &planning_plm.ms_query.trans_tables {
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(_) => {}
      proc::MSQueryStage::Update(query) => {
        // The TablePath exists, from the above.
        let gen = ctx.table_generation.static_read(&query.table, planning_plm.timestamp).unwrap();
        let schema = ctx.db_schema.get(&(query.table.clone(), gen.clone())).unwrap();
        for (col_name, _) in &query.assignment {
          if lookup(&schema.key_cols, col_name).is_some() {
            // If so, we return an InvalidUpdate to the External.
            return msg::MasteryQueryPlanningResult::InvalidUpdate;
          }
        }
      }
    }
  }

  // Next, we see if all required columns in Select and Update queries are present.
  let mut helper = PostReqColHelper::AllColsPresent;
  iterate_stage_ms_query(
    &mut |stage: GeneralStage| {
      fn req_col_helper(
        schema: &mut TableSchema,
        col_name: &ColName,
        timestamp: Timestamp,
        helper: &mut PostReqColHelper,
      ) {
        if lookup(&schema.key_cols, col_name).is_none() {
          if schema.val_cols.read(col_name, timestamp).is_none() {
            // Here, we know for sure this `col_name` does not exist.
            *helper = PostReqColHelper::OneColMissing(col_name.clone());
          }
        }
      }

      match helper {
        PostReqColHelper::OneColMissing(_) => {} // break out
        PostReqColHelper::AllColsPresent => {
          let timestamp = planning_plm.timestamp;
          match stage {
            GeneralStage::SuperSimpleSelect(query) => {
              if let proc::TableRef::TablePath(table_path) = &query.from {
                // The TablePath exists, from the above.
                let gen = ctx.table_generation.static_read(&table_path, timestamp).unwrap();
                let schema = ctx.db_schema.get_mut(&(table_path.clone(), gen.clone())).unwrap();
                for col_name in &query.projection {
                  req_col_helper(schema, col_name, timestamp, &mut helper);
                }
              }
            }
            GeneralStage::Update(query) => {
              // The TablePath exists, from the above.
              let gen = ctx.table_generation.static_read(&query.table, timestamp).unwrap();
              let schema = ctx.db_schema.get_mut(&(query.table.clone(), gen.clone())).unwrap();
              for (col_name, _) in &query.assignment {
                req_col_helper(schema, col_name, timestamp, &mut helper);
              }
            }
          }
        }
      }
    },
    &planning_plm.ms_query,
  );

  match helper {
    PostReqColHelper::OneColMissing(col_name) => {
      // Here, we return an RequiredColumnDNE to the External.
      return msg::MasteryQueryPlanningResult::RequiredColumnDNE(vec![col_name]);
    }
    PostReqColHelper::AllColsPresent => {}
  }

  // Next, we run the FrozenColUsageAlgorithm.
  let mut planner = ColUsagePlanner {
    db_schema: &ctx.db_schema,
    table_generation: &ctx.table_generation,
    timestamp: planning_plm.timestamp,
  };
  let col_usage_nodes = planner.plan_ms_query(&planning_plm.ms_query);

  // Check that the LATs are high enough.
  increase_nodes_lats(ctx, &col_usage_nodes, planning_plm.timestamp);

  // Finally we construct a MasterQueryPlan and respond to the sender.
  let all_tier_maps = compute_all_tier_maps(&planning_plm.ms_query);
  let (table_location_map, extra_req_cols) =
    compute_query_plan_data(&planning_plm.ms_query, &ctx.table_generation, planning_plm.timestamp);
  return msg::MasteryQueryPlanningResult::MasterQueryPlan(msg::MasterQueryPlan {
    all_tier_maps,
    table_location_map,
    extra_req_cols,
    col_usage_nodes,
  });
}

/// Checks if the LATs of all `ColName`s in `safe_present_cols` and `external_cols` are higher
/// that `timestamp` for all `FrozenColUsageNode`s under `node`, where that node refers to a
/// Table (as opposed to a TransTable).
fn increase_node_lats(ctx: &mut MasterContext, node: &FrozenColUsageNode, timestamp: Timestamp) {
  match &node.table_ref {
    proc::TableRef::TablePath(table_path) => {
      let gen = ctx.table_generation.static_read(table_path, timestamp).unwrap();
      let schema = ctx.db_schema.get_mut(&(table_path.clone(), gen.clone())).unwrap();
      // Check `safe_present_cols` and `external_cols`.
      for col_name in node.safe_present_cols.iter().chain(node.external_cols.iter()) {
        if lookup(&schema.key_cols, col_name).is_none() {
          schema.val_cols.update_lat(col_name, timestamp);
        }
      }
      // Check children
      for child in &node.children {
        for (_, (_, child_node)) in child {
          increase_node_lats(ctx, child_node, timestamp);
        }
      }
    }
    proc::TableRef::TransTableName(_) => {}
  }
}

/// Same as above, except we do it for every `FrozenColUsageNode` in `nodes`.
fn increase_nodes_lats(
  ctx: &mut MasterContext,
  nodes: &Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
  timestamp: Timestamp,
) {
  for (_, (_, node)) in nodes {
    increase_node_lats(ctx, node, timestamp);
  }
}
