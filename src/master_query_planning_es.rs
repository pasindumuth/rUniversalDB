use crate::col_usage::{
  free_external_cols, iterate_stage_ms_query, ColUsageError, ColUsagePlanner, FrozenColUsageNode,
  GeneralStage,
};
use crate::common::{lookup, TableSchema};
use crate::master::{plm, MasterContext};
use crate::model::common::proc::MSQueryStage;
use crate::model::common::{proc, CQueryPath, ColName, QueryId, Timestamp, TransTableName};
use crate::model::message as msg;
use crate::model::message::ExternalAbortedData::QueryPlanningError;
use crate::query_planning::{
  collect_table_paths, compute_all_tier_maps, compute_extra_req_cols, compute_query_plan_data,
  perform_static_validations, KeyValidationError,
};

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

fn respond_error(error: msg::QueryPlanningError) -> MasterQueryPlanningAction {
  MasterQueryPlanningAction::Respond(msg::MasteryQueryPlanningResult::QueryPlanningError(error))
}

/// Handle an incoming `PerformMasterQueryPlanning` message.
pub fn master_query_planning(
  ctx: &MasterContext,
  planning_msg: msg::PerformMasterQueryPlanning,
) -> MasterQueryPlanningAction {
  for table_path in collect_table_paths(&planning_msg.ms_query) {
    if ctx.table_generation.get_lat(&table_path) < planning_msg.timestamp {
      return MasterQueryPlanningAction::Wait;
    } else if ctx.table_generation.get_last_version(&table_path).is_none() {
      // Otherwise, if the TablePath does not exist, we respond accordingly.
      return respond_error(msg::QueryPlanningError::TablesDNE(vec![table_path]));
    }
  }

  // Next, we do various validations on the MSQuery.
  match perform_static_validations(
    &planning_msg.ms_query,
    &ctx.table_generation,
    &ctx.db_schema,
    planning_msg.timestamp,
  ) {
    Ok(_) => {}
    Err(KeyValidationError::InvalidUpdate) => {
      return respond_error(msg::QueryPlanningError::InvalidUpdate)
    }
    Err(KeyValidationError::InvalidInsert) => {
      return respond_error(msg::QueryPlanningError::InvalidInsert)
    }
  }

  // Next, we see if all required columns in all queries are present.
  let mut helper = PreReqColHelper::AllColsPresent;
  for (table_path, col_names) in compute_extra_req_cols(&planning_msg.ms_query) {
    for col_name in col_names {
      // The TablePath exists, from the above.
      let gen = ctx.table_generation.static_read(&table_path, planning_msg.timestamp).unwrap();
      let schema = ctx.db_schema.get(&(table_path.clone(), gen.clone())).unwrap();
      if lookup(&schema.key_cols, &col_name).is_none() {
        if schema.val_cols.get_lat(&col_name) < planning_msg.timestamp {
          // Here, we realize we certain do not have AllColsPresent. We continue on, in case
          // we (more strongly) realize that we can send back a `RequiredColumnDNE`.
          helper = PreReqColHelper::InsufficientLat;
        } else {
          if schema.val_cols.static_read(&col_name, planning_msg.timestamp).is_none() {
            // Here, we know for sure this `col_name` does not exist. We break out and
            // respond immediately.
            return respond_error(msg::QueryPlanningError::RequiredColumnDNE(vec![col_name]));
          }
        }
      }
    }
  }

  match helper {
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

  // TODO: I believe this is broken. Doing weak static reads and resulting in an error does
  //  not mean that we should respond as such. Some errors can be responded to definitively,
  //  like something with keycols (e.g. insert now having key col). Fix this. Like for runtime
  //  errors, if we let go of the desire to have idempotence or strong consistency for failure
  //  scenarios, then the below code is okay and we can extract lots of commonality between
  //  this function and the lower function.
  let col_usage_nodes = match planner.plan_ms_query(&planning_msg.ms_query) {
    Ok(col_usage_nodes) => col_usage_nodes,
    Err(error) => {
      return respond_error(match error {
        ColUsageError::InvalidColumnRef => msg::QueryPlanningError::InvalidColUsage,
        ColUsageError::InvalidSelectClause => msg::QueryPlanningError::InvalidSelect,
      });
    }
  };

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
  match &node.source.source_ref {
    proc::GeneralSourceRef::TablePath(table_path) => {
      let gen = ctx.table_generation.static_read(table_path, timestamp).unwrap();
      let schema = ctx.db_schema.get(&(table_path.clone(), gen.clone())).unwrap();
      // Check `safe_present_cols` and `external_cols`.
      let free_external_cols = free_external_cols(&node.external_cols);
      for col_name in node.safe_present_cols.iter().chain(free_external_cols.iter()) {
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
    proc::GeneralSourceRef::TransTableName(_) => {}
  }
  true
}

/// Same as above, except we do it for every `FrozenColUsageNode` in `nodes`.
fn check_nodes_lats(
  ctx: &MasterContext,
  nodes: &Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>,
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

/// Handle the insertion of a `MasterQueryPlanning` message.
pub fn master_query_planning_post(
  ctx: &mut MasterContext,
  planning_plm: plm::MasterQueryPlanning,
) -> msg::MasteryQueryPlanningResult {
  let table_paths = collect_table_paths(&planning_plm.ms_query);
  for table_path in &table_paths {
    if ctx.table_generation.read(table_path, planning_plm.timestamp).is_none() {
      // If the TablePath does not exist, we respond accordingly.
      return msg::MasteryQueryPlanningResult::QueryPlanningError(
        msg::QueryPlanningError::TablesDNE(vec![table_path.clone()]),
      );
    }
  }

  // Next, we do various validations on the MSQuery.
  match perform_static_validations(
    &planning_plm.ms_query,
    &ctx.table_generation,
    &ctx.db_schema,
    planning_plm.timestamp,
  ) {
    Ok(_) => {}
    Err(KeyValidationError::InvalidUpdate) => {
      return msg::MasteryQueryPlanningResult::QueryPlanningError(
        msg::QueryPlanningError::InvalidUpdate,
      );
    }
    Err(KeyValidationError::InvalidInsert) => {
      return msg::MasteryQueryPlanningResult::QueryPlanningError(
        msg::QueryPlanningError::InvalidInsert,
      );
    }
  }

  // Next, we see if all extra required columns in all queries are present, making sure to
  // increase the `lat` either way.
  for (table_path, col_names) in compute_extra_req_cols(&planning_plm.ms_query) {
    for col_name in col_names {
      // The TablePath exists, from the above.
      let gen = ctx.table_generation.static_read(&table_path, planning_plm.timestamp).unwrap();
      let schema = ctx.db_schema.get_mut(&(table_path.clone(), gen.clone())).unwrap();
      if lookup(&schema.key_cols, &col_name).is_none() {
        if schema.val_cols.read(&col_name, planning_plm.timestamp).is_none() {
          // Here, we know for sure this `col_name` does not exist, and will never since we
          // just increased the LAT. Thus, we respond with it to the sender.
          return msg::MasteryQueryPlanningResult::QueryPlanningError(
            msg::QueryPlanningError::RequiredColumnDNE(vec![col_name]),
          );
        }
      }
    }
  }

  // Next, we run the FrozenColUsageAlgorithm.
  let mut planner = ColUsagePlanner {
    db_schema: &ctx.db_schema,
    table_generation: &ctx.table_generation,
    timestamp: planning_plm.timestamp,
  };

  let col_usage_nodes = match planner.plan_ms_query(&planning_plm.ms_query) {
    Ok(col_usage_nodes) => col_usage_nodes,
    Err(error) => {
      return msg::MasteryQueryPlanningResult::QueryPlanningError(match error {
        ColUsageError::InvalidColumnRef => msg::QueryPlanningError::InvalidColUsage,
        ColUsageError::InvalidSelectClause => msg::QueryPlanningError::InvalidSelect,
      });
    }
  };

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
  match &node.source.source_ref {
    proc::GeneralSourceRef::TablePath(table_path) => {
      let gen = ctx.table_generation.static_read(table_path, timestamp).unwrap();
      let schema = ctx.db_schema.get_mut(&(table_path.clone(), gen.clone())).unwrap();
      // Check `safe_present_cols` and `external_cols`.
      let free_external_cols = free_external_cols(&node.external_cols);
      for col_name in node.safe_present_cols.iter().chain(free_external_cols.iter()) {
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
    proc::GeneralSourceRef::TransTableName(_) => {}
  }
}

/// Same as above, except we do it for every `FrozenColUsageNode` in `nodes`.
fn increase_nodes_lats(
  ctx: &mut MasterContext,
  nodes: &Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>,
  timestamp: Timestamp,
) {
  for (_, (_, node)) in nodes {
    increase_node_lats(ctx, node, timestamp);
  }
}
