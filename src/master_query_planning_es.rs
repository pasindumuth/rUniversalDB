use crate::col_usage::{
  free_external_cols, iterate_stage_ms_query, ColUsageError, ColUsageNode, ColUsagePlanner,
  GeneralStage,
};
use crate::common::{lookup, TableSchema, Timestamp};
use crate::master::{plm, MasterContext};
use crate::model::common::proc::MSQueryStage;
use crate::model::common::{
  proc, CQueryPath, ColName, ColType, Gen, QueryId, TablePath, TransTableName,
};
use crate::model::message as msg;
use crate::model::message::ExternalAbortedData::QueryPlanningError;
use crate::model::message::MasterQueryPlan;
use crate::multiversion_map::MVM;
use crate::query_planning::{
  check_cols_present, collect_table_paths, compute_all_tier_maps, compute_extra_req_cols,
  compute_table_location_map, perform_static_validations, KeyValidationError,
};
use sqlparser::test_utils::table;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Error Traits
// -----------------------------------------------------------------------------------------------

pub trait ColUsageErrorTrait {
  fn mk_error(err: ColUsageError) -> Self;
}

pub trait KeyValidationErrorTrait {
  fn mk_error(err: KeyValidationError) -> Self;
}

pub trait ReqTablePresenceError {
  fn mk_error(missing_col: TablePath) -> Self;
}

pub trait ReqColPresenceError {
  fn mk_error(missing_col: ColName) -> Self;
}

// -----------------------------------------------------------------------------------------------
//  DBSchemaView
// -----------------------------------------------------------------------------------------------

/// This exposes what is virtually an unversioned Database Schema.
pub trait DBSchemaView {
  type ErrorT;

  fn key_cols(&mut self, table_path: &TablePath) -> Result<&Vec<(ColName, ColType)>, Self::ErrorT>;

  fn get_gen(&mut self, table_path: &TablePath) -> Result<Gen, Self::ErrorT>;

  fn contains_table(&mut self, table_path: &TablePath) -> Result<bool, Self::ErrorT>;

  fn contains_col(
    &mut self,
    table_path: &TablePath,
    col_name: &ColName,
  ) -> Result<bool, Self::ErrorT>;
}

// -----------------------------------------------------------------------------------------------
//  CheckingDBSchemaView
// -----------------------------------------------------------------------------------------------

pub enum CheckingDBSchemaViewError {
  InsufficientLat,
  TableDNE(TablePath),
  ColUsageError(ColUsageError),
  KeyValidationError(KeyValidationError),
  ReqColPresenceError(ColName),
}

/// In this implementation of `DBSchemaView`, all functions are idempotent if they succeed.
/// Importantly, if a critical LAT is not high enough, the function calls fails with
/// `InsufficientLat`.
///
/// Importantly, the data members are immutable.
///
/// To elaborate on the idempotence property, suppose we call a method in `DBSchemaView` with
/// some args. Then, suppose we construct a new `CheckingDBSchemaView` with the same `timestamp`
/// and subsequent `db_schema` and `table_generation`. Then if the method is called with the
/// same arguments, the output will be the same.
struct CheckingDBSchemaView<'a> {
  pub db_schema: &'a BTreeMap<(TablePath, Gen), TableSchema>,
  pub table_generation: &'a MVM<TablePath, Gen>,
  pub timestamp: Timestamp,
}

impl<'a> CheckingDBSchemaView<'a> {
  fn get_table_schema(
    &mut self,
    table_path: &TablePath,
  ) -> Result<&TableSchema, CheckingDBSchemaViewError> {
    let gen = self.get_gen(table_path)?;
    Ok(self.db_schema.get(&(table_path.clone(), gen)).unwrap())
  }
}

impl<'a> DBSchemaView for CheckingDBSchemaView<'a> {
  type ErrorT = CheckingDBSchemaViewError;

  fn key_cols(&mut self, table_path: &TablePath) -> Result<&Vec<(ColName, ColType)>, Self::ErrorT> {
    Ok(&self.get_table_schema(table_path)?.key_cols)
  }

  fn get_gen(&mut self, table_path: &TablePath) -> Result<Gen, CheckingDBSchemaViewError> {
    if self.table_generation.get_lat(table_path) < self.timestamp {
      Err(CheckingDBSchemaViewError::InsufficientLat)
    } else {
      if let Some(gen) = self.table_generation.strong_static_read(table_path, &self.timestamp) {
        Ok(gen.clone())
      } else {
        Err(CheckingDBSchemaViewError::TableDNE(table_path.clone()))
      }
    }
  }

  fn contains_table(&mut self, table_path: &TablePath) -> Result<bool, Self::ErrorT> {
    if self.table_generation.get_lat(table_path) < self.timestamp {
      Err(CheckingDBSchemaViewError::InsufficientLat)
    } else {
      Ok(self.table_generation.strong_static_read(table_path, &self.timestamp).is_some())
    }
  }

  fn contains_col(
    &mut self,
    table_path: &TablePath,
    col_name: &ColName,
  ) -> Result<bool, CheckingDBSchemaViewError> {
    let timestamp = self.timestamp.clone();
    let schema = self.get_table_schema(table_path)?;
    if lookup(&schema.key_cols, col_name).is_none() {
      if schema.val_cols.get_lat(col_name) < timestamp {
        Err(CheckingDBSchemaViewError::InsufficientLat)
      } else {
        Ok(schema.val_cols.strong_static_read(col_name, &timestamp).is_some())
      }
    } else {
      Ok(true)
    }
  }
}

impl ColUsageErrorTrait for CheckingDBSchemaViewError {
  fn mk_error(err: ColUsageError) -> CheckingDBSchemaViewError {
    CheckingDBSchemaViewError::ColUsageError(err)
  }
}

impl KeyValidationErrorTrait for CheckingDBSchemaViewError {
  fn mk_error(err: KeyValidationError) -> Self {
    CheckingDBSchemaViewError::KeyValidationError(err)
  }
}

impl ReqTablePresenceError for CheckingDBSchemaViewError {
  fn mk_error(missing_table: TablePath) -> Self {
    CheckingDBSchemaViewError::TableDNE(missing_table)
  }
}

impl ReqColPresenceError for CheckingDBSchemaViewError {
  fn mk_error(missing_col: ColName) -> Self {
    CheckingDBSchemaViewError::ReqColPresenceError(missing_col)
  }
}

// -----------------------------------------------------------------------------------------------
//  LockingDBSchemaView
// -----------------------------------------------------------------------------------------------

pub enum LockingDBSchemaViewError {
  TableDNE(TablePath),
  ColUsageError(ColUsageError),
  KeyValidationError(KeyValidationError),
  ReqColPresenceError(ColName),
}

/// In this implementation of `DBSchemaView`, all functions are idempotent if they succeed.
/// Importantly, if a critical LAT is not high enough, it will be increased accordingly.
///
/// Importantly, the data members are mutable.
///
/// See `CheckingDBSchemaView` for a description of idempotence.
struct LockingDBSchemaView<'a> {
  pub db_schema: &'a mut BTreeMap<(TablePath, Gen), TableSchema>,
  pub table_generation: &'a mut MVM<TablePath, Gen>,
  pub timestamp: Timestamp,
}

impl<'a> LockingDBSchemaView<'a> {
  fn get_table_schema(
    &mut self,
    table_path: &TablePath,
  ) -> Result<&mut TableSchema, LockingDBSchemaViewError> {
    let gen = self.get_gen(table_path)?;
    Ok(self.db_schema.get_mut(&(table_path.clone(), gen)).unwrap())
  }
}

impl<'a> DBSchemaView for LockingDBSchemaView<'a> {
  type ErrorT = LockingDBSchemaViewError;

  fn key_cols(&mut self, table_path: &TablePath) -> Result<&Vec<(ColName, ColType)>, Self::ErrorT> {
    Ok(&self.get_table_schema(table_path)?.key_cols)
  }

  fn get_gen(&mut self, table_path: &TablePath) -> Result<Gen, LockingDBSchemaViewError> {
    if let Some(gen) = self.table_generation.read(table_path, &self.timestamp) {
      Ok(gen)
    } else {
      Err(LockingDBSchemaViewError::TableDNE(table_path.clone()))
    }
  }

  fn contains_table(&mut self, table_path: &TablePath) -> Result<bool, Self::ErrorT> {
    Ok(self.table_generation.read(table_path, &self.timestamp).is_some())
  }

  fn contains_col(
    &mut self,
    table_path: &TablePath,
    col_name: &ColName,
  ) -> Result<bool, LockingDBSchemaViewError> {
    let timestamp = self.timestamp.clone();
    let schema = self.get_table_schema(table_path)?;
    if lookup(&schema.key_cols, col_name).is_none() {
      Ok(schema.val_cols.read(col_name, &timestamp).is_some())
    } else {
      Ok(true)
    }
  }
}

impl ColUsageErrorTrait for LockingDBSchemaViewError {
  fn mk_error(err: ColUsageError) -> LockingDBSchemaViewError {
    LockingDBSchemaViewError::ColUsageError(err)
  }
}

impl KeyValidationErrorTrait for LockingDBSchemaViewError {
  fn mk_error(err: KeyValidationError) -> Self {
    LockingDBSchemaViewError::KeyValidationError(err)
  }
}

impl ReqTablePresenceError for LockingDBSchemaViewError {
  fn mk_error(missing_table: TablePath) -> Self {
    LockingDBSchemaViewError::TableDNE(missing_table)
  }
}

impl ReqColPresenceError for LockingDBSchemaViewError {
  fn mk_error(missing_col: ColName) -> Self {
    LockingDBSchemaViewError::ReqColPresenceError(missing_col)
  }
}

// -----------------------------------------------------------------------------------------------
//  StaticDBSchemaView
// -----------------------------------------------------------------------------------------------

pub enum StaticDBSchemaViewError {
  TableDNE(TablePath),
  ColUsageError(ColUsageError),
  KeyValidationError(KeyValidationError),
  ReqColPresenceError(ColName),
}

/// In this implementation of `DBSchemaView`, functions are not idempotent. We just use
/// static reads to get the nearest prior version, regardless of if that can change in the
/// future (i.e. if a LAT is high enough).
///
/// Importantly, the data members are immutable.
pub struct StaticDBSchemaView<'a> {
  pub db_schema: &'a BTreeMap<(TablePath, Gen), TableSchema>,
  pub table_generation: &'a MVM<TablePath, Gen>,
  pub timestamp: Timestamp,
}

impl<'a> StaticDBSchemaView<'a> {
  fn get_table_schema(
    &mut self,
    table_path: &TablePath,
  ) -> Result<&TableSchema, StaticDBSchemaViewError> {
    let gen = self.get_gen(table_path)?;
    Ok(self.db_schema.get(&(table_path.clone(), gen)).unwrap())
  }
}

impl<'a> DBSchemaView for StaticDBSchemaView<'a> {
  type ErrorT = StaticDBSchemaViewError;

  fn key_cols(&mut self, table_path: &TablePath) -> Result<&Vec<(ColName, ColType)>, Self::ErrorT> {
    Ok(&self.get_table_schema(table_path)?.key_cols)
  }

  fn get_gen(&mut self, table_path: &TablePath) -> Result<Gen, StaticDBSchemaViewError> {
    if let Some(gen) = self.table_generation.static_read(table_path, &self.timestamp) {
      Ok(gen.clone())
    } else {
      Err(StaticDBSchemaViewError::TableDNE(table_path.clone()))
    }
  }

  fn contains_table(&mut self, table_path: &TablePath) -> Result<bool, Self::ErrorT> {
    Ok(self.table_generation.static_read(table_path, &self.timestamp).is_some())
  }

  fn contains_col(
    &mut self,
    table_path: &TablePath,
    col_name: &ColName,
  ) -> Result<bool, StaticDBSchemaViewError> {
    let timestamp = self.timestamp.clone();
    let schema = self.get_table_schema(table_path)?;
    if lookup(&schema.key_cols, col_name).is_none() {
      Ok(schema.val_cols.static_read(col_name, &timestamp).is_some())
    } else {
      Ok(true)
    }
  }
}

impl ColUsageErrorTrait for StaticDBSchemaViewError {
  fn mk_error(err: ColUsageError) -> StaticDBSchemaViewError {
    StaticDBSchemaViewError::ColUsageError(err)
  }
}

impl KeyValidationErrorTrait for StaticDBSchemaViewError {
  fn mk_error(err: KeyValidationError) -> Self {
    StaticDBSchemaViewError::KeyValidationError(err)
  }
}

impl ReqTablePresenceError for StaticDBSchemaViewError {
  fn mk_error(missing_table: TablePath) -> Self {
    StaticDBSchemaViewError::TableDNE(missing_table)
  }
}

impl ReqColPresenceError for StaticDBSchemaViewError {
  fn mk_error(missing_col: ColName) -> Self {
    StaticDBSchemaViewError::ReqColPresenceError(missing_col)
  }
}

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
//  MasterQueryPlanning
// -----------------------------------------------------------------------------------------------

/// Actions for the Master to execute
pub enum MasterQueryPlanningAction {
  Wait,
  Respond(msg::MasteryQueryPlanningResult),
}

pub fn master_query_planning<
  ErrorT: ColUsageErrorTrait + KeyValidationErrorTrait + ReqTablePresenceError + ReqColPresenceError,
  ViewT: DBSchemaView<ErrorT = ErrorT>,
>(
  mut view: ViewT,
  ms_query: &proc::MSQuery,
) -> Result<msg::MasterQueryPlan, ErrorT> {
  // First, check that all TablePaths are present
  let table_paths = collect_table_paths(ms_query);
  let table_location_map = compute_table_location_map(&mut view, &table_paths)?;

  // Next, we do various validations on the MSQuery.
  perform_static_validations(&mut view, ms_query)?;

  // Next, we see if all required columns in all queries are present.
  let extra_req_cols = compute_extra_req_cols(ms_query);
  check_cols_present(&mut view, &extra_req_cols)?;

  // Next, we run the FrozenColUsageAlgorithm
  let mut planner = ColUsagePlanner { view };
  let col_usage_nodes = planner.plan_ms_query(ms_query)?;

  // Finally we construct a MasterQueryPlan and respond to the sender.
  let all_tier_maps = compute_all_tier_maps(ms_query);
  Ok(msg::MasterQueryPlan { all_tier_maps, table_location_map, extra_req_cols, col_usage_nodes })
}

// -----------------------------------------------------------------------------------------------
//  MasterQueryPlanningES Pre-Insertion
// -----------------------------------------------------------------------------------------------

/// Handle an incoming `PerformMasterQueryPlanning` message.
pub fn master_query_planning_pre(
  ctx: &MasterContext,
  planning_msg: msg::PerformMasterQueryPlanning,
) -> MasterQueryPlanningAction {
  let view = CheckingDBSchemaView {
    db_schema: &ctx.db_schema,
    table_generation: &ctx.table_generation,
    timestamp: planning_msg.timestamp.clone(),
  };

  fn respond_error(error: msg::QueryPlanningError) -> MasterQueryPlanningAction {
    MasterQueryPlanningAction::Respond(msg::MasteryQueryPlanningResult::QueryPlanningError(error))
  }

  match master_query_planning(view, &planning_msg.ms_query) {
    Ok(master_query_plan) => MasterQueryPlanningAction::Respond(
      msg::MasteryQueryPlanningResult::MasterQueryPlan(master_query_plan),
    ),
    Err(error) => match error {
      CheckingDBSchemaViewError::InsufficientLat => MasterQueryPlanningAction::Wait,
      CheckingDBSchemaViewError::TableDNE(table_path) => {
        respond_error(msg::QueryPlanningError::TablesDNE(vec![table_path]))
      }
      CheckingDBSchemaViewError::ColUsageError(error) => respond_error(match error {
        ColUsageError::InvalidColumnRef => msg::QueryPlanningError::InvalidColUsage,
        ColUsageError::InvalidSelectClause => msg::QueryPlanningError::InvalidSelect,
      }),
      CheckingDBSchemaViewError::KeyValidationError(error) => respond_error(match error {
        KeyValidationError::InvalidUpdate => msg::QueryPlanningError::InvalidUpdate,
        KeyValidationError::InvalidInsert => msg::QueryPlanningError::InvalidInsert,
      }),
      CheckingDBSchemaViewError::ReqColPresenceError(missing_col) => {
        respond_error(msg::QueryPlanningError::RequiredColumnDNE(vec![missing_col]))
      }
    },
  }
}

// -----------------------------------------------------------------------------------------------
//  MasterQueryPlanningES Post-Insertion
// -----------------------------------------------------------------------------------------------

/// Handle the insertion of a `MasterQueryPlanning` message.
pub fn master_query_planning_post(
  ctx: &mut MasterContext,
  planning_plm: plm::MasterQueryPlanning,
) -> msg::MasteryQueryPlanningResult {
  let mut view = LockingDBSchemaView {
    db_schema: &mut ctx.db_schema,
    table_generation: &mut ctx.table_generation,
    timestamp: planning_plm.timestamp.clone(),
  };

  match master_query_planning(view, &planning_plm.ms_query) {
    Ok(master_query_plan) => msg::MasteryQueryPlanningResult::MasterQueryPlan(master_query_plan),
    Err(error) => msg::MasteryQueryPlanningResult::QueryPlanningError(match error {
      LockingDBSchemaViewError::TableDNE(table_path) => {
        msg::QueryPlanningError::TablesDNE(vec![table_path.clone()])
      }
      LockingDBSchemaViewError::ColUsageError(error) => match error {
        ColUsageError::InvalidColumnRef => msg::QueryPlanningError::InvalidColUsage,
        ColUsageError::InvalidSelectClause => msg::QueryPlanningError::InvalidSelect,
      },
      LockingDBSchemaViewError::KeyValidationError(error) => match error {
        KeyValidationError::InvalidUpdate => msg::QueryPlanningError::InvalidUpdate,
        KeyValidationError::InvalidInsert => msg::QueryPlanningError::InvalidInsert,
      },
      LockingDBSchemaViewError::ReqColPresenceError(missing_col) => {
        msg::QueryPlanningError::RequiredColumnDNE(vec![missing_col])
      }
    }),
  }
}
