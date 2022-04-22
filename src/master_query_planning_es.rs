use crate::col_usage::{
  free_external_cols, iterate_stage_ms_query, ColUsageError, ColUsageNode, ColUsagePlanner,
  GeneralStage,
};
use crate::common::{
  add_item, default_get_mut, lookup, map_insert, FullGen, MasterIOCtx, RemoteLeaderChangedPLm,
  TableSchema, Timestamp,
};
use crate::common::{
  CQueryPath, ColName, ColType, Gen, PaxosGroupId, PaxosGroupIdTrait, QueryId, TablePath,
  TransTableName,
};
use crate::master::{MasterContext, MasterPLm};
use crate::message as msg;
use crate::multiversion_map::MVM;
use crate::query_planning::{
  collect_table_paths, compute_all_tier_maps, compute_table_location_map, perform_validations,
  StaticValidationError,
};
use crate::server::ServerContextBase;
use crate::sql_ast::proc;
use crate::sql_ast::proc::MSQueryStage;
use serde::{Deserialize, Serialize};
use sqlparser::test_utils::table;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Error Traits
// -----------------------------------------------------------------------------------------------

pub trait ColUsageErrorTrait {
  fn mk_error(err: ColUsageError) -> Self;
}

pub trait StaticValidationErrorTrait {
  fn mk_error(err: StaticValidationError) -> Self;
}

pub trait ReqTablePresenceError {
  fn mk_error(missing_col: TablePath) -> Self;
}

pub trait ReqColPresenceError {
  fn mk_error(missing_col: ColName) -> Self;
}

// -----------------------------------------------------------------------------------------------
//  ColPresenceReq
// -----------------------------------------------------------------------------------------------

/// Here, `present_cols` and `absent_cols` are disjoint, and all elements
/// in each vector are unique.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct ReqPresentAbsent {
  pub present_cols: Vec<ColName>,
  pub absent_cols: Vec<ColName>,
}

/// Here, all elements in `cols` are unique.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct ReqPresentExclusive {
  pub cols: Vec<ColName>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ColPresenceReq {
  ReqPresentAbsent(ReqPresentAbsent),
  ReqPresentExclusive(ReqPresentExclusive),
}

impl Default for ColPresenceReq {
  fn default() -> Self {
    ColPresenceReq::ReqPresentAbsent(ReqPresentAbsent::default())
  }
}

/// This marks the presence/absence of a ValCol `col_name` in the Table `table_path`.
///
/// The `col_name` and `present` must align with whatever `ColPresenceReq` currently is.
fn mark_val_col_presence(
  col_presence_req: &mut BTreeMap<TablePath, ColPresenceReq>,
  table_path: &TablePath,
  col_name: &ColName,
  present: bool,
) {
  // In practice, a call to this function should align with the existing
  // values in `ColPresenceReq`, so we `debug_assert`.
  let col_req = default_get_mut(col_presence_req, table_path);
  match col_req {
    ColPresenceReq::ReqPresentAbsent(req) => {
      if present {
        add_item(&mut req.present_cols, col_name);
        debug_assert!(!req.absent_cols.contains(col_name));
      } else {
        add_item(&mut req.absent_cols, col_name);
        debug_assert!(!req.present_cols.contains(col_name));
      };
    }
    ColPresenceReq::ReqPresentExclusive(req) => {
      debug_assert!(present == req.cols.contains(col_name));
    }
  }
}

/// Changes `ColPresenceReq` to `ReqPresentExclusive` containing `all_val_cols`. Note that
/// all elements in `all_val_cols` must be unique.
///
/// The `all_val_cols` must align with whatever `ColPresenceReq` currently is.
fn mark_all_val_cols(
  col_presence_req: &mut BTreeMap<TablePath, ColPresenceReq>,
  table_path: &TablePath,
  all_val_cols: Vec<ColName>,
) {
  // In practice, a call to this function should align with the existing
  // values in `ColPresenceReq`, so we `debug_assert`.
  let col_req = default_get_mut(col_presence_req, table_path);
  match col_req {
    ColPresenceReq::ReqPresentAbsent(req) => {
      debug_assert!((|| {
        for col in &req.present_cols {
          if !all_val_cols.contains(col) {
            return false;
          }
        }
        for col in &req.absent_cols {
          if all_val_cols.contains(col) {
            return false;
          }
        }
        return true;
      })());
      *col_req = ColPresenceReq::ReqPresentExclusive(ReqPresentExclusive { cols: all_val_cols });
    }
    ColPresenceReq::ReqPresentExclusive(req) => {
      debug_assert!((|| {
        if all_val_cols.len() != req.cols.len() {
          return false;
        }
        for col in &req.cols {
          if !all_val_cols.contains(col) {
            return false;
          }
        }
        return true;
      })());
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  DBSchemaView
// -----------------------------------------------------------------------------------------------

/// This exposes what is virtually an unversioned Database Schema.
pub trait DBSchemaView {
  type ErrorT;

  fn key_cols(&mut self, table_path: &TablePath) -> Result<&Vec<(ColName, ColType)>, Self::ErrorT>;

  fn get_gen(&mut self, table_path: &TablePath) -> Result<FullGen, Self::ErrorT>;

  fn contains_table(&mut self, table_path: &TablePath) -> Result<bool, Self::ErrorT>;

  fn contains_col(
    &mut self,
    table_path: &TablePath,
    col_name: &ColName,
  ) -> Result<bool, Self::ErrorT>;

  /// Returns all present `ColName`s, making sure to mark the presence of these columns,
  /// and the absence of all other columns, in the `BTreeMap<TablePath, ColPresenceReq>`.
  fn get_all_cols(&mut self, table_path: &TablePath) -> Result<Vec<ColName>, Self::ErrorT>;

  fn finish(self) -> BTreeMap<TablePath, ColPresenceReq>;
}

// -----------------------------------------------------------------------------------------------
//  CheckingDBSchemaView
// -----------------------------------------------------------------------------------------------

pub enum CheckingDBSchemaViewError {
  InsufficientLat,
  TableDNE(TablePath),
  ColUsageError(ColUsageError),
  StaticValidationError(StaticValidationError),
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
  pub table_generation: &'a MVM<TablePath, FullGen>,
  pub timestamp: Timestamp,
  pub col_presence_req: BTreeMap<TablePath, ColPresenceReq>,
}

impl<'a> CheckingDBSchemaView<'a> {
  fn get_table_schema(
    &mut self,
    table_path: &TablePath,
  ) -> Result<&TableSchema, CheckingDBSchemaViewError> {
    let (gen, _) = self.get_gen(table_path)?;
    Ok(self.db_schema.get(&(table_path.clone(), gen)).unwrap())
  }
}

impl<'a> DBSchemaView for CheckingDBSchemaView<'a> {
  type ErrorT = CheckingDBSchemaViewError;

  fn key_cols(&mut self, table_path: &TablePath) -> Result<&Vec<(ColName, ColType)>, Self::ErrorT> {
    Ok(&self.get_table_schema(table_path)?.key_cols)
  }

  fn get_gen(&mut self, table_path: &TablePath) -> Result<FullGen, CheckingDBSchemaViewError> {
    if self.table_generation.get_lat(table_path) < self.timestamp {
      Err(CheckingDBSchemaViewError::InsufficientLat)
    } else {
      if let Some(full_gen) = self.table_generation.strong_static_read(table_path, &self.timestamp)
      {
        Ok(full_gen.clone())
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
        let present = schema.val_cols.strong_static_read(col_name, &timestamp).is_some();
        mark_val_col_presence(&mut self.col_presence_req, table_path, col_name, present);
        Ok(present)
      }
    } else {
      Ok(true)
    }
  }

  fn get_all_cols(&mut self, table_path: &TablePath) -> Result<Vec<ColName>, Self::ErrorT> {
    let timestamp = self.timestamp.clone();
    let schema = self.get_table_schema(table_path)?;
    if schema.val_cols.get_min_lat() < timestamp {
      Err(CheckingDBSchemaViewError::InsufficientLat)
    } else {
      let all_cols = schema.get_schema_static(&timestamp);

      // Extract ValCols
      let mut all_val_cols = Vec::<ColName>::new();
      for col in &all_cols {
        if lookup(&schema.key_cols, col).is_none() {
          all_val_cols.push(col.clone());
        }
      }

      mark_all_val_cols(&mut self.col_presence_req, table_path, all_val_cols);
      Ok(all_cols)
    }
  }

  fn finish(self) -> BTreeMap<TablePath, ColPresenceReq> {
    self.col_presence_req
  }
}

impl ColUsageErrorTrait for CheckingDBSchemaViewError {
  fn mk_error(err: ColUsageError) -> CheckingDBSchemaViewError {
    CheckingDBSchemaViewError::ColUsageError(err)
  }
}

impl StaticValidationErrorTrait for CheckingDBSchemaViewError {
  fn mk_error(err: StaticValidationError) -> Self {
    CheckingDBSchemaViewError::StaticValidationError(err)
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
  StaticValidationError(StaticValidationError),
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
  pub table_generation: &'a mut MVM<TablePath, FullGen>,
  pub timestamp: Timestamp,
  pub col_presence_req: BTreeMap<TablePath, ColPresenceReq>,
}

impl<'a> LockingDBSchemaView<'a> {
  fn get_table_schema(
    &mut self,
    table_path: &TablePath,
  ) -> Result<&mut TableSchema, LockingDBSchemaViewError> {
    let (gen, _) = self.get_gen(table_path)?;
    Ok(self.db_schema.get_mut(&(table_path.clone(), gen)).unwrap())
  }
}

impl<'a> DBSchemaView for LockingDBSchemaView<'a> {
  type ErrorT = LockingDBSchemaViewError;

  fn key_cols(&mut self, table_path: &TablePath) -> Result<&Vec<(ColName, ColType)>, Self::ErrorT> {
    Ok(&self.get_table_schema(table_path)?.key_cols)
  }

  fn get_gen(&mut self, table_path: &TablePath) -> Result<FullGen, LockingDBSchemaViewError> {
    if let Some(full_gen) = self.table_generation.read(table_path, &self.timestamp) {
      Ok(full_gen)
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
      let present = schema.val_cols.read(col_name, &timestamp).is_some();
      mark_val_col_presence(&mut self.col_presence_req, table_path, col_name, present);
      Ok(present)
    } else {
      Ok(true)
    }
  }

  fn get_all_cols(&mut self, table_path: &TablePath) -> Result<Vec<ColName>, Self::ErrorT> {
    let timestamp = self.timestamp.clone();
    let schema = self.get_table_schema(table_path)?;
    let all_cols = schema.get_schema_static(&timestamp);
    schema.val_cols.update_all_lats(timestamp.clone());

    // Extract ValCols
    let mut all_val_cols = Vec::<ColName>::new();
    for col in &all_cols {
      if lookup(&schema.key_cols, col).is_none() {
        all_val_cols.push(col.clone());
      }
    }

    mark_all_val_cols(&mut self.col_presence_req, table_path, all_val_cols);
    Ok(all_cols)
  }

  fn finish(self) -> BTreeMap<TablePath, ColPresenceReq> {
    self.col_presence_req
  }
}

impl ColUsageErrorTrait for LockingDBSchemaViewError {
  fn mk_error(err: ColUsageError) -> LockingDBSchemaViewError {
    LockingDBSchemaViewError::ColUsageError(err)
  }
}

impl StaticValidationErrorTrait for LockingDBSchemaViewError {
  fn mk_error(err: StaticValidationError) -> Self {
    LockingDBSchemaViewError::StaticValidationError(err)
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
  StaticValidationError(StaticValidationError),
  ReqColPresenceError(ColName),
}

/// In this implementation of `DBSchemaView`, functions are not idempotent. We just use
/// static reads to get the nearest prior version, regardless of if that can change in the
/// future (i.e. if a LAT is high enough).
///
/// Importantly, the data members are immutable.
pub struct StaticDBSchemaView<'a> {
  pub db_schema: &'a BTreeMap<(TablePath, Gen), TableSchema>,
  pub table_generation: &'a MVM<TablePath, FullGen>,
  pub timestamp: Timestamp,
  pub col_presence_req: BTreeMap<TablePath, ColPresenceReq>,
}

impl<'a> StaticDBSchemaView<'a> {
  fn get_table_schema(
    &mut self,
    table_path: &TablePath,
  ) -> Result<&TableSchema, StaticDBSchemaViewError> {
    let (gen, _) = self.get_gen(table_path)?;
    Ok(self.db_schema.get(&(table_path.clone(), gen)).unwrap())
  }
}

impl<'a> DBSchemaView for StaticDBSchemaView<'a> {
  type ErrorT = StaticDBSchemaViewError;

  fn key_cols(&mut self, table_path: &TablePath) -> Result<&Vec<(ColName, ColType)>, Self::ErrorT> {
    Ok(&self.get_table_schema(table_path)?.key_cols)
  }

  fn get_gen(&mut self, table_path: &TablePath) -> Result<FullGen, StaticDBSchemaViewError> {
    if let Some(full_gen) = self.table_generation.static_read(table_path, &self.timestamp) {
      Ok(full_gen.clone())
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
      let present = schema.val_cols.static_read(col_name, &timestamp).is_some();
      mark_val_col_presence(&mut self.col_presence_req, table_path, col_name, present);
      Ok(present)
    } else {
      Ok(true)
    }
  }

  fn get_all_cols(&mut self, table_path: &TablePath) -> Result<Vec<ColName>, Self::ErrorT> {
    let timestamp = self.timestamp.clone();
    let schema = self.get_table_schema(table_path)?;
    let all_cols = schema.get_schema_static(&timestamp);

    // Extract ValCols
    let mut all_val_cols = Vec::<ColName>::new();
    for col in &all_cols {
      if lookup(&schema.key_cols, col).is_none() {
        all_val_cols.push(col.clone());
      }
    }

    mark_all_val_cols(&mut self.col_presence_req, table_path, all_val_cols);
    Ok(all_cols)
  }

  fn finish(self) -> BTreeMap<TablePath, ColPresenceReq> {
    self.col_presence_req
  }
}

impl ColUsageErrorTrait for StaticDBSchemaViewError {
  fn mk_error(err: ColUsageError) -> StaticDBSchemaViewError {
    StaticDBSchemaViewError::ColUsageError(err)
  }
}

impl StaticValidationErrorTrait for StaticDBSchemaViewError {
  fn mk_error(err: StaticValidationError) -> Self {
    StaticDBSchemaViewError::StaticValidationError(err)
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
//  MasterQueryPlanning
// -----------------------------------------------------------------------------------------------

/// Actions for the Master to execute
pub enum MasterQueryPlanningAction {
  Wait,
  Respond(msg::MasteryQueryPlanningResult),
}

pub fn master_query_planning<
  ErrorT: ColUsageErrorTrait + StaticValidationErrorTrait + ReqTablePresenceError + ReqColPresenceError,
  ViewT: DBSchemaView<ErrorT = ErrorT>,
>(
  mut view: ViewT,
  ms_query: &proc::MSQuery,
) -> Result<msg::MasterQueryPlan, ErrorT> {
  // First, check that all TablePaths are present
  let table_paths = collect_table_paths(ms_query);
  let table_location_map = compute_table_location_map(&mut view, &table_paths)?;

  // Next, we do various validations on the MSQuery.
  perform_validations(&mut view, ms_query)?;

  // Next, we run the FrozenColUsageAlgorithm
  let mut planner = ColUsagePlanner { view };
  let col_usage_nodes = planner.plan_ms_query(ms_query)?;

  // Finally we construct a MasterQueryPlan and respond to the sender.
  let all_tier_maps = compute_all_tier_maps(ms_query);
  Ok(msg::MasterQueryPlan {
    all_tier_maps,
    table_location_map,
    col_presence_req: planner.finish(),
    col_usage_nodes,
  })
}

// -----------------------------------------------------------------------------------------------
//  PLms
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterQueryPlanning {
  query_id: QueryId,
  timestamp: Timestamp,
  ms_query: proc::MSQuery,
}

// -----------------------------------------------------------------------------------------------
//  MasterQueryPlanningES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct MasterQueryPlanningES {
  sender_path: CQueryPath,
  query_id: QueryId,
  timestamp: Timestamp,
  ms_query: proc::MSQuery,
}

/// Handle an incoming `PerformMasterQueryPlanning` message.
fn master_query_planning_pre(
  ctx: &MasterContext,
  planning_msg: msg::PerformMasterQueryPlanning,
) -> MasterQueryPlanningAction {
  let gossip = ctx.gossip.get();
  let view = CheckingDBSchemaView {
    db_schema: gossip.db_schema,
    table_generation: gossip.table_generation,
    timestamp: planning_msg.timestamp,
    col_presence_req: Default::default(),
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
      CheckingDBSchemaViewError::StaticValidationError(error) => respond_error(match error {
        StaticValidationError::InvalidUpdate => msg::QueryPlanningError::InvalidUpdate,
        StaticValidationError::InvalidInsert => msg::QueryPlanningError::InvalidInsert,
      }),
      CheckingDBSchemaViewError::ReqColPresenceError(missing_col) => {
        respond_error(msg::QueryPlanningError::RequiredColumnDNE(vec![missing_col]))
      }
    },
  }
}

/// Handle the insertion of a `MasterQueryPlanning` message.
fn master_query_planning_post(
  ctx: &mut MasterContext,
  planning_plm: MasterQueryPlanning,
) -> msg::MasteryQueryPlanningResult {
  ctx.gossip.update(|gossip| {
    let mut view = LockingDBSchemaView {
      db_schema: gossip.db_schema,
      table_generation: gossip.table_generation,
      timestamp: planning_plm.timestamp,
      col_presence_req: Default::default(),
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
        LockingDBSchemaViewError::StaticValidationError(error) => match error {
          StaticValidationError::InvalidUpdate => msg::QueryPlanningError::InvalidUpdate,
          StaticValidationError::InvalidInsert => msg::QueryPlanningError::InvalidInsert,
        },
        LockingDBSchemaViewError::ReqColPresenceError(missing_col) => {
          msg::QueryPlanningError::RequiredColumnDNE(vec![missing_col])
        }
      }),
    }
  })
}

// -----------------------------------------------------------------------------------------------
//  ES Container Functions
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct MasterQueryPlanningESS {
  ess: BTreeMap<QueryId, MasterQueryPlanningES>,
}

impl MasterQueryPlanningESS {
  pub fn new() -> MasterQueryPlanningESS {
    MasterQueryPlanningESS { ess: Default::default() }
  }

  // Leader-only

  pub fn handle_msg<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    request: msg::MasterQueryPlanningRequest,
  ) {
    match request {
      msg::MasterQueryPlanningRequest::Perform(perform) => {
        let action = master_query_planning_pre(ctx, perform.clone());
        match action {
          MasterQueryPlanningAction::Wait => {
            self.ess.insert(
              perform.query_id.clone(),
              MasterQueryPlanningES {
                sender_path: perform.sender_path,
                query_id: perform.query_id,
                timestamp: perform.timestamp,
                ms_query: perform.ms_query,
              },
            );
          }
          MasterQueryPlanningAction::Respond(result) => {
            ctx.send_to_c(
              io_ctx,
              perform.sender_path.node_path,
              msg::CoordMessage::MasterQueryPlanningSuccess(msg::MasterQueryPlanningSuccess {
                return_qid: perform.sender_path.query_id,
                query_id: perform.query_id,
                result,
              }),
            );
          }
        }
      }
      msg::MasterQueryPlanningRequest::Cancel(cancel) => {
        self.ess.remove(&cancel.query_id);
      }
    }
  }

  /// For every `MasterQueryPlanningES`, we add a PLm
  pub fn handle_bundle_processed(&mut self, ctx: &mut MasterContext) {
    for (_, es) in &mut self.ess {
      ctx.master_bundle.plms.push(MasterPLm::MasterQueryPlanning(MasterQueryPlanning {
        query_id: es.query_id.clone(),
        timestamp: es.timestamp.clone(),
        ms_query: es.ms_query.clone(),
      }));
    }
  }

  // Leader and Follower

  pub fn handle_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    planning_plm: MasterQueryPlanning,
  ) {
    let query_id = planning_plm.query_id.clone();
    let result = master_query_planning_post(ctx, planning_plm);
    if ctx.is_leader() {
      if let Some(es) = self.ess.remove(&query_id) {
        // If the ES still exists, we respond.
        ctx.send_to_c(
          io_ctx,
          es.sender_path.node_path,
          msg::CoordMessage::MasterQueryPlanningSuccess(msg::MasterQueryPlanningSuccess {
            return_qid: es.sender_path.query_id,
            query_id: es.query_id,
            result,
          }),
        );
      }
    }
  }

  /// For `MasterQueryPlanningES`s, if the sending PaxosGroup's Leadership
  /// changed, we ECU (no response).
  pub fn handle_rlc(&mut self, remote_leader_changed: RemoteLeaderChangedPLm) {
    let query_ids: Vec<QueryId> = self.ess.keys().cloned().collect();
    for query_id in query_ids {
      let es = self.ess.get_mut(&query_id).unwrap();
      if es.sender_path.node_path.sid.to_gid() == remote_leader_changed.gid {
        self.ess.remove(&query_id);
      }
    }
  }

  /// Wink away all `MasterQueryPlanningES`s if we lost Leadership.
  pub fn handle_lc(&mut self, ctx: &mut MasterContext) {
    if !ctx.is_leader() {
      self.ess.clear();
    }
  }

  // Utils

  pub fn is_empty(&self) -> bool {
    self.ess.is_empty()
  }
}
