use crate::col_usage::FrozenColUsageNode;
use crate::common::{GossipDataSer, QueryPlan};
use crate::model::common::{
  proc, ColName, ColType, Context, EndpointId, Gen, NodeGroupId, QueryId, QueryPath, RequestId,
  SlaveGroupId, TablePath, TableView, TabletGroupId, TierMap, Timestamp, TransTableLocationPrefix,
  TransTableName,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// -------------------------------------------------------------------------------------------------
// The External Thread Message
// -------------------------------------------------------------------------------------------------

/// External Message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalMessage {
  ExternalQuerySuccess(ExternalQuerySuccess),
  ExternalQueryAborted(ExternalQueryAborted),
  ExternalDDLQuerySuccess(ExternalDDLQuerySuccess),
  ExternalDDLQueryAborted(ExternalDDLQueryAborted),
}

// -------------------------------------------------------------------------------------------------
//  Slave Thread Message
// -------------------------------------------------------------------------------------------------

/// Message that go into the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveMessage {
  // Transaction Processing messages
  PerformQuery(PerformQuery),
  CancelQuery(CancelQuery),
  QueryAborted(QueryAborted),
  QuerySuccess(QuerySuccess),

  // External Messages
  PerformExternalQuery(PerformExternalQuery),
  CancelExternalQuery(CancelExternalQuery),

  // Tablet forwarding message
  TabletMessage(TabletGroupId, TabletMessage),

  // Register message
  RegisterQuery(RegisterQuery),

  // 2PC backward messages
  Query2PCPrepared(Query2PCPrepared),
  Query2PCAborted(Query2PCAborted),

  // Master Responses
  MasterFrozenColUsageAborted(MasterFrozenColUsageAborted),
  MasterFrozenColUsageSuccess(MasterFrozenColUsageSuccess),
}

// -------------------------------------------------------------------------------------------------
//  Tablet Thread Message
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TabletMessage {
  PerformQuery(PerformQuery),
  CancelQuery(CancelQuery),
  QueryAborted(QueryAborted),
  QuerySuccess(QuerySuccess),

  // 2PC forward messages
  Query2PCPrepare(Query2PCPrepare),
  Query2PCAbort(Query2PCAbort),
  Query2PCCommit(Query2PCCommit),

  // Internal AlterTable Messages
  AlterTablePrepare(AlterTablePrepare),
  AlterTableAbort(AlterTableAbort),
  AlterTableCommit(AlterTableCommit),

  // Master Responses
  MasterFrozenColUsageAborted(MasterFrozenColUsageAborted),
  MasterFrozenColUsageSuccess(MasterFrozenColUsageSuccess),
}

// -------------------------------------------------------------------------------------------------
//  Master Thread Message
// -------------------------------------------------------------------------------------------------

/// Message that go into the Master
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterMessage {
  // External AlterTable Messages
  PerformExternalDDLQuery(PerformExternalDDLQuery),
  CancelExternalDDLQuery(CancelExternalDDLQuery),

  // Internal AlterTable Messages
  AlterTablePrepared(AlterTablePrepared),
  AlterTableAborted(AlterTableAborted),

  // Master FrozenColUsageAlgorithm
  PerformMasterFrozenColUsage(PerformMasterFrozenColUsage),
  CancelMasterFrozenColUsage(CancelMasterFrozenColUsage),
}

// -------------------------------------------------------------------------------------------------
//  Network Thread Message
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum NetworkMessage {
  External(ExternalMessage),
  Slave(SlaveMessage),
  Master(MasterMessage),
}

// -------------------------------------------------------------------------------------------------
//  Transaction PCSA Messages
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformQuery {
  pub root_query_path: QueryPath,
  pub sender_path: QueryPath,
  pub query_id: QueryId,
  pub tier_map: TierMap,
  pub query: GeneralQuery,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CancelQuery {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QuerySuccess {
  /// The receiving nodes' State that the responses should be routed to. (e.g. TMStatus)
  pub return_qid: QueryId,
  pub query_id: QueryId,
  pub result: (Vec<ColName>, Vec<TableView>),
  pub new_rms: Vec<QueryPath>,
}

/// These are Errors that are simply recursively propagated up to the Slave,
/// where all intermediary ESs are exited.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
  // Fatal Query Errors to be propagated to the user.
  TypeError { msg: String },
  RuntimeError { msg: String },
  RequiredColumnsDNE { msg: String },

  // Transient Errors that can be solved by retrying.
  WriteRegionConflictWithSubsequentRead,
  DeadlockSafetyAbortion,
  TimestampConflict,

  LateralError,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AbortedData {
  ColumnsDNE { missing_cols: Vec<ColName> },
  QueryError(QueryError),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryAborted {
  pub return_qid: QueryId,
  // The QueryId of the query that was aborted.
  pub query_id: QueryId,
  pub payload: AbortedData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum GeneralQuery {
  SuperSimpleTransTableSelectQuery(SuperSimpleTransTableSelectQuery),
  SuperSimpleTableSelectQuery(SuperSimpleTableSelectQuery),
  UpdateQuery(UpdateQuery),
}

// -------------------------------------------------------------------------------------------------
//  Transaction Inner Messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SuperSimpleTransTableSelectQuery {
  pub location_prefix: TransTableLocationPrefix,
  pub context: Context,
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SuperSimpleTableSelectQuery {
  pub timestamp: Timestamp,
  pub context: Context,
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateQuery {
  pub timestamp: Timestamp,
  pub context: Context,
  pub sql_query: proc::Update,
  pub query_plan: QueryPlan,
}

// -------------------------------------------------------------------------------------------------
//  Register message
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RegisterQuery {
  pub root_query_id: QueryId,
  pub query_path: QueryPath,
}

// -------------------------------------------------------------------------------------------------
//  2PC messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCPrepare {
  pub sender_path: QueryPath,
  pub ms_query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCPrepared {
  pub return_qid: QueryId,
  pub rm_path: QueryPath,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Query2PCAbortReason {
  /// The MSQueryES in the Tablet couldn't respond with Prepared because it was removed
  /// due to a DeadlockSafetyWriteAbort.
  DeadlockSafetyAbortion,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCAborted {
  pub return_qid: QueryId,
  pub rm_path: QueryPath,
  pub reason: Query2PCAbortReason,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCAbort {
  pub ms_query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCCommit {
  pub ms_query_id: QueryId,
}

// -------------------------------------------------------------------------------------------------
//  Transaction Processing External Messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformExternalQuery {
  pub sender_eid: EndpointId,
  pub request_id: RequestId,
  pub query: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CancelExternalQuery {
  pub sender_eid: EndpointId,
  pub request_id: RequestId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalQuerySuccess {
  pub request_id: RequestId,
  pub timestamp: Timestamp,
  pub result: TableView,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalAbortedData {
  /// Happens if we get an External Query with a RequestId that's already in use.
  NonUniqueRequestId,
  /// Happens during the initial parsing of the Query.
  ParseError(String),
  /// This occurs in the when the SQL query contains a table reference
  /// that is neither a TransTable or a Table in the gossiped_db_schema.
  TableDNE(String),
  /// This occurs if an Update appears as a Subquery (i.e. not at the top-level
  /// of the SQL transaction).
  InvalidUpdate,
  /// This is a fatal Query Execution error, including non-recoverable QueryErrors
  /// and ColumnsDNEs. We don't give any details for simplicity. The External should just
  /// understand that their query was invalid, but might become valid for the same timestamp
  /// later (i.e. the invalidity is not idempotent).
  QueryExecutionError,
  /// This is sent back as a response when a CancelExternalQuery comes in. If the
  /// transaction still exists, we make sure to abort it.
  ConfirmCancel,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalQueryAborted {
  pub request_id: RequestId,
  pub payload: ExternalAbortedData,
}

// -------------------------------------------------------------------------------------------------
//  Master FrozenColUsageAlgorithm messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ColUsageTree {
  MSQuery(proc::MSQuery),
  GRQuery(proc::GRQuery),
  MSQueryStage(proc::MSQueryStage),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FrozenColUsageTree {
  ColUsageNodes(Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>),
  ColUsageNode((Vec<ColName>, FrozenColUsageNode)),
}

// These messages follow the same PCSA pattern, including using common data members
// (i.e. `sender_path`, `query_id`, and `return_qid`).

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformMasterFrozenColUsage {
  pub sender_path: QueryPath,
  pub query_id: QueryId,
  pub timestamp: Timestamp,
  pub trans_table_schemas: HashMap<TransTableName, Vec<ColName>>,
  pub col_usage_tree: ColUsageTree,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CancelMasterFrozenColUsage {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterFrozenColUsageAborted {
  pub return_qid: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterFrozenColUsageSuccess {
  pub return_qid: QueryId,
  pub frozen_col_usage_tree: FrozenColUsageTree,
  pub gossip: GossipDataSer,
}

// -------------------------------------------------------------------------------------------------
//  AlterTable Messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePrepare {
  pub query_id: QueryId,
  pub alter_op: proc::AlterOp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePrepared {
  pub query_id: QueryId,
  pub tablet_group_id: TabletGroupId,
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAborted {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAbort {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableCommit {
  pub query_id: QueryId,
  pub timestamp: Timestamp,
  pub gossip_data: GossipDataSer,
}

// -------------------------------------------------------------------------------------------------
//  External DDL Messages (AlterTable, CreateTable, etc)
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformExternalDDLQuery {
  pub sender_eid: EndpointId,
  pub request_id: RequestId,
  pub query: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CancelExternalDDLQuery {
  pub request_id: RequestId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalDDLQueryAbortData {
  NonUniqueRequestId,
  ParseError(String),
  InvalidAlterOp,
  Cancelled,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalDDLQueryAborted {
  pub request_id: RequestId,
  pub payload: ExternalDDLQueryAbortData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalDDLQuerySuccess {
  pub request_id: RequestId,
  pub timestamp: Timestamp,
}
