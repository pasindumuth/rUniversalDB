use crate::col_usage::FrozenColUsageNode;
use crate::common::{GossipData, QueryPlan};
use crate::model::common::{
  proc, ColName, ColType, Context, EndpointId, Gen, NodeGroupId, QueryId, RequestId, SlaveGroupId,
  TablePath, TableView, TabletGroupId, TierMap, Timestamp, TransTableLocationPrefix,
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
  ExternalQueryAbort(ExternalQueryAbort),
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
pub enum SenderStatePath {
  TMStatusQueryId(QueryId),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryPath {
  pub slave_group_id: SlaveGroupId,
  pub maybe_tablet_group_id: Option<TabletGroupId>,
  pub query_id: SenderStatePath,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformQuery {
  pub root_query_path: QueryId,
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
  /// The receiving node's State that the responses should be routed to.
  pub sender_status_path: SenderStatePath,
  /// The Tablet/Slave that just succeeded.
  pub node_group_id: NodeGroupId,
  pub query_id: QueryId,
  pub result: (Vec<ColName>, Vec<TableView>),
  pub new_rms: Vec<TabletGroupId>,
}

/// These are Errors that are simply recursively propagated up to the Slave,
/// where all intermediary ESs are exited.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
  // Fatal Query Errors to be propagated to the user.
  TypeError { msg: String },
  RuntimeError { msg: String },
  ProjectedColumnsDNE { msg: String },

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
  pub sender_state_path: SenderStatePath,
  pub tablet_group_id: TabletGroupId,
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
//  2PC messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MSQueryCoordPath(pub QueryId);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCPrepare {
  pub sender_path: (SlaveGroupId, MSQueryCoordPath),
  pub root_query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCPrepared {
  pub return_path: MSQueryCoordPath,
  pub tablet_group_id: TabletGroupId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCAborted {
  pub return_path: MSQueryCoordPath,
  pub tablet_group_id: TabletGroupId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCAbort {
  pub root_query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Query2PCCommit {
  pub root_query_id: QueryId,
}

// -------------------------------------------------------------------------------------------------
//  External Messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformExternalQuery {
  pub sender_path: EndpointId,
  pub request_id: RequestId,
  pub query: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CancelExternalQuery {
  pub request_id: RequestId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalQuerySuccess {
  pub request_id: RequestId,
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
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalQueryAbort {
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
  ColUsageNodes(HashMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)>),
  ColUsageNode(FrozenColUsageNode),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformMasterFrozenColUsage {
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
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterFrozenColUsageSuccess {
  pub query_id: QueryId,
  pub frozen_col_usage_tree: FrozenColUsageTree,
  // TODO: to address the `gossip` problem, we can just hold the underlying map of the MVM.
  /* pub gossip: GossipData */
}
