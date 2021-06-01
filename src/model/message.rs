use crate::col_usage::FrozenColUsageNode;
use crate::common::GossipData;
use crate::model::common::{
  proc, ColName, ColType, Context, EndpointId, Gen, QueryId, RequestId, SlaveGroupId, TablePath,
  TableView, TabletGroupId, TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
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
}

// -------------------------------------------------------------------------------------------------
//  Network Thread Message
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum NetworkMessage {
  External(ExternalMessage),
  Slave(SlaveMessage),
}

// -------------------------------------------------------------------------------------------------
//  Transaction PCSA Messages
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SenderStatePath {
  ReadQueryPath { query_id: QueryId },
  WriteQueryPath { query_id: QueryId },
}

type NodePath = (SlaveGroupId, Option<TabletGroupId>);
type SenderPath = (NodePath, SenderStatePath);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformQuery {
  pub root_query_id: QueryId,
  pub sender_path: SenderPath,
  pub query_id: QueryId,
  pub tier_map: TierMap,
  pub query: GeneralQuery,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RecieverPath {
  ReadQuery { query_id: QueryId },
  MSReadQuery { query_id: QueryId },
  MSWriteQuery { query_id: QueryId },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CancelQuery {
  path: RecieverPath,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QuerySuccess {
  sender_path: SenderPath,
  tablet_group_id: TabletGroupId,
  query_id: QueryId,
  result: (Vec<ColName>, HashMap<u32, TableView>),
  new_rms: Vec<TabletGroupId>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AbortedData {
  // ColumnsDNE
  // TODO: to address the `gossip` problem, we can just hold the underlying map of the MVM.
  ColumnsDNE { missing_cols: Vec<ColName> /* TODO:  gossip: GossipData */ },

  // Fatal Query Errors to be propagated to the user.
  TypeError { msg: String },
  RuntimeError { msg: String },
  ProjectedColumnsDNE { msg: String },

  // Transient Errors that can be solved by retrying.
  WriteRegionConflictWithSubsequentRead,
  DeadlockSafetyAbortion,
  TimestampConflict,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryAborted {
  pub sender_path: SenderPath,
  pub tablet_group_id: TabletGroupId,
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
pub struct QueryPlan {
  pub gen: Gen,
  pub trans_table_schemas: HashMap<TransTableName, Vec<ColName>>,
  pub col_usage_node: FrozenColUsageNode,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SuperSimpleTransTableSelectQuery {
  pub location_prefix: TransTableLocationPrefix,
  pub context: Context,
  pub query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SuperSimpleTableSelectQuery {
  pub timestamp: Timestamp,
  pub context: Context,
  pub query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateQuery {
  pub timestamp: Timestamp,
  pub context: Context,
  pub query: proc::Update,
  pub query_plan: QueryPlan,
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
