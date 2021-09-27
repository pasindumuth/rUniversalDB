use crate::col_usage::FrozenColUsageNode;
use crate::common::{GossipDataSer, QueryPlan};
use crate::model::common::{
  proc, ColName, ColType, Context, CoordGroupId, EndpointId, Gen, LeadershipId, NodeGroupId,
  PaxosGroupId, QueryId, QueryPath, RequestId, SlaveGroupId, TablePath, TableView, TabletGroupId,
  TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// -------------------------------------------------------------------------------------------------
//  NetworkMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum NetworkMessage {
  External(ExternalMessage),
  Slave(SlaveMessage),
  Master(MasterMessage),
}

// -------------------------------------------------------------------------------------------------
//  MasterMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterMessage {
  // External AlterTable Messages
  PerformExternalDDLQuery(PerformExternalDDLQuery),
  CancelExternalDDLQuery(CancelExternalDDLQuery),

  // Master FrozenColUsageAlgorithm
  PerformMasterFrozenColUsage(PerformMasterFrozenColUsage),
  CancelMasterFrozenColUsage(CancelMasterFrozenColUsage),

  // CreateTable TM Messages
  CreateTablePrepared(CreateTablePrepared),
  CreateTableAborted(CreateTableAborted),
  CreateTableCloseConfirm(CreateTableCloseConfirm),

  // AlterTable TM Messages
  AlterTablePrepared(AlterTablePrepared),
  AlterTableAborted(AlterTableAborted),
  AlterTableCloseConfirm(AlterTableCloseConfirm),

  // DropTable TM Messages
  DropTablePrepared(DropTablePrepared),
  DropTableAborted(DropTableAborted),
  DropTableCloseConfirm(DropTableCloseConfirm),
}

// -------------------------------------------------------------------------------------------------
//  SlaveMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveExternalReq {
  PerformExternalQuery(PerformExternalQuery),
  CancelExternalQuery(CancelExternalQuery),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveMessage {
  ExternalMessage(SlaveExternalReq),
  RemoteMessage(RemoteMessage<SlaveRemotePayload>),
  PaxosMessage(PaxosMessage),
}

// -------------------------------------------------------------------------------------------------
// ExternalMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalMessage {
  ExternalQuerySuccess(ExternalQuerySuccess),
  ExternalQueryAborted(ExternalQueryAborted),
  ExternalDDLQuerySuccess(ExternalDDLQuerySuccess),
  ExternalDDLQueryAborted(ExternalDDLQueryAborted),
}

// -------------------------------------------------------------------------------------------------
//  RemoteMessage
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RemoteMessage<PayloadT> {
  pub payload: PayloadT,
  pub from_lid: LeadershipId,
  pub from_gid: PaxosGroupId,
  pub to_lid: LeadershipId,
  pub to_gid: PaxosGroupId,
}

// -------------------------------------------------------------------------------------------------
// PaxosMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PaxosMessage {}

// -------------------------------------------------------------------------------------------------
//  MasterRemotePayload
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterRemotePayload {
  RemoteLeaderChanged(RemoteLeaderChanged),

  // Master FrozenColUsageAlgorithm
  PerformMasterFrozenColUsage(PerformMasterFrozenColUsage),
  CancelMasterFrozenColUsage(CancelMasterFrozenColUsage),

  // CreateTable TM Messages
  CreateTablePrepared(CreateTablePrepared),
  CreateTableAborted(CreateTableAborted),
  CreateTableCloseConfirm(CreateTableCloseConfirm),

  // AlterTable TM Messages
  AlterTablePrepared(AlterTablePrepared),
  AlterTableAborted(AlterTableAborted),
  AlterTableCloseConfirm(AlterTableCloseConfirm),

  // DropTable TM Messages
  DropTablePrepared(DropTablePrepared),
  DropTableAborted(DropTableAborted),
  DropTableCloseConfirm(DropTableCloseConfirm),
}

// -------------------------------------------------------------------------------------------------
//  SlaveRemotePayload
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveRemotePayload {
  RemoteLeaderChanged(RemoteLeaderChanged),

  // CreateTable RM Messages
  CreateTablePrepare(CreateTablePrepare),
  CreateTableCommit(CreateTableCommit),
  CreateTableAbort(CreateTableAbort),

  MasterGossip(MasterGossip),

  // Forwarding Messages
  TabletMessage(TabletGroupId, TabletMessage),
  CoordMessage(CoordGroupId, CoordMessage),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TabletMessage {
  PerformQuery(PerformQuery),
  CancelQuery(CancelQuery),
  QueryAborted(QueryAborted),
  QuerySuccess(QuerySuccess),

  // FinishQuery RM Messages
  FinishQueryPrepare(FinishQueryPrepare),
  FinishQueryAbort(FinishQueryAbort),
  FinishQueryCommit(FinishQueryCommit),
  FinishQueryCheckPrepared(FinishQueryCheckPrepared),

  // AlterTable RM Messages
  AlterTablePrepare(AlterTablePrepare),
  AlterTableAbort(AlterTableAbort),
  AlterTableCommit(AlterTableCommit),

  // DropTable RM Messages
  DropTablePrepare(DropTablePrepare),
  DropTableAbort(DropTableAbort),
  DropTableCommit(DropTableCommit),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum CoordMessage {
  // Master Responses
  MasterFrozenColUsageAborted(MasterFrozenColUsageAborted),
  MasterFrozenColUsageSuccess(MasterFrozenColUsageSuccess),

  // PCSA
  PerformQuery(PerformQuery),
  CancelQuery(CancelQuery),
  QueryAborted(QueryAborted),
  QuerySuccess(QuerySuccess),

  // FinishQuery TM Messages
  FinishQueryPrepared(FinishQueryPrepared),
  FinishQueryAborted(FinishQueryAborted),
  FinishQueryInformPrepared(FinishQueryInformPrepared),
  FinishQueryWait(FinishQueryWait),

  // Register message
  RegisterQuery(RegisterQuery),
}

// -------------------------------------------------------------------------------------------------
//  RemoteLeaderChanged
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RemoteLeaderChanged {}

// -------------------------------------------------------------------------------------------------
//  Transaction PCSA Messages
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformQuery {
  pub root_query_path: QueryPath,
  pub sender_path: QueryPath,
  pub query_id: QueryId,
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
  /// Contains QueryId of the query that was succeeded.
  pub responder_path: QueryPath,
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

  // Transient Errors that can be solved by retrying.
  WriteRegionConflictWithSubsequentRead,
  DeadlockSafetyAbortion,
  TimestampConflict,

  LateralError,

  // Query Validation Errors
  InvalidLeadershipId,
  InvalidQueryPlan,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AbortedData {
  QueryError(QueryError),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryAborted {
  pub return_qid: QueryId,
  /// Contains QueryId of the query that was aborted.
  pub responder_path: QueryPath,
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
// TODO: rename to FinishQuery and fix the message structures.

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryPrepare {
  pub sender_path: QueryPath,
  pub ms_query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryPrepared {
  pub return_qid: QueryId,
  pub rm_path: QueryPath,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FinishQueryAbortReason {
  /// The MSQueryES in the Tablet couldn't respond with Prepared because it was removed
  /// due to a DeadlockSafetyWriteAbort.
  DeadlockSafetyAbortion,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryAborted {
  pub return_qid: QueryId,
  pub rm_path: QueryPath,
  pub reason: FinishQueryAbortReason,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryAbort {
  pub ms_query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryCommit {
  pub ms_query_id: QueryId,
}

// Other Paxos2PC messages

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryCheckPrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryInformPrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryWait {}

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
  pub gossip: GossipDataSer, // TODO: see if we cannot need GossipDataSer; MVM should be serializable.
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
}

// Other Paxos2PC messages

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableCloseConfirm {
  pub query_id: QueryId,
  /// The responding Tablet
  pub tablet_group_id: TabletGroupId,
}

// -------------------------------------------------------------------------------------------------
//  CreateTable Messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePrepare {
  pub query_id: QueryId,

  /// Randomly generated by the Master for use by the new Tablet.
  pub tablet_group_id: TabletGroupId,
  /// The `TablePath` of the new Tablet
  pub table_path: TablePath,
  /// The `Gen` of the new Table.
  pub gen: Gen,

  /// The initial schema of the Table.
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePrepared {
  pub query_id: QueryId,
  /// The responding Slave
  pub slave_group_id: SlaveGroupId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAborted {
  pub query_id: QueryId,
  /// The responding Slave
  pub slave_group_id: SlaveGroupId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAbort {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableCommit {
  pub query_id: QueryId,
}

// Other Paxos2PC messages

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableCloseConfirm {
  pub query_id: QueryId,
  /// The responding Slave
  pub slave_group_id: SlaveGroupId,
}

// -------------------------------------------------------------------------------------------------
//  DropTable Messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTablePrepare {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTablePrepared {
  pub query_id: QueryId,
  /// The responding Tablet
  pub tablet_group_id: TabletGroupId,
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableAborted {
  pub query_id: QueryId,
  /// The responding Tablet
  pub tablet_group_id: TabletGroupId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableAbort {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableCommit {
  pub query_id: QueryId,
  pub timestamp: Timestamp,
}

// Other Paxos2PC messages

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableCloseConfirm {
  pub query_id: QueryId,
  /// The responding Tablet
  pub tablet_group_id: TabletGroupId,
}

// -------------------------------------------------------------------------------------------------
//  Master Gossip
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterGossip {
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
  pub sender_eid: EndpointId,
  pub request_id: RequestId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalDDLQueryAbortData {
  NonUniqueRequestId,
  ParseError(String),
  InvalidAlterOp,
  ConfirmCancel,
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
