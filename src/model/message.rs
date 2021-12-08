use crate::alter_table_tm_es::AlterTablePayloadTypes;
use crate::col_usage::FrozenColUsageNode;
use crate::common::{GossipData, QueryPlan};
use crate::create_table_tm_es::CreateTablePayloadTypes;
use crate::drop_table_tm_es::DropTablePayloadTypes;
use crate::expression::EvalError;
use crate::finish_query_tm_es::FinishQueryPayloadTypes;
use crate::master::MasterBundle;
use crate::model::common::{
  proc, CQueryPath, CTQueryPath, ColName, Context, CoordGroupId, EndpointId, Gen, LeadershipId,
  PaxosGroupId, QueryId, RequestId, SlaveGroupId, TQueryPath, TablePath, TableView, TabletGroupId,
  TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::paxos2pc_tm;
use crate::slave::SharedPaxosBundle;
use crate::stmpaxos2pc_tm;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
pub enum MasterExternalReq {
  PerformExternalDDLQuery(PerformExternalDDLQuery),
  CancelExternalDDLQuery(CancelExternalDDLQuery),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterMessage {
  MasterExternalReq(MasterExternalReq),
  RemoteMessage(RemoteMessage<MasterRemotePayload>),
  RemoteLeaderChangedGossip(RemoteLeaderChangedGossip),
  PaxosDriverMessage(PaxosDriverMessage<MasterBundle>),
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
  SlaveExternalReq(SlaveExternalReq),
  RemoteMessage(RemoteMessage<SlaveRemotePayload>),
  RemoteLeaderChangedGossip(RemoteLeaderChangedGossip),
  PaxosDriverMessage(PaxosDriverMessage<SharedPaxosBundle>),
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
//  MasterRemotePayload
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterRemotePayload {
  // Master FrozenColUsageAlgorithm
  PerformMasterQueryPlanning(PerformMasterQueryPlanning),
  CancelMasterQueryPlanning(CancelMasterQueryPlanning),

  CreateTable(stmpaxos2pc_tm::TMMessage<CreateTablePayloadTypes>),
  AlterTable(stmpaxos2pc_tm::TMMessage<AlterTablePayloadTypes>),
  DropTable(stmpaxos2pc_tm::TMMessage<DropTablePayloadTypes>),

  // Gossip
  MasterGossipRequest(MasterGossipRequest),
}

// -------------------------------------------------------------------------------------------------
//  SlaveRemotePayload
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveRemotePayload {
  // CreateTable RM Messages
  CreateTable(stmpaxos2pc_tm::RMMessage<CreateTablePayloadTypes>),

  // Gossip
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
  FinishQuery(paxos2pc_tm::RMMessage<FinishQueryPayloadTypes>),

  // DDL RM Messages
  AlterTable(stmpaxos2pc_tm::RMMessage<AlterTablePayloadTypes>),
  DropTable(stmpaxos2pc_tm::RMMessage<DropTablePayloadTypes>),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum CoordMessage {
  // Master Responses
  MasterQueryPlanningAborted(MasterQueryPlanningAborted),
  MasterQueryPlanningSuccess(MasterQueryPlanningSuccess),

  // PCSA
  PerformQuery(PerformQuery),
  CancelQuery(CancelQuery),
  QueryAborted(QueryAborted),
  QuerySuccess(QuerySuccess),

  // FinishQuery TM Messages
  FinishQuery(paxos2pc_tm::TMMessage<FinishQueryPayloadTypes>),

  // Register message
  RegisterQuery(RegisterQuery),
}

// -------------------------------------------------------------------------------------------------
//  RemoteLeaderChanged
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RemoteLeaderChangedGossip {
  pub gid: PaxosGroupId,
  pub lid: LeadershipId,
}

// -------------------------------------------------------------------------------------------------
// PaxosMessage
// -------------------------------------------------------------------------------------------------

pub type Rnd = u32;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Prepare {
  pub crnd: Rnd,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Promise<ValT> {
  pub rnd: Rnd,
  pub vrnd_vval: Option<(Rnd, ValT)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Accept<ValT> {
  pub crnd: Rnd,
  pub cval: ValT,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Learn {
  pub vrnd: Rnd,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PaxosMessage<ValT> {
  Prepare(Prepare),
  Promise(Promise<ValT>),
  Accept(Accept<ValT>),
  Learn(Learn),
}

// -------------------------------------------------------------------------------------------------
// PaxosDriverMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LeaderChanged {
  pub lid: LeadershipId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PLEntry<BundleT> {
  Bundle(BundleT),
  LeaderChanged(LeaderChanged),
}

pub type PLIndex = u128;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MultiPaxosMessage<BundleT> {
  pub sender_eid: EndpointId,
  pub index: PLIndex,
  pub paxos_message: PaxosMessage<PLEntry<BundleT>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IsLeader {
  pub lid: LeadershipId,
  pub should_learned: Vec<(PLIndex, Rnd)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LogSyncRequest {
  pub sender_eid: EndpointId,
  pub next_index: PLIndex,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LogSyncResponse<BundleT> {
  pub learned: Vec<(PLIndex, Rnd, PLEntry<BundleT>)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NextIndexRequest {
  pub sender_eid: EndpointId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NextIndexResponse {
  pub responder_eid: EndpointId,
  pub next_index: PLIndex,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PaxosDriverMessage<BundleT> {
  MultiPaxosMessage(MultiPaxosMessage<BundleT>),
  IsLeader(IsLeader),
  LogSyncRequest(LogSyncRequest),
  LogSyncResponse(LogSyncResponse<BundleT>),
  NextIndexRequest(NextIndexRequest),
  NextIndexResponse(NextIndexResponse),
}

// -------------------------------------------------------------------------------------------------
//  Transaction PCSA Messages
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformQuery {
  pub root_query_path: CQueryPath,
  pub sender_path: CTQueryPath,
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
  pub responder_path: CTQueryPath,
  pub result: (Vec<Option<ColName>>, Vec<TableView>),
  pub new_rms: Vec<TQueryPath>,
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

  // Lateral error, used for recursive aborting but never to be sent back to the External
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
  pub responder_path: CTQueryPath,
  pub payload: AbortedData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum GeneralQuery {
  SuperSimpleTransTableSelectQuery(SuperSimpleTransTableSelectQuery),
  SuperSimpleTableSelectQuery(SuperSimpleTableSelectQuery),
  UpdateQuery(UpdateQuery),
  InsertQuery(InsertQuery),
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InsertQuery {
  pub timestamp: Timestamp,
  pub context: Context,
  pub sql_query: proc::Insert,
  pub query_plan: QueryPlan,
}

// -------------------------------------------------------------------------------------------------
//  Register message
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RegisterQuery {
  pub root_query_id: QueryId,
  pub query_path: TQueryPath,
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
pub enum QueryPlanningError {
  /// Occurs when the query contains a `TablePath` that does exist in the database schema.
  /// This is idempotent.
  TablesDNE(Vec<TablePath>),
  /// Occurs if an `Update` occurs as a subquery or if it is trying to write to a KeyCol.
  InvalidUpdate,
  /// Occurs if an Insert appears as a Subquery, if it does not write to every KeyCol,
  /// or if the VALUES clause does not correspond to the columns to insert to.
  InvalidInsert,
  /// Occurs if an Select has a mixure of aggregate columns and non-aggregate columns.
  InvalidSelect,
  /// Occurs if a `ColumnRef` has an `table_name`, but the reference table does not exist, or
  /// the does not contain the `col_name`, or if the `ColumnRef` appears as an `external_cols`
  /// in the top-level `FrozenColUsageNode`s
  InvalidColUsage,
  /// Occurs when `ColName`s are not present in the database schema.
  RequiredColumnDNE(Vec<ColName>),
}

/// Data to send back to the External in case of a fatal Error.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalQueryError {
  TypeError { msg: String },
  RuntimeError { msg: String },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalAbortedData {
  /// Happens if we get an External Query with a RequestId that's already in use.
  NonUniqueRequestId,
  /// Happens during the initial parsing of the Query.
  ParseError(String),
  /// QueryPlanning related errors
  QueryPlanningError(QueryPlanningError),
  /// Fatal, non-recoverable errors
  QueryExecutionError(ExternalQueryError),

  /// Cancellation

  /// This is sent back as a response when a CancelExternalQuery comes in. If the
  /// transaction still exists, we make sure to abort it.
  CancelConfirmed,
  /// This is sent back as a response when a CancelExternalQuery comes in if the request is
  /// in a state where it cannot be cancelled, like during FinishQueryTMES.
  CancelDenied,
  /// This is sent back as a response when a CancelExternalQuery comes in if the `RequestId`
  /// in the cancel request does not exist.
  CancelNonExistantRequestId,

  /// This is send back if the Coord's detects its Node's Leadership changes. This is to make
  /// sure that in case the Leadership change was spurious (i.e. the current node is still alive),
  /// the External is not left waiting forever.
  NodeDied,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalQueryAborted {
  pub request_id: RequestId,
  pub payload: ExternalAbortedData,
}

// -------------------------------------------------------------------------------------------------
//  MasterQueryPlanning messages
// -------------------------------------------------------------------------------------------------
// PCSA Messages

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformMasterQueryPlanning {
  pub sender_path: CQueryPath,
  pub query_id: QueryId,
  pub timestamp: Timestamp,
  pub ms_query: proc::MSQuery,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CancelMasterQueryPlanning {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterQueryPlanningAborted {
  pub return_qid: QueryId,
}

/// See `QueryPlan`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterQueryPlan {
  pub all_tier_maps: BTreeMap<TransTableName, TierMap>,
  pub table_location_map: BTreeMap<TablePath, Gen>,
  pub extra_req_cols: BTreeMap<TablePath, Vec<ColName>>,
  pub col_usage_nodes: Vec<(TransTableName, (Vec<Option<ColName>>, FrozenColUsageNode))>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasteryQueryPlanningResult {
  MasterQueryPlan(MasterQueryPlan),
  QueryPlanningError(QueryPlanningError),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterQueryPlanningSuccess {
  pub return_qid: QueryId,
  pub query_id: QueryId,
  pub result: MasteryQueryPlanningResult,
}

// -------------------------------------------------------------------------------------------------
//  Master Gossip
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterGossip {
  pub gossip_data: GossipData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterGossipRequest {
  /// Recall that a GossipData is shared for all threads (Tablets, Coords, and Slave) for a given
  /// node. Thus, we only use the `SlaveGroupId` as the path that the Master should respond to.
  pub sender_path: SlaveGroupId,
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
  InvalidDDLQuery,
  ConfirmCancel,
  NodeDied,
  Unknown,
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
