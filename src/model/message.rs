use crate::alter_table_tm_es::AlterTablePayloadTypes;
use crate::col_usage::FrozenColUsageNode;
use crate::common::{GossipData, QueryPlan};
use crate::master::MasterBundle;
use crate::model::common::{
  proc, CQueryPath, CSubNodePath, CTQueryPath, ColName, ColType, Context, CoordGroupId, EndpointId,
  Gen, LeadershipId, NodeGroupId, PaxosGroupId, QueryId, RequestId, SlaveGroupId, TNodePath,
  TQueryPath, TablePath, TableView, TabletGroupId, TabletKeyRange, TierMap, Timestamp,
  TransTableLocationPrefix, TransTableName,
};
use crate::slave::SharedPaxosBundle;
use crate::stmpaxos2pc_tm;
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
pub enum MasterExternalReq {
  PerformExternalDDLQuery(PerformExternalDDLQuery),
  CancelExternalDDLQuery(CancelExternalDDLQuery),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterMessage {
  MasterExternalReq(MasterExternalReq),
  RemoteMessage(RemoteMessage<MasterRemotePayload>),
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
  ExternalMessage(SlaveExternalReq),
  RemoteMessage(RemoteMessage<SlaveRemotePayload>),
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
  RemoteLeaderChanged(RemoteLeaderChanged),

  // Master FrozenColUsageAlgorithm
  PerformMasterQueryPlanning(PerformMasterQueryPlanning),
  CancelMasterQueryPlanning(CancelMasterQueryPlanning),

  // CreateTable TM Messages
  CreateTablePrepared(CreateTablePrepared),
  CreateTableAborted(CreateTableAborted),
  CreateTableCloseConfirm(CreateTableCloseConfirm),

  // AlterTable TM Messages
  AlterTablePrepared(AlterTablePrepared),
  AlterTableAborted(AlterTableAborted),
  AlterTableCloseConfirm(AlterTableCloseConfirm),

  // AlterTable TM Messages
  AlterTablePrepared2(stmpaxos2pc_tm::Prepared<AlterTablePayloadTypes>),
  AlterTableAborted2(stmpaxos2pc_tm::Aborted<AlterTablePayloadTypes>),
  AlterTableClosed2(stmpaxos2pc_tm::Closed<AlterTablePayloadTypes>),

  // DropTable TM Messages
  DropTablePrepared(DropTablePrepared),
  DropTableAborted(DropTableAborted),
  DropTableCloseConfirm(DropTableCloseConfirm),

  // Gossip
  MasterGossipRequest(MasterGossipRequest),
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

  // AlterTable RM Messages
  AlterTablePrepare2(stmpaxos2pc_tm::Prepare<AlterTablePayloadTypes>),
  AlterTableAbort2(stmpaxos2pc_tm::Abort<AlterTablePayloadTypes>),
  AlterTableCommit2(stmpaxos2pc_tm::Commit<AlterTablePayloadTypes>),

  // DropTable RM Messages
  DropTablePrepare(DropTablePrepare),
  DropTableAbort(DropTableAbort),
  DropTableCommit(DropTableCommit),
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
  pub leadership_id: LeadershipId,
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
  pub result: (Vec<ColName>, Vec<TableView>),
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
  pub query_path: TQueryPath,
}

// -------------------------------------------------------------------------------------------------
//  2PC messages
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryPrepare {
  pub tm: CQueryPath,
  pub all_rms: Vec<TQueryPath>,
  // This is the MSQueryES's QueryId. Recall that this will also be used
  // as the QueryId for the FinishQueryES in the RM.
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryPrepared {
  pub return_qid: QueryId,
  pub rm_path: TQueryPath,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryAborted {
  pub return_qid: QueryId,
  pub rm_path: TQueryPath,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryAbort {
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryCommit {
  pub query_id: QueryId,
}

// Other Paxos2PC messages

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryCheckPrepared {
  pub tm: CQueryPath,
  pub query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryInformPrepared {
  pub tm: CQueryPath,
  pub all_rms: Vec<TQueryPath>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryWait {
  pub return_qid: QueryId,
  pub rm_path: TQueryPath,
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
  /// of the SQL transaction). It also occurs if the Update is trying to write to a key column.
  InvalidUpdate,
  /// This is a fatal Query Execution error, including non-recoverable QueryErrors
  /// and ColumnsDNEs. We don't give any details for simplicity. The External should just
  /// understand that their query was invalid, but might become valid for the same timestamp
  /// later (i.e. the invalidity is not idempotent).
  QueryExecutionError,
  /// This is sent back as a response when a CancelExternalQuery comes in. If the
  /// transaction still exists, we make sure to abort it.
  ConfirmCancel,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterQueryPlan {
  pub all_tier_maps: HashMap<TransTableName, TierMap>,
  pub table_location_map: HashMap<TablePath, Gen>,
  pub extra_req_cols: HashMap<TablePath, Vec<ColName>>,
  pub col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasteryQueryPlanningResult {
  MasterQueryPlan(MasterQueryPlan),
  TablePathDNE(Vec<TablePath>),
  /// This is returned if one of the Update queries tried modifiying a KeyCol.
  InvalidUpdate,
  RequiredColumnDNE(Vec<ColName>),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterQueryPlanningSuccess {
  pub return_qid: QueryId,
  pub query_id: QueryId,
  pub result: MasteryQueryPlanningResult,
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
  pub rm: TNodePath,
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
  pub rm: TNodePath,
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
  /// The KeyRange that the Tablet created by this should be servicing.
  pub key_range: TabletKeyRange,

  /// The initial schema of the Table.
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePrepared {
  pub query_id: QueryId,
  /// The responding Slave
  pub sid: SlaveGroupId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAborted {
  pub query_id: QueryId,
  /// The responding Slave
  pub sid: SlaveGroupId,
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
  pub sid: SlaveGroupId,
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
  pub rm: TNodePath,
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableAborted {
  pub query_id: QueryId,
  pub rm: TNodePath,
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
  pub rm: TNodePath,
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
  pub sender_path: CTQueryPath,
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
