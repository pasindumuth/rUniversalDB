use crate::alter_table_tm_es::AlterTableTMPayloadTypes;
use crate::col_usage::ColUsageNode;
use crate::common::{
  CQueryPath, CTQueryPath, ColName, Context, CoordGroupId, EndpointId, Gen, LeadershipId,
  PaxosGroupId, QueryId, RequestId, SlaveGroupId, TQueryPath, TablePath, TableView, TabletGroupId,
  TabletKeyRange, TierMap, TransTableLocationPrefix, TransTableName,
};
use crate::common::{FullGen, GossipData, LeaderMap, QueryPlan, RemoteLeaderChangedPLm, Timestamp};
use crate::create_table_tm_es::CreateTableTMPayloadTypes;
use crate::drop_table_tm_es::DropTableTMPayloadTypes;
use crate::expression::EvalError;
use crate::finish_query_tm_es::FinishQueryPayloadTypes;
use crate::free_node_manager::FreeNodeType;
use crate::master::{MasterBundle, MasterSnapshot};
use crate::master_query_planning_es::ColPresenceReq;
use crate::paxos2pc_tm;
use crate::shard_split_tm_es::{STRange, ShardSplitTMPayloadTypes};
use crate::slave::{SharedPaxosBundle, SlaveSnapshot};
use crate::sql_ast::proc;
use crate::stmpaxos2pc_tm;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

// -------------------------------------------------------------------------------------------------
//  NetworkMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum NetworkMessage {
  External(ExternalMessage),
  Slave(SlaveMessage),
  Master(MasterMessage),
  FreeNode(FreeNodeMessage),
}

// -------------------------------------------------------------------------------------------------
//  MasterMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterExternalReq {
  // DDL
  PerformExternalDDLQuery(PerformExternalDDLQuery),
  CancelExternalDDLQuery(CancelExternalDDLQuery),
  /// Sharding
  PerformExternalSharding(PerformExternalSharding),
  CancelExternalSharding(CancelExternalSharding),
  /// This is used for debugging purposes during development.
  ExternalDebugRequest(ExternalDebugRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterMessage {
  MasterExternalReq(MasterExternalReq),
  FreeNodeAssoc(FreeNodeAssoc),
  RemoteMessage(RemoteMessage<MasterRemotePayload>),
  RemoteLeaderChangedGossip(RemoteLeaderChangedGossip),
  PaxosDriverMessage(PaxosDriverMessage<MasterBundle>),
}

impl MasterMessage {
  pub fn is_tier_1(&self) -> bool {
    match self {
      Self::PaxosDriverMessage(PaxosDriverMessage::InformLearned(_)) => true,
      Self::PaxosDriverMessage(PaxosDriverMessage::LogSyncResponse(_)) => true,
      Self::FreeNodeAssoc(_) => true,
      _ => false,
    }
  }
}

// -------------------------------------------------------------------------------------------------
//  FreeNodeAssoc
// -------------------------------------------------------------------------------------------------

/// These are messages sent from a FreeNode to the Master.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FreeNodeAssoc {
  RegisterFreeNode(RegisterFreeNode),
  FreeNodeHeartbeat(FreeNodeHeartbeat),
  ConfirmSlaveCreation(ConfirmSlaveCreation),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RegisterFreeNode {
  pub sender_eid: EndpointId,
  /// The type of FreeNode that we want to be registered as.
  pub node_type: FreeNodeType,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FreeNodeHeartbeat {
  pub sender_eid: EndpointId,
  /// The Master `LeadershipId` the FreeNode is sending the hearbeat to.
  pub cur_lid: LeadershipId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ConfirmSlaveCreation {
  pub sid: SlaveGroupId,
  pub sender_eid: EndpointId,
}

// -------------------------------------------------------------------------------------------------
//  FreeNodeMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FreeNodeMessage {
  StartMaster(StartMaster),
  FreeNodeRegistered(FreeNodeRegistered),
  MasterLeadershipId(LeadershipId),
  ShutdownNode,
  CreateSlaveGroup(CreateSlaveGroup),
  SlaveSnapshot(SlaveSnapshot),
  MasterSnapshot(MasterSnapshot),
}

/// This sent by an admin client to the some initial master nodes to get them to start.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StartMaster {
  pub master_eids: Vec<EndpointId>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FreeNodeRegistered {
  /// The `LeadershipId` of the sending Master node
  pub cur_lid: LeadershipId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateSlaveGroup {
  /// Note that `gossip` and `leader_map` here will correspond exactly.
  pub gossip: GossipData,
  pub leader_map: LeaderMap,

  pub sid: SlaveGroupId,
  pub paxos_nodes: Vec<EndpointId>,

  /// We send the `CoordGroupId`s to use
  pub coord_ids: Vec<CoordGroupId>,
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
  MasterGossip(MasterGossip),
  RemoteLeaderChangedGossip(RemoteLeaderChangedGossip),
  PaxosDriverMessage(PaxosDriverMessage<SharedPaxosBundle>),
}

impl SlaveMessage {
  pub fn is_tier_1(&self) -> bool {
    match self {
      Self::PaxosDriverMessage(PaxosDriverMessage::InformLearned(_)) => true,
      Self::PaxosDriverMessage(PaxosDriverMessage::LogSyncResponse(_)) => true,
      Self::MasterGossip(_) => true,
      Self::RemoteMessage(remote_message) => {
        // We pass MasterGossip through to avoid the case where the Master Leadership
        // changes to a newly reconfigured node, but the Slave never learned about it
        // so all messages sent out by it are rejected (including MasterGossip, which
        // is the only remedy to this situation).
        if let SlaveRemotePayload::MasterGossip(_) = &remote_message.payload {
          true
        } else {
          false
        }
      }
      _ => false,
    }
  }
}

// -------------------------------------------------------------------------------------------------
// ExternalMessage
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalMessage {
  /// DQL
  ExternalQuerySuccess(ExternalQuerySuccess),
  ExternalQueryAborted(ExternalQueryAborted),
  /// DDL
  ExternalDDLQuerySuccess(ExternalDDLQuerySuccess),
  ExternalDDLQueryAborted(ExternalDDLQueryAborted),
  /// Sharding
  ExternalShardingSuccess(ExternalShardingSuccess),
  ExternalShardingAborted(ExternalShardingAborted),
  /// Debug. This is used for debugging purposes during development.
  ExternalDebugResponse(ExternalDebugResponse),
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
  MasterQueryPlanning(MasterQueryPlanningRequest),

  // DDL STMPaxos2PC
  CreateTable(stmpaxos2pc_tm::TMMessage<CreateTableTMPayloadTypes>),
  AlterTable(stmpaxos2pc_tm::TMMessage<AlterTableTMPayloadTypes>),
  DropTable(stmpaxos2pc_tm::TMMessage<DropTableTMPayloadTypes>),
  ShardSplit(stmpaxos2pc_tm::TMMessage<ShardSplitTMPayloadTypes>),

  // Reconfig
  SlaveReconfig(SlaveReconfig),

  // Gossip
  MasterGossipRequest(MasterGossipRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveReconfig {
  NodesDead(NodesDead),
  SlaveGroupReconfigured(SlaveGroupReconfigured),
}

// -------------------------------------------------------------------------------------------------
//  SlaveRemotePayload
// -------------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveRemotePayload {
  // CreateTable RM Messages
  CreateTable(stmpaxos2pc_tm::RMMessage<CreateTableTMPayloadTypes>),
  ShardSplit(stmpaxos2pc_tm::RMMessage<ShardSplitTMPayloadTypes>),

  // Reconfig
  ReconfigSlaveGroup(ReconfigSlaveGroup),

  /// Gossip. This is different from the one at `SlaveMessage` (which is for general
  /// broadcasting) because this is a response to `MasterGossipRequest`.
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
  AlterTable(stmpaxos2pc_tm::RMMessage<AlterTableTMPayloadTypes>),
  DropTable(stmpaxos2pc_tm::RMMessage<DropTableTMPayloadTypes>),
  ShardSplit(stmpaxos2pc_tm::RMMessage<ShardSplitTMPayloadTypes>),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum CoordMessage {
  // Master Responses
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
pub struct ReconfigBundle<BundleT> {
  pub rem_eids: Vec<EndpointId>,
  pub new_eids: Vec<EndpointId>,
  pub bundle: BundleT,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LeaderChanged {
  pub lid: LeadershipId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PLEntry<BundleT> {
  Bundle(BundleT),
  ReconfigBundle(ReconfigBundle<BundleT>),
  LeaderChanged(LeaderChanged),
}

pub type PLIndex = u128;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MultiPaxosMessage<BundleT> {
  pub sender_eid: EndpointId,
  pub paxos_nodes: Vec<EndpointId>,
  pub index: PLIndex,
  pub paxos_message: PaxosMessage<PLEntry<BundleT>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IsLeader {
  pub lid: LeadershipId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InformLearned {
  pub sender_eid: EndpointId,
  /// The `PLIndex`s here are contiguous.
  pub should_learned: BTreeMap<PLIndex, Rnd>,
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
pub struct StartNewNode<BundleT> {
  pub sender_eid: EndpointId,

  // Data for Reconfiguration
  pub paxos_nodes: Vec<EndpointId>,
  pub remote_next_indices: BTreeMap<EndpointId, PLIndex>,
  pub next_index: PLIndex,
  pub paxos_instance_vals: BTreeMap<PLIndex, (Rnd, PLEntry<BundleT>)>,
  pub unconfirmed_eids: BTreeSet<EndpointId>,
  pub leader: LeadershipId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NewNodeStarted {
  pub paxos_node: EndpointId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PaxosDriverMessage<BundleT> {
  MultiPaxosMessage(MultiPaxosMessage<BundleT>),
  IsLeader(IsLeader),
  InformLearned(InformLearned),
  LogSyncRequest(LogSyncRequest),
  LogSyncResponse(LogSyncResponse<BundleT>),
  NextIndexRequest(NextIndexRequest),
  NextIndexResponse(NextIndexResponse),
  StartNewNode(StartNewNode<BundleT>),
  NewNodeStarted(NewNodeStarted),
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
  DeleteQuery(DeleteQuery),
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DeleteQuery {
  pub timestamp: Timestamp,
  pub context: Context,
  pub sql_query: proc::Delete,
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
  /// Occurs if a Delete appears as a Subquery.
  InvalidDelete,
  /// Occurs if an Select has a mixure of aggregate columns and non-aggregate columns.
  InvalidSelect,
  /// Occurs if a `ColumnRef` has an `table_name`, but the reference table does not exist, or
  /// the does not contain the `col_name`, or if the `ColumnRef` appears as an `external_cols`
  /// in the top-level `ColUsageNode`s
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

  /// This is sent back as a response when a CancelExternalQuery comes in. If the
  /// transaction still exists, we make sure to abort it.
  CancelConfirmed,
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
pub enum MasterQueryPlanningRequest {
  Perform(PerformMasterQueryPlanning),
  Cancel(CancelMasterQueryPlanning),
}

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

/// See `QueryPlan`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterQueryPlan {
  pub all_tier_maps: BTreeMap<TransTableName, TierMap>,
  pub table_location_map: BTreeMap<TablePath, FullGen>,
  pub col_presence_req: BTreeMap<TablePath, ColPresenceReq>,
  pub col_usage_nodes: Vec<(TransTableName, ColUsageNode)>,
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
//  Reconfig messages
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NodesDead {
  pub sid: SlaveGroupId,
  pub eids: Vec<EndpointId>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ReconfigSlaveGroup {
  pub new_eids: Vec<EndpointId>,
  pub rem_eids: Vec<EndpointId>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SlaveGroupReconfigured {
  pub sid: SlaveGroupId,
}

// -------------------------------------------------------------------------------------------------
//  Master Gossip
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterGossip {
  pub gossip_data: GossipData,
  // This is use to distribute a valid Leadership of a Slave so that Slaves that do not yet
  // know about a it can populate their LeaderMap properly.
  pub leader_map: LeaderMap,
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
  CancelConfirmed,
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

// -------------------------------------------------------------------------------------------------
//  External Sharding
// -------------------------------------------------------------------------------------------------

/// Constructed by the Admin as a command to Split off a
/// part of the `target_old` into a `target_new` Tablet.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SplitShardingOp {
  pub table_path: TablePath,
  pub target_old: STRange,
  pub target_new: STRange,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ShardingOp {
  Split(SplitShardingOp),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PerformExternalSharding {
  pub sender_eid: EndpointId,
  pub request_id: RequestId,
  pub op: ShardingOp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CancelExternalSharding {
  pub sender_eid: EndpointId,
  pub request_id: RequestId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalShardingAbortData {
  NonUniqueRequestId,
  InvalidShardingOp,
  CancelConfirmed,
  Unknown,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalShardingAborted {
  pub request_id: RequestId,
  pub payload: ExternalShardingAbortData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalShardingSuccess {
  pub request_id: RequestId,
  pub timestamp: Timestamp,
}

// -------------------------------------------------------------------------------------------------
//  External Debug
// -------------------------------------------------------------------------------------------------
/// This is used for debugging purposes during development.

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalDebugRequest {
  pub sender_eid: EndpointId,
  pub request_id: RequestId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExternalDebugResponse {
  pub sender_eid: EndpointId,
  pub request_id: RequestId,
  pub debug_str: String,
}
