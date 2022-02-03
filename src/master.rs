use crate::alter_table_tm_es::{
  AlterTablePayloadTypes, AlterTableTMES, AlterTableTMInner, ResponseData,
};
use crate::common::{
  lookup_pos, map_insert, mk_qid, mk_t, mk_tid, GeneralTraceMessage, GossipData, MasterIOCtx,
  MasterTraceMessage, TableSchema, Timestamp, VersionedValue,
};
use crate::common::{BasicIOCtx, RemoteLeaderChangedPLm};
use crate::create_table_tm_es::{CreateTablePayloadTypes, CreateTableTMES, CreateTableTMInner};
use crate::drop_table_tm_es::{DropTablePayloadTypes, DropTableTMES, DropTableTMInner};
use crate::free_node_manager::{FreeNodeManager, FreeNodeManagerContext, FreeNodeType};
use crate::master::plm::{
  ConfirmCreateGroup, FreeNodeManagerPLm, ReconfigSlaveGroupPLm, SlaveGroupReconfiguredPLm,
};
use crate::master_query_planning_es::{
  master_query_planning_post, master_query_planning_pre, MasterQueryPlanningAction,
  MasterQueryPlanningES,
};
use crate::model::common::{
  proc, ColName, ColType, ColVal, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait,
  PrimaryKey, QueryId, RequestId, SlaveGroupId, TNodePath, TablePath, TabletGroupId,
  TabletKeyRange,
};
use crate::model::message as msg;
use crate::model::message::{
  ExternalDDLQueryAbortData, FreeNodeAssoc, MasterExternalReq, MasterMessage, MasterRemotePayload,
  PLEntry,
};
use crate::multiversion_map::MVM;
use crate::network_driver::{NetworkDriver, NetworkDriverContext};
use crate::paxos::{PaxosConfig, PaxosContextBase, PaxosDriver, PaxosTimerEvent, UserPLEntry};
use crate::server::{contains_col_latest, MasterServerContext, ServerContextBase};
use crate::slave_group_create_es::SlaveGroupCreateES;
use crate::slave_reconfig_es::SlaveReconfigES;
use crate::sql_parser::{convert_ddl_ast, DDLQuery};
use crate::stmpaxos2pc_tm as paxos2pc;
use crate::stmpaxos2pc_tm::{
  handle_tm_msg, handle_tm_plm, STMPaxos2PCTMAction, State, TMServerContext,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::iter::FromIterator;

#[path = "./master_test.rs"]
pub mod master_test;

// -----------------------------------------------------------------------------------------------
//  MasterPLm
// -----------------------------------------------------------------------------------------------

pub mod plm {
  use crate::common::Timestamp;
  use crate::free_node_manager::FreeNodeType;
  use crate::model::common::{proc, CoordGroupId, EndpointId, PaxosGroupId, QueryId, SlaveGroupId};
  use serde::{Deserialize, Serialize};
  use std::collections::BTreeMap;

  // MasterQueryPlanning

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct MasterQueryPlanning {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
    pub ms_query: proc::MSQuery,
  }

  // FreeNodeManager

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct FreeNodeManagerPLm {
    /// Here, we hold enough data such that `msg::CreateSlaveGroup` can be Derived State.
    pub new_slave_groups: BTreeMap<SlaveGroupId, (Vec<EndpointId>, Vec<CoordGroupId>)>,
    pub new_nodes: Vec<(EndpointId, FreeNodeType)>,
    pub nodes_dead: Vec<EndpointId>,
    pub granted_reconfig_eids: BTreeMap<PaxosGroupId, Vec<EndpointId>>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct ConfirmCreateGroup {
    pub sid: SlaveGroupId,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct ReconfigSlaveGroupPLm {
    pub sid: SlaveGroupId,
    pub new_eids: Vec<EndpointId>,
    pub rem_eids: Vec<EndpointId>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SlaveGroupReconfiguredPLm {
    pub sid: SlaveGroupId,
  }
}

// -----------------------------------------------------------------------------------------------
//  MasterBundle
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct MasterBundle {
  remote_leader_changes: Vec<RemoteLeaderChangedPLm>,
  pub plms: Vec<MasterPLm>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterPLm {
  MasterQueryPlanning(plm::MasterQueryPlanning),

  // DDL and STMPaxos2PC
  CreateTable(paxos2pc::TMPLm<CreateTablePayloadTypes>),
  AlterTable(paxos2pc::TMPLm<AlterTablePayloadTypes>),
  DropTable(paxos2pc::TMPLm<DropTablePayloadTypes>),

  // FreeNode
  FreeNodeManagerPLm(FreeNodeManagerPLm),
  ConfirmCreateGroup(ConfirmCreateGroup),
  ReconfigSlaveGroupPLm(ReconfigSlaveGroupPLm),
  SlaveGroupReconfiguredPLm(SlaveGroupReconfiguredPLm),
}

// -----------------------------------------------------------------------------------------------
//  MasterPaxosContext
// -----------------------------------------------------------------------------------------------

pub struct MasterPaxosContext<'a, IO: MasterIOCtx> {
  /// IO
  pub io_ctx: &'a mut IO,

  // Metadata
  pub this_eid: &'a EndpointId,
}

impl<'a, IO: MasterIOCtx> PaxosContextBase<MasterBundle> for MasterPaxosContext<'a, IO> {
  type RngCoreT = IO::RngCoreT;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    self.io_ctx.rand()
  }

  fn this_eid(&self) -> &EndpointId {
    self.this_eid
  }

  fn send(&mut self, eid: &EndpointId, message: msg::PaxosDriverMessage<MasterBundle>) {
    self
      .io_ctx
      .send(eid, msg::NetworkMessage::Master(msg::MasterMessage::PaxosDriverMessage(message)));
  }

  fn defer(&mut self, defer_time: Timestamp, timer_event: PaxosTimerEvent) {
    self.io_ctx.defer(defer_time, MasterTimerInput::PaxosTimerEvent(timer_event));
  }
}

// -----------------------------------------------------------------------------------------------
//  MasterForwardMsg
// -----------------------------------------------------------------------------------------------

pub enum MasterForwardMsg {
  MasterBundle(Vec<MasterPLm>),
  MasterExternalReq(msg::MasterExternalReq),
  MasterRemotePayload(msg::MasterRemotePayload),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
  LeaderChanged(msg::LeaderChanged),
  MasterTimerInput(MasterTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  FullMasterInput
// -----------------------------------------------------------------------------------------------

/// Messages deferred by the Slave to be run on the Slave.
#[derive(Debug)]
pub enum MasterTimerInput {
  PaxosTimerEvent(PaxosTimerEvent),
  /// This is used to periodically propagate out RemoteLeaderChanged. It is
  /// only used by the Leader.
  RemoteLeaderChanged,
  /// This is used to periodically broadcast the `GossipData` to the Slave Leaderships. This
  /// ensures all Slave Nodes eventually get the latest `GossipData`.
  BroadcastGossip,
  /// A Time event to detect if the Master PaxosGroup has a failed node.
  PaxosGroupFailureDetector,
  /// A Timer event to increase the heartbeat for FreeNodes
  FreeNodeHeartbeatTimer,
}

pub enum FullMasterInput {
  MasterMessage(msg::MasterMessage),
  MasterTimerInput(MasterTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  Constants
// -----------------------------------------------------------------------------------------------

const REMOTE_LEADER_CHANGED_PERIOD_MS: u128 = 5;
const GOSSIP_DATA_PERIOD_MS: u128 = 5;
const FAILURE_DETECTOR_PERIOD_MS: u128 = 5;
const FREE_NODE_HEARTBEAT_TIMER_MS: u128 = 5;

// -----------------------------------------------------------------------------------------------
//  TMServerContext AlterTable
// -----------------------------------------------------------------------------------------------

impl TMServerContext<AlterTablePayloadTypes> for MasterContext {
  fn push_plm(&mut self, plm: MasterPLm) {
    self.master_bundle.plms.push(plm);
  }

  fn send_to_rm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    rm: &TNodePath,
    msg: msg::TabletMessage,
  ) {
    self.ctx(io_ctx).send_to_t(rm.clone(), msg);
  }

  fn mk_node_path(&self) -> () {
    ()
  }

  fn is_leader(&self) -> bool {
    MasterContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  TMServerContext AlterTable
// -----------------------------------------------------------------------------------------------

impl TMServerContext<DropTablePayloadTypes> for MasterContext {
  fn push_plm(&mut self, plm: MasterPLm) {
    self.master_bundle.plms.push(plm);
  }

  fn send_to_rm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    rm: &TNodePath,
    msg: msg::TabletMessage,
  ) {
    self.ctx(io_ctx).send_to_t(rm.clone(), msg);
  }

  fn mk_node_path(&self) -> () {
    ()
  }

  fn is_leader(&self) -> bool {
    MasterContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  TMServerContext CreateTable
// -----------------------------------------------------------------------------------------------

impl TMServerContext<CreateTablePayloadTypes> for MasterContext {
  fn push_plm(&mut self, plm: MasterPLm) {
    self.master_bundle.plms.push(plm);
  }

  fn send_to_rm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    rm: &SlaveGroupId,
    msg: msg::SlaveRemotePayload,
  ) {
    self.ctx(io_ctx).send_to_slave_common(rm.clone(), msg);
  }

  fn mk_node_path(&self) -> () {
    ()
  }

  fn is_leader(&self) -> bool {
    MasterContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  MasterConfig
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MasterConfig {
  /// This is used for generate the `suffix` of a Timestamp, where we just generate
  /// a random `u64` and take the remainder after dividing by `timestamp_suffix_divisor`.
  /// This cannot be 0; the default value is 1, making the suffix always be 0.
  pub timestamp_suffix_divisor: u64,
}

impl Default for MasterConfig {
  fn default() -> Self {
    MasterConfig { timestamp_suffix_divisor: 1 }
  }
}

// -----------------------------------------------------------------------------------------------
//  MasterReconfigES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum ReconfigState {
  WaitingRequestedNodes,
  InsertingReconfig { new_eids: Vec<EndpointId> },
}

#[derive(Debug)]
pub struct MasterReconfigES {
  rem_eids: Vec<EndpointId>,
  state: ReconfigState,
}

// -----------------------------------------------------------------------------------------------
//  MasterSnapshot
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterSnapshot {
  pub gossip: GossipData,
  pub leader_map: BTreeMap<PaxosGroupId, LeadershipId>,
  pub free_nodes: BTreeMap<EndpointId, FreeNodeType>,
  pub paxos_driver_start: msg::StartNewNode<MasterBundle>,
}

// -----------------------------------------------------------------------------------------------
//  Master State
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct Statuses {
  pub create_table_tm_ess: BTreeMap<QueryId, CreateTableTMES>,
  pub alter_table_tm_ess: BTreeMap<QueryId, AlterTableTMES>,
  pub drop_table_tm_ess: BTreeMap<QueryId, DropTableTMES>,
  pub planning_ess: BTreeMap<QueryId, MasterQueryPlanningES>,

  pub slave_group_create_ess: BTreeMap<SlaveGroupId, SlaveGroupCreateES>,
  pub slave_reconfig_ess: BTreeMap<SlaveGroupId, SlaveReconfigES>,

  pub master_reconfig_es: Option<MasterReconfigES>,
}

#[derive(Debug)]
pub struct MasterState {
  pub ctx: MasterContext,
  pub statuses: Statuses,
}

pub struct MasterContext {
  /// Metadata
  pub master_config: MasterConfig,
  pub this_eid: EndpointId,

  /// Metadata that is Gossiped out to the Slaves
  pub gossip: GossipData,

  /// LeaderMap
  pub leader_map: BTreeMap<PaxosGroupId, LeadershipId>,

  /// NetworkDriver
  pub network_driver: NetworkDriver<msg::MasterRemotePayload>,

  /// FreeNodeManager
  pub free_node_manager: FreeNodeManager,

  /// Request Management
  pub external_request_id_map: BTreeMap<RequestId, QueryId>,

  // Paxos
  pub master_bundle: MasterBundle,
  pub paxos_driver: PaxosDriver<MasterBundle>,
}

impl Debug for MasterContext {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut debug_trait_builder = f.debug_struct("MasterContext");
    let _ = debug_trait_builder.field("this_eid", &self.this_eid);
    let _ = debug_trait_builder.field("gossip", &self.gossip);
    let _ = debug_trait_builder.field("leader_map", &self.leader_map);
    let _ = debug_trait_builder.field("network_driver", &self.network_driver);
    let _ = debug_trait_builder.field("external_request_id_map", &self.external_request_id_map);
    let _ = debug_trait_builder.field("master_bundle", &self.master_bundle);
    debug_trait_builder.finish()
  }
}

impl MasterState {
  pub fn new(ctx: MasterContext) -> MasterState {
    MasterState { ctx, statuses: Default::default() }
  }

  /// This should be called at the very start of the life of a Master node. This
  /// will start the timer events, paxos insertion, etc.
  pub fn bootstrap<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO) {
    let ctx = &mut MasterPaxosContext { io_ctx, this_eid: &self.ctx.this_eid };
    if self.ctx.is_leader() {
      // Start the Paxos insertion cycle with an empty bundle.
      self.ctx.paxos_driver.insert_bundle(ctx, UserPLEntry::Bundle(MasterBundle::default()));
    }

    // Start Paxos Timer Events
    self.ctx.paxos_driver.timer_event(ctx, PaxosTimerEvent::LeaderHeartbeat);
    self.ctx.paxos_driver.timer_event(ctx, PaxosTimerEvent::NextIndex);

    // Start other time events
    for event in [
      MasterTimerInput::RemoteLeaderChanged,
      MasterTimerInput::BroadcastGossip,
      MasterTimerInput::PaxosGroupFailureDetector,
      MasterTimerInput::FreeNodeHeartbeatTimer,
    ] {
      self.ctx.handle_input(io_ctx, &mut self.statuses, MasterForwardMsg::MasterTimerInput(event));
    }
  }

  pub fn handle_input<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO, input: FullMasterInput) {
    match input {
      FullMasterInput::MasterMessage(message) => {
        self.ctx.handle_incoming_message(io_ctx, &mut self.statuses, message);
      }
      FullMasterInput::MasterTimerInput(timer_input) => {
        let forward_msg = MasterForwardMsg::MasterTimerInput(timer_input);
        self.ctx.handle_input(io_ctx, &mut self.statuses, forward_msg);
      }
    }
  }

  pub fn get_eids(&self) -> VersionedValue<BTreeSet<EndpointId>> {
    // TODO: do this properly
    panic!();
  }
}

impl MasterContext {
  /// Constructs a `MasterContext` for a node in the initial PaxosGroup.
  pub fn create_initial(
    master_config: MasterConfig,
    this_eid: EndpointId,
    slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
    master_address_config: Vec<EndpointId>,
    leader_map: BTreeMap<PaxosGroupId, LeadershipId>,
    paxos_config: PaxosConfig,
  ) -> MasterContext {
    let all_gids = leader_map.keys().cloned().collect();
    MasterContext {
      master_config,
      this_eid,
      gossip: GossipData::new(slave_address_config.clone(), master_address_config.clone()),
      leader_map,
      network_driver: NetworkDriver::new(all_gids),
      free_node_manager: FreeNodeManager::new(),
      external_request_id_map: Default::default(),
      master_bundle: MasterBundle::default(),
      paxos_driver: PaxosDriver::create_initial(master_address_config, paxos_config),
    }
  }

  /// Handles a `MasterSnapshot` to initiate a reconfigured node properly
  pub fn create_reconfig<IO: MasterIOCtx>(
    io_ctx: &mut IO,
    master_config: MasterConfig,
    this_eid: EndpointId,
    snapshot: MasterSnapshot,
    paxos_config: PaxosConfig,
  ) -> MasterContext {
    // TODO: fix the NetworkDriver
    MasterContext {
      master_config,
      this_eid: this_eid.clone(),
      gossip: snapshot.gossip,
      leader_map: snapshot.leader_map,
      network_driver: NetworkDriver::new(vec![]),
      free_node_manager: FreeNodeManager::create_reconfig(snapshot.free_nodes),
      external_request_id_map: Default::default(),
      master_bundle: Default::default(),
      paxos_driver: PaxosDriver::create_reconfig(
        &mut MasterPaxosContext { io_ctx, this_eid: &this_eid },
        snapshot.paxos_driver_start,
        paxos_config,
      ),
    }
  }

  pub fn ctx<'a, IO: BasicIOCtx>(&'a self, io_ctx: &'a mut IO) -> MasterServerContext<'a, IO> {
    MasterServerContext { io_ctx, this_eid: &self.this_eid, leader_map: &self.leader_map }
  }

  pub fn handle_incoming_message<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    message: msg::MasterMessage,
  ) {
    match message {
      MasterMessage::MasterExternalReq(request) => {
        if self.is_leader() {
          self.handle_input(io_ctx, statuses, MasterForwardMsg::MasterExternalReq(request))
        }
      }
      MasterMessage::RemoteMessage(remote_message) => {
        if self.is_leader() {
          // Pass the message through the NetworkDriver
          let maybe_delivered = self.network_driver.receive(
            NetworkDriverContext {
              this_gid: &PaxosGroupId::Master,
              this_eid: &self.this_eid,
              leader_map: &self.leader_map,
              remote_leader_changes: &mut self.master_bundle.remote_leader_changes,
            },
            remote_message,
          );
          if let Some(payload) = maybe_delivered {
            // Deliver if the message passed through
            self.handle_input(io_ctx, statuses, MasterForwardMsg::MasterRemotePayload(payload));
          }
        }
      }
      MasterMessage::RemoteLeaderChangedGossip(msg::RemoteLeaderChangedGossip { gid, lid }) => {
        if self.is_leader() {
          if &lid.gen > &self.leader_map.get(&gid).unwrap().gen {
            // If the incoming RemoteLeaderChanged would increase the generation
            // in LeaderMap, then persist it.
            self.master_bundle.remote_leader_changes.push(RemoteLeaderChangedPLm { gid, lid })
          }
        }
      }
      MasterMessage::FreeNodeAssoc(free_node_msg) => {
        if self.is_leader() {
          match free_node_msg {
            FreeNodeAssoc::RegisterFreeNode(register) => {
              self.free_node_manager.handle_register(register);
            }
            FreeNodeAssoc::FreeNodeHeartbeat(heartbeat) => {
              self.free_node_manager.handle_heartbeat(
                FreeNodeManagerContext {
                  this_eid: &self.this_eid,
                  leader_map: &self.leader_map,
                  master_bundle: &mut self.master_bundle,
                },
                heartbeat,
              );
            }
            FreeNodeAssoc::ConfirmSlaveCreation(confirm_msg) => {
              // Recall that the ES here could have been completed already.
              if let Some(es) = statuses.slave_group_create_ess.get_mut(&confirm_msg.sid) {
                es.handle_confirm_msg(self, io_ctx, confirm_msg);
              }
            }
          }
        }
      }
      MasterMessage::PaxosDriverMessage(paxos_message) => {
        let pl_entries = self.paxos_driver.handle_paxos_message(
          &mut MasterPaxosContext { io_ctx, this_eid: &self.this_eid },
          paxos_message,
        );
        for pl_entry in pl_entries {
          match pl_entry {
            msg::PLEntry::LeaderChanged(leader_changed) => {
              // Forward to Master Backend
              self.handle_input(
                io_ctx,
                statuses,
                MasterForwardMsg::LeaderChanged(leader_changed.clone()),
              );

              // Trace for testing
              io_ctx.trace(MasterTraceMessage::LeaderChanged(leader_changed.lid));
            }
            msg::PLEntry::Bundle(master_bundle) => {
              self.process_bundle(io_ctx, statuses, master_bundle);
            }
            msg::PLEntry::Reconfig(reconfig) => {
              // See if this node was kicked out of the PaxosGroup (by the
              // current Leader), in which case we would exit before returning.
              if !reconfig.rem_eids.contains(&self.this_eid) {
                io_ctx.mark_exit();
                return;
              } else {
                // First, process the bundle data as normal.
                self.process_bundle(io_ctx, statuses, reconfig.bundle);

                // Next, send the new `EndpointId`s a MasterSnapshot so that they can start up.
                for new_eid in &reconfig.new_eids {
                  let paxos_driver_start = self.paxos_driver.mk_start_new_node(
                    &MasterPaxosContext { io_ctx, this_eid: &self.this_eid },
                    new_eid.clone(),
                  );

                  // Construct the snapshot
                  let snapshot = MasterSnapshot {
                    gossip: self.gossip.clone(),
                    leader_map: self.leader_map.clone(),
                    free_nodes: self.free_node_manager.mk_free_nodes(),
                    paxos_driver_start,
                  };

                  // Send the Snapshot
                  io_ctx.send(
                    new_eid,
                    msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::MasterSnapshot(snapshot)),
                  )
                }
              }
            }
          }
        }
      }
    }
  }

  /// Processes the insertion of a `MasterBundle`.
  pub fn process_bundle<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    master_bundle: MasterBundle,
  ) {
    // Dispatch RemoteLeaderChanges
    for remote_change in master_bundle.remote_leader_changes.clone() {
      let forward_msg = MasterForwardMsg::RemoteLeaderChanged(remote_change.clone());
      self.handle_input(io_ctx, statuses, forward_msg);
    }

    // Dispatch the PaxosBundles
    self.handle_input(io_ctx, statuses, MasterForwardMsg::MasterBundle(master_bundle.plms));

    // Dispatch any messages that were buffered in the NetworkDriver.
    // Note: we must do this after RemoteLeaderChanges have been executed. Also note
    // that there will be no payloads in the NetworkBuffer if this nodes is a Follower.
    for remote_change in master_bundle.remote_leader_changes {
      if remote_change.lid.gen == self.leader_map.get(&remote_change.gid).unwrap().gen {
        // We need this guard, since one Bundle can hold multiple `RemoteLeaderChanged`s
        // for the same `gid` with different `gen`s.
        let payloads =
          self.network_driver.deliver_blocked_messages(remote_change.gid, remote_change.lid);
        for payload in payloads {
          self.handle_input(io_ctx, statuses, MasterForwardMsg::MasterRemotePayload(payload));
        }
      }
    }
  }

  pub fn handle_input<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    master_input: MasterForwardMsg,
  ) {
    match master_input {
      MasterForwardMsg::MasterTimerInput(timer_input) => match timer_input {
        MasterTimerInput::PaxosTimerEvent(timer_event) => {
          self
            .paxos_driver
            .timer_event(&mut MasterPaxosContext { io_ctx, this_eid: &self.this_eid }, timer_event);
        }
        MasterTimerInput::RemoteLeaderChanged => {
          if self.is_leader() {
            // If this node is the Leader, then send out RemoteLeaderChanged to all Slaves.
            self.broadcast_leadership(io_ctx);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          io_ctx
            .defer(mk_t(REMOTE_LEADER_CHANGED_PERIOD_MS), MasterTimerInput::RemoteLeaderChanged);
        }
        MasterTimerInput::BroadcastGossip => {
          if self.is_leader() {
            // If this node is the Leader, then send out GossipData to all Slaves.
            self.broadcast_gossip(io_ctx);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          io_ctx.defer(mk_t(GOSSIP_DATA_PERIOD_MS), MasterTimerInput::BroadcastGossip);
        }
        MasterTimerInput::PaxosGroupFailureDetector => {
          if self.is_leader() {
            // Only do PaxosGroup failure detection if we are not already trying to reconfigure.
            if statuses.master_reconfig_es.is_none() {
              let maybe_dead_eids = self.paxos_driver.get_maybe_dead();
              // If there are nodes that might be dead, we take only the first and then
              // attempt a Reconfiguration. Recall we only reconfigure one-at-a-time to
              // preserve our liveness properties.
              if let Some(eid) = maybe_dead_eids.into_iter().next() {
                self.free_node_manager.request_new_eids(PaxosGroupId::Master, 1);
                statuses.master_reconfig_es = Some(MasterReconfigES {
                  rem_eids: vec![eid],
                  state: ReconfigState::WaitingRequestedNodes,
                });
              }
            }
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          io_ctx
            .defer(mk_t(FAILURE_DETECTOR_PERIOD_MS), MasterTimerInput::PaxosGroupFailureDetector);
        }
        MasterTimerInput::FreeNodeHeartbeatTimer => {
          if self.is_leader() {
            self.free_node_manager.handle_timer(FreeNodeManagerContext {
              this_eid: &self.this_eid,
              leader_map: &self.leader_map,
              master_bundle: &mut self.master_bundle,
            });
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          io_ctx
            .defer(mk_t(FREE_NODE_HEARTBEAT_TIMER_MS), MasterTimerInput::FreeNodeHeartbeatTimer);
        }
      },
      MasterForwardMsg::MasterBundle(bundle) => {
        for paxos_log_msg in bundle {
          match paxos_log_msg {
            MasterPLm::MasterQueryPlanning(planning_plm) => {
              let query_id = planning_plm.query_id.clone();
              let result = master_query_planning_post(self, planning_plm);

              if self.is_leader() {
                if let Some(es) = statuses.planning_ess.remove(&query_id) {
                  // If the ES still exists, we respond.
                  self.ctx(io_ctx).send_to_c(
                    es.sender_path.node_path,
                    msg::CoordMessage::MasterQueryPlanningSuccess(
                      msg::MasterQueryPlanningSuccess {
                        return_qid: es.sender_path.query_id,
                        query_id: es.query_id,
                        result,
                      },
                    ),
                  );
                }
              }
            }
            // CreateTable
            MasterPLm::CreateTable(plm) => {
              let (query_id, action) =
                handle_tm_plm(self, io_ctx, &mut statuses.create_table_tm_ess, plm);
              self.handle_create_table_es_action(statuses, query_id, action);
            }
            // AlterTable
            MasterPLm::AlterTable(plm) => {
              let (query_id, action) =
                handle_tm_plm(self, io_ctx, &mut statuses.alter_table_tm_ess, plm);
              self.handle_alter_table_es_action(statuses, query_id, action);
            }
            // DropTable
            MasterPLm::DropTable(plm) => {
              let (query_id, action) =
                handle_tm_plm(self, io_ctx, &mut statuses.drop_table_tm_ess, plm);
              self.handle_drop_table_es_action(statuses, query_id, action);
            }
            // FreeNode PLms
            MasterPLm::FreeNodeManagerPLm(plm) => {
              let new_slave_groups = self.free_node_manager.handle_plm(
                FreeNodeManagerContext {
                  this_eid: &self.this_eid,
                  leader_map: &self.leader_map,
                  master_bundle: &mut self.master_bundle,
                },
                io_ctx,
                plm,
              );

              // Construct `SlaveGroupCreateES`s accordingly
              for (sid, (paxos_nodes, coord_ids)) in new_slave_groups {
                statuses.slave_group_create_ess.insert(
                  sid.clone(),
                  SlaveGroupCreateES::create(self, io_ctx, sid, paxos_nodes, coord_ids),
                );
              }
            }
            MasterPLm::ConfirmCreateGroup(confirm_create) => {
              // Here, we remove the ES and then finish it off.
              let mut es = statuses.slave_group_create_ess.remove(&confirm_create.sid).unwrap();
              es.handle_confirm_plm(self, io_ctx);
            }
            MasterPLm::ReconfigSlaveGroupPLm(reconfig) => {
              if let Some(es) = statuses.slave_reconfig_ess.get_mut(&reconfig.sid) {
                es.handle_reconfig_plm(self, io_ctx, reconfig);
              } else {
                // If there is not a `SlaveReconfigES`, this is a backup and we should make one.
                statuses
                  .slave_reconfig_ess
                  .insert(reconfig.sid.clone(), SlaveReconfigES::new_follower(self, reconfig));
              }
            }
            MasterPLm::SlaveGroupReconfiguredPLm(reconfigured) => {
              // Here, we remove the ES and then finish it off.
              let mut es = statuses.slave_reconfig_ess.remove(&reconfigured.sid).unwrap();
              es.handle_reconfigured_plm(self, io_ctx, reconfigured);
            }
          }
        }

        if self.is_leader() {
          // Run the Main Loop
          self.run_main_loop(io_ctx, statuses);

          // Inform all DDL ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.create_table_tm_ess {
            es.start_inserting(self, io_ctx);
          }
          for (_, es) in &mut statuses.alter_table_tm_ess {
            es.start_inserting(self, io_ctx);
          }
          for (_, es) in &mut statuses.drop_table_tm_ess {
            es.start_inserting(self, io_ctx);
          }

          // For every MasterQueryPlanningES, we add a PLm
          for (_, es) in &mut statuses.planning_ess {
            self.master_bundle.plms.push(MasterPLm::MasterQueryPlanning(
              plm::MasterQueryPlanning {
                query_id: es.query_id.clone(),
                timestamp: es.timestamp.clone(),
                ms_query: es.ms_query.clone(),
              },
            ));
          }

          // Construct PLms related to FreeNode management.
          let granted_reconfig_eids = self.free_node_manager.process(
            FreeNodeManagerContext {
              this_eid: &self.this_eid,
              leader_map: &self.leader_map,
              master_bundle: &mut self.master_bundle,
            },
            io_ctx,
          );

          // Forwarded the granted `EndpointId`s to the `SlaveGroupReconfig`s that requested it.
          // We might also be doing a Master Reconfig, record the new_eids for that.
          let mut do_reconfig: Option<(Vec<EndpointId>, Vec<EndpointId>)> = None;
          for (gid, new_eids) in granted_reconfig_eids {
            match gid {
              PaxosGroupId::Master => {
                let es = statuses.master_reconfig_es.as_mut().unwrap();
                do_reconfig = Some((es.rem_eids.clone(), new_eids.clone()));
                es.state = ReconfigState::InsertingReconfig { new_eids };
              }
              PaxosGroupId::Slave(sid) => {
                let es = statuses.slave_reconfig_ess.get_mut(&sid).unwrap();
                es.handle_eids_granted(self, new_eids);
              }
            }
          }

          // Continue the insert cycle. If the Master needs to reconfigure, we choose
          // `Reconfig`. Otherwise, we choose `Bundle`.
          let bundle = std::mem::replace(&mut self.master_bundle, MasterBundle::default());
          let user_entry = if let Some((rem_eids, new_eids)) = do_reconfig {
            UserPLEntry::Reconfig(msg::Reconfig { rem_eids, new_eids, bundle })
          } else {
            UserPLEntry::Bundle(bundle)
          };
          self.paxos_driver.insert_bundle(
            &mut MasterPaxosContext { io_ctx, this_eid: &self.this_eid },
            user_entry,
          );
        }
      }
      MasterForwardMsg::MasterExternalReq(message) => {
        match message {
          MasterExternalReq::PerformExternalDDLQuery(external_query) => {
            match self.validate_ddl_query(&external_query) {
              Ok(ddl_query) => {
                let query_id = mk_qid(io_ctx.rand());
                let sender_eid = external_query.sender_eid;
                let request_id = external_query.request_id;

                // Update the QueryId that's stored in the `external_request_id_map` and trace it.
                self.external_request_id_map.insert(request_id.clone(), query_id.clone());
                io_ctx.general_trace(GeneralTraceMessage::RequestIdQueryId(
                  request_id.clone(),
                  query_id.clone(),
                ));

                match ddl_query {
                  DDLQuery::Create(create_table) => {
                    // Generate random shards
                    let shards = self.simple_partition(io_ctx, &create_table);

                    // Construct ES
                    map_insert(
                      &mut statuses.create_table_tm_ess,
                      &query_id,
                      CreateTableTMES::new(
                        query_id.clone(),
                        CreateTableTMInner {
                          response_data: Some(ResponseData { request_id, sender_eid }),
                          table_path: create_table.table_path,
                          key_cols: create_table.key_cols,
                          val_cols: create_table.val_cols,
                          shards,
                          did_commit: false,
                        },
                      ),
                    );
                  }
                  DDLQuery::Alter(alter_table) => {
                    // Construct ES
                    map_insert(
                      &mut statuses.alter_table_tm_ess,
                      &query_id,
                      AlterTableTMES::new(
                        query_id.clone(),
                        AlterTableTMInner {
                          response_data: Some(ResponseData { request_id, sender_eid }),
                          table_path: alter_table.table_path,
                          alter_op: alter_table.alter_op,
                        },
                      ),
                    );
                  }
                  DDLQuery::Drop(drop_table) => {
                    // Construct ES
                    map_insert(
                      &mut statuses.drop_table_tm_ess,
                      &query_id,
                      DropTableTMES::new(
                        query_id.clone(),
                        DropTableTMInner {
                          response_data: Some(ResponseData { request_id, sender_eid }),
                          table_path: drop_table.table_path,
                        },
                      ),
                    );
                  }
                }
              }
              Err(payload) => {
                // We send the error back to the External.
                io_ctx.send(
                  &external_query.sender_eid,
                  msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
                    msg::ExternalDDLQueryAborted { request_id: external_query.request_id, payload },
                  )),
                );
              }
            }
          }
          MasterExternalReq::CancelExternalDDLQuery(cancel) => {
            if let Some(query_id) = self.external_request_id_map.remove(&cancel.request_id) {
              // Utility to send a `ConfirmCancelled` back to the External referred to by `cancel`.
              fn respond_cancelled<IO: MasterIOCtx>(
                io_ctx: &mut IO,
                cancel: msg::CancelExternalDDLQuery,
              ) {
                io_ctx.send(
                  &cancel.sender_eid,
                  msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
                    msg::ExternalDDLQueryAborted {
                      request_id: cancel.request_id,
                      payload: msg::ExternalDDLQueryAbortData::CancelConfirmed,
                    },
                  )),
                );
              }

              // If the query_id corresponds to an early CreateTable, then abort it.
              if let Some(es) = statuses.create_table_tm_ess.get(&query_id) {
                match &es.state {
                  State::Start | State::WaitingInsertTMPrepared => {
                    statuses.create_table_tm_ess.remove(&query_id);
                    respond_cancelled(io_ctx, cancel);
                  }
                  _ => {}
                }
              }
              // Otherwise, if the query_id corresponds to an early AlterTable, then abort it.
              else if let Some(es) = statuses.alter_table_tm_ess.get(&query_id) {
                match &es.state {
                  State::Start | State::WaitingInsertTMPrepared => {
                    statuses.alter_table_tm_ess.remove(&query_id);
                    respond_cancelled(io_ctx, cancel);
                  }
                  _ => {}
                }
              }
              // Otherwise, if the query_id corresponds to an early DropTable, then abort it.
              else if let Some(es) = statuses.drop_table_tm_ess.get(&query_id) {
                match &es.state {
                  State::Start | State::WaitingInsertTMPrepared => {
                    statuses.drop_table_tm_ess.remove(&query_id);
                    respond_cancelled(io_ctx, cancel);
                  }
                  _ => {}
                }
              }
            }
          }
        }

        // Run Main Loop
        self.run_main_loop(io_ctx, statuses);
      }
      MasterForwardMsg::MasterRemotePayload(payload) => {
        match payload {
          MasterRemotePayload::PerformMasterQueryPlanning(query_planning) => {
            let action = master_query_planning_pre(self, query_planning.clone());
            match action {
              MasterQueryPlanningAction::Wait => {
                map_insert(
                  &mut statuses.planning_ess,
                  &query_planning.query_id.clone(),
                  MasterQueryPlanningES {
                    sender_path: query_planning.sender_path,
                    query_id: query_planning.query_id,
                    timestamp: query_planning.timestamp,
                    ms_query: query_planning.ms_query,
                  },
                );
              }
              MasterQueryPlanningAction::Respond(result) => {
                self.ctx(io_ctx).send_to_c(
                  query_planning.sender_path.node_path,
                  msg::CoordMessage::MasterQueryPlanningSuccess(msg::MasterQueryPlanningSuccess {
                    return_qid: query_planning.sender_path.query_id,
                    query_id: query_planning.query_id,
                    result,
                  }),
                );
              }
            }
          }
          MasterRemotePayload::CancelMasterQueryPlanning(cancel_query_planning) => {
            statuses.planning_ess.remove(&cancel_query_planning.query_id);
          }
          // CreateTable
          MasterRemotePayload::CreateTable(message) => {
            let (query_id, action) =
              handle_tm_msg(self, io_ctx, &mut statuses.create_table_tm_ess, message);
            self.handle_create_table_es_action(statuses, query_id, action);
          }
          // AlterTable
          MasterRemotePayload::AlterTable(message) => {
            let (query_id, action) =
              handle_tm_msg(self, io_ctx, &mut statuses.alter_table_tm_ess, message);
            self.handle_alter_table_es_action(statuses, query_id, action);
          }
          // DropTable
          MasterRemotePayload::DropTable(message) => {
            let (query_id, action) =
              handle_tm_msg(self, io_ctx, &mut statuses.drop_table_tm_ess, message);
            self.handle_drop_table_es_action(statuses, query_id, action);
          }
          MasterRemotePayload::MasterGossipRequest(gossip_req) => {
            self.send_gossip(io_ctx, gossip_req.sender_path);
          }
          MasterRemotePayload::NodesDead(nodes_dead) => {
            // Construct an `SlaveReconfigES` if it does not already exist.
            let sid = nodes_dead.sid;
            if !statuses.slave_reconfig_ess.contains_key(&sid) {
              statuses
                .slave_reconfig_ess
                .insert(sid.clone(), SlaveReconfigES::new(self, sid, nodes_dead.eids));
            }
          }
          MasterRemotePayload::SlaveGroupReconfigured(reconfigured) => {
            if let Some(es) = statuses.slave_reconfig_ess.get_mut(&reconfigured.sid) {
              es.handle_reconfigured_msg(self, io_ctx, reconfigured);
            }
          }
        }

        // Run Main Loop
        self.run_main_loop(io_ctx, statuses);
      }
      MasterForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();
        if lid.gen > self.leader_map.get(&gid).unwrap().gen {
          // Only update the LeadershipId if the new one increases the old one.
          self.leader_map.insert(gid.clone(), lid.clone());

          // CreateTable
          let query_ids: Vec<QueryId> = statuses.create_table_tm_ess.keys().cloned().collect();
          for query_id in query_ids {
            let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
            self.handle_create_table_es_action(statuses, query_id, action);
          }

          // AlterTable
          let query_ids: Vec<QueryId> = statuses.alter_table_tm_ess.keys().cloned().collect();
          for query_id in query_ids {
            let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
            self.handle_alter_table_es_action(statuses, query_id, action);
          }

          // DropTable
          let query_ids: Vec<QueryId> = statuses.drop_table_tm_ess.keys().cloned().collect();
          for query_id in query_ids {
            let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
            self.handle_drop_table_es_action(statuses, query_id, action);
          }

          // SlaveReconfigES
          for (_, es) in &mut statuses.slave_reconfig_ess {
            es.remote_leader_changed(self, io_ctx, &remote_leader_changed);
          }

          // For MasterQueryPlanningESs, if the sending PaxosGroup's Leadership changed,
          // we ECU (no response).
          let query_ids: Vec<QueryId> = statuses.planning_ess.keys().cloned().collect();
          for query_id in query_ids {
            let es = statuses.planning_ess.get_mut(&query_id).unwrap();
            if es.sender_path.node_path.sid.to_gid() == gid {
              statuses.planning_ess.remove(&query_id);
            }
          }
        }
      }
      MasterForwardMsg::LeaderChanged(leader_changed) => {
        self.leader_map.insert(PaxosGroupId::Master, leader_changed.lid); // Update the LeadershipId

        if self.is_leader() {
          // By the SharedPaxosInserter, these must be empty at the start of Leadership.
          self.master_bundle = MasterBundle::default();
        }

        // CreateTable
        let query_ids: Vec<QueryId> = statuses.create_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self, io_ctx);
          self.handle_create_table_es_action(statuses, query_id, action);
        }

        // AlterTable
        let query_ids: Vec<QueryId> = statuses.alter_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self, io_ctx);
          self.handle_alter_table_es_action(statuses, query_id, action);
        }

        // DropTable
        let query_ids: Vec<QueryId> = statuses.drop_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self, io_ctx);
          self.handle_drop_table_es_action(statuses, query_id, action);
        }

        // SlaveGroupCreate
        for (_, es) in &mut statuses.slave_group_create_ess {
          es.leader_changed(self, io_ctx);
        }

        // SlaveReconfig
        let sids: Vec<SlaveGroupId> = statuses.slave_reconfig_ess.keys().cloned().collect();
        for sid in sids {
          let es = statuses.slave_reconfig_ess.get_mut(&sid).unwrap();
          if !es.leader_changed(self, io_ctx) {
            statuses.slave_reconfig_ess.remove(&sid);
          }
        }

        // MasterReconfig
        statuses.master_reconfig_es = None;

        // FreeNodeManager
        self.free_node_manager.leader_changed(FreeNodeManagerContext {
          this_eid: &self.this_eid,
          leader_map: &self.leader_map,
          master_bundle: &mut self.master_bundle,
        });

        // NetworkDriver
        self.network_driver.leader_changed();

        // Check if this node just lost Leadership
        if !self.is_leader() {
          // Wink away all MasterQueryPlanningESs.
          statuses.planning_ess.clear();
        } else {
          // TODO: should we be running the main loop here?
          // Run Main Loop
          self.run_main_loop(io_ctx, statuses);

          // This node is the new Leader
          self.broadcast_leadership(io_ctx); // Broadcast RemoteLeaderChanged

          // Start the insert cycle.
          self.paxos_driver.insert_bundle(
            &mut MasterPaxosContext { io_ctx, this_eid: &self.this_eid },
            UserPLEntry::Bundle(std::mem::replace(
              &mut self.master_bundle,
              MasterBundle::default(),
            )),
          );
        }
      }
    }
  }

  /// Validate the uniqueness of `RequestId` and return the parsed SQL, or an
  /// appropriate error if this fails.
  fn validate_ddl_query(
    &self,
    external_query: &msg::PerformExternalDDLQuery,
  ) -> Result<DDLQuery, msg::ExternalDDLQueryAbortData> {
    if self.external_request_id_map.contains_key(&external_query.request_id) {
      // Duplicate RequestId; respond with an abort.
      Err(msg::ExternalDDLQueryAbortData::NonUniqueRequestId)
    } else {
      // Parse the SQL
      match Parser::parse_sql(&GenericDialect {}, &external_query.query) {
        Ok(parsed_ast) => match convert_ddl_ast(parsed_ast) {
          Ok(ddl_ast) => Ok(ddl_ast),
          Err(parse_error) => Err(msg::ExternalDDLQueryAbortData::ParseError(parse_error)),
        },
        Err(parse_error) => {
          // Extract error string
          Err(msg::ExternalDDLQueryAbortData::ParseError(match parse_error {
            TokenizerError(err_msg) => err_msg,
            ParserError(err_msg) => err_msg,
          }))
        }
      }
    }
  }

  /// Uses a simple scheme to create an initial partition of the Table.
  /// NOTE: This is only meant to help test sharding while we lack automatic partitioning.
  fn simple_partition<IO: MasterIOCtx>(
    &self,
    io_ctx: &mut IO,
    create_table: &proc::CreateTable,
  ) -> Vec<(TabletKeyRange, TabletGroupId, SlaveGroupId)> {
    let mut sids = Vec::from_iter(self.gossip.get().slave_address_config.keys().into_iter());
    debug_assert!(!sids.is_empty());
    let mut shards = Vec::<(TabletKeyRange, TabletGroupId, SlaveGroupId)>::new();

    // For all remaining KeyCols, amend `pkey` with the minimum value of the corresponding type.
    fn mk_key(
      first_val: ColVal,
      mut rem_cols: core::slice::Iter<(ColName, ColType)>,
    ) -> PrimaryKey {
      let mut split_key = PrimaryKey { cols: vec![] };
      split_key.cols.push(first_val);
      while let Some((_, col_type)) = rem_cols.next() {
        match col_type {
          ColType::Int => split_key.cols.push(ColVal::Int(i32::MIN)),
          ColType::Bool => split_key.cols.push(ColVal::Bool(false)),
          ColType::String => split_key.cols.push(ColVal::String("".to_string())),
        }
      }
      split_key
    }

    // Removes one random element from `sids` (which the caller must ensure exists) and
    // constructs the output.
    fn mk_shard<IO: MasterIOCtx>(
      io_ctx: &mut IO,
      sids: &mut Vec<&SlaveGroupId>,
      start: Option<PrimaryKey>,
      end: Option<PrimaryKey>,
    ) -> (TabletKeyRange, TabletGroupId, SlaveGroupId) {
      let idx = io_ctx.rand().next_u32() as usize % sids.len();
      let sid = sids.remove(idx);
      (TabletKeyRange { start, end }, mk_tid(io_ctx.rand()), sid.clone())
    }

    // If the Table has no KeyCols (which means the Tablet can have only at-most one row),
    // we just create a trivial shard. Otherwise, we create non-trivial shards.
    if create_table.key_cols.is_empty() {
      // 1 shard
      shards.push(mk_shard(io_ctx, &mut sids, None, None));
    } else {
      // Get the first KeyCol
      let mut it = create_table.key_cols.iter();
      let (_, col_type) = it.next().unwrap();
      match col_type {
        ColType::Int => {
          // Decide if we want 1 shard or 2 shards. Make sure we do not choose more shards
          // than there are Slaves.
          let num_shards = min(io_ctx.rand().next_u32() % 2 + 1, sids.len() as u32);
          if num_shards == 1 {
            // 1 shard
            shards.push(mk_shard(io_ctx, &mut sids, None, None));
          } else {
            // 2 shards
            let split_key = mk_key(ColVal::Int(0), it);
            shards.push(mk_shard(io_ctx, &mut sids, None, Some(split_key.clone())));
            shards.push(mk_shard(io_ctx, &mut sids, Some(split_key.clone()), None));
          }
        }
        ColType::Bool => {
          // Decide if we want 1 shard or 2 shards. Make sure we do not choose more shards
          // than there are Slaves.
          let num_shards = min(io_ctx.rand().next_u32() % 2 + 1, sids.len() as u32);
          if num_shards == 1 {
            // 1 shard
            shards.push(mk_shard(io_ctx, &mut sids, None, None));
          } else {
            // 2 shards
            let split_key = mk_key(ColVal::Bool(true), it);
            shards.push(mk_shard(io_ctx, &mut sids, None, Some(split_key.clone())));
            shards.push(mk_shard(io_ctx, &mut sids, Some(split_key.clone()), None));
          }
        }
        ColType::String => {
          // Decide the number of shards, which is 1 <= and <= 4. Make sure we do not choose
          // more shards than there are Slaves.
          let num_shards = min(io_ctx.rand().next_u32() % 4 + 1, sids.len() as u32);

          // Create the splitting characters. These must be unique and sorted in ascending order.
          let mut all_shards = Vec::from_iter("abcdefghijklmnopqrstuvwxyz".to_string().chars());
          let mut split_chars = Vec::<char>::new();
          for _ in 0..(num_shards - 1) {
            split_chars
              .push(all_shards.remove(io_ctx.rand().next_u32() as usize % all_shards.len()));
          }
          split_chars.sort();

          // Construct the shards
          let mut char_it = split_chars.into_iter();
          if let Some(first_char) = char_it.next() {
            // Create the first partition
            let mut prev_split_key = mk_key(ColVal::String(first_char.to_string()), it.clone());
            shards.push(mk_shard(io_ctx, &mut sids, None, Some(prev_split_key.clone())));

            // Create middle partitions
            while let Some(next_char) = char_it.next() {
              let split_key = mk_key(ColVal::String(next_char.to_string()), it.clone());
              shards.push(mk_shard(
                io_ctx,
                &mut sids,
                Some(std::mem::replace(&mut prev_split_key, split_key.clone())),
                Some(split_key),
              ));
            }

            // Create final partition
            shards.push(mk_shard(io_ctx, &mut sids, Some(prev_split_key.clone()), None));
          } else {
            // 1 shard
            shards.push(mk_shard(io_ctx, &mut sids, None, None));
          }
        }
      };
    }

    return shards;
  }

  /// Used to broadcast out `RemoteLeaderChanged` to all other
  /// PaxosGroups to help maintain their LeaderMaps.
  fn broadcast_leadership<IO: BasicIOCtx<msg::NetworkMessage>>(&self, io_ctx: &mut IO) {
    let this_lid = self.leader_map.get(&PaxosGroupId::Master).unwrap().clone();
    for (_, eids) in self.gossip.get().slave_address_config {
      for eid in eids {
        io_ctx.send(
          eid,
          msg::NetworkMessage::Slave(msg::SlaveMessage::RemoteLeaderChangedGossip(
            msg::RemoteLeaderChangedGossip { gid: PaxosGroupId::Master, lid: this_lid.clone() },
          )),
        )
      }
    }
  }

  /// Runs the `run_main_loop_once` until it finally results in changes to the node's state.
  fn run_main_loop<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO, statuses: &mut Statuses) {
    while self.run_main_loop_once(io_ctx, statuses) {}
  }

  /// Thus runs one iteration of the Main Loop, returning `false` exactly when nothing changes.
  fn run_main_loop_once<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
  ) -> bool {
    self.advance_ddl_ess(io_ctx, statuses);
    false
  }

  /// This function advances the DDL ESs as far as possible. Properties:
  ///    1. Running this function again should not modify anything.
  fn advance_ddl_ess<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO, statuses: &mut Statuses) {
    let gossip = self.gossip.get();

    // First, we accumulate all TablePaths that currently have a a DDL Query running for them.
    let mut tables_being_modified = BTreeSet::<TablePath>::new();
    for (_, es) in &statuses.create_table_tm_ess {
      if let paxos2pc::State::Start = &es.state {
      } else {
        tables_being_modified.insert(es.inner.table_path.clone());
      }
    }

    for (_, es) in &statuses.alter_table_tm_ess {
      if let paxos2pc::State::Start = &es.state {
      } else {
        tables_being_modified.insert(es.inner.table_path.clone());
      }
    }

    for (_, es) in &statuses.drop_table_tm_ess {
      if let paxos2pc::State::Start = &es.state {
      } else {
        tables_being_modified.insert(es.inner.table_path.clone());
      }
    }

    // Move CreateTableESs forward for TablePaths not in `tables_being_modified`
    {
      let mut ess_to_remove = Vec::<QueryId>::new();
      for (_, es) in &mut statuses.create_table_tm_ess {
        if let paxos2pc::State::Start = &es.state {
          if !tables_being_modified.contains(&es.inner.table_path) {
            // Check Table Validity
            if gossip.table_generation.get_last_version(&es.inner.table_path).is_none() {
              // If the table does not exist, we move the ES to WaitingInsertTMPrepared.
              es.state = paxos2pc::State::WaitingInsertTMPrepared;
              tables_being_modified.insert(es.inner.table_path.clone());
              continue;
            }

            // Otherwise, we abort the CreateTableES.
            ess_to_remove.push(es.query_id.clone())
          }
        }
      }
      for query_id in ess_to_remove {
        let es = statuses.create_table_tm_ess.remove(&query_id).unwrap();
        if let Some(response_data) = &es.inner.response_data {
          respond_invalid_ddl(io_ctx, response_data);
        }
      }
    }

    // Move AlterTableESs forward for TablePaths not in `tables_being_modified`
    {
      let mut ess_to_remove = Vec::<QueryId>::new();
      for (_, es) in &mut statuses.alter_table_tm_ess {
        if let paxos2pc::State::Start = &es.state {
          if !tables_being_modified.contains(&es.inner.table_path) {
            // Check Column Validity (which is where the Table exists and the column exists
            // or does not exist, depending on if alter_op is a DROP COLUMN or ADD COLUMN).
            if let Some(gen) = gossip.table_generation.get_last_version(&es.inner.table_path) {
              // The Table Exists.
              let schema =
                &gossip.db_schema.get(&(es.inner.table_path.clone(), gen.clone())).unwrap();
              if lookup_pos(&schema.key_cols, &es.inner.alter_op.col_name).is_none() {
                // The `col_name` is not a KeyCol.
                let contains_col = contains_col_latest(schema, &es.inner.alter_op.col_name);
                let is_add_col = es.inner.alter_op.maybe_col_type.is_some();
                if contains_col && !is_add_col || !contains_col && is_add_col {
                  // We have Column Validity, so we move it to WaitingInsertTMPrepared.
                  es.state = paxos2pc::State::WaitingInsertTMPrepared;
                  tables_being_modified.insert(es.inner.table_path.clone());
                  continue;
                }
              }
            }

            // We do not have Column Validity, so we abort.
            ess_to_remove.push(es.query_id.clone())
          }
        }
      }
      for query_id in ess_to_remove {
        let es = statuses.alter_table_tm_ess.remove(&query_id).unwrap();
        if let Some(response_data) = &es.inner.response_data {
          respond_invalid_ddl(io_ctx, response_data);
        }
      }
    }

    // Move DropTableESs forward for TablePaths not in `tables_being_modified`
    {
      let mut ess_to_remove = Vec::<QueryId>::new();
      for (_, es) in &mut statuses.drop_table_tm_ess {
        if let paxos2pc::State::Start = &es.state {
          if !tables_being_modified.contains(&es.inner.table_path) {
            // Check Table Validity
            if gossip.table_generation.get_last_version(&es.inner.table_path).is_some() {
              // If the table exists, we move the ES to WaitingInsertTMPrepared.
              es.state = paxos2pc::State::WaitingInsertTMPrepared;
              tables_being_modified.insert(es.inner.table_path.clone());
              continue;
            }

            // Otherwise, we abort the DropTableES.
            ess_to_remove.push(es.query_id.clone())
          }
        }
      }
      for query_id in ess_to_remove {
        let es = statuses.drop_table_tm_ess.remove(&query_id).unwrap();
        if let Some(response_data) = &es.inner.response_data {
          respond_invalid_ddl(io_ctx, response_data);
        }
      }
    }
  }

  /// Handles the actions specified by a CreateTableES.
  fn handle_create_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: STMPaxos2PCTMAction,
  ) {
    match action {
      STMPaxos2PCTMAction::Wait => {}
      STMPaxos2PCTMAction::Exit => {
        statuses.create_table_tm_ess.remove(&query_id);
      }
    }
  }

  /// Handles the actions specified by a AlterTableES.
  fn handle_alter_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: STMPaxos2PCTMAction,
  ) {
    match action {
      STMPaxos2PCTMAction::Wait => {}
      STMPaxos2PCTMAction::Exit => {
        statuses.alter_table_tm_ess.remove(&query_id);
      }
    }
  }

  /// Handles the actions specified by a AlterTableES.
  fn handle_drop_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: STMPaxos2PCTMAction,
  ) {
    match action {
      STMPaxos2PCTMAction::Wait => {}
      STMPaxos2PCTMAction::Exit => {
        statuses.drop_table_tm_ess.remove(&query_id);
      }
    }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&PaxosGroupId::Master).unwrap();
    lid.eid == self.this_eid
  }

  /// Broadcast GossipData
  // TODO: broadcast to free nodes
  pub fn broadcast_gossip<IO: BasicIOCtx>(&mut self, io_ctx: &mut IO) {
    let sids: Vec<SlaveGroupId> = self.gossip.get().slave_address_config.keys().cloned().collect();
    for sid in sids {
      self.send_gossip(io_ctx, sid);
    }
  }

  /// Send GossipData
  pub fn send_gossip<IO: BasicIOCtx>(&mut self, io_ctx: &mut IO, sid: SlaveGroupId) {
    self.ctx(io_ctx).send_to_slave_common(
      sid,
      msg::SlaveRemotePayload::MasterGossip(msg::MasterGossip { gossip_data: self.gossip.clone() }),
    );
  }
}

/// Send `InvalidDDLQuery` to the given `ResponseData`
fn respond_invalid_ddl<IO: MasterIOCtx>(io_ctx: &mut IO, response_data: &ResponseData) {
  io_ctx.send(
    &response_data.sender_eid,
    msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
      msg::ExternalDDLQueryAborted {
        request_id: response_data.request_id.clone(),
        payload: ExternalDDLQueryAbortData::InvalidDDLQuery,
      },
    )),
  )
}
