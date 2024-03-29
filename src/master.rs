use crate::alter_table_tm_es::{
  AlterTableTMES, AlterTableTMInner, AlterTableTMPayloadTypes, ResponseData,
};
use crate::common::{
  lookup_pos, map_insert, mk_qid, mk_t, mk_tid, remove_item, update_all_eids, GeneralTraceMessage,
  GossipData, LeaderMap, MasterIOCtx, MasterTraceMessage, TableSchema, Timestamp, VersionedValue,
  ALPHABET,
};
use crate::common::{BasicIOCtx, RemoteLeaderChangedPLm};
use crate::common::{
  ColName, ColType, ColVal, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait,
  PrimaryKey, QueryId, RequestId, SlaveGroupId, TNodePath, TablePath, TabletGroupId,
  TabletKeyRange,
};
use crate::create_table_tm_es::{CreateTableTMES, CreateTableTMInner, CreateTableTMPayloadTypes};
use crate::drop_table_tm_es::{DropTableTMES, DropTableTMInner, DropTableTMPayloadTypes};
use crate::expression::does_types_match;
use crate::free_node_manager::{FreeNodeManager, FreeNodeManagerPLm, FreeNodeType};
use crate::master_query_planning_es::{MasterQueryPlanning, MasterQueryPlanningESS};
use crate::message as msg;
use crate::multiversion_map::MVM;
use crate::network_driver::{NetworkDriver, NetworkDriverContext};
use crate::paxos::{PaxosConfig, PaxosContextBase, PaxosDriver, PaxosTimerEvent, UserPLEntry};
use crate::server::{contains_col_latest, ServerContextBase};
use crate::shard_split_tm_es::{ShardSplitTMES, ShardSplitTMInner, ShardSplitTMPayloadTypes};
use crate::slave_group_create_es::{ConfirmCreateGroup, SlaveGroupCreateESS};
use crate::slave_reconfig_es::{SlaveReconfigESS, SlaveReconfigPLm};
use crate::sql_ast::proc;
use crate::sql_parser::{convert_ddl_ast, DDLQuery};
use crate::stmpaxos2pc_tm as paxos2pc;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::iter::FromIterator;

#[path = "test/master_test.rs"]
pub mod master_test;

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
  MasterQueryPlanning(MasterQueryPlanning),

  // DDL and Sharding STMPaxos2PC
  CreateTable(paxos2pc::TMPLm<CreateTableTMPayloadTypes>),
  AlterTable(paxos2pc::TMPLm<AlterTableTMPayloadTypes>),
  DropTable(paxos2pc::TMPLm<DropTableTMPayloadTypes>),
  ShardSplit(paxos2pc::TMPLm<ShardSplitTMPayloadTypes>),

  // FreeNode
  FreeNodeManagerPLm(FreeNodeManagerPLm),
  ConfirmCreateGroup(ConfirmCreateGroup),
  SlaveConfigPLm(SlaveReconfigPLm),
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
  /// This is used to periodically broadcast the `GossipData` to the Slave Leaderships. Most
  /// Slaves will get the latest `GossipData` this way. Nevertheless, some rare conditions can
  /// make this fail for a given Slave, at which point we need the `GeneralBroadcastGossip`.
  FastBroadcastGossip,
  /// A less frequent but more reliable way of broadcasting `GossipData`
  /// than `FastBroadcastGossip`.
  GeneralBroadcastGossip,
  /// A Time event to detect if the Master PaxosGroup has a failed node.
  PaxosGroupFailureDetector,
  /// A timer event to detect if there are any `unconfirmed_eids` in the PaxosDriver. We
  /// use this to start constructing a `MasterSnapshot` if there is.
  CheckUnconfirmedEids,
  /// A Timer event to increase the heartbeat for FreeNodes
  FreeNodeHeartbeatTimer,
}

pub enum FullMasterInput {
  MasterMessage(msg::MasterMessage),
  MasterTimerInput(MasterTimerInput),
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

  /// The size that PaxosGroups should be.
  pub slave_group_size: u32,

  /// Timer events
  pub remote_leader_changed_period_ms: u128,
  pub failure_detector_period_ms: u128,
  pub check_unconfirmed_eids_period_ms: u128,
  pub gossip_data_period_ms: u128,

  /// FreeNodeManager
  pub num_coords: u32,
  pub free_node_heartbeat_timer_ms: u128,
}

// -----------------------------------------------------------------------------------------------
//  MasterSnapshot
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MasterSnapshot {
  pub gossip: GossipData,
  pub leader_map: LeaderMap,
  pub free_nodes: BTreeMap<EndpointId, FreeNodeType>,
  pub paxos_driver_start: msg::StartNewNode<MasterBundle>,

  // Statuses
  /// If this is a Follower, we copy over the ESs in `Statuses` to the below. If this
  /// is the Leader, we compute the ESs that would result as a result of a Leadership
  /// change and populate the below.
  pub create_table_tm_ess: BTreeMap<QueryId, CreateTableTMES>,
  pub alter_table_tm_ess: BTreeMap<QueryId, AlterTableTMES>,
  pub drop_table_tm_ess: BTreeMap<QueryId, DropTableTMES>,
  pub shard_split_tm_ess: BTreeMap<QueryId, ShardSplitTMES>,

  // ESS
  pub slave_group_create_ess: SlaveGroupCreateESS,
  pub slave_reconfig_ess: SlaveReconfigESS,
}

// -----------------------------------------------------------------------------------------------
//  Server Context
// -----------------------------------------------------------------------------------------------

impl ServerContextBase for MasterContext {
  fn leader_map(&self) -> &LeaderMap {
    &self.leader_map.value()
  }

  fn this_gid(&self) -> &PaxosGroupId {
    &PaxosGroupId::Master
  }

  fn this_eid(&self) -> &EndpointId {
    &self.this_eid
  }
}

// -----------------------------------------------------------------------------------------------
//  Master State
// -----------------------------------------------------------------------------------------------

/// NOTE: When adding a new element here, amend the `MasterSnapshot` accordingly.
#[derive(Debug)]
pub struct Statuses {
  // DDL ES and Sharding
  pub create_table_tm_ess: BTreeMap<QueryId, CreateTableTMES>,
  pub alter_table_tm_ess: BTreeMap<QueryId, AlterTableTMES>,
  pub drop_table_tm_ess: BTreeMap<QueryId, DropTableTMES>,
  pub shard_split_tm_ess: BTreeMap<QueryId, ShardSplitTMES>,

  // ESS
  pub planning_ess: MasterQueryPlanningESS,
  pub slave_group_create_ess: SlaveGroupCreateESS,
  pub slave_reconfig_ess: SlaveReconfigESS,

  /// FreeNodeManager
  pub free_node_manager: FreeNodeManager,

  /// When a reconfig seems warranted, we set this to be present so that during the
  /// next bundle insertion, we make sure to insert a `ReconfigBundle`. We hold
  /// onto the removed `EndpointId`s to help do this. We continue to hold onto this
  /// until the `ReconfigBundle` is inserted, since if we do not, then
  /// `PaxosGroupFailureDetector` might re-create `do_reconfig` and
  /// request us to remove a node that already becomes removed.
  pub do_reconfig: Option<Vec<EndpointId>>,
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

  /// Between calls to `handle_input`, we should maintain the following properties:
  /// 1. `leader_map` should contain an entry for every `PaxosGroupId` in the
  ///     Current PaxosGroup View.
  /// 2. The Leaderships in `leader_map` should be in the Current PaxosGroup View.  
  /// 3. `all_eids` should contain all `EndpointId` in Current PaxosGroup View.
  ///
  /// See the out-of-line docs for the definition of Current PaxosGroup View. For the
  /// Master, this is the same as what is in `gossip`.

  /// Metadata that is Gossiped out to the Slaves
  pub gossip: GossipData,
  /// LeaderMap. We use a `VerionedValue` primarily for the `NetworkDriver`
  pub leader_map: VersionedValue<LeaderMap>,
  /// The set `EndpointId` that this Slave is accepting delivery of messages from.
  pub all_eids: VersionedValue<BTreeSet<EndpointId>>,

  /// NetworkDriver
  pub network_driver: NetworkDriver<msg::MasterRemotePayload>,

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
    MasterState {
      ctx,
      statuses: Statuses {
        create_table_tm_ess: Default::default(),
        alter_table_tm_ess: Default::default(),
        drop_table_tm_ess: Default::default(),
        shard_split_tm_ess: Default::default(),
        planning_ess: MasterQueryPlanningESS::new(),
        slave_group_create_ess: SlaveGroupCreateESS::new(),
        slave_reconfig_ess: SlaveReconfigESS::new(),
        free_node_manager: FreeNodeManager::new(),
        do_reconfig: None,
      },
    }
  }

  /// Handles a `MasterSnapshot` to initiate a reconfigured `MasterState` properly.
  pub fn create_reconfig<IO: MasterIOCtx>(
    io_ctx: &mut IO,
    snapshot: MasterSnapshot,
    master_config: MasterConfig,
    paxos_config: PaxosConfig,
    this_eid: EndpointId,
  ) -> MasterState {
    // Create Statuses
    let statuses = Statuses {
      create_table_tm_ess: snapshot.create_table_tm_ess,
      alter_table_tm_ess: snapshot.alter_table_tm_ess,
      drop_table_tm_ess: snapshot.drop_table_tm_ess,
      shard_split_tm_ess: snapshot.shard_split_tm_ess,
      planning_ess: MasterQueryPlanningESS::new(),
      slave_group_create_ess: snapshot.slave_group_create_ess,
      slave_reconfig_ess: snapshot.slave_reconfig_ess,
      free_node_manager: FreeNodeManager::create_reconfig(snapshot.free_nodes),
      do_reconfig: None,
    };

    // Create the MasterCtx
    let leader_map = VersionedValue::new(snapshot.leader_map);

    // Recall that here, `paxos_nodes` for this group what is in `gossip`.
    let mut all_eids = BTreeSet::<EndpointId>::new();
    for (_, eids) in snapshot.gossip.get().slave_address_config {
      all_eids.extend(eids.clone());
    }
    all_eids.extend(snapshot.gossip.get().master_address_config.clone());

    let network_driver = NetworkDriver::new(&leader_map);
    let ctx = MasterContext {
      master_config,
      this_eid: this_eid.clone(),
      gossip: snapshot.gossip,
      leader_map,
      all_eids: VersionedValue::new(all_eids),
      network_driver,
      external_request_id_map: Default::default(),
      master_bundle: Default::default(),
      paxos_driver: PaxosDriver::create_reconfig(
        &mut MasterPaxosContext { io_ctx, this_eid: &this_eid },
        snapshot.paxos_driver_start,
        paxos_config,
      ),
    };

    MasterState { ctx, statuses }
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
      MasterTimerInput::FastBroadcastGossip,
      MasterTimerInput::GeneralBroadcastGossip,
      MasterTimerInput::PaxosGroupFailureDetector,
      MasterTimerInput::CheckUnconfirmedEids,
      MasterTimerInput::FreeNodeHeartbeatTimer,
    ] {
      self.ctx.handle_input(io_ctx, &mut self.statuses, MasterForwardMsg::MasterTimerInput(event));
    }

    // Trace the creation for testing
    let lid = LeadershipId::mk_first(self.ctx.paxos_driver.paxos_nodes().get(0).unwrap().clone());
    io_ctx.trace(MasterTraceMessage::MasterCreation(lid));
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

  pub fn get_eids(&self) -> &VersionedValue<BTreeSet<EndpointId>> {
    &self.ctx.all_eids
  }
}

impl MasterContext {
  /// Constructs a `MasterContext` for a node in the initial PaxosGroup.
  pub fn create_initial(
    master_address_config: Vec<EndpointId>,
    leader_map: LeaderMap,
    this_eid: EndpointId,
    master_config: MasterConfig,
    paxos_config: PaxosConfig,
  ) -> MasterContext {
    let leader_map = VersionedValue::new(leader_map);

    // Recall that here, `paxos_nodes` for this group what is in `gossip`.
    let mut all_eids = BTreeSet::<EndpointId>::new();
    all_eids.extend(master_address_config.clone());

    let network_driver = NetworkDriver::new(&leader_map);
    MasterContext {
      master_config,
      this_eid,
      gossip: GossipData::new(BTreeMap::default(), master_address_config.clone()),
      leader_map,
      all_eids: VersionedValue::new(all_eids),
      network_driver,
      external_request_id_map: Default::default(),
      master_bundle: MasterBundle::default(),
      paxos_driver: PaxosDriver::create_initial(master_address_config, paxos_config),
    }
  }

  pub fn handle_incoming_message<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    message: msg::MasterMessage,
  ) {
    match message {
      msg::MasterMessage::MasterExternalReq(request) => {
        if self.is_leader() {
          self.handle_input(io_ctx, statuses, MasterForwardMsg::MasterExternalReq(request))
        }
      }
      msg::MasterMessage::RemoteMessage(remote_message) => {
        if self.is_leader() {
          // Pass the message through the NetworkDriver

          // To prove the precondition of `receive`, recall that either a `NetworkMessage` is
          // a `MasterMessage::is_tier_1` message or not. If it is, notice that none of those
          // messages will hit this branch, thus sustaining the property. Otherwise,
          // recall that the `NodeState` only passes through a message from the Current Paxos
          // View, thus sustaining the property.
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
      msg::MasterMessage::RemoteLeaderChangedGossip(msg::RemoteLeaderChangedGossip {
        gid,
        lid,
      }) => {
        if self.is_leader() {
          // We filter out messages for PaxosGroupIds urecongnized by this node.
          if let Some(cur_lid) = self.leader_map.value().get(&gid) {
            // If the incoming RemoteLeaderChanged would increase the generation
            // in LeaderMap, then persist it.
            if &lid.gen > &cur_lid.gen {
              self.master_bundle.remote_leader_changes.push(RemoteLeaderChangedPLm { gid, lid })
            }
          }
        }
      }
      msg::MasterMessage::FreeNodeAssoc(free_node_msg) => {
        if self.is_leader() {
          match free_node_msg {
            msg::FreeNodeAssoc::RegisterFreeNode(register) => {
              statuses.free_node_manager.handle_register(register);
            }
            msg::FreeNodeAssoc::FreeNodeHeartbeat(heartbeat) => {
              statuses.free_node_manager.handle_heartbeat(self, heartbeat);
            }
            msg::FreeNodeAssoc::ConfirmSlaveCreation(confirm_msg) => {
              statuses.slave_group_create_ess.handle_msg(self, io_ctx, confirm_msg);
            }
          }
        }
      }
      msg::MasterMessage::PaxosDriverMessage(paxos_message) => {
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
              // Process the persisted data only first.
              let remote_leader_changed = master_bundle.remote_leader_changes.clone();
              self.process_bundle(io_ctx, statuses, master_bundle);
              // Then, deliver any messages that were blocked.
              self.deliver_blocked_messages(io_ctx, statuses, remote_leader_changed);
            }
            msg::PLEntry::ReconfigBundle(reconfig) => {
              // See if this node was kicked out of the PaxosGroup (by the
              // current Leader), in which case we would exit before returning.
              if reconfig.rem_eids.contains(&self.this_eid) {
                io_ctx.mark_exit();
                return;
              } else {
                // Reset the Reconfig ES.
                statuses.do_reconfig = None;

                // Process the persisted data only first.
                let remote_leader_changed = reconfig.bundle.remote_leader_changes.clone();
                self.process_bundle(io_ctx, statuses, reconfig.bundle);

                // Update GossipData.
                let rem_eids = reconfig.rem_eids;
                let new_eids = reconfig.new_eids;
                self.gossip.update(|gossip| {
                  // `master_address_config` should become equal to `paxos_driver.paxos_nodes`.
                  for rem_eid in &rem_eids {
                    remove_item(gossip.master_address_config, rem_eid);
                  }
                  for new_eid in &new_eids {
                    gossip.master_address_config.push(new_eid.clone());
                  }
                });

                // Update the `all_eids`
                update_all_eids(&mut self.all_eids, &rem_eids, new_eids.clone());

                // If this is the leader, we send a MasterSnapshot.
                if self.is_leader() {
                  self.start_snapshot(io_ctx, statuses);
                }

                // Trace the reconfig event.
                io_ctx.general_trace(GeneralTraceMessage::Reconfig(PaxosGroupId::Master, new_eids));

                // Then, deliver any messages that were blocked.
                self.deliver_blocked_messages(io_ctx, statuses, remote_leader_changed);
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
  }

  /// Delivery any messages in the `network_driver` that were blocked by not having
  /// a high enough leadership.
  pub fn deliver_blocked_messages<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    remote_leader_changes: Vec<RemoteLeaderChangedPLm>,
  ) {
    // Dispatch any messages that were buffered in the NetworkDriver.
    // Note: we must do this after RemoteLeaderChanges have been executed. Also note
    // that there will be no payloads in the NetworkBuffer if this nodes is a Follower.
    for remote_change in remote_leader_changes {
      if remote_change.lid.gen == self.leader_map.value().get(&remote_change.gid).unwrap().gen {
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
            self.broadcast_leadership(io_ctx, statuses);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.master_config.remote_leader_changed_period_ms);
          io_ctx.defer(defer_time, MasterTimerInput::RemoteLeaderChanged);
        }
        MasterTimerInput::FastBroadcastGossip => {
          if self.is_leader() {
            // If this node is the Leader, then send out GossipData to all Slaves Leaderships.
            self.broadcast_gossip(io_ctx);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.master_config.gossip_data_period_ms);
          io_ctx.defer(defer_time, MasterTimerInput::FastBroadcastGossip);
        }
        MasterTimerInput::GeneralBroadcastGossip => {
          if self.is_leader() {
            // If this node is the Leader, then send out GossipData to all Slaves Nodes.
            self.general_broadcast_gossip(io_ctx);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.master_config.gossip_data_period_ms * 100);
          io_ctx.defer(defer_time, MasterTimerInput::GeneralBroadcastGossip);
        }
        MasterTimerInput::PaxosGroupFailureDetector => {
          if self.is_leader() {
            // Only do PaxosGroup failure detection if we are not already trying to reconfigure.
            if statuses.do_reconfig.is_none() {
              let mut maybe_dead_eids = self.paxos_driver.get_maybe_dead(&self.this_eid);
              if !maybe_dead_eids.is_empty() {
                // Recall that the above returns only as much as we are capable of reconfiguring.
                statuses
                  .free_node_manager
                  .request_new_eids(PaxosGroupId::Master, maybe_dead_eids.len());
                statuses.do_reconfig = Some(maybe_dead_eids);
              }
            }
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.master_config.failure_detector_period_ms);
          io_ctx.defer(defer_time, MasterTimerInput::PaxosGroupFailureDetector);
        }
        MasterTimerInput::CheckUnconfirmedEids => {
          // We do this for both the Leader and Followers. If there are `unconfirmed_eids` in
          // the PaxosDriver, then we attempt to send a `MasterSnapshot`.
          if self.paxos_driver.has_unsent_unconfirmed() {
            self.start_snapshot(io_ctx, statuses);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.master_config.check_unconfirmed_eids_period_ms);
          io_ctx.defer(defer_time, MasterTimerInput::CheckUnconfirmedEids);
        }
        MasterTimerInput::FreeNodeHeartbeatTimer => {
          if self.is_leader() {
            statuses.free_node_manager.handle_timer(self);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.master_config.failure_detector_period_ms);
          io_ctx.defer(defer_time, MasterTimerInput::FreeNodeHeartbeatTimer);
        }
      },
      MasterForwardMsg::MasterBundle(bundle) => {
        for paxos_log_msg in bundle {
          match paxos_log_msg {
            // MasterQueryPlanning
            MasterPLm::MasterQueryPlanning(plm) => {
              statuses.planning_ess.handle_plm(self, io_ctx, plm);
            }
            // CreateTable
            MasterPLm::CreateTable(plm) => {
              paxos2pc::handle_plm(self, io_ctx, &mut statuses.create_table_tm_ess, plm);
            }
            // AlterTable
            MasterPLm::AlterTable(plm) => {
              paxos2pc::handle_plm(self, io_ctx, &mut statuses.alter_table_tm_ess, plm);
            }
            // DropTable
            MasterPLm::DropTable(plm) => {
              paxos2pc::handle_plm(self, io_ctx, &mut statuses.drop_table_tm_ess, plm);
            }
            // ShardSplit
            MasterPLm::ShardSplit(plm) => {
              paxos2pc::handle_plm(self, io_ctx, &mut statuses.shard_split_tm_ess, plm);
            }
            // FreeNode PLms
            MasterPLm::FreeNodeManagerPLm(plm) => {
              let new_slave_groups = statuses.free_node_manager.handle_plm(self, io_ctx, plm);
              statuses.slave_group_create_ess.handle_new_slaves(self, io_ctx, new_slave_groups);
            }
            MasterPLm::ConfirmCreateGroup(confirm_create) => {
              statuses.slave_group_create_ess.handle_plm(self, io_ctx, confirm_create);
            }
            MasterPLm::SlaveConfigPLm(plm) => {
              statuses.slave_reconfig_ess.handle_plm(self, io_ctx, plm);
            }
          }
        }

        if self.is_leader() {
          // Run the Main Loop
          self.run_main_loop(io_ctx, statuses);

          // CreateTable
          paxos2pc::handle_bundle_processed(self, io_ctx, &mut statuses.create_table_tm_ess);
          // AlterTable
          paxos2pc::handle_bundle_processed(self, io_ctx, &mut statuses.alter_table_tm_ess);
          // DropTable
          paxos2pc::handle_bundle_processed(self, io_ctx, &mut statuses.drop_table_tm_ess);
          // ShardSplit
          paxos2pc::handle_bundle_processed(self, io_ctx, &mut statuses.shard_split_tm_ess);
          // MasterQueryPlanningES
          statuses.planning_ess.handle_bundle_processed(self);

          // Construct PLms related to FreeNode management.
          let granted_reconfig_eids = statuses.free_node_manager.process(self, io_ctx);

          // Forwarded the granted `EndpointId`s to the `SlaveGroupReconfig`s that requested it.
          // We might also be doing a Master Reconfig, record the new_eids for that.
          let mut do_reconfig: Option<(Vec<EndpointId>, Vec<EndpointId>)> = None;
          for (gid, new_eids) in granted_reconfig_eids {
            match gid {
              PaxosGroupId::Master => {
                let rem_eids = statuses.do_reconfig.clone().unwrap();
                do_reconfig = Some((rem_eids, new_eids));
              }
              PaxosGroupId::Slave(sid) => {
                statuses.slave_reconfig_ess.handle_eids_granted(self, &sid, new_eids);
              }
            }
          }

          // Continue the insert cycle. If the Master needs to reconfigure, we choose
          // `Reconfig`. Otherwise, we choose `Bundle`.
          let bundle = std::mem::replace(&mut self.master_bundle, MasterBundle::default());
          let user_entry = if let Some((rem_eids, new_eids)) = do_reconfig {
            UserPLEntry::ReconfigBundle(msg::ReconfigBundle { rem_eids, new_eids, bundle })
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
          msg::MasterExternalReq::PerformExternalDDLQuery(external_query) => {
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
          msg::MasterExternalReq::CancelExternalDDLQuery(cancel) => {
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
                  paxos2pc::State::Start | paxos2pc::State::WaitingInsertTMPrepared => {
                    statuses.create_table_tm_ess.remove(&query_id);
                    respond_cancelled(io_ctx, cancel);
                  }
                  _ => {}
                }
              }
              // Otherwise, if the query_id corresponds to an early AlterTable, then abort it.
              else if let Some(es) = statuses.alter_table_tm_ess.get(&query_id) {
                match &es.state {
                  paxos2pc::State::Start | paxos2pc::State::WaitingInsertTMPrepared => {
                    statuses.alter_table_tm_ess.remove(&query_id);
                    respond_cancelled(io_ctx, cancel);
                  }
                  _ => {}
                }
              }
              // Otherwise, if the query_id corresponds to an early DropTable, then abort it.
              else if let Some(es) = statuses.drop_table_tm_ess.get(&query_id) {
                match &es.state {
                  paxos2pc::State::Start | paxos2pc::State::WaitingInsertTMPrepared => {
                    statuses.drop_table_tm_ess.remove(&query_id);
                    respond_cancelled(io_ctx, cancel);
                  }
                  _ => {}
                }
              }
            }
          }
          msg::MasterExternalReq::PerformExternalSharding(external_query) => {
            let query_id = mk_qid(io_ctx.rand());
            let sender_eid = external_query.sender_eid;
            let request_id = external_query.request_id;

            // Update the QueryId that's stored in the `external_request_id_map` and trace it.
            self.external_request_id_map.insert(request_id.clone(), query_id.clone());
            io_ctx.general_trace(GeneralTraceMessage::RequestIdQueryId(
              request_id.clone(),
              query_id.clone(),
            ));

            // For each type of `ShardingOp`, simply construct the corresponding
            // ES. The Main Loop will validate them, abort them, and start them.
            match external_query.op {
              msg::ShardingOp::Split(split) => {
                map_insert(
                  &mut statuses.shard_split_tm_ess,
                  &query_id,
                  ShardSplitTMES::new(
                    query_id.clone(),
                    ShardSplitTMInner {
                      response_data: Some(ResponseData { request_id, sender_eid }),
                      table_path: split.table_path,
                      target_old: split.target_old,
                      target_new: split.target_new,
                      did_commit: false,
                    },
                  ),
                );
              }
            }
          }
          msg::MasterExternalReq::CancelExternalSharding(cancel) => {
            if let Some(query_id) = self.external_request_id_map.remove(&cancel.request_id) {
              // Utility to send a `ConfirmCancelled` back to the External referred to by `cancel`.
              fn respond_cancelled<IO: MasterIOCtx>(
                io_ctx: &mut IO,
                cancel: msg::CancelExternalSharding,
              ) {
                io_ctx.send(
                  &cancel.sender_eid,
                  msg::NetworkMessage::External(msg::ExternalMessage::ExternalShardingAborted(
                    msg::ExternalShardingAborted {
                      request_id: cancel.request_id,
                      payload: msg::ExternalShardingAbortData::NonUniqueRequestId,
                    },
                  )),
                );
              }

              // If the query_id corresponds to an early ShardSplit, then abort it.
              if let Some(es) = statuses.shard_split_tm_ess.get(&query_id) {
                match &es.state {
                  paxos2pc::State::Start | paxos2pc::State::WaitingInsertTMPrepared => {
                    statuses.shard_split_tm_ess.remove(&query_id);
                    respond_cancelled(io_ctx, cancel);
                  }
                  _ => {}
                }
              }
            }
          }
          msg::MasterExternalReq::ExternalMetadataRequest(request) => {
            // Send back metadata.
            io_ctx.send(
              &request.sender_eid.clone(),
              msg::NetworkMessage::External(msg::ExternalMessage::ExternalMetadataResponse(
                msg::ExternalMetadataResponse {
                  sender_eid: request.sender_eid,
                  request_id: request.request_id,
                  gossip_data: self.gossip.clone(),
                  leader_map: self.leader_map.value().clone(),
                  free_node_eids: statuses.free_node_manager.free_node_eids(),
                },
              )),
            );
          }
        }

        // Run Main Loop
        self.run_main_loop(io_ctx, statuses);
      }
      MasterForwardMsg::MasterRemotePayload(payload) => {
        match payload {
          // MasterQueryPlanning
          msg::MasterRemotePayload::MasterQueryPlanning(request) => {
            statuses.planning_ess.handle_msg(self, io_ctx, request);
          }
          // CreateTable
          msg::MasterRemotePayload::CreateTable(message) => {
            paxos2pc::handle_msg(self, io_ctx, &mut statuses.create_table_tm_ess, message);
          }
          // AlterTable
          msg::MasterRemotePayload::AlterTable(message) => {
            paxos2pc::handle_msg(self, io_ctx, &mut statuses.alter_table_tm_ess, message);
          }
          // DropTable
          msg::MasterRemotePayload::DropTable(message) => {
            paxos2pc::handle_msg(self, io_ctx, &mut statuses.drop_table_tm_ess, message);
          }
          // ShardSplit
          msg::MasterRemotePayload::ShardSplit(message) => {
            paxos2pc::handle_msg(self, io_ctx, &mut statuses.shard_split_tm_ess, message);
          }
          // MasterGossipRequest
          msg::MasterRemotePayload::MasterGossipRequest(gossip_req) => {
            self.send_gossip(io_ctx, gossip_req.sender_path);
          }
          // SlaveReconfig
          msg::MasterRemotePayload::SlaveReconfig(reconfig) => {
            statuses.slave_reconfig_ess.handle_msg(
              self,
              io_ctx,
              &mut statuses.free_node_manager,
              reconfig,
            );
          }
        }

        // Run Main Loop
        self.run_main_loop(io_ctx, statuses);
      }
      MasterForwardMsg::RemoteLeaderChanged(rlc) => {
        let gid = rlc.gid.clone();
        let lid = rlc.lid.clone();

        // Only accept a RemoteLeaderChanged if the PaxosGroupId is already known.
        if let Some(cur_lid) = self.leader_map.value().get(&gid) {
          // Only update the LeadershipId if the new one increases the old one.
          if lid.gen > cur_lid.gen {
            self.leader_map.update(|leader_map| {
              leader_map.insert(gid.clone(), lid.clone());
            });

            // CreateTable
            paxos2pc::handle_rlc(self, io_ctx, &mut statuses.create_table_tm_ess, rlc.clone());
            // AlterTable
            paxos2pc::handle_rlc(self, io_ctx, &mut statuses.alter_table_tm_ess, rlc.clone());
            // DropTable
            paxos2pc::handle_rlc(self, io_ctx, &mut statuses.drop_table_tm_ess, rlc.clone());
            // ShardSplit
            paxos2pc::handle_rlc(self, io_ctx, &mut statuses.shard_split_tm_ess, rlc.clone());
            // SlaveReconfigES
            statuses.slave_reconfig_ess.handle_rlc(self, io_ctx, rlc.clone());
            // MasterQueryPlanningES
            statuses.planning_ess.handle_rlc(rlc.clone());
          }
        }
      }
      MasterForwardMsg::LeaderChanged(leader_changed) => {
        self.leader_map.update(move |leader_map| {
          leader_map.insert(PaxosGroupId::Master, leader_changed.lid);
        });

        if self.is_leader() {
          // By the SharedPaxosInserter, these must be empty at the start of Leadership.
          self.master_bundle = MasterBundle::default();
        }

        // CreateTable
        paxos2pc::handle_lc(self, io_ctx, &mut statuses.create_table_tm_ess);
        // AlterTable
        paxos2pc::handle_lc(self, io_ctx, &mut statuses.alter_table_tm_ess);
        // DropTable
        paxos2pc::handle_lc(self, io_ctx, &mut statuses.drop_table_tm_ess);
        // ShardSplit
        paxos2pc::handle_lc(self, io_ctx, &mut statuses.shard_split_tm_ess);
        // SlaveGroupCreate
        statuses.slave_group_create_ess.handle_lc(self, io_ctx);
        // SlaveReconfig
        statuses.slave_reconfig_ess.handle_lc(self, io_ctx);
        // MasterQueryPlanningES
        statuses.planning_ess.handle_lc(self);

        // MasterReconfig
        statuses.do_reconfig = None;

        // FreeNodeManager
        statuses.free_node_manager.leader_changed(self);

        // NetworkDriver
        self.network_driver.leader_changed();

        // Check if this node just lost Leadership
        if !self.is_leader() {
          // Wink away all RequestIds
          self.external_request_id_map.clear();
        } else {
          // TODO: should we be running the main loop here?
          // Run Main Loop
          self.run_main_loop(io_ctx, statuses);

          // This node is the new Leader
          self.broadcast_leadership(io_ctx, statuses); // Broadcast RemoteLeaderChanged

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

    // Removes one random element from `sids` (which the caller must ensure exists) and
    // constructs the output.
    fn mk_shard<IO: MasterIOCtx>(
      io_ctx: &mut IO,
      sids: &mut Vec<&SlaveGroupId>,
      start: Option<ColVal>,
      end: Option<ColVal>,
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
      let (_, col_type) = create_table.key_cols.iter().next().unwrap();
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
            let split_key = ColVal::Int(0);
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
            let split_key = ColVal::Bool(true);
            shards.push(mk_shard(io_ctx, &mut sids, None, Some(split_key.clone())));
            shards.push(mk_shard(io_ctx, &mut sids, Some(split_key.clone()), None));
          }
        }
        ColType::String => {
          // Decide the number of shards, which is 1 <= and <= 4. Make sure we do not choose
          // more shards than there are Slaves.
          let num_shards = min(io_ctx.rand().next_u32() % 4 + 1, sids.len() as u32);

          // Create the splitting characters. These must be unique and sorted in ascending order.
          let mut all_shards = Vec::from_iter(ALPHABET.to_string().chars());
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
            let mut prev_split_key = ColVal::String(first_char.to_string());
            shards.push(mk_shard(io_ctx, &mut sids, None, Some(prev_split_key.clone())));

            // Create middle partitions
            while let Some(next_char) = char_it.next() {
              let split_key = ColVal::String(next_char.to_string());
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
  fn broadcast_leadership<IO: BasicIOCtx<msg::NetworkMessage>>(
    &self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
  ) {
    let this_lid = self.leader_map.value().get(&PaxosGroupId::Master).unwrap().clone();

    // Send to SlaveGroups
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

    // Send to FreeNodes
    for (eid, _) in statuses.free_node_manager.free_nodes() {
      io_ctx.send(
        eid,
        msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::MasterLeadershipId(this_lid.clone())),
      )
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
    self.advance_ddl_sharding_ess(io_ctx, statuses);
    false
  }

  /// This function advances the DDL and Sharding ESs as far as possible. Properties:
  ///    1. Running this function again should not modify anything.
  fn advance_ddl_sharding_ess<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
  ) {
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

    for (_, es) in &statuses.shard_split_tm_ess {
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
            if self.gossip.get().table_generation.get_last_version(&es.inner.table_path).is_none() {
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
          self.respond_invalid_ddl(io_ctx, response_data);
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
            let gossip = self.gossip.get();
            if let Some((gen, _)) = gossip.table_generation.get_last_version(&es.inner.table_path) {
              // The Table Exists.
              let schema =
                gossip.db_schema.get(&(es.inner.table_path.clone(), gen.clone())).unwrap();
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
          self.respond_invalid_ddl(io_ctx, response_data);
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
            if self.gossip.get().table_generation.get_last_version(&es.inner.table_path).is_some() {
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
          self.respond_invalid_ddl(io_ctx, response_data);
        }
      }
    }

    // Move `ShardSplit`s forward for `TablePath`s not in `tables_being_modified`
    {
      let mut ess_to_remove = Vec::<QueryId>::new();
      for (_, es) in &mut statuses.shard_split_tm_ess {
        if let paxos2pc::State::Start = &es.state {
          if !tables_being_modified.contains(&es.inner.table_path) {
            // Check that the Table Exists
            let gossip = self.gossip.get();
            if let Some(full_gen) = gossip.table_generation.get_last_version(&es.inner.table_path) {
              // Check that the old TabletGroupId is present
              let tablet_path_full_gen = (es.inner.table_path.clone(), full_gen.clone());
              let shards = gossip.sharding_config.get(&tablet_path_full_gen).unwrap();

              // See if the `es.inner.old` TabletGroupId is a part of `es.inner.table_path`.
              if let Some((orig_range, _)) =
                shards.iter().find(|(_, t)| t == &es.inner.target_old.tid)
              {
                // Check that the SlaveGroupId of `es.inner.new` exists, but not the TabletGroupId
                if gossip.slave_address_config.contains_key(&es.inner.target_new.sid)
                  && !gossip.tablet_address_config.contains_key(&es.inner.target_new.tid)
                {
                  // Check that the `orig_range` is split perfectly.
                  if orig_range.start == es.inner.target_old.range.start
                    && es.inner.target_old.range.end == es.inner.target_new.range.start
                    && es.inner.target_new.range.end == orig_range.end
                  {
                    // Check that the split point is not `None` and that it is contained
                    // within the original range.
                    let split_key = &es.inner.target_old.range.end;
                    let (gen, _) = full_gen;
                    let db_schema =
                      gossip.db_schema.get(&(es.inner.table_path.clone(), gen.clone())).unwrap();
                    if db_schema.is_valid_range_key(split_key)
                      && split_key.is_some()
                      && orig_range.contains(split_key.as_ref().unwrap())
                    {
                      // Start the ES.
                      es.state = paxos2pc::State::WaitingInsertTMPrepared;
                      tables_being_modified.insert(es.inner.table_path.clone());
                      continue;
                    }
                  }
                }
              }
            }

            // Otherwise, we abort the Sharding ES because of the checks above failed.
            ess_to_remove.push(es.query_id.clone())
          }
        }
      }
      for query_id in ess_to_remove {
        let es = statuses.shard_split_tm_ess.remove(&query_id).unwrap();
        if let Some(response_data) = &es.inner.response_data {
          self.respond_invalid_sharding(io_ctx, response_data);
        }
      }
    }
  }

  /// Creates and sends a `MasterSnapshot` to all `unconfirmed_eids` that map to `false.
  fn start_snapshot<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO, statuses: &Statuses) {
    // Next, send the new `EndpointId`s a MasterSnapshot so that they can start up.
    let (paxos_driver_start, non_started_eids) =
      self.paxos_driver.mk_start_new_node(&MasterPaxosContext { io_ctx, this_eid: &self.this_eid });

    // Construct the snapshot
    let mut snapshot = MasterSnapshot {
      gossip: self.gossip.clone(),
      leader_map: self.leader_map.value().clone(),
      free_nodes: statuses.free_node_manager.free_nodes().clone(),
      paxos_driver_start,
      create_table_tm_ess: paxos2pc::handle_reconfig_snapshot(&statuses.create_table_tm_ess),
      alter_table_tm_ess: paxos2pc::handle_reconfig_snapshot(&statuses.alter_table_tm_ess),
      drop_table_tm_ess: paxos2pc::handle_reconfig_snapshot(&statuses.drop_table_tm_ess),
      shard_split_tm_ess: paxos2pc::handle_reconfig_snapshot(&statuses.shard_split_tm_ess),
      slave_group_create_ess: statuses.slave_group_create_ess.handle_reconfig_snapshot(),
      slave_reconfig_ess: statuses.slave_reconfig_ess.handle_reconfig_snapshot(),
    };

    // Send the Snapshot.
    for new_eid in &non_started_eids {
      io_ctx.send(
        new_eid,
        msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::MasterSnapshot(snapshot.clone())),
      )
    }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.value().get(&PaxosGroupId::Master).unwrap();
    lid.eid == self.this_eid
  }

  /// Broadcast GossipData
  pub fn broadcast_gossip<IO: BasicIOCtx>(&mut self, io_ctx: &mut IO) {
    let sids: Vec<SlaveGroupId> = self.gossip.get().slave_address_config.keys().cloned().collect();
    for sid in sids {
      self.send_gossip(io_ctx, sid);
    }
  }

  /// Broadcast out `MasterGossip` to *all* Slave Nodes.
  pub fn general_broadcast_gossip<IO: BasicIOCtx<msg::NetworkMessage>>(&self, io_ctx: &mut IO) {
    // Send to SlaveGroups
    for (_, eids) in self.gossip.get().slave_address_config {
      for eid in eids {
        io_ctx.send(
          eid,
          msg::NetworkMessage::Slave(msg::SlaveMessage::MasterGossip(msg::MasterGossip {
            gossip_data: self.gossip.clone(),
            leader_map: self.leader_map.value().clone(),
          })),
        )
      }
    }
  }

  /// Send GossipData
  pub fn send_gossip<IO: BasicIOCtx>(&mut self, io_ctx: &mut IO, sid: SlaveGroupId) {
    self.send_to_slave_common(
      io_ctx,
      sid,
      msg::SlaveRemotePayload::MasterGossip(msg::MasterGossip {
        gossip_data: self.gossip.clone(),
        leader_map: self.leader_map.value().clone(),
      }),
    );
  }

  /// Send `InvalidDDLQuery` to the given `ResponseData`
  fn respond_invalid_ddl<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    response_data: &ResponseData,
  ) {
    self.external_request_id_map.remove(&response_data.request_id);
    io_ctx.send(
      &response_data.sender_eid,
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
        msg::ExternalDDLQueryAborted {
          request_id: response_data.request_id.clone(),
          payload: msg::ExternalDDLQueryAbortData::InvalidDDLQuery,
        },
      )),
    )
  }

  /// Send `InvalidShardingOp` to the given `ResponseData`
  fn respond_invalid_sharding<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    response_data: &ResponseData,
  ) {
    self.external_request_id_map.remove(&response_data.request_id);
    io_ctx.send(
      &response_data.sender_eid,
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalShardingAborted(
        msg::ExternalShardingAborted {
          request_id: response_data.request_id.clone(),
          payload: msg::ExternalShardingAbortData::InvalidShardingOp,
        },
      )),
    )
  }
}
