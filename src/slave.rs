use crate::common::{
  lookup, mk_t, update_all_eids, update_leader_map, BasicIOCtx, GeneralTraceMessage, GossipData,
  LeaderMap, RemoteLeaderChangedPLm, SlaveIOCtx, SlaveTraceMessage, Timestamp, VersionedValue,
};
use crate::common::{
  CoordGroupId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, SlaveGroupId, TabletGroupId,
};
use crate::common::{EndpointId, QueryId};
use crate::coord::CoordForwardMsg;
use crate::create_table_rm_es::{CreateTableRMAction, CreateTableRMES, CreateTableRMPayloadTypes};
use crate::create_table_tm_es::CreateTableTMPayloadTypes;
use crate::message as msg;
use crate::message::SlaveRemotePayload;
use crate::network_driver::{NetworkDriver, NetworkDriverContext};
use crate::paxos::{PaxosConfig, PaxosContextBase, PaxosDriver, PaxosTimerEvent, UserPLEntry};
use crate::server::ServerContextBase;
use crate::shard_split_slave_rm_es::{
  ShardSplitSlaveRMAction, ShardSplitSlaveRMES, ShardSplitSlaveRMPayloadTypes,
};
use crate::stmpaxos2pc_rm;
use crate::tablet::{TabletBundle, TabletForwardMsg, TabletSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[path = "test/slave_test.rs"]
pub mod slave_test;

// -----------------------------------------------------------------------------------------------
//  SlavePLm
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct SlaveBundle {
  remote_leader_changes: Vec<RemoteLeaderChangedPLm>,
  gossip_data: Option<(GossipData, LeaderMap)>,
  pub plms: Vec<SlavePLm>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlavePLm {
  CreateTable(stmpaxos2pc_rm::RMPLm<CreateTableRMPayloadTypes>),
  ShardSplit(stmpaxos2pc_rm::RMPLm<ShardSplitSlaveRMPayloadTypes>),
}

// -----------------------------------------------------------------------------------------------
//  Shared Paxos
// -----------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct SharedPaxosBundle {
  slave: SlaveBundle,
  tablet: BTreeMap<TabletGroupId, TabletBundle>,
}

// -----------------------------------------------------------------------------------------------
//  SlavePaxosContext
// -----------------------------------------------------------------------------------------------

pub struct SlavePaxosContext<'a, IO: SlaveIOCtx> {
  /// IO Objects.
  pub io_ctx: &'a mut IO,
  pub this_eid: &'a EndpointId,
}

impl<'a, IO: SlaveIOCtx> PaxosContextBase<SharedPaxosBundle> for SlavePaxosContext<'a, IO> {
  type RngCoreT = IO::RngCoreT;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    self.io_ctx.rand()
  }

  fn this_eid(&self) -> &EndpointId {
    self.this_eid
  }

  fn send(&mut self, eid: &EndpointId, message: msg::PaxosDriverMessage<SharedPaxosBundle>) {
    self
      .io_ctx
      .send(eid, msg::NetworkMessage::Slave(msg::SlaveMessage::PaxosDriverMessage(message)));
  }

  fn defer(&mut self, defer_time: Timestamp, timer_event: PaxosTimerEvent) {
    self.io_ctx.defer(defer_time, SlaveTimerInput::PaxosTimerEvent(timer_event));
  }
}

// -----------------------------------------------------------------------------------------------
//  SlaveForwardMsg
// -----------------------------------------------------------------------------------------------
pub enum SlaveForwardMsg {
  SlaveBundle(Vec<SlavePLm>),
  SlaveExternalReq(msg::SlaveExternalReq),
  SlaveRemotePayload(msg::SlaveRemotePayload),
  GossipData(Arc<GossipData>, LeaderMap),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
  LeaderChanged(msg::LeaderChanged),

  // Internal
  SlaveBackMessage(SlaveBackMessage),
  SlaveTimerInput(SlaveTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  Full Slave Input
// -----------------------------------------------------------------------------------------------

/// Messages send from Tablets to the Slave
#[derive(Debug)]
pub struct TabletBundleInsertion {
  /// The Tablet that is sending this message
  pub tid: TabletGroupId,
  /// The LeadershipId that it believe it is inserting for
  pub lid: LeadershipId,
  pub bundle: TabletBundle,
}

#[derive(Debug)]
pub enum SlaveBackMessage {
  TabletBundleInsertion(TabletBundleInsertion),
  TabletSnapshot(TabletSnapshot),
}

/// Messages deferred by the Slave to be run on the Slave.
#[derive(Debug)]
pub enum SlaveTimerInput {
  PaxosTimerEvent(PaxosTimerEvent),
  /// This is used to periodically propagate out RemoteLeaderChanged. It is
  /// only used by the Leader.
  RemoteLeaderChanged,
  /// A timer event to detect if the Slave PaxosGroup has a failed node.
  PaxosGroupFailureDetector,
  /// A timer event to detect if there are any `unconfirmed_eids` in the PaxosDriver. We
  /// use this to start constructing a `SlaveSnapshot` if there is.
  CheckUnconfirmedEids,
}

pub enum FullSlaveInput {
  SlaveMessage(msg::SlaveMessage),
  SlaveBackMessage(SlaveBackMessage),
  SlaveTimerInput(SlaveTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  SlaveSnapshot
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SlaveSnapshot {
  pub this_sid: SlaveGroupId,
  pub coord_positions: Vec<CoordGroupId>,
  pub gossip: GossipData,
  pub leader_map: LeaderMap,
  pub paxos_driver_start: msg::StartNewNode<SharedPaxosBundle>,

  // Statuses
  /// If this is a Follower, we copy over the ESs in `Statuses` to the below. If this
  /// is the Leader, we compute the ESs that would result as a result of a Leadership
  /// change and populate the below.
  pub create_table_ess: BTreeMap<QueryId, CreateTableRMES>,
  pub shard_split_ess: BTreeMap<QueryId, ShardSplitSlaveRMES>,

  /// We remember the set of Tablets to wait for here so that.
  pub tablet_snapshots: BTreeMap<TabletGroupId, TabletSnapshot>,
}

// -----------------------------------------------------------------------------------------------
//  Status
// -----------------------------------------------------------------------------------------------

/// This contains every Slave Status. Every QueryId here is unique across all
/// other members here.
/// NOTE: When adding a new element here, amend the `SlaveSnapshot` accordingly.
#[derive(Debug, Default)]
pub struct Statuses {
  create_table_ess: BTreeMap<QueryId, CreateTableRMES>,
  shard_split_ess: BTreeMap<QueryId, ShardSplitSlaveRMES>,

  /// We create this once a `ReconfigSlaveGroup` arrives that contains a new config. We use
  /// `do_reconfig` to communicate to the point of `SharedPaxosBundle` to insert a `ReconfigBundle`
  /// instead. We should keep `do_reconfig` around until this insertion happens and where
  /// `paxos_nodes.contains` reflects this fact. However, we need to be careful to clear
  /// `do_reconfig` before the next time a SharedPaxosBundle is computed for insertion, since
  /// we don’t want to accidentally make that into a ReconfigBundle too.
  do_reconfig: Option<(Vec<EndpointId>, Vec<EndpointId>)>,
  /// This is populated whenever we start building a `SlaveSnapshot`. We call the PaxosDriver
  /// to get the current set of `unconfirmed_eids` that map to `false` (held in `Vec<EndpointId>`),
  /// which also returns the `paxos_driver_start`. We send `ConstructTabletSnapshot` to the current
  /// set of Tablets and we remember `io_ctx.num_tablets` so that we can determine when all Tablets
  /// have responded with their their snapshots.
  pending_snapshot: Option<(SlaveSnapshot, Vec<EndpointId>, usize)>,
}

// -----------------------------------------------------------------------------------------------
//  SlaveConfig
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SlaveConfig {
  /// This is used for generate the `suffix` of a Timestamp, where we just generate
  /// a random `u64` and take the remainder after dividing by `timestamp_suffix_divisor`.
  /// This cannot be 0; the default value is 1, making the suffix always be 0.
  pub timestamp_suffix_divisor: u64,

  /// Timer events
  pub remote_leader_changed_period_ms: u128,
  pub failure_detector_period_ms: u128,
  pub check_unconfirmed_eids_period_ms: u128,
}

// -----------------------------------------------------------------------------------------------
//  Server Context
// -----------------------------------------------------------------------------------------------

impl ServerContextBase for SlaveContext {
  fn leader_map(&self) -> &LeaderMap {
    &self.leader_map.value()
  }

  fn this_gid(&self) -> &PaxosGroupId {
    &self.this_gid
  }

  fn this_eid(&self) -> &EndpointId {
    &self.this_eid
  }
}

// -----------------------------------------------------------------------------------------------
//  Slave State
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct SlaveState {
  pub ctx: SlaveContext,
  pub statuses: Statuses,
}

/// The SlaveState that holds all the state of the Slave
pub struct SlaveContext {
  /// Maps integer values to Coords for the purpose of routing External requests.
  pub coord_positions: Vec<CoordGroupId>,

  // Metadata
  pub slave_config: SlaveConfig,
  pub this_sid: SlaveGroupId,
  pub this_gid: PaxosGroupId, // self.this_sid.to_gid()
  pub this_eid: EndpointId,

  /// Between calls to `handle_input`, we should maintain the following properties:
  /// 1. `leader_map` should contain an entry for every `PaxosGroupId` in the
  ///     Current PaxosGroup View.
  /// 2. The Leaderships in `leader_map` should be in the Current PaxosGroup View.  
  /// 3. `all_eids` should contain all `EndpointId` in Current PaxosGroup View.
  ///
  /// See the out-of-line docs for the definition of Current PaxosGroup View.

  /// Gossip
  pub gossip: Arc<GossipData>,
  /// LeaderMap. We use a `VerionedValue` primarily for the `NetworkDriver`
  pub leader_map: VersionedValue<LeaderMap>,
  /// The set `EndpointId` that this Slave is accepting delivery of messages from.
  pub all_eids: VersionedValue<BTreeSet<EndpointId>>,

  /// NetworkDriver
  pub network_driver: NetworkDriver<msg::SlaveRemotePayload>,

  // Paxos
  pub slave_bundle: SlaveBundle,
  /// After a `SharedPaxosBundle` is inserted, this is cleared.
  pub tablet_bundles: BTreeMap<TabletGroupId, TabletBundle>,
  pub paxos_driver: PaxosDriver<SharedPaxosBundle>,
}

impl Debug for SlaveContext {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut debug_trait_builder = f.debug_struct("SlaveContext");
    let _ = debug_trait_builder.field("coord_positions", &self.coord_positions);
    let _ = debug_trait_builder.field("this_sid", &self.this_sid);
    let _ = debug_trait_builder.field("this_gid", &self.this_gid);
    let _ = debug_trait_builder.field("this_eid", &self.this_eid);
    let _ = debug_trait_builder.field("gossip", &self.gossip);
    let _ = debug_trait_builder.field("leader_map", &self.leader_map);
    let _ = debug_trait_builder.field("network_driver", &self.network_driver);
    let _ = debug_trait_builder.field("slave_bundle", &self.slave_bundle);
    let _ = debug_trait_builder.field("tablet_bundles", &self.tablet_bundles);
    debug_trait_builder.finish()
  }
}

impl SlaveState {
  pub fn new(ctx: SlaveContext) -> SlaveState {
    SlaveState { ctx, statuses: Default::default() }
  }

  /// Handles a `MasterSnapshot` to initiate a reconfigured `MasterState` properly.
  pub fn create_reconfig<IO: SlaveIOCtx>(
    io_ctx: &mut IO,
    this_sid: SlaveGroupId,
    coord_positions: Vec<CoordGroupId>,
    gossip: Arc<GossipData>,
    mut leader_map: LeaderMap,
    paxos_driver_start: msg::StartNewNode<SharedPaxosBundle>,
    create_table_ess: BTreeMap<QueryId, CreateTableRMES>,
    shard_split_ess: BTreeMap<QueryId, ShardSplitSlaveRMES>,
    this_eid: EndpointId,
    slave_config: SlaveConfig,
    paxos_config: PaxosConfig,
  ) -> SlaveState {
    // Create Statuses
    let statuses =
      Statuses { create_table_ess, shard_split_ess, do_reconfig: None, pending_snapshot: None };

    // Create the SlaveCtx
    let paxos_nodes = paxos_driver_start.paxos_nodes.clone();
    let lid = LeadershipId::mk_first(paxos_nodes.get(0).unwrap().clone());
    leader_map.insert(this_sid.to_gid(), lid);
    let leader_map = VersionedValue::new(leader_map);

    // Recall that `gossip` does not contain `paxos_nodes`.
    let mut all_eids = BTreeSet::<EndpointId>::new();
    for (_, eids) in gossip.get().slave_address_config {
      all_eids.extend(eids.clone());
    }
    all_eids.extend(gossip.get().master_address_config.clone());
    all_eids.extend(paxos_nodes.clone());

    let network_driver = NetworkDriver::new(&leader_map);
    let ctx = SlaveContext {
      coord_positions,
      slave_config,
      this_sid: this_sid.clone(),
      this_gid: this_sid.to_gid(),
      this_eid: this_eid.clone(),
      gossip,
      leader_map,
      all_eids: VersionedValue::new(all_eids),
      network_driver,
      slave_bundle: Default::default(),
      tablet_bundles: Default::default(),
      paxos_driver: PaxosDriver::create_reconfig(
        &mut SlavePaxosContext { io_ctx, this_eid: &this_eid },
        paxos_driver_start,
        paxos_config,
      ),
    };

    SlaveState { ctx, statuses }
  }

  /// This should be called at the very start of the life of a Slave node. This
  /// will start the timer events, paxos insertion, etc. Here, `tids` is the initial
  /// set of Tablets.
  pub fn bootstrap<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO, tids: Vec<TabletGroupId>) {
    let ctx = &mut SlavePaxosContext { io_ctx, this_eid: &self.ctx.this_eid };
    if self.ctx.is_leader() {
      // Start the Paxos insertion cycle with an empty bundle. Note that we create
      // the TabletBundles as well.
      self.ctx.paxos_driver.insert_bundle(
        ctx,
        UserPLEntry::Bundle(SharedPaxosBundle {
          slave: Default::default(),
          tablet: tids.into_iter().map(|tid| (tid, TabletBundle::default())).collect(),
        }),
      );
    }

    // Start Paxos Timer Events
    self.ctx.paxos_driver.timer_event(ctx, PaxosTimerEvent::LeaderHeartbeat);
    self.ctx.paxos_driver.timer_event(ctx, PaxosTimerEvent::NextIndex);

    // Start other time events
    for event in [
      SlaveTimerInput::RemoteLeaderChanged,
      SlaveTimerInput::PaxosGroupFailureDetector,
      SlaveTimerInput::CheckUnconfirmedEids,
    ] {
      self.ctx.handle_input(io_ctx, &mut self.statuses, SlaveForwardMsg::SlaveTimerInput(event));
    }
  }

  pub fn handle_input<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO, input: FullSlaveInput) {
    match input {
      FullSlaveInput::SlaveMessage(message) => {
        self.ctx.handle_incoming_message(io_ctx, &mut self.statuses, message);
      }
      FullSlaveInput::SlaveBackMessage(message) => {
        // Handles messages that were send from the Tablets back to the Slave.
        let forward_msg = SlaveForwardMsg::SlaveBackMessage(message);
        self.ctx.handle_input(io_ctx, &mut self.statuses, forward_msg);
      }
      FullSlaveInput::SlaveTimerInput(timer_input) => {
        let forward_msg = SlaveForwardMsg::SlaveTimerInput(timer_input);
        self.ctx.handle_input(io_ctx, &mut self.statuses, forward_msg);
      }
    }
  }

  pub fn get_eids(&self) -> &VersionedValue<BTreeSet<EndpointId>> {
    &self.ctx.all_eids
  }
}

impl SlaveContext {
  /// This is used to first create a Slave node. The `gossip` and `leader_map` are
  /// exactly what would come in a `CreateSlaveGroup`; i.e. they do not contain
  /// `paxos_nodes`.
  pub fn new(
    this_sid: SlaveGroupId,
    coord_positions: Vec<CoordGroupId>,
    gossip: Arc<GossipData>,
    mut leader_map: LeaderMap,
    paxos_nodes: Vec<EndpointId>,
    this_eid: EndpointId,
    slave_config: SlaveConfig,
    paxos_config: PaxosConfig,
  ) -> SlaveContext {
    // Amend the LeaderMap to include this new Slave.
    let lid = LeadershipId::mk_first(paxos_nodes.get(0).unwrap().clone());
    leader_map.insert(this_sid.to_gid(), lid);
    let leader_map = VersionedValue::new(leader_map);

    // Recall that `gossip` does not contain `paxos_nodes`.
    let mut all_eids = BTreeSet::<EndpointId>::new();
    for (_, eids) in gossip.get().slave_address_config {
      all_eids.extend(eids.clone());
    }
    all_eids.extend(gossip.get().master_address_config.clone());
    all_eids.extend(paxos_nodes.clone());

    let network_driver = NetworkDriver::new(&leader_map);
    SlaveContext {
      coord_positions,
      slave_config,
      this_sid: this_sid.clone(),
      this_gid: this_sid.to_gid(),
      this_eid,
      gossip,
      leader_map,
      all_eids: VersionedValue::new(all_eids),
      network_driver,
      slave_bundle: Default::default(),
      tablet_bundles: Default::default(),
      paxos_driver: PaxosDriver::create_initial(paxos_nodes, paxos_config),
    }
  }

  /// Handles all messages, coming from Tablets, the Slave, External, etc.
  pub fn handle_incoming_message<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    message: msg::SlaveMessage,
  ) {
    match message {
      msg::SlaveMessage::SlaveExternalReq(request) => {
        if self.is_leader() {
          self.handle_input(io_ctx, statuses, SlaveForwardMsg::SlaveExternalReq(request))
        }
      }
      msg::SlaveMessage::RemoteMessage(remote_message) => {
        if self.is_leader() {
          // Pass the message through the NetworkDriver
          let maybe_delivered = self.network_driver.receive(
            NetworkDriverContext {
              this_gid: &self.this_gid,
              this_eid: &self.this_eid,
              leader_map: &self.leader_map,
              remote_leader_changes: &mut self.slave_bundle.remote_leader_changes,
            },
            remote_message,
          );
          if let Some(payload) = maybe_delivered {
            // Deliver if the message passed through
            self.handle_input(io_ctx, statuses, SlaveForwardMsg::SlaveRemotePayload(payload));
          }
        }
      }
      msg::SlaveMessage::RemoteLeaderChangedGossip(msg::RemoteLeaderChangedGossip { gid, lid }) => {
        if self.is_leader() {
          // We filter out messages for PaxosGroupIds urecongnized by this node.
          if let Some(cur_lid) = self.leader_map.value().get(&gid) {
            // If the incoming RemoteLeaderChanged would increase the generation
            // in LeaderMap, then persist it.
            if &lid.gen > &cur_lid.gen {
              self.slave_bundle.remote_leader_changes.push(RemoteLeaderChangedPLm { gid, lid })
            }
          }
        }
      }
      msg::SlaveMessage::MasterGossip(master_gossip) => {
        if self.is_leader() {
          self.handle_master_gossip(master_gossip);
        }
      }
      msg::SlaveMessage::PaxosDriverMessage(paxos_message) => {
        let bundles = self.paxos_driver.handle_paxos_message(
          &mut SlavePaxosContext { io_ctx, this_eid: &self.this_eid },
          paxos_message,
        );
        for shared_bundle in bundles {
          match shared_bundle {
            msg::PLEntry::LeaderChanged(leader_changed) => {
              // Forward the LeaderChanged to all Tablets.
              let all_tids = io_ctx.all_tids();
              for tid in all_tids {
                let forward_msg = TabletForwardMsg::LeaderChanged(leader_changed.clone());
                io_ctx.tablet_forward(&tid, forward_msg);
              }

              // Forward the LeaderChanged to all Coords.
              let all_tids = io_ctx.all_cids();
              for cid in all_tids {
                let forward_msg = CoordForwardMsg::LeaderChanged(leader_changed.clone());
                io_ctx.coord_forward(&cid, forward_msg);
              }

              // Forward to Slave Backend
              self.handle_input(
                io_ctx,
                statuses,
                SlaveForwardMsg::LeaderChanged(leader_changed.clone()),
              );

              // Trace for testing
              io_ctx
                .trace(SlaveTraceMessage::LeaderChanged(self.this_gid.clone(), leader_changed.lid));
            }
            msg::PLEntry::Bundle(shared_bundle) => {
              // Process the persisted data only first.
              let remote_leader_changed = shared_bundle.slave.remote_leader_changes.clone();
              self.process_bundle(io_ctx, statuses, shared_bundle);
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
                // We clear this here to guarantee that we do not accidentally
                // insert another ReconfigBundle.
                statuses.do_reconfig = None;

                // Process the persisted data only first.
                let remote_leader_changed = reconfig.bundle.slave.remote_leader_changes.clone();
                self.process_bundle(io_ctx, statuses, reconfig.bundle);

                // Update the `all_eids`
                let rem_eids = reconfig.rem_eids;
                let new_eids = reconfig.new_eids;
                update_all_eids(&mut self.all_eids, &rem_eids, new_eids.clone());

                // If this is a Leader, we try to start building a SlaveSnapshot. Note that there
                // might already one being constructed (from an unfortunately timed
                // `CheckUnconfirmedEids`), in which case we do nothing. This is okay; we will
                // compensate on a subsequent `CheckUnconfirmedEids`.
                if self.is_leader() {
                  self.maybe_start_snapshot(io_ctx, statuses);

                  // Inform the Master that the Reconfiguration was a success.
                  self.send_to_master(
                    io_ctx,
                    msg::MasterRemotePayload::SlaveReconfig(
                      msg::SlaveReconfig::SlaveGroupReconfigured(msg::SlaveGroupReconfigured {
                        sid: self.this_sid.clone(),
                      }),
                    ),
                  );
                }

                // Trace the reconfig event.
                io_ctx
                  .general_trace(GeneralTraceMessage::Reconfig(self.this_gid.clone(), new_eids));

                // Then, deliver any messages that were blocked.
                self.deliver_blocked_messages(io_ctx, statuses, remote_leader_changed);
              }
            }
          }
        }
      }
    }
  }

  /// Processes the insertion of a `SlaveBundle`.
  pub fn process_bundle<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    shared_bundle: SharedPaxosBundle,
  ) {
    let all_tids = io_ctx.all_tids();
    let all_cids = io_ctx.all_cids();
    let slave_bundle = shared_bundle.slave;

    // We buffer the SlaveForwardMsgs and execute them at the end.
    let mut slave_forward_msgs = Vec::<SlaveForwardMsg>::new();

    // Dispatch RemoteLeaderChanges
    for remote_change in slave_bundle.remote_leader_changes {
      let forward_msg = SlaveForwardMsg::RemoteLeaderChanged(remote_change.clone());
      slave_forward_msgs.push(forward_msg);
      for tid in &all_tids {
        let forward_msg = TabletForwardMsg::RemoteLeaderChanged(remote_change.clone());
        io_ctx.tablet_forward(&tid, forward_msg);
      }
      for cid in &all_cids {
        let forward_msg = CoordForwardMsg::RemoteLeaderChanged(remote_change.clone());
        io_ctx.coord_forward(&cid, forward_msg);
      }
    }

    // Dispatch GossipData
    if let Some((gossip_data, some_leader_map)) = slave_bundle.gossip_data {
      let gossip = Arc::new(gossip_data);
      for tid in &all_tids {
        let forward_msg = TabletForwardMsg::GossipData(gossip.clone(), some_leader_map.clone());
        io_ctx.tablet_forward(&tid, forward_msg);
      }
      for cid in &all_cids {
        let forward_msg = CoordForwardMsg::GossipData(gossip.clone(), some_leader_map.clone());
        io_ctx.coord_forward(&cid, forward_msg);
      }
      slave_forward_msgs.push(SlaveForwardMsg::GossipData(gossip, some_leader_map));
    }

    // Dispatch the PaxosBundles
    slave_forward_msgs.push(SlaveForwardMsg::SlaveBundle(slave_bundle.plms));
    for (tid, tablet_bundle) in shared_bundle.tablet {
      let forward_msg = TabletForwardMsg::TabletBundle(tablet_bundle);
      io_ctx.tablet_forward(&tid, forward_msg);
    }

    // Forward to Slave Backend
    for forward_msg in slave_forward_msgs {
      self.handle_input(io_ctx, statuses, forward_msg);
    }
  }

  /// Delivery any messages in the `network_driver` that were blocked by not having
  /// a high enough leadership.
  pub fn deliver_blocked_messages<IO: SlaveIOCtx>(
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
          self.handle_input(io_ctx, statuses, SlaveForwardMsg::SlaveRemotePayload(payload));
        }
      }
    }
  }

  /// Handles inputs to the Slave Backend.
  fn handle_input<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    slave_input: SlaveForwardMsg,
  ) {
    match slave_input {
      SlaveForwardMsg::SlaveBackMessage(slave_back_msg) => match slave_back_msg {
        SlaveBackMessage::TabletBundleInsertion(insert) => {
          if self.leader_map.value().get(&self.this_gid).unwrap() == &insert.lid {
            self.tablet_bundles.insert(insert.tid, insert.bundle);
            self.maybe_start_insert(io_ctx, statuses);
          }
        }
        SlaveBackMessage::TabletSnapshot(tablet_snapshot) => {
          // Recall that there will definitely be a `pending_snapshot`, since that is
          // never erased until it is complete
          let (snapshot, new_eids, num_tablets) = statuses.pending_snapshot.as_mut().unwrap();
          snapshot.tablet_snapshots.insert(tablet_snapshot.this_tid.clone(), tablet_snapshot);

          // If all TabletSnapshots have been added, we clear `pending_snapshot` and send
          // it off to the new nodes.
          if snapshot.tablet_snapshots.len() == *num_tablets {
            for new_eid in new_eids {
              io_ctx.send(
                new_eid,
                msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::SlaveSnapshot(
                  snapshot.clone(),
                )),
              )
            }
            statuses.pending_snapshot = None;
          }
        }
      },
      SlaveForwardMsg::SlaveTimerInput(timer_input) => match timer_input {
        SlaveTimerInput::PaxosTimerEvent(timer_event) => {
          self
            .paxos_driver
            .timer_event(&mut SlavePaxosContext { io_ctx, this_eid: &self.this_eid }, timer_event);
        }
        SlaveTimerInput::RemoteLeaderChanged => {
          if self.is_leader() {
            // If this node is the Leader, then send out RemoteLeaderChanged.
            self.broadcast_leadership(io_ctx);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.slave_config.remote_leader_changed_period_ms);
          io_ctx.defer(defer_time, SlaveTimerInput::RemoteLeaderChanged);
        }
        SlaveTimerInput::PaxosGroupFailureDetector => {
          if self.is_leader() {
            let maybe_dead_eids = self.paxos_driver.get_maybe_dead();
            if !maybe_dead_eids.is_empty() {
              // Recall that the above returns only as much as we are capable of reconfiguring.
              let nodes_dead = msg::NodesDead { sid: self.this_sid.clone(), eids: maybe_dead_eids };
              // Here, we send the Master a `NodesDead` message indicating that we
              // want to reconfigure. Recall we only reconfigure one-at-a-time to
              // preserve our liveness properties.
              self.send_to_master(
                io_ctx,
                msg::MasterRemotePayload::SlaveReconfig(msg::SlaveReconfig::NodesDead(nodes_dead)),
              );
            }
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.slave_config.failure_detector_period_ms);
          io_ctx.defer(defer_time, SlaveTimerInput::PaxosGroupFailureDetector);
        }
        SlaveTimerInput::CheckUnconfirmedEids => {
          // We do this for both the Leader and Followers. If there are `unconfirmed_eids` in
          // the PaxosDriver, then we attempt to send a `SlaveSnapshot`.
          if self.paxos_driver.has_unsent_unconfirmed() {
            self.maybe_start_snapshot(io_ctx, statuses);
          }

          // We schedule this both for all nodes, not just Leaders, so that when a Follower
          // becomes the Leader, these timer events will already be working.
          let defer_time = mk_t(self.slave_config.check_unconfirmed_eids_period_ms);
          io_ctx.defer(defer_time, SlaveTimerInput::CheckUnconfirmedEids);
        }
      },
      SlaveForwardMsg::SlaveBundle(bundle) => {
        for paxos_log_msg in bundle {
          match paxos_log_msg {
            SlavePLm::CreateTable(plm) => {
              let (query_id, action) =
                stmpaxos2pc_rm::handle_rm_plm(self, io_ctx, &mut statuses.create_table_ess, plm);
              self.handle_create_table_es_action(io_ctx, statuses, query_id, action);
            }
            SlavePLm::ShardSplit(plm) => {
              let (query_id, action) =
                stmpaxos2pc_rm::handle_rm_plm(self, io_ctx, &mut statuses.shard_split_ess, plm);
              self.handle_shard_split_es_action(io_ctx, statuses, query_id, action);
            }
          }
        }

        if self.is_leader() {
          // Inform all ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.create_table_ess {
            es.start_inserting(self, io_ctx);
          }
          for (_, es) in &mut statuses.shard_split_ess {
            es.start_inserting(self, io_ctx);
          }

          // Continue the insert cycle if there are no Tablets.
          self.maybe_start_insert(io_ctx, statuses);
        }
      }
      SlaveForwardMsg::SlaveExternalReq(request) => {
        // Compute the hash of the request Id.
        let request_id = match &request {
          msg::SlaveExternalReq::PerformExternalQuery(perform) => perform.request_id.clone(),
          msg::SlaveExternalReq::CancelExternalQuery(cancel) => cancel.request_id.clone(),
        };
        let mut hasher = DefaultHasher::new();
        request_id.hash(&mut hasher);
        let hash_value = hasher.finish();

        // Route the request to a Coord determined by the hash.
        let coord_index = hash_value % self.coord_positions.len() as u64;
        let cid = self.coord_positions.get(coord_index as usize).unwrap();
        io_ctx.coord_forward(cid, CoordForwardMsg::ExternalMessage(request));
      }
      SlaveForwardMsg::SlaveRemotePayload(payload) => {
        match payload {
          msg::SlaveRemotePayload::CreateTable(message) => {
            let (query_id, action) =
              stmpaxos2pc_rm::handle_rm_msg(self, io_ctx, &mut statuses.create_table_ess, message);
            self.handle_create_table_es_action(io_ctx, statuses, query_id, action);
          }
          msg::SlaveRemotePayload::ShardSplit(message) => {
            let (query_id, action) =
              stmpaxos2pc_rm::handle_rm_msg(self, io_ctx, &mut statuses.shard_split_ess, message);
            self.handle_shard_split_es_action(io_ctx, statuses, query_id, action);
          }
          msg::SlaveRemotePayload::MasterGossip(master_gossip) => {
            self.handle_master_gossip(master_gossip);
          }
          msg::SlaveRemotePayload::TabletMessage(tid, tablet_msg) => {
            io_ctx.tablet_forward(&tid, TabletForwardMsg::TabletMessage(tablet_msg))
          }
          msg::SlaveRemotePayload::CoordMessage(cid, coord_msg) => {
            io_ctx.coord_forward(&cid, CoordForwardMsg::CoordMessage(coord_msg))
          }
          msg::SlaveRemotePayload::ReconfigSlaveGroup(reconfig) => {
            // Respond affirmative immeidately if this node has already completed this reconfig.
            if self.paxos_driver.contains_nodes(&reconfig.new_eids) {
              self.send_to_master(
                io_ctx,
                msg::MasterRemotePayload::SlaveReconfig(
                  msg::SlaveReconfig::SlaveGroupReconfigured(msg::SlaveGroupReconfigured {
                    sid: self.this_sid.clone(),
                  }),
                ),
              );
            } else {
              // Otherwise, do nothing if we are already going to do this reconfig.
              if let Some((rem_eids, new_eids)) = &statuses.do_reconfig {
                debug_assert!(rem_eids == &reconfig.rem_eids);
                debug_assert!(new_eids == &reconfig.new_eids);
              } else {
                // Otherwise, populate `statuses` to make sure we insert a `ReconfigBundle` next.
                statuses.do_reconfig = Some((reconfig.rem_eids, reconfig.new_eids));
              }
            }
          }
        }
      }
      SlaveForwardMsg::GossipData(gossip, some_leader_map) => {
        // We only accept new Gossips where the generation increases.
        if self.gossip.get_gen() < gossip.get_gen() {
          // Amend the local LeaderMap to refect the new GossipData.
          update_leader_map(
            &mut self.leader_map,
            self.gossip.as_ref(),
            &some_leader_map,
            gossip.as_ref(),
          );

          // Update `all_eids` by fully recomputing it.
          let mut all_eids = BTreeSet::<EndpointId>::new();
          for (sid, eids) in gossip.get().slave_address_config {
            if sid != &self.this_sid {
              all_eids.extend(eids.clone());
            }
          }
          all_eids.extend(self.paxos_driver.paxos_nodes().clone());
          all_eids.extend(gossip.get().master_address_config.clone());
          self.all_eids.set(all_eids);

          // Update Gossip
          self.gossip = gossip;
        }
      }
      SlaveForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid;
        let lid = remote_leader_changed.lid;

        // Only accept a RemoteLeaderChanged if the PaxosGroupId is already known.
        if let Some(cur_lid) = self.leader_map.value().get(&gid) {
          // Only update the LeadershipId if the new one increases the old one.
          if lid.gen > cur_lid.gen {
            self.leader_map.update(|leader_map| {
              leader_map.insert(gid, lid);
            });
          }
        }
      }
      SlaveForwardMsg::LeaderChanged(leader_changed) => {
        // Update the LeadershipId
        let this_gid = self.this_gid.clone();
        self.leader_map.update(move |leader_map| {
          leader_map.insert(this_gid, leader_changed.lid);
        });

        if self.is_leader() {
          // By the SharedPaxosInserter, these must be empty at the start of Leadership.
          self.slave_bundle = SlaveBundle::default();
          self.tablet_bundles = BTreeMap::default();
        }

        // Inform CreateQueryES
        let query_ids: Vec<QueryId> = statuses.create_table_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.create_table_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self);
          self.handle_create_table_es_action(io_ctx, statuses, query_id.clone(), action);
        }

        // Inform ShardSplitSlaveRMES
        let query_ids: Vec<QueryId> = statuses.shard_split_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.shard_split_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self);
          self.handle_shard_split_es_action(io_ctx, statuses, query_id.clone(), action);
        }

        // MasterReconfig
        statuses.do_reconfig = None;

        // Inform the NetworkDriver
        self.network_driver.leader_changed();

        if self.is_leader() {
          // This node is the new Leader

          // Broadcast RemoteLeaderChanged
          self.broadcast_leadership(io_ctx);
          // Start the insert cycle if there are no Tablets.
          self.maybe_start_insert(io_ctx, statuses);
        }
      }
    }
  }

  /// Used to broadcast out `RemoteLeaderChanged` to all other
  /// PaxosGroups to help maintain their LeaderMaps.
  fn broadcast_leadership<IO: BasicIOCtx<msg::NetworkMessage>>(&self, io_ctx: &mut IO) {
    let this_lid = self.leader_map.value().get(&self.this_gid).unwrap().clone();
    for (sid, eids) in self.gossip.get().slave_address_config {
      if sid == &self.this_sid {
        // Make sure to avoid sending this PaxosGroup the RemoteLeaderChanged.
        continue;
      }
      for eid in eids {
        io_ctx.send(
          eid,
          msg::NetworkMessage::Slave(msg::SlaveMessage::RemoteLeaderChangedGossip(
            msg::RemoteLeaderChangedGossip { gid: self.this_gid.clone(), lid: this_lid.clone() },
          )),
        )
      }
    }
    for eid in self.gossip.get().master_address_config {
      io_ctx.send(
        eid,
        msg::NetworkMessage::Master(msg::MasterMessage::RemoteLeaderChangedGossip(
          msg::RemoteLeaderChangedGossip { gid: self.this_gid.clone(), lid: this_lid.clone() },
        )),
      )
    }
  }

  /// Checks whether all Tablets have forwarded their `TabletBundle`s back up to the Slave, and
  /// if so, send it to the PaxosDriver for insertion. We also clear the current set of Bundles.
  fn maybe_start_insert<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO, statuses: &mut Statuses) {
    if self.tablet_bundles.len() == io_ctx.num_tablets() {
      let bundle = SharedPaxosBundle {
        slave: std::mem::take(&mut self.slave_bundle),
        tablet: std::mem::take(&mut self.tablet_bundles),
      };

      // Check if we should reconfigure, construct the appropriate `UserPLEntry`.
      let user_entry = if let Some((rem_eids, new_eids)) = statuses.do_reconfig.clone() {
        UserPLEntry::ReconfigBundle(msg::ReconfigBundle { rem_eids, new_eids, bundle })
      } else {
        UserPLEntry::Bundle(bundle)
      };
      self
        .paxos_driver
        .insert_bundle(&mut SlavePaxosContext { io_ctx, this_eid: &self.this_eid }, user_entry);
    }
  }

  // TODO: this can be pulled out and isolated so that nobody else touches `pending_snapshot`
  /// This start the construction of a `SlaveSnapshot` if one is not already being constructed.
  /// We only ever try to create `SlaveSnapshot` via this methods. This guarantees that each
  /// one is constructed successfully (i.e. not preempted), which is important for making sure
  /// that all `EndpointId`s returned by `mk_start_new_node` actually do get a `SlaveSnapshot`
  /// sent to it.
  ///
  /// Importantly, merely needs to be called between the processing of Inserted bundles. That way
  /// the `paxos_driver_start`, along with the `SlaveSnapshot` and `TabletSnapshot`s we get would
  /// be sufficient to start up a new node successfully.
  fn maybe_start_snapshot<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO, statuses: &mut Statuses) {
    if statuses.pending_snapshot.is_none() {
      let (paxos_driver_start, non_started_eids) = self
        .paxos_driver
        .mk_start_new_node(&SlavePaxosContext { io_ctx, this_eid: &self.this_eid });

      // Construct the snapshot.
      let mut snapshot = SlaveSnapshot {
        this_sid: self.this_sid.clone(),
        coord_positions: io_ctx.all_cids(),
        gossip: self.gossip.as_ref().clone(),
        // We do not need to transer the version of the LeaderMap, since that is
        // primarily used as an optimization by the NetworkDriver.
        leader_map: self.leader_map.value().clone(),
        paxos_driver_start,
        create_table_ess: Default::default(),
        shard_split_ess: Default::default(),
        tablet_snapshots: Default::default(),
      };

      // Add in the CreateTableRMES that have at least been Prepared.
      for (qid, es) in &statuses.create_table_ess {
        if let Some(es) = es.reconfig_snapshot() {
          snapshot.create_table_ess.insert(qid.clone(), es);
        }
      }

      // Add in the ShardSplitSlaveRMES that have at least been Prepared.
      for (qid, es) in &statuses.shard_split_ess {
        if let Some(es) = es.reconfig_snapshot() {
          snapshot.shard_split_ess.insert(qid.clone(), es);
        }
      }

      // Request the Tablets to send back a TabletSnapshot
      for tid in io_ctx.all_tids() {
        io_ctx.tablet_forward(&tid, TabletForwardMsg::ConstructTabletSnapshot);
      }

      // If there are no Tablets in this Slave, then we can send off the SlaveSnapshot
      // early. (Otherwise, we must wait).
      let num_tablets = io_ctx.num_tablets();
      if num_tablets == 0 {
        for new_eid in &non_started_eids {
          io_ctx.send(
            new_eid,
            msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::SlaveSnapshot(snapshot.clone())),
          )
        }
      } else {
        statuses.pending_snapshot = Some((snapshot, non_started_eids, num_tablets));
      }
    }
  }

  /// Handles the actions produced by a CreateTableES.
  fn handle_create_table_es_action<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: CreateTableRMAction,
  ) {
    match action {
      CreateTableRMAction::Wait => {}
      CreateTableRMAction::Exit(maybe_commit_action) => {
        statuses.create_table_ess.remove(&query_id);
        if let Some(helper) = maybe_commit_action {
          // This means the ES had Committed, and we should use this `helper`
          // to construct the Tablet.
          let this_tid = helper.this_tid.clone();
          io_ctx.create_tablet(helper);
          // We amend tablet_bundles with an initial value, as per SharedPaxosInserter
          self.tablet_bundles.insert(this_tid, TabletBundle::default());
        }
      }
    }
  }

  /// Handles the actions produced by a ShardSplitSlaveRMES.
  fn handle_shard_split_es_action<IO: SlaveIOCtx>(
    &mut self,
    _: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: ShardSplitSlaveRMAction,
  ) {
    match action {
      ShardSplitSlaveRMAction::Wait => {}
      ShardSplitSlaveRMAction::Exit(maybe_commit_action) => {
        statuses.shard_split_ess.remove(&query_id);
        if let Some(_) = maybe_commit_action {
          // This means the ES had Committed
          // TODO: finish.
        }
      }
    }
  }

  /// Checks if the incoming `master_gossip` has a more recent `gen`, and starts inserting
  /// that into the `slave_bundle` if so.
  fn handle_master_gossip(&mut self, master_gossip: msg::MasterGossip) {
    let incoming_gossip = master_gossip.gossip_data;
    let incoming_leader_map = master_gossip.leader_map;
    if self.gossip.get_gen() < incoming_gossip.get_gen() {
      if let Some((cur_next_gossip, _)) = &self.slave_bundle.gossip_data {
        // Check if there is an existing GossipData about to be inserted.
        if cur_next_gossip.get_gen() < incoming_gossip.get_gen() {
          self.slave_bundle.gossip_data = Some((incoming_gossip, incoming_leader_map));
        }
      } else {
        self.slave_bundle.gossip_data = Some((incoming_gossip, incoming_leader_map));
      }
    }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.value().get(&self.this_gid).unwrap();
    lid.eid == self.this_eid
  }
}
