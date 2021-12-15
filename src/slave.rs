use crate::common::{BasicIOCtx, GossipData, RemoteLeaderChangedPLm, SlaveIOCtx};
use crate::coord::CoordForwardMsg;
use crate::create_table_rm_es::CreateTableRMES;
use crate::create_table_tm_es::CreateTablePayloadTypes;
use crate::model::common::{
  CoordGroupId, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, SlaveGroupId, TabletGroupId,
};
use crate::model::common::{EndpointId, QueryId};
use crate::model::message as msg;
use crate::network_driver::{NetworkDriver, NetworkDriverContext};
use crate::paxos::{PaxosContextBase, PaxosDriver, PaxosTimerEvent};
use crate::server::{MainSlaveServerContext, ServerContextBase};
use crate::stmpaxos2pc_rm::{handle_rm_msg, handle_rm_plm, STMPaxos2PCRMAction};
use crate::stmpaxos2pc_tm as paxos2pc;
use crate::stmpaxos2pc_tm::RMServerContext;
use crate::tablet::{TabletBundle, TabletForwardMsg};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[path = "./slave_test.rs"]
pub mod slave_test;

// -----------------------------------------------------------------------------------------------
//  SlavePLm
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct SlaveBundle {
  remote_leader_changes: Vec<RemoteLeaderChangedPLm>,
  gossip_data: Option<GossipData>,
  pub plms: Vec<SlavePLm>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlavePLm {
  CreateTable(paxos2pc::RMPLm<CreateTablePayloadTypes>),
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

  fn defer(&mut self, defer_time: u128, timer_event: PaxosTimerEvent) {
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
  GossipData(Arc<GossipData>),
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
}

/// Messages deferred by the Slave to be run on the Slave.
#[derive(Debug)]
pub enum SlaveTimerInput {
  PaxosTimerEvent(PaxosTimerEvent),
  /// This is used to periodically propagate out RemoteLeaderChanged. It is
  /// only used by the Leader.
  RemoteLeaderChanged,
}

pub enum FullSlaveInput {
  SlaveMessage(msg::SlaveMessage),
  SlaveBackMessage(SlaveBackMessage),
  SlaveTimerInput(SlaveTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  Constants
// -----------------------------------------------------------------------------------------------

pub const REMOTE_LEADER_CHANGED_PERIOD: u128 = 4;

// -----------------------------------------------------------------------------------------------
//  Status
// -----------------------------------------------------------------------------------------------

/// This contains every Slave Status. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  create_table_ess: BTreeMap<QueryId, CreateTableRMES>,
}

// -----------------------------------------------------------------------------------------------
//  RMServerContext
// -----------------------------------------------------------------------------------------------

impl RMServerContext<CreateTablePayloadTypes> for SlaveContext {
  fn push_plm(&mut self, plm: SlavePLm) {
    self.slave_bundle.plms.push(plm);
  }

  fn send_to_tm<IO: BasicIOCtx>(&mut self, io_ctx: &mut IO, _: &(), msg: msg::MasterRemotePayload) {
    self.ctx(io_ctx).send_to_master(msg);
  }

  fn mk_node_path(&self) -> SlaveGroupId {
    self.this_sid.clone()
  }

  fn is_leader(&self) -> bool {
    SlaveContext::is_leader(self)
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
  pub this_sid: SlaveGroupId,
  pub this_gid: PaxosGroupId, // self.this_sid.to_gid()
  pub this_eid: EndpointId,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// LeaderMap
  pub leader_map: BTreeMap<PaxosGroupId, LeadershipId>,

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

  /// This should be called at the very start of the life of a Master node. This
  /// will start the timer events, paxos insertion, etc.
  pub fn bootstrap<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO) {
    debug_assert_eq!(io_ctx.num_tablets(), 0);
    let ctx = &mut SlavePaxosContext { io_ctx, this_eid: &self.ctx.this_eid };
    if self.ctx.is_leader() {
      // Start the Paxos insertion cycle with an empty bundle. Recall that since Slaves
      // start with no Tablets, the SharedPaxosBundle is trivially constructible.
      self.ctx.paxos_driver.insert_bundle(ctx, SharedPaxosBundle::default());
    }

    // Start Paxos Timer Events
    self.ctx.paxos_driver.timer_event(ctx, PaxosTimerEvent::LeaderHeartbeat);
    self.ctx.paxos_driver.timer_event(ctx, PaxosTimerEvent::NextIndex);

    // Broadcast Gossip data
    self.ctx.handle_input(
      io_ctx,
      &mut self.statuses,
      SlaveForwardMsg::SlaveTimerInput(SlaveTimerInput::RemoteLeaderChanged),
    )
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
}

impl SlaveContext {
  pub fn new(
    coord_positions: Vec<CoordGroupId>,
    this_sid: SlaveGroupId,
    this_eid: EndpointId,
    gossip: Arc<GossipData>,
    leader_map: BTreeMap<PaxosGroupId, LeadershipId>,
  ) -> SlaveContext {
    let all_gids = leader_map.keys().cloned().collect();
    let paxos_nodes = gossip.slave_address_config.get(&this_sid).unwrap().clone();
    SlaveContext {
      coord_positions,
      this_sid: this_sid.clone(),
      this_gid: this_sid.to_gid(),
      this_eid,
      gossip,
      leader_map,
      network_driver: NetworkDriver::new(all_gids),
      slave_bundle: Default::default(),
      tablet_bundles: Default::default(),
      paxos_driver: PaxosDriver::new(paxos_nodes),
    }
  }

  pub fn ctx<'a, IO: BasicIOCtx>(&'a self, io_ctx: &'a mut IO) -> MainSlaveServerContext<'a, IO> {
    MainSlaveServerContext {
      io_ctx,
      this_sid: &self.this_sid,
      this_eid: &self.this_eid,
      leader_map: &self.leader_map,
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
          if &lid.gen > &self.leader_map.get(&gid).unwrap().gen {
            // If the incoming RemoteLeaderChanged would increase the generation
            // in LeaderMap, then persist it.
            self.slave_bundle.remote_leader_changes.push(RemoteLeaderChangedPLm { gid, lid })
          }
        }
      }
      msg::SlaveMessage::PaxosDriverMessage(paxos_message) => {
        let bundles = self.paxos_driver.handle_paxos_message(
          &mut SlavePaxosContext { io_ctx, this_eid: &self.this_eid },
          paxos_message,
        );
        for shared_bundle in bundles {
          match shared_bundle {
            msg::PLEntry::Bundle(shared_bundle) => {
              let all_tids = io_ctx.all_tids();
              let all_cids = io_ctx.all_cids();
              let slave_bundle = shared_bundle.slave;

              // We buffer the SlaveForwardMsgs and execute them at the end.
              let mut slave_forward_msgs = Vec::<SlaveForwardMsg>::new();

              // Dispatch RemoteLeaderChanges
              for remote_change in slave_bundle.remote_leader_changes.clone() {
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
              if let Some(gossip_data) = slave_bundle.gossip_data {
                if self.gossip.gen < gossip_data.gen {
                  let gossip = Arc::new(gossip_data);
                  slave_forward_msgs.push(SlaveForwardMsg::GossipData(gossip.clone()));
                  for tid in &all_tids {
                    let forward_msg = TabletForwardMsg::GossipData(gossip.clone());
                    io_ctx.tablet_forward(&tid, forward_msg);
                  }
                  for cid in &all_cids {
                    let forward_msg = CoordForwardMsg::GossipData(gossip.clone());
                    io_ctx.coord_forward(&cid, forward_msg);
                  }
                }
              }

              // Dispatch the PaxosBundles
              slave_forward_msgs.push(SlaveForwardMsg::SlaveBundle(slave_bundle.plms));
              for (tid, tablet_bundle) in shared_bundle.tablet {
                let forward_msg = TabletForwardMsg::TabletBundle(tablet_bundle);
                io_ctx.tablet_forward(&tid, forward_msg);
              }

              // Dispatch any messages that were buffered in the NetworkDriver.
              // Note: we must do this after RemoteLeaderChanges. Also note that there will
              // be no payloads in the NetworkBuffer if this nodes is a Follower.
              for remote_change in slave_bundle.remote_leader_changes {
                if remote_change.lid.gen == self.leader_map.get(&remote_change.gid).unwrap().gen {
                  // We need this guard, since one Bundle can hold multiple `RemoteLeaderChanged`s
                  // for the same `gid` with different `gen`s.
                  let payloads = self
                    .network_driver
                    .deliver_blocked_messages(remote_change.gid, remote_change.lid);
                  for payload in payloads {
                    slave_forward_msgs.push(SlaveForwardMsg::SlaveRemotePayload(payload))
                  }
                }
              }

              // Forward to Slave Backend
              for forward_msg in slave_forward_msgs {
                self.handle_input(io_ctx, statuses, forward_msg);
              }
            }
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
            }
          }
        }
      }
    }
  }

  /// Handles inputs the Slave Backend.
  fn handle_input<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    slave_input: SlaveForwardMsg,
  ) {
    match slave_input {
      SlaveForwardMsg::SlaveBackMessage(slave_back_msg) => match slave_back_msg {
        SlaveBackMessage::TabletBundleInsertion(insert) => {
          if self.leader_map.get(&self.this_gid).unwrap() == &insert.lid {
            self.tablet_bundles.insert(insert.tid, insert.bundle);
            self.maybe_start_insert(io_ctx);
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
          io_ctx.defer(REMOTE_LEADER_CHANGED_PERIOD, SlaveTimerInput::RemoteLeaderChanged);
        }
      },
      SlaveForwardMsg::SlaveBundle(bundle) => {
        for paxos_log_msg in bundle {
          match paxos_log_msg {
            SlavePLm::CreateTable(plm) => {
              let (query_id, action) =
                handle_rm_plm(self, io_ctx, &mut statuses.create_table_ess, plm);
              self.handle_create_table_es_action(io_ctx, statuses, query_id, action);
            }
          }
        }

        if self.is_leader() {
          // Inform all ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.create_table_ess {
            es.start_inserting(self, io_ctx);
          }

          // Continue the insert cycle if there are no Tablets.
          self.maybe_start_insert(io_ctx);
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
              handle_rm_msg(self, io_ctx, &mut statuses.create_table_ess, message);
            self.handle_create_table_es_action(io_ctx, statuses, query_id, action);
          }
          msg::SlaveRemotePayload::MasterGossip(master_gossip) => {
            let incoming_gossip = master_gossip.gossip_data;
            if self.gossip.gen < incoming_gossip.gen {
              if let Some(cur_next_gossip) = &self.slave_bundle.gossip_data {
                // Check if there is an existing GossipData about to be inserted.
                if cur_next_gossip.gen < incoming_gossip.gen {
                  self.slave_bundle.gossip_data = Some(incoming_gossip);
                }
              } else {
                self.slave_bundle.gossip_data = Some(incoming_gossip);
              }
            }
          }
          msg::SlaveRemotePayload::TabletMessage(tid, tablet_msg) => {
            io_ctx.tablet_forward(&tid, TabletForwardMsg::TabletMessage(tablet_msg))
          }
          msg::SlaveRemotePayload::CoordMessage(cid, coord_msg) => {
            io_ctx.coord_forward(&cid, CoordForwardMsg::CoordMessage(coord_msg))
          }
        }
      }
      SlaveForwardMsg::GossipData(gossip) => {
        self.gossip = gossip;
      }
      SlaveForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();
        if lid.gen > self.leader_map.get(&gid).unwrap().gen {
          // Only update the LeadershipId if the new one increases the old one.
          self.leader_map.insert(gid.clone(), lid.clone());
        }
      }
      SlaveForwardMsg::LeaderChanged(leader_changed) => {
        let this_gid = self.this_sid.to_gid();
        self.leader_map.insert(this_gid, leader_changed.lid); // Update the LeadershipId

        if self.is_leader() {
          // By the SharedPaxosInserter, these must be empty at the start of Leadership.
          self.slave_bundle = SlaveBundle::default();
          self.tablet_bundles = BTreeMap::default();
        }

        // Inform CreateQueryES
        let query_ids: Vec<QueryId> = statuses.create_table_ess.keys().cloned().collect();
        for query_id in query_ids {
          let create_query_es = statuses.create_table_ess.get_mut(&query_id).unwrap();
          let action = create_query_es.leader_changed(self);
          self.handle_create_table_es_action(io_ctx, statuses, query_id.clone(), action);
        }

        // Inform the NetworkDriver
        self.network_driver.leader_changed();

        if self.is_leader() {
          // This node is the new Leader
          self.broadcast_leadership(io_ctx); // Broadcast RemoteLeaderChanged
          self.maybe_start_insert(io_ctx); // Start the insert cycle if there are no Tablets.
        }
      }
    }
  }

  /// Used to broadcast out `RemoteLeaderChanged` to all other
  /// PaxosGroups to help maintain their LeaderMaps.
  fn broadcast_leadership<IO: BasicIOCtx<msg::NetworkMessage>>(&self, io_ctx: &mut IO) {
    let this_lid = self.leader_map.get(&self.this_gid).unwrap().clone();
    for (sid, eids) in &self.gossip.slave_address_config {
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
    for eid in &self.gossip.master_address_config {
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
  fn maybe_start_insert<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO) {
    if self.tablet_bundles.len() == io_ctx.num_tablets() {
      let shared_bundle = SharedPaxosBundle {
        slave: std::mem::replace(&mut self.slave_bundle, SlaveBundle::default()),
        tablet: std::mem::replace(&mut self.tablet_bundles, BTreeMap::default()),
      };
      self
        .paxos_driver
        .insert_bundle(&mut SlavePaxosContext { io_ctx, this_eid: &self.this_eid }, shared_bundle);
    }
  }

  /// Handles the actions produced by a CreateTableES.
  fn handle_create_table_es_action<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: STMPaxos2PCRMAction,
  ) {
    match action {
      STMPaxos2PCRMAction::Wait => {}
      STMPaxos2PCRMAction::Exit => {
        let es = statuses.create_table_ess.remove(&query_id).unwrap();
        if let Some(helper) = es.inner.committed_helper {
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

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&self.this_gid).unwrap();
    lid.eid == self.this_eid
  }
}
