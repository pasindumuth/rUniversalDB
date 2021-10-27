use crate::common::{
  lookup, lookup_pos, map_insert, merge_table_views, mk_qid, remove_item, BasicIOCtx, GossipData,
  OrigP, RemoteLeaderChangedPLm, SlaveIOCtx, TMStatus, UUID,
};
use crate::coord::CoordForwardMsg;
use crate::create_table_rm_es::CreateTableRMES;
use crate::create_table_tm_es::CreateTablePayloadTypes;
use crate::model::common::{
  iast, proc, CTQueryPath, ColName, ColType, Context, ContextRow, CoordGroupId, Gen, LeadershipId,
  NodeGroupId, PaxosGroupId, SlaveGroupId, TablePath, TableView, TabletGroupId, TabletKeyRange,
  TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{EndpointId, QueryId};
use crate::model::message as msg;
use crate::network_driver::{NetworkDriver, NetworkDriverContext};
use crate::paxos::{PaxosContextBase, PaxosDriver, PaxosTimerEvent};
use crate::server::{MainSlaveServerContext, ServerContextBase};
use crate::stmpaxos2pc_rm::{handle_rm_msg, handle_rm_plm, STMPaxos2PCRMAction};
use crate::stmpaxos2pc_tm as paxos2pc;
use crate::stmpaxos2pc_tm::{Closed, RMMessage, RMPLm, RMServerContext, TMMessage};
use crate::tablet::{GRQueryESWrapper, TabletBundle, TabletForwardMsg, TransTableReadESWrapper};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
  tablet: HashMap<TabletGroupId, TabletBundle>,
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
pub enum SlaveBackMessage {
  TabletBundle(TabletGroupId, TabletBundle),
}

/// Messages deferred by the Slave to be run on the Slave.
#[derive(Debug)]
pub enum SlaveTimerInput {
  PaxosTimerEvent(PaxosTimerEvent),
}

pub enum FullSlaveInput {
  SlaveMessage(msg::SlaveMessage),
  SlaveBackMessage(SlaveBackMessage),
  SlaveTimerInput(SlaveTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  Status
// -----------------------------------------------------------------------------------------------

/// This contains every Slave Status. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  create_table_ess: HashMap<QueryId, CreateTableRMES>,
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
  pub context: SlaveContext,
  pub statuses: Statuses,
}

/// The SlaveState that holds all the state of the Slave
#[derive(Debug)]
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
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  /// NetworkDriver
  pub network_driver: NetworkDriver<msg::SlaveRemotePayload>,

  // Paxos
  pub slave_bundle: SlaveBundle,
  /// After a `SharedPaxosBundle` is inserted, this is cleared.
  pub tablet_bundles: HashMap<TabletGroupId, TabletBundle>,
  pub paxos_driver: PaxosDriver<SharedPaxosBundle>,
}

impl SlaveState {
  pub fn new(slave_context: SlaveContext) -> SlaveState {
    SlaveState { context: slave_context, statuses: Default::default() }
  }

  pub fn handle_input<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO, input: FullSlaveInput) {
    match input {
      FullSlaveInput::SlaveMessage(message) => {
        self.context.handle_incoming_message(io_ctx, &mut self.statuses, message);
      }
      FullSlaveInput::SlaveBackMessage(message) => {
        /// Handles messages that were send from the Tablets back to the Slave.
        let forward_msg = SlaveForwardMsg::SlaveBackMessage(message);
        self.context.handle_input(io_ctx, &mut self.statuses, forward_msg);
      }
      FullSlaveInput::SlaveTimerInput(timer_input) => {
        let forward_msg = SlaveForwardMsg::SlaveTimerInput(timer_input);
        self.context.handle_input(io_ctx, &mut self.statuses, forward_msg);
      }
    }
  }
}

impl SlaveContext {
  pub fn new(
    coord_positions: Vec<CoordGroupId>,
    this_slave_group_id: SlaveGroupId,
    this_eid: EndpointId,
    gossip: Arc<GossipData>,
    leader_map: HashMap<PaxosGroupId, LeadershipId>,
  ) -> SlaveContext {
    let all_gids = leader_map.keys().cloned().collect();
    let paxos_nodes = gossip.slave_address_config.get(&this_slave_group_id).unwrap().clone();
    SlaveContext {
      coord_positions,
      this_sid: this_slave_group_id.clone(),
      this_gid: this_slave_group_id.to_gid(),
      this_eid,
      gossip,
      leader_map,
      network_driver: NetworkDriver::new(all_gids),
      slave_bundle: Default::default(),
      tablet_bundles: Default::default(),
      paxos_driver: PaxosDriver::new(paxos_nodes),
    }
  }

  pub fn ctx<'a, IO: BasicIOCtx>(
    &'a mut self,
    io_ctx: &'a mut IO,
  ) -> MainSlaveServerContext<'a, IO> {
    MainSlaveServerContext {
      io_ctx,
      this_slave_group_id: &self.this_sid,
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
      msg::SlaveMessage::ExternalMessage(request) => {
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
                let payloads = self
                  .network_driver
                  .deliver_blocked_messages(remote_change.gid, remote_change.lid);
                for payload in payloads {
                  slave_forward_msgs.push(SlaveForwardMsg::SlaveRemotePayload(payload))
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
        SlaveBackMessage::TabletBundle(tid, tablet_bundle) => {
          self.tablet_bundles.insert(tid, tablet_bundle);
          if self.tablet_bundles.len() == io_ctx.num_tablets() {
            let shared_bundle = SharedPaxosBundle {
              slave: std::mem::replace(&mut self.slave_bundle, SlaveBundle::default()),
              tablet: std::mem::replace(&mut self.tablet_bundles, HashMap::default()),
            };
            self.paxos_driver.insert_bundle(
              &mut SlavePaxosContext { io_ctx, this_eid: &self.this_eid },
              shared_bundle,
            );
          }
        }
      },
      SlaveForwardMsg::SlaveTimerInput(timer_input) => match timer_input {
        SlaveTimerInput::PaxosTimerEvent(timer_event) => {
          self
            .paxos_driver
            .timer_event(&mut SlavePaxosContext { io_ctx, this_eid: &self.this_eid }, timer_event);
        }
      },
      SlaveForwardMsg::SlaveBundle(bundle) => {
        if self.is_leader() {
          for paxos_log_msg in bundle {
            match paxos_log_msg {
              SlavePLm::CreateTable(plm) => {
                let (query_id, action) =
                  handle_rm_plm(self, io_ctx, &mut statuses.create_table_ess, plm);
                self.handle_create_table_es_action(io_ctx, statuses, query_id, action);
              }
            }
          }

          // Inform all ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.create_table_ess {
            es.start_inserting(self, io_ctx);
          }
        } else {
          for paxos_log_msg in bundle {
            match paxos_log_msg {
              SlavePLm::CreateTable(plm) => {
                let (query_id, action) =
                  handle_rm_plm(self, io_ctx, &mut statuses.create_table_ess, plm);
                self.handle_create_table_es_action(io_ctx, statuses, query_id, action);
              }
            }
          }
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
          msg::SlaveRemotePayload::RemoteLeaderChanged(_) => {}
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
        self.leader_map.insert(gid.clone(), lid.clone()); // Update the LeadershipId
      }
      SlaveForwardMsg::LeaderChanged(leader_changed) => {
        let this_gid = self.this_sid.to_gid();
        self.leader_map.insert(this_gid, leader_changed.lid); // Update the LeadershipId

        // Inform CreateQueryES
        let query_ids: Vec<QueryId> = statuses.create_table_ess.keys().cloned().collect();
        for query_id in query_ids {
          let create_query_es = statuses.create_table_ess.get_mut(&query_id).unwrap();
          let action = create_query_es.leader_changed(self);
          self.handle_create_table_es_action(io_ctx, statuses, query_id.clone(), action);
        }

        // Inform the NetworkDriver
        self.network_driver.leader_changed();

        if !self.is_leader() {
          // Clear SlaveBundle
          self.slave_bundle = SlaveBundle::default();
        }
      }
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
          io_ctx.create_tablet(helper);
        }
      }
    }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&self.this_sid.to_gid()).unwrap();
    lid.eid == self.this_eid
  }
}
