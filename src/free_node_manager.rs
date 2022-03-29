use crate::common::{mk_cid, mk_sid, LeaderMap, MasterIOCtx, MasterTraceMessage};
use crate::master::plm::FreeNodeManagerPLm;
use crate::master::{MasterBundle, MasterConfig, MasterContext, MasterPLm};
use crate::model::common::{CoordGroupId, EndpointId, LeadershipId, PaxosGroupId, SlaveGroupId};
use crate::model::message as msg;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

// -----------------------------------------------------------------------------------------------
//  FreeNodeManager
// -----------------------------------------------------------------------------------------------

const HEARTBEAT_DEAD_THRESHOLD: u32 = 6;
const HEARTBEAT_BACKUP_VALUE: u32 = 3;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum FreeNodeType {
  NewSlaveFreeNode,
  ReconfigFreeNode,
}

#[derive(Debug)]
pub enum FreeNodeAction {
  GrantedReconfigEids(BTreeMap<SlaveGroupId, Vec<EndpointId>>),
  NewSlaveGroups(BTreeMap<SlaveGroupId, Vec<EndpointId>>),
}

#[derive(Debug)]
pub struct FreeNodeManager {
  free_nodes: BTreeMap<EndpointId, FreeNodeType>,
  pending_new_free_nodes: BTreeSet<(EndpointId, FreeNodeType)>,
  free_node_heartbeat: BTreeMap<EndpointId, u32>,
  requested_reconfig_eids: BTreeMap<PaxosGroupId, usize>,
}

impl FreeNodeManager {
  pub fn new() -> FreeNodeManager {
    FreeNodeManager {
      free_nodes: Default::default(),
      pending_new_free_nodes: Default::default(),
      free_node_heartbeat: Default::default(),
      requested_reconfig_eids: Default::default(),
    }
  }

  /// Handles a `free_nodes` to initiate a `FreeNodeManager` for a reconfigured node properly.
  pub fn create_reconfig(free_nodes: BTreeMap<EndpointId, FreeNodeType>) -> FreeNodeManager {
    let mut free_node_heartbeat = BTreeMap::<EndpointId, u32>::new();
    for (eid, _) in &free_nodes {
      free_node_heartbeat.insert(eid.clone(), 0);
    }
    FreeNodeManager {
      free_nodes,
      pending_new_free_nodes: Default::default(),
      free_node_heartbeat,
      requested_reconfig_eids: Default::default(),
    }
  }

  pub fn handle_register(&mut self, register: msg::RegisterFreeNode) {
    self.pending_new_free_nodes.insert((register.sender_eid, register.node_type));
  }

  pub fn handle_heartbeat(&mut self, ctx: &MasterContext, heartbeat: msg::FreeNodeHeartbeat) {
    debug_assert!(ctx.is_leader());
    // We filter the heartbeat for the current LeadershipId (this is only a formality).
    let cur_lid = ctx.leader_map.value().get(&PaxosGroupId::Master).unwrap();
    if &heartbeat.cur_lid == cur_lid {
      // Update the heartbeat count of the FreeNode still exists.
      if let Some(count) = self.free_node_heartbeat.get_mut(&heartbeat.sender_eid) {
        *count = 0;
      }
    }
  }

  pub fn handle_timer(&mut self, ctx: &MasterContext) {
    if ctx.is_leader() {
      for (_, count) in &mut self.free_node_heartbeat {
        *count += 1;
      }
    }
  }

  pub fn leader_changed(&mut self, ctx: &MasterContext) {
    // Check if we lost Leadership.
    if !ctx.is_leader() {
      self.pending_new_free_nodes.clear();

      // Set the heartbeats to a steady but non-zero value.
      for (_, count) in &mut self.free_node_heartbeat {
        *count = HEARTBEAT_BACKUP_VALUE;
      }

      // Clear grant requests
      self.requested_reconfig_eids.clear();
    }
  }

  /// Used by `SlaveReconfigES`s to request `count` many new nodes to reconfigure `sid` with.
  pub fn request_new_eids(&mut self, gid: PaxosGroupId, count: usize) {
    self.requested_reconfig_eids.insert(gid, count);
  }

  /// This returns the new SlaveGroup `EndpointId`s that we should use to create new Groups.
  pub fn handle_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &MasterContext,
    io_ctx: &mut IO,
    plm: FreeNodeManagerPLm,
  ) -> BTreeMap<SlaveGroupId, (Vec<EndpointId>, Vec<CoordGroupId>)> {
    // Add new nodes
    for (new_eid, node_type) in plm.new_nodes {
      self.free_nodes.insert(new_eid.clone(), node_type);
      self.free_node_heartbeat.insert(new_eid.clone(), 0);
      // Send back a FreeNodeRegistered message
      if ctx.is_leader() {
        let cur_lid = ctx.leader_map.value().get(&PaxosGroupId::Master).unwrap().clone();
        io_ctx.send(
          &new_eid,
          msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::FreeNodeRegistered(
            msg::FreeNodeRegistered { cur_lid },
          )),
        )
      }
    }

    // Remove dead nodes
    for old_eid in &plm.nodes_dead {
      self.free_nodes.remove(old_eid);
      self.free_node_heartbeat.remove(old_eid);
      // Send back a Shutdown, just to make sure they are dead.
      if ctx.is_leader() {
        io_ctx.send(old_eid, msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::ShutdownNode))
      }
    }

    // Remove free nodes that were given off for Reconfiguration
    for (_, eids) in plm.granted_reconfig_eids {
      for eid in eids {
        self.free_nodes.remove(&eid);
        self.free_node_heartbeat.remove(&eid);
      }
    }

    // Remove free nodes that were given off for creating new SlaveGroups
    for (sid, (eids, _)) in plm.new_slave_groups.clone() {
      // Trace the creation for testing
      let lid = LeadershipId::mk_first(eids.get(0).unwrap().clone());
      io_ctx.trace(MasterTraceMessage::SlaveCreated(sid.clone(), lid));

      // Remove the EndpointId as a FreeNode.
      for eid in eids {
        self.free_nodes.remove(&eid);
        self.free_node_heartbeat.remove(&eid);
      }
    }

    plm.new_slave_groups
  }

  /// This returns the requested `EndpointId`s that were granted by this manager. This
  /// granting still needs to be persisted (i.e. by the `FreeNodeManagerPLm` that is added
  /// to the MasterBundle here)
  ///
  /// Note that this should only be called if this is the Leader.
  pub fn process<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> BTreeMap<PaxosGroupId, Vec<EndpointId>> {
    debug_assert!(ctx.is_leader());

    // Construct the new PLm
    let mut plm = FreeNodeManagerPLm {
      new_slave_groups: Default::default(),
      new_nodes: vec![],
      nodes_dead: vec![],
      granted_reconfig_eids: Default::default(),
    };

    // Process all pending_free_nodes.
    for (new_eid, node_type) in std::mem::take(&mut self.pending_new_free_nodes) {
      plm.new_nodes.push((new_eid, node_type));
    }

    // See if any free nodes are dead.
    for (eid, count) in &self.free_node_heartbeat {
      if count >= &HEARTBEAT_DEAD_THRESHOLD {
        plm.nodes_dead.push(eid.clone());
      }
    }

    // Compute the set of nodes that will be available after this PLm is inserted.
    let mut available_reconfig_nodes = BTreeSet::<EndpointId>::new();
    let mut available_slave_nodes = BTreeSet::<EndpointId>::new();
    for (eid, node_type) in self.free_nodes.clone() {
      if self.free_node_heartbeat.get(&eid).unwrap() < &HEARTBEAT_BACKUP_VALUE {
        // This filter ensures that if this Master node just gained Leadership, then
        // we got a heartbeat from this `eid` since then.
        match node_type {
          FreeNodeType::ReconfigFreeNode => available_reconfig_nodes.insert(eid),
          FreeNodeType::NewSlaveFreeNode => available_slave_nodes.insert(eid),
        };
      }
    }
    for (eid, node_type) in self.free_nodes.clone().into_iter().chain(plm.new_nodes.clone()) {
      match node_type {
        FreeNodeType::ReconfigFreeNode => available_reconfig_nodes.insert(eid),
        FreeNodeType::NewSlaveFreeNode => available_slave_nodes.insert(eid),
      };
    }
    for eid in &plm.nodes_dead {
      available_reconfig_nodes.remove(eid);
      available_slave_nodes.remove(eid);
    }

    // Delegate out `available_reconfig_nodes` for reconfig
    // requests (also removing the satisfied requests).
    let mut it = available_reconfig_nodes.into_iter();
    'outer: for (gid, count) in self.requested_reconfig_eids.clone() {
      let mut reconfig_eids = Vec::<EndpointId>::new();
      for _ in 0..count {
        if let Some(eid) = it.next() {
          reconfig_eids.push(eid);
        } else {
          // There are no more nodes left to delegate, so break out.
          break 'outer;
        }
      }
      self.requested_reconfig_eids.remove(&gid);
      plm.granted_reconfig_eids.insert(gid, reconfig_eids);
    }

    // Delegate out `available_slave_nodes` for creating new SlaveGroups
    let mut it = available_slave_nodes.into_iter();
    'outer: loop {
      let mut new_slave_eids = Vec::<EndpointId>::new();
      for _ in 0..ctx.master_config.slave_group_size {
        if let Some(eid) = it.next() {
          new_slave_eids.push(eid);
        } else {
          // There are no more nodes left to delegate, so break out.
          break 'outer;
        }
      }
      let sid = mk_sid(&mut io_ctx.rand());
      let mut coord_ids = Vec::<CoordGroupId>::new();
      for _ in 0..ctx.master_config.num_coords {
        coord_ids.push(mk_cid(&mut io_ctx.rand()));
      }
      plm.new_slave_groups.insert(sid, (new_slave_eids, coord_ids));
    }

    // Add the PLm to the MasterBundle
    let granted_reconfig_eids = plm.granted_reconfig_eids.clone();
    ctx.master_bundle.plms.push(MasterPLm::FreeNodeManagerPLm(plm));

    // Return reconfig eids
    granted_reconfig_eids
  }

  // -----------------------------------------------------------------------------------------------
  //  Const Accessors
  // -----------------------------------------------------------------------------------------------

  pub fn free_nodes(&self) -> &BTreeMap<EndpointId, FreeNodeType> {
    &self.free_nodes
  }
}
