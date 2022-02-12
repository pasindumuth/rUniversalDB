use crate::common::{remove_item, update_all_eids, MasterIOCtx, RemoteLeaderChangedPLm};
use crate::master::{MasterContext, MasterPLm};
use crate::model::common::{EndpointId, PaxosGroupId, PaxosGroupIdTrait, SlaveGroupId};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  PLms
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ReconfigSlaveGroup {
  sid: SlaveGroupId,
  new_eids: Vec<EndpointId>,
  rem_eids: Vec<EndpointId>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SlaveGroupReconfigured {
  sid: SlaveGroupId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveReconfigPLm {
  Reconfig(ReconfigSlaveGroup),
  Reconfigured(SlaveGroupReconfigured),
}

// -----------------------------------------------------------------------------------------------
//  SlaveReconfigES
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
enum State {
  Follower { new_eids: Vec<EndpointId> },
  WaitingRequestedNodes,
  InsertingReconfig { new_eids: Vec<EndpointId> },
  Reconfig { new_eids: Vec<EndpointId> },
  InsertingCompletion { new_eids: Vec<EndpointId> },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SlaveReconfigES {
  sid: SlaveGroupId,
  rem_eids: Vec<EndpointId>,
  state: State,
}

impl SlaveReconfigES {
  /// This is called by the Leader
  fn new(ctx: &mut MasterContext, sid: SlaveGroupId, rem_eids: Vec<EndpointId>) -> SlaveReconfigES {
    debug_assert!(ctx.is_leader());
    // Request new `EndpointId`s from the FreeNodeManager
    ctx.free_node_manager.request_new_eids(sid.to_gid(), rem_eids.len());
    SlaveReconfigES { sid, rem_eids, state: State::WaitingRequestedNodes }
  }

  /// This is only called by a follower
  fn new_follower(ctx: &mut MasterContext, plm: ReconfigSlaveGroup) -> SlaveReconfigES {
    debug_assert!(!ctx.is_leader());
    SlaveReconfigES {
      sid: plm.sid,
      rem_eids: plm.rem_eids,
      state: State::Follower { new_eids: plm.new_eids },
    }
  }

  /// This is called `FreeNodeManager` provides the necessary `EndpointId`s
  pub fn handle_eids_granted(&mut self, ctx: &mut MasterContext, new_eids: Vec<EndpointId>) {
    match &self.state {
      State::WaitingRequestedNodes => {
        // Amend MasterBundle
        ctx.master_bundle.plms.push(MasterPLm::SlaveConfigPLm(SlaveReconfigPLm::Reconfig(
          ReconfigSlaveGroup {
            sid: self.sid.clone(),
            new_eids: new_eids.clone(),
            rem_eids: self.rem_eids.clone(),
          },
        )));

        // Advance
        self.state = State::InsertingReconfig { new_eids };
      }
      _ => {}
    }
  }

  fn handle_reconfig_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    _: ReconfigSlaveGroup,
  ) {
    match &mut self.state {
      State::InsertingReconfig { new_eids } => {
        // Send a `ReconfigSlaveGroup`
        let new_eids = std::mem::take(new_eids);
        self.send_reconfig_slave_group(ctx, io_ctx, &new_eids);
        self.state = State::Reconfig { new_eids }
      }
      _ => {}
    }
  }

  fn handle_reconfigured_msg<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    _: msg::SlaveGroupReconfigured,
  ) {
    match &mut self.state {
      State::Reconfig { new_eids } => {
        // Amend MasterBundle
        ctx.master_bundle.plms.push(MasterPLm::SlaveConfigPLm(SlaveReconfigPLm::Reconfigured(
          SlaveGroupReconfigured { sid: self.sid.clone() },
        )));

        // Advance
        self.state = State::InsertingCompletion { new_eids: std::mem::take(new_eids) }
      }
      _ => {}
    }
  }

  /// This is the final transition for this `ES`; it should be cleaned up afterwards.
  fn handle_reconfigured_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    _: SlaveGroupReconfigured,
  ) {
    match &self.state {
      State::Follower { new_eids } | State::InsertingCompletion { new_eids } => {
        // Update the GossipData
        ctx.gossip.update(|gossip| {
          let paxos_nodes = gossip.slave_address_config.get_mut(&self.sid).unwrap();
          for eid in new_eids {
            paxos_nodes.push(eid.clone());
          }
          for eid in &self.rem_eids {
            remove_item(paxos_nodes, eid);
          }
        });

        // Update the `all_eids`
        update_all_eids(&mut ctx.all_eids, &self.rem_eids, new_eids.clone());

        if ctx.is_leader() {
          // Broadcast GossipData
          ctx.broadcast_gossip(io_ctx);

          // Send ShutdownNode to ensure the removed nodes are gone
          for eid in &self.rem_eids {
            io_ctx.send(eid, msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::ShutdownNode));
          }
        }
      }
      _ => debug_assert!(false),
    }
  }

  fn remote_leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    gid: &PaxosGroupId,
  ) {
    match &self.state {
      State::Reconfig { new_eids } => {
        // If the `sid` had a Leadership change, we contact it again.
        if gid == &self.sid.to_gid() {
          self.send_reconfig_slave_group(ctx, io_ctx, &new_eids);
        }
      }
      _ => {}
    }
  }

  /// Returns `true` iff this should be exited.
  fn leader_changed<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) -> bool {
    match &mut self.state {
      State::Follower { new_eids } => {
        if ctx.is_leader() {
          // Send a `ReconfigSlaveGroup`
          let new_eids = std::mem::take(new_eids);
          self.send_reconfig_slave_group(ctx, io_ctx, &new_eids);
          self.state = State::Reconfig { new_eids };
        }
        false
      }
      State::WaitingRequestedNodes | State::InsertingReconfig { .. } => true,
      State::Reconfig { new_eids } | State::InsertingCompletion { new_eids } => {
        self.state = State::Follower { new_eids: std::mem::take(new_eids) };
        false
      }
    }
  }

  /// If this node is a Follower, a copy of this `SlaveReconfigES` is returned. If this
  /// node is a Leader, then the value of this `SlaveReconfigES` that would result from
  /// losing Leadership is returned (i.e. after calling `leader_changed`).
  fn reconfig_snapshot(&self) -> Option<SlaveReconfigES> {
    match &self.state {
      State::WaitingRequestedNodes | State::InsertingReconfig { .. } => None,
      State::Follower { new_eids }
      | State::Reconfig { new_eids }
      | State::InsertingCompletion { new_eids } => Some(SlaveReconfigES {
        sid: self.sid.clone(),
        rem_eids: self.rem_eids.clone(),
        state: State::Follower { new_eids: new_eids.clone() },
      }),
    }
  }

  // Helpers

  /// Used to chase the `self.sid` for sending `msg::ReconfigSlaveGroup`.
  fn send_reconfig_slave_group<IO: MasterIOCtx>(
    &self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    new_eids: &Vec<EndpointId>,
  ) {
    ctx.ctx(io_ctx).send_to_slave_common(
      self.sid.clone(),
      msg::SlaveRemotePayload::ReconfigSlaveGroup(msg::ReconfigSlaveGroup {
        new_eids: new_eids.clone(),
        rem_eids: self.rem_eids.clone(),
      }),
    );
  }
}

// -----------------------------------------------------------------------------------------------
//  ES Container Functions
// -----------------------------------------------------------------------------------------------

// Leader-only

pub fn handle_msg<IO: MasterIOCtx>(
  ctx: &mut MasterContext,
  io_ctx: &mut IO,
  slave_reconfig_ess: &mut BTreeMap<SlaveGroupId, SlaveReconfigES>,
  reconfig_msg: msg::SlaveReconfig,
) {
  match reconfig_msg {
    msg::SlaveReconfig::NodesDead(nodes_dead) => {
      // Construct an `SlaveReconfigES` if it does not already exist.
      let sid = nodes_dead.sid;
      if !slave_reconfig_ess.contains_key(&sid) {
        slave_reconfig_ess.insert(sid.clone(), SlaveReconfigES::new(ctx, sid, nodes_dead.eids));
      }
    }
    msg::SlaveReconfig::SlaveGroupReconfigured(reconfigured) => {
      if let Some(es) = slave_reconfig_ess.get_mut(&reconfigured.sid) {
        es.handle_reconfigured_msg(ctx, io_ctx, reconfigured);
      }
    }
  }
}

// Leader and Follower

pub fn handle_plm<IO: MasterIOCtx>(
  ctx: &mut MasterContext,
  io_ctx: &mut IO,
  slave_reconfig_ess: &mut BTreeMap<SlaveGroupId, SlaveReconfigES>,
  reconfig_plm: SlaveReconfigPLm,
) {
  match reconfig_plm {
    SlaveReconfigPLm::Reconfig(reconfig) => {
      let sid = reconfig.sid.clone();
      if ctx.is_leader() {
        let es = slave_reconfig_ess.get_mut(&sid).unwrap();
        es.handle_reconfig_plm(ctx, io_ctx, reconfig);
      } else {
        // This is a backup, so we should make a `SlaveReconfigES`.
        let es = SlaveReconfigES::new_follower(ctx, reconfig);
        slave_reconfig_ess.insert(sid, es);
      }
    }
    SlaveReconfigPLm::Reconfigured(reconfigured) => {
      // Here, we remove the ES and then finish it off.
      let mut es = slave_reconfig_ess.remove(&reconfigured.sid).unwrap();
      es.handle_reconfigured_plm(ctx, io_ctx, reconfigured);
    }
  }
}

pub fn handle_remote_leader_changed<IO: MasterIOCtx>(
  ctx: &mut MasterContext,
  io_ctx: &mut IO,
  slave_reconfig_ess: &mut BTreeMap<SlaveGroupId, SlaveReconfigES>,
  gid: &PaxosGroupId,
) {
  for (_, es) in slave_reconfig_ess {
    es.remote_leader_changed(ctx, io_ctx, gid);
  }
}

pub fn handle_leader_changed<IO: MasterIOCtx>(
  ctx: &mut MasterContext,
  io_ctx: &mut IO,
  slave_reconfig_ess: &mut BTreeMap<SlaveGroupId, SlaveReconfigES>,
) {
  let sids: Vec<SlaveGroupId> = slave_reconfig_ess.keys().cloned().collect();
  for sid in sids {
    let es = slave_reconfig_ess.get_mut(&sid).unwrap();
    if es.leader_changed(ctx, io_ctx) {
      slave_reconfig_ess.remove(&sid);
    }
  }
}

/// Add in the `SlaveReconfigES` where at least `ReconfigSlaveGroup` PLm has been inserted.
pub fn handle_reconfig_snapshot(
  slave_reconfig_ess: &BTreeMap<SlaveGroupId, SlaveReconfigES>,
) -> BTreeMap<SlaveGroupId, SlaveReconfigES> {
  let mut ess = BTreeMap::<SlaveGroupId, SlaveReconfigES>::new();
  for (qid, es) in slave_reconfig_ess {
    if let Some(es) = es.reconfig_snapshot() {
      ess.insert(qid.clone(), es);
    }
  }
  ess
}
