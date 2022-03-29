use crate::common::{mk_cid, mk_sid, update_all_eids, MasterIOCtx};
use crate::master::{MasterContext, MasterPLm};
use crate::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupIdTrait, SlaveGroupId,
};
use crate::model::message as msg;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

// -----------------------------------------------------------------------------------------------
//  PLms
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ConfirmCreateGroup {
  sid: SlaveGroupId,
}

// -----------------------------------------------------------------------------------------------
//  SlaveGroupCreateES
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
enum State {
  Follower,
  WaitingConfirmed(BTreeSet<EndpointId>),
  InsertingConfirmed,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct SlaveGroupCreateES {
  create_msg: msg::CreateSlaveGroup,
  paxos_nodes: Vec<EndpointId>,
  state: State,
}

impl SlaveGroupCreateES {
  /// Constructs an `ES`, sending out `CreateSlaveGroup` if this is the Master node.
  fn create<IO: MasterIOCtx>(
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    sid: SlaveGroupId,
    paxos_nodes: Vec<EndpointId>,
    coord_ids: Vec<CoordGroupId>,
  ) -> SlaveGroupCreateES {
    // Construct the `CreateSlaveGroup` message.
    let create_msg = msg::CreateSlaveGroup {
      gossip: ctx.gossip.clone(),
      leader_map: ctx.leader_map.value().clone(),
      sid,
      paxos_nodes: paxos_nodes.clone(),
      coord_ids,
    };

    // If this is the Leader, start the new Slave Nodes
    let state = if ctx.is_leader() {
      for eid in &paxos_nodes {
        io_ctx.send(
          eid,
          msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::CreateSlaveGroup(create_msg.clone())),
        )
      }
      State::WaitingConfirmed(BTreeSet::new())
    } else {
      // Otherwise, start in the `Follower` state.
      State::Follower
    };

    SlaveGroupCreateES { create_msg, paxos_nodes, state }
  }

  /// Handles the `ConfirmSlaveCreation` sent back by a node that successfully constructed itself.
  fn handle_confirm_msg<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    msg: msg::ConfirmSlaveCreation,
  ) {
    match &mut self.state {
      State::WaitingConfirmed(eids) => {
        // Add in the incoming `EndpointId`.
        debug_assert!(self.paxos_nodes.contains(&msg.sender_eid));
        eids.insert(msg.sender_eid.clone());

        // If a majority of nodes have responded, we can finish.
        if 2 * eids.len() > self.paxos_nodes.len() {
          ctx.master_bundle.plms.push(MasterPLm::ConfirmCreateGroup(ConfirmCreateGroup {
            sid: self.create_msg.sid.clone(),
          }));
          self.state = State::InsertingConfirmed;
        }
      }
      _ => {}
    }
  }

  /// Handles the insertion of the `ConfirmCreateGroup` PLm.
  fn handle_confirm_plm<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    match &self.state {
      State::Follower | State::InsertingConfirmed => {
        // Update the GossipData
        let sid = &self.create_msg.sid;
        let paxos_nodes = &self.create_msg.paxos_nodes;
        ctx.gossip.update(|gossip_data| {
          gossip_data.slave_address_config.insert(sid.clone(), paxos_nodes.clone())
        });

        // Update the LeaderMap
        let lid = LeadershipId { gen: Gen(0), eid: paxos_nodes.get(0).unwrap().clone() };
        ctx.leader_map.update(move |leader_map| {
          leader_map.insert(sid.to_gid(), lid);
        });

        // Update the `all_eids`
        update_all_eids(&mut ctx.all_eids, &vec![], self.create_msg.paxos_nodes.clone());

        if ctx.is_leader() {
          // Broadcast the GossipData.
          ctx.broadcast_gossip(io_ctx);
        }
      }
      State::WaitingConfirmed(_) => {}
    }
  }

  /// Handle the current (Master) leader changing.
  fn leader_changed<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    match &self.state {
      State::Follower => {
        if ctx.is_leader() {
          // Broadcast `CreateSlaveGroup` and then go to `WaitingConfirmed`.
          for eid in &self.paxos_nodes {
            io_ctx.send(
              eid,
              msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::CreateSlaveGroup(
                self.create_msg.clone(),
              )),
            )
          }
          self.state = State::WaitingConfirmed(BTreeSet::new())
        }
      }
      State::WaitingConfirmed(_) | State::InsertingConfirmed => {
        self.state = State::Follower;
      }
    }
  }

  /// If this node is a Follower, a copy of this `SlaveGroupCreateES` is returned. If this
  /// node is a Leader, then the value of this `SlaveGroupCreateES` that would result from
  /// losing Leadership is returned (i.e. after calling `leader_changed`).
  fn reconfig_snapshot(&self) -> SlaveGroupCreateES {
    SlaveGroupCreateES {
      create_msg: self.create_msg.clone(),
      paxos_nodes: self.paxos_nodes.clone(),
      state: State::Follower,
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  ES Container Functions
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SlaveGroupCreateESS {
  ess: BTreeMap<SlaveGroupId, SlaveGroupCreateES>,
}

impl SlaveGroupCreateESS {
  pub fn new() -> SlaveGroupCreateESS {
    SlaveGroupCreateESS { ess: Default::default() }
  }

  // Leader-only

  pub fn handle_msg<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    confirm_msg: msg::ConfirmSlaveCreation,
  ) {
    if let Some(es) = self.ess.get_mut(&confirm_msg.sid) {
      es.handle_confirm_msg(ctx, io_ctx, confirm_msg);
    }
  }

  pub fn handle_new_slaves<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    new_slave_groups: BTreeMap<SlaveGroupId, (Vec<EndpointId>, Vec<CoordGroupId>)>,
  ) {
    // Construct `SlaveGroupCreateES`s accordingly
    for (sid, (paxos_nodes, coord_ids)) in new_slave_groups {
      let es = SlaveGroupCreateES::create(ctx, io_ctx, sid.clone(), paxos_nodes, coord_ids);
      self.ess.insert(sid, es);
    }
  }

  // Leader and Follower

  pub fn handle_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    confirm_create: ConfirmCreateGroup,
  ) {
    // Here, we remove the ES and then finish it off.
    let mut es = self.ess.remove(&confirm_create.sid).unwrap();
    es.handle_confirm_plm(ctx, io_ctx);
  }

  pub fn handle_lc<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    for (_, es) in &mut self.ess {
      es.leader_changed(ctx, io_ctx);
    }
  }

  /// Add in the `SlaveGroupCreateES` where at least `ReconfigSlaveGroup` PLm has been inserted.
  pub fn handle_reconfig_snapshot(&self) -> SlaveGroupCreateESS {
    let mut create_ess = SlaveGroupCreateESS::new();
    for (qid, es) in &self.ess {
      let es = es.reconfig_snapshot();
      create_ess.ess.insert(qid.clone(), es);
    }
    create_ess
  }
}
