use crate::common::{mk_cid, mk_sid, MasterIOCtx, NUM_COORDS};
use crate::master::plm::ConfirmCreateGroup;
use crate::master::{MasterContext, MasterPLm};
use crate::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupIdTrait, SlaveGroupId,
};
use crate::model::message as msg;
use std::collections::BTreeSet;

// -----------------------------------------------------------------------------------------------
//  SlaveGroupCreateES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum State {
  Follower,
  WaitingConfirmed(BTreeSet<EndpointId>),
  InsertingConfirmed,
}

#[derive(Debug)]
pub struct SlaveGroupCreateES {
  create_msg: msg::CreateSlaveGroup,
  paxos_nodes: Vec<EndpointId>,
  state: State,
}

impl SlaveGroupCreateES {
  /// Constructs an `ES`, sending out `CreateSlaveGroup` if this is the Master node.
  pub fn create<IO: MasterIOCtx>(
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    sid: SlaveGroupId,
    paxos_nodes: Vec<EndpointId>,
    coord_ids: Vec<CoordGroupId>,
  ) -> SlaveGroupCreateES {
    // Construct the `CreateSlaveGroup` message.
    let create_msg = msg::CreateSlaveGroup {
      gossip: ctx.gossip.clone(),
      leader_map: ctx.leader_map.clone(),
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
  pub fn handle_confirm_msg<IO: MasterIOCtx>(
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
  pub fn handle_confirm_plm<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
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
        ctx.leader_map.insert(sid.to_gid(), lid);

        if ctx.is_leader() {
          // Broadcast the GossipData.
          ctx.broadcast_gossip(io_ctx);
        }
      }
      State::WaitingConfirmed(_) => {}
    }
  }

  /// Handle the current (Master) leader changing.
  pub fn leader_changed<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
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
}
