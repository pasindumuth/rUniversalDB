use crate::common::{remove_item, MasterIOCtx, RemoteLeaderChangedPLm};
use crate::master::plm::{ReconfigSlaveGroupPLm, SlaveGroupReconfiguredPLm};
use crate::master::{MasterContext, MasterPLm};
use crate::model::common::{EndpointId, PaxosGroupIdTrait, SlaveGroupId};
use crate::model::message as msg;
use crate::server::ServerContextBase;

// -----------------------------------------------------------------------------------------------
//  SlaveReconfigES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum State {
  Follower { new_eids: Vec<EndpointId> },
  WaitingRequestedNodes,
  InsertingReconfig { new_eids: Vec<EndpointId> },
  Reconfig { new_eids: Vec<EndpointId> },
  InsertingCompletion { new_eids: Vec<EndpointId> },
  Done,
}

#[derive(Debug)]
pub struct SlaveReconfigES {
  sid: SlaveGroupId,
  rem_eids: Vec<EndpointId>,
  state: State,
}

impl SlaveReconfigES {
  /// This is called by the Leader
  pub fn new(
    ctx: &mut MasterContext,
    sid: SlaveGroupId,
    rem_eids: Vec<EndpointId>,
  ) -> SlaveReconfigES {
    debug_assert!(ctx.is_leader());
    // Request new `EndpointId`s from the FreeNodeManager
    ctx.free_node_manager.request_new_eids(sid.to_gid(), rem_eids.len());
    SlaveReconfigES { sid, rem_eids, state: State::WaitingRequestedNodes }
  }

  /// This is only called by a follower
  pub fn new_follower(ctx: &mut MasterContext, plm: ReconfigSlaveGroupPLm) -> SlaveReconfigES {
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
        ctx.master_bundle.plms.push(MasterPLm::ReconfigSlaveGroupPLm(ReconfigSlaveGroupPLm {
          sid: self.sid.clone(),
          new_eids: new_eids.clone(),
          rem_eids: self.rem_eids.clone(),
        }));

        // Advance
        self.state = State::InsertingReconfig { new_eids };
      }
      _ => {}
    }
  }

  pub fn handle_reconfig_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    _: ReconfigSlaveGroupPLm,
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

  pub fn handle_reconfigured_msg<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    _: msg::SlaveGroupReconfigured,
  ) {
    match &mut self.state {
      State::Reconfig { new_eids } => {
        // Amend MasterBundle
        ctx.master_bundle.plms.push(MasterPLm::SlaveGroupReconfiguredPLm(
          SlaveGroupReconfiguredPLm { sid: self.sid.clone() },
        ));

        // Advance
        self.state = State::InsertingCompletion { new_eids: std::mem::take(new_eids) }
      }
      _ => {}
    }
  }

  /// This is the final transition for this `ES`; it should be cleaned up afterwards.
  pub fn handle_reconfigured_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    _: SlaveGroupReconfiguredPLm,
  ) {
    match &self.state {
      State::Follower { new_eids } | State::InsertingCompletion { new_eids } => {
        // Update the GossipData
        ctx.gossip.update(|gossip| {
          let paxos_nodes = gossip.slave_address_config.get_mut(&self.sid).unwrap();

          // Add new nodes
          for eid in new_eids {
            paxos_nodes.push(eid.clone());
          }

          // Remove olds nodes
          for eid in &self.rem_eids {
            remove_item(paxos_nodes, eid);
          }
        });

        if ctx.is_leader() {
          // Broadcast GossipData
          ctx.broadcast_gossip(io_ctx);

          // Send ShutdownNode to ensure the removed nodes are gone
          for eid in &self.rem_eids {
            io_ctx.send(eid, msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::ShutdownNode));
          }
        }

        self.state = State::Done;
      }
      _ => debug_assert!(false),
    }
  }

  /// Returns `true` iff this should be exited.
  pub fn remote_leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    remote_leader_changed: &RemoteLeaderChangedPLm,
  ) {
    match &self.state {
      State::Reconfig { new_eids } => {
        // If the `sid` had a Leadership change, we contact it again.
        if remote_leader_changed.gid == self.sid.to_gid() {
          self.send_reconfig_slave_group(ctx, io_ctx, &new_eids);
        }
      }
      _ => {}
    }
  }

  /// Returns `true` iff this should be exited.
  pub fn leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> bool {
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
      State::WaitingRequestedNodes | State::InsertingReconfig { .. } => {
        self.state = State::Done;
        true
      }
      State::Reconfig { new_eids } | State::InsertingCompletion { new_eids } => {
        self.state = State::Follower { new_eids: std::mem::take(new_eids) };
        false
      }
      State::Done => true,
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
