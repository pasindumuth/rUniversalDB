use crate::message as msg;
use crate::message::{ExternalMessage, SlaveMessage, SlaveRemotePayload};
use crate::simple_rm_es::SimpleRMES;
use crate::simple_tm_es::{SimplePayloadTypes, SimplePrepare, SimpleTMES, SimpleTMInner};
use crate::simulation::ISlaveIOCtx;
use crate::stm_simple_rm_es::STMSimpleRMES;
use crate::stm_simple_tm_es::{
  STMSimpleAborted, STMSimplePayloadTypes, STMSimpleTMES, STMSimpleTMInner,
};
use rand::RngCore;
use runiversal::common::{mk_t, BasicIOCtx, RemoteLeaderChangedPLm};
use runiversal::model::common::{EndpointId, PaxosGroupIdTrait, QueryId};
use runiversal::model::common::{LeadershipId, PaxosGroupId, SlaveGroupId};
use runiversal::network_driver::{NetworkDriver, NetworkDriverContext};
use runiversal::paxos2pc_rm;
use runiversal::paxos2pc_rm::Paxos2PCRMAction;
use runiversal::paxos2pc_tm;
use runiversal::paxos2pc_tm::Paxos2PCTMAction;
use runiversal::slave::REMOTE_LEADER_CHANGED_PERIOD_MS;
use runiversal::stmpaxos2pc_rm;
use runiversal::stmpaxos2pc_rm::STMPaxos2PCRMAction;
use runiversal::stmpaxos2pc_tm;
use runiversal::stmpaxos2pc_tm::STMPaxos2PCTMAction;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  SlavePLm
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct SlaveBundle {
  remote_leader_changes: Vec<RemoteLeaderChangedPLm>,
  pub plms: Vec<SlavePLm>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlavePLm {
  SimpleSTMTM(stmpaxos2pc_tm::TMPLm<STMSimplePayloadTypes>),
  SimpleSTMRM(stmpaxos2pc_tm::RMPLm<STMSimplePayloadTypes>),
  SimpleRM(paxos2pc_tm::RMPLm<SimplePayloadTypes>),
}

// -----------------------------------------------------------------------------------------------
//  SlaveForwardMsg
// -----------------------------------------------------------------------------------------------
pub enum SlaveForwardMsg {
  SlaveBundle(Vec<SlavePLm>),
  SlaveExternalReq(msg::ExternalMessage),
  SlaveRemotePayload(msg::SlaveRemotePayload),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
  LeaderChanged(msg::LeaderChanged),
  SlaveTimerInput(SlaveTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  Full Slave Input
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum SlaveTimerInput {
  RemoteLeaderChanged,
}

pub enum FullSlaveInput {
  SlaveMessage(msg::SlaveMessage),
  PaxosMessage(msg::PLEntry<SlaveBundle>),
  SlaveTimerInput(SlaveTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  Status
// -----------------------------------------------------------------------------------------------

/// This contains every Slave Status. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  stm_simple_rm_ess: BTreeMap<QueryId, STMSimpleRMES>,
  stm_simple_tm_ess: BTreeMap<QueryId, STMSimpleTMES>,
  simple_rm_ess: BTreeMap<QueryId, SimpleRMES>,
  simple_tm_ess: BTreeMap<QueryId, SimpleTMES>,
}

// -----------------------------------------------------------------------------------------------
//  STM TMServerContext
// -----------------------------------------------------------------------------------------------

impl stmpaxos2pc_tm::TMServerContext<STMSimplePayloadTypes> for SlaveContext {
  fn push_plm(&mut self, plm: SlavePLm) {
    self.slave_bundle.plms.push(plm);
  }

  fn send_to_rm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    io_ctx: &mut IO,
    rm: &SlaveGroupId,
    msg: msg::SlaveRemotePayload,
  ) {
    self.send(io_ctx, rm, msg);
  }

  fn mk_node_path(&self) -> SlaveGroupId {
    self.this_sid.clone()
  }

  fn is_leader(&self) -> bool {
    SlaveContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  STM RMServerContext
// -----------------------------------------------------------------------------------------------

impl stmpaxos2pc_tm::RMServerContext<STMSimplePayloadTypes> for SlaveContext {
  fn push_plm(&mut self, plm: SlavePLm) {
    self.slave_bundle.plms.push(plm);
  }

  fn send_to_tm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    io_ctx: &mut IO,
    tm: &SlaveGroupId,
    msg: msg::SlaveRemotePayload,
  ) {
    self.send(io_ctx, tm, msg);
  }

  fn mk_node_path(&self) -> SlaveGroupId {
    self.this_sid.clone()
  }

  fn is_leader(&self) -> bool {
    SlaveContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  TMServerContext
// -----------------------------------------------------------------------------------------------

impl paxos2pc_tm::TMServerContext<SimplePayloadTypes> for SlaveContext {
  fn send_to_rm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    io_ctx: &mut IO,
    rm: &SlaveGroupId,
    msg: msg::SlaveRemotePayload,
  ) {
    self.send(io_ctx, rm, msg);
  }

  fn mk_node_path(&self) -> SlaveGroupId {
    self.this_sid.clone()
  }

  fn is_leader(&self) -> bool {
    SlaveContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  RMServerContext
// -----------------------------------------------------------------------------------------------

impl paxos2pc_tm::RMServerContext<SimplePayloadTypes> for SlaveContext {
  fn push_plm(&mut self, plm: SlavePLm) {
    self.slave_bundle.plms.push(plm);
  }

  fn send_to_tm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    io_ctx: &mut IO,
    tm: &SlaveGroupId,
    msg: msg::SlaveRemotePayload,
  ) {
    self.send(io_ctx, tm, msg);
  }

  fn mk_node_path(&self) -> SlaveGroupId {
    self.this_sid.clone()
  }

  fn is_leader(&self) -> bool {
    SlaveContext::is_leader(self)
  }

  fn leader_map(&self) -> &BTreeMap<PaxosGroupId, LeadershipId> {
    &self.leader_map
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
  // Metadata
  pub this_sid: SlaveGroupId,
  pub this_gid: PaxosGroupId, // self.this_sid.to_gid()
  pub this_eid: EndpointId,

  /// Gossip
  pub slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,

  /// LeaderMap
  pub leader_map: BTreeMap<PaxosGroupId, LeadershipId>,

  /// NetworkDriver
  pub network_driver: NetworkDriver<msg::SlaveRemotePayload>,

  // Paxos
  pub slave_bundle: SlaveBundle,
}

impl SlaveState {
  pub fn new(slave_context: SlaveContext) -> SlaveState {
    SlaveState { context: slave_context, statuses: Default::default() }
  }

  pub fn handle_full_input<IO: ISlaveIOCtx>(&mut self, io_ctx: &mut IO, input: FullSlaveInput) {
    self.context.handle_full_input(io_ctx, &mut self.statuses, input);
  }

  pub fn initialize<IO: ISlaveIOCtx>(&mut self, io_ctx: &mut IO) {
    // Start the RemoteLeaderChange dispatch cycle
    io_ctx.defer(mk_t(REMOTE_LEADER_CHANGED_PERIOD_MS), SlaveTimerInput::RemoteLeaderChanged);
    if self.context.is_leader() {
      // Start the bundle insertion cycle for this PaxosGroup.
      io_ctx.insert_bundle(SlaveBundle::default());
    }
  }
}

impl SlaveContext {
  pub fn new(
    this_sid: SlaveGroupId,
    this_eid: EndpointId,
    slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
    leader_map: BTreeMap<PaxosGroupId, LeadershipId>,
  ) -> SlaveContext {
    let all_gids = leader_map.keys().cloned().collect();
    SlaveContext {
      this_sid: this_sid.clone(),
      this_gid: this_sid.to_gid(),
      this_eid,
      slave_address_config,
      leader_map,
      network_driver: NetworkDriver::new(all_gids),
      slave_bundle: Default::default(),
    }
  }

  pub fn handle_full_input<IO: ISlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    input: FullSlaveInput,
  ) {
    match input {
      FullSlaveInput::SlaveMessage(message) => match message {
        SlaveMessage::ExternalMessage(request) => {
          if self.is_leader() {
            self.handle_input(io_ctx, statuses, SlaveForwardMsg::SlaveExternalReq(request))
          }
        }
        SlaveMessage::RemoteMessage(remote_message) => {
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
        SlaveMessage::RemoteLeaderChangedGossip(msg::RemoteLeaderChangedGossip { gid, lid }) => {
          if self.is_leader() {
            if &lid.gen > &self.leader_map.get(&gid).unwrap().gen {
              // If the incoming RemoteLeaderChanged would increase the generation
              // in LeaderMap, then persist it.
              self.slave_bundle.remote_leader_changes.push(RemoteLeaderChangedPLm { gid, lid })
            }
          }
        }
      },
      FullSlaveInput::PaxosMessage(pl_entry) => {
        match pl_entry {
          msg::PLEntry::Bundle(bundle) => {
            // Dispatch RemoteLeaderChanges
            for remote_change in bundle.remote_leader_changes.clone() {
              let forward_msg = SlaveForwardMsg::RemoteLeaderChanged(remote_change.clone());
              self.handle_input(io_ctx, statuses, forward_msg);
            }

            // Dispatch the PaxosBundles
            self.handle_input(io_ctx, statuses, SlaveForwardMsg::SlaveBundle(bundle.plms));

            // Dispatch any messages that were buffered in the NetworkDriver.
            // Note: we must do this after RemoteLeaderChanges have been executed. Also note
            // that there will be no payloads in the NetworkBuffer if this nodes is a Follower.
            for remote_change in bundle.remote_leader_changes {
              if remote_change.lid.gen == self.leader_map.get(&remote_change.gid).unwrap().gen {
                // We need this guard, since one Bundle can hold multiple `RemoteLeaderChanged`s
                // for the same `gid` with different `gen`s.
                let payloads = self
                  .network_driver
                  .deliver_blocked_messages(remote_change.gid, remote_change.lid);
                for payload in payloads {
                  self.handle_input(io_ctx, statuses, SlaveForwardMsg::SlaveRemotePayload(payload));
                }
              }
            }
          }
          msg::PLEntry::LeaderChanged(leader_changed) => {
            // Forward to Slave Backend
            self.handle_input(
              io_ctx,
              statuses,
              SlaveForwardMsg::LeaderChanged(leader_changed.clone()),
            );
          }
          msg::PLEntry::Reconfig(_) => {
            // TODO: do
          }
        }
      }
      FullSlaveInput::SlaveTimerInput(timer_input) => {
        self.handle_input(io_ctx, statuses, SlaveForwardMsg::SlaveTimerInput(timer_input));
      }
    }
  }

  /// Handles inputs the Slave Backend.
  fn handle_input<IO: ISlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    slave_input: SlaveForwardMsg,
  ) {
    match slave_input {
      SlaveForwardMsg::SlaveBundle(bundle) => {
        for paxos_log_msg in bundle {
          match paxos_log_msg {
            SlavePLm::SimpleSTMRM(plm) => {
              let (query_id, action) =
                stmpaxos2pc_rm::handle_rm_plm(self, io_ctx, &mut statuses.stm_simple_rm_ess, plm);
              self.handle_stm_simple_rm_es_action(statuses, query_id, action);
            }
            SlavePLm::SimpleSTMTM(plm) => {
              let (query_id, action) =
                stmpaxos2pc_tm::handle_tm_plm(self, io_ctx, &mut statuses.stm_simple_tm_ess, plm);
              self.handle_stm_simple_tm_es_action(statuses, query_id, action);
            }
            SlavePLm::SimpleRM(plm) => {
              let (query_id, action) =
                paxos2pc_rm::handle_rm_plm(self, io_ctx, &mut statuses.simple_rm_ess, plm);
              self.handle_simple_rm_es_action(statuses, query_id, action);
            }
          }
        }

        // Inform all ESs in WaitingInserting and start inserting a PLm.
        if self.is_leader() {
          for (_, es) in &mut statuses.stm_simple_rm_ess {
            es.start_inserting(self, io_ctx);
          }
          for (_, es) in &mut statuses.stm_simple_tm_ess {
            es.start_inserting(self, io_ctx);
          }
          for (_, es) in &mut statuses.simple_rm_ess {
            es.start_inserting(self, io_ctx);
          }

          // Continue the insert cycle.
          let bundle = std::mem::replace(&mut self.slave_bundle, SlaveBundle::default());
          io_ctx.insert_bundle(bundle);
        }
      }
      SlaveForwardMsg::SlaveExternalReq(request) => match request {
        ExternalMessage::STMSimpleRequest(simple_req) => {
          let query_id = simple_req.query_id;
          let mut es =
            STMSimpleTMES::new(query_id.clone(), STMSimpleTMInner { rms: simple_req.rms });
          es.state = stmpaxos2pc_tm::State::WaitingInsertTMPrepared;
          statuses.stm_simple_tm_ess.insert(query_id, es);
        }
        ExternalMessage::SimpleRequest(simple_req) => {
          let query_id = simple_req.query_id;

          // Construct SimplePrepares
          let mut prepare_payloads = BTreeMap::<SlaveGroupId, SimplePrepare>::new();
          for rm in simple_req.rms {
            prepare_payloads.insert(rm, SimplePrepare {});
          }

          // Start the TM
          let outer = SimpleTMES::start_orig(
            self,
            io_ctx,
            query_id.clone(),
            SimpleTMInner {},
            prepare_payloads,
          );

          statuses.simple_tm_ess.insert(query_id, outer);
        }
      },
      SlaveForwardMsg::SlaveRemotePayload(payload) => match payload {
        SlaveRemotePayload::STMRMMessage(message) => {
          if let stmpaxos2pc_tm::RMMessage::Prepare(prepare) = message.clone() {
            // Here, we randomly decide whether to accept the `Prepare` and proceed to
            // insert `Prepared`, or whether to respond immediately with an `Aborted`.
            // We respond with an `Aborted` with a 5% chance.
            if io_ctx.rand().next_u32() % 100 < 5 {
              self.send(
                io_ctx,
                &prepare.tm,
                msg::SlaveRemotePayload::STMTMMessage(stmpaxos2pc_tm::TMMessage::Aborted(
                  stmpaxos2pc_tm::Aborted {
                    query_id: prepare.query_id,
                    payload: STMSimpleAborted {},
                  },
                )),
              );
              return;
            }
          }

          // If we do not abort, we just forward the `Prepare` to `simple_rm_ess`.
          let (query_id, action) =
            stmpaxos2pc_rm::handle_rm_msg(self, io_ctx, &mut statuses.stm_simple_rm_ess, message);
          self.handle_stm_simple_rm_es_action(statuses, query_id, action);
        }
        SlaveRemotePayload::STMTMMessage(message) => {
          let (query_id, action) =
            stmpaxos2pc_tm::handle_tm_msg(self, io_ctx, &mut statuses.stm_simple_tm_ess, message);
          self.handle_stm_simple_tm_es_action(statuses, query_id, action);
        }
        SlaveRemotePayload::RMMessage(message) => {
          let (query_id, action) =
            paxos2pc_rm::handle_rm_msg(self, io_ctx, &mut statuses.simple_rm_ess, &mut (), message);
          self.handle_simple_rm_es_action(statuses, query_id, action);
        }
        SlaveRemotePayload::TMMessage(message) => {
          let (query_id, action) =
            paxos2pc_tm::handle_tm_msg(self, io_ctx, &mut statuses.simple_tm_ess, message);
          self.handle_simple_tm_es_action(statuses, query_id, action);
        }
      },
      SlaveForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();
        if lid.gen > self.leader_map.get(&gid).unwrap().gen {
          // Only update the LeadershipId if the new one increases the old one.
          self.leader_map.insert(gid.clone(), lid.clone());

          // Inform STMSimpleTM
          let query_ids: Vec<QueryId> = statuses.stm_simple_tm_ess.keys().cloned().collect();
          for query_id in query_ids {
            let es = statuses.stm_simple_tm_ess.get_mut(&query_id).unwrap();
            let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
            self.handle_stm_simple_tm_es_action(statuses, query_id, action);
          }

          // Inform SimpleTM
          let query_ids: Vec<QueryId> = statuses.simple_tm_ess.keys().cloned().collect();
          for query_id in query_ids {
            let es = statuses.simple_tm_ess.get_mut(&query_id).unwrap();
            let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
            self.handle_simple_tm_es_action(statuses, query_id, action);
          }

          // Inform SimpleRM
          let query_ids: Vec<QueryId> = statuses.simple_rm_ess.keys().cloned().collect();
          for query_id in query_ids {
            let es = statuses.simple_rm_ess.get_mut(&query_id).unwrap();
            let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
            self.handle_simple_rm_es_action(statuses, query_id, action);
          }
        }
      }
      SlaveForwardMsg::LeaderChanged(leader_changed) => {
        // Update the LeadershipId
        self.leader_map.insert(self.this_gid.clone(), leader_changed.lid);

        if self.is_leader() {
          // By the SharedPaxosInserter, these must be empty at the start of Leadership.
          self.slave_bundle = SlaveBundle::default();
        }

        // Inform STMSimpleTM
        let query_ids: Vec<QueryId> = statuses.stm_simple_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.stm_simple_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self, io_ctx);
          self.handle_stm_simple_tm_es_action(statuses, query_id.clone(), action);
        }

        // Inform STMSimpleRM
        let query_ids: Vec<QueryId> = statuses.stm_simple_rm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.stm_simple_rm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self);
          self.handle_stm_simple_rm_es_action(statuses, query_id.clone(), action);
        }

        // Wink away SimpleTM
        statuses.simple_tm_ess.clear();

        // Inform SimpleRM
        let query_ids: Vec<QueryId> = statuses.simple_rm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.simple_rm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self, io_ctx);
          self.handle_simple_rm_es_action(statuses, query_id.clone(), action);
        }

        // Inform the NetworkDriver
        self.network_driver.leader_changed();

        if self.is_leader() {
          // This node is the new Leader
          self.broadcast_leadership(io_ctx); // Broadcast RemoteLeaderChanged
          let bundle = std::mem::replace(&mut self.slave_bundle, SlaveBundle::default());
          io_ctx.insert_bundle(bundle); // Start the insert cycle.
        }
      }
      SlaveForwardMsg::SlaveTimerInput(timer_input) => {
        match timer_input {
          SlaveTimerInput::RemoteLeaderChanged => {
            if self.is_leader() {
              // If this node is the Leader, then send out RemoteLeaderChanged.
              self.broadcast_leadership(io_ctx);
            }

            // Do this again 5 ms later.
            io_ctx
              .defer(mk_t(REMOTE_LEADER_CHANGED_PERIOD_MS), SlaveTimerInput::RemoteLeaderChanged);
          }
        }
      }
    }
  }

  /// Used to broadcast out `RemoteLeaderChanged` to all other
  /// PaxosGroups to help maintain their LeaderMaps.
  fn broadcast_leadership<IO: BasicIOCtx<msg::NetworkMessage>>(&self, io_ctx: &mut IO) {
    let this_lid = self.leader_map.get(&self.this_gid).unwrap().clone();
    for (sid, eids) in &self.slave_address_config {
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
  }

  /// Handles the actions produced by a STMSimpleRM.
  fn handle_stm_simple_rm_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: STMPaxos2PCRMAction,
  ) {
    match action {
      STMPaxos2PCRMAction::Wait => {}
      STMPaxos2PCRMAction::Exit => {
        statuses.stm_simple_rm_ess.remove(&query_id).unwrap();
      }
    }
  }

  /// Handles the actions produced by a STMSimpleTM.
  fn handle_stm_simple_tm_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: STMPaxos2PCTMAction,
  ) {
    match action {
      STMPaxos2PCTMAction::Wait => {}
      STMPaxos2PCTMAction::Exit => {
        statuses.stm_simple_tm_ess.remove(&query_id).unwrap();
      }
    }
  }

  /// Handles the actions produced by a SimpleRM.
  fn handle_simple_rm_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: Paxos2PCRMAction,
  ) {
    match action {
      Paxos2PCRMAction::Wait => {}
      Paxos2PCRMAction::Exit => {
        statuses.simple_rm_ess.remove(&query_id).unwrap();
      }
    }
  }

  /// Handles the actions produced by a SimpleTM.
  fn handle_simple_tm_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: Paxos2PCTMAction,
  ) {
    match action {
      Paxos2PCTMAction::Wait => {}
      Paxos2PCTMAction::Exit => {
        statuses.simple_tm_ess.remove(&query_id).unwrap();
      }
    }
  }

  pub fn send<IO: BasicIOCtx<msg::NetworkMessage>>(
    &self,
    io_ctx: &mut IO,
    sid: &SlaveGroupId,
    payload: msg::SlaveRemotePayload,
  ) {
    if self.is_leader() {
      // Only send out messages if this node is the Leader. This ensures that
      // followers do not leak out Leadership information of this PaxosGroup.

      let this_gid = self.this_gid.clone();
      let this_lid = self.leader_map.get(&this_gid).unwrap();

      let to_gid = sid.to_gid();
      let to_lid = self.leader_map.get(&to_gid).unwrap();

      let remote_message = msg::RemoteMessage {
        payload,
        from_lid: this_lid.clone(),
        from_gid: this_gid,
        to_lid: to_lid.clone(),
        to_gid,
      };

      io_ctx.send(
        &to_lid.eid,
        msg::NetworkMessage::Slave(msg::SlaveMessage::RemoteMessage(remote_message)),
      );
    }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&self.this_gid).unwrap();
    lid.eid == self.this_eid
  }
}
