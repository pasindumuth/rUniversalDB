use crate::common::{mk_t, mk_uuid, remove_item, Timestamp, UUID};
use crate::model::common::{EndpointId, Gen, LeadershipId};
use crate::model::message as msg;
use crate::model::message::{
  LeaderChanged, PLEntry, PLIndex, PaxosDriverMessage, PaxosMessage, Rnd,
};
use rand::RngCore;
use sqlparser::dialect::keywords::Keyword::NEXT;
use std::cmp::{max, min};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;

// -----------------------------------------------------------------------------------------------
//  Single Paxos State
// -----------------------------------------------------------------------------------------------

type Crnd = Rnd;
type Vrnd = Rnd;

#[derive(Debug)]
pub struct Proposal<BundleT> {
  crnd: Crnd,
  cval: PLEntry<BundleT>,
  promises: Vec<Option<(Vrnd, PLEntry<BundleT>)>>,
}

#[derive(Debug)]
pub struct ProposerState<BundleT> {
  proposals: BTreeMap<Crnd, Proposal<BundleT>>,
  /// This is the maximum key in `proposals`, or 0 otherwise.
  latest_crnd: u32,
}

#[derive(Debug)]
pub struct AcceptorState<BundleT> {
  rnd: Rnd,
  vrnd_vval: Option<(Vrnd, PLEntry<BundleT>)>,
}

#[derive(Debug)]
pub struct LearnerState {
  learned: BTreeMap<Vrnd, u32>,
  /// This is equal to the smallest `Vrnd` in `learned` with a majority count.
  learned_vrnd: Option<Vrnd>,
}

#[derive(Debug)]
pub struct PaxosInstance<BundleT> {
  proposer_state: ProposerState<BundleT>,
  acceptor_state: AcceptorState<BundleT>,
  learner_state: LearnerState,
}

impl<BundleT> PaxosInstance<BundleT> {
  fn new() -> PaxosInstance<BundleT> {
    PaxosInstance {
      proposer_state: ProposerState { proposals: Default::default(), latest_crnd: 0 },
      acceptor_state: AcceptorState { rnd: 0, vrnd_vval: None },
      learner_state: LearnerState { learned: Default::default(), learned_vrnd: None },
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Constants
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PaxosConfig {
  pub heartbeat_threshold: u32,
  pub heartbeat_period_ms: Timestamp,
  pub next_index_period_ms: Timestamp,
  pub retry_defer_time_ms: Timestamp,
  pub proposal_increment: u32,

  /// The threshold for how far back the `remote_next_index` of a PaxosNode
  /// can be before it might be suspected of being dead.
  pub remote_next_index_thresh: u32,
}

impl PaxosConfig {
  /// Default config values to use in production
  pub fn prod() -> PaxosConfig {
    PaxosConfig {
      heartbeat_threshold: 5,
      heartbeat_period_ms: mk_t(1000),
      next_index_period_ms: mk_t(1000),
      retry_defer_time_ms: mk_t(1000),
      proposal_increment: 1000,
      remote_next_index_thresh: 100,
    }
  }

  /// Default config values to use in production
  pub fn test() -> PaxosConfig {
    PaxosConfig {
      heartbeat_threshold: 3,
      heartbeat_period_ms: mk_t(5),
      next_index_period_ms: mk_t(10),
      retry_defer_time_ms: mk_t(5),
      proposal_increment: 1000,
      remote_next_index_thresh: 5,
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  PaxosContextBase
// -----------------------------------------------------------------------------------------------

pub trait PaxosContextBase<BundleT> {
  type RngCoreT: RngCore;

  /// Getters
  fn rand(&mut self) -> &mut Self::RngCoreT;
  fn this_eid(&self) -> &EndpointId;

  /// Methods
  fn send(&mut self, eid: &EndpointId, message: msg::PaxosDriverMessage<BundleT>);
  fn defer(&mut self, defer_time: Timestamp, timer_event: PaxosTimerEvent);
}

// -----------------------------------------------------------------------------------------------
//  Timer Events
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum PaxosTimerEvent {
  RetryInsert(UUID),
  LeaderHeartbeat,
  /// When this event occurs, PaxosDriver sends out `IndexRequests`, which, recall, is used to
  /// clean up the `PaxosLog`.
  NextIndex,
}

// -----------------------------------------------------------------------------------------------
//  PaxosDriver
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum UserPLEntry<BundleT> {
  Bundle(BundleT),
  ReconfigBundle(msg::ReconfigBundle<BundleT>),
}

impl<BundleT> UserPLEntry<BundleT> {
  fn convert(self) -> msg::PLEntry<BundleT> {
    match self {
      UserPLEntry::Bundle(bundle) => msg::PLEntry::Bundle(bundle),
      UserPLEntry::ReconfigBundle(reconfig) => msg::PLEntry::ReconfigBundle(reconfig),
    }
  }
}

pub fn majority<T>(vec: &Vec<T>) -> usize {
  vec.len() / 2 + 1
}

#[derive(Debug)]
struct InstanceEntry<BundleT> {
  instance: Option<PaxosInstance<BundleT>>,
  /// The learned value for the PaxosInstance. This could be through any means,
  /// including Paxos and LogSyncResponse.
  learned_rnd_val: Option<(Vrnd, PLEntry<BundleT>)>,
}

#[derive(Debug)]
pub struct PaxosDriver<BundleT> {
  /// Metadata
  paxos_config: PaxosConfig,

  /// The current Paxos configuration.
  paxos_nodes: Vec<EndpointId>,
  /// Maps all PaxosNodes in this PaxosGroup to the last known `PLIndex` that was
  /// returned by a `NextIndexResponse`. This contains `this_eid()`, but the value
  /// generally lags `next_index`.
  remote_next_indices: BTreeMap<EndpointId, PLIndex>,

  /// This holds the first index that this node has not learned the Vval of.
  next_index: PLIndex,
  /// In practice, the `PLIndex`s will be present contiguously from the first until one before
  /// `next_index`. After that, they do not have to be contiguous. The first `PLIndex` is one
  /// after the maximum of `remote_next_indices`.
  paxos_instances: BTreeMap<PLIndex, InstanceEntry<BundleT>>,
  /// Paxos messages at subsequent `PLIndex`s. We do not process them until the
  /// current `PLIndex` is populated.
  buffered_messages: BTreeMap<PLIndex, VecDeque<msg::MultiPaxosMessage<BundleT>>>,

  /// The latest Leadership by `next_index`.
  leader: LeadershipId,
  leader_heartbeat: u32,

  /// Set of new PaxosNodes that we have not confirmed have started up yet.
  unconfirmed_eids: BTreeMap<EndpointId, bool>,

  /// Insert state. Once this is set to `Some(_)`, the internal value is never modified. This is
  /// only unset when `next_index` is incremented due to insertion.
  next_insert: Option<(UUID, UserPLEntry<BundleT>)>,
}

impl<BundleT: Clone + Debug> PaxosDriver<BundleT> {
  /// Constructs a `PaxosDriver` for a node in the initial PaxosGroup. The `paxos_nodes` is
  /// this initial configuration. The first entry is the first Leader.
  pub fn create_initial(
    paxos_nodes: Vec<EndpointId>,
    paxos_config: PaxosConfig,
  ) -> PaxosDriver<BundleT> {
    let mut remote_next_indices = BTreeMap::<EndpointId, PLIndex>::new();
    for node in &paxos_nodes {
      remote_next_indices.insert(node.clone(), 0);
    }
    let leader_eid = paxos_nodes[0].clone();
    PaxosDriver {
      paxos_nodes,
      paxos_config,
      remote_next_indices,
      next_index: 0,
      paxos_instances: Default::default(),
      buffered_messages: Default::default(),
      leader: LeadershipId { gen: Gen(0), eid: leader_eid },
      leader_heartbeat: 0,
      unconfirmed_eids: Default::default(),
      next_insert: None,
    }
  }

  /// Handles a `StartNewNode` to initiate a reconfigured node properly
  pub fn create_reconfig<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    ctx: &mut PaxosContextBaseT,
    mut start: msg::StartNewNode<BundleT>,
    paxos_config: PaxosConfig,
  ) -> PaxosDriver<BundleT> {
    let this_eid = ctx.this_eid().clone();

    // Construct `paxos_instances`
    let mut paxos_instances = BTreeMap::<PLIndex, InstanceEntry<BundleT>>::new();
    for (index, rnd_val) in start.paxos_instance_vals {
      paxos_instances
        .insert(index, InstanceEntry { instance: None, learned_rnd_val: Some(rnd_val) });
    }

    // Construct `unconfirmed_eids`
    let mut unconfirmed_eids = BTreeMap::<EndpointId, bool>::new();
    // Recall `start.unconfirmed_eids` will contain this node
    start.unconfirmed_eids.remove(&this_eid);
    for eid in start.unconfirmed_eids {
      unconfirmed_eids.insert(eid, false);
    }

    // Broadcast `NewNodeStarted`
    for eid in &start.paxos_nodes {
      ctx.send(
        eid,
        msg::PaxosDriverMessage::NewNodeStarted(msg::NewNodeStarted {
          paxos_node: this_eid.clone(),
        }),
      );
    }

    PaxosDriver {
      paxos_config,
      paxos_nodes: start.paxos_nodes,
      remote_next_indices: start.remote_next_indices,
      next_index: start.next_index,
      paxos_instances,
      buffered_messages: Default::default(),
      leader: start.leader,
      leader_heartbeat: 0,
      unconfirmed_eids,
      next_insert: None,
    }
  }

  /// This returns the set of `EndpointId`s that might be dead, based on how far
  /// back the `remote_next_index` is.
  pub fn get_maybe_dead(&self) -> Vec<EndpointId> {
    let mut maybe_dead_eids = Vec::<EndpointId>::new();
    for (eid, index) in &self.remote_next_indices {
      if *index + (self.paxos_config.remote_next_index_thresh as u128) < self.next_index {
        maybe_dead_eids.push(eid.clone())
      }
    }

    maybe_dead_eids
  }

  /// Get the send of PaxosNodes
  pub fn paxos_nodes(&self) -> &Vec<EndpointId> {
    &self.paxos_nodes
  }

  /// Compute the minimum of `remote_next_indices`
  fn min_complete_index(&self) -> u128 {
    // We start with `next_index` since the minimum in `remote_next_indices`
    // is certainly <= to this.
    let mut min_index = self.next_index;
    for remote_index in self.remote_next_indices.values() {
      min_index = min(min_index, *remote_index);
    }
    min_index
  }

  pub fn is_leader<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &self,
    ctx: &PaxosContextBaseT,
  ) -> bool {
    &self.leader.eid == ctx.this_eid()
  }

  // -----------------------------------------------------------------------------------------------
  //  StartNewNode Utilities
  // -----------------------------------------------------------------------------------------------

  /// This returns `true` iff there are `unconfirmed_eids` that map to `false`. (This will
  /// will primarily only be the case in backups, where Snapshot sending is lazy).
  pub fn has_unsent_unconfirmed(&self) -> bool {
    for (_, did_send_start) in &self.unconfirmed_eids {
      if !did_send_start {
        return true;
      }
    }

    false
  }

  /// Construct a `StartNewNode`, which is called from the outside (e.g. Master, Slave)
  /// and attached to *its* reconfig Snapshot before it is sent off. This is typically
  /// called immediately after processing a `UserPLEntry::Reconfig` element if this node
  /// is the Leader, but can also be called by a backup.
  ///
  /// We also return the set of `EndpointId`s that we should send `StartNewNode` to.
  pub fn mk_start_new_node<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &PaxosContextBaseT,
  ) -> (msg::StartNewNode<BundleT>, Vec<EndpointId>) {
    let mut start = msg::StartNewNode {
      sender_eid: ctx.this_eid().clone(),
      paxos_nodes: self.paxos_nodes.clone(),
      remote_next_indices: self.remote_next_indices.clone(),
      next_index: self.next_index.clone(),
      paxos_instance_vals: Default::default(),
      unconfirmed_eids: Default::default(),
      leader: self.leader.clone(),
    };

    // Populate `paxos_instance_vals`.
    for (index, instance_entry) in &self.paxos_instances {
      if index < &self.next_index {
        // Recall all entries prior to next_index will have learned a value.
        let rnd_val = instance_entry.learned_rnd_val.as_ref().unwrap().clone();
        start.paxos_instance_vals.insert(index.clone(), rnd_val);
      } else {
        break;
      }
    }

    // Populate `unconfirmed_eids`
    for (eid, _) in &self.unconfirmed_eids {
      start.unconfirmed_eids.insert(eid.clone());
    }

    // Compute all `EndpointId` we did not send `StartNewNode`.
    // (We will send these nodes a Snapshot.)
    let mut non_started_eids = Vec::<EndpointId>::new();
    for (eid, did_send_start) in &mut self.unconfirmed_eids {
      if !*did_send_start {
        non_started_eids.push(eid.clone());
        *did_send_start = true;
      }
    }

    (start, non_started_eids)
  }

  // -----------------------------------------------------------------------------------------------
  //  Message Handler
  // -----------------------------------------------------------------------------------------------

  pub fn handle_paxos_message<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    message: msg::PaxosDriverMessage<BundleT>,
  ) -> Vec<msg::PLEntry<BundleT>> {
    let mut learned_entries = self.handle_paxos_message_internal(ctx, message);
    // Poll and process all `buffered_messages` which an index that is low enough.
    self.handle_buffered_messages(ctx, &mut learned_entries);

    // Note that the caller is responsible for processing Reconfig PLms where
    // this node had been kicked out.
    learned_entries
  }

  pub fn handle_paxos_message_internal<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    message: msg::PaxosDriverMessage<BundleT>,
  ) -> Vec<msg::PLEntry<BundleT>> {
    match message {
      PaxosDriverMessage::MultiPaxosMessage(multi) => {
        if let msg::PaxosMessage::Prepare(_) = &multi.paxos_message {
          if multi.sender_eid != self.leader.eid
            && self.leader_heartbeat <= self.paxos_config.heartbeat_threshold
          {
            // If we get a `Prepare` message from a non-leader, and the leader still seems
            // to be alive, we drop the message. This is just a simple technique we use to reduce
            // the rate of spurious leadership changes.
            return Vec::new();
          }
        }

        // Inspect the index to see if we should break out early.
        let index = multi.index.clone();
        if index < self.min_complete_index() {
          // We drop this message if it is below `min_complete_index`
          return Vec::new();
        } else if index > self.next_index {
          // We buffer this message if the `index` is too high.
          if let Some(messages) = self.buffered_messages.get_mut(&index) {
            messages.push_back(multi);
          } else {
            self.buffered_messages.insert(index, VecDeque::from([multi]));
          }
          return Vec::new();
        }

        // Create a PaxosInstance if it does not exist already
        if let Some(instance_entry) = self.paxos_instances.get_mut(&multi.index) {
          if instance_entry.instance.is_none() {
            instance_entry.instance = Some(PaxosInstance::new());
          }
        } else {
          self.paxos_instances.insert(
            multi.index.clone(),
            InstanceEntry { instance: Some(PaxosInstance::new()), learned_rnd_val: None },
          );
        }

        // Finally, forward the PaxosMessage to the PaxosInstance
        let instance_entry = self.paxos_instances.get_mut(&multi.index).unwrap();
        let paxos_instance = instance_entry.instance.as_mut().unwrap();
        match multi.paxos_message {
          PaxosMessage::Prepare(prepare) => {
            let state = &mut paxos_instance.acceptor_state;
            if prepare.crnd > state.rnd {
              state.rnd = prepare.crnd;
              // Reply
              let this_eid = ctx.this_eid().clone();
              ctx.send(
                &multi.sender_eid,
                msg::PaxosDriverMessage::MultiPaxosMessage(msg::MultiPaxosMessage {
                  sender_eid: this_eid,
                  paxos_nodes: multi.paxos_nodes,
                  index: multi.index.clone(),
                  paxos_message: msg::PaxosMessage::Promise(msg::Promise {
                    rnd: state.rnd.clone(),
                    vrnd_vval: state.vrnd_vval.clone(),
                  }),
                }),
              );
            }
          }
          PaxosMessage::Promise(promise) => {
            let state = paxos_instance.proposer_state.proposals.get_mut(&promise.rnd).unwrap();
            state.promises.push(promise.vrnd_vval);
            if state.promises.len() == majority(&self.paxos_nodes) {
              // A majority of the nodes have sent back a promise.
              let mut max_vrnd_vval: &Option<(Vrnd, PLEntry<BundleT>)> = &None;
              for vrnd_vval in &state.promises {
                if let Some((vrnd, _)) = vrnd_vval {
                  if let Some((rnd, _)) = max_vrnd_vval {
                    if vrnd > rnd {
                      max_vrnd_vval = vrnd_vval
                    }
                  } else {
                    max_vrnd_vval = vrnd_vval
                  }
                }
              }

              // Construct `Accept`, choosing cval according to the promises.
              let accept = msg::Accept {
                crnd: state.crnd.clone(),
                cval: if let Some((_, vval)) = max_vrnd_vval {
                  vval.clone()
                } else {
                  state.cval.clone()
                },
              };

              // Broadcast
              let this_eid = ctx.this_eid().clone();
              for eid in &self.paxos_nodes {
                ctx.send(
                  &eid,
                  msg::PaxosDriverMessage::MultiPaxosMessage(msg::MultiPaxosMessage {
                    sender_eid: this_eid.clone(),
                    paxos_nodes: multi.paxos_nodes.clone(),
                    index: multi.index.clone(),
                    paxos_message: msg::PaxosMessage::Accept(accept.clone()),
                  }),
                );
              }
            }
          }
          PaxosMessage::Accept(accept) => {
            let state = &mut paxos_instance.acceptor_state;
            if accept.crnd >= state.rnd {
              state.rnd = accept.crnd.clone();
              state.vrnd_vval = Some((accept.crnd.clone(), accept.cval.clone()));

              // Broadcast
              let this_eid = ctx.this_eid().clone();
              for eid in &self.paxos_nodes {
                ctx.send(
                  &eid,
                  msg::PaxosDriverMessage::MultiPaxosMessage(msg::MultiPaxosMessage {
                    sender_eid: this_eid.clone(),
                    paxos_nodes: multi.paxos_nodes.clone(),
                    index: multi.index.clone(),
                    paxos_message: msg::PaxosMessage::Learn(msg::Learn {
                      vrnd: accept.crnd.clone(),
                    }),
                  }),
                );
              }

              // Check if the newly accepted value is >= to a learned vrnd
              if instance_entry.learned_rnd_val.is_none() {
                if let Some(vrnd) = paxos_instance.learner_state.learned_vrnd {
                  if vrnd <= accept.crnd {
                    instance_entry.learned_rnd_val = Some((vrnd, accept.cval.clone()));
                    return self.deliver_learned_entries();
                  }
                }
              }
            }
          }
          PaxosMessage::Learn(learn) => {
            let state = &mut paxos_instance.learner_state;
            if !state.learned.contains_key(&learn.vrnd) {
              state.learned.insert(learn.vrnd.clone(), 0);
            }
            let count = state.learned.get_mut(&learn.vrnd).unwrap();
            *count += 1;
            if *count == majority(&self.paxos_nodes) as u32 {
              // This Vrnd has been confirmed to contain the Learned value.
              if let Some(cur_vrnd) = state.learned_vrnd {
                if learn.vrnd < cur_vrnd {
                  // The newly learned Vrnd is lower than the existing, so we update it.
                  state.learned_vrnd = Some(learn.vrnd);
                }
              } else {
                state.learned_vrnd = Some(learn.vrnd);
              }

              let learned_vrnd = paxos_instance.learner_state.learned_vrnd.unwrap();

              // Check if the newly learned rnd is <= to an accepted vrnd already
              if instance_entry.learned_rnd_val.is_none() {
                if let Some((vrnd, vval)) = &paxos_instance.acceptor_state.vrnd_vval {
                  if &learned_vrnd <= vrnd {
                    // This means `vval` is the learned value for this PaxosInstance.
                    instance_entry.learned_rnd_val = Some((learned_vrnd, vval.clone()));
                    return self.deliver_learned_entries();
                  }
                }
              }
            }
          }
        }
      }
      PaxosDriverMessage::IsLeader(is_leader) => {
        if is_leader.lid == self.leader {
          // Reset heartbeat
          self.leader_heartbeat = 0;
        }
      }
      PaxosDriverMessage::InformLearned(inform_learned) => {
        let mut learned_entries = Vec::<PLEntry<BundleT>>::new();
        loop {
          // Poll and process a `buffered_messages` if there is one with low enough index.
          self.handle_buffered_messages(ctx, &mut learned_entries);

          // Use the next `learned_rnd` in `should_learned` to learn a new value
          if let Some(learned_rnd) = inform_learned.should_learned.get(&self.next_index) {
            if let Some(instance_entry) = self.paxos_instances.get_mut(&self.next_index) {
              if let Some(paxos_instance) = &instance_entry.instance {
                if let Some((vrnd, vval)) = &paxos_instance.acceptor_state.vrnd_vval {
                  if learned_rnd <= vrnd {
                    instance_entry.learned_rnd_val = Some((learned_rnd.clone(), vval.clone()));
                    learned_entries.extend(self.deliver_learned_entries());
                    // Restart this loop, since `next_index` could have increased and there
                    // might be buffered messages we can process.
                    continue;
                  }
                }
              }
            }

            // Here, we were not able use this `learned_rnd` to learn a new
            // entry, so we send the sender a LogSyncRequest get the actual vvals.
            let this_sid = ctx.this_eid().clone();
            ctx.send(
              &inform_learned.sender_eid,
              msg::PaxosDriverMessage::LogSyncRequest(msg::LogSyncRequest {
                sender_eid: this_sid,
                next_index: self.next_index.clone(),
              }),
            );
          }

          // Here, we have processed all the `should_learned`s we could, so we finish.
          return learned_entries;
        }
      }
      PaxosDriverMessage::LogSyncRequest(request) => {
        let mut learned = Vec::<(PLIndex, Rnd, PLEntry<BundleT>)>::new();
        for index in request.next_index..self.next_index {
          // Note that `index` will exist in `paxos_instances` because the only way it would not
          // is if a sufficiently high NextIndexResponse had arrived, which cannot be, since those
          // are sent directly between nodes and the network queues are FIFO.
          let paxos_instance = self.paxos_instances.get(&index).unwrap();
          // Note that `paxos_instance` has already learned a value, since `self.next_index` is
          // beyond `index` already.
          let (vrnd, vval) = paxos_instance.learned_rnd_val.clone().unwrap();
          learned.push((index, vrnd, vval))
        }
        ctx.send(
          &request.sender_eid,
          msg::PaxosDriverMessage::LogSyncResponse(msg::LogSyncResponse { learned }),
        );
      }
      PaxosDriverMessage::LogSyncResponse(response) => {
        // Learn all values sent here
        for (index, vrnd, vval) in response.learned {
          // We guard with `self.index`, since all indices prior are already learned.
          if index >= self.next_index {
            // Add the `learned_rnd_val`, creating an InstanceEntry if it is not present.
            if let Some(instance_entry) = self.paxos_instances.get_mut(&index) {
              if instance_entry.learned_rnd_val.is_none() {
                instance_entry.learned_rnd_val = Some((vrnd, vval));
              }
            } else {
              self.paxos_instances.insert(
                index.clone(),
                InstanceEntry { instance: None, learned_rnd_val: Some((vrnd, vval)) },
              );
            }
          }
        }

        // Extend `next_index` as far as possible.
        let mut learned_entries = self.deliver_learned_entries();

        // Poll and process all `buffered_messages` which an index that is low enough.
        self.handle_buffered_messages(ctx, &mut learned_entries);

        // Here, we have processed all the `should_learned`s we could, so we finish.
        return learned_entries;
      }
      PaxosDriverMessage::NextIndexRequest(request) => {
        let this_eid = ctx.this_eid();
        ctx.send(
          &request.sender_eid,
          msg::PaxosDriverMessage::NextIndexResponse(msg::NextIndexResponse {
            responder_eid: this_eid.clone(),
            next_index: self.next_index,
          }),
        );
      }
      PaxosDriverMessage::NextIndexResponse(response) => {
        let orig_min_index = self.min_complete_index();

        // Update remote index
        let cur_remote_index = self.remote_next_indices.get_mut(&response.responder_eid).unwrap();
        *cur_remote_index = max(*cur_remote_index, response.next_index);

        // Purge all PaxosInstances prior to the new min_index
        for index in orig_min_index..self.min_complete_index() {
          self.paxos_instances.remove(&index);
        }
      }
      PaxosDriverMessage::StartNewNode(start) => {
        // Here, we simply respond with a `NewNodeStarted`. Recall that redundant
        // `StartNewNode` messages like this are possible.
        let this_eid = ctx.this_eid().clone();
        ctx.send(
          &start.sender_eid,
          msg::PaxosDriverMessage::NewNodeStarted(msg::NewNodeStarted { paxos_node: this_eid }),
        );
      }
      PaxosDriverMessage::NewNodeStarted(started) => {
        self.unconfirmed_eids.remove(&started.paxos_node);
      }
    }

    Vec::new()
  }

  /// Process as many buffered messages as possible. At the end, there should be no
  /// messages left that is <= `next_index`, and nothing should be learnable at
  /// `next_index` either.
  fn handle_buffered_messages<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    learned_entries: &mut Vec<PLEntry<BundleT>>,
  ) {
    loop {
      // Poll and process a `buffered_messages` if there is one with low enough index.
      if let Some(mut entry) = self.buffered_messages.first_entry() {
        let index = entry.key().clone();
        if index <= self.next_index {
          let messages = entry.get_mut();
          if messages.is_empty() {
            self.buffered_messages.remove(&index);
          } else {
            // Here, there is a MultiPaxosMessage with low enough index, so we process it.
            let msg = msg::PaxosDriverMessage::MultiPaxosMessage(messages.pop_front().unwrap());
            learned_entries.extend(self.handle_paxos_message_internal(ctx, msg));
          }
          continue;
        }
      }
      break;
    }
  }

  /// Checks whether there are any new learned values passed `next_index`. If so, we increase
  /// `next_index` to the next unlearned index and returned all learned PLEntrys.
  fn deliver_learned_entries(&mut self) -> Vec<PLEntry<BundleT>> {
    // Collect all newly learned entries
    let mut new_entries = Vec::<PLEntry<BundleT>>::new();
    loop {
      if let Some(instance_entry) = self.paxos_instances.get(&self.next_index) {
        if let Some((_, learned_val)) = &instance_entry.learned_rnd_val {
          // There is a learned_val for this index.
          self.next_index += 1;
          match learned_val {
            PLEntry::Bundle(_) => {}
            PLEntry::ReconfigBundle(reconfig) => {
              // Process new_eids
              for eid in &reconfig.new_eids {
                self.remote_next_indices.insert(eid.clone(), self.next_index.clone());
                self.unconfirmed_eids.insert(eid.clone(), false);
                self.paxos_nodes.push(eid.clone());
              }

              // Process rem_eids
              for eid in &reconfig.rem_eids {
                self.remote_next_indices.remove(eid);
                self.unconfirmed_eids.remove(eid);
                remove_item(&mut self.paxos_nodes, eid);
              }
            }
            PLEntry::LeaderChanged(leader_changed) => {
              self.leader = leader_changed.lid.clone();
              self.leader_heartbeat = 0;
            }
          }

          new_entries.push(learned_val.clone());
          continue;
        }
      }
      break;
    }

    if !new_entries.is_empty() {
      // If a PLEntry was inserted, we clear `next_insert` to avoid accidentally
      // inserting it at an index it was not meant for.
      self.next_insert = None;
    }

    return new_entries;
  }

  // -----------------------------------------------------------------------------------------------
  //  Bundle Insertion
  // -----------------------------------------------------------------------------------------------

  pub fn insert_bundle<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    user_entry: UserPLEntry<BundleT>,
  ) {
    // We guard for the case that this node is the leader and that there is not already
    // something that is attempting to be inserted.
    if self.is_leader(ctx) && self.next_insert.is_none() {
      let uuid = mk_uuid(ctx.rand());
      self.next_insert = Some((uuid.clone(), user_entry.clone()));

      // Schedule a retry in `retry_defer_time_ms` ms.
      let defer_time = self.paxos_config.retry_defer_time_ms.clone();
      ctx.defer(defer_time, PaxosTimerEvent::RetryInsert(uuid));

      // Propose the bundle at the next index.
      self.propose_next_index(ctx, user_entry.convert());
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  Timer Events
  // -----------------------------------------------------------------------------------------------

  pub fn timer_event<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    event: PaxosTimerEvent,
  ) {
    match event {
      PaxosTimerEvent::RetryInsert(uuid) => {
        self.retry_insert(ctx, uuid);
      }
      PaxosTimerEvent::LeaderHeartbeat => {
        self.leader_heartbeat(ctx);
      }
      PaxosTimerEvent::NextIndex => {
        self.next_index_timer(ctx);
      }
    }
  }

  fn retry_insert<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    uuid: UUID,
  ) {
    if let Some((cur_uuid, user_entry)) = &self.next_insert {
      // Check if the incoming `uuid` is meant for this `next_insert`.
      if cur_uuid == &uuid {
        // Schedule a retry in `retry_defer_time_ms` ms.
        let defer_time = self.paxos_config.retry_defer_time_ms.clone();
        ctx.defer(defer_time, PaxosTimerEvent::RetryInsert(uuid));

        // Propose the bundle at the next index.
        self.propose_next_index(ctx, user_entry.clone().convert());
      }
    }
  }

  fn leader_heartbeat<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
  ) {
    if self.is_leader(ctx) {
      // Send out IsLeader to each PaxosNode.
      for eid in &self.paxos_nodes {
        ctx.send(
          &eid,
          msg::PaxosDriverMessage::IsLeader(msg::IsLeader { lid: self.leader.clone() }),
        );
      }

      // Broadcast `InformLearned` to all PaxosNodes
      self.broadcast_inform_learned(ctx);
    } else {
      // Increment Heartbeat counter
      self.leader_heartbeat += 1;
      if self.leader_heartbeat > self.paxos_config.heartbeat_threshold {
        // This node tries proposing itself as the leader.
        let gen = self.leader.gen.next();
        let eid = ctx.this_eid().clone();
        self.propose_next_index(
          ctx,
          PLEntry::LeaderChanged(LeaderChanged { lid: LeadershipId { gen, eid } }),
        );

        // Broadcast `InformLearned` to all PaxosNodes
        self.broadcast_inform_learned(ctx);
      }
    }

    // Schedule another `LeaderHeartbeat`
    ctx.defer(self.paxos_config.heartbeat_period_ms.clone(), PaxosTimerEvent::LeaderHeartbeat);
  }

  fn next_index_timer<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
  ) {
    // Send out NextIndexRequest
    let this_eid = ctx.this_eid().clone();
    for eid in &self.paxos_nodes {
      ctx.send(
        &eid,
        msg::PaxosDriverMessage::NextIndexRequest(msg::NextIndexRequest {
          sender_eid: this_eid.clone(),
        }),
      );
    }

    // Schedule another `NextIndex`
    ctx.defer(self.paxos_config.next_index_period_ms.clone(), PaxosTimerEvent::NextIndex);
  }

  // -----------------------------------------------------------------------------------------------
  //  Utilities
  // -----------------------------------------------------------------------------------------------

  /// Checks if all `EndpointId` in `eids` is in `paxos_nodes`.
  pub fn contains_nodes(&self, eids: &Vec<EndpointId>) -> bool {
    for eid in eids {
      if !self.paxos_nodes.contains(eid) {
        return false;
      }
    }

    true
  }

  /// Propose `entry` at the `self.next_index` once.
  fn propose_next_index<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    entry: PLEntry<BundleT>,
  ) {
    // Create a PaxosInstance if it does not exist already and start inserting.
    if let Some(instance_entry) = self.paxos_instances.get_mut(&self.next_index) {
      if instance_entry.instance.is_none() {
        instance_entry.instance = Some(PaxosInstance::new());
      }
    } else {
      self.paxos_instances.insert(
        self.next_index.clone(),
        InstanceEntry { instance: Some(PaxosInstance::new()), learned_rnd_val: None },
      );
    }

    // Get PaxosInstance
    let instance_entry = self.paxos_instances.get_mut(&self.next_index).unwrap();
    let paxos_instance = instance_entry.instance.as_mut().unwrap();

    // Compute next proposal number.
    let latest_rnd = paxos_instance.proposer_state.latest_crnd;
    let next_rnd = (ctx.rand().next_u32() % self.paxos_config.proposal_increment) + latest_rnd;

    // Update the ProposerState
    paxos_instance
      .proposer_state
      .proposals
      .insert(next_rnd, Proposal { crnd: next_rnd, cval: entry.clone(), promises: vec![] });
    paxos_instance.proposer_state.latest_crnd = next_rnd;

    // Send out Prepare
    let this_eid = ctx.this_eid().clone();
    for eid in &self.paxos_nodes {
      ctx.send(
        &eid,
        msg::PaxosDriverMessage::MultiPaxosMessage(msg::MultiPaxosMessage {
          sender_eid: this_eid.clone(),
          paxos_nodes: self.paxos_nodes.clone(),
          index: self.next_index.clone(),
          paxos_message: msg::PaxosMessage::Prepare(msg::Prepare { crnd: next_rnd }),
        }),
      );
    }
  }

  /// Broadcast `InformLearned` to all PaxosNodes
  fn broadcast_inform_learned<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
  ) {
    let this_eid = ctx.this_eid().clone();
    for eid in &self.paxos_nodes {
      let mut should_learned = BTreeMap::<PLIndex, Rnd>::new();
      for index in *self.remote_next_indices.get(eid).unwrap()..self.next_index {
        // Note that `index` will exist in the `paxos_instances` and there will surely be a
        // learned value (since it is less than `self.next_index`).
        let instance_entry = self.paxos_instances.get(&index).unwrap();
        let (vrnd, _) = instance_entry.learned_rnd_val.clone().unwrap();
        should_learned.insert(index.clone(), vrnd);
      }
      ctx.send(
        &eid,
        msg::PaxosDriverMessage::InformLearned(msg::InformLearned {
          sender_eid: this_eid.clone(),
          should_learned,
        }),
      );
    }
  }
}
