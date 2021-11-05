use crate::common::{mk_uuid, UUID};
use crate::model::common::{EndpointId, Gen, LeadershipId, Timestamp};
use crate::model::message as msg;
use crate::model::message::{
  LeaderChanged, PLEntry, PLIndex, PaxosDriverMessage, PaxosMessage, Rnd,
};
use rand::RngCore;
use sqlparser::dialect::keywords::Keyword::NEXT;
use std::cmp::{max, min};
use std::collections::BTreeMap;

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
  /// The small `Vrnd` with a majority count.
  learned_vrnd: Option<Vrnd>,
}

#[derive(Debug)]
pub struct PaxosInstance<BundleT> {
  proposer_state: ProposerState<BundleT>,
  acceptor_state: AcceptorState<BundleT>,
  learner_state: LearnerState,

  /// The learned value for the PaxosInstance. This could be through any means,
  /// including Paxos and LogSyncResponse.
  learned_rndval: Option<(Vrnd, PLEntry<BundleT>)>,
}

// -----------------------------------------------------------------------------------------------
//  Constants
// -----------------------------------------------------------------------------------------------

const HEARTBEAT_THRESHOLD: u32 = 5;
const HEARTBEAT_PRIOD: u128 = 1;
const NEXT_INDEX_PERIOD: u128 = 1;
const RETRY_DEFER_TIME: u128 = 1;
const PROPOSAL_INCREMENT: u32 = 1000;

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
  NextIndex,
}

// -----------------------------------------------------------------------------------------------
//  PaxosDriver
// -----------------------------------------------------------------------------------------------

pub fn majority<T>(vec: &Vec<T>) -> usize {
  vec.len() / 2 + 1
}

#[derive(Debug)]
pub struct PaxosDriver<BundleT> {
  /// Metadata
  pub paxos_nodes: Vec<EndpointId>,

  ///  PaxosInstance state

  /// Maps all PaxosNodes in this PaxosGroup to the last known `PLIndex` that was returned
  /// by a `NextIndexResponse`.
  pub remote_next_indices: BTreeMap<EndpointId, PLIndex>,
  pub next_index: PLIndex,
  pub paxos_instances: BTreeMap<PLIndex, PaxosInstance<BundleT>>,

  /// Leadership state
  pub leader: LeadershipId,
  pub leader_heartbeat: u32,

  /// Insert state
  pub next_insert: Option<(UUID, BundleT)>,
}

impl<BundleT: Clone> PaxosDriver<BundleT> {
  /// Constructs a `PaxosDriver` for a Slave that is part of the initial system bootstrap.
  pub fn new(paxos_nodes: Vec<EndpointId>) -> PaxosDriver<BundleT> {
    let mut remote_next_indices = BTreeMap::<EndpointId, PLIndex>::new();
    for node in &paxos_nodes {
      remote_next_indices.insert(node.clone(), 0);
    }
    let leader_eid = paxos_nodes[0].clone();
    PaxosDriver {
      paxos_nodes,
      remote_next_indices,
      next_index: 0,
      paxos_instances: Default::default(),
      leader: LeadershipId { gen: Gen(0), eid: leader_eid },
      leader_heartbeat: 0,
      next_insert: None,
    }
  }

  fn min_complete_index(&self) -> u128 {
    // We start with `next_index` since the minimum in `remote_next_indices`
    // is certainly <= to this.
    let mut min_index = self.next_index;
    for remote_index in self.remote_next_indices.values() {
      min_index = min(min_index, *remote_index);
    }
    min_index
  }

  fn is_leader<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &self,
    ctx: &mut PaxosContextBaseT,
  ) -> bool {
    &self.leader.eid == ctx.this_eid()
  }

  fn deliver_learned_entries(&mut self) -> Vec<PLEntry<BundleT>> {
    // Collect all newly learned entries
    let mut new_entries = Vec::<PLEntry<BundleT>>::new();
    loop {
      if let Some(paxos_instance) = self.paxos_instances.get(&self.next_index) {
        if let Some((_, learned_val)) = &paxos_instance.learned_rndval {
          new_entries.push(learned_val.clone());
          self.next_index += 1;
          continue;
        }
      }
      break;
    }

    // We guard against is_empty to avoid clearing `next_insert` in spuriously,
    // which may cause a deadlock.
    if !new_entries.is_empty() {
      // Ensure next_insert is None.
      self.next_insert = None;

      // Potentially change the Leadership
      for entry in new_entries.iter().rev() {
        match entry {
          PLEntry::Bundle(_) => {}
          PLEntry::LeaderChanged(leader_changed) => {
            self.leader = leader_changed.lid.clone();
            self.leader_heartbeat = 0;
            break;
          }
        }
      }
    }

    return new_entries;
  }

  // Message Handler

  pub fn handle_paxos_message<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    message: msg::PaxosDriverMessage<BundleT>,
  ) -> Vec<msg::PLEntry<BundleT>> {
    match message {
      PaxosDriverMessage::MultiPaxosMessage(multi) => {
        if let msg::PaxosMessage::Prepare(_) = multi.paxos_message {
          if multi.sender_eid != self.leader.eid && self.leader_heartbeat < HEARTBEAT_THRESHOLD {
            // If we get a `Prepare` message from a non-leader, and the leader still seems
            // to be alive, we drop the message. This is just a simple technique we use to reduce
            // the rate of spurious leadership changes.
            return Vec::new();
          }
        }

        if multi.index < self.min_complete_index() {
          // We drop this message if it is below `min_complete_index`
          return Vec::new();
        }

        // Create a PaxosInstance if it does not exist already
        if !self.paxos_instances.contains_key(&multi.index) {
          self.paxos_instances.insert(
            multi.index.clone(),
            PaxosInstance {
              proposer_state: ProposerState { proposals: Default::default(), latest_crnd: 0 },
              acceptor_state: AcceptorState { rnd: 0, vrnd_vval: None },
              learner_state: LearnerState { learned: Default::default(), learned_vrnd: None },
              learned_rndval: None,
            },
          );
        }

        // Finally, forward the PaxosMessage to the PaxosInstance
        let paxos_instance = self.paxos_instances.get_mut(&multi.index).unwrap();
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
              if let Some((vrnd, vval)) = max_vrnd_vval {
                // Broadcast
                let this_eid = ctx.this_eid().clone();
                for eid in &self.paxos_nodes {
                  ctx.send(
                    &eid,
                    msg::PaxosDriverMessage::MultiPaxosMessage(msg::MultiPaxosMessage {
                      sender_eid: this_eid.clone(),
                      index: multi.index.clone(),
                      paxos_message: msg::PaxosMessage::Accept(msg::Accept {
                        crnd: vrnd.clone(),
                        cval: vval.clone(),
                      }),
                    }),
                  );
                }
              }
            }
          }
          PaxosMessage::Accept(accept) => {
            let state = &mut paxos_instance.acceptor_state;
            if accept.crnd > state.rnd {
              state.rnd = accept.crnd.clone();
              state.vrnd_vval = Some((accept.crnd.clone(), accept.cval.clone()));

              // Broadcast
              let this_eid = ctx.this_eid().clone();
              for eid in &self.paxos_nodes {
                ctx.send(
                  &eid,
                  msg::PaxosDriverMessage::MultiPaxosMessage(msg::MultiPaxosMessage {
                    sender_eid: this_eid.clone(),
                    index: multi.index.clone(),
                    paxos_message: msg::PaxosMessage::Learn(msg::Learn {
                      vrnd: accept.crnd.clone(),
                    }),
                  }),
                );
              }

              // Check if the newly accepted value is >= to a learned vrnd
              if paxos_instance.learned_rndval.is_none() {
                if let Some(vrnd) = paxos_instance.learner_state.learned_vrnd {
                  if vrnd <= accept.crnd.clone() {
                    paxos_instance.learned_rndval = Some((vrnd, accept.cval.clone()));
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

              // Check if the newly learned value is <= to an accepted vrnd already
              if paxos_instance.learned_rndval.is_none() {
                if let Some((vrnd, vval)) = &paxos_instance.acceptor_state.vrnd_vval {
                  if learn.vrnd <= vrnd.clone() {
                    paxos_instance.learned_rndval = Some((learn.vrnd, vval.clone()));
                    return self.deliver_learned_entries();
                  }
                }
              }
            }
          }
        }
      }
      PaxosDriverMessage::IsLeader(is_leader) => {
        if is_leader.leadership_id == self.leader {
          // Reset heartbeat
          self.leader_heartbeat = 0;
        }

        if !is_leader.should_learned.is_empty() {
          let (expected_index, _) = is_leader.should_learned.last().unwrap().clone();

          // Process every learned Vrnd.
          for (index, vrnd) in is_leader.should_learned {
            if let Some(paxos_instance) = self.paxos_instances.get_mut(&index) {
              if let Some(cur_vrnd) = paxos_instance.learner_state.learned_vrnd {
                if vrnd < cur_vrnd {
                  // The newly learned Vrnd is lower than the existing, so we update it.
                  paxos_instance.learner_state.learned_vrnd = Some(vrnd);
                }
              } else {
                paxos_instance.learner_state.learned_vrnd = Some(vrnd);
              }

              let learned_rnd = paxos_instance.learner_state.learned_vrnd.unwrap();

              // Check if the newly learned value is <= to an accepted vrnd already
              if paxos_instance.learned_rndval.is_none() {
                if let Some((vrnd, vval)) = &paxos_instance.acceptor_state.vrnd_vval {
                  if learned_rnd <= vrnd.clone() {
                    paxos_instance.learned_rndval = Some((learned_rnd, vval.clone()));
                  }
                }
              }
            }
          }

          // Get all new PLEntries. Recall that this also update
          // `self.next_index` as high as possible.
          let entries = self.deliver_learned_entries();

          // Check if we we had all the learned values present in the Acceptor
          // state, and broadcast LogSyncRequest if we did not.
          if self.next_index < expected_index {
            // Broadcast
            let this_eid = ctx.this_eid().clone();
            for eid in &self.paxos_nodes {
              ctx.send(
                &eid,
                msg::PaxosDriverMessage::LogSyncRequest(msg::LogSyncRequest {
                  sender_eid: this_eid.clone(),
                  next_index: self.next_index,
                }),
              );
            }
          }

          // Return all learned values.
          return entries;
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
          let (vrnd, vval) = paxos_instance.learned_rndval.clone().unwrap();
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
          // We guard with `self.index`, since all indicies prior are already learned.
          if index >= self.next_index {
            // Create a PaxosInstance if it does not exist already
            if !self.paxos_instances.contains_key(&index) {
              self.paxos_instances.insert(
                index.clone(),
                PaxosInstance {
                  proposer_state: ProposerState { proposals: Default::default(), latest_crnd: 0 },
                  acceptor_state: AcceptorState { rnd: 0, vrnd_vval: None },
                  learner_state: LearnerState { learned: Default::default(), learned_vrnd: None },
                  learned_rndval: None,
                },
              );
            }

            // Get PaxosInstance
            let paxos_instance = self.paxos_instances.get_mut(&index).unwrap();
            paxos_instance.learned_rndval = Some((vrnd, vval));
          }
        }

        return self.deliver_learned_entries();
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
    }
    Vec::new()
  }

  // Bundle Insertion

  pub fn insert_bundle<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    bundle: BundleT,
  ) {
    // We guard for the case that this node is the leader and that there is not already
    // something that is attempting to be inserted.
    if self.is_leader(ctx) && self.next_insert.is_none() {
      let uuid = mk_uuid(ctx.rand());
      self.next_insert = Some((uuid.clone(), bundle.clone()));

      // Schedule a retry in 1 ms.
      ctx.defer(RETRY_DEFER_TIME, PaxosTimerEvent::RetryInsert(uuid));

      // Propose the bundle at the next index.
      self.propose_next_index(ctx, PLEntry::Bundle(bundle));
    }
  }

  fn propose_next_index<PaxosContextBaseT: PaxosContextBase<BundleT>>(
    &mut self,
    ctx: &mut PaxosContextBaseT,
    entry: PLEntry<BundleT>,
  ) {
    // Create a PaxosInstance if it does not exist already and start inserting.
    if !self.paxos_instances.contains_key(&self.next_index) {
      self.paxos_instances.insert(
        self.next_index.clone(),
        PaxosInstance {
          proposer_state: ProposerState { proposals: Default::default(), latest_crnd: 0 },
          acceptor_state: AcceptorState { rnd: 0, vrnd_vval: None },
          learner_state: LearnerState { learned: Default::default(), learned_vrnd: None },
          learned_rndval: None,
        },
      );
    }

    // Get PaxosInstance
    let paxos_instance = self.paxos_instances.get_mut(&self.next_index).unwrap();

    // Compute next proposal number.
    let latest_rnd = paxos_instance.proposer_state.latest_crnd;
    let next_rnd = (ctx.rand().next_u32() % PROPOSAL_INCREMENT) + latest_rnd;

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
          index: self.next_index.clone(),
          paxos_message: msg::PaxosMessage::Prepare(msg::Prepare { crnd: next_rnd }),
        }),
      );
    }
  }

  // Timer Events

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
    if let Some((cur_uuid, cur_bundle)) = &self.next_insert {
      if cur_uuid == &uuid {
        // Here, since `next_insert` has the same UUID, we nothing could have been
        // inserted in the meanwhile.

        // Schedule another retry in 1 ms.
        ctx.defer(RETRY_DEFER_TIME, PaxosTimerEvent::RetryInsert(uuid));

        // Propose the bundle at the next index.
        self.propose_next_index(ctx, PLEntry::Bundle(cur_bundle.clone()));
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
        let mut should_learned = Vec::<(PLIndex, Rnd)>::new();
        for index in *self.remote_next_indices.get(eid).unwrap()..self.next_index {
          // Note that `index` will exist in the `paxos_instances` and there will surely be a
          // learned value (since it is less than `self.next_index`).
          let paxos_instance = self.paxos_instances.get(&index).unwrap();
          let (vrnd, _) = paxos_instance.learned_rndval.clone().unwrap();
          should_learned.push((index.clone(), vrnd));
        }
        ctx.send(
          &eid,
          msg::PaxosDriverMessage::IsLeader(msg::IsLeader {
            leadership_id: self.leader.clone(),
            should_learned,
          }),
        );
      }
    } else {
      // Increment Heartbeat counter
      self.leader_heartbeat += 1;
      if self.leader_heartbeat > HEARTBEAT_THRESHOLD {
        // This node tries proposing itself as the leader.
        let gen = self.leader.gen.next();
        let eid = ctx.this_eid().clone();
        self.propose_next_index(
          ctx,
          PLEntry::LeaderChanged(LeaderChanged { lid: LeadershipId { gen, eid } }),
        );
      }
    }

    // Schedule another `LeaderHeartbeat`
    ctx.defer(HEARTBEAT_PRIOD, PaxosTimerEvent::LeaderHeartbeat);
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
    ctx.defer(NEXT_INDEX_PERIOD, PaxosTimerEvent::NextIndex);
  }
}
