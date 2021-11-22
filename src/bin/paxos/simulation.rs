use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::RangeEnds;
use runiversal::model::common::{EndpointId, Timestamp};
use runiversal::model::message as msg;
use runiversal::paxos::{PaxosContextBase, PaxosDriver, PaxosTimerEvent};
use runiversal::simulation_utils::{add_msg, mk_paxos_eid};
use std::cmp::min;
use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc::channel;

// -----------------------------------------------------------------------------------------------
//  PaxosContext
// -----------------------------------------------------------------------------------------------

pub struct PaxosContext<'a> {
  rand: &'a mut XorShiftRng,
  current_time: u128,
  queues: &'a mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_eid: &'a EndpointId,

  /// Deferred timer tasks
  tasks: &'a mut BTreeMap<Timestamp, Vec<PaxosTimerEvent>>,
}

impl<'a> PaxosContextBase<SimpleBundle> for PaxosContext<'a> {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    self.rand
  }

  fn this_eid(&self) -> &EndpointId {
    self.this_eid
  }

  fn send(&mut self, eid: &EndpointId, message: msg::PaxosDriverMessage<SimpleBundle>) {
    add_msg(&mut self.queues, &mut self.nonempty_queues, message, &self.this_eid, eid);
  }

  fn defer(&mut self, defer_time: u128, timer_event: PaxosTimerEvent) {
    let deferred_time = self.current_time + defer_time;
    if let Some(timer_inputs) = self.tasks.get_mut(&deferred_time) {
      timer_inputs.push(timer_event);
    } else {
      self.tasks.insert(deferred_time, vec![timer_event]);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Simulation
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleBundle {
  val: u32,
}

type NetworkMessage = msg::PaxosDriverMessage<SimpleBundle>;

#[derive(Debug)]
pub struct PaxosNodeData {
  paxos_driver: PaxosDriver<SimpleBundle>,
  tasks: BTreeMap<Timestamp, Vec<PaxosTimerEvent>>,
  /// The Local PaxosLogs that accumulate the results for every node.
  pub paxos_log: VecDeque<msg::PLEntry<SimpleBundle>>,
}

#[derive(Debug)]
pub enum QueuePauseMode {
  /// Here, a queue is down permanently until it is explicitly brought back up.
  Permanent,
  /// Here, a queue is down only for as many milliseconds as `u32`.
  Temporary(u32),
}

#[derive(Debug)]
pub struct SimConfig {
  /// These are the faction of queues that are not Permanently blocked
  /// that we want to make Temporarily blocked.
  pub target_temp_blocked_frac: f32,
  /// The maximum amount of time a queue can be Paused in `Temporary` at a time.
  pub max_pause_time_ms: u32,
}

#[derive(Debug)]
pub struct Simulation {
  pub rand: XorShiftRng,

  /// Basic Network

  /// Message queues between all nodes in the network
  queues: BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<NetworkMessage>>>,
  /// The set of queues that have messages to deliever
  nonempty_queues: Vec<(EndpointId, EndpointId)>,
  /// All `EndpointIds`.
  pub address_config: Vec<EndpointId>,

  /// Network queue pausing

  /// Holds the set of queues that are temporarily unavailable for delivering messages.
  /// This holds a probability value that will be used to bring the queue back up.
  paused_queues: BTreeMap<(EndpointId, EndpointId), QueuePauseMode>,
  /// The number of elements in `paused_queues` where `QueuePauseMode` is `Permanent`.
  num_permanent_down: u32,

  /// PaxosNodes and Global Paxos Log

  /// The set of PaxosDrivers that make up the PaxosGroups
  pub paxos_data: BTreeMap<EndpointId, PaxosNodeData>,
  /// The highest index that has been learned by all `PaxosDrivers`.
  pub max_common_index: usize,
  /// Global Paxos Log that is constructed as PLEntry's are learned by at least one PaxosNode,
  /// and where the learned values by other PaxosNode's are cross-checked against.
  pub global_paxos_log: Vec<msg::PLEntry<SimpleBundle>>,

  /// Meta
  next_int: u32,
  true_timestamp: u128,
  config: SimConfig,
}

impl Simulation {
  /// Here, we create as many `PaxosNodeData`s as `num_paxos_data`.
  pub fn new(seed: [u8; 16], num_paxos_data: u32, config: SimConfig) -> Simulation {
    assert!(num_paxos_data > 0); // We expect to at least have one node.
    let mut sim = Simulation {
      rand: XorShiftRng::from_seed(seed),
      queues: Default::default(),
      nonempty_queues: Default::default(),
      address_config: Default::default(),
      paused_queues: Default::default(),
      num_permanent_down: 0,
      paxos_data: Default::default(),
      max_common_index: 0,
      global_paxos_log: vec![],
      next_int: 0,
      true_timestamp: Default::default(),
      config,
    };

    // PaxosNode EndpointIds
    let eids: Vec<EndpointId> =
      RangeEnds::rvec(0, num_paxos_data as i32).iter().map(mk_paxos_eid).collect();
    for from_eid in &eids {
      sim.queues.insert(from_eid.clone(), Default::default());
      for to_eid in &eids {
        sim.queues.get_mut(from_eid).unwrap().insert(to_eid.clone(), VecDeque::new());
      }
    }
    sim.address_config = eids.clone();

    // Construct PaxosDrivers
    for eid in eids.clone() {
      sim.paxos_data.insert(
        eid.clone(),
        PaxosNodeData {
          paxos_driver: PaxosDriver::new(eids.clone()),
          tasks: Default::default(),
          paxos_log: Default::default(),
        },
      );
    }

    Self::initialize_paxos_nodes(sim)
  }

  /// Starts the Async insertion cycles of Paxos.
  fn initialize_paxos_nodes(mut sim: Simulation) -> Simulation {
    let leader_eid = mk_paxos_eid(&0);
    for (eid, paxos_data) in &mut sim.paxos_data {
      let current_time = sim.true_timestamp;
      let mut ctx = PaxosContext {
        rand: &mut sim.rand,
        current_time, // TODO: simulate clock skew
        queues: &mut sim.queues,
        nonempty_queues: &mut sim.nonempty_queues,
        this_eid: &leader_eid,
        tasks: &mut paxos_data.tasks,
      };

      // If this node is the first Leader, then start inserting.
      if eid == &leader_eid {
        paxos_data.paxos_driver.insert_bundle(&mut ctx, SimpleBundle { val: sim.next_int });
      }

      // Start Paxos Timer Events
      paxos_data.paxos_driver.timer_event(&mut ctx, PaxosTimerEvent::LeaderHeartbeat);
      paxos_data.paxos_driver.timer_event(&mut ctx, PaxosTimerEvent::NextIndex);
    }

    return sim;
  }

  // -----------------------------------------------------------------------------------------------
  //  Simulation Methods
  // -----------------------------------------------------------------------------------------------

  /// Add a message between two nodes in the network.
  fn add_msg(&mut self, msg: NetworkMessage, from_eid: &EndpointId, to_eid: &EndpointId) {
    add_msg(&mut self.queues, &mut self.nonempty_queues, msg, from_eid, to_eid);
  }

  /// Poll a message between two nodes in the network.
  fn poll_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) -> Option<NetworkMessage> {
    let queue = self.queues.get_mut(from_eid).unwrap().get_mut(to_eid).unwrap();
    if queue.len() == 1 {
      if let Some(index) = self
        .nonempty_queues
        .iter()
        .position(|(from_eid2, to_eid2)| from_eid2 == from_eid && to_eid2 == to_eid)
      {
        self.nonempty_queues.remove(index);
      }
    }
    queue.pop_front()
  }

  /// Add the `entries` to the Local PaxosLog of at `eid`.
  fn extend_paxos_log(&mut self, eid: &EndpointId, entries: Vec<msg::PLEntry<SimpleBundle>>) {
    let paxos_data = self.paxos_data.get_mut(eid).unwrap();

    // Add the PLEntrys to `global_paxos_log`, and verify that they match.
    for entry in entries.into_iter() {
      let next_index = self.max_common_index + paxos_data.paxos_log.len();
      if let Some(global_entry) = self.global_paxos_log.get(next_index) {
        assert_eq!(global_entry, &entry);
      } else {
        self.global_paxos_log.push(entry.clone());
      }
      paxos_data.paxos_log.push_back(entry);
    }

    // Remove PLEntrys in the Local PaxosLogs if all PaxosNodes have it.
    let mut num_to_remove: Option<usize> = None;
    for (_, paxos_data) in &self.paxos_data {
      if let Some(num) = &mut num_to_remove {
        *num = min(*num, paxos_data.paxos_log.len());
      } else {
        num_to_remove = Some(paxos_data.paxos_log.len());
      }
    }

    let num_to_remove = num_to_remove.unwrap();
    for (_, paxos_data) in &mut self.paxos_data {
      for _ in 0..num_to_remove {
        paxos_data.paxos_log.pop_front();
      }
    }

    self.max_common_index += num_to_remove;
  }

  /// When this is called, the `msg` will already have been popped from `queues`.
  /// This function will run the PaxosNode at the `to_eid` end, which might have
  /// any number of side effects, including adding new messages into `queues`.
  fn run_paxos_message(&mut self, _: &EndpointId, to_eid: &EndpointId, msg: NetworkMessage) {
    let paxos_data = self.paxos_data.get_mut(to_eid).unwrap();
    let current_time = self.true_timestamp;
    let mut ctx = PaxosContext {
      rand: &mut self.rand,
      current_time, // TODO: simulate clock skew
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_eid: to_eid,
      tasks: &mut paxos_data.tasks,
    };

    let entries = paxos_data.paxos_driver.handle_paxos_message(&mut ctx, msg);
    if !entries.is_empty() && paxos_data.paxos_driver.is_leader(&ctx) {
      // Start inserting a new SimpleBundle
      self.next_int += 1;
      paxos_data.paxos_driver.insert_bundle(&mut ctx, SimpleBundle { val: self.next_int });
    }

    self.extend_paxos_log(to_eid, entries);
  }

  /// The endpoints provided must exist. This function polls a message from
  /// the message queue between them and delivers it to the PaxosDriver at `to_eid`.
  fn deliver_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) {
    if let Some(msg) = self.poll_msg(from_eid, to_eid) {
      self.run_paxos_message(from_eid, to_eid, msg);
    }
  }

  /// Execute the timer events up to the current `true_timestamp`.
  pub fn run_timer_events(&mut self) {
    for (eid, paxos_data) in &mut self.paxos_data {
      let current_time = self.true_timestamp;
      let mut ctx = PaxosContext {
        rand: &mut self.rand,
        current_time, // TODO: simulate clock skew
        queues: &mut self.queues,
        nonempty_queues: &mut self.nonempty_queues,
        this_eid: eid,
        tasks: &mut paxos_data.tasks,
      };

      // Execute all async timer tasks.
      loop {
        if let Some((next_timestamp, _)) = ctx.tasks.first_key_value() {
          if next_timestamp <= &current_time {
            // All data in this first entry should be dispatched.
            let next_timestamp = next_timestamp.clone();
            for timer_input in ctx.tasks.remove(&next_timestamp).unwrap() {
              paxos_data.paxos_driver.timer_event(&mut ctx, timer_input);
            }
            continue;
          }
        }

        // This means there are no more left.
        break;
      }
    }
  }

  /// This function simply increments the `true_time` by 1ms and delivers 1ms worth of
  /// messages. For simplicity, we assume that this means that every non-empty queue
  /// of messages delivers about one message in this time.
  pub fn simulate1ms(&mut self) {
    self.true_timestamp += 1;
    let num_msgs_to_deliver = self.nonempty_queues.len();
    for _ in 0..num_msgs_to_deliver {
      let r = self.rand.next_u32() as usize % self.nonempty_queues.len();
      let channel_key = self.nonempty_queues.get(r).unwrap().clone();
      // Only deliver a message if the queue is not paused.
      if !self.paused_queues.contains_key(&channel_key) {
        let (from_eid, to_eid) = channel_key;
        self.deliver_msg(&from_eid, &to_eid);
      }
    }

    self.run_timer_events();
    self.update_paused_queues();
  }

  fn update_paused_queues(&mut self) {
    // These are the faction of queues that are not Permanently blocked that we want to
    // make Temporarily blocked.
    let num_queues = self.address_config.len().pow(2) as u32;
    let target_num_blocked =
      ((num_queues - self.num_permanent_down) as f32 * self.config.target_temp_blocked_frac) as u32;
    if self.paused_queues.len() as u32 - self.num_permanent_down < target_num_blocked {
      let from_idx = self.rand.next_u32() as usize % self.address_config.len();
      let to_idx = self.rand.next_u32() as usize % self.address_config.len();
      let channel_key = (
        self.address_config.get(from_idx).unwrap().clone(),
        self.address_config.get(to_idx).unwrap().clone(),
      );

      // Only amend `channel_key` to `paused_queues` if it is not already there.
      if !self.paused_queues.contains_key(&channel_key) {
        if self.config.max_pause_time_ms > 0 {
          // Choose a duration that is > 0, and add it in.
          let duration = (self.rand.next_u32() % self.config.max_pause_time_ms) + 1;
          self.paused_queues.insert(channel_key, QueuePauseMode::Temporary(duration));
        }
      }
    }

    // Decrement durations in `paused_queues`, removing keys if it goes to 0.
    let mut keys_to_remove = Vec::<(EndpointId, EndpointId)>::new();
    for (channel_key, pause_mode) in &mut self.paused_queues {
      match pause_mode {
        QueuePauseMode::Permanent => {}
        QueuePauseMode::Temporary(duration) => {
          *duration -= 1;
          if *duration == 0 {
            keys_to_remove.push(channel_key.clone());
          }
        }
      }
    }
    for key in keys_to_remove {
      self.paused_queues.remove(&key);
    }
  }

  /// Pause the message deliver of a queue permanently.
  pub fn block_queue_permanently(&mut self, from_eid: EndpointId, to_eid: EndpointId) {
    let channel_key = (from_eid, to_eid);
    if let Some(pause_mode) = self.paused_queues.get_mut(&channel_key) {
      match pause_mode {
        QueuePauseMode::Permanent => {}
        QueuePauseMode::Temporary(_) => {
          *pause_mode = QueuePauseMode::Permanent;
          self.num_permanent_down += 1;
        }
      }
    } else {
      self.paused_queues.insert(channel_key, QueuePauseMode::Permanent);
      self.num_permanent_down += 1;
    }
  }

  pub fn simulate_n_ms(&mut self, n: u32) {
    for _ in 0..n {
      self.simulate1ms();
    }
  }
}
