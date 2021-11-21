use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::rvec;
use runiversal::model::common::{EndpointId, Timestamp};
use runiversal::model::message as msg;
use runiversal::paxos::{PaxosContextBase, PaxosDriver, PaxosTimerEvent};
use std::collections::{BTreeMap, VecDeque};

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

const NUM_COORDS: u32 = 3;

type NetworkMessage = msg::PaxosDriverMessage<SimpleBundle>;

#[derive(Debug)]
pub struct PaxosNodeData {
  paxos_driver: PaxosDriver<SimpleBundle>,
  tasks: BTreeMap<Timestamp, Vec<PaxosTimerEvent>>,
  /// The Local PaxosLogs that accumulate the results for every node.
  pub paxos_log: Vec<msg::PLEntry<SimpleBundle>>,
}

#[derive(Debug)]
pub struct Simulation {
  pub rand: XorShiftRng,

  /// Message queues between all nodes in the network
  queues: BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<NetworkMessage>>>,
  /// The set of queues that have messages to deliever
  nonempty_queues: Vec<(EndpointId, EndpointId)>,
  /// Holds the set of queues that are temporarily unavailable for delivering messages.
  /// This holds a probability value that will be used to bring the queue back up.
  paused_queues: BTreeMap<(EndpointId, EndpointId), u32>,
  address_config: Vec<EndpointId>,

  // The set of PaxosDrivers that make up the PaxosGroups
  pub paxos_data: BTreeMap<EndpointId, PaxosNodeData>,

  /// Meta
  next_int: u32,
  true_timestamp: u128,
}

impl Simulation {
  /// Here, we create as many `PaxosNodeData`s as `num_paxos_data`.
  pub fn new(seed: [u8; 16], num_paxos_data: u32) -> Simulation {
    let mut sim = Simulation {
      rand: XorShiftRng::from_seed(seed),
      queues: Default::default(),
      nonempty_queues: Default::default(),
      paused_queues: Default::default(),
      address_config: vec![],
      paxos_data: Default::default(),
      next_int: 0,
      true_timestamp: Default::default(),
    };

    // PaxosNode EndpointIds
    let eids: Vec<EndpointId> = rvec(0, num_paxos_data as i32).iter().map(paxos_eid).collect();
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

    // Start inserting the first SimpleBundle at the leader.
    let leader_eid = paxos_eid(&0);
    let paxos_data = sim.paxos_data.get_mut(&leader_eid).unwrap();
    let current_time = sim.true_timestamp;
    let mut ctx = PaxosContext {
      rand: &mut sim.rand,
      current_time, // TODO: simulate clock skew
      queues: &mut sim.queues,
      nonempty_queues: &mut sim.nonempty_queues,
      this_eid: &leader_eid,
      tasks: &mut paxos_data.tasks,
    };
    paxos_data.paxos_driver.insert_bundle(&mut ctx, SimpleBundle { val: sim.next_int });

    return sim;
  }

  // -----------------------------------------------------------------------------------------------
  //  Simulation Methods
  // -----------------------------------------------------------------------------------------------

  /// Add a message between two nodes in the network.
  pub fn add_msg(&mut self, msg: NetworkMessage, from_eid: &EndpointId, to_eid: &EndpointId) {
    add_msg(&mut self.queues, &mut self.nonempty_queues, msg, from_eid, to_eid);
  }

  /// Poll a message between two nodes in the network.
  pub fn poll_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) -> Option<NetworkMessage> {
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

  /// When this is called, the `msg` will already have been popped from `queues`.
  /// This function will run the PaxosNode at the `to_eid` end, which might have
  /// any number of side effects, including adding new messages into `queues`.
  pub fn run_paxos_message(&mut self, _: &EndpointId, to_eid: &EndpointId, msg: NetworkMessage) {
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
    paxos_data.paxos_log.extend(entries.into_iter());

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

  /// The endpoints provided must exist. This function polls a message from
  /// the message queue between them and delivers it to the PaxosDriver at `to_eid`.
  pub fn deliver_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) {
    if let Some(msg) = self.poll_msg(from_eid, to_eid) {
      self.run_paxos_message(from_eid, to_eid, msg);
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
      let (from_eid, to_eid) = self.nonempty_queues.get(r).unwrap().clone();
      self.deliver_msg(&from_eid, &to_eid);
    }
  }

  pub fn simulate_n_ms(&mut self, n: u32) {
    for _ in 0..n {
      self.simulate1ms();
    }
  }

  /// Returns true iff there is no more work left to be done in the Execution.
  pub fn is_done(&self) -> bool {
    if !self.nonempty_queues.is_empty() {
      return false;
    }

    for (_, paxos_data) in &self.paxos_data {
      if !paxos_data.tasks.is_empty() {
        return false;
      }
    }

    return true;
  }

  pub fn simulate_all(&mut self) {
    while !self.is_done() {
      self.simulate1ms();
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

// Construct the PaxosNode EndpointIds of the paxos at the given index.
pub fn paxos_eid(i: &i32) -> EndpointId {
  EndpointId(format!("e{}", i))
}

/// Add a message between two nodes in the network.
fn add_msg(
  queues: &mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<NetworkMessage>>>,
  nonempty_queues: &mut Vec<(EndpointId, EndpointId)>,
  msg: NetworkMessage,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
) {
  let queue = queues.get_mut(from_eid).unwrap().get_mut(to_eid).unwrap();
  if queue.len() == 0 {
    let queue_id = (from_eid.clone(), to_eid.clone());
    nonempty_queues.push(queue_id);
  }
  queue.push_back(msg);
}
