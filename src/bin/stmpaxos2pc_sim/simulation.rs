use crate::message as msg;
use crate::slave::{SlaveBundle, SlaveContext, SlaveState};
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use runiversal::common::{rvec, BasicIOCtx};
use runiversal::model::common::{EndpointId, Gen, LeadershipId, PaxosGroupId, SlaveGroupId};
use std::collections::{HashMap, VecDeque};

// -------------------------------------------------------------------------------------------------
//  SlaveIOCtx
// -------------------------------------------------------------------------------------------------

/// We avoid depending on SlaveIOCtx directly, opting to depend on ISlaveIOCtx
/// instead to be more consistent with production code.
pub trait ISlaveIOCtx: BasicIOCtx<msg::NetworkMessage> {
  fn insert_bundle(&mut self, bundle: SlaveBundle);
}

pub struct SlaveIOCtx<'a> {
  rand: &'a mut XorShiftRng,
  current_time: u128,
  queues: &'a mut HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_sid: &'a SlaveGroupId,
  this_eid: &'a EndpointId,

  // Paxos
  pending_insert: &'a mut HashMap<EndpointId, Option<msg::PLEntry<SlaveBundle>>>,
  insert_queues: &'a mut HashMap<EndpointId, VecDeque<msg::PLEntry<SlaveBundle>>>,
}

impl<'a> BasicIOCtx<msg::NetworkMessage> for SlaveIOCtx<'a> {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> u128 {
    self.current_time
  }

  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage) {
    add_msg(self.queues, self.nonempty_queues, msg, &self.this_eid, eid);
  }
}

impl<'a> ISlaveIOCtx for SlaveIOCtx<'a> {
  fn insert_bundle(&mut self, bundle: SlaveBundle) {
    if self.insert_queues.get(&self.this_eid).unwrap().is_empty() {
      // Here, the node has received every PLEntry, so we may propose this `bundle`.
      *self.pending_insert.get_mut(&self.this_eid).unwrap() = Some(msg::PLEntry::Bundle(bundle));
    }
  }
}

// -------------------------------------------------------------------------------------------------
//  Simulation
// -------------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct Simulation {
  pub rand: XorShiftRng,

  /// Message queues between all nodes in the network
  queues: HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  /// The set of queues that have messages to deliver
  nonempty_queues: Vec<(EndpointId, EndpointId)>,

  /// SlaveState
  slave_states: HashMap<EndpointId, SlaveState>,
  /// Accumulated client responses for each client
  client_msgs_received: HashMap<EndpointId, Vec<msg::NetworkMessage>>,

  // Paxos Configuration for Master and Slaves
  slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,

  // Global Paxos
  pending_insert: HashMap<EndpointId, Option<msg::PLEntry<SlaveBundle>>>,
  insert_queues: HashMap<EndpointId, VecDeque<msg::PLEntry<SlaveBundle>>>,

  /// Meta
  true_timestamp: u128,
}

impl Simulation {
  /// Here, for every key in `tablet_config`, we create a slave, and
  /// we create as many tablets as there are `TabletGroupId` for that slave.
  pub fn new(
    seed: [u8; 16],
    num_clients: i32,
    slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,
  ) -> Simulation {
    let mut sim = Simulation {
      rand: XorShiftRng::from_seed(seed),
      queues: Default::default(),
      nonempty_queues: Default::default(),
      slave_states: Default::default(),
      client_msgs_received: Default::default(),
      slave_address_config: slave_address_config.clone(),
      pending_insert: Default::default(),
      insert_queues: Default::default(),
      true_timestamp: Default::default(),
    };

    // Setup eids
    let slave_eids: Vec<EndpointId> =
      slave_address_config.values().cloned().into_iter().flatten().collect();
    let client_eids: Vec<EndpointId> = rvec(0, num_clients).iter().map(client_eid).collect();
    let all_eids: Vec<EndpointId> = vec![]
      .into_iter()
      .chain(slave_eids.iter().cloned())
      .chain(client_eids.iter().cloned())
      .collect();

    for from_eid in &all_eids {
      sim.queues.insert(from_eid.clone(), Default::default());
      for to_eid in &all_eids {
        sim.queues.get_mut(from_eid).unwrap().insert(to_eid.clone(), VecDeque::new());
      }
    }

    // Construct LeaderMap
    let mut leader_map = HashMap::<PaxosGroupId, LeadershipId>::new();
    for (sid, eids) in &slave_address_config {
      leader_map.insert(sid.to_gid(), LeadershipId { gen: Gen(0), eid: eids[0].clone() });
    }

    // Construct and add SlaveStates
    for (sid, eids) in &slave_address_config {
      for eid in eids {
        sim.slave_states.insert(
          eid.clone(),
          SlaveState::new(SlaveContext::new(sid.clone(), eid.clone(), leader_map.clone())),
        );
      }
    }

    // External
    for eid in &client_eids {
      sim.client_msgs_received.insert(eid.clone(), Vec::new());
    }

    // Setup Simulated Paxos Insertion
    for eid in slave_eids {
      sim.pending_insert.insert(eid.clone(), None);
      sim.insert_queues.insert(eid.clone(), VecDeque::new());
    }

    // Metadata
    sim.true_timestamp = 0;
    return sim;
  }
}

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

// Construct the Slave id of the slave at the given index.
pub fn slave_eid(i: &i32) -> EndpointId {
  EndpointId(format!("se{}", i))
}

// Construct the Client id of the slave at the given index.
pub fn client_eid(i: &i32) -> EndpointId {
  EndpointId(format!("ce{}", i))
}

/// Add a message between two nodes in the network.
fn add_msg(
  queues: &mut HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &mut Vec<(EndpointId, EndpointId)>,
  msg: msg::NetworkMessage,
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
