use crate::message as msg;
use crate::slave::{FullSlaveInput, SlaveBundle, SlaveContext, SlaveState, SlaveTimerInput};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{mk_t, BasicIOCtx, GeneralTraceMessage, RangeEnds, Timestamp};
use runiversal::model::common::{
  EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, SlaveGroupId,
};
use runiversal::model::message::LeaderChanged;
use runiversal::simulation_utils::{add_msg, mk_client_eid};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// -------------------------------------------------------------------------------------------------
//  SlaveIOCtx
// -------------------------------------------------------------------------------------------------

/// We avoid depending on SlaveIOCtx directly, opting to depend on ISlaveIOCtx
/// instead to be more consistent with production code.
pub trait ISlaveIOCtx: BasicIOCtx<msg::NetworkMessage> {
  fn insert_bundle(&mut self, bundle: SlaveBundle);

  fn defer(&mut self, defer_time: Timestamp, timer_input: SlaveTimerInput);
}

pub struct SlaveIOCtx<'a> {
  rand: &'a mut XorShiftRng,
  current_time: Timestamp,
  queues: &'a mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_sid: &'a SlaveGroupId,
  this_eid: &'a EndpointId,

  // Paxos
  pending_insert: &'a mut BTreeMap<EndpointId, msg::PLEntry<SlaveBundle>>,
  insert_queues: &'a mut BTreeMap<EndpointId, VecDeque<msg::PLEntry<SlaveBundle>>>,

  /// Deferred timer tasks
  tasks: &'a mut BTreeMap<Timestamp, Vec<SlaveTimerInput>>,
}

impl<'a> BasicIOCtx<msg::NetworkMessage> for SlaveIOCtx<'a> {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> Timestamp {
    self.current_time.clone()
  }

  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage) {
    add_msg(self.queues, self.nonempty_queues, msg, &self.this_eid, eid);
  }

  fn general_trace(&mut self, _: GeneralTraceMessage) {
    unimplemented!()
  }
}

impl<'a> ISlaveIOCtx for SlaveIOCtx<'a> {
  fn insert_bundle(&mut self, bundle: SlaveBundle) {
    if !self.insert_queues.contains_key(&self.this_eid) {
      // This means the node has already received every PLEntry, so we may propose this `bundle`.
      self.pending_insert.insert(self.this_eid.clone(), msg::PLEntry::Bundle(bundle));
    }
  }

  fn defer(&mut self, defer_time: Timestamp, timer_input: SlaveTimerInput) {
    let deferred_time = self.now().add(defer_time);
    if let Some(timer_inputs) = self.tasks.get_mut(&deferred_time) {
      timer_inputs.push(timer_input);
    } else {
      self.tasks.insert(deferred_time, vec![timer_input]);
    }
  }
}

// -------------------------------------------------------------------------------------------------
//  Simulation
// -------------------------------------------------------------------------------------------------

/// These are values that control the execution of the simulation that can be varied
/// from the outside during the middle of a simulation.
#[derive(Debug)]
pub struct SimConfig {
  /// The probability (in %) of `PLEntry` delivery.
  pub pl_entry_delivery_prob: u32,
  /// The probability (in %) of Global PL insertion. The remaining % is the probability
  /// that there is a Leadership Change.
  pub global_pl_insertion_prob: u32,
}

#[derive(Debug)]
pub struct SlaveData {
  slave_state: SlaveState,
  tasks: BTreeMap<Timestamp, Vec<SlaveTimerInput>>,
}

#[derive(Debug)]
pub struct Simulation {
  pub rand: XorShiftRng,

  /// Message queues between all nodes in the network
  queues: BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  /// The set of queues that have messages to deliver
  nonempty_queues: Vec<(EndpointId, EndpointId)>,

  /// SlaveState
  slave_data: BTreeMap<EndpointId, SlaveData>,
  /// Accumulated client responses for each client
  client_msgs_received: BTreeMap<EndpointId, Vec<msg::NetworkMessage>>,

  // Paxos Configuration for Master and Slaves
  slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
  slave_address_config_inverse: BTreeMap<EndpointId, SlaveGroupId>,

  // Global Paxos
  /// Maps nodes that are currently proposing a PLEntry to the PLEntry that it is proposing.
  pending_insert: BTreeMap<EndpointId, msg::PLEntry<SlaveBundle>>,
  /// Maps nodes to all PLEntrys that still needs to be delivered.
  /// If empty, there will be no entry in this map.
  insert_queues: BTreeMap<EndpointId, VecDeque<msg::PLEntry<SlaveBundle>>>,
  /// The current set of Leaders according to the Global PaxosLog
  pub leader_map: BTreeMap<SlaveGroupId, LeadershipId>,
  /// Non-`LeaderChanged``PLEntry` inserted into every PaxosGroups Global PL.
  pub global_pls: BTreeMap<SlaveGroupId, Vec<msg::PLEntry<SlaveBundle>>>,

  /// Meta
  true_timestamp: Timestamp,

  // Configurables
  pub sim_params: SimConfig,
}

impl Simulation {
  /// Here, for every key in `tablet_config`, we create a slave, and
  /// we create as many tablets as there are `TabletGroupId` for that slave.
  pub fn new(
    seed: [u8; 16],
    num_clients: u32,
    slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
  ) -> Simulation {
    let mut sim = Simulation {
      rand: XorShiftRng::from_seed(seed),
      queues: Default::default(),
      nonempty_queues: vec![],
      slave_data: Default::default(),
      client_msgs_received: Default::default(),
      slave_address_config: slave_address_config.clone(),
      slave_address_config_inverse: Default::default(),
      pending_insert: Default::default(),
      insert_queues: Default::default(),
      leader_map: Default::default(),
      global_pls: Default::default(),
      true_timestamp: mk_t(0),
      sim_params: SimConfig { pl_entry_delivery_prob: 70, global_pl_insertion_prob: 25 },
    };

    // Setup eids
    let slave_eids: Vec<EndpointId> =
      slave_address_config.values().cloned().into_iter().flatten().collect();
    let client_eids: Vec<EndpointId> =
      RangeEnds::rvec(0, num_clients).iter().map(|i| mk_client_eid(*i)).collect();
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
    let mut leader_map = BTreeMap::<PaxosGroupId, LeadershipId>::new();
    for (sid, eids) in &slave_address_config {
      leader_map.insert(sid.to_gid(), LeadershipId { gen: Gen(0), eid: eids[0].clone() });
    }

    // Construct and add SlaveStates
    for (sid, eids) in &slave_address_config {
      for eid in eids {
        sim.slave_data.insert(
          eid.clone(),
          SlaveData {
            slave_state: SlaveState::new(SlaveContext::new(
              sid.clone(),
              eid.clone(),
              slave_address_config.clone(),
              leader_map.clone(),
            )),
            tasks: Default::default(),
          },
        );
      }
    }

    // External
    for eid in &client_eids {
      sim.client_msgs_received.insert(eid.clone(), Vec::new());
    }

    // Invert the slave_address_config
    for (sid, eids) in slave_address_config.clone() {
      for eid in eids {
        sim.slave_address_config_inverse.insert(eid, sid.clone());
      }
    }

    // LeaderMap and Paxos
    for (sid, eids) in slave_address_config {
      sim.leader_map.insert(sid.clone(), LeadershipId { gen: Gen(0), eid: eids[0].clone() });
      sim.global_pls.insert(sid, vec![]);
      for eid in eids {
        let mut queue = VecDeque::<msg::PLEntry<SlaveBundle>>::new();
        queue.push_back(msg::PLEntry::Bundle(SlaveBundle::default()));
        sim.insert_queues.insert(eid, queue);
      }
    }

    // Initialize the Slaves
    for (this_eid, slave_data) in &mut sim.slave_data {
      let mut io_ctx = SlaveIOCtx {
        rand: &mut sim.rand,
        current_time: sim.true_timestamp.clone(), // TODO: simulate clock skew
        queues: &mut sim.queues,
        nonempty_queues: &mut sim.nonempty_queues,
        this_sid: &slave_data.slave_state.context.this_sid.clone(),
        this_eid,
        pending_insert: &mut sim.pending_insert,
        insert_queues: &mut sim.insert_queues,
        tasks: &mut slave_data.tasks,
      };

      slave_data.slave_state.initialize(&mut io_ctx);
    }

    // Metadata
    return sim;
  }

  // -----------------------------------------------------------------------------------------------
  //  Simulation Methods
  // -----------------------------------------------------------------------------------------------

  /// Add a message between two nodes in the network.
  pub fn add_msg(&mut self, msg: msg::NetworkMessage, from_eid: &EndpointId, to_eid: &EndpointId) {
    add_msg(&mut self.queues, &mut self.nonempty_queues, msg, from_eid, to_eid);
  }

  /// Poll a message between two nodes in the network.
  pub fn poll_msg(
    &mut self,
    from_eid: &EndpointId,
    to_eid: &EndpointId,
  ) -> Option<msg::NetworkMessage> {
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
  /// This function will run the Slave on the `to_eid` end, which might have any
  /// number of side effects, including adding new messages into `queues`.
  pub fn deliver_slave_input(&mut self, to_eid: &EndpointId, input: FullSlaveInput) {
    let slave_data = self.slave_data.get_mut(&to_eid).unwrap();

    let current_time = self.true_timestamp.clone();
    let mut io_ctx = SlaveIOCtx {
      rand: &mut self.rand,
      current_time: current_time.clone(), // TODO: simulate clock skew
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_sid: &slave_data.slave_state.context.this_sid.clone(),
      this_eid: to_eid,
      pending_insert: &mut self.pending_insert,
      insert_queues: &mut self.insert_queues,
      tasks: &mut slave_data.tasks,
    };

    // Deliver the input message to the Slave.
    slave_data.slave_state.handle_full_input(&mut io_ctx, input);

    // Send all SlaveTimerInputs.
    loop {
      if let Some((next_timestamp, _)) = io_ctx.tasks.first_key_value() {
        if next_timestamp <= &current_time {
          // All data in this first entry should be dispatched.
          let next_timestamp = next_timestamp.clone();
          for timer_input in io_ctx.tasks.remove(&next_timestamp).unwrap() {
            slave_data
              .slave_state
              .handle_full_input(&mut io_ctx, FullSlaveInput::SlaveTimerInput(timer_input));
          }
          continue;
        }
      }

      // This means there are no more left.
      break;
    }
  }

  /// The endpoints provided must exist. This function polls a message from
  /// the message queue between them and delivers to the `to_eid`. If that's
  /// a client, the message is added to `client_msgs_received`, and if it's
  /// a slave, the slave processes the message.
  pub fn deliver_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) {
    if let Some(msg) = self.poll_msg(from_eid, to_eid) {
      if self.slave_data.contains_key(to_eid) {
        let msg::NetworkMessage::Slave(slave_msg) = msg;
        self.deliver_slave_input(to_eid, FullSlaveInput::SlaveMessage(slave_msg));
      } else if let Some(msgs) = self.client_msgs_received.get_mut(to_eid) {
        msgs.push(msg);
      } else {
        panic!("Endpoint {:?} does not exist", to_eid);
      }
    }
  }

  /// Adds the `pl_entry` to the `insert_queues` of all nodes in the PaxosGroup `sid`, and also
  /// clears the `pending_insert` for nodes.
  fn global_pl_insert(&mut self, sid: &SlaveGroupId, pl_entry: msg::PLEntry<SlaveBundle>) {
    self.global_pls.get_mut(sid).unwrap().push(pl_entry.clone());
    if let msg::PLEntry::LeaderChanged(leader_changed) = &pl_entry {
      self.leader_map.insert(sid.clone(), leader_changed.lid.clone());
    }
    for eid in self.slave_address_config.get(sid).unwrap() {
      self.pending_insert.remove(eid);
      if !self.insert_queues.contains_key(eid) {
        // Re-insert an insert_queue here
        self.insert_queues.insert(eid.clone(), VecDeque::new());
      }
      self.insert_queues.get_mut(eid).unwrap().push_back(pl_entry.clone());
    }
  }

  /// Here, we lookup Deliver a PLEntry
  pub fn deliver_pl_entry(&mut self) {
    // 2. Choose a message to queue up all insert queues.
    // 3. Randomly queue up a Leadership change.
    let rnd = self.rand.next_u32() % 100;
    if rnd < self.sim_params.pl_entry_delivery_prob {
      // Here, we simply Deliver a PLEntry to a node with a 70% chance.
      if !self.insert_queues.is_empty() {
        // Pick a eid that has a non-empty insert_queue
        let i = self.rand.next_u32() % self.insert_queues.len() as u32;
        let eids: Vec<EndpointId> = self.insert_queues.keys().cloned().collect();
        let eid = &eids[i as usize];

        // Poll a PLEntry from it
        let queue = self.insert_queues.get_mut(&eid).unwrap();
        let pl_entry = queue.pop_front().unwrap();
        if queue.is_empty() {
          self.insert_queues.remove(&eid);
        }

        // Deliver the PLEntry
        self.deliver_slave_input(&eid, FullSlaveInput::PaxosMessage(pl_entry));
      }
    } else if rnd
      < self.sim_params.pl_entry_delivery_prob + self.sim_params.global_pl_insertion_prob
    {
      // Here, we choose the next PLEntry in the GlobalPaxosLog of a PaxosGroup with 15% chance.
      if !self.pending_insert.is_empty() {
        // Pick a eid that has a non-empty pending_insert
        let i = self.rand.next_u32() % self.pending_insert.len() as u32;
        let eids: Vec<EndpointId> = self.pending_insert.keys().cloned().collect();
        let eid = &eids[i as usize];
        let pl_entry = self.pending_insert.get(eid).unwrap();

        // Add the entry to even EndpointId in the PaxosGroup
        let sid = self.slave_address_config_inverse.get(eid).unwrap();
        self.global_pl_insert(&sid.clone(), pl_entry.clone());
      }
    } else {
      // Here, we randomly change the Leadership of a PaxosGroup with a 5% chance.

      // Pick a random EndpointId to make a leader of
      let mut eids: BTreeSet<EndpointId> =
        self.slave_address_config_inverse.keys().cloned().collect();
      for (_, lid) in &self.leader_map {
        // Remove existing leaders
        eids.remove(&lid.eid);
      }
      let eids: Vec<EndpointId> = eids.into_iter().collect();
      let i = self.rand.next_u32() % eids.len() as u32;
      let eid = &eids[i as usize];

      // Add a LeaderChanged entry to the EndpointId
      let sid = self.slave_address_config_inverse.get(eid).unwrap();
      let old_lid = self.leader_map.get(sid).unwrap();
      let new_lid = LeadershipId { gen: old_lid.gen.next(), eid: eid.clone() };
      self.global_pl_insert(
        &sid.clone(),
        msg::PLEntry::LeaderChanged(LeaderChanged { lid: new_lid }),
      );
    }
  }

  /// This function simply increments the `true_time` by 1ms and delivers 1ms worth of
  /// messages. For simplicity, we assume that this means that every non-empty queue
  /// of messages delivers about one message in this time.
  pub fn simulate1ms(&mut self) {
    self.true_timestamp = self.true_timestamp.add(mk_t(1));
    let num_msgs_to_deliver = self.nonempty_queues.len();
    for _ in 0..num_msgs_to_deliver {
      let r = self.rand.next_u32() as usize % self.nonempty_queues.len();
      let (from_eid, to_eid) = self.nonempty_queues.get(r).unwrap().clone();
      self.deliver_msg(&from_eid, &to_eid);
    }

    // We also do one Paxos-related action every ms
    self.deliver_pl_entry();
  }

  pub fn simulate_n_ms(&mut self, n: u32) {
    for _ in 0..n {
      self.simulate1ms();
    }
  }
}
