use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{mk_cid, rvec, CoreIOCtx, GossipData, MasterIOCtx, SlaveIOCtx};
use runiversal::coord::{CoordContext, CoordForwardMsg, CoordState};
use runiversal::master::{FullMasterInput, MasterContext, MasterState, MasterTimerInput};
use runiversal::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, RequestId, SlaveGroupId, TablePath,
  TabletGroupId, TabletKeyRange, Timestamp,
};
use runiversal::model::message as msg;
use runiversal::model::message::NetworkMessage;
use runiversal::multiversion_map::MVM;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::tablet::{TabletContext, TabletCreateHelper, TabletForwardMsg, TabletState};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  TestSlaveIOCtx
// -----------------------------------------------------------------------------------------------

/// This `SlaveIOCtx` right before a `handle_input` call to a Slave. The `current_time`
/// For this Round of Execution is static. Thus, it does not advance when Executing the Tablet
/// messages,
pub struct TestSlaveIOCtx<'a> {
  // Basic
  rand: &'a mut XorShiftRng,
  current_time: u128,
  queues: &'a mut HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_sid: &'a SlaveGroupId,
  this_eid: &'a EndpointId,

  /// Tablets for this Slave
  tablet_states: &'a mut HashMap<TabletGroupId, TabletState>,
  /// We hold the `SlaveBackMessages` generated by the Tablets, since this SlaveIOCtx
  /// executes Tablet message receival as well.
  slave_back_messages: &'a mut VecDeque<SlaveBackMessage>,

  /// Coords for this Slave
  coord_states: &'a mut HashMap<CoordGroupId, CoordState>,

  /// Deferred timer tasks
  tasks: &'a mut BTreeMap<Timestamp, Vec<SlaveTimerInput>>,
}

impl<'a> SlaveIOCtx for TestSlaveIOCtx<'a> {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> u128 {
    self.current_time
  }

  fn send(&mut self, eid: &EndpointId, msg: NetworkMessage) {
    add_msg(self.queues, self.nonempty_queues, msg, &self.this_eid, eid);
  }

  fn create_tablet(&mut self, helper: TabletCreateHelper) {
    let tid = helper.this_tablet_group_id.clone();
    self.tablet_states.insert(tid, TabletState::new(TabletContext::new(helper)));
  }

  fn tablet_forward(&mut self, tid: &TabletGroupId, forward_msg: TabletForwardMsg) {
    let tablet = self.tablet_states.get_mut(tid).unwrap();
    let mut io_ctx = TestCoreIOCtx {
      rand: &mut self.rand,
      current_time: self.current_time,
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_eid: &self.this_eid,
      slave_back_messages: &mut Default::default(),
    };
    tablet.handle_input(&mut io_ctx, forward_msg);
  }

  fn all_tids(&self) -> Vec<TabletGroupId> {
    self.tablet_states.keys().cloned().collect()
  }

  fn num_tablets(&self) -> usize {
    self.tablet_states.len()
  }

  fn coord_forward(&mut self, cid: &CoordGroupId, forward_msg: CoordForwardMsg) {
    let coord = self.coord_states.get_mut(cid).unwrap();
    let mut io_ctx = TestCoreIOCtx {
      rand: &mut self.rand,
      current_time: self.current_time,
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_eid: &self.this_eid,
      slave_back_messages: &mut Default::default(),
    };
    coord.handle_input(&mut io_ctx, forward_msg);
  }

  fn all_cids(&self) -> Vec<CoordGroupId> {
    self.coord_states.keys().cloned().collect()
  }

  fn defer(&mut self, defer_time: u128, timer_input: SlaveTimerInput) {
    let deferred_time = self.current_time + defer_time;
    if let Some(timer_inputs) = self.tasks.get_mut(&deferred_time) {
      timer_inputs.push(timer_input);
    } else {
      self.tasks.insert(deferred_time, vec![timer_input]);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  TestCoreIOCtx
// -----------------------------------------------------------------------------------------------

pub struct TestCoreIOCtx<'a> {
  // Basic
  rand: &'a mut XorShiftRng,
  current_time: u128,
  queues: &'a mut HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_eid: &'a EndpointId,

  // Tablet
  slave_back_messages: &'a mut VecDeque<SlaveBackMessage>,
}

impl<'a> CoreIOCtx for TestCoreIOCtx<'a> {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> u128 {
    self.current_time
  }

  fn send(&mut self, eid: &EndpointId, msg: NetworkMessage) {
    add_msg(self.queues, self.nonempty_queues, msg, &self.this_eid, eid);
  }

  fn slave_forward(&mut self, msg: SlaveBackMessage) {
    self.slave_back_messages.push_back(msg);
  }
}

// -----------------------------------------------------------------------------------------------
//  TestMasterIOCtx
// -----------------------------------------------------------------------------------------------

pub struct TestMasterIOCtx<'a> {
  // Basic
  rand: &'a mut XorShiftRng,
  current_time: u128,
  queues: &'a mut HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_eid: &'a EndpointId,

  /// Deferred timer tasks
  tasks: &'a mut BTreeMap<Timestamp, Vec<MasterTimerInput>>,
}

impl<'a> MasterIOCtx for TestMasterIOCtx<'a> {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> u128 {
    self.current_time
  }

  fn send(&mut self, eid: &EndpointId, msg: NetworkMessage) {
    add_msg(self.queues, self.nonempty_queues, msg, &self.this_eid, eid);
  }

  fn defer(&mut self, defer_time: u128, timer_input: MasterTimerInput) {
    let deferred_time = self.current_time + defer_time;
    if let Some(timer_inputs) = self.tasks.get_mut(&deferred_time) {
      timer_inputs.push(timer_input);
    } else {
      self.tasks.insert(deferred_time, vec![timer_input]);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Simulation
// -----------------------------------------------------------------------------------------------

const NUM_COORDS: u32 = 3;

#[derive(Debug)]
pub struct MasterData {
  master_state: MasterState,
  tasks: BTreeMap<Timestamp, Vec<MasterTimerInput>>,
}

#[derive(Debug)]
pub struct SlaveData {
  slave_state: SlaveState,
  tablet_states: HashMap<TabletGroupId, TabletState>,
  coord_states: HashMap<CoordGroupId, CoordState>,
  tasks: BTreeMap<Timestamp, Vec<SlaveTimerInput>>,
}

#[derive(Debug)]
pub struct Simulation {
  pub rand: XorShiftRng,

  /// Message queues between all nodes in the network
  queues: HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  /// The set of queues that have messages to deliever
  nonempty_queues: Vec<(EndpointId, EndpointId)>,

  /// MasterData
  master_data: HashMap<EndpointId, MasterData>,
  /// SlaveData
  slave_data: HashMap<EndpointId, SlaveData>,
  /// Accumulated client responses for each client
  client_msgs_received: HashMap<EndpointId, Vec<msg::NetworkMessage>>,

  // Paxos Configuration for Master and Slaves
  slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,
  master_address_config: Vec<EndpointId>,

  /// Meta
  next_int: i32,
  true_timestamp: u128,
}

impl Simulation {
  /// Here, for every key in `tablet_config`, we create a slave, and
  /// we create as many tablets as there are `TabletGroupId` for that slave.
  pub fn new(
    seed: [u8; 16],
    num_clients: i32,
    slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,
    master_address_config: Vec<EndpointId>,
  ) -> Simulation {
    let mut sim = Simulation {
      rand: XorShiftRng::from_seed(seed),
      queues: Default::default(),
      nonempty_queues: Default::default(),
      master_data: Default::default(),
      slave_data: Default::default(),
      slave_address_config: slave_address_config.clone(),
      master_address_config: master_address_config.clone(),
      next_int: Default::default(),
      true_timestamp: Default::default(),
      client_msgs_received: Default::default(),
    };

    // Setup eids
    let slave_eids: Vec<EndpointId> =
      slave_address_config.values().cloned().into_iter().flatten().collect();
    let client_eids: Vec<EndpointId> = rvec(0, num_clients).iter().map(client_eid).collect();
    let all_eids: Vec<EndpointId> = vec![]
      .into_iter()
      .chain(slave_eids.iter().cloned())
      .chain(master_address_config.iter().cloned())
      .chain(client_eids.iter().cloned())
      .collect();

    for from_eid in &all_eids {
      sim.queues.insert(from_eid.clone(), Default::default());
      for to_eid in &all_eids {
        sim.queues.get_mut(from_eid).unwrap().insert(to_eid.clone(), VecDeque::new());
      }
    }

    // Gossip
    let gossip = GossipData {
      gen: Gen(0),
      db_schema: Default::default(),
      table_generation: MVM::new(),
      sharding_config: Default::default(),
      tablet_address_config: Default::default(),
      slave_address_config: slave_address_config.clone(),
      master_address_config: master_address_config.clone(),
    };

    // Construct LeaderMap
    let mut leader_map = HashMap::<PaxosGroupId, LeadershipId>::new();
    leader_map.insert(
      PaxosGroupId::Master,
      LeadershipId { gen: Gen(0), eid: master_address_config[0].clone() },
    );
    for (sid, eids) in &slave_address_config {
      leader_map.insert(sid.to_gid(), LeadershipId { gen: Gen(0), eid: eids[0].clone() });
    }

    // Construct MasterState
    for eid in master_address_config.clone() {
      let master_state = MasterState::new(MasterContext::new(
        eid.clone(),
        slave_address_config.clone(),
        master_address_config.clone(),
        leader_map.clone(),
      ));
      sim.master_data.insert(eid.clone(), MasterData { master_state, tasks: Default::default() });
    }

    // Construct SlaveState
    for (sid, eids) in &slave_address_config {
      for eid in eids {
        let mut coord_states = HashMap::<CoordGroupId, CoordState>::new();
        for _ in 0..NUM_COORDS {
          let cid = mk_cid(&mut sim.rand);

          // Create the Coord
          let coord_state = CoordState::new(CoordContext::new(
            sid.clone(),
            cid.clone(),
            eid.clone(),
            Arc::new(gossip.clone()),
            leader_map.clone(),
          ));

          coord_states.insert(cid, coord_state);
        }

        // Create Slave
        let slave_state = SlaveState::new(SlaveContext::new(
          coord_states.keys().cloned().collect(),
          sid.clone(),
          eid.clone(),
          Arc::new(gossip.clone()),
          leader_map.clone(),
        ));
        sim.slave_data.insert(
          eid.clone(),
          SlaveData {
            slave_state,
            tablet_states: Default::default(),
            coord_states,
            tasks: Default::default(),
          },
        );
      }
    }

    // External
    for eid in &client_eids {
      sim.client_msgs_received.insert(eid.clone(), Vec::new());
    }

    // Metadata
    sim.next_int = 0;
    sim.true_timestamp = 0;
    return sim;
  }

  // -----------------------------------------------------------------------------------------------
  //  Const getters
  // -----------------------------------------------------------------------------------------------

  pub fn get_responses(&self) -> &HashMap<EndpointId, Vec<msg::NetworkMessage>> {
    return &self.client_msgs_received;
  }

  // -----------------------------------------------------------------------------------------------
  //  Reflection
  // -----------------------------------------------------------------------------------------------

  pub fn print(&self) {
    println!("{:#?}", self);
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

  /// When this is called, the `msg` will already have been popped from
  /// `queues`. This function will run the Master on the `to_eid` end, which
  /// might have any number of side effects, including adding new messages into
  /// `queues`.
  pub fn run_master_message(
    &mut self,
    _: &EndpointId,
    to_eid: &EndpointId,
    msg: msg::MasterMessage,
  ) {
    let master_data = self.master_data.get_mut(&to_eid).unwrap();

    let current_time = self.true_timestamp;
    let mut io_ctx = TestMasterIOCtx {
      rand: &mut self.rand,
      current_time, // TODO: simulate clock skew
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_eid: to_eid,
      tasks: &mut master_data.tasks,
    };

    // Deliver the network message to the Master.
    master_data.master_state.handle_input(&mut io_ctx, FullMasterInput::MasterMessage(msg));

    // Send all MasterBackMessages and MasterTimerInputs.
    loop {
      if let Some((next_timestamp, _)) = io_ctx.tasks.first_key_value() {
        if next_timestamp <= &current_time {
          // All data in this first entry should be dispatched.
          let next_timestamp = next_timestamp.clone();
          for timer_input in io_ctx.tasks.remove(&next_timestamp).unwrap() {
            master_data
              .master_state
              .handle_input(&mut io_ctx, FullMasterInput::MasterTimerInput(timer_input));
          }
          continue;
        }
      }

      // This means there are no more left.
      break;
    }
  }

  /// Same as above, except for a Slave.
  pub fn run_slave_message(&mut self, _: &EndpointId, to_eid: &EndpointId, msg: msg::SlaveMessage) {
    let slave_data = self.slave_data.get_mut(&to_eid).unwrap();

    let current_time = self.true_timestamp;
    let mut slave_back_messages = VecDeque::<SlaveBackMessage>::new();
    let mut io_ctx = TestSlaveIOCtx {
      rand: &mut self.rand,
      current_time, // TODO: simulate clock skew
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_sid: &slave_data.slave_state.context.this_sid.clone(),
      this_eid: to_eid,
      tablet_states: &mut slave_data.tablet_states,
      slave_back_messages: &mut slave_back_messages,
      coord_states: &mut slave_data.coord_states,
      tasks: &mut slave_data.tasks,
    };

    // Deliver the network message to the Slave.
    slave_data.slave_state.handle_input(&mut io_ctx, FullSlaveInput::SlaveMessage(msg));

    // Send all SlaveBackMessages and SlaveTimerInputs.
    loop {
      if let Some(back_msg) = io_ctx.slave_back_messages.pop_front() {
        slave_data
          .slave_state
          .handle_input(&mut io_ctx, FullSlaveInput::SlaveBackMessage(back_msg));
        continue;
      }

      if let Some((next_timestamp, _)) = io_ctx.tasks.first_key_value() {
        if next_timestamp <= &current_time {
          // All data in this first entry should be dispatched.
          let next_timestamp = next_timestamp.clone();
          for timer_input in io_ctx.tasks.remove(&next_timestamp).unwrap() {
            slave_data
              .slave_state
              .handle_input(&mut io_ctx, FullSlaveInput::SlaveTimerInput(timer_input));
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
      if self.master_data.contains_key(to_eid) {
        if let msg::NetworkMessage::Master(master_msg) = msg {
          self.run_master_message(from_eid, to_eid, master_msg)
        } else {
          panic!("Endpoint {:?} is a Master but received a non-MasterMessage {:?} ", to_eid, msg)
        }
      } else if self.slave_data.contains_key(to_eid) {
        if let msg::NetworkMessage::Slave(slave_msg) = msg {
          self.run_slave_message(from_eid, to_eid, slave_msg)
        } else {
          panic!("Endpoint {:?} is a Slave but received a non-SlaveMessage {:?} ", to_eid, msg)
        }
      } else if let Some(msgs) = self.client_msgs_received.get_mut(to_eid) {
        msgs.push(msg);
      } else {
        panic!("Endpoint {:?} does not exist", to_eid);
      }
    }
  }

  /// Drops messages until `num_msgs` have been dropped, or until
  /// there are no more messages to drop.
  pub fn drop_messages(&mut self, num_msgs: i32) {
    for _ in 0..num_msgs {
      if self.nonempty_queues.len() > 0 {
        let r = self.rand.next_u32() as usize % self.nonempty_queues.len();
        let (from_eid, to_eid) = self.nonempty_queues.get(r).unwrap().clone();
        self.poll_msg(&from_eid, &to_eid);
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
      let (from_eid, to_eid) = self.nonempty_queues.get(r).unwrap().clone();
      self.deliver_msg(&from_eid, &to_eid);
    }
  }

  pub fn simulate_n_ms(&mut self, n: i32) {
    for _ in 0..n {
      self.simulate1ms();
    }
  }

  /// Returns true iff there is no more work left to be done in the Execution.
  pub fn is_done(&self) -> bool {
    if !self.nonempty_queues.is_empty() {
      return false;
    }

    for (_, slave_data) in &self.slave_data {
      if !slave_data.tasks.is_empty() {
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

  pub fn mk_request_id(&mut self) -> RequestId {
    let request_id = RequestId(self.next_int.to_string());
    self.next_int += 1;
    return request_id;
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
