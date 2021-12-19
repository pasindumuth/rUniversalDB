use crate::stats::Stats;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  mk_cid, BasicIOCtx, CoreIOCtx, GossipData, MasterIOCtx, MasterTraceMessage, RangeEnds,
  SlaveIOCtx, SlaveTraceMessage,
};
use runiversal::coord::coord_test::{assert_coord_consistency, check_coord_clean};
use runiversal::coord::{CoordContext, CoordForwardMsg, CoordState};
use runiversal::master::master_test::check_master_clean;
use runiversal::master::{
  FullDBSchema, FullMasterInput, MasterContext, MasterState, MasterTimerInput,
};
use runiversal::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, RequestId,
  SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
};
use runiversal::model::message as msg;
use runiversal::multiversion_map::MVM;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::{add_msg, mk_client_eid};
use runiversal::slave::slave_test::check_slave_clean;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::tablet::tablet_test::{assert_tablet_consistency, check_tablet_clean};
use runiversal::tablet::{TabletContext, TabletCreateHelper, TabletForwardMsg, TabletState};
use runiversal::test_utils::CheckCtx;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Debug, Formatter};
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
  queues: &'a mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_sid: &'a SlaveGroupId,
  this_eid: &'a EndpointId,

  /// Tablets for this Slave
  tablet_states: &'a mut BTreeMap<TabletGroupId, TabletState>,
  /// We hold the `SlaveBackMessages` generated by the Tablets, since this SlaveIOCtx
  /// executes Tablet message receival as well.
  slave_back_messages: &'a mut VecDeque<SlaveBackMessage>,

  /// Coords for this Slave
  coord_states: &'a mut BTreeMap<CoordGroupId, CoordState>,

  /// Deferred timer tasks
  tasks: &'a mut BTreeMap<Timestamp, Vec<SlaveTimerInput>>,

  /// Collection of trace messages
  trace_msgs: &'a mut VecDeque<SlaveTraceMessage>,
}

impl<'a> BasicIOCtx for TestSlaveIOCtx<'a> {
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

impl<'a> SlaveIOCtx for TestSlaveIOCtx<'a> {
  fn create_tablet(&mut self, helper: TabletCreateHelper) {
    let tid = helper.this_tid.clone();
    self.tablet_states.insert(tid, TabletState::new(TabletContext::new(helper)));
  }

  fn tablet_forward(&mut self, tid: &TabletGroupId, forward_msg: TabletForwardMsg) {
    let tablet = self.tablet_states.get_mut(tid).unwrap();
    let mut io_ctx = TestCoreIOCtx {
      rand: self.rand,
      current_time: self.current_time,
      queues: self.queues,
      nonempty_queues: self.nonempty_queues,
      this_eid: self.this_eid,
      slave_back_messages: self.slave_back_messages,
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
      rand: self.rand,
      current_time: self.current_time,
      queues: self.queues,
      nonempty_queues: self.nonempty_queues,
      this_eid: self.this_eid,
      slave_back_messages: self.slave_back_messages,
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

  fn trace(&mut self, trace_msg: SlaveTraceMessage) {
    self.trace_msgs.push_back(trace_msg);
  }
}

// -----------------------------------------------------------------------------------------------
//  TestCoreIOCtx
// -----------------------------------------------------------------------------------------------

pub struct TestCoreIOCtx<'a> {
  // Basic
  rand: &'a mut XorShiftRng,
  current_time: u128,
  queues: &'a mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_eid: &'a EndpointId,

  // Tablet
  slave_back_messages: &'a mut VecDeque<SlaveBackMessage>,
}

impl<'a> BasicIOCtx for TestCoreIOCtx<'a> {
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

impl<'a> CoreIOCtx for TestCoreIOCtx<'a> {
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
  queues: &'a mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_eid: &'a EndpointId,

  /// Deferred timer tasks
  tasks: &'a mut BTreeMap<Timestamp, Vec<MasterTimerInput>>,

  /// Collection of trace messages
  trace_msgs: &'a mut VecDeque<MasterTraceMessage>,
}

impl<'a> BasicIOCtx for TestMasterIOCtx<'a> {
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

impl<'a> MasterIOCtx for TestMasterIOCtx<'a> {
  fn defer(&mut self, defer_time: u128, timer_input: MasterTimerInput) {
    let deferred_time = self.current_time + defer_time;
    if let Some(timer_inputs) = self.tasks.get_mut(&deferred_time) {
      timer_inputs.push(timer_input);
    } else {
      self.tasks.insert(deferred_time, vec![timer_input]);
    }
  }

  fn trace(&mut self, trace_msg: MasterTraceMessage) {
    self.trace_msgs.push_back(trace_msg);
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

pub struct SlaveData {
  slave_state: SlaveState,
  tablet_states: BTreeMap<TabletGroupId, TabletState>,
  coord_states: BTreeMap<CoordGroupId, CoordState>,
  tasks: BTreeMap<Timestamp, Vec<SlaveTimerInput>>,
  slave_back_messages: VecDeque<SlaveBackMessage>,
}

impl Debug for SlaveData {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut debug_trait_builder = f.debug_struct("SlaveData");
    let _ = debug_trait_builder.field("slave_state", &self.slave_state);
    let _ = debug_trait_builder.field("tablet_states", &self.tablet_states);
    let _ = debug_trait_builder.field("coord_states", &self.coord_states);
    debug_trait_builder.finish()
  }
}

#[derive(Debug)]
pub struct Simulation {
  pub rand: XorShiftRng,

  /// Message queues between all nodes in the network
  queues: BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  /// The set of queues that have messages to deliever
  nonempty_queues: Vec<(EndpointId, EndpointId)>,

  /// MasterData
  master_data: BTreeMap<EndpointId, MasterData>,
  /// SlaveData
  slave_data: BTreeMap<EndpointId, SlaveData>,
  /// Accumulated client responses for each client
  client_msgs_received: BTreeMap<EndpointId, Vec<msg::NetworkMessage>>,

  // Paxos Configuration for Master and Slaves
  slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
  master_address_config: Vec<EndpointId>,

  /// Meta
  next_int: i32,
  true_timestamp: u128,
  stats: Stats,

  /// Inferred LeaderMap. This will evolve continuously (there will be no jumps in
  /// LeadershipId; the `gen` will increases one by one).
  pub leader_map: BTreeMap<PaxosGroupId, LeadershipId>,
  /// If this is set, the `IsLeader` messages disseminated from this Leadership will be blocked.
  blocked_leadership: Option<(PaxosGroupId, LeadershipId)>,
}

impl Simulation {
  /// Here, for every key in `tablet_config`, we create a slave, and
  /// we create as many tablets as there are `TabletGroupId` for that slave.
  pub fn new(
    seed: [u8; 16],
    num_clients: i32,
    slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
    master_address_config: Vec<EndpointId>,
    paxos_config: PaxosConfig,
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
      stats: Stats::default(),
      leader_map: Default::default(),
      blocked_leadership: None,
    };

    // Setup eids
    let slave_eids: Vec<EndpointId> =
      slave_address_config.values().cloned().into_iter().flatten().collect();
    let client_eids: Vec<EndpointId> =
      RangeEnds::rvec(0, num_clients as u32).iter().map(|i| mk_client_eid(*i)).collect();
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
    sim.leader_map.insert(
      PaxosGroupId::Master,
      LeadershipId { gen: Gen(0), eid: master_address_config[0].clone() },
    );
    for (sid, eids) in &slave_address_config {
      sim.leader_map.insert(sid.to_gid(), LeadershipId { gen: Gen(0), eid: eids[0].clone() });
    }

    // Construct MasterState
    for eid in master_address_config.clone() {
      let master_state = MasterState::new(MasterContext::new(
        eid.clone(),
        slave_address_config.clone(),
        master_address_config.clone(),
        sim.leader_map.clone(),
        paxos_config.clone(),
      ));
      sim.master_data.insert(eid.clone(), MasterData { master_state, tasks: Default::default() });
    }

    // Construct SlaveState
    for (sid, eids) in &slave_address_config {
      // Every Slave in a PaxosGroup should use the same CoordGroupIds, since Paxos2PC needs it.
      let mut coord_ids = Vec::<CoordGroupId>::new();
      for _ in 0..NUM_COORDS {
        coord_ids.push(mk_cid(&mut sim.rand));
      }
      for eid in eids {
        let mut coord_states = BTreeMap::<CoordGroupId, CoordState>::new();
        for cid in &coord_ids {
          // Create the Coord
          let coord_state = CoordState::new(CoordContext::new(
            sid.clone(),
            cid.clone(),
            eid.clone(),
            Arc::new(gossip.clone()),
            sim.leader_map.clone(),
          ));

          coord_states.insert(cid.clone(), coord_state);
        }

        // Create Slave
        let slave_state = SlaveState::new(SlaveContext::new(
          coord_ids.clone(),
          sid.clone(),
          eid.clone(),
          Arc::new(gossip.clone()),
          sim.leader_map.clone(),
          paxos_config.clone(),
        ));
        sim.slave_data.insert(
          eid.clone(),
          SlaveData {
            slave_state,
            tablet_states: Default::default(),
            coord_states,
            tasks: Default::default(),
            slave_back_messages: Default::default(),
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

    // Bootstrap the nodes
    for (_, slave_data) in &mut sim.slave_data {
      let current_time = sim.true_timestamp;
      let this_eid = slave_data.slave_state.ctx.this_eid.clone();
      let mut trace_msgs = VecDeque::<SlaveTraceMessage>::new();
      let mut io_ctx = TestSlaveIOCtx {
        rand: &mut sim.rand,
        current_time, // TODO: simulate clock skew
        queues: &mut sim.queues,
        nonempty_queues: &mut sim.nonempty_queues,
        this_sid: &slave_data.slave_state.ctx.this_sid.clone(),
        this_eid: &this_eid,
        tablet_states: &mut slave_data.tablet_states,
        slave_back_messages: &mut slave_data.slave_back_messages,
        coord_states: &mut slave_data.coord_states,
        tasks: &mut slave_data.tasks,
        trace_msgs: &mut trace_msgs,
      };

      slave_data.slave_state.bootstrap(&mut io_ctx);
      assert!(trace_msgs.is_empty());
    }

    let mut trace_msgs = VecDeque::<MasterTraceMessage>::new();
    for (_, master_data) in &mut sim.master_data {
      let current_time = sim.true_timestamp;
      let this_eid = master_data.master_state.ctx.this_eid.clone();
      let mut io_ctx = TestMasterIOCtx {
        rand: &mut sim.rand,
        current_time, // TODO: simulate clock skew
        queues: &mut sim.queues,
        nonempty_queues: &mut sim.nonempty_queues,
        this_eid: &this_eid,
        tasks: &mut master_data.tasks,
        trace_msgs: &mut trace_msgs,
      };

      master_data.master_state.bootstrap(&mut io_ctx);
      assert!(trace_msgs.is_empty());
    }

    return sim;
  }

  // -----------------------------------------------------------------------------------------------
  //  Const getters
  // -----------------------------------------------------------------------------------------------

  pub fn get_all_responses(&self) -> &BTreeMap<EndpointId, Vec<msg::NetworkMessage>> {
    return &self.client_msgs_received;
  }

  /// Move out and return all responses from `client_msgs_received`, starting it again from empty.
  pub fn remove_all_responses(&mut self) -> BTreeMap<EndpointId, Vec<msg::NetworkMessage>> {
    let mut empty_client_msgs_received = BTreeMap::new();
    for (eid, _) in &self.client_msgs_received {
      empty_client_msgs_received.insert(eid.clone(), Vec::new());
    }
    std::mem::replace(&mut self.client_msgs_received, empty_client_msgs_received)
  }

  /// Returns all responses at `eid`.
  pub fn get_responses(&self, eid: &EndpointId) -> &Vec<msg::NetworkMessage> {
    self.client_msgs_received.get(eid).unwrap()
  }

  pub fn true_timestamp(&self) -> &Timestamp {
    &self.true_timestamp
  }

  /// This returns the `FullDBSchema` of some random Master node. This might not be
  /// the most recent Leader. Thus, this is only an approximate of the latest `FullDBSchema`.
  pub fn full_db_schema(&mut self) -> FullDBSchema {
    let master_eid = self.master_address_config.get(0).unwrap();
    let master_data = self.master_data.get(master_eid).unwrap();
    master_data.master_state.ctx.full_db_schema()
  }

  pub fn slave_address_config(&self) -> &BTreeMap<SlaveGroupId, Vec<EndpointId>> {
    &self.slave_address_config
  }

  // -----------------------------------------------------------------------------------------------
  //  Network Control
  // -----------------------------------------------------------------------------------------------

  /// Start forcibly changing the Leadership of `gid`.
  pub fn start_leadership_change(&mut self, gid: PaxosGroupId) {
    let lid = self.leader_map.get(&gid).unwrap().clone();
    self.blocked_leadership = Some((gid, lid.clone()));
  }

  /// Stop trying to forcibly change the Leadership of `blocked_leadership`. If it already
  /// happened, this effectively does nothing (since `blocked_leadership` will have been
  /// cleared). Returns `false` if this function did nothing, `true` otherwise.
  pub fn stop_leadership_change(&mut self) -> bool {
    if self.blocked_leadership.is_none() {
      false
    } else {
      self.blocked_leadership = None;
      true
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  Simulation Methods
  // -----------------------------------------------------------------------------------------------

  /// Add a message between two nodes in the network.
  pub fn add_msg(&mut self, msg: msg::NetworkMessage, from_eid: &EndpointId, to_eid: &EndpointId) {
    add_msg(&mut self.queues, &mut self.nonempty_queues, msg, from_eid, to_eid);
  }

  /// Peek a message in the queue from one node to another
  pub fn peek_msg(
    &self,
    from_eid: &EndpointId,
    to_eid: &EndpointId,
  ) -> Option<&msg::NetworkMessage> {
    let queue = self.queues.get(from_eid).unwrap().get(to_eid).unwrap();
    queue.front()
  }

  /// Poll a message in the queue from one node to another
  pub fn poll_msg(
    &mut self,
    from_eid: &EndpointId,
    to_eid: &EndpointId,
  ) -> Option<msg::NetworkMessage> {
    let queue = self.queues.get_mut(from_eid).unwrap().get_mut(to_eid).unwrap();
    if queue.len() == 1 {
      let index = self
        .nonempty_queues
        .iter()
        .position(|(from_eid2, to_eid2)| from_eid2 == from_eid && to_eid2 == to_eid)
        .unwrap();
      self.nonempty_queues.remove(index);
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
    let mut trace_msgs = VecDeque::<MasterTraceMessage>::new();
    let mut io_ctx = TestMasterIOCtx {
      rand: &mut self.rand,
      current_time, // TODO: simulate clock skew
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_eid: to_eid,
      tasks: &mut master_data.tasks,
      trace_msgs: &mut trace_msgs,
    };

    // Deliver the network message to the Master.
    master_data.master_state.handle_input(&mut io_ctx, FullMasterInput::MasterMessage(msg));

    // Update the `leader_map` if a new leader was traced.
    for trace_msg in trace_msgs {
      match trace_msg {
        MasterTraceMessage::LeaderChanged(lid) => {
          let cur_lid = self.leader_map.get_mut(&PaxosGroupId::Master).unwrap();
          if cur_lid.gen < lid.gen {
            *cur_lid = lid;
          }

          // Clear blocked_leadership if at node is no longer the Leader.
          if let Some((blocked_gid, blocked_lid)) = &self.blocked_leadership {
            if blocked_gid == &PaxosGroupId::Master {
              if blocked_lid.gen < cur_lid.gen {
                self.blocked_leadership = None
              }
            }
          }
        }
      }
    }
  }

  /// Same as above, except for a Slave.
  pub fn run_slave_message(&mut self, _: &EndpointId, to_eid: &EndpointId, msg: msg::SlaveMessage) {
    let slave_data = self.slave_data.get_mut(&to_eid).unwrap();
    let sid = slave_data.slave_state.ctx.this_sid.clone();
    let current_time = self.true_timestamp;
    let mut trace_msgs = VecDeque::<SlaveTraceMessage>::new();
    let mut io_ctx = TestSlaveIOCtx {
      rand: &mut self.rand,
      current_time, // TODO: simulate clock skew
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_sid: &sid,
      this_eid: to_eid,
      tablet_states: &mut slave_data.tablet_states,
      slave_back_messages: &mut slave_data.slave_back_messages,
      coord_states: &mut slave_data.coord_states,
      tasks: &mut slave_data.tasks,
      trace_msgs: &mut trace_msgs,
    };

    // Deliver the network message to the Slave.
    slave_data.slave_state.handle_input(&mut io_ctx, FullSlaveInput::SlaveMessage(msg));

    // Update the `leader_map` if a new leader was traced.
    let gid = sid.to_gid();
    for trace_msg in trace_msgs {
      match trace_msg {
        SlaveTraceMessage::LeaderChanged(lid) => {
          let cur_lid = self.leader_map.get_mut(&gid).unwrap();
          if cur_lid.gen < lid.gen {
            *cur_lid = lid;

            // Clear blocked_leadership if at node is no longer the Leader.
            if let Some((blocked_gid, blocked_lid)) = &self.blocked_leadership {
              if blocked_gid == &gid {
                if blocked_lid.gen < cur_lid.gen {
                  self.blocked_leadership = None
                }
              }
            }
          }
        }
      }
    }
  }

  /// The endpoints provided must exist. This function polls a message from
  /// the message queue between them and delivers to the `to_eid`. If that's
  /// a client, the message is added to `client_msgs_received`, and if it's
  /// a slave, the slave processes the message.
  pub fn deliver_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) {
    // First, check whether this queue is blocked due to `blocked_leadership`, returning if so.
    if let Some((_, lid)) = &self.blocked_leadership {
      if let Some(front_msg) = self.peek_msg(from_eid, to_eid) {
        match front_msg {
          msg::NetworkMessage::Slave(msg::SlaveMessage::PaxosDriverMessage(
            msg::PaxosDriverMessage::IsLeader(is_leader),
          )) => {
            if lid == &is_leader.lid {
              return;
            }
          }
          msg::NetworkMessage::Master(msg::MasterMessage::PaxosDriverMessage(
            msg::PaxosDriverMessage::IsLeader(is_leader),
          )) => {
            if lid == &is_leader.lid {
              return;
            }
          }
          _ => {}
        }
      }
    }

    // Otherwise, deliver the message
    if let Some(msg) = self.poll_msg(from_eid, to_eid) {
      self.stats.record(&msg);
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

  /// Execute the timer events up to the current `true_timestamp` for the Master.
  pub fn run_master_timer_events(&mut self) {
    for (eid, master_data) in &mut self.master_data {
      let current_time = self.true_timestamp;
      let mut trace_msgs = VecDeque::<MasterTraceMessage>::new();
      let mut io_ctx = TestMasterIOCtx {
        rand: &mut self.rand,
        current_time, // TODO: simulate clock skew
        queues: &mut self.queues,
        nonempty_queues: &mut self.nonempty_queues,
        this_eid: eid,
        tasks: &mut master_data.tasks,
        trace_msgs: &mut trace_msgs,
      };

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
        assert!(trace_msgs.is_empty());
        break;
      }
    }
  }

  /// Execute the timer events up to the current `true_timestamp` for the Slave.
  pub fn run_slave_timer_events(&mut self) {
    for (eid, slave_data) in &mut self.slave_data {
      let current_time = self.true_timestamp;
      let mut trace_msgs = VecDeque::<SlaveTraceMessage>::new();
      let mut io_ctx = TestSlaveIOCtx {
        rand: &mut self.rand,
        current_time, // TODO: simulate clock skew
        queues: &mut self.queues,
        nonempty_queues: &mut self.nonempty_queues,
        this_sid: &slave_data.slave_state.ctx.this_sid.clone(),
        this_eid: eid,
        tablet_states: &mut slave_data.tablet_states,
        slave_back_messages: &mut slave_data.slave_back_messages,
        coord_states: &mut slave_data.coord_states,
        tasks: &mut slave_data.tasks,
        trace_msgs: &mut trace_msgs,
      };

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
        assert!(trace_msgs.is_empty());
        break;
      }
    }
  }

  /// Execute the timer events up to the current `true_timestamp` in all nodes.
  pub fn run_timer_events(&mut self) {
    self.run_master_timer_events();
    self.run_slave_timer_events();
  }

  /// Run various consistency checks on the system validate whether it is in a good state.
  pub fn run_consistency_check(&mut self) {
    for (_, slave_data) in &self.slave_data {
      for (_, coord) in &slave_data.coord_states {
        assert_coord_consistency(coord);
      }
      for (_, tablet) in &slave_data.tablet_states {
        assert_tablet_consistency(tablet);
      }
    }
  }

  /// Checks that all nodes in the system are in their steady state after some
  /// time without any new requests.
  pub fn check_resources_clean(&mut self, should_assert: bool) -> bool {
    let mut check_ctx = CheckCtx::new(should_assert);
    for (_, master_data) in &self.master_data {
      check_master_clean(&master_data.master_state, &mut check_ctx);
    }
    for (_, slave_data) in &self.slave_data {
      check_slave_clean(&slave_data.slave_state, &mut check_ctx);
      for (_, coord) in &slave_data.coord_states {
        check_coord_clean(coord, &mut check_ctx);
      }
      for (_, tablet) in &slave_data.tablet_states {
        check_tablet_clean(tablet, &mut check_ctx);
      }
    }
    check_ctx.get_result()
  }

  /// This function simply increments the `true_time` by 1ms and delivers 1ms worth of
  /// messages. For simplicity, we assume that this means that every non-empty queue
  /// of messages delivers about as many messages are in that queue at a time, assuming all queues
  /// have about the same number of messages.
  ///
  /// Analysis:
  ///   - We wish to deliver messages sensibly as a real system would.
  ///   - A very large number of messages are RemoteLeaderChanged gossip messages, which
  ///     results in every leader sending every other node (not just leaders) a message.
  pub fn simulate1ms(&mut self) {
    self.true_timestamp += 1;
    let mut num_msgs_to_deliver = 0;
    for (from_eid, to_eid) in &self.nonempty_queues {
      let num_msgs = self.queues.get(from_eid).unwrap().get(to_eid).unwrap().len();
      num_msgs_to_deliver += num_msgs;
    }
    for _ in 0..num_msgs_to_deliver {
      let r = self.rand.next_u32() as usize % self.nonempty_queues.len();
      let (from_eid, to_eid) = self.nonempty_queues.get(r).unwrap().clone();
      self.deliver_msg(&from_eid, &to_eid);
    }

    self.run_timer_events();
    self.run_consistency_check();
  }

  pub fn simulate_n_ms(&mut self, n: u32) {
    for _ in 0..n {
      self.simulate1ms();
    }
  }

  pub fn mk_request_id(&mut self) -> RequestId {
    let request_id = RequestId(self.next_int.to_string());
    self.next_int += 1;
    return request_id;
  }
}
