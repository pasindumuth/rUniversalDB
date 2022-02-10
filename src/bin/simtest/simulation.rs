use crate::stats::Stats;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  mk_cid, mk_t, BasicIOCtx, CoreIOCtx, FreeNodeIOCtx, GeneralTraceMessage, GossipData,
  GossipDataView, LeaderMap, MasterIOCtx, MasterTraceMessage, NodeIOCtx, RangeEnds, SlaveIOCtx,
  SlaveTraceMessage, Timestamp, NUM_COORDS,
};
use runiversal::coord::coord_test::{assert_coord_consistency, check_coord_clean};
use runiversal::coord::{CoordConfig, CoordContext, CoordForwardMsg, CoordState};
use runiversal::free_node_manager::FreeNodeType;
use runiversal::master::master_test::check_master_clean;
use runiversal::master::{
  FullMasterInput, MasterConfig, MasterContext, MasterState, MasterTimerInput,
};
use runiversal::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, QueryId, RequestId,
  SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange,
};
use runiversal::model::message as msg;
use runiversal::model::message::NetworkMessage;
use runiversal::multiversion_map::MVM;
use runiversal::node::node_test::check_node_clean;
use runiversal::node::{GenericInput, NodeState};
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::{add_msg, mk_client_eid, mk_node_eid};
use runiversal::slave::slave_test::check_slave_clean;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveConfig, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::tablet::tablet_test::{assert_tablet_consistency, check_tablet_clean};
use runiversal::tablet::{TabletContext, TabletCreateHelper, TabletForwardMsg, TabletState};
use runiversal::test_utils::CheckCtx;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  TestIOCtx
// -----------------------------------------------------------------------------------------------

/// This is the IOCtx we use for nodes. The `current_time` for a given round of execution is
/// static. Thus, it does not advance when Executing the Tablet messages,
pub struct TestIOCtx<'a> {
  // Basic
  rand: &'a mut XorShiftRng,
  current_time: Timestamp,
  queues: &'a mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_eid: &'a EndpointId,

  // Basic
  exited: &'a mut bool,

  /// The means by which we send messages back to the top of the node.
  to_node: &'a mut VecDeque<GenericInput>,

  /// Tablets for this Slave
  tablet_states: &'a mut BTreeMap<TabletGroupId, TabletState>,
  /// Coords for this Slave
  coord_states: &'a mut BTreeMap<CoordGroupId, CoordState>,

  /// Deferred timer tasks
  slave_tasks: &'a mut BTreeMap<Timestamp, Vec<SlaveTimerInput>>,
  master_tasks: &'a mut BTreeMap<Timestamp, Vec<MasterTimerInput>>,

  /// Collection of trace messages
  success_tracer: &'a mut RequestSuccessTracer,
  slave_trace_msgs: &'a mut VecDeque<SlaveTraceMessage>,
  master_trace_msgs: &'a mut VecDeque<MasterTraceMessage>,
}

impl<'a> BasicIOCtx for TestIOCtx<'a> {
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

  fn general_trace(&mut self, trace_msg: GeneralTraceMessage) {
    self.success_tracer.process(trace_msg);
  }
}

impl<'a> FreeNodeIOCtx for TestIOCtx<'a> {
  fn create_tablet_full(&mut self, ctx: TabletContext) {
    let tid = ctx.this_tid.clone();
    self.tablet_states.insert(tid, TabletState::new(ctx));
  }

  fn create_coord_full(&mut self, ctx: CoordContext) {
    let cid = ctx.this_cid.clone();
    self.coord_states.insert(cid, CoordState::new(ctx));
  }
}

impl<'a> SlaveIOCtx for TestIOCtx<'a> {
  fn mark_exit(&mut self) {
    *self.exited = true;
  }

  fn did_exit(&mut self) -> bool {
    *self.exited
  }

  fn create_tablet(&mut self, helper: TabletCreateHelper) {
    let tid = helper.this_tid.clone();
    self.tablet_states.insert(tid, TabletState::new(TabletContext::new(helper)));
  }

  fn tablet_forward(&mut self, tid: &TabletGroupId, forward_msg: TabletForwardMsg) {
    let tablet = self.tablet_states.get_mut(tid).unwrap();
    let mut io_ctx = TestCoreIOCtx {
      rand: self.rand,
      current_time: self.current_time.clone(),
      queues: self.queues,
      nonempty_queues: self.nonempty_queues,
      this_eid: self.this_eid,
      success_tracer: self.success_tracer,
      to_node: self.to_node,
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
      current_time: self.current_time.clone(),
      queues: self.queues,
      nonempty_queues: self.nonempty_queues,
      this_eid: self.this_eid,
      success_tracer: self.success_tracer,
      to_node: self.to_node,
    };
    coord.handle_input(&mut io_ctx, forward_msg);
  }

  fn all_cids(&self) -> Vec<CoordGroupId> {
    self.coord_states.keys().cloned().collect()
  }

  fn defer(&mut self, defer_time: Timestamp, timer_input: SlaveTimerInput) {
    let deferred_time = self.now().add(defer_time);
    if let Some(timer_inputs) = self.slave_tasks.get_mut(&deferred_time) {
      timer_inputs.push(timer_input);
    } else {
      self.slave_tasks.insert(deferred_time, vec![timer_input]);
    }
  }

  fn trace(&mut self, trace_msg: SlaveTraceMessage) {
    self.slave_trace_msgs.push_back(trace_msg);
  }
}

impl<'a> MasterIOCtx for TestIOCtx<'a> {
  fn mark_exit(&mut self) {
    *self.exited = true;
  }

  fn did_exit(&mut self) -> bool {
    *self.exited
  }

  fn defer(&mut self, defer_time: Timestamp, timer_input: MasterTimerInput) {
    let deferred_time = self.now().add(defer_time);
    if let Some(timer_inputs) = self.master_tasks.get_mut(&deferred_time) {
      timer_inputs.push(timer_input);
    } else {
      self.master_tasks.insert(deferred_time, vec![timer_input]);
    }
  }

  fn trace(&mut self, trace_msg: MasterTraceMessage) {
    self.master_trace_msgs.push_back(trace_msg);
  }
}

impl<'a> NodeIOCtx for TestIOCtx<'a> {}

// -----------------------------------------------------------------------------------------------
//  TestCoreIOCtx
// -----------------------------------------------------------------------------------------------

pub struct TestCoreIOCtx<'a> {
  // Basic
  rand: &'a mut XorShiftRng,
  current_time: Timestamp,
  queues: &'a mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<msg::NetworkMessage>>>,
  nonempty_queues: &'a mut Vec<(EndpointId, EndpointId)>,

  // Metadata
  this_eid: &'a EndpointId,

  // Tablet
  success_tracer: &'a mut RequestSuccessTracer,
  to_node: &'a mut VecDeque<GenericInput>,
}

impl<'a> BasicIOCtx for TestCoreIOCtx<'a> {
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

  fn general_trace(&mut self, trace_msg: GeneralTraceMessage) {
    self.success_tracer.process(trace_msg);
  }
}

impl<'a> CoreIOCtx for TestCoreIOCtx<'a> {
  fn slave_forward(&mut self, msg: SlaveBackMessage) {
    self.to_node.push_back(GenericInput::SlaveBackMessage(msg));
  }
}

// -----------------------------------------------------------------------------------------------
//  Tracing
// -----------------------------------------------------------------------------------------------

/// This is used to keep track of which `RequestId`s were successful. This is useful
/// for Requests that respond to the External with a `NodeDied` message, does not give
/// information whether the query completed successfully or not.
#[derive(Debug)]
struct RequestSuccessTracer {
  rid_qid_map: BTreeMap<RequestId, QueryId>,
  qid_rid_map: BTreeMap<QueryId, RequestId>,
  successful_reqs: BTreeMap<RequestId, Timestamp>,
}

impl RequestSuccessTracer {
  fn new() -> RequestSuccessTracer {
    RequestSuccessTracer {
      rid_qid_map: Default::default(),
      qid_rid_map: Default::default(),
      successful_reqs: Default::default(),
    }
  }

  fn process(&mut self, trace_msg: GeneralTraceMessage) {
    match trace_msg {
      GeneralTraceMessage::RequestIdQueryId(rid, qid) => {
        // First, potentially remove an existing entry if it exists.
        if let Some(prev_qid) = self.rid_qid_map.remove(&rid) {
          assert_eq!(rid, self.qid_rid_map.remove(&prev_qid).unwrap());
        }

        // Add in the new entry
        self.rid_qid_map.insert(rid.clone(), qid.clone());
        self.qid_rid_map.insert(qid.clone(), rid.clone());
      }
      GeneralTraceMessage::CommittedQueryId(qid, timestamp) => {
        // Add this `qid` to `successful_reqs`. We do not remove it since other RMs can
        // also trace this message.
        let rid = self.qid_rid_map.get(&qid).unwrap();
        if let Some(cur_timestamp) = self.successful_reqs.get(&rid) {
          // If a success is already recorded, we check that the timestamps align.
          assert_eq!(&timestamp, cur_timestamp);
        } else {
          self.successful_reqs.insert(rid.clone(), timestamp);
        }
      }
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Simulation
// -----------------------------------------------------------------------------------------------

pub struct NodeData {
  node: NodeState,
  exited: bool,
  tablet_states: BTreeMap<TabletGroupId, TabletState>,
  coord_states: BTreeMap<CoordGroupId, CoordState>,
  slave_tasks: BTreeMap<Timestamp, Vec<SlaveTimerInput>>,
  master_tasks: BTreeMap<Timestamp, Vec<MasterTimerInput>>,
  to_node: VecDeque<GenericInput>,
}

impl Debug for NodeData {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut debug_trait_builder = f.debug_struct("NodeData");
    let _ = debug_trait_builder.field("node", &self.node);
    let _ = debug_trait_builder.field("exited", &self.exited);
    let _ = debug_trait_builder.field("tablet_states", &self.tablet_states);
    let _ = debug_trait_builder.field("coord_states", &self.coord_states);
    let _ = debug_trait_builder.field("slave_tasks", &self.slave_tasks);
    let _ = debug_trait_builder.field("master_tasks", &self.master_tasks);
    let _ = debug_trait_builder.field("to_node", &self.to_node);
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
  node_datas: BTreeMap<EndpointId, NodeData>,
  /// Accumulated client responses for each client
  client_msgs_received: BTreeMap<EndpointId, Vec<msg::NetworkMessage>>,

  /// Meta
  true_timestamp: Timestamp,
  stats: Stats,

  /// Tracer
  success_tracer: RequestSuccessTracer,

  /// Inferred LeaderMap. `PaxosGroupId`s will be added here trace messages. Then, it
  /// will evolve continuously (there will be no jumps in LeadershipId; the `gen` will
  /// increases one by one).
  pub leader_map: LeaderMap,
  /// If this is set, the `IsLeader` messages disseminated from this Leadership will be blocked.
  blocked_leadership: Option<(PaxosGroupId, LeadershipId)>,
}

impl Simulation {
  /// Here, for every key in `tablet_config`, we create a slave, and
  /// we create as many tablets as there are `TabletGroupId` for that slave.
  pub fn new(
    seed: [u8; 16],
    num_clients: u32,
    num_nodes: u32,
    paxos_config: PaxosConfig,
    coord_config: CoordConfig,
    master_config: MasterConfig,
    slave_config: SlaveConfig,
  ) -> Simulation {
    let mut sim = Simulation {
      rand: XorShiftRng::from_seed(seed),
      queues: Default::default(),
      nonempty_queues: Default::default(),
      node_datas: Default::default(),
      true_timestamp: mk_t(0),
      client_msgs_received: Default::default(),
      stats: Stats::default(),
      success_tracer: RequestSuccessTracer::new(),
      leader_map: Default::default(),
      blocked_leadership: None,
    };

    // Setup eids
    let node_eids: Vec<EndpointId> =
      RangeEnds::rvec(0, num_nodes).iter().map(|i| mk_node_eid(*i)).collect();
    let client_eids: Vec<EndpointId> =
      RangeEnds::rvec(0, num_clients).iter().map(|i| mk_client_eid(*i)).collect();
    let all_eids: Vec<EndpointId> = vec![]
      .into_iter()
      .chain(node_eids.iter().cloned())
      .chain(client_eids.iter().cloned())
      .collect();

    for from_eid in &all_eids {
      sim.queues.insert(from_eid.clone(), Default::default());
      for to_eid in &all_eids {
        sim.queues.get_mut(from_eid).unwrap().insert(to_eid.clone(), VecDeque::new());
      }
    }

    for eid in &node_eids {
      sim.node_datas.insert(
        eid.clone(),
        NodeData {
          node: NodeState::new(
            eid.clone(),
            paxos_config.clone(),
            coord_config.clone(),
            master_config.clone(),
            slave_config.clone(),
          ),
          exited: false,
          tablet_states: Default::default(),
          coord_states: Default::default(),
          slave_tasks: Default::default(),
          master_tasks: Default::default(),
          to_node: Default::default(),
        },
      );
    }

    // External
    for eid in &client_eids {
      sim.client_msgs_received.insert(eid.clone(), Vec::new());
    }

    // Metadata
    sim.true_timestamp = mk_t(0);

    return sim;
  }

  // -----------------------------------------------------------------------------------------------
  //  Const Accessors
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

  /// This returns the `FullDBSchema` of the current Master Leader. Note that the Master
  /// Groups needs to be instantiated for this to work.
  pub fn full_db_schema(&self) -> GossipDataView {
    let lid = self.leader_map.get(&PaxosGroupId::Master).unwrap();
    let node_data = self.node_datas.get(&lid.eid).unwrap();
    node_data.node.full_db_schema()
  }

  pub fn get_success_reqs(&self) -> BTreeMap<RequestId, Timestamp> {
    self.success_tracer.successful_reqs.clone()
  }

  pub fn get_stats(&self) -> &Stats {
    &self.stats
  }

  /// Checks if the node and `eid` exists. Note that `eid` must be a valid `EndpointId`.
  pub fn does_node_exist(&self, eid: &EndpointId) -> bool {
    self.node_datas.get(eid).unwrap().node.does_exist()
  }

  /// Checks if the node in `eids` exists. Note that all `eids` must be a valid `EndpointId`.
  pub fn do_nodes_exist(&self, eids: &Vec<EndpointId>) -> bool {
    for eid in eids {
      if !self.does_node_exist(eid) {
        return false;
      }
    }

    true
  }

  // -----------------------------------------------------------------------------------------------
  //  FreeNode
  // -----------------------------------------------------------------------------------------------

  /// The `eid` here should only a node that is in `DNEState`. In addition, a Master
  /// groups must be instantiated by this point.
  pub fn register_free_node(&mut self, eid: &EndpointId) {
    let lid = self.leader_map.get(&PaxosGroupId::Master).unwrap().clone();
    self.add_msg(
      msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
        msg::FreeNodeAssoc::RegisterFreeNode(msg::RegisterFreeNode {
          sender_eid: eid.clone(),
          node_type: FreeNodeType::NewSlaveFreeNode,
        }),
      )),
      &eid,
      &lid.eid,
    );
  }

  // -----------------------------------------------------------------------------------------------
  //  Network Control
  // -----------------------------------------------------------------------------------------------

  /// Start forcibly changing the Leadership of `gid`.
  pub fn start_leadership_change(&mut self, gid: PaxosGroupId) {
    let lid = self.leader_map.get(&gid).unwrap().clone();
    self.blocked_leadership = Some((gid, lid.clone()));
  }

  /// Check if the `Simulation` was set to try and change the Leadership.
  pub fn is_leadership_changing(&self) -> bool {
    self.blocked_leadership.is_some()
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

  /// Execute a message at the given node (which `to_end` is guaranteed to contain).
  /// When this is called, the `msg` will already have been popped from
  /// `queues`. This function will run the node on the `to_eid` end, which
  /// might have any number of side effects, including adding new messages into
  /// `queues`.
  pub fn run_node_message(
    &mut self,
    from_eid: &EndpointId,
    to_eid: &EndpointId,
    msg: msg::NetworkMessage,
  ) {
    let node_data = self.node_datas.get_mut(&to_eid).unwrap();
    let current_time = self.true_timestamp.clone();
    let mut slave_trace_msgs = VecDeque::<SlaveTraceMessage>::new();
    let mut master_trace_msgs = VecDeque::<MasterTraceMessage>::new();
    let mut io_ctx = TestIOCtx {
      rand: &mut self.rand,
      current_time, // TODO: simulate clock skew
      queues: &mut self.queues,
      nonempty_queues: &mut self.nonempty_queues,
      this_eid: to_eid,
      exited: &mut node_data.exited,
      to_node: &mut node_data.to_node,
      tablet_states: &mut node_data.tablet_states,
      coord_states: &mut node_data.coord_states,
      slave_tasks: &mut node_data.slave_tasks,
      master_tasks: &mut node_data.master_tasks,
      success_tracer: &mut self.success_tracer,
      slave_trace_msgs: &mut slave_trace_msgs,
      master_trace_msgs: &mut master_trace_msgs,
    };

    // Deliver the network message to the Node.
    node_data.node.process_input(&mut io_ctx, GenericInput::Message(from_eid.clone(), msg));

    // Update the `leader_map` if a new leader was traced.
    for trace_msg in slave_trace_msgs {
      match trace_msg {
        SlaveTraceMessage::LeaderChanged(gid, lid) => {
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

    // Update the `leader_map` if a new leader was traced.
    for trace_msg in master_trace_msgs {
      match trace_msg {
        MasterTraceMessage::LeaderChanged(lid) => {
          let cur_lid = self.leader_map.get_mut(&PaxosGroupId::Master).unwrap();
          if cur_lid.gen < lid.gen {
            *cur_lid = lid;

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
        MasterTraceMessage::MasterCreation(lid) => {
          if !self.leader_map.contains_key(&PaxosGroupId::Master) {
            self.leader_map.insert(PaxosGroupId::Master, lid);
          }
        }
        MasterTraceMessage::SlaveCreated(sid, lid) => {
          let gid = sid.to_gid();
          if !self.leader_map.contains_key(&gid) {
            self.leader_map.insert(gid, lid);
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
      if self.node_datas.contains_key(to_eid) {
        self.run_node_message(from_eid, to_eid, msg);
      } else if let Some(msgs) = self.client_msgs_received.get_mut(to_eid) {
        msgs.push(msg);
      } else {
        panic!("Endpoint {:?} does not exist", to_eid);
      }
    }
  }

  /// Execute the timer events up to the current `true_timestamp` for the Node.
  pub fn run_node_timer_events(&mut self) {
    for (eid, node_data) in &mut self.node_datas {
      let current_time = self.true_timestamp.clone();
      let mut trace_msgs = VecDeque::<SlaveTraceMessage>::new();
      let mut io_ctx = TestIOCtx {
        rand: &mut self.rand,
        current_time: current_time.clone(), // TODO: simulate clock skew
        queues: &mut self.queues,
        nonempty_queues: &mut self.nonempty_queues,
        this_eid: eid,
        exited: &mut node_data.exited,
        to_node: &mut node_data.to_node,
        tablet_states: &mut node_data.tablet_states,
        coord_states: &mut node_data.coord_states,
        slave_tasks: &mut node_data.slave_tasks,
        master_tasks: &mut node_data.master_tasks,
        success_tracer: &mut self.success_tracer,
        slave_trace_msgs: &mut Default::default(),
        master_trace_msgs: &mut Default::default(),
      };

      // Send Process all back messages and timer events

      loop {
        // Process back messages
        if let Some(back_msg) = io_ctx.to_node.pop_front() {
          node_data.node.process_input(&mut io_ctx, back_msg);
          continue;
        }

        // Process slave timer inputs
        if let Some((next_timestamp, _)) = io_ctx.slave_tasks.first_key_value() {
          if next_timestamp <= &current_time {
            // All data in this first entry should be dispatched.
            let next_timestamp = next_timestamp.clone();
            for timer_input in io_ctx.slave_tasks.remove(&next_timestamp).unwrap() {
              node_data.node.process_input(&mut io_ctx, GenericInput::SlaveTimerInput(timer_input));
            }
            continue;
          }
        }

        // Process master timer inputs
        if let Some((next_timestamp, _)) = io_ctx.master_tasks.first_key_value() {
          if next_timestamp <= &current_time {
            // All data in this first entry should be dispatched.
            let next_timestamp = next_timestamp.clone();
            for timer_input in io_ctx.master_tasks.remove(&next_timestamp).unwrap() {
              node_data
                .node
                .process_input(&mut io_ctx, GenericInput::MasterTimerInput(timer_input));
            }
            continue;
          }
        }

        // Getting here means there are no more left, so we exit.
        assert!(trace_msgs.is_empty());
        break;
      }
    }
  }

  /// Execute the timer events up to the current `true_timestamp` in all nodes.
  pub fn run_timer_events(&mut self) {
    self.run_node_timer_events();
  }

  /// Run various consistency checks on the system validate whether it is in a good state.
  pub fn run_consistency_check(&mut self) {
    for (_, node_data) in &self.node_datas {
      for (_, coord) in &node_data.coord_states {
        assert_coord_consistency(coord);
      }
      for (_, tablet) in &node_data.tablet_states {
        assert_tablet_consistency(tablet);
      }
    }
  }

  /// Checks that all nodes in the system are in their steady state after some
  /// time without any new requests.
  pub fn check_resources_clean(&mut self, should_assert: bool) -> bool {
    let mut check_ctx = CheckCtx::new(should_assert);
    for (_, node_data) in &self.node_datas {
      check_node_clean(&node_data.node, &mut check_ctx);
      for (_, coord) in &node_data.coord_states {
        check_coord_clean(coord, &mut check_ctx);
      }
      for (_, tablet) in &node_data.tablet_states {
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
    self.true_timestamp = self.true_timestamp.add(mk_t(1));
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
}
