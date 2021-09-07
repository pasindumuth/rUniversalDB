use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  rvec, Clock, GossipData, IOTypes, NetworkOut, TableSchema, TabletForwardOut,
};
use runiversal::model::common::{
  EndpointId, Gen, RequestId, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
};
use runiversal::model::message as msg;
use runiversal::slave::SlaveState;
use runiversal::tablet::TabletState;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter, Result};
use std::rc::Rc;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct TestClock {}

impl Clock for TestClock {
  fn now(&mut self) -> Timestamp {
    Timestamp(0) // TODO: make this proper.
  }
}

/// An interface for test network interaction. `NetworkMessages`
/// are passed through to the target node by adding it to a queue
/// in `queues`.
#[derive(Clone)]
struct TestNetworkOut {
  queues: Rc<RefCell<HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>>>,
  nonempty_queues: Rc<RefCell<Vec<(EndpointId, EndpointId)>>>,
  from_eid: EndpointId,
}

impl NetworkOut for TestNetworkOut {
  fn send(&mut self, to_eid: &EndpointId, msg: msg::NetworkMessage) {
    add_msg(&mut self.queues, &mut self.nonempty_queues, msg, &self.from_eid, to_eid);
  }
}

impl Debug for TestNetworkOut {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "TestNetworkOut")
  }
}

/// A simple interface for pushing messages from the Slave to
/// the Tablets.
struct TestTabletForwardOut {
  tablet_states: Rc<RefCell<HashMap<EndpointId, HashMap<TabletGroupId, TabletState<TestIOTypes>>>>>,
  from_eid: EndpointId,
}

impl TabletForwardOut for TestTabletForwardOut {
  fn forward(&mut self, tablet_group_id: &TabletGroupId, msg: msg::TabletMessage) {
    self
      .tablet_states
      .borrow_mut()
      .get_mut(&self.from_eid)
      .unwrap()
      .get_mut(tablet_group_id)
      .unwrap()
      .handle_incoming_message(msg);
  }
}

impl Debug for TestTabletForwardOut {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "TestTabletForwardOut")
  }
}

/// IOTypes for testing purposes.
struct TestIOTypes {}

impl IOTypes for TestIOTypes {
  type RngCoreT = XorShiftRng;
  type ClockT = TestClock;
  type NetworkOutT = TestNetworkOut;
  type TabletForwardOutT = TestTabletForwardOut;
}

impl Debug for TestIOTypes {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "TestIOTypes")
  }
}

// -----------------------------------------------------------------------------------------------
//  Simulation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct Simulation {
  pub rng: XorShiftRng,
  slave_eids: Vec<EndpointId>,
  client_eids: Vec<EndpointId>,
  /// Message queues between nodes. This field contains contains 2 queues (in for
  /// each direction) for every pair of client EndpointIds and slave Endpoints.
  queues: Rc<RefCell<HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>>>,
  /// We use pairs of endpoints as identifiers of a queue.
  /// This field contain all queue IDs where the queue is non-empty
  nonempty_queues: Rc<RefCell<Vec<(EndpointId, EndpointId)>>>,
  slave_states: Rc<RefCell<HashMap<EndpointId, SlaveState<TestIOTypes>>>>,
  tablet_states: Rc<RefCell<HashMap<EndpointId, HashMap<TabletGroupId, TabletState<TestIOTypes>>>>>,
  /// Meta
  next_int: i32,
  true_timestamp: i64,
  /// Accumulated client responses for each client.
  client_msgs_received: HashMap<EndpointId, Vec<msg::NetworkMessage>>,
}

impl Simulation {
  /// Here, for every key in `tablet_config`, we create a slave, and
  /// we create as many tablets as there are `TabletGroupId` for that slave.
  pub fn new(
    seed: [u8; 16],
    num_clients: i32,

    // Arguments that are specific to SlaveState and TabletState
    schema: HashMap<TablePath, TableSchema>,
    sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, EndpointId>,
  ) -> Simulation {
    let mut sim = Simulation {
      rng: XorShiftRng::from_seed(seed),
      slave_eids: Default::default(),
      client_eids: Default::default(),
      queues: Default::default(),
      nonempty_queues: Default::default(),
      slave_states: Default::default(),
      tablet_states: Default::default(),
      next_int: Default::default(),
      true_timestamp: Default::default(),
      client_msgs_received: Default::default(),
    };

    // Setup eids
    sim.slave_eids = slave_address_config.values().cloned().collect();
    sim.client_eids = rvec(0, num_clients).iter().map(client_eid).collect();
    let all_eids: Vec<EndpointId> =
      sim.slave_eids.iter().cloned().chain(sim.client_eids.iter().cloned()).collect();
    for from_eid in &all_eids {
      sim.queues.borrow_mut().insert(from_eid.clone(), Default::default());
      for to_eid in &all_eids {
        sim.queues.borrow_mut().get_mut(from_eid).unwrap().insert(to_eid.clone(), VecDeque::new());
      }
    }

    // Invert `tablet_address_config` for use later.
    let mut tablets_for_slave = HashMap::<SlaveGroupId, Vec<TabletGroupId>>::new();
    for (tid, sid) in &tablet_address_config {
      if !tablets_for_slave.contains_key(sid) {
        tablets_for_slave.insert(sid.clone(), Vec::new());
      }
      tablets_for_slave.get_mut(sid).unwrap().push(tid.clone());
    }

    // Construct SlaveState and TabletState
    for (sid, eid) in &slave_address_config {
      let network_out = TestNetworkOut {
        queues: sim.queues.clone(),
        nonempty_queues: sim.nonempty_queues.clone(),
        from_eid: eid.clone(),
      };
      let clock = TestClock {};
      let gossip = Arc::new(GossipData {
        gen: Gen(0),
        db_schema: schema.clone(),
        table_generation: Default::default(),
        sharding_config: sharding_config.clone(),
        tablet_address_config: tablet_address_config.clone(),
        slave_address_config: slave_address_config.clone(),
      });
      let mut seed = [0; 16];
      sim.rng.fill_bytes(&mut seed);
      sim.slave_states.borrow_mut().insert(
        eid.clone(),
        SlaveState::new(
          XorShiftRng::from_seed(seed),
          clock.clone(),
          network_out.clone(),
          TestTabletForwardOut { tablet_states: sim.tablet_states.clone(), from_eid: eid.clone() },
          gossip.clone(),
          sid.clone(),
          EndpointId("".to_string()),
        ),
      );
      sim.tablet_states.borrow_mut().insert(eid.clone(), Default::default());
      for tid in tablets_for_slave.get(sid).unwrap() {
        let mut seed = [0; 16];
        sim.rng.fill_bytes(&mut seed);
        sim.tablet_states.borrow_mut().get_mut(eid).unwrap().insert(
          tid.clone(),
          TabletState::new(
            XorShiftRng::from_seed(seed),
            clock.clone(),
            network_out.clone(),
            gossip.clone(),
            sid.clone(),
            tid.clone(),
            EndpointId("".to_string()), // TODO: Implement Master properly.
          ),
        );
      }
    }
    sim.next_int = 0;
    sim.true_timestamp = 0;
    for eid in &sim.client_eids {
      sim.client_msgs_received.insert(eid.clone(), Vec::new());
    }
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
    let mut queues = self.queues.borrow_mut();
    let queue = queues.get_mut(from_eid).unwrap().get_mut(to_eid).unwrap();
    if queue.len() == 1 {
      let mut nonempty_queues = self.nonempty_queues.borrow_mut();
      if let Some(index) = nonempty_queues
        .iter()
        .position(|(from_eid2, to_eid2)| from_eid2 == from_eid && to_eid2 == to_eid)
      {
        nonempty_queues.remove(index);
      }
    }
    queue.pop_front()
  }

  /// When this is called, the `msg` will already have been popped from
  /// `queues`. This function will run the Slave on the `to_eid` end, which
  /// might have any number of side effects, including adding new messages into
  /// `queues`.
  pub fn run_slave_message(&mut self, _: &EndpointId, to_eid: &EndpointId, msg: msg::SlaveMessage) {
    self.slave_states.borrow_mut().get_mut(&to_eid).unwrap().handle_incoming_message(msg);
  }

  /// The endpoints provided must exist. This function polls a message from
  /// the message queue between them and delivers to the `to_eid`. If that's
  /// a client, the message is added to `client_msgs_received`, and if it's
  /// a slave, the slave processes the message.
  pub fn deliver_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) {
    if let Some(msg) = self.poll_msg(from_eid, to_eid) {
      if self.slave_states.borrow_mut().contains_key(to_eid) {
        match msg {
          msg::NetworkMessage::Slave(slave_msg) => {
            self.run_slave_message(from_eid, to_eid, slave_msg)
          }
          _ => {
            panic!("Endpoint {:?} is a Slave but received a non-SlaveMessage {:?} ", to_eid, msg)
          }
        }
      } else if self.client_msgs_received.contains_key(to_eid) {
        if let Some(msgs) = self.client_msgs_received.get_mut(to_eid) {
          msgs.push(msg);
        } else {
          self.client_msgs_received.insert(to_eid.clone(), vec![msg]);
        }
      } else {
        panic!("Endpoint {:?} does not exist", to_eid);
      }
    }
  }

  /// Drops messages until `num_msgs` have been dropped, or until
  /// there are no more messages to drop.
  pub fn drop_messages(&mut self, num_msgs: i32) {
    for _ in 0..num_msgs {
      if self.nonempty_queues.borrow_mut().len() > 0 {
        let r = self.rng.next_u32() as usize % self.nonempty_queues.borrow_mut().len();
        let (from_eid, to_eid) = self.nonempty_queues.borrow_mut().get(r).unwrap().clone();
        self.poll_msg(&from_eid, &to_eid);
      }
    }
  }

  /// This function simply delivers 1ms worth of messages. For simplicity, we assume
  /// that this means that every non-empty queue of messages delivers about one
  /// message in this time.
  pub fn simulate1ms(&mut self) {
    // `num_msgs` is about the number of message to deliver for this ms.
    let num_msgs = self.nonempty_queues.borrow_mut().len();
    for _ in 0..num_msgs {
      let r = self.rng.next_u32() as usize % self.nonempty_queues.borrow_mut().len();
      let (from_eid, to_eid) = self.nonempty_queues.borrow_mut().get(r).unwrap().clone();
      self.deliver_msg(&from_eid, &to_eid);
    }
  }

  pub fn simulate_n_ms(&mut self, n: i32) {
    for _ in 0..n {
      self.simulate1ms();
    }
  }

  pub fn simulate_all(&mut self) {
    while self.nonempty_queues.borrow_mut().len() > 0 {
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
  queues: &Rc<RefCell<HashMap<EndpointId, HashMap<EndpointId, VecDeque<msg::NetworkMessage>>>>>,
  nonempty_queues: &Rc<RefCell<Vec<(EndpointId, EndpointId)>>>,
  msg: msg::NetworkMessage,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
) {
  let mut queues = queues.borrow_mut();
  let queue = queues.get_mut(from_eid).unwrap().get_mut(to_eid).unwrap();
  if queue.len() == 0 {
    let queue_id = (from_eid.clone(), to_eid.clone());
    nonempty_queues.borrow_mut().push(queue_id);
  }
  queue.push_back(msg);
}
