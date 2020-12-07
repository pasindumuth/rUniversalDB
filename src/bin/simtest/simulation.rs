use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::lang::rvec;
use runiversal::common::rand::RandGen;
use runiversal::model::common::{EndpointId, RequestId, Schema, TabletShape};
use runiversal::model::message::{
  AdminMessage, AdminRequest, NetworkMessage, SlaveAction, SlaveMessage, TabletAction,
  TabletMessage,
};
use runiversal::slave::slave::{SlaveSideEffects, SlaveState};
use runiversal::tablet::tablet::{TabletSideEffects, TabletState};
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub struct Simulation {
  rng: XorShiftRng,
  slave_eids: Vec<EndpointId>,
  client_eids: Vec<EndpointId>,
  /// Message queues between nodes. This field contains contains 2 queues (in for
  /// each direction) for every pair of client EndpointIds and slave Endpoints.
  queues: HashMap<EndpointId, HashMap<EndpointId, VecDeque<NetworkMessage>>>,
  /// We use pairs of endpoints as identifiers of a queue.
  /// This field contain all queue IDs where the queue is non-empty
  nonempty_queues: Vec<(EndpointId, EndpointId)>,
  slave_states: HashMap<EndpointId, SlaveState>,
  tablet_states: HashMap<EndpointId, HashMap<TabletShape, TabletState>>,
  /// Meta
  next_int: i32,
  true_timestamp: i64,
  /// Accumulated client responses for each client.
  client_msgs_received: HashMap<EndpointId, Vec<NetworkMessage>>,
}

pub fn slave_id(i: &i32) -> EndpointId {
  EndpointId(format!("s{}", i))
}

pub fn client_id(i: &i32) -> EndpointId {
  EndpointId(format!("c{}", i))
}

impl Simulation {
  pub fn new(
    seed: [u8; 16],
    static_schema: Schema,
    key_space_config: HashMap<EndpointId, Vec<TabletShape>>,
    num_clients: i32,
  ) -> Simulation {
    let mut rng = XorShiftRng::from_seed(seed);
    let slave_eids: Vec<EndpointId> = key_space_config.keys().cloned().collect();
    let client_eids: Vec<EndpointId> = rvec(0, num_clients).iter().map(client_id).collect();
    let all_eids: Vec<EndpointId> = slave_eids
      .iter()
      .cloned()
      .chain(client_eids.iter().cloned())
      .collect();
    let queues = all_eids
      .iter()
      .map(|from_eid| {
        (
          from_eid.clone(),
          all_eids
            .iter()
            .map(|to_eid| (to_eid.clone(), VecDeque::new()))
            .collect(),
        )
      })
      .collect();
    let mut slave_states = HashMap::new();
    for eid in &slave_eids {
      let mut seed = [0; 16];
      rng.fill_bytes(&mut seed);
      slave_states.insert(
        eid.clone(),
        SlaveState::new(
          RandGen {
            rng: Box::new(XorShiftRng::from_seed(seed)),
          },
          eid.clone(),
        ),
      );
    }
    let mut tablet_states = HashMap::new();
    for eid in &slave_eids {
      for shape in &key_space_config[eid] {
        let mut slave_tablet_states = HashMap::new();
        let mut seed = [0; 16];
        rng.fill_bytes(&mut seed);
        slave_tablet_states.insert(
          shape.clone(),
          TabletState::new(
            RandGen {
              rng: Box::new(XorShiftRng::from_seed(seed)),
            },
            shape.clone(),
            static_schema.clone(),
          ),
        );
        tablet_states.insert(eid.clone(), slave_tablet_states);
      }
    }
    let client_msgs_received = client_eids
      .iter()
      .map(|eid| (eid.clone(), Vec::new()))
      .collect();
    Simulation {
      rng,
      slave_eids,
      client_eids,
      queues,
      nonempty_queues: Default::default(),
      slave_states,
      tablet_states,
      next_int: 0,
      true_timestamp: 0,
      client_msgs_received,
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  Const getters
  // -----------------------------------------------------------------------------------------------

  pub fn get_responses(&self) -> &HashMap<EndpointId, Vec<NetworkMessage>> {
    return &self.client_msgs_received;
  }

  // -----------------------------------------------------------------------------------------------
  //  Simulation Methods
  // -----------------------------------------------------------------------------------------------

  /// Add a message between two nodes in the network.
  pub fn add_msg(&mut self, msg: NetworkMessage, from_eid: &EndpointId, to_eid: &EndpointId) {
    let queue = self
      .queues
      .get_mut(from_eid)
      .unwrap()
      .get_mut(to_eid)
      .unwrap();
    if queue.len() == 0 {
      let queue_id = (from_eid.clone(), to_eid.clone());
      self.nonempty_queues.push(queue_id);
    }
    queue.push_back(msg);
  }

  /// Poll a message between two nodes in the network.
  pub fn poll_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) -> Option<NetworkMessage> {
    let queue = self
      .queues
      .get_mut(from_eid)
      .unwrap()
      .get_mut(to_eid)
      .unwrap();
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
  /// `queues`. This function will run the Slave on the `to_eid` end,
  /// running any of it's tablets as necessary, before placing any new
  /// message back into `queues`.
  pub fn run_slave_message(
    &mut self,
    msg: SlaveMessage,
    from_eid: &EndpointId,
    to_eid: &EndpointId,
  ) {
    let mut side_effects = SlaveSideEffects::new();
    self
      .slave_states
      .get_mut(&to_eid)
      .unwrap()
      .handle_incoming_message(&mut side_effects, from_eid, msg);
    for action in side_effects.actions {
      match action {
        SlaveAction::Send { eid, msg } => self.add_msg(msg, &to_eid, &eid),
        SlaveAction::Forward { shape, msg } => {
          self.run_tablet_message(msg, to_eid, &shape);
        }
      }
    }
    return;
  }

  /// This function will run the Tablet at `to_eid` and `shape` before
  /// placing any new message back into `queues`.
  pub fn run_tablet_message(
    &mut self,
    msg: TabletMessage,
    to_eid: &EndpointId,
    shape: &TabletShape,
  ) {
    let mut side_effects = TabletSideEffects::new();
    self
      .tablet_states
      .get_mut(&to_eid)
      .unwrap()
      .get_mut(&shape)
      .unwrap()
      .handle_incoming_message(&mut side_effects, msg);
    for action in side_effects.actions {
      match action {
        TabletAction::Send { eid, msg } => self.add_msg(msg, &to_eid, &eid),
      }
    }
  }

  /// The endpoints provided must exist. This function polls a message from
  /// the message queue between them and delivers to the `to_eid`. If that's
  /// a client, the message is added to `client_msgs_received`, and if it's
  /// a slave, the slave processes the message.
  pub fn deliver_msg(&mut self, from_eid: &EndpointId, to_eid: &EndpointId) {
    if let Some(msg) = self.poll_msg(from_eid, to_eid) {
      if self.slave_states.contains_key(to_eid) {
        match msg {
          NetworkMessage::Slave(slave_msg) => self.run_slave_message(slave_msg, from_eid, to_eid),
          _ => panic!(
            "Endpoint {:?} is a Slave but received a non-SlaveMessage {:?} ",
            to_eid, msg
          ),
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
      if self.nonempty_queues.len() > 0 {
        let r = self.rng.next_u32() as usize % self.nonempty_queues.len();
        let (from_eid, to_eid) = self.nonempty_queues.get(r).unwrap().clone();
        self.poll_msg(&from_eid, &to_eid);
      }
    }
  }

  /// This function simply delivers 1ms worth of messages. For simplicity, we assume
  /// that this means that every non-empty queue of messsages delivers about one
  /// message in this time.
  pub fn simulate1ms(&mut self) {
    // `num_msgs` is about the number of message to deliver for this ms.
    let num_msgs = self.nonempty_queues.len();
    for _ in 0..num_msgs {
      let r = self.rng.next_u32() as usize % self.nonempty_queues.len();
      let (from_eid, to_eid) = self.nonempty_queues.get(r).unwrap().clone();
      self.deliver_msg(&from_eid, &to_eid);
    }
  }

  pub fn simulate_n_ms(&mut self, n: i32) {
    for _ in 0..n {
      self.simulate1ms();
    }
  }

  pub fn simulate_all(&mut self) {
    while self.nonempty_queues.len() > 0 {
      self.simulate1ms();
    }
  }

  pub fn mk_request_id(&mut self) -> RequestId {
    let request_id = RequestId(self.next_int.to_string());
    self.next_int += 1;
    return request_id;
  }
}
