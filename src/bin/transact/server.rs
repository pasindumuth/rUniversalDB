use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  btree_multimap_insert, mk_cid, mk_sid, BasicIOCtx, CoreIOCtx, GossipData, SlaveIOCtx,
};
use runiversal::coord::{CoordContext, CoordForwardMsg, CoordState};
use runiversal::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, SlaveGroupId, TabletGroupId, Timestamp,
};
use runiversal::model::message::NetworkMessage;
use runiversal::multiversion_map::MVM;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::tablet::{TabletContext, TabletCreateHelper, TabletForwardMsg, TabletState};
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

// -----------------------------------------------------------------------------------------------
//  ProdSlaveIOCtx
// -----------------------------------------------------------------------------------------------

/// The granularity in which Timer events are executed, in microseconds
const TIMER_INCREMENT: u64 = 250;

pub struct ProdSlaveIOCtx {
  // Basic
  rand: XorShiftRng,
  net_conn_map: Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,

  // Constructing and communicating with Tablets
  to_slave: Sender<FullSlaveInput>,
  tablet_map: HashMap<TabletGroupId, Sender<TabletForwardMsg>>,

  // Coord
  coord_map: HashMap<CoordGroupId, Sender<CoordForwardMsg>>,

  // Deferred timer tasks
  tasks: Arc<Mutex<BTreeMap<Timestamp, Vec<SlaveTimerInput>>>>,
}

impl ProdSlaveIOCtx {
  /// Construct a helper thread that will poll `SlaveTimerInput` from `tasks` and push
  /// them back to the Slave via `to_slave`.
  fn start(&mut self) {
    let to_slave = self.to_slave.clone();
    let tasks = self.tasks.clone();
    thread::spawn(move || loop {
      // Sleep
      let increment = std::time::Duration::from_micros(TIMER_INCREMENT);
      thread::sleep(increment);

      // Poll all tasks from `tasks` prior to the current time, and push them to the Slave.
      let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
      let mut tasks = tasks.lock().unwrap();
      while let Some((next_timestamp, _)) = tasks.first_key_value() {
        if next_timestamp <= &now {
          // All data in this first entry should be dispatched.
          let next_timestamp = next_timestamp.clone();
          for timer_input in tasks.remove(&next_timestamp).unwrap() {
            to_slave.send(FullSlaveInput::SlaveTimerInput(timer_input));
          }
        }
      }
    });
  }
}

impl BasicIOCtx for ProdSlaveIOCtx {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
  }

  fn send(&mut self, eid: &EndpointId, msg: NetworkMessage) {
    let net_conn_map = self.net_conn_map.lock().unwrap();
    let sender = net_conn_map.get(eid).unwrap();
    sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
  }
}

impl SlaveIOCtx for ProdSlaveIOCtx {
  fn create_tablet(&mut self, helper: TabletCreateHelper) {
    // Create an RNG using the random seed provided by the Slave.
    let rand = XorShiftRng::from_seed(helper.rand_seed);

    // Create mpsc queue for Slave-Tablet communication.
    let (to_tablet_sender, to_tablet_receiver) = mpsc::channel::<TabletForwardMsg>();
    self.tablet_map.insert(helper.this_tablet_group_id.clone(), to_tablet_sender);

    // Spawn a new thread and create the Tablet.
    let tablet_context = TabletContext::new(helper);
    let mut io_ctx = ProdCoreIOCtx {
      net_conn_map: self.net_conn_map.clone(),
      rand,
      to_slave: self.to_slave.clone(),
    };
    thread::spawn(move || {
      let mut tablet = TabletState::new(tablet_context);
      loop {
        let tablet_msg = to_tablet_receiver.recv().unwrap();
        tablet.handle_input(&mut io_ctx, tablet_msg);
      }
    });
  }

  fn tablet_forward(&mut self, tablet_group_id: &TabletGroupId, msg: TabletForwardMsg) {
    self.tablet_map.get(tablet_group_id).unwrap().send(msg).unwrap();
  }

  fn all_tids(&self) -> Vec<TabletGroupId> {
    self.tablet_map.keys().cloned().collect()
  }

  fn num_tablets(&self) -> usize {
    self.tablet_map.keys().len()
  }

  fn coord_forward(&mut self, coord_group_id: &CoordGroupId, msg: CoordForwardMsg) {
    self.coord_map.get(coord_group_id).unwrap().send(msg).unwrap();
  }

  fn all_cids(&self) -> Vec<CoordGroupId> {
    self.coord_map.keys().cloned().collect()
  }

  fn defer(&mut self, defer_time: u128, timer_input: SlaveTimerInput) {
    let timestamp = self.now() + defer_time;
    let mut tasks = self.tasks.lock().unwrap();
    if let Some(timer_inputs) = tasks.get_mut(&timestamp) {
      timer_inputs.push(timer_input);
    } else {
      tasks.insert(timestamp.clone(), vec![timer_input]);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdCoreIOCtx
// -----------------------------------------------------------------------------------------------

pub struct ProdCoreIOCtx {
  // Basic
  rand: XorShiftRng,
  net_conn_map: Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,

  // Slave
  to_slave: Sender<FullSlaveInput>,
}

impl BasicIOCtx for ProdCoreIOCtx {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
  }

  fn send(&mut self, eid: &EndpointId, msg: NetworkMessage) {
    let net_conn_map = self.net_conn_map.lock().unwrap();
    let sender = net_conn_map.get(eid).unwrap();
    sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
  }
}

impl CoreIOCtx for ProdCoreIOCtx {
  fn slave_forward(&mut self, msg: SlaveBackMessage) {
    self.to_slave.send(FullSlaveInput::SlaveBackMessage(msg));
  }
}

// -----------------------------------------------------------------------------------------------
//  SlaveStarter
// -----------------------------------------------------------------------------------------------

const NUM_COORDS: u32 = 3;

/// This initializes a Slave for a system that is bootstrapping. All Slave and Master
/// nodes should already be constrcuted and network connections should already be
/// established before this function is called.
pub fn start_server(
  to_server_sender: Sender<FullSlaveInput>,
  to_server_receiver: Receiver<FullSlaveInput>,
  net_conn_map: &Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
  this_eid: EndpointId,
  this_sid: SlaveGroupId,
  slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,
  master_address_config: Vec<EndpointId>,
) {
  // Create Slave RNG.
  let mut rand = XorShiftRng::from_entropy();

  // Create common Gossip
  let gossip = Arc::new(GossipData {
    gen: Gen(0),
    db_schema: Default::default(),
    table_generation: MVM::new(),
    sharding_config: Default::default(),
    tablet_address_config: Default::default(),
    slave_address_config: slave_address_config.clone(),
    master_address_config: master_address_config.clone(),
  });

  // Construct LeaderMap
  let mut leader_map = HashMap::<PaxosGroupId, LeadershipId>::new();
  leader_map.insert(
    PaxosGroupId::Master,
    LeadershipId { gen: Gen(0), eid: master_address_config[0].clone() },
  );
  for (sid, eids) in &gossip.slave_address_config {
    leader_map.insert(sid.to_gid(), LeadershipId { gen: Gen(0), eid: eids[0].clone() });
  }

  // Create the Coord
  let mut coord_map = HashMap::<CoordGroupId, Sender<CoordForwardMsg>>::new();
  let mut coord_positions: Vec<CoordGroupId> = Vec::new();
  for _ in 0..NUM_COORDS {
    let coord_group_id = mk_cid(&mut rand);
    coord_positions.push(coord_group_id.clone());
    // Create the seed for the Tablet's RNG. We use the Slave's
    // RNG to create a random seed.
    let mut seed = [0; 16];
    rand.fill_bytes(&mut seed);
    let rand = XorShiftRng::from_seed(seed);

    // Create mpsc queue for Slave-Coord communication.
    let (to_coord_sender, to_coord_receiver) = mpsc::channel();
    coord_map.insert(coord_group_id.clone(), to_coord_sender);

    // Create the Tablet
    let coord_context = CoordContext::new(
      this_sid.clone(),
      coord_group_id,
      this_eid.clone(),
      gossip.clone(),
      leader_map.clone(),
    );
    let mut io_ctx = ProdCoreIOCtx {
      net_conn_map: net_conn_map.clone(),
      rand,
      to_slave: to_server_sender.clone(),
    };
    thread::spawn(move || {
      let mut coord = CoordState::new(coord_context);
      loop {
        let coord_msg = to_coord_receiver.recv().unwrap();
        coord.handle_input(&mut io_ctx, coord_msg);
      }
    });
  }

  // Construct the SlaveState
  let mut io_ctx = ProdSlaveIOCtx {
    rand,
    net_conn_map: net_conn_map.clone(),
    to_slave: to_server_sender.clone(),
    tablet_map: Default::default(),
    coord_map,
    tasks: Arc::new(Mutex::new(Default::default())),
  };
  io_ctx.start();
  let slave_context = SlaveContext::new(coord_positions, this_sid, this_eid, gossip, leader_map);
  let mut slave = SlaveState::new(slave_context);
  loop {
    // Receive data from the `to_server_receiver` and update the SlaveState accordingly.
    // This is the steady state that the slaves enters.
    let full_input = to_server_receiver.recv().unwrap();
    slave.handle_input(&mut io_ctx, full_input);
  }
}
