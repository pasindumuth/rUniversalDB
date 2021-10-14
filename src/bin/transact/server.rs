use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  btree_multimap_insert, mk_cid, mk_sid, Clock, CoordForwardOut, GossipData, IOTypes,
  MasterTimerOut, NetworkOut, SlaveForwardOut, SlaveTimerOut, TabletForwardOut,
};
use runiversal::coord::{CoordContext, CoordForwardMsg, CoordState};
use runiversal::master::MasterTimerInput;
use runiversal::model::common::{
  CTSubNodePath, CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, SlaveGroupId,
  TabletGroupId, Timestamp,
};
use runiversal::model::message as msg;
use runiversal::multiversion_map::MVM;
use runiversal::network_driver::NetworkDriver;
use runiversal::paxos::PaxosDriver;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::tablet::{TabletContext, TabletCreateHelper, TabletForwardMsg, TabletState};
use std::collections::{BTreeMap, BTreeSet, Bound, HashMap};
use std::fmt::{Debug, Formatter, Result};
use std::ops::{Deref, DerefMut};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

// -----------------------------------------------------------------------------------------------
//  ProdClock
// -----------------------------------------------------------------------------------------------

/// A lock that simply wraps the system clock.
#[derive(Clone)]
struct ProdClock {}

impl Clock for ProdClock {
  fn now(&mut self) -> Timestamp {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()
  }
}

impl Debug for ProdClock {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "ProdClock")
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdNetworkOut
// -----------------------------------------------------------------------------------------------

/// An interface for real network interaction, `NetworkMessages`
/// are pass through to the ToNetwork Threads via the FromServer Queue.
/// This struct must be cloneable to easily utilize the Arc underneath.
#[derive(Clone)]
struct ProdNetworkOut {
  net_conn_map: Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
}

impl NetworkOut for ProdNetworkOut {
  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage) {
    let net_conn_map = self.net_conn_map.lock().unwrap();
    let sender = net_conn_map.get(eid).unwrap();
    sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
  }
}

impl Debug for ProdNetworkOut {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "ProdNetworkOut")
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdSlaveForwardOut
// -----------------------------------------------------------------------------------------------

/// Interface for sending data from a Tablet to the Slave.
#[derive(Clone)]
struct ProdSlaveForwardOut {
  slave_out: Sender<FullSlaveInput>,
}

impl SlaveForwardOut for ProdSlaveForwardOut {
  fn forward(&mut self, back_msg: SlaveBackMessage) {
    self.slave_out.send(FullSlaveInput::SlaveBackMessage(back_msg));
  }
}

impl Debug for ProdSlaveForwardOut {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "ProdSlaveForwardOut")
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdTabletForwardOut
// -----------------------------------------------------------------------------------------------

/// A simple interface for pushing messages from the Slave to the Tablets.
struct ProdTabletForwardOut {
  tablet_map: HashMap<TabletGroupId, Sender<TabletForwardMsg>>,

  // Fields for creating Tablets
  network_output: ProdNetworkOut,
  slave_forward_out: ProdSlaveForwardOut,
}

impl TabletForwardOut for ProdTabletForwardOut {
  fn forward(&mut self, tablet_group_id: &TabletGroupId, msg: TabletForwardMsg) {
    self.tablet_map.get(tablet_group_id).unwrap().send(msg).unwrap();
  }

  fn all_tids(&self) -> Vec<TabletGroupId> {
    self.tablet_map.keys().cloned().collect()
  }

  fn num_tablets(&self) -> usize {
    self.tablet_map.keys().len()
  }

  fn create_tablet(&mut self, helper: TabletCreateHelper) {
    // Create an RNG using the random seed provided by the Slave.
    let rand = XorShiftRng::from_seed(helper.rand_seed);

    // Create mpsc queue for Slave-Tablet communication.
    let (to_tablet_sender, to_tablet_receiver) = mpsc::channel::<TabletForwardMsg>();
    self.tablet_map.insert(helper.this_tablet_group_id.clone(), to_tablet_sender);

    // Spawn a new thread and create the Tablet.
    let tablet_context = TabletContext::new(
      rand,
      ProdClock {},
      self.network_output.clone(),
      self.slave_forward_out.clone(),
      helper,
    );
    thread::spawn(move || {
      let mut tablet = TabletState::<ProdIOTypes>::new2(tablet_context);
      loop {
        let tablet_msg = to_tablet_receiver.recv().unwrap();
        tablet.handle_input(tablet_msg);
      }
    });
  }
}

impl Debug for ProdTabletForwardOut {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "ProdTabletForwardOut")
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdCoordForwardOut
// -----------------------------------------------------------------------------------------------

struct ProdCoordForwardOut {
  coord_map: HashMap<CoordGroupId, Sender<CoordForwardMsg>>,
}

impl CoordForwardOut for ProdCoordForwardOut {
  fn forward(&mut self, coord_group_id: &CoordGroupId, msg: CoordForwardMsg) {
    self.coord_map.get(coord_group_id).unwrap().send(msg).unwrap();
  }

  fn all_cids(&self) -> Vec<CoordGroupId> {
    self.coord_map.keys().cloned().collect()
  }
}

impl Debug for ProdCoordForwardOut {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "ProdCoordForwardOut")
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdSlaveTimerOut
// -----------------------------------------------------------------------------------------------

/// The granularity in which Timer events are executed, in microseconds
const TIMER_INCREMENT: u64 = 250;

struct ProdSlaveTimerOut {
  /// A `Sender` to slave `FullSlaveInput` to the Slave with.
  slave_out: Sender<FullSlaveInput>,
  clock: ProdClock,
  /// Holds the `SlaveTimerInput`s that should send to the Slave, indexed by the
  /// time which they should be sent.
  tasks: Arc<Mutex<BTreeMap<Timestamp, Vec<SlaveTimerInput>>>>,
}

impl ProdSlaveTimerOut {
  fn new(slave_out: Sender<FullSlaveInput>) -> ProdSlaveTimerOut {
    let mut timer = ProdSlaveTimerOut { slave_out, clock: ProdClock {}, tasks: Default::default() };
    timer.start();
    timer
  }

  /// Construct a helper thread that will push
  fn start(&mut self) {
    let slave_out = self.slave_out.clone();
    let mut clock = self.clock.clone();
    let tasks = self.tasks.clone();
    thread::spawn(move || loop {
      // Sleep
      let increment = std::time::Duration::from_micros(TIMER_INCREMENT);
      thread::sleep(increment);

      // Poll all tasks from `tasks` prior to the current time, and push them to the Slave.
      let now = clock.now();
      let mut tasks = tasks.lock().unwrap();
      while let Some((next_timestamp, _)) = tasks.first_key_value() {
        if next_timestamp <= &now {
          // All data in this first entry should be dispatched.
          let next_timestamp = next_timestamp.clone();
          for timer_input in tasks.remove(&next_timestamp).unwrap() {
            slave_out.send(FullSlaveInput::SlaveTimerInput(timer_input));
          }
        }
      }
    });
  }
}

impl SlaveTimerOut for ProdSlaveTimerOut {
  fn defer(&mut self, defer_time: Timestamp, timer_input: SlaveTimerInput) {
    let timestamp = self.clock.now() + defer_time;
    let mut tasks = self.tasks.lock().unwrap();
    if let Some(timer_inputs) = tasks.get_mut(&timestamp) {
      timer_inputs.push(timer_input);
    } else {
      tasks.insert(timestamp.clone(), vec![timer_input]);
    }
  }
}

impl Debug for ProdSlaveTimerOut {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "ProdSlaveTimerOut")
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdMasterTimerOut
// -----------------------------------------------------------------------------------------------

struct ProdMasterTimerOut {}

impl MasterTimerOut for ProdMasterTimerOut {
  fn defer(&mut self, defer_time: Timestamp, msg: MasterTimerInput) {
    panic!() // TODO: do this right
  }
}

impl Debug for ProdMasterTimerOut {
  fn fmt(&self, f: &mut Formatter) -> Result {
    write!(f, "ProdMasterTimerOut")
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdIOTypes
// -----------------------------------------------------------------------------------------------

struct ProdIOTypes {}

impl IOTypes for ProdIOTypes {
  type RngCoreT = XorShiftRng;
  type ClockT = ProdClock;
  type NetworkOutT = ProdNetworkOut;
  type TabletForwardOutT = ProdTabletForwardOut;
  type CoordForwardOutT = ProdCoordForwardOut;
  type SlaveForwardOutT = ProdSlaveForwardOut;
  type SlaveTimerOutT = ProdSlaveTimerOut;
  type MasterTimerOutT = ProdMasterTimerOut;
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

  // Create the Tablets
  let network_output = ProdNetworkOut { net_conn_map: net_conn_map.clone() };
  let tablet_forward_output = ProdTabletForwardOut {
    tablet_map: HashMap::<TabletGroupId, Sender<TabletForwardMsg>>::new(),
    network_output: network_output.clone(),
    slave_forward_out: ProdSlaveForwardOut { slave_out: to_server_sender.clone() },
  };

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
    let gossip = gossip.clone();
    let mut coord_context = CoordContext::<ProdIOTypes>::new(
      rand,
      ProdClock {},
      network_output.clone(),
      this_sid.clone(),
      coord_group_id,
      this_eid.clone(),
      gossip,
      leader_map.clone(),
    );
    thread::spawn(move || {
      let mut coord = CoordState::<ProdIOTypes>::new(coord_context);
      loop {
        let coord_msg = to_coord_receiver.recv().unwrap();
        coord.handle_input(coord_msg);
      }
    });
  }

  // Construct the SlaveState
  let slave_context = SlaveContext::<ProdIOTypes>::new(
    rand,
    ProdClock {},
    network_output,
    tablet_forward_output,
    ProdCoordForwardOut { coord_map },
    ProdSlaveTimerOut::new(to_server_sender.clone()),
    coord_positions,
    this_sid,
    this_eid,
    gossip,
    leader_map,
  );
  let mut slave = SlaveState::new2(slave_context);
  loop {
    // Receive data from the `to_server_receiver` and update the SlaveState accordingly.
    // This is the steady state that the slaves enters.
    let full_input = to_server_receiver.recv().unwrap();
    slave.handle_full_input(full_input);
  }
}
