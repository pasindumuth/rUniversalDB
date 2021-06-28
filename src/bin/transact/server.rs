use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{Clock, GossipData, IOTypes, NetworkOut, TabletForwardOut};
use runiversal::model::common::{EndpointId, Gen, SlaveGroupId, TabletGroupId, Timestamp};
use runiversal::model::message::{NetworkMessage, SlaveMessage, TabletMessage};
use runiversal::slave::SlaveState;
use runiversal::tablet::TabletState;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
struct ProdClock {}

impl Clock for ProdClock {
  fn now(&mut self) -> Timestamp {
    Timestamp(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis())
  }
}

/// An interface for real network interaction, `NetworkMessages`
/// are pass through to the ToNetwork Threads via the FromServer Queue.
/// This struct must be cloneable to easily utilize the Arc underneath.
#[derive(Clone)]
struct ProdNetworkOut {
  net_conn_map: Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
}

impl NetworkOut for ProdNetworkOut {
  fn send(&mut self, eid: &EndpointId, msg: NetworkMessage) {
    let net_conn_map = self.net_conn_map.lock().unwrap();
    let sender = net_conn_map.get(eid).unwrap();
    sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
  }
}

/// A simple interface for pushing messages from the Slave to
/// the Tablets.
struct ProdTabletForwardOut {
  tablet_map: HashMap<TabletGroupId, Sender<TabletMessage>>,
}

impl TabletForwardOut for ProdTabletForwardOut {
  fn forward(&mut self, tablet_group_id: &TabletGroupId, msg: TabletMessage) {
    self.tablet_map.get(tablet_group_id).unwrap().send(msg).unwrap();
  }
}

struct ProdIOTypes {}

impl IOTypes for ProdIOTypes {
  type RngCoreT = XorShiftRng;
  type ClockT = ProdClock;
  type NetworkOutT = ProdNetworkOut;
  type TabletForwardOutT = ProdTabletForwardOut;
}

pub fn start_server(
  to_server_receiver: Receiver<(EndpointId, Vec<u8>)>,
  net_conn_map: &Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
  slave_index: u32,
) {
  // Create Slave RNG. We create the seed that this Slave uses for random number
  // generation. It's 16 bytes long, so we do (16 * slave_index + i) to make sure
  // every element of the seed is different across all slaves.
  let mut seed = [0; 16];
  for i in 0..16 {
    seed[i] = (16 * slave_index + i as u32) as u8;
  }
  let mut rand = XorShiftRng::from_seed(seed);

  // Create the network output interface, used by both the Slave and the Tablets.
  let network_output = ProdNetworkOut { net_conn_map: net_conn_map.clone() };
  let clock = ProdClock {};

  // Create common Gossip
  let gossip = Arc::new(GossipData { gossiped_db_schema: Default::default(), gossip_gen: Gen(0) });

  // Create the Tablets
  let mut tablet_map = HashMap::<TabletGroupId, Sender<TabletMessage>>::new();
  for tablet_group_id in vec!["t1", "t2", "t3"] {
    // Create the seed for the Tablet's RNG. We use the Slave's
    // RNG to create a random seed.
    let mut seed = [0; 16];
    rand.fill_bytes(&mut seed);
    let rand = XorShiftRng::from_seed(seed);

    // Create mpsc queue for Slave-Tablet communication.
    let (to_tablet_sender, to_tablet_receiver) = mpsc::channel();
    tablet_map.insert(TabletGroupId(tablet_group_id.to_string()), to_tablet_sender);

    // Create the Tablet
    let clock = clock.clone();
    let network_output = network_output.clone();
    let gossip = gossip.clone();
    thread::spawn(move || {
      let mut tablet = TabletState::<ProdIOTypes>::new(
        rand,
        clock,
        network_output,
        gossip,
        Default::default(),
        Default::default(),
        Default::default(),
        SlaveGroupId("".to_string()),
        TabletGroupId("".to_string()),
        EndpointId("".to_string()),
      );
      loop {
        let tablet_msg = to_tablet_receiver.recv().unwrap();
        tablet.handle_incoming_message(tablet_msg);
      }
    });
  }

  // Construct the SlaveState
  let mut slave = SlaveState::<ProdIOTypes>::new(
    rand,
    clock,
    network_output,
    ProdTabletForwardOut { tablet_map },
    gossip.clone(),
    Default::default(),
    Default::default(),
    Default::default(),
    SlaveGroupId("".to_string()),
    EndpointId("".to_string()),
  );
  loop {
    // Receive data from the `to_server_receiver` and update the SlaveState accordingly.
    // This is the steady state that the slaves enters.
    let (_, data) = to_server_receiver.recv().unwrap();
    let slave_msg: SlaveMessage = rmp_serde::from_read_ref(&data).unwrap();
    slave.handle_incoming_message(slave_msg);
  }
}
