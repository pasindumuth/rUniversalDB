use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  btree_multimap_insert, mk_cid, mk_sid, mk_t, BasicIOCtx, CoreIOCtx, FreeNodeIOCtx,
  GeneralTraceMessage, GossipData, MasterIOCtx, MasterTraceMessage, NodeIOCtx, SlaveIOCtx,
  SlaveTraceMessage, Timestamp,
};
use runiversal::coord::{CoordConfig, CoordContext, CoordForwardMsg, CoordState};
use runiversal::master::{FullMasterInput, MasterTimerInput};
use runiversal::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, SlaveGroupId,
  TabletGroupId,
};
use runiversal::model::message as msg;
use runiversal::multiversion_map::MVM;
use runiversal::net::{recv, send_bytes};
use runiversal::node::GenericInput;
use runiversal::paxos::PaxosConfig;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveConfig, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::tablet::{TabletContext, TabletCreateHelper, TabletForwardMsg, TabletState};
use runiversal::test_utils::mk_seed;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::net::TcpStream;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

// -----------------------------------------------------------------------------------------------
//  Network Helpers
// -----------------------------------------------------------------------------------------------

pub const SERVER_PORT: u32 = 1610;

/// Creates the FromNetwork threads for this new Incoming Connection, `stream`.
pub fn handle_conn(to_server_sender: &Sender<GenericInput>, stream: TcpStream) -> EndpointId {
  let other_ip = EndpointId(stream.peer_addr().unwrap().ip().to_string());

  // Setup FromNetwork Thread
  {
    let to_server_sender = to_server_sender.clone();
    let stream = stream.try_clone().unwrap();
    let other_ip = other_ip.clone();
    thread::spawn(move || loop {
      let data = recv(&stream);
      let network_msg: msg::NetworkMessage = rmp_serde::from_read_ref(&data).unwrap();
      to_server_sender.send(GenericInput::Message(other_ip.clone(), network_msg)).unwrap();
    });
  }

  other_ip
}

/// Creates a thread that acts as both the FromNetwork and ToNetwork Threads,
/// setting up both the Incoming Connection as well as Outgoing Connection at once.
pub fn handle_self_conn(
  this_ip: &EndpointId,
  out_conn_map: &Arc<Mutex<BTreeMap<EndpointId, Sender<Vec<u8>>>>>,
  to_server_sender: &Sender<GenericInput>,
) {
  let mut out_conn_map = out_conn_map.lock().unwrap();
  let (sender, receiver) = mpsc::channel();
  out_conn_map.insert(this_ip.clone(), sender);

  // Setup Self Connection Thread
  let to_server_sender = to_server_sender.clone();
  let this_ip = this_ip.clone();
  thread::spawn(move || loop {
    let data = receiver.recv().unwrap();
    let network_msg: msg::NetworkMessage = rmp_serde::from_read_ref(&data).unwrap();
    to_server_sender.send(GenericInput::Message(this_ip.clone(), network_msg)).unwrap();
  });
}

/// Send `msg` to the given `eid`. Note that it must be present in `out_conn_map`.
pub fn send_msg(
  out_conn_map: &Arc<Mutex<BTreeMap<EndpointId, Sender<Vec<u8>>>>>,
  eid: &EndpointId,
  msg: msg::NetworkMessage,
) {
  let mut out_conn_map = out_conn_map.lock().unwrap();

  // If there is not an out-going connection to `eid`, then make one.
  if !out_conn_map.contains_key(eid) {
    // We create the ToNetwork thread.
    let (sender, receiver) = mpsc::channel();
    out_conn_map.insert(eid.clone(), sender);
    let EndpointId(ip) = eid.clone();
    thread::spawn(move || {
      let stream = TcpStream::connect(format!("{}:{}", ip, SERVER_PORT)).unwrap();
      loop {
        let data_out = receiver.recv().unwrap();
        send_bytes(&data_out, &stream);
      }
    });
  }

  // Send the `msg` to the ToNetwork thread.
  let sender = out_conn_map.get(eid).unwrap();
  sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
}

// -----------------------------------------------------------------------------------------------
//  IOCtx
// -----------------------------------------------------------------------------------------------

/// The granularity in which Timer events are executed, in microseconds
pub const TIMER_INCREMENT: u64 = 250;

pub struct ProdIOCtx {
  // Basic
  pub rand: XorShiftRng,
  pub out_conn_map: Arc<Mutex<BTreeMap<EndpointId, Sender<Vec<u8>>>>>,
  pub exited: bool,

  // Constructing and communicating with Tablets
  pub to_top: Sender<GenericInput>,

  // Threads maps
  pub tablet_map: BTreeMap<TabletGroupId, Sender<TabletForwardMsg>>,
  pub coord_map: BTreeMap<CoordGroupId, Sender<CoordForwardMsg>>,

  // Timer Tasks
  pub slave_tasks: Arc<Mutex<BTreeMap<Timestamp, Vec<SlaveTimerInput>>>>,
  pub master_tasks: Arc<Mutex<BTreeMap<Timestamp, Vec<MasterTimerInput>>>>,
}

impl ProdIOCtx {
  /// Construct a helper thread that will poll all time tasks and push them back
  /// to the top via `to_top`.
  pub fn start(&mut self) {
    let to_top = self.to_top.clone();
    let master_tasks = self.master_tasks.clone();
    let slave_tasks = self.slave_tasks.clone();
    thread::spawn(move || loop {
      // Sleep
      let increment = std::time::Duration::from_micros(TIMER_INCREMENT);
      thread::sleep(increment);

      // Poll all tasks from `tasks` prior to the current time, and push them to the Slave.
      let now = mk_t(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());

      // Process master tasks
      let mut master_tasks = master_tasks.lock().unwrap();
      while let Some((next_timestamp, _)) = master_tasks.first_key_value() {
        if next_timestamp <= &now {
          // All data in this first entry should be dispatched.
          let next_timestamp = next_timestamp.clone();
          for timer_input in master_tasks.remove(&next_timestamp).unwrap() {
            to_top.send(GenericInput::MasterTimerInput(timer_input));
          }
        }
      }

      // Process slave tasks
      let mut slave_tasks = slave_tasks.lock().unwrap();
      while let Some((next_timestamp, _)) = slave_tasks.first_key_value() {
        if next_timestamp <= &now {
          // All data in this first entry should be dispatched.
          let next_timestamp = next_timestamp.clone();
          for timer_input in slave_tasks.remove(&next_timestamp).unwrap() {
            to_top.send(GenericInput::SlaveTimerInput(timer_input));
          }
        }
      }
    });
  }
}

impl BasicIOCtx for ProdIOCtx {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> Timestamp {
    mk_t(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis())
  }

  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage) {
    send_msg(&self.out_conn_map, eid, msg);
  }

  fn mark_exit(&mut self) {
    self.exited = true;
  }

  fn did_exit(&mut self) -> bool {
    self.exited
  }

  fn general_trace(&mut self, _: GeneralTraceMessage) {}
}

impl FreeNodeIOCtx for ProdIOCtx {
  fn create_tablet_full(&mut self, mut ctx: TabletContext) {
    // Create mpsc queue for Slave-Tablet communication.
    let (to_tablet_sender, to_tablet_receiver) = mpsc::channel::<TabletForwardMsg>();
    self.tablet_map.insert(ctx.this_tid.clone(), to_tablet_sender);

    // Spawn a new thread and create the Tablet.
    let mut io_ctx = ProdCoreIOCtx {
      out_conn_map: self.out_conn_map.clone(),
      exited: false,
      rand: XorShiftRng::from_entropy(),
      to_top: self.to_top.clone(),
    };
    thread::spawn(move || {
      let mut tablet = TabletState::new(ctx);
      loop {
        let tablet_msg = to_tablet_receiver.recv().unwrap();
        tablet.handle_input(&mut io_ctx, tablet_msg);
      }
    });
  }

  fn create_coord_full(&mut self, mut ctx: CoordContext) {
    // Create mpsc queue for Slave-Coord communication.
    let (to_coord_sender, to_coord_receiver) = mpsc::channel::<CoordForwardMsg>();
    self.coord_map.insert(ctx.this_cid.clone(), to_coord_sender);

    // Spawn a new thread and create the Coord.
    let mut io_ctx = ProdCoreIOCtx {
      out_conn_map: self.out_conn_map.clone(),
      exited: false,
      rand: XorShiftRng::from_entropy(),
      to_top: self.to_top.clone(),
    };
    thread::spawn(move || {
      let mut coord = CoordState::new(ctx);
      loop {
        let coord_msg = to_coord_receiver.recv().unwrap();
        coord.handle_input(&mut io_ctx, coord_msg);
      }
    });
  }
}

impl SlaveIOCtx for ProdIOCtx {
  fn create_tablet(&mut self, helper: TabletCreateHelper) {
    // Create an RNG using the random seed provided by the Slave.
    let rand = XorShiftRng::from_seed(helper.rand_seed);

    // Create mpsc queue for Slave-Tablet communication.
    let (to_tablet_sender, to_tablet_receiver) = mpsc::channel::<TabletForwardMsg>();
    self.tablet_map.insert(helper.this_tid.clone(), to_tablet_sender);

    // Spawn a new thread and create the Tablet.
    let tablet_context = TabletContext::new(helper);
    let mut io_ctx = ProdCoreIOCtx {
      out_conn_map: self.out_conn_map.clone(),
      exited: false,
      rand,
      to_top: self.to_top.clone(),
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

  fn defer(&mut self, defer_time: Timestamp, timer_input: SlaveTimerInput) {
    let timestamp = self.now().add(defer_time);
    let mut slave_tasks = self.slave_tasks.lock().unwrap();
    if let Some(timer_inputs) = slave_tasks.get_mut(&timestamp) {
      timer_inputs.push(timer_input);
    } else {
      slave_tasks.insert(timestamp.clone(), vec![timer_input]);
    }
  }

  fn trace(&mut self, _: SlaveTraceMessage) {}
}

impl MasterIOCtx for ProdIOCtx {
  fn defer(&mut self, defer_time: Timestamp, timer_input: MasterTimerInput) {
    let timestamp = self.now().add(defer_time);
    let mut master_tasks = self.master_tasks.lock().unwrap();
    if let Some(timer_inputs) = master_tasks.get_mut(&timestamp) {
      timer_inputs.push(timer_input);
    } else {
      master_tasks.insert(timestamp.clone(), vec![timer_input]);
    }
  }

  fn trace(&mut self, _: MasterTraceMessage) {}
}

impl NodeIOCtx for ProdIOCtx {}

impl Debug for ProdIOCtx {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut debug_trait_builder = f.debug_struct("ProdIOCtx");
    debug_trait_builder.finish()
  }
}

// -----------------------------------------------------------------------------------------------
//  ProdCoreIOCtx
// -----------------------------------------------------------------------------------------------

pub struct ProdCoreIOCtx {
  // Basic
  pub rand: XorShiftRng,
  pub out_conn_map: Arc<Mutex<BTreeMap<EndpointId, Sender<Vec<u8>>>>>,
  pub exited: bool,

  // Slave
  pub to_top: Sender<GenericInput>,
}

impl BasicIOCtx for ProdCoreIOCtx {
  type RngCoreT = XorShiftRng;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    &mut self.rand
  }

  fn now(&mut self) -> Timestamp {
    mk_t(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis())
  }

  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage) {
    let out_conn_map = self.out_conn_map.lock().unwrap();
    let sender = out_conn_map.get(eid).unwrap();
    sender.send(rmp_serde::to_vec(&msg).unwrap()).unwrap();
  }

  fn mark_exit(&mut self) {
    self.exited = true;
  }

  fn did_exit(&mut self) -> bool {
    self.exited
  }

  fn general_trace(&mut self, _: GeneralTraceMessage) {}
}

impl CoreIOCtx for ProdCoreIOCtx {
  fn slave_forward(&mut self, msg: SlaveBackMessage) {
    self.to_top.send(GenericInput::SlaveBackMessage(msg));
  }
}
