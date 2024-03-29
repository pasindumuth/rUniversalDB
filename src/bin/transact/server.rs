use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  btree_multimap_insert, mk_cid, mk_sid, mk_t, BasicIOCtx, CoreIOCtx, FreeNodeIOCtx,
  GeneralTraceMessage, GossipData, InternalMode, MasterIOCtx, MasterTraceMessage, NodeIOCtx,
  SlaveIOCtx, SlaveTraceMessage, Timestamp,
};
use runiversal::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, SlaveGroupId,
  TabletGroupId,
};
use runiversal::coord::{CoordConfig, CoordContext, CoordForwardMsg, CoordState};
use runiversal::master::{FullMasterInput, MasterTimerInput};
use runiversal::message as msg;
use runiversal::multiversion_map::MVM;
use runiversal::net::{send_msg, SendAction};
use runiversal::node::{GenericInput, GenericTimerInput};
use runiversal::paxos::PaxosConfig;
use runiversal::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveConfig, SlaveContext, SlaveState, SlaveTimerInput,
};
use runiversal::tablet::{
  TabletConfig, TabletContext, TabletForwardMsg, TabletSnapshot, TabletState,
};
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
//  IOCtx
// -----------------------------------------------------------------------------------------------

/// The granularity in which Timer events are executed, in microseconds
pub const TIMER_INCREMENT: u64 = 250;

pub struct ProdIOCtx {
  // Basic
  pub rand: XorShiftRng,
  pub out_conn_map: Arc<Mutex<BTreeMap<EndpointId, Sender<SendAction>>>>,
  pub exited: bool,

  // Constructing and communicating with Tablets
  pub to_top: Sender<GenericInput>,

  // Threads maps
  pub tablet_map: BTreeMap<TabletGroupId, Sender<TabletForwardMsg>>,
  pub coord_map: BTreeMap<CoordGroupId, Sender<CoordForwardMsg>>,

  // Timer Tasks
  pub tasks: Arc<Mutex<BTreeMap<Timestamp, Vec<GenericTimerInput>>>>,
}

impl ProdIOCtx {
  /// Construct a helper thread that will poll all time tasks and push them back
  /// to the top via `to_top`.
  pub fn start(&mut self) {
    let to_top = self.to_top.clone();
    let tasks = self.tasks.clone();
    thread::Builder::new()
      .name(format!("Timer"))
      .spawn(move || loop {
        // Sleep
        let increment = std::time::Duration::from_micros(TIMER_INCREMENT);
        thread::sleep(increment);

        // Poll all tasks from `tasks` prior to the current time, and push them to the Slave.
        let now = mk_t(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis());

        // Process tasks
        let mut tasks = tasks.lock().unwrap();
        while let Some((next_timestamp, _)) = tasks.first_key_value() {
          if next_timestamp <= &now {
            // All data in this first entry should be dispatched.
            let next_timestamp = next_timestamp.clone();
            for timer_input in tasks.remove(&next_timestamp).unwrap() {
              to_top.send(GenericInput::TimerInput(timer_input)).unwrap();
            }
          } else {
            break;
          }
        }
      })
      .unwrap();
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
    send_msg(&self.out_conn_map, eid, SendAction::new(msg, None), &InternalMode::Internal);
  }

  fn general_trace(&mut self, _: GeneralTraceMessage) {}
}

impl FreeNodeIOCtx for ProdIOCtx {
  fn create_tablet_full(
    &mut self,
    gossip: Arc<GossipData>,
    snapshot: TabletSnapshot,
    this_eid: EndpointId,
    tablet_config: TabletConfig,
  ) {
    // Create mpsc queue for Slave-Tablet communication.
    let (to_tablet_sender, to_tablet_receiver) = mpsc::channel::<TabletForwardMsg>();
    self.tablet_map.insert(snapshot.this_tid.clone(), to_tablet_sender);

    // Spawn a new thread and create the Tablet.
    let mut io_ctx = ProdCoreIOCtx {
      out_conn_map: self.out_conn_map.clone(),
      rand: XorShiftRng::from_entropy(),
      to_top: self.to_top.clone(),
    };
    thread::Builder::new()
      .name(format!("TabletGroup {}", snapshot.this_tid.0))
      .spawn(move || {
        let mut tablet = TabletState::create_reconfig(gossip, snapshot, this_eid, tablet_config);
        loop {
          let tablet_msg = to_tablet_receiver.recv().unwrap();
          tablet.handle_input(&mut io_ctx, tablet_msg);
        }
      })
      .unwrap();
  }

  fn create_coord_full(&mut self, mut ctx: CoordContext) {
    // Create mpsc queue for Slave-Coord communication.
    let (to_coord_sender, to_coord_receiver) = mpsc::channel::<CoordForwardMsg>();
    self.coord_map.insert(ctx.this_cid.clone(), to_coord_sender);

    // Spawn a new thread and create the Coord.
    let mut io_ctx = ProdCoreIOCtx {
      out_conn_map: self.out_conn_map.clone(),
      rand: XorShiftRng::from_entropy(),
      to_top: self.to_top.clone(),
    };
    thread::Builder::new()
      .name(format!("CoordGroup {}", ctx.this_cid.0))
      .spawn(move || {
        let mut coord = CoordState::new(ctx);
        loop {
          let coord_msg = to_coord_receiver.recv().unwrap();
          coord.handle_input(&mut io_ctx, coord_msg);
        }
      })
      .unwrap();
  }

  fn defer(&mut self, defer_time: Timestamp, timer_input: GenericTimerInput) {
    let timestamp = self.now().add(defer_time);
    let mut tasks = self.tasks.lock().unwrap();
    if let Some(timer_inputs) = tasks.get_mut(&timestamp) {
      timer_inputs.push(timer_input);
    } else {
      tasks.insert(timestamp.clone(), vec![timer_input]);
    }
  }
}

impl SlaveIOCtx for ProdIOCtx {
  fn mark_exit(&mut self) {
    self.exited = true;
  }

  fn did_exit(&mut self) -> bool {
    self.exited
  }

  fn create_tablet(&mut self, ctx: TabletContext) {
    // Create mpsc queue for Slave-Tablet communication.
    let (to_tablet_sender, to_tablet_receiver) = mpsc::channel::<TabletForwardMsg>();
    self.tablet_map.insert(ctx.this_tid.clone(), to_tablet_sender);

    // Spawn a new thread and create the Tablet.
    let mut io_ctx = ProdCoreIOCtx {
      out_conn_map: self.out_conn_map.clone(),
      rand: XorShiftRng::from_entropy(),
      to_top: self.to_top.clone(),
    };
    thread::Builder::new()
      .name(format!("TabletGroup {}", ctx.this_tid.0))
      .spawn(move || {
        let mut tablet = TabletState::new(ctx);
        loop {
          let tablet_msg = to_tablet_receiver.recv().unwrap();
          tablet.handle_input(&mut io_ctx, tablet_msg);
        }
      })
      .unwrap();
  }

  fn tablet_forward(
    &mut self,
    tablet_group_id: &TabletGroupId,
    forward_msg: TabletForwardMsg,
  ) -> Result<(), TabletForwardMsg> {
    if let Some(tablet) = self.tablet_map.get(tablet_group_id) {
      tablet.send(forward_msg).unwrap();
      Ok(())
    } else {
      Err(forward_msg)
    }
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
    FreeNodeIOCtx::defer(self, defer_time, GenericTimerInput::SlaveTimerInput(timer_input));
  }

  fn trace(&mut self, _: SlaveTraceMessage) {}
}

impl MasterIOCtx for ProdIOCtx {
  fn mark_exit(&mut self) {
    self.exited = true;
  }

  fn did_exit(&mut self) -> bool {
    self.exited
  }

  fn defer(&mut self, defer_time: Timestamp, timer_input: MasterTimerInput) {
    FreeNodeIOCtx::defer(self, defer_time, GenericTimerInput::MasterTimerInput(timer_input));
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
  pub out_conn_map: Arc<Mutex<BTreeMap<EndpointId, Sender<SendAction>>>>,

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
    send_msg(&self.out_conn_map, eid, SendAction::new(msg, None), &InternalMode::Internal);
  }

  fn general_trace(&mut self, _: GeneralTraceMessage) {}
}

impl CoreIOCtx for ProdCoreIOCtx {
  fn slave_forward(&mut self, msg: SlaveBackMessage) {
    self.to_top.send(GenericInput::SlaveBackMessage(msg)).unwrap();
  }
}
