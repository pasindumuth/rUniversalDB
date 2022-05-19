use crate::common::{
  mk_t, FreeNodeIOCtx, GossipDataView, MasterIOCtx, NodeIOCtx, SlaveIOCtx, VersionedValue,
};
use crate::common::{CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait};
use crate::coord::{CoordConfig, CoordContext};
use crate::master::{FullMasterInput, MasterConfig, MasterContext, MasterState, MasterTimerInput};
use crate::message as msg;
use crate::net::GenericInputTrait;
use crate::paxos::PaxosConfig;
use crate::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveConfig, SlaveContext, SlaveState, SlaveTimerInput,
};
use crate::tablet::TabletConfig;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[path = "test/node_test.rs"]
pub mod node_test;

const SERVER_PORT: u32 = 1610;

// -----------------------------------------------------------------------------------------------
//  GenericInput
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum GenericTimerInput {
  FreeNodeHeartbeat,
  SlaveTimerInput(SlaveTimerInput),
  MasterTimerInput(MasterTimerInput),
}

#[derive(Debug)]
pub enum GenericInput {
  Message(EndpointId, msg::NetworkMessage),
  SlaveBackMessage(SlaveBackMessage),
  TimerInput(GenericTimerInput),
}

impl GenericInputTrait for GenericInput {
  fn from_network(eid: EndpointId, message: msg::NetworkMessage) -> GenericInput {
    GenericInput::Message(eid, message)
  }
}

// -----------------------------------------------------------------------------------------------
//  Buffer Helpers
// -----------------------------------------------------------------------------------------------

/// Used to add elements form a BTree MultiMap
pub fn amend_buffer<MessageT>(
  buffered_messages: &mut BTreeMap<EndpointId, Vec<MessageT>>,
  eid: &EndpointId,
  message: MessageT,
) {
  if let Some(buffer) = buffered_messages.get_mut(eid) {
    buffer.push(message);
  } else {
    buffered_messages.insert(eid.clone(), vec![message]);
  }
}

// -----------------------------------------------------------------------------------------------
//  MessageTrait
// -----------------------------------------------------------------------------------------------

trait MessageTrait {
  fn is_tier_1(&self) -> bool;

  fn is_external(&self) -> bool;
}

/// `Master` implementation.
impl MessageTrait for msg::MasterMessage {
  fn is_tier_1(&self) -> bool {
    msg::MasterMessage::is_tier_1(self)
  }

  fn is_external(&self) -> bool {
    if let msg::MasterMessage::MasterExternalReq(_) = self {
      true
    } else {
      false
    }
  }
}

/// `Slave` implementation.
impl MessageTrait for msg::SlaveMessage {
  fn is_tier_1(&self) -> bool {
    msg::SlaveMessage::is_tier_1(self)
  }

  fn is_external(&self) -> bool {
    if let msg::SlaveMessage::SlaveExternalReq(_) = self {
      true
    } else {
      false
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  InnerStateTrait
// -----------------------------------------------------------------------------------------------

trait InnerStateTrait {
  type MessageT: MessageTrait;

  fn get_eids(&self) -> &VersionedValue<BTreeSet<EndpointId>>;

  fn handle_input(&mut self, message: Self::MessageT);
}

/// `Master` implementation.
struct MasterInnerState<'a, IO> {
  io_ctx: &'a mut IO,
  state: &'a mut MasterState,
}

impl<'a, IO: MasterIOCtx> InnerStateTrait for MasterInnerState<'a, IO> {
  type MessageT = msg::MasterMessage;

  fn get_eids(&self) -> &VersionedValue<BTreeSet<EndpointId>> {
    self.state.get_eids()
  }

  fn handle_input(&mut self, message: Self::MessageT) {
    self.state.handle_input(self.io_ctx, FullMasterInput::MasterMessage(message));
  }
}

/// `Slave` implementation.
struct SlaveInnerState<'a, IO> {
  io_ctx: &'a mut IO,
  state: &'a mut SlaveState,
}

impl<'a, IO: SlaveIOCtx> InnerStateTrait for SlaveInnerState<'a, IO> {
  type MessageT = msg::SlaveMessage;

  fn get_eids(&self) -> &VersionedValue<BTreeSet<EndpointId>> {
    self.state.get_eids()
  }

  fn handle_input(&mut self, message: Self::MessageT) {
    self.state.handle_input(self.io_ctx, FullSlaveInput::SlaveMessage(message));
  }
}

// -----------------------------------------------------------------------------------------------
//  NominalState Container
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct NominalState<MessageT> {
  /// Messages (which are not client messages) that came from `EndpointId`s that
  /// is not present in `state.get_eids`.
  buffered_messages: BTreeMap<EndpointId, Vec<MessageT>>,
  /// The `Gen` of the set of `EndpointId`s that `state` currently allows
  /// messages to pass through from.
  cur_gen: Gen,
}

impl<MessageT: MessageTrait> NominalState<MessageT> {
  fn init<InnerStateT>(
    inner_state: &mut InnerStateT,
    buffered_messages: BTreeMap<EndpointId, Vec<MessageT>>,
  ) -> NominalState<MessageT>
  where
    InnerStateT: InnerStateTrait<MessageT = MessageT>,
  {
    // Record and maintain the current version of `get_eids`, which we use to
    // detect if `get_eids` changes and ensure that all buffered messages that
    // should be delivered actually are.
    let mut cur_gen = inner_state.get_eids().gen().clone();

    // Construct NominalState
    let mut nominal_state = NominalState { buffered_messages, cur_gen };

    // Deliver all buffered messages, leaving buffered only what should be
    // buffered. (Importantly, there might be Master messages, like Paxos,
    // already present from the other Master nodes being bootstrapped).
    nominal_state.deliver_all_once(inner_state);

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    nominal_state.deliver_all(inner_state);
    nominal_state
  }

  fn handle_msg<InnerStateT: InnerStateTrait>(
    &mut self,
    inner_state: &mut InnerStateT,
    eid: &EndpointId,
    message: MessageT,
  ) where
    InnerStateT: InnerStateTrait<MessageT = MessageT>,
  {
    // Pass through normally or buffer.
    self.deliver_single_once(inner_state, eid, message);

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    self.deliver_all(inner_state);
  }

  /// Deliver the `master_message` to `master_state`, or buffer it.
  fn deliver_single_once<InnerStateT: InnerStateTrait>(
    &mut self,
    inner_state: &mut InnerStateT,
    eid: &EndpointId,
    message: MessageT,
  ) where
    InnerStateT: InnerStateTrait<MessageT = MessageT>,
  {
    // If the message is from the External, we deliver it.
    if message.is_external() {
      inner_state.handle_input(message);
    }
    // Otherwise, if the message is a tier 1 message, we deliver it
    else if message.is_tier_1() {
      inner_state.handle_input(message);
    }
    // Otherwise, if it is from an EndpointId from `get_eids`, we deliver it.
    else if inner_state.get_eids().value().contains(eid) {
      inner_state.handle_input(message);
    } else {
      // Otherwise, we buffer the message.
      amend_buffer(&mut self.buffered_messages, eid, message);
    }
  }

  fn deliver_all_once<InnerStateT: InnerStateTrait>(&mut self, inner_state: &mut InnerStateT)
  where
    InnerStateT: InnerStateTrait<MessageT = MessageT>,
  {
    // Deliver all messages that were buffered so far.
    let mut old_buffered_messages = std::mem::take(&mut self.buffered_messages);
    for (eid, buffer) in old_buffered_messages {
      for message in buffer {
        self.deliver_single_once(inner_state, &eid, message);
      }
    }
  }

  fn deliver_all<InnerStateT: InnerStateTrait>(&mut self, inner_state: &mut InnerStateT)
  where
    InnerStateT: InnerStateTrait<MessageT = MessageT>,
  {
    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    while &self.cur_gen != inner_state.get_eids().gen() {
      self.cur_gen = *inner_state.get_eids().gen();
      self.deliver_all_once(inner_state);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Node Config
// -----------------------------------------------------------------------------------------------
// Generally, the primary things that we put into `NodeConfig` are things that might vary between
// testing and production. This largely includes timer-based things (since testing typically
// would like to accelerate things), but also includes other things (e.g. PaxosGroup size).

/// Config that holds all configs that a Node would need (for both Master, Slave, and other
/// threads (e.g. Coord and Tablet)).
#[derive(Debug, Clone)]
pub struct NodeConfig {
  // Configs for the Top Level
  pub free_node_heartbeat_timer_ms: u128,

  // Sub Configs
  pub paxos_config: PaxosConfig,
  pub coord_config: CoordConfig,
  pub master_config: MasterConfig,
  pub slave_config: SlaveConfig,
  pub tablet_config: TabletConfig,
}

/// Build the `NodeConfig` we should use for production.
pub fn get_prod_configs() -> NodeConfig {
  let timestamp_suffix_divisor = 5;

  let paxos_config = PaxosConfig {
    heartbeat_threshold: 5,
    heartbeat_period_ms: mk_t(1000),
    next_index_period_ms: mk_t(1000),
    retry_defer_time_ms: mk_t(1000),
    proposal_increment: 1000,
    remote_next_index_thresh: 100,
    max_failable: 1,
  };

  let remote_leader_changed_period_ms = 1000;
  let failure_detector_period_ms = 1000;
  let check_unconfirmed_eids_period_ms = 5000;
  let free_node_heartbeat_timer_ms = 1000;
  let master_config = MasterConfig {
    timestamp_suffix_divisor,
    slave_group_size: 5,
    remote_leader_changed_period_ms,
    failure_detector_period_ms,
    check_unconfirmed_eids_period_ms,
    gossip_data_period_ms: 5000,
    num_coords: 3,
    free_node_heartbeat_timer_ms,
  };
  let slave_config = SlaveConfig {
    timestamp_suffix_divisor,
    remote_leader_changed_period_ms,
    failure_detector_period_ms,
    check_unconfirmed_eids_period_ms,
  };

  let coord_config = CoordConfig { timestamp_suffix_divisor };
  let tablet_config = TabletConfig { timestamp_suffix_divisor };

  // Combine the above
  NodeConfig {
    free_node_heartbeat_timer_ms,
    paxos_config,
    coord_config,
    master_config,
    slave_config,
    tablet_config,
  }
}

// -----------------------------------------------------------------------------------------------
//  State
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum State {
  DNEState(BTreeMap<EndpointId, Vec<msg::NetworkMessage>>),
  FreeNodeState(LeadershipId, BTreeMap<EndpointId, Vec<msg::NetworkMessage>>),
  NominalSlaveState(SlaveState, NominalState<msg::SlaveMessage>),
  NominalMasterState(MasterState, NominalState<msg::MasterMessage>),
  PostExistence,
}

#[derive(Debug)]
pub struct NodeState {
  this_eid: EndpointId,
  node_config: NodeConfig,

  /// State
  state: State,
}

impl NodeState {
  pub fn new(this_eid: EndpointId, node_config: NodeConfig) -> NodeState {
    NodeState { this_eid, node_config, state: State::DNEState(BTreeMap::default()) }
  }

  /// This should be called at the very start of the life of a Master node. This
  /// will start the timer events, etc.
  pub fn bootstrap<IOCtx: NodeIOCtx>(&mut self, io_ctx: &mut IOCtx) {
    self.process_input(io_ctx, GenericInput::TimerInput(GenericTimerInput::FreeNodeHeartbeat));
  }

  /// The main input entrypoint for a top-level node (i.e. Master and Slave). `GenericInput`
  /// includes all network events, timer events, and cross-thread events for the top-level
  /// threads (e.g. Master and Slave).
  pub fn process_input<IOCtx: NodeIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    generic_input: GenericInput,
  ) {
    match &mut self.state {
      State::DNEState(buffered_messages) => match generic_input {
        GenericInput::Message(eid, message) => {
          // Handle FreeNode messages
          if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
            match free_node_msg {
              msg::FreeNodeMessage::FreeNodeRegistered(registered) => {
                self.state =
                  State::FreeNodeState(registered.cur_lid, std::mem::take(buffered_messages));
              }
              msg::FreeNodeMessage::ShutdownNode => {
                self.state = State::PostExistence;
              }
              msg::FreeNodeMessage::StartMaster(start) => {
                // Create the MasterState
                let leader = start.master_eids.get(0).unwrap();
                let master_lid = LeadershipId { gen: Gen(0), eid: leader.clone() };
                let mut leader_map = BTreeMap::<PaxosGroupId, LeadershipId>::new();
                leader_map.insert(PaxosGroupId::Master, master_lid);
                let mut master_state = MasterState::new(MasterContext::create_initial(
                  start.master_eids,
                  leader_map.clone(),
                  self.this_eid.clone(),
                  self.node_config.master_config.clone(),
                  self.node_config.paxos_config.clone(),
                ));

                // Bootstrap the Master
                master_state.bootstrap(io_ctx);

                // Convert all buffered messages to MasterMessage, since those should be all
                // that is present.
                let mut master_buffered_msgs =
                  BTreeMap::<EndpointId, Vec<msg::MasterMessage>>::new();
                for (eid, buffer) in std::mem::take(buffered_messages) {
                  for message in buffer {
                    let master_msg = cast!(msg::NetworkMessage::Master, message).unwrap();
                    amend_buffer(&mut master_buffered_msgs, &eid, master_msg);
                  }
                }

                // Advance
                let mut inner_state = MasterInnerState { io_ctx, state: &mut master_state };
                let nominal_state = NominalState::init(&mut inner_state, master_buffered_msgs);
                self.state = State::NominalMasterState(master_state, nominal_state);
              }
              _ => {}
            }
          } else {
            // Otherwise, buffer the message
            amend_buffer(buffered_messages, &eid, message);
          }
        }
        GenericInput::TimerInput(GenericTimerInput::FreeNodeHeartbeat) => {
          // Schedule the next FreeNodeTimerInput.
          let defer_time = mk_t(self.node_config.free_node_heartbeat_timer_ms);
          FreeNodeIOCtx::defer(io_ctx, defer_time, GenericTimerInput::FreeNodeHeartbeat);
        }
        _ => {}
      },
      State::FreeNodeState(lid, buffered_messages) => match generic_input {
        GenericInput::Message(eid, message) => {
          // Handle FreeNode messages
          if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
            match free_node_msg {
              msg::FreeNodeMessage::MasterLeadershipId(new_lid) => {
                // Update the Leadership Id if it is more recent.
                if new_lid.gen > lid.gen {
                  *lid = new_lid;
                }
              }
              msg::FreeNodeMessage::CreateSlaveGroup(create) => {
                // Create GossipData
                let gossip = Arc::new(create.gossip);

                // Create the Coords
                let mut coord_positions: Vec<CoordGroupId> = Vec::new();
                for cid in create.coord_ids {
                  let coord_context = CoordContext::new(
                    create.sid.clone(),
                    cid.clone(),
                    gossip.clone(),
                    create.leader_map.clone(),
                    create.paxos_nodes.clone(),
                    self.this_eid.clone(),
                    self.node_config.coord_config.clone(),
                  );
                  io_ctx.create_coord_full(coord_context);
                  coord_positions.push(cid);
                }

                // Construct the SlaveState
                let slave_context = SlaveContext::new(
                  create.sid.clone(),
                  coord_positions,
                  gossip,
                  create.leader_map,
                  create.paxos_nodes,
                  self.this_eid.clone(),
                  self.node_config.slave_config.clone(),
                  self.node_config.paxos_config.clone(),
                );
                let mut slave_state = SlaveState::new(slave_context);

                // Bootstrap the slave
                slave_state.bootstrap(io_ctx, vec![]);

                // Convert all buffered messages to SlaveMessages, since those should be all
                // that is present.
                let mut slave_buffered_msgs = BTreeMap::<EndpointId, Vec<msg::SlaveMessage>>::new();
                for (eid, buffer) in std::mem::take(buffered_messages) {
                  for message in buffer {
                    let slave_msg = cast!(msg::NetworkMessage::Slave, message).unwrap();
                    amend_buffer(&mut slave_buffered_msgs, &eid, slave_msg);
                  }
                }

                // Advance
                let mut inner_state = SlaveInnerState { io_ctx, state: &mut slave_state };
                let nominal_state = NominalState::init(&mut inner_state, slave_buffered_msgs);
                self.state = State::NominalSlaveState(slave_state, nominal_state);

                // Respond with a `ConfirmSlaveCreation`.
                io_ctx.send(
                  &eid,
                  msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
                    msg::FreeNodeAssoc::ConfirmSlaveCreation(msg::ConfirmSlaveCreation {
                      sid: create.sid,
                      sender_eid: self.this_eid.clone(),
                    }),
                  )),
                );
              }
              msg::FreeNodeMessage::SlaveSnapshot(snapshot) => {
                let gossip = Arc::new(snapshot.gossip);

                // Create the Coords
                for cid in snapshot.coord_positions.clone() {
                  let coord_context = CoordContext::new(
                    snapshot.this_sid.clone(),
                    cid.clone(),
                    gossip.clone(),
                    snapshot.leader_map.clone(),
                    snapshot.paxos_driver_start.paxos_nodes.clone(),
                    self.this_eid.clone(),
                    self.node_config.coord_config.clone(),
                  );
                  io_ctx.create_coord_full(coord_context);
                }

                // Create the Tablets
                let mut tids: Vec<_> = snapshot.tablet_snapshots.keys().cloned().collect();
                for (_, tablet_snapshot) in snapshot.tablet_snapshots {
                  io_ctx.create_tablet_full(
                    gossip.clone(),
                    tablet_snapshot,
                    self.this_eid.clone(),
                    self.node_config.tablet_config.clone(),
                  );
                }

                // Create the SlaveState
                let mut slave_state = SlaveState::create_reconfig(
                  io_ctx,
                  snapshot.this_sid.clone(),
                  snapshot.coord_positions,
                  gossip.clone(),
                  snapshot.leader_map.clone(),
                  snapshot.paxos_driver_start.clone(),
                  snapshot.create_table_ess,
                  snapshot.shard_split_ess,
                  self.this_eid.clone(),
                  self.node_config.slave_config.clone(),
                  self.node_config.paxos_config.clone(),
                );

                // Bootstrap the slave
                slave_state.bootstrap(io_ctx, tids);

                // Convert all buffered messages to SlaveMessages, since those should be all
                // that is present.
                let mut slave_buffered_msgs = BTreeMap::<EndpointId, Vec<msg::SlaveMessage>>::new();
                for (eid, buffer) in std::mem::take(buffered_messages) {
                  for message in buffer {
                    let slave_msg = cast!(msg::NetworkMessage::Slave, message).unwrap();
                    amend_buffer(&mut slave_buffered_msgs, &eid, slave_msg);
                  }
                }

                // Advance
                let mut inner_state = SlaveInnerState { io_ctx, state: &mut slave_state };
                let nominal_state = NominalState::init(&mut inner_state, slave_buffered_msgs);
                self.state = State::NominalSlaveState(slave_state, nominal_state);
              }
              msg::FreeNodeMessage::MasterSnapshot(snapshot) => {
                // Create the MasterState
                let mut master_state = MasterState::create_reconfig(
                  io_ctx,
                  snapshot,
                  self.node_config.master_config.clone(),
                  self.node_config.paxos_config.clone(),
                  self.this_eid.clone(),
                );

                // Bootstrap the Master
                master_state.bootstrap(io_ctx);

                // Convert all buffered messages to MasterMessage, since those should be all
                // that is present.
                let mut master_buffered_msgs =
                  BTreeMap::<EndpointId, Vec<msg::MasterMessage>>::new();
                for (eid, buffer) in std::mem::take(buffered_messages) {
                  for message in buffer {
                    let master_msg = cast!(msg::NetworkMessage::Master, message).unwrap();
                    amend_buffer(&mut master_buffered_msgs, &eid, master_msg);
                  }
                }

                // Advance
                let mut inner_state = MasterInnerState { io_ctx, state: &mut master_state };
                let nominal_state = NominalState::init(&mut inner_state, master_buffered_msgs);
                self.state = State::NominalMasterState(master_state, nominal_state);
              }
              msg::FreeNodeMessage::ShutdownNode => {
                self.state = State::PostExistence;
              }
              _ => {}
            }
          } else {
            // Otherwise, buffer the message
            amend_buffer(buffered_messages, &eid, message);
          }
        }
        GenericInput::TimerInput(GenericTimerInput::FreeNodeHeartbeat) => {
          // Send out `FreeNodeHeartbeat`
          io_ctx.send(
            &lid.eid,
            msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
              msg::FreeNodeAssoc::FreeNodeHeartbeat(msg::FreeNodeHeartbeat {
                sender_eid: self.this_eid.clone(),
                cur_lid: lid.clone(),
              }),
            )),
          );

          // Schedule the next FreeNodeTimerInput.
          let defer_time = mk_t(self.node_config.free_node_heartbeat_timer_ms);
          FreeNodeIOCtx::defer(io_ctx, defer_time, GenericTimerInput::FreeNodeHeartbeat);
        }
        _ => {}
      },
      State::NominalSlaveState(slave_state, nominal_state) => {
        match generic_input {
          GenericInput::Message(eid, message) => {
            // Handle FreeNode messages
            if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
              match free_node_msg {
                msg::FreeNodeMessage::CreateSlaveGroup(_) => {
                  // Respond with a `ConfirmSlaveCreation`.
                  io_ctx.send(
                    &eid,
                    msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
                      msg::FreeNodeAssoc::ConfirmSlaveCreation(msg::ConfirmSlaveCreation {
                        sid: slave_state.ctx.this_sid.clone(),
                        sender_eid: self.this_eid.clone(),
                      }),
                    )),
                  );
                }
                msg::FreeNodeMessage::SlaveSnapshot(_) => {
                  // Respond with a `NewNodeStarted`.
                  io_ctx.send(
                    &eid,
                    msg::NetworkMessage::Slave(msg::SlaveMessage::PaxosDriverMessage(
                      msg::PaxosDriverMessage::NewNodeStarted(msg::NewNodeStarted {
                        paxos_node: self.this_eid.clone(),
                      }),
                    )),
                  );
                }
                msg::FreeNodeMessage::ShutdownNode => {
                  SlaveIOCtx::mark_exit(io_ctx);
                }
                _ => {}
              }
            } else if let msg::NetworkMessage::Slave(slave_msg) = message {
              // Forward the message.
              let mut inner_state = SlaveInnerState { io_ctx, state: slave_state };
              nominal_state.handle_msg(&mut inner_state, &eid, slave_msg);
            }
          }
          GenericInput::TimerInput(GenericTimerInput::SlaveTimerInput(timer_input)) => {
            // Forward the `SlaveTimerInput`
            slave_state.handle_input(io_ctx, FullSlaveInput::SlaveTimerInput(timer_input));
          }
          GenericInput::SlaveBackMessage(back_msg) => {
            // Forward the `SlaveBackMessage`
            slave_state.handle_input(io_ctx, FullSlaveInput::SlaveBackMessage(back_msg));
          }
          _ => {}
        }

        // Check the IOCtx and see if we should go to post existence.
        if SlaveIOCtx::did_exit(io_ctx) {
          self.state = State::PostExistence;
        }
      }
      State::NominalMasterState(master_state, nominal_state) => {
        match generic_input {
          GenericInput::Message(eid, message) => {
            // Handle FreeNode messages
            if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
              match free_node_msg {
                msg::FreeNodeMessage::MasterSnapshot(_) => {
                  // Respond with a `NewNodeStarted`.
                  io_ctx.send(
                    &eid,
                    msg::NetworkMessage::Master(msg::MasterMessage::PaxosDriverMessage(
                      msg::PaxosDriverMessage::NewNodeStarted(msg::NewNodeStarted {
                        paxos_node: self.this_eid.clone(),
                      }),
                    )),
                  );
                }
                msg::FreeNodeMessage::ShutdownNode => {
                  MasterIOCtx::mark_exit(io_ctx);
                }
                _ => {}
              }
            } else if let msg::NetworkMessage::Master(master_msg) = message {
              // Forward the message.
              let mut inner_state = MasterInnerState { io_ctx, state: master_state };
              nominal_state.handle_msg(&mut inner_state, &eid, master_msg);
            }
          }
          GenericInput::TimerInput(GenericTimerInput::MasterTimerInput(timer_input)) => {
            // Forward the `MasterTimerInput`
            master_state.handle_input(io_ctx, FullMasterInput::MasterTimerInput(timer_input));
          }
          _ => {}
        }

        // Check the IOCtx and see if we should go to post existence.
        if MasterIOCtx::did_exit(io_ctx) {
          self.state = State::PostExistence;
        }
      }
      State::PostExistence => {}
    }
  }

  /// This method should ideally only be called if this node is a Master. It will
  /// return the the GossipData underneath. If this node is not the Master (i.e. it
  /// is in `PostExistence`), then we return `None`.
  pub fn full_db_schema(&self) -> Option<GossipDataView> {
    if let State::NominalMasterState(master_state, _) = &self.state {
      Some(master_state.ctx.gossip.get())
    } else {
      None
    }
  }

  /// Returns `true` iff this node is beyond DNEState
  pub fn does_exist(&self) -> bool {
    if let State::DNEState(_) = &self.state {
      false
    } else {
      true
    }
  }

  /// Returns `true` iff this node is beyond DNEState
  pub fn did_exit(&self) -> bool {
    if let State::PostExistence = &self.state {
      true
    } else {
      false
    }
  }
}
