use crate::common::{GossipDataView, MasterIOCtx, NodeIOCtx, SlaveIOCtx};
use crate::coord::{CoordConfig, CoordContext};
use crate::master::{FullMasterInput, MasterConfig, MasterContext, MasterState, MasterTimerInput};
use crate::model::common::{
  CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait,
};
use crate::model::message as msg;
use crate::model::message::FreeNodeMessage;
use crate::paxos::PaxosConfig;
use crate::slave::{
  FullSlaveInput, SlaveBackMessage, SlaveConfig, SlaveContext, SlaveState, SlaveTimerInput,
};
use crate::tablet::TabletConfig;
use std::collections::BTreeMap;
use std::sync::Arc;

#[path = "./node_test.rs"]
pub mod node_test;

const SERVER_PORT: u32 = 1610;

// -----------------------------------------------------------------------------------------------
//  GenericInput
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum GenericInput {
  Message(EndpointId, msg::NetworkMessage),
  SlaveTimerInput(SlaveTimerInput),
  SlaveBackMessage(SlaveBackMessage),
  MasterTimerInput(MasterTimerInput),
  FreeNodeTimerInput,
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
//  NominalSlaveState
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct NominalSlaveState {
  state: SlaveState,
  /// Messages (which are not client messages) that came from `EndpointId`s that is not present
  /// in `state.get_eids`.
  buffered_messages: BTreeMap<EndpointId, Vec<msg::SlaveMessage>>,
  /// The `Gen` of the set of `EndpointId`s that `state` currently allows
  /// messages to pass through from.
  cur_gen: Gen,
}

impl NominalSlaveState {
  fn init<IO: SlaveIOCtx>(
    io_ctx: &mut IO,
    state: SlaveState,
    buffered_messages: BTreeMap<EndpointId, Vec<msg::SlaveMessage>>,
  ) -> NominalSlaveState {
    // Record and maintain the current version of `get_eids`, which we use to
    // detect if `get_eids` changes and ensure that all buffered messages that
    // should be delivered actually are.
    let mut cur_gen = state.get_eids().gen().clone();

    // Construct NominalSlaveState
    let mut nominal_state = NominalSlaveState { state, buffered_messages, cur_gen };

    // Deliver all buffered messages, leaving buffered only what should be
    // buffered. (Importantly, there might be Slave messages, like Paxos,
    // already present from the other Slave nodes being bootstrapped).
    nominal_state.deliver_all_once(io_ctx);

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    nominal_state.deliver_all(io_ctx);
    nominal_state
  }

  fn handle_msg<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    eid: &EndpointId,
    slave_msg: msg::SlaveMessage,
  ) {
    // Pass through normally or buffer.
    self.deliver_single_once(io_ctx, eid, slave_msg);

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    self.deliver_all(io_ctx);
  }

  /// Deliver the `slave_message` to `slave_state`, or buffer it.
  fn deliver_single_once<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    eid: &EndpointId,
    slave_msg: msg::SlaveMessage,
  ) {
    // If the message is from the External, we deliver it.
    if let msg::SlaveMessage::SlaveExternalReq(_) = &slave_msg {
      self.state.handle_input(io_ctx, FullSlaveInput::SlaveMessage(slave_msg));
    }
    // Otherwise, if it is from an EndpointId from `get_eids`, we deliver it.
    else if self.state.get_eids().value().contains(eid) {
      self.state.handle_input(io_ctx, FullSlaveInput::SlaveMessage(slave_msg));
    }
    // Otherwise, if the message is a tier 1 message, we deliver it
    else if slave_msg.is_tier_1() {
      self.state.handle_input(io_ctx, FullSlaveInput::SlaveMessage(slave_msg));
    } else {
      // Otherwise, we buffer the message.
      amend_buffer(&mut self.buffered_messages, eid, slave_msg);
    }
  }

  fn deliver_all_once<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO) {
    // Deliver all messages that were buffered so far.
    let mut old_buffered_messages = std::mem::take(&mut self.buffered_messages);
    for (eid, buffer) in old_buffered_messages {
      for slave_msg in buffer {
        self.deliver_single_once(io_ctx, &eid, slave_msg);
      }
    }
  }

  fn deliver_all<IO: SlaveIOCtx>(&mut self, io_ctx: &mut IO) {
    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    let mut gen_did_change = &self.cur_gen != self.state.get_eids().gen();
    while gen_did_change {
      let cur_gen = self.state.get_eids().gen().clone();
      self.deliver_all_once(io_ctx);
      // Check again whether the `get_eids` changed.
      gen_did_change = &cur_gen != self.state.get_eids().gen();
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  NominalMasterState
// -----------------------------------------------------------------------------------------------

// TODO: add a tier to the network message to help reasoning about the
//  below easier.

#[derive(Debug)]
struct NominalMasterState {
  state: MasterState,
  /// Messages (which are not client messages) that came from `EndpointId`s that is not present
  /// in `state.get_eids`.
  buffered_messages: BTreeMap<EndpointId, Vec<msg::MasterMessage>>,
  /// The `Gen` of the set of `EndpointId`s that `state` currently allows
  /// messages to pass through from.
  cur_gen: Gen,
}

impl NominalMasterState {
  fn init<IO: MasterIOCtx>(
    io_ctx: &mut IO,
    state: MasterState,
    buffered_messages: BTreeMap<EndpointId, Vec<msg::MasterMessage>>,
  ) -> NominalMasterState {
    // Record and maintain the current version of `get_eids`, which we use to
    // detect if `get_eids` changes and ensure that all buffered messages that
    // should be delivered actually are.
    let mut cur_gen = state.get_eids().gen().clone();

    // Construct NominalMasterState
    let mut nominal_state = NominalMasterState { state, buffered_messages, cur_gen };

    // Deliver all buffered messages, leaving buffered only what should be
    // buffered. (Importantly, there might be Master messages, like Paxos,
    // already present from the other Master nodes being bootstrapped).
    nominal_state.deliver_all_once(io_ctx);

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    nominal_state.deliver_all(io_ctx);
    nominal_state
  }

  fn handle_msg<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    eid: &EndpointId,
    master_msg: msg::MasterMessage,
  ) {
    // Pass through normally or buffer.
    self.deliver_single_once(io_ctx, eid, master_msg);

    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    self.deliver_all(io_ctx);
  }

  /// Deliver the `master_message` to `master_state`, or buffer it.
  fn deliver_single_once<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    eid: &EndpointId,
    master_msg: msg::MasterMessage,
  ) {
    // If the message is from the External, we deliver it.
    if let msg::MasterMessage::MasterExternalReq(_) = &master_msg {
      self.state.handle_input(io_ctx, FullMasterInput::MasterMessage(master_msg));
    }
    // Otherwise, if it is from an EndpointId from `get_eids`, we deliver it.
    else if self.state.get_eids().value().contains(eid) {
      self.state.handle_input(io_ctx, FullMasterInput::MasterMessage(master_msg));
    }
    // Otherwise, if the message is a tier 1 message, we deliver it
    else if master_msg.is_tier_1() {
      self.state.handle_input(io_ctx, FullMasterInput::MasterMessage(master_msg));
    } else {
      // Otherwise, we buffer the message.
      amend_buffer(&mut self.buffered_messages, eid, master_msg);
    }
  }

  fn deliver_all_once<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO) {
    // Deliver all messages that were buffered so far.
    let mut old_buffered_messages = std::mem::take(&mut self.buffered_messages);
    for (eid, buffer) in old_buffered_messages {
      for master_msg in buffer {
        self.deliver_single_once(io_ctx, &eid, master_msg);
      }
    }
  }

  fn deliver_all<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO) {
    // Next, we see if `get_eids` have changed. If so, there might now be buffered
    // messages that need to be delivered.
    let mut gen_did_change = &self.cur_gen != self.state.get_eids().gen();
    while gen_did_change {
      let cur_gen = self.state.get_eids().gen().clone();
      self.deliver_all_once(io_ctx);
      // Check again whether the `get_eids` changed.
      gen_did_change = &cur_gen != self.state.get_eids().gen();
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  State
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum State {
  DNEState(BTreeMap<EndpointId, Vec<msg::NetworkMessage>>),
  FreeNodeState(LeadershipId, BTreeMap<EndpointId, Vec<msg::NetworkMessage>>),
  NominalSlaveState(NominalSlaveState),
  NominalMasterState(NominalMasterState),
  PostExistence,
}

#[derive(Debug)]
pub struct NodeState {
  this_eid: EndpointId,

  // The configs that should be used when constructing various state (e.g. `SlaveState`)
  paxos_config: PaxosConfig,
  coord_config: CoordConfig,
  master_config: MasterConfig,
  slave_config: SlaveConfig,
  tablet_config: TabletConfig,

  state: State,
}

impl NodeState {
  pub fn new(
    this_eid: EndpointId,
    paxos_config: PaxosConfig,
    coord_config: CoordConfig,
    master_config: MasterConfig,
    slave_config: SlaveConfig,
    tablet_config: TabletConfig,
  ) -> NodeState {
    NodeState {
      this_eid,
      paxos_config,
      coord_config,
      master_config,
      slave_config,
      tablet_config,
      state: State::DNEState(BTreeMap::default()),
    }
  }

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
                  self.master_config.clone(),
                  self.paxos_config.clone(),
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
                self.state = State::NominalMasterState(NominalMasterState::init(
                  io_ctx,
                  master_state,
                  master_buffered_msgs,
                ));
              }
              _ => {}
            }
          } else {
            // Otherwise, buffer the message
            amend_buffer(buffered_messages, &eid, message);
          }
        }
        _ => {}
      },
      State::FreeNodeState(lid, buffered_messages) => match generic_input {
        GenericInput::Message(eid, message) => {
          // Handle FreeNode messages
          if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
            match free_node_msg {
              FreeNodeMessage::MasterLeadershipId(new_lid) => {
                // Update the Leadership Id if it is more recent.
                if new_lid.gen > lid.gen {
                  *lid = new_lid;
                }
              }
              FreeNodeMessage::CreateSlaveGroup(create) => {
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
                    self.coord_config.clone(),
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
                  self.slave_config.clone(),
                  self.paxos_config.clone(),
                );
                let mut slave_state = SlaveState::new(slave_context);

                // Bootstrap the slave
                slave_state.bootstrap(io_ctx);

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
                self.state = State::NominalSlaveState(NominalSlaveState::init(
                  io_ctx,
                  slave_state,
                  slave_buffered_msgs,
                ));

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
              FreeNodeMessage::SlaveSnapshot(snapshot) => {
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
                    self.coord_config.clone(),
                  );
                  io_ctx.create_coord_full(coord_context);
                }

                // Create the Tablets
                for (_, tablet_snapshot) in snapshot.tablet_snapshots {
                  io_ctx.create_tablet_full(
                    gossip.clone(),
                    tablet_snapshot,
                    self.tablet_config.clone(),
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
                  self.this_eid.clone(),
                  self.slave_config.clone(),
                  self.paxos_config.clone(),
                );

                // Bootstrap the slave
                slave_state.bootstrap(io_ctx);

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
                self.state = State::NominalSlaveState(NominalSlaveState::init(
                  io_ctx,
                  slave_state,
                  slave_buffered_msgs,
                ));
              }
              FreeNodeMessage::MasterSnapshot(snapshot) => {
                // Create the MasterState
                let mut master_state = MasterState::create_reconfig(
                  io_ctx,
                  snapshot,
                  self.master_config.clone(),
                  self.paxos_config.clone(),
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
                self.state = State::NominalMasterState(NominalMasterState::init(
                  io_ctx,
                  master_state,
                  master_buffered_msgs,
                ));
              }
              FreeNodeMessage::ShutdownNode => {
                self.state = State::PostExistence;
              }
              _ => {}
            }
          } else {
            // Otherwise, buffer the message
            amend_buffer(buffered_messages, &eid, message);
          }
        }
        GenericInput::FreeNodeTimerInput => {
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
        }
        _ => {}
      },
      State::NominalSlaveState(nominal_state) => {
        match generic_input {
          GenericInput::Message(eid, message) => {
            // Handle FreeNode messages
            if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
              match free_node_msg {
                FreeNodeMessage::CreateSlaveGroup(_) => {
                  // Respond with a `ConfirmSlaveCreation`.
                  io_ctx.send(
                    &eid,
                    msg::NetworkMessage::Master(msg::MasterMessage::FreeNodeAssoc(
                      msg::FreeNodeAssoc::ConfirmSlaveCreation(msg::ConfirmSlaveCreation {
                        sid: nominal_state.state.ctx.this_sid.clone(),
                        sender_eid: self.this_eid.clone(),
                      }),
                    )),
                  );
                }
                FreeNodeMessage::SlaveSnapshot(_) => {
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
                FreeNodeMessage::ShutdownNode => {
                  SlaveIOCtx::mark_exit(io_ctx);
                }
                _ => {}
              }
            } else if let msg::NetworkMessage::Slave(slave_msg) = message {
              // Forward the message.
              nominal_state.handle_msg(io_ctx, &eid, slave_msg);
            }
          }
          GenericInput::SlaveTimerInput(timer_input) => {
            // Forward the `SlaveTimerInput`
            nominal_state.state.handle_input(io_ctx, FullSlaveInput::SlaveTimerInput(timer_input));
          }
          GenericInput::SlaveBackMessage(back_msg) => {
            // Forward the `SlaveBackMessage`
            nominal_state.state.handle_input(io_ctx, FullSlaveInput::SlaveBackMessage(back_msg));
          }
          _ => {}
        }

        // Check the IOCtx and see if we should go to post existence.
        if SlaveIOCtx::did_exit(io_ctx) {
          self.state = State::PostExistence;
        }
      }
      State::NominalMasterState(nominal_state) => {
        match generic_input {
          GenericInput::Message(eid, message) => {
            // Handle FreeNode messages
            if let msg::NetworkMessage::FreeNode(free_node_msg) = message {
              match free_node_msg {
                FreeNodeMessage::MasterSnapshot(_) => {
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
                FreeNodeMessage::ShutdownNode => {
                  MasterIOCtx::mark_exit(io_ctx);
                }
                _ => {}
              }
            } else if let msg::NetworkMessage::Master(master_msg) = message {
              // Forward the message.
              nominal_state.handle_msg(io_ctx, &eid, master_msg);
            }
          }
          GenericInput::MasterTimerInput(timer_input) => {
            // Forward the `MasterTimerInput`
            nominal_state
              .state
              .handle_input(io_ctx, FullMasterInput::MasterTimerInput(timer_input));
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

  /// This method should only be called if this node is a Master. it will
  /// return the the GossipData underneath.
  pub fn full_db_schema(&self) -> GossipDataView {
    let master = cast!(State::NominalMasterState, &self.state).unwrap();
    master.state.ctx.gossip.get()
  }

  /// Returns `true` iff this node is beyond DNEState
  pub fn does_exist(&self) -> bool {
    if let State::DNEState(_) = &self.state {
      false
    } else {
      true
    }
  }
}
