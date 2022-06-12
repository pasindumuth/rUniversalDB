use crate::common::{
  CTSubNodePath, PaxosGroupId, PaxosGroupIdTrait, QueryId, SlaveIOCtx, TNodePath, TabletGroupId,
};
use crate::paxos2pc_tm::Paxos2PCContainer;
use crate::server::ServerContextBase;
use crate::shard_split_slave_rm_es::{
  ShardSplitSlaveRMAction, ShardSplitSlaveRMES, ShardSplitSlaveRMPayloadTypes,
};
use crate::shard_split_tm_es::ShardSplitTMPayloadTypes;
use crate::slave::{SlaveContext, SlavePLm};
use crate::tablet::{
  ShardingSnapshot, TabletBundle, TabletConfig, TabletContext, TabletForwardMsg,
};
use crate::{message as msg, stmpaxos2pc_rm, stmpaxos2pc_tm};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  PLms
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardingSnapshotPLm {
  query_id: QueryId,
  node_path: TNodePath,
  snapshot: ShardingSnapshot,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ShardingSplitPLm {
  ShardSplit(stmpaxos2pc_rm::RMPLm<ShardSplitSlaveRMPayloadTypes>),
  ShardingSnapshotPLm(ShardingSnapshotPLm),
}

// -----------------------------------------------------------------------------------------------
//  PendingShardingES
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
enum State {
  Follower,
  WaitingShardingSnapshot { buffered_msgs: Vec<msg::TabletMessage> },
  InsertingShardingSnapshot { buffered_msgs: Vec<msg::TabletMessage> },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct PendingShardingES {
  state: State,
}

impl PendingShardingES {
  fn create(
    ctx: &mut SlaveContext,
    maybe_sharding_msg: Option<msg::ShardingMessage>,
  ) -> PendingShardingES {
    let state = if ctx.is_leader() {
      // If the `ShardingSnapshot` had already arrived from the sending Tablet,
      // then start inserting immediately.
      if let Some(sharding_msg) = maybe_sharding_msg {
        ctx.slave_bundle.plms.push(SlavePLm::ShardingSplitPLm(
          ShardingSplitPLm::ShardingSnapshotPLm(ShardingSnapshotPLm {
            query_id: sharding_msg.query_id,
            node_path: sharding_msg.node_path,
            snapshot: sharding_msg.snapshot,
          }),
        ));
        State::InsertingShardingSnapshot { buffered_msgs: vec![] }
      } else {
        State::WaitingShardingSnapshot { buffered_msgs: vec![] }
      }
    } else {
      State::Follower
    };

    PendingShardingES { state }
  }

  fn handle_sharding_msg<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    _: &mut IO,
    sharding_msg: msg::ShardingMessage,
  ) {
    match &mut self.state {
      State::WaitingShardingSnapshot { buffered_msgs } => {
        ctx.slave_bundle.plms.push(SlavePLm::ShardingSplitPLm(
          ShardingSplitPLm::ShardingSnapshotPLm(ShardingSnapshotPLm {
            query_id: sharding_msg.query_id,
            node_path: sharding_msg.node_path,
            snapshot: sharding_msg.snapshot,
          }),
        ));
        let buffered_msgs = std::mem::take(buffered_msgs);
        self.state = State::InsertingShardingSnapshot { buffered_msgs }
      }
      _ => {}
    }
  }

  /// Buffer any messages that are meant for this Tablet.
  fn handle_tablet_msg(&mut self, tablet_msg: msg::TabletMessage) {
    match &mut self.state {
      State::WaitingShardingSnapshot { buffered_msgs }
      | State::InsertingShardingSnapshot { buffered_msgs } => {
        buffered_msgs.push(tablet_msg);
      }
      _ => {}
    }
  }

  /// Updates the Slave state that is Purely Derived from this `plm`.
  fn create_tablet<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
    snapshot: ShardingSnapshot,
  ) {
    // Create the new Tablet
    io_ctx.create_tablet(TabletContext {
      tablet_config: TabletConfig {
        timestamp_suffix_divisor: ctx.slave_config.timestamp_suffix_divisor,
      },
      this_sid: ctx.this_sid.clone(),
      this_gid: ctx.this_sid.to_gid(),
      this_tid: snapshot.this_tid.clone(),
      sub_node_path: CTSubNodePath::Tablet(snapshot.this_tid.clone()),
      this_eid: ctx.this_eid.clone(),
      gossip: ctx.gossip.clone(),
      leader_map: ctx.leader_map.value().clone(),
      storage: snapshot.storage,
      this_table_path: snapshot.this_table_path,
      this_sharding_gen: snapshot.this_sharding_gen,
      this_tablet_key_range: snapshot.this_table_key_range,
      sharding_done: true,
      table_schema: snapshot.table_schema,
      presence_timestamp: snapshot.presence_timestamp,
      verifying_writes: Default::default(),
      inserting_prepared_writes: Default::default(),
      prepared_writes: Default::default(),
      committed_writes: snapshot.committed_writes,
      waiting_read_protected: Default::default(),
      inserting_read_protected: Default::default(),
      read_protected: snapshot.read_protected,
      waiting_locked_cols: Default::default(),
      inserting_locked_cols: Default::default(),
      ms_root_query_map: Default::default(),
      tablet_bundle: vec![],
    });
  }

  fn handle_sharding_plm<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
    plm: ShardingSnapshotPLm,
  ) {
    match &mut self.state {
      State::Follower => {
        self.create_tablet(ctx, io_ctx, plm.snapshot);
      }
      State::InsertingShardingSnapshot { buffered_msgs } => {
        let buffered_messages = std::mem::take(buffered_msgs);
        let this_tid = plm.snapshot.this_tid.clone();
        self.create_tablet(ctx, io_ctx, plm.snapshot);

        // Send all buffered data to the new Tablet.
        for msg in buffered_messages {
          io_ctx.tablet_forward(&this_tid, TabletForwardMsg::TabletMessage(msg)).unwrap();
        }

        // Send back a `ShardingConfirmed` to current Leadership of the sending Tablet.
        ctx.send_to_t(
          io_ctx,
          plm.node_path,
          msg::TabletMessage::ShardingConfirmed(msg::ShardingConfirmed { qid: plm.query_id }),
        );
      }
      _ => {}
    }
  }

  fn leader_changed(&mut self, ctx: &mut SlaveContext) {
    match &mut self.state {
      State::Follower => {
        if ctx.is_leader() {
          self.state = State::WaitingShardingSnapshot { buffered_msgs: vec![] };
        }
      }
      State::WaitingShardingSnapshot { .. } | State::InsertingShardingSnapshot { .. } => {
        self.state = State::Follower
      }
    }
  }

  /// If this node is a Follower, a copy of this `PendingShardingES` is returned. If this
  /// node is a Leader, then the value of this `PendingShardingES` that would result from
  /// losing Leadership is returned (i.e. after calling `leader_changed`).
  fn reconfig_snapshot(&self) -> Option<PendingShardingES> {
    Some(PendingShardingES { state: State::Follower })
  }
}

// -----------------------------------------------------------------------------------------------
//  Sharding Container
// -----------------------------------------------------------------------------------------------
/// Implementation for DDLES

impl Paxos2PCContainer<ShardSplitSlaveRMES>
  for BTreeMap<QueryId, (Option<msg::ShardingMessage>, ShardSplitSlaveRMES)>
{
  fn get_mut(&mut self, query_id: &QueryId) -> Option<&mut ShardSplitSlaveRMES> {
    self.get_mut(query_id).map(|(_, es)| es)
  }

  fn insert(&mut self, query_id: QueryId, es: ShardSplitSlaveRMES) {
    self.insert(query_id, (None, es));
  }
}

// -----------------------------------------------------------------------------------------------
//  ES Container Functions
// -----------------------------------------------------------------------------------------------
// Here, we hold all containers related to Sharding that occurs in a Slave

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardSplitESS {
  /// The `STMPaxos2PC` container shard splitting.
  shard_split_rm_ess: BTreeMap<QueryId, (Option<msg::ShardingMessage>, ShardSplitSlaveRMES)>,
  /// Holds ESs that anticipate the arrival of a `ShardingSnapshot`, and creates the Tablet.
  shard_pending_ess: BTreeMap<QueryId, PendingShardingES>,
  /// This an element here exists iff the corresponding element in `shard_pending_ess` exists.
  pending_shards: BTreeMap<TabletGroupId, QueryId>,
}

impl ShardSplitESS {
  pub fn handle_rm_msg<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
    msg: stmpaxos2pc_tm::RMMessage<ShardSplitTMPayloadTypes>,
  ) {
    let (query_id, action) =
      stmpaxos2pc_rm::handle_rm_msg(ctx, io_ctx, &mut self.shard_split_rm_ess, msg);
    self.handle_shard_split_es_action(io_ctx, ctx, query_id, action);
  }

  pub fn handle_sharding_msg<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
    sharding_msg: msg::ShardingMessage,
  ) {
    // Send the snapshot to the corresponding `ShardES`.
    let query_id = &sharding_msg.query_id;
    if let Some((maybe_sharding_msg, _)) = self.shard_split_rm_ess.get_mut(query_id) {
      *maybe_sharding_msg = Some(sharding_msg);
    } else if let Some(es) = self.shard_pending_ess.get_mut(query_id) {
      es.handle_sharding_msg(ctx, io_ctx, sharding_msg);
    } else {
      // This means that a prior snapshot for thie `query_id` had already been
      // processed. Thus, we immediately respond successfully.
      ctx.send_to_t(
        io_ctx,
        sharding_msg.node_path,
        msg::TabletMessage::ShardingConfirmed(msg::ShardingConfirmed {
          qid: sharding_msg.query_id,
        }),
      )
    }
  }

  /// Handles all incoming `TabletMessages`
  pub fn handle_tablet_msg<IO: SlaveIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    tid: TabletGroupId,
    tablet_msg: msg::TabletMessage,
  ) {
    match io_ctx.tablet_forward(&tid, TabletForwardMsg::TabletMessage(tablet_msg)) {
      Ok(_) => {}
      Err(forward_msg) => {
        // If forwarding fails, that means the Tablet is still pending creation.
        let query_id = self.pending_shards.get(&tid).unwrap();
        let es = self.shard_pending_ess.get_mut(query_id).unwrap();
        es.handle_tablet_msg(cast!(TabletForwardMsg::TabletMessage, forward_msg).unwrap());
      }
    }
  }

  pub fn handle_plm<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
    plm: ShardingSplitPLm,
  ) {
    match plm {
      ShardingSplitPLm::ShardSplit(plm) => {
        let (query_id, action) =
          stmpaxos2pc_rm::handle_rm_plm(ctx, io_ctx, &mut self.shard_split_rm_ess, plm);
        self.handle_shard_split_es_action(io_ctx, ctx, query_id, action);
      }
      ShardingSplitPLm::ShardingSnapshotPLm(plm) => {
        let this_tid = plm.snapshot.this_tid.clone();
        // Here, the `PendingShardingES` should be done, so we also remove it.
        if let Some(mut es) = self.shard_pending_ess.remove(&plm.query_id) {
          // Update `pending_shards`
          self.pending_shards.remove(&this_tid);
          // Forward to the ES.
          es.handle_sharding_plm(ctx, io_ctx, plm);
          // Amend `tablet_bundles` so that SharedPaxosInserter can work properly
          ctx.tablet_bundles.insert(this_tid, TabletBundle::default());
        }
      }
    }
  }

  /// For every `ShardSplitSlaveRMES` that is Working, we start processing it.
  pub fn handle_bundle_processed<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
  ) {
    for (_, (_, es)) in &mut self.shard_split_rm_ess {
      es.start_inserting(ctx, io_ctx);
    }
  }

  pub fn handle_leader_changed<IO: SlaveIOCtx>(&mut self, ctx: &mut SlaveContext, io_ctx: &mut IO) {
    // Informed `ShardSplitSlaveRMES`.
    let query_ids: Vec<QueryId> = self.shard_split_rm_ess.keys().cloned().collect();
    for query_id in query_ids {
      let (maybe_sharding_msg, es) = self.shard_split_rm_ess.get_mut(&query_id).unwrap();
      *maybe_sharding_msg = None; // Clear Transient state
      let action = es.leader_changed(ctx);
      self.handle_shard_split_es_action(io_ctx, ctx, query_id, action);
    }

    // Informed `PendingShardingES`.
    for (_, es) in &mut self.shard_pending_ess {
      es.leader_changed(ctx)
    }
  }

  /// Handles the actions produced by a ShardSplitSlaveRMES.
  fn handle_shard_split_es_action<IO: SlaveIOCtx>(
    &mut self,
    _: &mut IO,
    ctx: &mut SlaveContext,
    query_id: QueryId,
    action: ShardSplitSlaveRMAction,
  ) {
    match action {
      ShardSplitSlaveRMAction::Wait => {}
      ShardSplitSlaveRMAction::Exit(maybe_commit_action) => {
        // Remove the `ShardSplitSlaveRMES`, converting it to `PendingShardingES`
        // if the STMPaxos2PC committed.
        let shard_es = self.shard_split_rm_ess.remove(&query_id).unwrap();
        let (maybe_sharding_msg, _) = shard_es;
        if let Some((tid, qid)) = maybe_commit_action {
          // This means that the ES had Committed. We convert it to a `PendingSharding`
          // and amend `pending_shards`.
          self.pending_shards.insert(tid, qid);
          self
            .shard_pending_ess
            .insert(query_id, PendingShardingES::create(ctx, maybe_sharding_msg));
        }
      }
    }
  }

  /// Construct the version of `ShardSplitESS` that would result by losing Leadership.
  pub fn reconfig_snapshot(&self) -> ShardSplitESS {
    let mut ess = ShardSplitESS::default();
    for (qid, (_, es)) in &self.shard_split_rm_ess {
      if let Some(es) = es.reconfig_snapshot() {
        ess.shard_split_rm_ess.insert(qid.clone(), (None, es));
      }
    }
    for (qid, es) in &self.shard_pending_ess {
      if let Some(es) = es.reconfig_snapshot() {
        ess.shard_pending_ess.insert(qid.clone(), es);
      }
    }
    ess.pending_shards = self.pending_shards.clone();
    ess
  }

  pub fn is_empty(&self) -> bool {
    self.shard_split_rm_ess.is_empty()
      && self.shard_pending_ess.is_empty()
      && self.pending_shards.is_empty()
  }
}
