use crate::alter_table_tm_es::{get_rms, ResponseData};
use crate::common::{cur_timestamp, mk_t, BasicIOCtx, FullGen, GeneralTraceMessage, Timestamp};
use crate::common::{
  PaxosGroupId, PaxosGroupIdTrait, ShardingGen, SlaveGroupId, TNodePath, TSubNodePath, TablePath,
  TabletGroupId, TabletKeyRange,
};
use crate::master::{MasterContext, MasterPLm};
use crate::message as msg;
use crate::server::ServerContextBase;
use crate::stmpaxos2pc_tm::{
  RMMessage, STMPaxos2PCTMInner, STMPaxos2PCTMOuter, TMClosedPLm, TMCommittedPLm, TMMessage, TMPLm,
  TMPayloadTypes, TMServerContext,
};
use crate::tablet::{TabletContext, TabletPLm};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTMPayloadTypes {}

impl TMPayloadTypes for ShardSplitTMPayloadTypes {
  // Master
  type RMPath = ShardNodePath;
  type TMPath = ();
  type NetworkMessageT = msg::NetworkMessage;
  type TMContext = MasterContext;

  // TM PLm
  type TMPreparedPLm = ShardSplitTMPrepared;
  type TMCommittedPLm = ShardSplitTMCommitted;
  type TMAbortedPLm = ShardSplitTMAborted;
  type TMClosedPLm = ShardSplitTMClosed;

  // TM-to-RM Messages
  type Prepare = ShardSplitPrepare;
  type Abort = ShardSplitAbort;
  type Commit = ShardSplitCommit;

  // RM-to-TM Messages
  type Prepared = ShardSplitPrepared;
  type Aborted = ShardSplitAborted;
  type Closed = ShardSplitClosed;
}

// TM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTMPrepared {
  pub table_path: TablePath,
  pub target_old: STRange,
  pub target_new: STRange,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTMCommitted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTMAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTMClosed {
  pub timestamp_hint: Option<Timestamp>,
}

// TM-to-RM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitPrepare {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitAbort {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitCommit {
  pub sharding_gen: ShardingGen,
  pub target_old: STRange,
  pub target_new: STRange,
}

// RM-to-TM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitPrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitClosed {}

// -----------------------------------------------------------------------------------------------
//  TMServerContext ShardSplit
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ShardNodePath {
  Tablet(TNodePath),
  Slave(SlaveGroupId),
}

impl PaxosGroupIdTrait for ShardNodePath {
  fn to_gid(&self) -> PaxosGroupId {
    match self {
      ShardNodePath::Tablet(node_path) => node_path.to_gid(),
      ShardNodePath::Slave(sid) => sid.to_gid(),
    }
  }
}

impl TMServerContext<ShardSplitTMPayloadTypes> for MasterContext {
  fn push_plm(&mut self, plm: TMPLm<ShardSplitTMPayloadTypes>) {
    self.master_bundle.plms.push(MasterPLm::ShardSplit(plm));
  }

  fn send_to_rm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    rm: &ShardNodePath,
    msg: RMMessage<ShardSplitTMPayloadTypes>,
  ) {
    match rm {
      ShardNodePath::Tablet(rm) => {
        self.send_to_t(io_ctx, rm.clone(), msg::TabletMessage::ShardSplit(msg));
      }
      ShardNodePath::Slave(rm) => {
        self.send_to_slave_common(io_ctx, rm.clone(), msg::SlaveRemotePayload::ShardSplit(msg));
      }
    }
  }

  fn mk_node_path(&self) -> () {
    ()
  }

  fn is_leader(&self) -> bool {
    MasterContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  ShardSplit Implementation
// -----------------------------------------------------------------------------------------------

pub type ShardSplitTMES = STMPaxos2PCTMOuter<ShardSplitTMPayloadTypes, ShardSplitTMInner>;

/// A Slave-Tablet-Range struct, used to point to a `TabletKeyRange`
/// in a fully-qualified manner.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STRange {
  pub sid: SlaveGroupId,
  pub tid: TabletGroupId,
  pub range: TabletKeyRange,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardSplitTMInner {
  // Response data
  pub response_data: Option<ResponseData>,

  // ShardSplit Query data
  pub table_path: TablePath,
  pub target_old: STRange,
  pub target_new: STRange,

  /// This is set when `Committed` or `Aborted` gets inserted
  /// for use when constructing `Closed`.
  pub did_commit: bool,
}

impl ShardSplitTMInner {
  /// For a given `message`, construct a map where the Keys are the various RMs
  /// that are a part of this STMPaxos2PC.
  fn mk_msgs<MsgT: Clone>(
    &self,
    ctx: &mut MasterContext,
    message: MsgT,
  ) -> BTreeMap<ShardNodePath, MsgT> {
    let mut messages = BTreeMap::<ShardNodePath, MsgT>::new();

    // Add the old Tablet as an RM
    let tid = self.target_old.tid.clone();
    let sid = ctx.gossip.get().tablet_address_config.get(&tid).unwrap().clone();
    messages.insert(
      ShardNodePath::Tablet(TNodePath { sid, sub: TSubNodePath::Tablet(tid.clone()) }),
      message.clone(),
    );

    // Add the new Tablet's Slave as an RM
    let sid = self.target_new.sid.clone();
    messages.insert(ShardNodePath::Slave(sid), message);

    messages
  }

  /// Add the new Shard for the Table and update the old one
  /// (based on the `timestamp_hint` and from GossipData).
  fn apply_sharding<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    timestamp_hint: Timestamp,
  ) -> Timestamp {
    ctx.gossip.update(|gossip| {
      let commit_timestamp =
        max(timestamp_hint, gossip.table_generation.get_lat(&self.table_path).add(mk_t(1)));
      let full_gen =
        gossip.table_generation.get_last_present_version(&self.table_path).unwrap().clone();
      let (gen, sharding_gen) = full_gen.clone();
      let next_full_gen = (gen, sharding_gen.next());

      // Update `table_generation`
      gossip.table_generation.write(
        &self.table_path,
        Some(next_full_gen.clone()),
        commit_timestamp.clone(),
      );

      // Update `sharding_config`.
      let table_path_full_gen = (self.table_path.clone(), full_gen);
      let shards = gossip.sharding_config.get(&table_path_full_gen).unwrap();
      let mut new_shards = Vec::<(TabletKeyRange, TabletGroupId)>::new();
      for shard in shards {
        // If this is the old Tablet, we update the `tablet_key_range` and add the new shard.
        let (_, tid) = shard;
        if &self.target_old.tid == tid {
          new_shards.push((self.target_old.range.clone(), self.target_old.tid.clone()));
          new_shards.push((self.target_new.range.clone(), self.target_new.tid.clone()));
        } else {
          // Otherwise, we just copy the current shard.
          new_shards.push(shard.clone());
        }
      }
      let next_table_path_full_gen = (self.table_path.clone(), next_full_gen);
      gossip.sharding_config.insert(next_table_path_full_gen, new_shards);

      // Update `tablet_address_config`.
      gossip.tablet_address_config.insert(self.target_new.tid.clone(), self.target_new.sid.clone());

      commit_timestamp
    })
  }
}

impl STMPaxos2PCTMInner<ShardSplitTMPayloadTypes> for ShardSplitTMInner {
  fn new_follower<IO: BasicIOCtx>(
    _: &mut MasterContext,
    _: &mut IO,
    payload: ShardSplitTMPrepared,
  ) -> ShardSplitTMInner {
    ShardSplitTMInner {
      response_data: None,
      table_path: payload.table_path,
      target_old: payload.target_old,
      target_new: payload.target_new,
      did_commit: false,
    }
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> ShardSplitTMPrepared {
    ShardSplitTMPrepared {
      table_path: self.table_path.clone(),
      target_old: self.target_old.clone(),
      target_new: self.target_new.clone(),
    }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> BTreeMap<ShardNodePath, ShardSplitPrepare> {
    self.mk_msgs(ctx, ShardSplitPrepare {})
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
    _: &BTreeMap<ShardNodePath, ShardSplitPrepared>,
  ) -> ShardSplitTMCommitted {
    ShardSplitTMCommitted {}
  }

  /// Apply this `alter_op` to the system and construct Commit messages with the
  /// commit timestamp (which is resolved form the resolved from `timestamp_hint`).
  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    _: &TMCommittedPLm<ShardSplitTMPayloadTypes>,
  ) -> BTreeMap<ShardNodePath, ShardSplitCommit> {
    self.did_commit = true;

    // Construct the `commit` message
    let full_gen = ctx.gossip.get().table_generation.get_last_version(&self.table_path).unwrap();
    let (_, sharding_gen) = full_gen;
    let commit = ShardSplitCommit {
      sharding_gen: sharding_gen.clone(),
      target_old: self.target_old.clone(),
      target_new: self.target_new.clone(),
    };

    // Construct message map
    self.mk_msgs(ctx, commit)
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> ShardSplitTMAborted {
    ShardSplitTMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> BTreeMap<ShardNodePath, ShardSplitAbort> {
    // Potentially respond to the External if we are the leader.
    if ctx.is_leader() {
      if let Some(response_data) = &self.response_data {
        ctx.external_request_id_map.remove(&response_data.request_id);
        io_ctx.send(
          &response_data.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalShardingAborted(
            msg::ExternalShardingAborted {
              request_id: response_data.request_id.clone(),
              payload: msg::ExternalShardingAbortData::Unknown,
            },
          )),
        );
        self.response_data = None;
      }
    }

    self.mk_msgs(ctx, ShardSplitAbort {})
  }

  fn mk_closed_plm<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> ShardSplitTMClosed {
    let timestamp_hint = if self.did_commit {
      Some(cur_timestamp(io_ctx, ctx.master_config.timestamp_suffix_divisor))
    } else {
      None
    };
    ShardSplitTMClosed { timestamp_hint }
  }

  fn closed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    closed_plm: &TMClosedPLm<ShardSplitTMPayloadTypes>,
  ) {
    if let Some(timestamp_hint) = &closed_plm.payload.timestamp_hint {
      // This means that the closed_plm is a result of committing the ShardSplit.
      let commit_timestamp = self.apply_sharding(ctx, io_ctx, timestamp_hint.clone());

      // Potentially respond to the External if we are the leader.
      // Note: Recall we will already have responded if the ShardSplit had failed.
      if ctx.is_leader() {
        if let Some(response_data) = &self.response_data {
          // This means this is the original Leader that got the query.
          ctx.external_request_id_map.remove(&response_data.request_id);
          io_ctx.send(
            &response_data.sender_eid,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(
              msg::ExternalDDLQuerySuccess {
                request_id: response_data.request_id.clone(),
                timestamp: commit_timestamp.clone(),
              },
            )),
          );
          self.response_data = None;
        }
      }

      // Trace this commit.
      io_ctx.general_trace(GeneralTraceMessage::CommittedQueryId(
        closed_plm.query_id.clone(),
        commit_timestamp.clone(),
      ));

      // Send out GossipData to all Slaves.
      ctx.broadcast_gossip(io_ctx);
    }
  }

  fn leader_changed<IO: BasicIOCtx>(&mut self, _: &mut MasterContext, _: &mut IO) {
    self.response_data = None;
  }

  fn reconfig_snapshot(&self) -> ShardSplitTMInner {
    ShardSplitTMInner {
      response_data: None,
      table_path: self.table_path.clone(),
      target_old: self.target_old.clone(),
      target_new: self.target_new.clone(),
      did_commit: self.did_commit.clone(),
    }
  }
}

/// Compute the next `ShardingGen`.
fn next_sharding_gen((_, sharding_gen): &FullGen) -> ShardingGen {
  sharding_gen.next()
}
