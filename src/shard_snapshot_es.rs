use crate::common::{
  CTSubNodePath, CoreIOCtx, PaxosGroupId, PaxosGroupIdTrait, QueryId, RemoteLeaderChangedPLm,
  SlaveIOCtx, TNodePath, TabletGroupId,
};
use crate::expression::range_might_intersect_row_region;
use crate::finish_query_rm_es::FinishQueryRMES;
use crate::message as msg;
use crate::server::ServerContextBase;
use crate::shard_split_tm_es::STRange;
use crate::slave::{SlaveContext, SlavePLm};
use crate::storage::{compute_range_storage, remove_range, GenericMVTable};
use crate::tablet::{ShardingSnapshot, TabletConfig, TabletContext, TabletForwardMsg, TabletPLm};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, Bound};

// -----------------------------------------------------------------------------------------------
//  PLms
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardingConfirmedPLm {
  query_id: QueryId,
}

// -----------------------------------------------------------------------------------------------
//  ShardingSnapshotES
// -----------------------------------------------------------------------------------------------

pub enum ShardingSnapshotAction {
  Wait,
  Exit,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
enum State {
  Follower,
  WaitingPreparedWrites,
  ShardingSnapshotSent,
  InsertingShardingConfirmed,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardingSnapshotES {
  pub query_id: QueryId,
  /// The Target to send the `ShardingSnapshot` to.
  target: STRange,
  /// If this is `true`, the snapshot is sent to Slave to create the `target`. Otherwise,
  /// the `target` already exists.
  is_new: bool,
  state: State,
}

impl ShardingSnapshotES {
  pub fn create_split<IO: CoreIOCtx>(
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    finish_query_ess: &BTreeMap<QueryId, FinishQueryRMES>,
    query_id: QueryId,
    target: STRange,
  ) -> ShardingSnapshotES {
    let mut es = ShardingSnapshotES { query_id, target, is_new: false, state: State::Follower };
    es.start(ctx, io_ctx, finish_query_ess);
    es
  }

  fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    finish_query_ess: &BTreeMap<QueryId, FinishQueryRMES>,
  ) -> ShardingSnapshotAction {
    if ctx.is_leader() {
      self.advance_prepared(ctx, io_ctx, finish_query_ess);
    } else {
      self.state = State::Follower;
    }

    ShardingSnapshotAction::Wait
  }

  fn send_sharding_snapshot<IO: CoreIOCtx>(&mut self, ctx: &mut TabletContext, io_ctx: &mut IO) {
    // Construct the ShardingSnapshot
    let snapshot = ShardingSnapshot {
      this_tid: self.target.tid.clone(),
      this_table_path: ctx.this_table_path.clone(),
      this_sharding_gen: ctx.this_sharding_gen.clone(),
      this_table_key_range: self.target.range.clone(),
      storage: compute_range_storage(&ctx.storage, &self.target.range),
      table_schema: ctx.table_schema.clone(),
      presence_timestamp: ctx.presence_timestamp.clone(),
      committed_writes: ctx.committed_writes.clone(),
      read_protected: ctx.read_protected.clone(),
    };

    // Send the Snapshot
    let node_path = ctx.mk_node_path();
    ctx.send_to_slave_common(
      io_ctx,
      self.target.sid.clone(),
      msg::SlaveRemotePayload::ShardingMessage(msg::ShardingMessage {
        query_id: self.query_id.clone(),
        node_path,
        snapshot,
      }),
    );
  }

  fn advance_prepared<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    finish_query_ess: &BTreeMap<QueryId, FinishQueryRMES>,
  ) {
    let ready_to_send = (|| -> bool {
      // We compute if all `FinishQueryESs` with old `ShardingGen` are done
      for (_, es) in finish_query_ess {
        match es {
          FinishQueryRMES::Committed => {}
          FinishQueryRMES::Aborted => {}
          FinishQueryRMES::Paxos2PCRMExecOuter(es) => {
            if es.inner.sharding_gen < ctx.this_sharding_gen {
              return false;
            }
          }
          _ => {}
        }
      }

      // Then, we check that all ReadRegions in `(waiting/inserting)_read_protected` are within
      // the new TabletKeyRange here. To do this, we simply see if ReadRegion
      // intersects with the part of the TabletKeyRange that is being sent off.
      let unpersisted_read_protected =
        ctx.waiting_read_protected.iter().chain(ctx.inserting_read_protected.iter());
      for (_, reqs) in unpersisted_read_protected {
        for req in reqs {
          if range_might_intersect_row_region(
            &ctx.table_schema.key_cols,
            &self.target.range,
            &req.read_region.row_region,
          ) {
            return false;
          }
        }
      }

      true
    })();

    // If so, construct and send the snapshot. Either way, advance the state.
    if ready_to_send {
      self.send_sharding_snapshot(ctx, io_ctx);
      self.state = State::ShardingSnapshotSent;
    } else {
      self.state = State::WaitingPreparedWrites;
    }
  }

  /// In order to figure out if all `FinishQueryES`s and `(waiting/inserting)_read_protected`
  /// that must be finished are finished, we check every time a `TabletBundle` is inserted.
  /// Note: A less wasteful scheme might be possible later.
  pub fn handle_bundle_processed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    finish_query_ess: &BTreeMap<QueryId, FinishQueryRMES>,
  ) -> ShardingSnapshotAction {
    match &self.state {
      State::WaitingPreparedWrites => {
        self.advance_prepared(ctx, io_ctx, finish_query_ess);
      }
      _ => {}
    }
    ShardingSnapshotAction::Wait
  }

  pub fn handle_msg(
    &mut self,
    ctx: &mut TabletContext,
    confirm: msg::ShardingConfirmed,
  ) -> ShardingSnapshotAction {
    match &self.state {
      State::ShardingSnapshotSent => {
        ctx
          .tablet_bundle
          .push(TabletPLm::ShardingConfirmedPLm(ShardingConfirmedPLm { query_id: confirm.qid }));
        self.state = State::InsertingShardingConfirmed;
      }
      _ => {
        debug_assert!(false);
      }
    }
    ShardingSnapshotAction::Wait
  }

  /// This function returns `true` iff this ES is finished.
  pub fn handle_plm(
    &mut self,
    ctx: &mut TabletContext,
    _: ShardingConfirmedPLm,
  ) -> ShardingSnapshotAction {
    match &self.state {
      State::InsertingShardingConfirmed | State::Follower => {
        // Remove all the storage data that this Tablet no longer manages.
        let remaining = remove_range(&mut ctx.storage, &self.target.range);
        debug_assert!(remaining.is_empty());
        ShardingSnapshotAction::Exit
      }
      _ => {
        debug_assert!(false);
        ShardingSnapshotAction::Wait
      }
    }
  }

  pub fn handle_lc<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    finish_query_ess: &BTreeMap<QueryId, FinishQueryRMES>,
  ) -> ShardingSnapshotAction {
    match &self.state {
      State::Follower => {
        if ctx.is_leader() {
          self.advance_prepared(ctx, io_ctx, finish_query_ess);
        }
      }
      State::WaitingPreparedWrites
      | State::ShardingSnapshotSent
      | State::InsertingShardingConfirmed => self.state = State::Follower,
    }
    ShardingSnapshotAction::Wait
  }

  pub fn handle_rlc<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> ShardingSnapshotAction {
    match &self.state {
      State::ShardingSnapshotSent => {
        // If the Leader that changed was of the target SlaveGroupId, we resend the snapshot.
        if remote_leader_changed.gid == self.target.sid.to_gid() {
          self.send_sharding_snapshot(ctx, io_ctx);
        }
      }
      _ => {}
    }
    ShardingSnapshotAction::Wait
  }

  /// Construct the version of `ShardingSnapshotES` that would result by losing Leadership.
  pub fn reconfig_snapshot(&self) -> ShardingSnapshotES {
    let mut es = self.clone();
    es.state = State::Follower;
    es
  }
}
