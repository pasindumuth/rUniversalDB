use crate::common::{MasterIOCtx, RemoteLeaderChangedPLm};
use crate::create_table_tm_es::ResponseData;
use crate::master::{plm, MasterContext, MasterPLm};
use crate::model::common::{
  proc, EndpointId, Gen, QueryId, RequestId, TNodePath, TSubNodePath, TablePath, TabletGroupId,
  Timestamp,
};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use std::cmp::max;
use std::collections::HashSet;

// -----------------------------------------------------------------------------------------------
//  AlterTableES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum Follower {
  Preparing,
  Committed(Timestamp),
  Aborted,
}

#[derive(Debug)]
pub struct Preparing {
  /// The `Timestamp`s sent back by the RMs.
  prepared_timestamps: Vec<Timestamp>,
  /// The set of RMs that still have not prepared.
  rms_remaining: HashSet<TNodePath>,
}

#[derive(Debug)]
pub struct Committed {
  /// The `Timestamp`s at which to commit.
  commit_timestamp: Timestamp,
  /// The set of RMs that still have not committed.
  rms_remaining: HashSet<TNodePath>,
}

#[derive(Debug)]
pub struct Aborted {
  /// The set of RMs that still have not aborted.
  rms_remaining: HashSet<TNodePath>,
}

#[derive(Debug)]
pub enum InsertingTMClosed {
  Committed(Timestamp),
  Aborted,
}

#[derive(Debug)]
pub enum AlterTableTMS {
  Start,
  Follower(Follower),
  WaitingInsertTMPrepared,
  InsertTMPreparing,
  Preparing(Preparing),
  InsertingTMCommitted,
  Committed(Committed),
  InsertingTMAborted,
  Aborted(Aborted),
  InsertingTMClosed(InsertingTMClosed),
}

#[derive(Debug)]
pub struct AlterTableTMES {
  // Response data
  pub response_data: Option<ResponseData>,

  // AlterTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,
  pub alter_op: proc::AlterOp,

  // STMPaxos2PCTM state
  pub state: AlterTableTMS,
}

#[derive(Debug)]
pub enum AlterTableTMAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl AlterTableTMES {
  // STMPaxos2PC messages

  pub fn handle_prepared<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: msg::AlterTablePrepared,
  ) -> AlterTableTMAction {
    match &mut self.state {
      AlterTableTMS::Preparing(preparing) => {
        if preparing.rms_remaining.remove(&prepared.rm) {
          preparing.prepared_timestamps.push(prepared.timestamp.clone());
          if preparing.rms_remaining.is_empty() {
            // All RMs have prepared
            let mut timestamp_hint = io_ctx.now();
            for timestamp in &preparing.prepared_timestamps {
              timestamp_hint = max(timestamp_hint, *timestamp);
            }
            ctx.master_bundle.plms.push(MasterPLm::AlterTableTMCommitted(
              plm::AlterTableTMCommitted { query_id: self.query_id.clone(), timestamp_hint },
            ));
            self.state = AlterTableTMS::InsertingTMCommitted;
          }
        }
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }

  pub fn handle_aborted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> AlterTableTMAction {
    match &mut self.state {
      AlterTableTMS::Preparing(_) => {
        ctx.master_bundle.plms.push(MasterPLm::AlterTableTMAborted(plm::AlterTableTMAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = AlterTableTMS::InsertingTMAborted;
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }

  pub fn handle_close_confirmed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    closed: msg::AlterTableCloseConfirm,
  ) -> AlterTableTMAction {
    match &mut self.state {
      AlterTableTMS::Committed(committed) => {
        if committed.rms_remaining.remove(&closed.rm) {
          if committed.rms_remaining.is_empty() {
            // All RMs have committed
            ctx.master_bundle.plms.push(MasterPLm::AlterTableTMClosed(plm::AlterTableTMClosed {
              query_id: self.query_id.clone(),
            }));
            self.state = AlterTableTMS::InsertingTMClosed(InsertingTMClosed::Committed(
              committed.commit_timestamp,
            ));
          }
        }
      }
      AlterTableTMS::Aborted(aborted) => {
        if aborted.rms_remaining.remove(&closed.rm) {
          if aborted.rms_remaining.is_empty() {
            // All RMs have aborted
            ctx.master_bundle.plms.push(MasterPLm::AlterTableTMClosed(plm::AlterTableTMClosed {
              query_id: self.query_id.clone(),
            }));
            self.state = AlterTableTMS::InsertingTMClosed(InsertingTMClosed::Aborted);
          }
        }
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }

  // STMPaxos2PC PLm Insertions

  /// Change state to `Preparing` and broadcast `AlterTablePrepare` to the RMs.
  fn advance_to_prepared<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms(ctx, io_ctx, &self.table_path) {
      ctx.ctx(io_ctx).send_to_t(
        rm.clone(),
        msg::TabletMessage::AlterTablePrepare(msg::AlterTablePrepare {
          query_id: self.query_id.clone(),
          alter_op: self.alter_op.clone(),
        }),
      );
      rms_remaining.insert(rm);
    }
    let prepared = Preparing { prepared_timestamps: Vec::new(), rms_remaining };
    self.state = AlterTableTMS::Preparing(prepared);
  }

  pub fn handle_prepared_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> AlterTableTMAction {
    match &self.state {
      AlterTableTMS::InsertTMPreparing => {
        self.advance_to_prepared(ctx, io_ctx);
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }

  /// Change state to `Aborted` and broadcast `AlterTableAbort` to the RMs.
  fn advance_to_aborted<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms(ctx, io_ctx, &self.table_path) {
      ctx.ctx(io_ctx).send_to_t(
        rm.clone(),
        msg::TabletMessage::AlterTableAbort(msg::AlterTableAbort {
          query_id: self.query_id.clone(),
        }),
      );
      rms_remaining.insert(rm);
    }

    self.state = AlterTableTMS::Aborted(Aborted { rms_remaining });
  }

  pub fn handle_aborted_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> AlterTableTMAction {
    match &self.state {
      AlterTableTMS::Follower(_) => {
        self.state = AlterTableTMS::Follower(Follower::Aborted);
      }
      AlterTableTMS::InsertingTMAborted => {
        // Send a abort response to the External
        if let Some(response_data) = &self.response_data {
          ctx.external_request_id_map.remove(&response_data.request_id);
          io_ctx.send(
            &response_data.sender_eid,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
              msg::ExternalDDLQueryAborted {
                request_id: response_data.request_id.clone(),
                payload: msg::ExternalDDLQueryAbortData::Unknown,
              },
            )),
          );
          self.response_data = None;
        }

        self.advance_to_aborted(ctx, io_ctx);
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }

  /// Apply this `alter_op` to the system and returned the commit `Timestamp` (that is
  /// resolved from the `timestamp_hint` and from GossipData).
  fn apply_alter_op<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    committed_plm: plm::AlterTableTMCommitted,
  ) -> Timestamp {
    let gen = ctx.table_generation.get_last_version(&self.table_path).unwrap();
    let table_schema = ctx.db_schema.get_mut(&(self.table_path.clone(), gen.clone())).unwrap();

    // Compute the resolved timestamp
    let mut commit_timestamp = committed_plm.timestamp_hint;
    commit_timestamp = max(commit_timestamp, ctx.table_generation.get_lat(&self.table_path) + 1);
    commit_timestamp =
      max(commit_timestamp, table_schema.val_cols.get_lat(&self.alter_op.col_name) + 1);

    // Apply the AlterOp
    ctx.gen.0 += 1;
    ctx.table_generation.update_lat(&self.table_path, commit_timestamp);
    table_schema.val_cols.write(
      &self.alter_op.col_name,
      self.alter_op.maybe_col_type.clone(),
      commit_timestamp,
    );

    commit_timestamp
  }

  /// Change state to `Committed` and broadcast `AlterTableCommit` to the RMs.
  fn advance_to_committed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    commit_timestamp: Timestamp,
  ) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms(ctx, io_ctx, &self.table_path) {
      ctx.ctx(io_ctx).send_to_t(
        rm.clone(),
        msg::TabletMessage::AlterTableCommit(msg::AlterTableCommit {
          query_id: self.query_id.clone(),
          timestamp: commit_timestamp,
        }),
      );
      rms_remaining.insert(rm);
    }

    self.state = AlterTableTMS::Committed(Committed { commit_timestamp, rms_remaining });
  }

  pub fn handle_committed_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    committed_plm: plm::AlterTableTMCommitted,
  ) -> AlterTableTMAction {
    match &self.state {
      AlterTableTMS::Follower(_) => {
        let commit_timestamp = self.apply_alter_op(ctx, io_ctx, committed_plm);
        self.state = AlterTableTMS::Follower(Follower::Committed(commit_timestamp));
      }
      AlterTableTMS::InsertingTMCommitted => {
        let commit_timestamp = self.apply_alter_op(ctx, io_ctx, committed_plm);

        // Send a success response to the External
        if let Some(response_data) = &self.response_data {
          ctx.external_request_id_map.remove(&response_data.request_id);
          io_ctx.send(
            &response_data.sender_eid,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(
              msg::ExternalDDLQuerySuccess {
                request_id: response_data.request_id.clone(),
                timestamp: commit_timestamp,
              },
            )),
          );
          self.response_data = None;
        }

        self.advance_to_committed(ctx, io_ctx, commit_timestamp);

        // Broadcast a GossipData
        ctx.broadcast_gossip(io_ctx);
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }

  pub fn handle_closed_plm<IO: MasterIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> AlterTableTMAction {
    match &self.state {
      AlterTableTMS::Follower(_) => AlterTableTMAction::Exit,
      AlterTableTMS::InsertingTMClosed(_) => AlterTableTMAction::Exit,
      _ => AlterTableTMAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> AlterTableTMAction {
    match &self.state {
      AlterTableTMS::WaitingInsertTMPrepared => {
        ctx.master_bundle.plms.push(MasterPLm::AlterTableTMPrepared(plm::AlterTableTMPrepared {
          query_id: self.query_id.clone(),
          table_path: self.table_path.clone(),
          alter_op: self.alter_op.clone(),
        }));
        self.state = AlterTableTMS::InsertTMPreparing;
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }

  pub fn leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> AlterTableTMAction {
    match &self.state {
      AlterTableTMS::Start => AlterTableTMAction::Wait,
      AlterTableTMS::Follower(follower) => {
        if ctx.is_leader() {
          match follower {
            Follower::Preparing => {
              self.advance_to_prepared(ctx, io_ctx);
            }
            Follower::Committed(commit_timestamp) => {
              self.advance_to_committed(ctx, io_ctx, commit_timestamp.clone());
            }
            Follower::Aborted => {
              self.advance_to_aborted(ctx, io_ctx);
            }
          }
        }
        AlterTableTMAction::Wait
      }
      AlterTableTMS::WaitingInsertTMPrepared => {
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        AlterTableTMAction::Exit
      }
      AlterTableTMS::InsertTMPreparing => {
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        AlterTableTMAction::Exit
      }
      AlterTableTMS::Preparing(_)
      | AlterTableTMS::InsertingTMCommitted
      | AlterTableTMS::InsertingTMAborted => {
        self.state = AlterTableTMS::Follower(Follower::Preparing);
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        AlterTableTMAction::Wait
      }
      AlterTableTMS::Committed(committed) => {
        self.state = AlterTableTMS::Follower(Follower::Committed(committed.commit_timestamp));
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        AlterTableTMAction::Wait
      }
      AlterTableTMS::Aborted(_) => {
        self.state = AlterTableTMS::Follower(Follower::Aborted);
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        AlterTableTMAction::Wait
      }
      AlterTableTMS::InsertingTMClosed(tm_closed) => {
        self.state = AlterTableTMS::Follower(match tm_closed {
          InsertingTMClosed::Committed(timestamp) => Follower::Committed(timestamp.clone()),
          InsertingTMClosed::Aborted => Follower::Aborted,
        });
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        AlterTableTMAction::Wait
      }
    }
  }

  pub fn remote_leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> AlterTableTMAction {
    match &self.state {
      AlterTableTMS::Preparing(preparing) => {
        for rm in &preparing.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Prepare.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_t(
              rm.clone(),
              msg::TabletMessage::AlterTablePrepare(msg::AlterTablePrepare {
                query_id: self.query_id.clone(),
                alter_op: self.alter_op.clone(),
              }),
            );
          }
        }
      }
      AlterTableTMS::Committed(committed) => {
        for rm in &committed.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Commit.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_t(
              rm.clone(),
              msg::TabletMessage::AlterTableCommit(msg::AlterTableCommit {
                query_id: self.query_id.clone(),
                timestamp: committed.commit_timestamp,
              }),
            );
          }
        }
      }
      AlterTableTMS::Aborted(aborted) => {
        for rm in &aborted.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Abort.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_t(
              rm.clone(),
              msg::TabletMessage::AlterTableAbort(msg::AlterTableAbort {
                query_id: self.query_id.clone(),
              }),
            );
          }
        }
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }
}

/// This returns the current set of RMs associated with the given `TablePath`. Recall that while
/// the ES is alive, we ensure that this is idempotent.
pub fn get_rms<IO: MasterIOCtx>(
  ctx: &mut MasterContext,
  _: &mut IO,
  table_path: &TablePath,
) -> Vec<TNodePath> {
  let gen = ctx.table_generation.get_last_version(table_path).unwrap();
  let mut rms = Vec::<TNodePath>::new();
  for (_, tid) in ctx.sharding_config.get(&(table_path.clone(), gen.clone())).unwrap() {
    let sid = ctx.tablet_address_config.get(&tid).unwrap().clone();
    rms.push(TNodePath { sid, sub: TSubNodePath::Tablet(tid.clone()) });
  }
  rms
}

/// Send a response back to the External, informing them that the current Master Leader died.
pub fn maybe_respond_dead<IO: MasterIOCtx>(
  response_data: &mut Option<ResponseData>,
  ctx: &mut MasterContext,
  io_ctx: &mut IO,
) {
  if let Some(data) = response_data {
    ctx.external_request_id_map.remove(&data.request_id);
    io_ctx.send(
      &data.sender_eid,
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
        msg::ExternalDDLQueryAborted {
          request_id: data.request_id.clone(),
          payload: msg::ExternalDDLQueryAbortData::NodeDied,
        },
      )),
    );
    *response_data = None;
  }
}
