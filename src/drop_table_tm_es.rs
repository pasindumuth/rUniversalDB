use crate::alter_table_tm_es::{get_rms, maybe_respond_dead};
use crate::common::{MasterIOCtx, RemoteLeaderChangedPLm};
use crate::create_table_tm_es::ResponseData;
use crate::master::{plm, MasterContext, MasterPLm};
use crate::model::common::{
  EndpointId, QueryId, RequestId, TNodePath, TSubNodePath, TablePath, TabletGroupId, Timestamp,
};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use std::cmp::max;
use std::collections::HashSet;

// -----------------------------------------------------------------------------------------------
//  DropTableTMES
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
pub enum DropTableTMS {
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
pub struct DropTableTMES {
  // Response data
  pub response_data: Option<ResponseData>,

  // DropTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,

  // STMPaxos2PCTM state
  pub state: DropTableTMS,
}

#[derive(Debug)]
pub enum DropTableTMAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl DropTableTMES {
  // STMPaxos2PC messages

  pub fn handle_prepared<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: msg::DropTablePrepared,
  ) -> DropTableTMAction {
    match &mut self.state {
      DropTableTMS::Preparing(preparing) => {
        if preparing.rms_remaining.remove(&prepared.rm) {
          preparing.prepared_timestamps.push(prepared.timestamp.clone());
          if preparing.rms_remaining.is_empty() {
            // All RMs have prepared
            let mut timestamp_hint = io_ctx.now();
            for timestamp in &preparing.prepared_timestamps {
              timestamp_hint = max(timestamp_hint, *timestamp);
            }
            ctx.master_bundle.plms.push(MasterPLm::DropTableTMCommitted(
              plm::DropTableTMCommitted { query_id: self.query_id.clone(), timestamp_hint },
            ));
            self.state = DropTableTMS::InsertingTMCommitted;
          }
        }
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }

  pub fn handle_aborted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> DropTableTMAction {
    match &mut self.state {
      DropTableTMS::Preparing(_) => {
        ctx.master_bundle.plms.push(MasterPLm::DropTableTMAborted(plm::DropTableTMAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = DropTableTMS::InsertingTMAborted;
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }

  pub fn handle_close_confirmed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    closed: msg::DropTableCloseConfirm,
  ) -> DropTableTMAction {
    match &mut self.state {
      DropTableTMS::Committed(committed) => {
        if committed.rms_remaining.remove(&closed.rm) {
          if committed.rms_remaining.is_empty() {
            // All RMs have committed
            ctx.master_bundle.plms.push(MasterPLm::DropTableTMClosed(plm::DropTableTMClosed {
              query_id: self.query_id.clone(),
            }));
            self.state = DropTableTMS::InsertingTMClosed(InsertingTMClosed::Committed(
              committed.commit_timestamp,
            ));
          }
        }
      }
      DropTableTMS::Aborted(aborted) => {
        if aborted.rms_remaining.remove(&closed.rm) {
          if aborted.rms_remaining.is_empty() {
            // All RMs have aborted
            ctx.master_bundle.plms.push(MasterPLm::DropTableTMClosed(plm::DropTableTMClosed {
              query_id: self.query_id.clone(),
            }));
            self.state = DropTableTMS::InsertingTMClosed(InsertingTMClosed::Aborted);
          }
        }
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }

  // STMPaxos2PC PLm Insertions

  /// Change state to `Preparing` and broadcast `DropTablePrepare` to the RMs.
  fn advance_to_prepared<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms::<IO>(ctx, &self.table_path) {
      ctx.ctx(io_ctx).send_to_t(
        rm.clone(),
        msg::TabletMessage::DropTablePrepare(msg::DropTablePrepare {
          query_id: self.query_id.clone(),
        }),
      );
      rms_remaining.insert(rm);
    }
    let prepared = Preparing { prepared_timestamps: Vec::new(), rms_remaining };
    self.state = DropTableTMS::Preparing(prepared);
  }

  pub fn handle_prepared_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::InsertTMPreparing => {
        self.advance_to_prepared(ctx, io_ctx);
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }

  /// Change state to `Aborted` and broadcast `DropTableAbort` to the RMs.
  fn advance_to_aborted<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms::<IO>(ctx, &self.table_path) {
      ctx.ctx(io_ctx).send_to_t(
        rm.clone(),
        msg::TabletMessage::DropTableAbort(msg::DropTableAbort { query_id: self.query_id.clone() }),
      );
      rms_remaining.insert(rm);
    }

    self.state = DropTableTMS::Aborted(Aborted { rms_remaining });
  }

  pub fn handle_aborted_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Follower(_) => {
        self.state = DropTableTMS::Follower(Follower::Aborted);
      }
      DropTableTMS::InsertingTMAborted => {
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
    DropTableTMAction::Wait
  }

  /// Apply this `alter_op` to the system and returned the commit `Timestamp` (that is
  /// resolved from the `timestamp_hint` and from GossipData).
  fn apply_drop<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    committed_plm: plm::DropTableTMCommitted,
  ) -> Timestamp {
    // Compute the resolved timestamp
    let mut commit_timestamp = committed_plm.timestamp_hint;
    commit_timestamp = max(commit_timestamp, ctx.table_generation.get_lat(&self.table_path) + 1);

    // Update Gossip Gen
    ctx.gen.0 += 1;

    // Update `table_generation`
    ctx.table_generation.write(&self.table_path, None, commit_timestamp);

    commit_timestamp
  }

  /// Change state to `Committed` and broadcast `DropTableCommit` to the RMs.
  fn advance_to_committed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    commit_timestamp: Timestamp,
  ) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms::<IO>(ctx, &self.table_path) {
      ctx.ctx(io_ctx).send_to_t(
        rm.clone(),
        msg::TabletMessage::DropTableCommit(msg::DropTableCommit {
          query_id: self.query_id.clone(),
          timestamp: commit_timestamp,
        }),
      );
      rms_remaining.insert(rm);
    }

    self.state = DropTableTMS::Committed(Committed { commit_timestamp, rms_remaining });
  }

  pub fn handle_committed_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    committed_plm: plm::DropTableTMCommitted,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Follower(_) => {
        let commit_timestamp = self.apply_drop(ctx, io_ctx, committed_plm);
        self.state = DropTableTMS::Follower(Follower::Committed(commit_timestamp));
      }
      DropTableTMS::InsertingTMCommitted => {
        let commit_timestamp = self.apply_drop(ctx, io_ctx, committed_plm);

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
    DropTableTMAction::Wait
  }

  pub fn handle_closed_plm<IO: MasterIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Follower(_) => DropTableTMAction::Exit,
      DropTableTMS::InsertingTMClosed(_) => DropTableTMAction::Exit,
      _ => DropTableTMAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::WaitingInsertTMPrepared => {
        ctx.master_bundle.plms.push(MasterPLm::DropTableTMPrepared(plm::DropTableTMPrepared {
          query_id: self.query_id.clone(),
          table_path: self.table_path.clone(),
        }));
        self.state = DropTableTMS::InsertTMPreparing;
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }

  pub fn leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Follower(follower) => {
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
        DropTableTMAction::Wait
      }
      DropTableTMS::Start
      | DropTableTMS::InsertTMPreparing
      | DropTableTMS::WaitingInsertTMPrepared => {
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        DropTableTMAction::Exit
      }
      DropTableTMS::Preparing(_)
      | DropTableTMS::InsertingTMCommitted
      | DropTableTMS::InsertingTMAborted => {
        self.state = DropTableTMS::Follower(Follower::Preparing);
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        DropTableTMAction::Wait
      }
      DropTableTMS::Committed(committed) => {
        self.state = DropTableTMS::Follower(Follower::Committed(committed.commit_timestamp));
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        DropTableTMAction::Wait
      }
      DropTableTMS::Aborted(_) => {
        self.state = DropTableTMS::Follower(Follower::Aborted);
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        DropTableTMAction::Wait
      }
      DropTableTMS::InsertingTMClosed(tm_closed) => {
        self.state = DropTableTMS::Follower(match tm_closed {
          InsertingTMClosed::Committed(timestamp) => Follower::Committed(timestamp.clone()),
          InsertingTMClosed::Aborted => Follower::Aborted,
        });
        maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
        DropTableTMAction::Wait
      }
    }
  }

  pub fn remote_leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Preparing(preparing) => {
        for rm in &preparing.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Prepare.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_t(
              rm.clone(),
              msg::TabletMessage::DropTablePrepare(msg::DropTablePrepare {
                query_id: self.query_id.clone(),
              }),
            );
          }
        }
      }
      DropTableTMS::Committed(committed) => {
        for rm in &committed.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Commit.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_t(
              rm.clone(),
              msg::TabletMessage::DropTableCommit(msg::DropTableCommit {
                query_id: self.query_id.clone(),
                timestamp: committed.commit_timestamp,
              }),
            );
          }
        }
      }
      DropTableTMS::Aborted(aborted) => {
        for rm in &aborted.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Abort.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_t(
              rm.clone(),
              msg::TabletMessage::DropTableAbort(msg::DropTableAbort {
                query_id: self.query_id.clone(),
              }),
            );
          }
        }
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }
}
