use crate::common::{Clock, IOTypes, NetworkOut, RemoteLeaderChangedPLm};
use crate::create_table_tm_es::ResponseData;
use crate::master::{plm, MasterContext, MasterPLm};
use crate::model::common::{
  EndpointId, QueryId, RequestId, TNodePath, TSubNodePath, TablePath, TabletGroupId, Timestamp,
};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use std::cmp::max;
use std::collections::{HashMap, HashSet};

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

  pub fn handle_prepared<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
    prepared: msg::DropTablePrepared,
  ) -> DropTableTMAction {
    match &mut self.state {
      DropTableTMS::Preparing(preparing) => {
        if preparing.rms_remaining.remove(&prepared.rm) {
          preparing.prepared_timestamps.push(prepared.timestamp.clone());
          if preparing.rms_remaining.is_empty() {
            // All RMs have prepared
            let mut timestamp_hint = ctx.clock.now();
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

  pub fn handle_aborted<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> DropTableTMAction {
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

  pub fn handle_close_confirmed<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
    closed: msg::DropTableCloseConfirm,
  ) -> DropTableTMAction {
    match &mut self.state {
      DropTableTMS::Committed(committed) => {
        if committed.rms_remaining.contains(&closed.rm) {
          committed.rms_remaining.remove(&closed.rm);
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
        if aborted.rms_remaining.contains(&closed.rm) {
          aborted.rms_remaining.remove(&closed.rm);
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
  fn advance_to_prepared<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms(ctx, &self.table_path) {
      ctx.ctx().send_to_t(
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

  pub fn handle_prepared_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::InsertTMPreparing => {
        self.advance_to_prepared(ctx);
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }

  /// Change state to `Aborted` and broadcast `DropTableAbort` to the RMs.
  fn advance_to_aborted<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms(ctx, &self.table_path) {
      ctx.ctx().send_to_t(
        rm.clone(),
        msg::TabletMessage::DropTableAbort(msg::DropTableAbort { query_id: self.query_id.clone() }),
      );
      rms_remaining.insert(rm);
    }

    self.state = DropTableTMS::Aborted(Aborted { rms_remaining });
  }

  pub fn handle_aborted_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Follower(_) => {
        self.state = DropTableTMS::Follower(Follower::Aborted);
      }
      DropTableTMS::InsertingTMAborted => {
        // Send a abort response to the External
        if let Some(response_data) = &self.response_data {
          ctx.external_request_id_map.remove(&response_data.request_id);
          ctx.network_output.send(
            &response_data.sender_eid,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
              msg::ExternalDDLQueryAborted {
                request_id: response_data.request_id.clone(),
                payload: msg::ExternalDDLQueryAbortData::Unknown,
              },
            )),
          )
        }

        self.advance_to_aborted(ctx);
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }

  /// Apply this `alter_op` to the system and returned the commit `Timestamp` (that is
  /// resolved from the `timestamp_hint` and from GossipData).
  fn apply_drop<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
    committed_plm: plm::DropTableTMCommitted,
  ) -> Timestamp {
    // Compute the resolved timestamp
    let mut commit_timestamp = committed_plm.timestamp_hint;
    commit_timestamp = max(commit_timestamp, ctx.table_generation.get_lat(&self.table_path) + 1);

    // Apply the Drop
    ctx.gen.0 += 1;
    ctx.table_generation.write(&self.table_path, None, commit_timestamp);

    commit_timestamp
  }

  /// Change state to `Committed` and broadcast `DropTableCommit` to the RMs.
  fn advance_to_committed<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
    commit_timestamp: Timestamp,
  ) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for rm in get_rms(ctx, &self.table_path) {
      ctx.ctx().send_to_t(
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

  pub fn handle_committed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
    committed_plm: plm::DropTableTMCommitted,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Follower(_) => {
        let commit_timestamp = self.apply_drop(ctx, committed_plm);
        self.state = DropTableTMS::Follower(Follower::Committed(commit_timestamp));
      }
      DropTableTMS::InsertingTMCommitted => {
        let commit_timestamp = self.apply_drop(ctx, committed_plm);

        // Send a success response to the External
        if let Some(response_data) = &self.response_data {
          ctx.external_request_id_map.remove(&response_data.request_id);
          ctx.network_output.send(
            &response_data.sender_eid,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(
              msg::ExternalDDLQuerySuccess {
                request_id: response_data.request_id.clone(),
                timestamp: commit_timestamp,
              },
            )),
          )
        }

        // Broadcast a GossipData
        ctx.broadcast_gossip();

        self.advance_to_committed(ctx, commit_timestamp);
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }

  pub fn handle_closed_plm<T: IOTypes>(&mut self, _: &mut MasterContext<T>) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Follower(_) => DropTableTMAction::Exit,
      DropTableTMS::InsertingTMClosed(_) => DropTableTMAction::Exit,
      _ => DropTableTMAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> DropTableTMAction {
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

  pub fn leader_changed<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Start => DropTableTMAction::Wait,
      DropTableTMS::Follower(follower) => {
        if ctx.is_leader() {
          match follower {
            Follower::Preparing => {
              self.advance_to_prepared(ctx);
            }
            Follower::Committed(commit_timestamp) => {
              self.advance_to_committed(ctx, commit_timestamp.clone());
            }
            Follower::Aborted => {
              self.advance_to_aborted(ctx);
            }
          }
        }
        DropTableTMAction::Wait
      }
      DropTableTMS::WaitingInsertTMPrepared => {
        self.maybe_respond_dead(ctx);
        DropTableTMAction::Exit
      }
      DropTableTMS::InsertTMPreparing => {
        self.maybe_respond_dead(ctx);
        DropTableTMAction::Exit
      }
      DropTableTMS::Preparing(_)
      | DropTableTMS::InsertingTMCommitted
      | DropTableTMS::InsertingTMAborted => {
        self.state = DropTableTMS::Follower(Follower::Preparing);
        self.maybe_respond_dead(ctx);
        DropTableTMAction::Wait
      }
      DropTableTMS::Committed(committed) => {
        self.state = DropTableTMS::Follower(Follower::Committed(committed.commit_timestamp));
        self.maybe_respond_dead(ctx);
        DropTableTMAction::Wait
      }
      DropTableTMS::Aborted(_) => {
        self.state = DropTableTMS::Follower(Follower::Aborted);
        self.maybe_respond_dead(ctx);
        DropTableTMAction::Wait
      }
      DropTableTMS::InsertingTMClosed(tm_closed) => {
        self.state = DropTableTMS::Follower(match tm_closed {
          InsertingTMClosed::Committed(timestamp) => Follower::Committed(timestamp.clone()),
          InsertingTMClosed::Aborted => Follower::Aborted,
        });
        self.maybe_respond_dead(ctx);
        DropTableTMAction::Wait
      }
    }
  }

  pub fn remote_leader_changed<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> DropTableTMAction {
    match &self.state {
      DropTableTMS::Preparing(preparing) => {
        for rm in &preparing.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Prepare.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx().send_to_t(
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
            ctx.ctx().send_to_t(
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
            ctx.ctx().send_to_t(
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

  fn maybe_respond_dead<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) {
    if let Some(response_data) = &self.response_data {
      ctx.external_request_id_map.remove(&response_data.request_id);
      ctx.network_output.send(
        &response_data.sender_eid,
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
          msg::ExternalDDLQueryAborted {
            request_id: response_data.request_id.clone(),
            payload: msg::ExternalDDLQueryAbortData::NodeDied,
          },
        )),
      );
      self.response_data = None;
    }
  }
}

/// This returns the current set of RMs used by the `DropTableTMES`. Recall that while
/// the ES is alive, we ensure that this is idempotent.
fn get_rms<T: IOTypes>(ctx: &mut MasterContext<T>, table_path: &TablePath) -> Vec<TNodePath> {
  let gen = ctx.table_generation.get_last_version(table_path).unwrap();
  let mut rms = Vec::<TNodePath>::new();
  for (_, tid) in ctx.sharding_config.get(&(table_path.clone(), gen.clone())).unwrap() {
    let sid = ctx.tablet_address_config.get(&tid).unwrap().clone();
    rms.push(TNodePath { sid, sub: TSubNodePath::Tablet(tid.clone()) });
  }
  rms
}
