use crate::alter_table_tm_es::maybe_respond_dead;
use crate::common::{MasterIOCtx, RemoteLeaderChangedPLm};
use crate::create_table_tm_es::ResponseData;
use crate::master::{MasterContext, MasterPLm};
use crate::model::common::{proc, QueryId, TNodePath, TablePath, Timestamp};
use crate::model::message as msg;
use crate::model::message::TabletMessage;
use crate::server::ServerContextBase;
use crate::stmpaxos2pc::FollowerState::Committed;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PC
// -----------------------------------------------------------------------------------------------

pub trait PayloadTypes: Sized {
  // TM PLm
  type TMPreparedPLm: Debug + Clone;
  type TMCommittedPLm: Debug + Clone;
  type TMAbortedPLm: Debug + Clone;
  type TMClosedPLm: Debug + Clone;

  fn master_prepared_plm(prepared_plm: TMPreparedPLm<Self>) -> MasterPLm;
  fn master_committed_plm(committed_plm: TMCommittedPLm<Self>) -> MasterPLm;
  fn master_aborted_plm(aborted_plm: TMAbortedPLm<Self>) -> MasterPLm;
  fn master_closed_plm(closed_plm: TMClosedPLm<Self>) -> MasterPLm;

  // RM PLm
  type RMPreparedPLm: Debug + Clone;
  type RMCommittedPLm: Debug + Clone;
  type RMAbortedPLm: Debug + Clone;

  // TM-to-RM Messages
  type Prepare: Debug + Clone;
  type Abort: Debug + Clone;
  type Commit: Debug + Clone;

  fn tablet_prepare(prepare: Prepare<Self>) -> msg::TabletMessage;
  fn tablet_commit(commit: Commit<Self>) -> msg::TabletMessage;
  fn tablet_abort(abort: Abort<Self>) -> msg::TabletMessage;

  // RM-to-TM Messages
  type Prepared: Debug + Clone;
  type Aborted: Debug + Clone;
  type Closed: Debug + Clone;
}

// TM PLm
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TMPreparedPLm<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::TMPreparedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TMCommittedPLm<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::TMCommittedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TMAbortedPLm<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::TMAbortedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TMClosedPLm<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::TMClosedPLm,
}

// RM PLm
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RMPreparedPLm<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::RMPreparedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RMCommittedPLm<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::RMCommittedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RMAbortedPLm<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::RMAbortedPLm,
}

// TM-to-RM Messages
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Prepare<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::Prepare,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Abort<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::Abort,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Commit<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::Commit,
}

// RM-to-TM Messages
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Prepared<T: PayloadTypes> {
  pub query_id: QueryId,
  pub rm: TNodePath,
  pub payload: T::Prepared,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Aborted<T: PayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::Aborted,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Closed<T: PayloadTypes> {
  pub query_id: QueryId,
  pub rm: TNodePath,
  pub payload: T::Closed,
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCTM Outer
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct PreparingSt<T: PayloadTypes> {
  rms_remaining: HashSet<TNodePath>,
  prepared: HashMap<TNodePath, T::Prepared>,
}

#[derive(Debug)]
pub struct CommittedSt {
  rms_remaining: HashSet<TNodePath>,
}

#[derive(Debug)]
pub struct AbortedSt {
  rms_remaining: HashSet<TNodePath>,
}

#[derive(Debug)]
pub enum FollowerState<T: PayloadTypes> {
  Preparing(HashMap<TNodePath, T::Prepare>),
  Committed(HashMap<TNodePath, T::Commit>),
  Aborted(HashMap<TNodePath, T::Abort>),
}

#[derive(Debug)]
pub enum State<T: PayloadTypes> {
  Following,
  WaitingInsertTMPrepared,
  InsertTMPreparing,
  Preparing(PreparingSt<T>),
  InsertingTMCommitted,
  Committed(CommittedSt),
  InsertingTMAborted,
  Aborted(AbortedSt),
  InsertingTMClosed,
}

pub enum STMPaxos2PCAction {
  Wait,
  Exit,
}

#[derive(Debug)]
pub struct STMPaxos2PCOuter<T: PayloadTypes, InnerT> {
  query_id: QueryId,
  follower: FollowerState<T>,
  state: State<T>,
  inner: InnerT,
}

impl<T: PayloadTypes, InnerT: STMPaxos2PCTMInner<T>> STMPaxos2PCOuter<T, InnerT> {
  // STMPaxos2PC messages

  pub fn handle_prepared<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: Prepared<T>,
  ) -> STMPaxos2PCAction {
    match &mut self.state {
      State::Preparing(preparing) => {
        if preparing.rms_remaining.remove(&prepared.rm) {
          preparing.prepared.insert(prepared.rm.clone(), prepared.payload);
          if preparing.rms_remaining.is_empty() {
            let committed_plm = T::master_committed_plm(TMCommittedPLm {
              query_id: self.query_id.clone(),
              payload: self.inner.mk_committed_plm(ctx, io_ctx, &preparing.prepared),
            });
            ctx.master_bundle.plms.push(committed_plm);
            self.state = State::InsertingTMCommitted;
          }
        }
      }
      _ => {}
    }
    STMPaxos2PCAction::Wait
  }

  pub fn handle_aborted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCAction {
    match &mut self.state {
      State::Preparing(_) => {
        let aborted_plm = T::master_aborted_plm(TMAbortedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_aborted_plm(ctx, io_ctx),
        });
        ctx.master_bundle.plms.push(aborted_plm);
        self.state = State::InsertingTMAborted;
      }
      _ => {}
    }
    STMPaxos2PCAction::Wait
  }

  pub fn handle_close_confirmed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    closed: msg::AlterTableCloseConfirm,
  ) -> STMPaxos2PCAction {
    match &mut self.state {
      State::Committed(committed) => {
        if committed.rms_remaining.remove(&closed.rm) {
          if committed.rms_remaining.is_empty() {
            // All RMs have committed
            let closed_plm = T::master_closed_plm(TMClosedPLm {
              query_id: self.query_id.clone(),
              payload: self.inner.mk_closed_plm(ctx, io_ctx),
            });
            ctx.master_bundle.plms.push(closed_plm);
            self.state = State::InsertingTMClosed;
          }
        }
      }
      State::Aborted(aborted) => {
        if aborted.rms_remaining.remove(&closed.rm) {
          if aborted.rms_remaining.is_empty() {
            // All RMs have aborted
            let closed_plm = T::master_closed_plm(TMClosedPLm {
              query_id: self.query_id.clone(),
              payload: self.inner.mk_closed_plm(ctx, io_ctx),
            });
            ctx.master_bundle.plms.push(closed_plm);
            self.state = State::InsertingTMClosed;
          }
        }
      }
      _ => {}
    }
    STMPaxos2PCAction::Wait
  }

  // STMPaxos2PC PLm Insertions

  /// Change state to `Preparing` and broadcast `Prepare` to the RMs.
  fn advance_to_prepared<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    prepare_payloads: HashMap<TNodePath, T::Prepare>,
  ) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for (rm, payload) in prepare_payloads.clone() {
      let prepare = Prepare { query_id: self.query_id.clone(), payload };
      ctx.ctx(io_ctx).send_to_t(rm.clone(), T::tablet_prepare(prepare));
      rms_remaining.insert(rm);
    }

    self.follower = FollowerState::Preparing(prepare_payloads);
    self.state = State::Preparing(PreparingSt { rms_remaining, prepared: Default::default() });
  }

  pub fn handle_prepared_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCAction {
    match &self.state {
      State::InsertTMPreparing => {
        let prepare_payloads = self.inner.prepared_plm_inserted(ctx, io_ctx);
        self.advance_to_prepared(ctx, io_ctx, prepare_payloads);
      }
      _ => {}
    }
    STMPaxos2PCAction::Wait
  }

  /// Change state to `Committed` and broadcast `AlterTableCommit` to the RMs.
  fn advance_to_committed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    commit_payloads: HashMap<TNodePath, T::Commit>,
  ) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for (rm, payload) in commit_payloads.clone() {
      let commit = Commit { query_id: self.query_id.clone(), payload };
      ctx.ctx(io_ctx).send_to_t(rm.clone(), T::tablet_commit(commit));
      rms_remaining.insert(rm);
    }

    self.follower = FollowerState::Committed(commit_payloads);
    self.state = State::Committed(CommittedSt { rms_remaining });
  }

  pub fn handle_committed_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    committed_plm: TMCommittedPLm<T>,
  ) -> STMPaxos2PCAction {
    match &self.state {
      State::Following => {
        let commit_payloads = self.inner.committed_plm_inserted(ctx, io_ctx, &committed_plm);
        self.follower = FollowerState::Committed(commit_payloads);
      }
      State::InsertingTMCommitted => {
        let commit_payloads = self.inner.committed_plm_inserted(ctx, io_ctx, &committed_plm);
        self.follower = FollowerState::Committed(commit_payloads.clone());

        // Change state to Committed
        self.advance_to_committed(ctx, io_ctx, commit_payloads);

        // Broadcast a GossipData
        ctx.broadcast_gossip(io_ctx);
      }
      _ => {}
    }
    STMPaxos2PCAction::Wait
  }

  /// Change state to `Aborted` and broadcast `AlterTableAbort` to the RMs.
  fn advance_to_aborted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    abort_payloads: HashMap<TNodePath, T::Abort>,
  ) {
    let mut rms_remaining = HashSet::<TNodePath>::new();
    for (rm, payload) in abort_payloads.clone() {
      let abort = Abort { query_id: self.query_id.clone(), payload };
      ctx.ctx(io_ctx).send_to_t(rm.clone(), T::tablet_abort(abort));
      rms_remaining.insert(rm);
    }

    self.follower = FollowerState::Aborted(abort_payloads);
    self.state = State::Aborted(AbortedSt { rms_remaining });
  }

  pub fn handle_aborted_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCAction {
    match &self.state {
      State::Following => {
        let abort_payloads = self.inner.aborted_plm_inserted(ctx, io_ctx);
        self.follower = FollowerState::Aborted(abort_payloads);
      }
      State::InsertingTMAborted => {
        let abort_payloads = self.inner.aborted_plm_inserted(ctx, io_ctx);
        self.follower = FollowerState::Aborted(abort_payloads.clone());

        // Change state to Aborted
        self.advance_to_aborted(ctx, io_ctx, abort_payloads);
      }
      _ => {}
    }
    STMPaxos2PCAction::Wait
  }

  /// Simply return Exit in the appropriate states.
  pub fn handle_closed_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCAction {
    match &self.state {
      State::Following => {
        self.inner.closed_plm_inserted(ctx, io_ctx);
        STMPaxos2PCAction::Exit
      }
      State::InsertingTMClosed => {
        self.inner.closed_plm_inserted(ctx, io_ctx);
        STMPaxos2PCAction::Exit
      }
      _ => STMPaxos2PCAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCAction {
    match &self.state {
      State::WaitingInsertTMPrepared => {
        let prepared = T::master_prepared_plm(TMPreparedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_prepared_plm(ctx, io_ctx),
        });
        ctx.master_bundle.plms.push(prepared);
        self.state = State::InsertTMPreparing;
      }
      _ => {}
    }
    STMPaxos2PCAction::Wait
  }

  pub fn leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCAction {
    match &self.state {
      State::Following => {
        if ctx.is_leader() {
          match &self.follower {
            FollowerState::Preparing(prepare_payloads) => {
              self.advance_to_prepared(ctx, io_ctx, prepare_payloads.clone())
            }
            FollowerState::Committed(commit_payloads) => {
              self.advance_to_committed(ctx, io_ctx, commit_payloads.clone())
            }
            FollowerState::Aborted(abort_payloads) => {
              self.advance_to_aborted(ctx, io_ctx, abort_payloads.clone())
            }
          }
        }
        STMPaxos2PCAction::Wait
      }
      State::WaitingInsertTMPrepared | State::InsertTMPreparing => {
        self.inner.node_died(ctx, io_ctx);
        STMPaxos2PCAction::Exit
      }
      State::Preparing(_)
      | State::InsertingTMCommitted
      | State::Committed(_)
      | State::InsertingTMAborted
      | State::Aborted(_)
      | State::InsertingTMClosed => {
        self.state = State::Following;
        self.inner.node_died(ctx, io_ctx);
        STMPaxos2PCAction::Wait
      }
    }
  }

  pub fn remote_leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> STMPaxos2PCAction {
    match &self.state {
      State::Preparing(preparing) => {
        let prepare_payloads = cast!(FollowerState::Preparing, &self.follower).unwrap();
        for rm in &preparing.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Prepare.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            let payload = prepare_payloads.get(rm).unwrap().clone();
            let prepare = Prepare { query_id: self.query_id.clone(), payload };
            ctx.ctx(io_ctx).send_to_t(rm.clone(), T::tablet_prepare(prepare));
          }
        }
      }
      State::Committed(committed) => {
        let commit_payloads = cast!(FollowerState::Committed, &self.follower).unwrap();
        for rm in &committed.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Commit.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            let payload = commit_payloads.get(rm).unwrap().clone();
            let commit = Commit { query_id: self.query_id.clone(), payload };
            ctx.ctx(io_ctx).send_to_t(rm.clone(), T::tablet_commit(commit));
          }
        }
      }
      State::Aborted(aborted) => {
        let abort_payloads = cast!(FollowerState::Aborted, &self.follower).unwrap();
        for rm in &aborted.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Abort.
          if rm.sid.to_gid() == remote_leader_changed.gid {
            let payload = abort_payloads.get(rm).unwrap().clone();
            let abort = Abort { query_id: self.query_id.clone(), payload };
            ctx.ctx(io_ctx).send_to_t(rm.clone(), T::tablet_abort(abort));
          }
        }
      }
      _ => {}
    }
    STMPaxos2PCAction::Wait
  }
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCTM Inner
// -----------------------------------------------------------------------------------------------

trait STMPaxos2PCTMInner<T: PayloadTypes> {
  /// Called in order to get the `TMPreparedPLm` to insert.
  fn mk_prepared_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> T::TMPreparedPLm;

  /// Called after PreparedPLm is inserted.
  fn prepared_plm_inserted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> HashMap<TNodePath, T::Prepare>;

  /// Called after all RMs have Prepared.
  fn mk_committed_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: &HashMap<TNodePath, T::Prepared>,
  ) -> T::TMCommittedPLm;

  /// Called after CommittedPLm is inserted.
  fn committed_plm_inserted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    committed_plm: &TMCommittedPLm<T>,
  ) -> HashMap<TNodePath, T::Commit>;

  /// Called if one of the RMs returned Aborted.
  fn mk_aborted_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> T::TMAbortedPLm;

  /// Called after AbortedPLm is inserted.
  fn aborted_plm_inserted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> HashMap<TNodePath, T::Abort>;

  /// Called after all RMs have processed the `Commit` or or `Abort` message.
  fn mk_closed_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> T::TMClosedPLm;

  /// Called after all RMs have processed the `Commit` or or `Abort` message.
  fn closed_plm_inserted<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO);

  // This is called when the node died.
  fn node_died<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO);
}

// -----------------------------------------------------------------------------------------------
//  AlterTable Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct AlterTableTMInner {
  // Response data
  pub response_data: Option<ResponseData>,

  // AlterTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,
  pub alter_op: proc::AlterOp,

  // RMs
  pub rms: Vec<TNodePath>,
}

impl STMPaxos2PCTMInner<AlterTablePayloadTypes> for AlterTableTMInner {
  fn mk_prepared_plm<IO: MasterIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> plm::AlterTableTMPrepared {
    plm::AlterTableTMPrepared {
      table_path: self.table_path.clone(),
      alter_op: self.alter_op.clone(),
    }
  }

  fn prepared_plm_inserted<IO: MasterIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> HashMap<TNodePath, AlterTablePrepare> {
    let mut prepares = HashMap::<TNodePath, AlterTablePrepare>::new();
    for rm in &self.rms {
      prepares.insert(rm.clone(), AlterTablePrepare { alter_op: self.alter_op.clone() });
    }
    prepares
  }

  fn mk_committed_plm<IO: MasterIOCtx>(
    &mut self,
    _: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: &HashMap<TNodePath, AlterTablePrepared>,
  ) -> plm::AlterTableTMCommitted {
    let mut timestamp_hint = io_ctx.now();
    for (_, prepared) in prepared {
      timestamp_hint = max(timestamp_hint, prepared.timestamp);
    }
    plm::AlterTableTMCommitted { timestamp_hint }
  }

  /// Apply this `alter_op` to the system and construct Commit messages with the
  /// commit timestamp (which is resolved form the resolved from `timestamp_hint`).
  fn committed_plm_inserted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    committed_plm: &TMCommittedPLm<AlterTablePayloadTypes>,
  ) -> HashMap<TNodePath, AlterTableCommit> {
    let gen = ctx.table_generation.get_last_version(&self.table_path).unwrap();
    let table_schema = ctx.db_schema.get_mut(&(self.table_path.clone(), gen.clone())).unwrap();

    // Compute the resolved timestamp
    let mut commit_timestamp = committed_plm.payload.timestamp_hint;
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

    // Potentially respond to the External if we are the leader.
    if ctx.is_leader() {
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
    }

    let mut commits = HashMap::<TNodePath, AlterTableCommit>::new();
    for rm in &self.rms {
      commits.insert(rm.clone(), AlterTableCommit { timestamp: commit_timestamp });
    }
    commits
  }

  fn mk_aborted_plm<IO: MasterIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> plm::AlterTableTMAborted {
    plm::AlterTableTMAborted {}
  }

  fn aborted_plm_inserted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> HashMap<TNodePath, AlterTableAbort> {
    // Potentially respond to the External if we are the leader.
    if ctx.is_leader() {
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
    }

    let mut aborts = HashMap::<TNodePath, AlterTableAbort>::new();
    for rm in &self.rms {
      aborts.insert(rm.clone(), AlterTableAbort {});
    }
    aborts
  }

  fn mk_closed_plm<IO: MasterIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> plm::AlterTableTMClosed {
    plm::AlterTableTMClosed {}
  }

  fn closed_plm_inserted<IO: MasterIOCtx>(&mut self, _: &mut MasterContext, _: &mut IO) {}

  fn node_died<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
  }
}

// -----------------------------------------------------------------------------------------------
//  Experimental
// -----------------------------------------------------------------------------------------------

pub mod plm {
  use crate::model::common::{proc, QueryId, TablePath, Timestamp};
  use serde::{Deserialize, Serialize};

  // TM PLm

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableTMPrepared {
    pub table_path: TablePath,
    pub alter_op: proc::AlterOp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableTMCommitted {
    pub timestamp_hint: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableTMAborted {}

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableTMClosed {}

  // RM PLm

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableRMPrepared {
    pub alter_op: proc::AlterOp,
    pub timestamp: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableRMCommitted {
    pub timestamp: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableRMAborted {}
}

// RM-to-TM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePrepare {
  pub alter_op: proc::AlterOp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePrepared {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAborted {}

// TM-to-RM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAbort {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableCommit {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableClosed {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePayloadTypes {}

impl PayloadTypes for AlterTablePayloadTypes {
  // TM PLm
  type TMPreparedPLm = plm::AlterTableTMPrepared;
  type TMCommittedPLm = plm::AlterTableTMCommitted;
  type TMAbortedPLm = plm::AlterTableTMAborted;
  type TMClosedPLm = plm::AlterTableTMClosed;

  fn master_prepared_plm(prepared_plm: TMPreparedPLm<Self>) -> MasterPLm {
    MasterPLm::AlterTableTMPrepared2(prepared_plm)
  }

  fn master_committed_plm(committed_plm: TMCommittedPLm<Self>) -> MasterPLm {
    MasterPLm::AlterTableTMCommitted2(committed_plm)
  }

  fn master_aborted_plm(aborted_plm: TMAbortedPLm<Self>) -> MasterPLm {
    MasterPLm::AlterTableTMAborted2(aborted_plm)
  }

  fn master_closed_plm(closed_plm: TMClosedPLm<Self>) -> MasterPLm {
    MasterPLm::AlterTableTMClosed2(closed_plm)
  }

  // RM PLm
  type RMPreparedPLm = plm::AlterTableRMPrepared;
  type RMCommittedPLm = plm::AlterTableRMCommitted;
  type RMAbortedPLm = plm::AlterTableRMAborted;

  // TM-to-RM Messages
  type Prepare = AlterTablePrepare;
  type Abort = AlterTableAbort;
  type Commit = AlterTableCommit;

  fn tablet_prepare(prepare: Prepare<Self>) -> msg::TabletMessage {
    TabletMessage::AlterTablePrepare2(prepare)
  }

  fn tablet_commit(commit: Commit<Self>) -> msg::TabletMessage {
    TabletMessage::AlterTableCommit2(commit)
  }

  fn tablet_abort(abort: Abort<Self>) -> msg::TabletMessage {
    TabletMessage::AlterTableAbort2(abort)
  }

  // RM-to-TM Messages
  type Prepared = AlterTablePrepared;
  type Aborted = AlterTableAborted;
  type Closed = AlterTableClosed;
}
