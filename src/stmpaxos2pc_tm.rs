use crate::common::{BasicIOCtx, RemoteLeaderChangedPLm};
use crate::common::{PaxosGroupId, PaxosGroupIdTrait, QueryId};
use crate::paxos2pc_tm::Paxos2PCContainer;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

// -----------------------------------------------------------------------------------------------
//  TMServerContext
// -----------------------------------------------------------------------------------------------

pub trait TMServerContext<T: TMPayloadTypes> {
  fn push_plm(&mut self, plm: TMPLm<T>);

  fn send_to_rm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    io_ctx: &mut IO,
    rm: &T::RMPath,
    msg: RMMessage<T>,
  );

  fn mk_node_path(&self) -> T::TMPath;

  fn is_leader(&self) -> bool;
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCTM
// -----------------------------------------------------------------------------------------------

pub trait TMPayloadTypes: Clone {
  // Meta
  type RMPath: Serialize
    + DeserializeOwned
    + Debug
    + Clone
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + PaxosGroupIdTrait;
  type TMPath: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type NetworkMessageT;
  type TMContext: TMServerContext<Self>;

  // TM PLm
  type TMPreparedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type TMCommittedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type TMAbortedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type TMClosedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;

  // TM-to-RM Messages
  type Prepare: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type Abort: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type Commit: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;

  // RM-to-TM Messages
  type Prepared: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type Aborted: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type Closed: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
}

// TM PLm
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TMPreparedPLm<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::TMPreparedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TMCommittedPLm<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::TMCommittedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TMAbortedPLm<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::TMAbortedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TMClosedPLm<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::TMClosedPLm,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TMPLm<T: TMPayloadTypes> {
  Prepared(TMPreparedPLm<T>),
  Committed(TMCommittedPLm<T>),
  Aborted(TMAbortedPLm<T>),
  Closed(TMClosedPLm<T>),
}

// TM-to-RM Messages
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Prepare<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
  pub payload: T::Prepare,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Abort<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
  pub payload: T::Abort,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Commit<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
  pub payload: T::Commit,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RMMessage<T: TMPayloadTypes> {
  Prepare(Prepare<T>),
  Abort(Abort<T>),
  Commit(Commit<T>),
}

// RM-to-TM Messages
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Prepared<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub rm: T::RMPath,
  pub payload: T::Prepared,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Aborted<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub payload: T::Aborted,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Closed<T: TMPayloadTypes> {
  pub query_id: QueryId,
  pub rm: T::RMPath,
  pub payload: T::Closed,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TMMessage<T: TMPayloadTypes> {
  Prepared(Prepared<T>),
  Aborted(Aborted<T>),
  Closed(Closed<T>),
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCTMInner
// -----------------------------------------------------------------------------------------------

/// Properties that must be satisfied by an implementation.
/// 1. All calls to `*_plm_inserted` must return the same set of keys, i.e. RMs.
/// 2. The set of RMs must be non-empty (otherwise we deadlock).
pub trait STMPaxos2PCTMInner<T: TMPayloadTypes> {
  /// Constructs an instance of `STMPaxos2PCTMInner` from a Prepared PLm. This is used primarily
  /// by the Follower.
  fn new_follower<IO: BasicIOCtx<T::NetworkMessageT>>(
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    payload: T::TMPreparedPLm,
  ) -> Self;

  /// Called in order to get the `TMPreparedPLm` to insert.
  fn mk_prepared_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> T::TMPreparedPLm;

  /// Called after PreparedPLm is inserted.
  fn prepared_plm_inserted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> BTreeMap<T::RMPath, T::Prepare>;

  /// Called after all RMs have Prepared.
  fn mk_committed_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    prepared: &BTreeMap<T::RMPath, T::Prepared>,
  ) -> T::TMCommittedPLm;

  /// Called after CommittedPLm is inserted.
  fn committed_plm_inserted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    committed_plm: &TMCommittedPLm<T>,
  ) -> BTreeMap<T::RMPath, T::Commit>;

  /// Called if one of the RMs returned Aborted.
  fn mk_aborted_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> T::TMAbortedPLm;

  /// Called after AbortedPLm is inserted.
  fn aborted_plm_inserted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> BTreeMap<T::RMPath, T::Abort>;

  /// Called after all RMs have processed the `Commit` or `Abort` message.
  fn mk_closed_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> T::TMClosedPLm;

  /// Called after ClosedPLm is inserted.
  fn closed_plm_inserted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    closed_plm: &TMClosedPLm<T>,
  );

  /// This is called when the leader of the node changes.
  fn leader_changed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  );

  /// If this node is a Follower, a copy of this `Inner` is returned. If this node is
  /// a Leader, then the value of this `STMPaxos2PCTMInner` that would result from losing
  /// Leadership is returned (i.e. after the `Outer` calls `leader_changed`).
  fn reconfig_snapshot(&self) -> Self;
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCTMOuter
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PreparingSt<T: TMPayloadTypes> {
  rms_remaining: BTreeSet<T::RMPath>,
  prepared: BTreeMap<T::RMPath, T::Prepared>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CommittedSt<T: TMPayloadTypes> {
  rms_remaining: BTreeSet<T::RMPath>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AbortedSt<T: TMPayloadTypes> {
  rms_remaining: BTreeSet<T::RMPath>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FollowerState<T: TMPayloadTypes> {
  Preparing(BTreeMap<T::RMPath, T::Prepare>),
  Committed(BTreeMap<T::RMPath, T::Commit>),
  Aborted(BTreeMap<T::RMPath, T::Abort>),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum State<T: TMPayloadTypes> {
  Following,
  Start,
  WaitingInsertTMPrepared,
  InsertTMPreparing,
  Preparing(PreparingSt<T>),
  InsertingTMCommitted,
  Committed(CommittedSt<T>),
  InsertingTMAborted,
  Aborted(AbortedSt<T>),
  InsertingTMClosed,
}

pub enum STMPaxos2PCTMAction {
  Wait,
  Exit,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMPaxos2PCTMOuter<T: TMPayloadTypes, InnerT> {
  pub query_id: QueryId,
  /// This is only `None` when no related paxos messages have been inserted.
  pub follower: Option<FollowerState<T>>,
  pub state: State<T>,
  pub inner: InnerT,
}

impl<T: TMPayloadTypes, InnerT: STMPaxos2PCTMInner<T>> STMPaxos2PCTMOuter<T, InnerT> {
  pub fn new(query_id: QueryId, inner: InnerT) -> STMPaxos2PCTMOuter<T, InnerT> {
    STMPaxos2PCTMOuter { query_id, follower: None, state: State::Start, inner }
  }

  /// This is only called when the `PreparedPLm` is insert at a Follower node.
  fn init_follower<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) {
    let prepare_payloads = self.inner.prepared_plm_inserted(ctx, io_ctx);
    self.follower = Some(FollowerState::Preparing(prepare_payloads));
    self.state = State::Following;
  }

  // STMPaxos2PC messages

  fn handle_prepared<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    prepared: Prepared<T>,
  ) -> STMPaxos2PCTMAction {
    match &mut self.state {
      State::Preparing(preparing) => {
        if preparing.rms_remaining.remove(&prepared.rm) {
          preparing.prepared.insert(prepared.rm.clone(), prepared.payload);
          if preparing.rms_remaining.is_empty() {
            let committed_plm = TMPLm::Committed(TMCommittedPLm {
              query_id: self.query_id.clone(),
              payload: self.inner.mk_committed_plm(ctx, io_ctx, &preparing.prepared),
            });
            ctx.push_plm(committed_plm);
            self.state = State::InsertingTMCommitted;
          }
        }
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  fn handle_aborted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCTMAction {
    match &mut self.state {
      State::Preparing(_) => {
        let aborted_plm = TMPLm::Aborted(TMAbortedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_aborted_plm(ctx, io_ctx),
        });
        ctx.push_plm(aborted_plm);
        self.state = State::InsertingTMAborted;
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  fn handle_closed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    closed: Closed<T>,
  ) -> STMPaxos2PCTMAction {
    match &mut self.state {
      State::Committed(CommittedSt { rms_remaining })
      | State::Aborted(AbortedSt { rms_remaining }) => {
        if rms_remaining.remove(&closed.rm) {
          if rms_remaining.is_empty() {
            // All RMs have aborted
            let closed_plm = TMPLm::Closed(TMClosedPLm {
              query_id: self.query_id.clone(),
              payload: self.inner.mk_closed_plm(ctx, io_ctx),
            });
            ctx.push_plm(closed_plm);
            self.state = State::InsertingTMClosed;
          }
        }
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  // STMPaxos2PC PLm Insertions

  /// Change state to `Preparing` and broadcast `Prepare` to the RMs.
  fn advance_to_prepared<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    prepare_payloads: BTreeMap<T::RMPath, T::Prepare>,
  ) {
    let mut rms_remaining = BTreeSet::<T::RMPath>::new();
    for (rm, payload) in prepare_payloads.clone() {
      let prepare = Prepare { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
      ctx.send_to_rm(io_ctx, &rm, RMMessage::Prepare(prepare));
      rms_remaining.insert(rm);
    }

    self.follower = Some(FollowerState::Preparing(prepare_payloads));
    self.state = State::Preparing(PreparingSt { rms_remaining, prepared: Default::default() });
  }

  fn handle_prepared_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCTMAction {
    match &self.state {
      State::InsertTMPreparing => {
        let prepare_payloads = self.inner.prepared_plm_inserted(ctx, io_ctx);
        self.advance_to_prepared(ctx, io_ctx, prepare_payloads);
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  /// Change state to `Committed` and broadcast `AlterTableCommit` to the RMs.
  fn advance_to_committed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    commit_payloads: BTreeMap<T::RMPath, T::Commit>,
  ) {
    let mut rms_remaining = BTreeSet::<T::RMPath>::new();
    for (rm, payload) in commit_payloads.clone() {
      let commit = Commit { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
      ctx.send_to_rm(io_ctx, &rm, RMMessage::Commit(commit));
      rms_remaining.insert(rm);
    }

    self.follower = Some(FollowerState::Committed(commit_payloads));
    self.state = State::Committed(CommittedSt { rms_remaining });
  }

  fn handle_committed_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    committed_plm: TMCommittedPLm<T>,
  ) -> STMPaxos2PCTMAction {
    match &self.state {
      State::Following => {
        let commit_payloads = self.inner.committed_plm_inserted(ctx, io_ctx, &committed_plm);
        self.follower = Some(FollowerState::Committed(commit_payloads));
      }
      State::InsertingTMCommitted => {
        let commit_payloads = self.inner.committed_plm_inserted(ctx, io_ctx, &committed_plm);
        self.advance_to_committed(ctx, io_ctx, commit_payloads);
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  /// Change state to `Aborted` and broadcast `AlterTableAbort` to the RMs.
  fn advance_to_aborted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    abort_payloads: BTreeMap<T::RMPath, T::Abort>,
  ) {
    let mut rms_remaining = BTreeSet::<T::RMPath>::new();
    for (rm, payload) in abort_payloads.clone() {
      let abort = Abort { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
      ctx.send_to_rm(io_ctx, &rm, RMMessage::Abort(abort));
      rms_remaining.insert(rm);
    }

    self.follower = Some(FollowerState::Aborted(abort_payloads));
    self.state = State::Aborted(AbortedSt { rms_remaining });
  }

  fn handle_aborted_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCTMAction {
    match &self.state {
      State::Following => {
        let abort_payloads = self.inner.aborted_plm_inserted(ctx, io_ctx);
        self.follower = Some(FollowerState::Aborted(abort_payloads));
      }
      State::InsertingTMAborted => {
        let abort_payloads = self.inner.aborted_plm_inserted(ctx, io_ctx);
        self.advance_to_aborted(ctx, io_ctx, abort_payloads);
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  /// Simply return Exit in the appropriate states.
  fn handle_closed_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    closed_plm: TMClosedPLm<T>,
  ) -> STMPaxos2PCTMAction {
    match &self.state {
      State::Following => {
        self.inner.closed_plm_inserted(ctx, io_ctx, &closed_plm);
        STMPaxos2PCTMAction::Exit
      }
      State::InsertingTMClosed => {
        self.inner.closed_plm_inserted(ctx, io_ctx, &closed_plm);
        STMPaxos2PCTMAction::Exit
      }
      _ => STMPaxos2PCTMAction::Wait,
    }
  }

  // Other

  fn start_inserting<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCTMAction {
    match &self.state {
      State::WaitingInsertTMPrepared => {
        let prepared = TMPLm::Prepared(TMPreparedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_prepared_plm(ctx, io_ctx),
        });
        ctx.push_plm(prepared);
        self.state = State::InsertTMPreparing;
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  fn leader_changed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCTMAction {
    match &self.state {
      State::Following => {
        if ctx.is_leader() {
          // Recall that if State was ever set to `Following`, then `follower` must have been set.
          match self.follower.as_ref().unwrap() {
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
        STMPaxos2PCTMAction::Wait
      }
      State::Start | State::WaitingInsertTMPrepared | State::InsertTMPreparing => {
        self.inner.leader_changed(ctx, io_ctx);
        STMPaxos2PCTMAction::Exit
      }
      State::Preparing(_)
      | State::InsertingTMCommitted
      | State::Committed(_)
      | State::InsertingTMAborted
      | State::Aborted(_)
      | State::InsertingTMClosed => {
        self.state = State::Following;
        self.inner.leader_changed(ctx, io_ctx);
        STMPaxos2PCTMAction::Wait
      }
    }
  }

  /// If this node is a Follower, a copy of this `Outer` is returned. If this node is
  /// a Leader, then the value of this `STMPaxos2PCTMOuter` that would result from losing
  /// Leadership is returned (i.e. after calling `leader_changed`).
  fn reconfig_snapshot(&self) -> Option<STMPaxos2PCTMOuter<T, InnerT>> {
    match &self.state {
      State::Start | State::WaitingInsertTMPrepared | State::InsertTMPreparing => None,
      State::Following
      | State::Preparing(_)
      | State::InsertingTMCommitted
      | State::Committed(_)
      | State::InsertingTMAborted
      | State::Aborted(_)
      | State::InsertingTMClosed => Some(STMPaxos2PCTMOuter {
        query_id: self.query_id.clone(),
        follower: self.follower.clone(),
        state: State::Following,
        inner: self.inner.reconfig_snapshot(),
      }),
    }
  }

  /// Called when a `RemoteLeaderChangedPLm` is inserted in the.
  fn remote_leader_changed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> STMPaxos2PCTMAction {
    match &self.state {
      State::Preparing(preparing) => {
        let follower = self.follower.as_ref().unwrap();
        let prepare_payloads = cast!(FollowerState::Preparing, follower).unwrap();
        for rm in &preparing.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Prepare.
          if rm.to_gid() == remote_leader_changed.gid {
            let payload = prepare_payloads.get(rm).unwrap().clone();
            let prepare =
              Prepare { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
            ctx.send_to_rm(io_ctx, &rm, RMMessage::Prepare(prepare));
          }
        }
      }
      State::Committed(committed) => {
        let follower = self.follower.as_ref().unwrap();
        let commit_payloads = cast!(FollowerState::Committed, follower).unwrap();
        for rm in &committed.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Commit.
          if rm.to_gid() == remote_leader_changed.gid {
            let payload = commit_payloads.get(rm).unwrap().clone();
            let commit =
              Commit { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
            ctx.send_to_rm(io_ctx, &rm, RMMessage::Commit(commit));
          }
        }
      }
      State::Aborted(aborted) => {
        let follower = self.follower.as_ref().unwrap();
        let abort_payloads = cast!(FollowerState::Aborted, follower).unwrap();
        for rm in &aborted.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Abort.
          if rm.to_gid() == remote_leader_changed.gid {
            let payload = abort_payloads.get(rm).unwrap().clone();
            let abort = Abort { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
            ctx.send_to_rm(io_ctx, &rm, RMMessage::Abort(abort));
          }
        }
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }
}

// -----------------------------------------------------------------------------------------------
//  Aggregate STM ES Management
// -----------------------------------------------------------------------------------------------

// Utilities

/// Handles the actions specified by a AlterTableES.
fn handle_action<T: TMPayloadTypes, InnerT: STMPaxos2PCTMInner<T>>(
  query_id: &QueryId,
  con: &mut BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>>,
  action: STMPaxos2PCTMAction,
) {
  match action {
    STMPaxos2PCTMAction::Wait => {}
    STMPaxos2PCTMAction::Exit => {
      con.remove(&query_id);
    }
  }
}

// Leader-only

/// Function to handle the arrive of an `TMMessage` for a given `AggregateContainer`.
pub fn handle_msg<
  T: TMPayloadTypes,
  InnerT: STMPaxos2PCTMInner<T>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::TMContext,
  io_ctx: &mut IO,
  con: &mut BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>>,
  msg: TMMessage<T>,
) {
  let (query_id, action) = match msg {
    TMMessage::Prepared(prepared) => {
      // We can `unwrap` here because in order for the ES to disappear, all `Closed` messages
      // must arrive, meaning all Prepared messages sent back to the TM must also arrive before.
      let es = con.get_mut(&prepared.query_id).unwrap();
      (prepared.query_id.clone(), es.handle_prepared(ctx, io_ctx, prepared))
    }
    TMMessage::Aborted(aborted) => {
      // We can `unwrap` here because in order for the ES to disappear, all `Closed` messages
      // must arrive, meaning all Aborted messages sent back to the TM must also arrive before.
      let es = con.get_mut(&aborted.query_id).unwrap();
      (aborted.query_id, es.handle_aborted(ctx, io_ctx))
    }
    TMMessage::Closed(closed) => {
      let es = con.get_mut(&closed.query_id).unwrap();
      (closed.query_id.clone(), es.handle_closed(ctx, io_ctx, closed))
    }
  };
  handle_action(&query_id, con, action);
}

pub fn handle_bundle_processed<
  T: TMPayloadTypes,
  InnerT: STMPaxos2PCTMInner<T>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::TMContext,
  io_ctx: &mut IO,
  con: &mut BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>>,
) {
  for (_, es) in con {
    es.start_inserting(ctx, io_ctx);
  }
}

// Leader and Follower

/// Function to handle the insertion of an `TMPLm` for a given `AggregateContainer`.
pub fn handle_plm<
  T: TMPayloadTypes,
  InnerT: STMPaxos2PCTMInner<T>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::TMContext,
  io_ctx: &mut IO,
  con: &mut BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>>,
  plm: TMPLm<T>,
) {
  let (query_id, action) = match plm {
    TMPLm::Prepared(prepared) => {
      if ctx.is_leader() {
        let es = con.get_mut(&prepared.query_id).unwrap();
        (prepared.query_id, es.handle_prepared_plm(ctx, io_ctx))
      } else {
        // Recall that for a Follower, for a Prepared, we must contruct
        // the ES for the first time.
        let mut outer = STMPaxos2PCTMOuter::new(
          prepared.query_id.clone(),
          InnerT::new_follower(ctx, io_ctx, prepared.payload),
        );
        outer.init_follower(ctx, io_ctx);
        con.insert(prepared.query_id.clone(), outer);
        (prepared.query_id, STMPaxos2PCTMAction::Wait)
      }
    }
    TMPLm::Committed(committed) => {
      let query_id = committed.query_id.clone();
      let es = con.get_mut(&query_id).unwrap();
      (query_id, es.handle_committed_plm(ctx, io_ctx, committed))
    }
    TMPLm::Aborted(aborted) => {
      let es = con.get_mut(&aborted.query_id).unwrap();
      (aborted.query_id, es.handle_aborted_plm(ctx, io_ctx))
    }
    TMPLm::Closed(closed) => {
      let es = con.get_mut(&closed.query_id).unwrap();
      (closed.query_id.clone(), es.handle_closed_plm(ctx, io_ctx, closed))
    }
  };
  handle_action(&query_id, con, action);
}

pub fn handle_rlc<
  T: TMPayloadTypes,
  InnerT: STMPaxos2PCTMInner<T>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::TMContext,
  io_ctx: &mut IO,
  con: &mut BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>>,
  remote_leader_changed: RemoteLeaderChangedPLm,
) {
  let query_ids: Vec<_> = con.keys().cloned().collect();
  for query_id in query_ids {
    let es = con.get_mut(&query_id).unwrap();
    let action = es.remote_leader_changed(ctx, io_ctx, remote_leader_changed.clone());
    handle_action(&query_id, con, action);
  }
}

pub fn handle_lc<
  T: TMPayloadTypes,
  InnerT: STMPaxos2PCTMInner<T>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::TMContext,
  io_ctx: &mut IO,
  con: &mut BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>>,
) {
  let query_ids: Vec<_> = con.keys().cloned().collect();
  for query_id in query_ids {
    let es = con.get_mut(&query_id).unwrap();
    let action = es.leader_changed(ctx, io_ctx);
    handle_action(&query_id, con, action);
  }
}

/// Add in the `STMPaxos2PCTMOuter`s that have at least been Prepared.
pub fn handle_reconfig_snapshot<T: TMPayloadTypes, InnerT: STMPaxos2PCTMInner<T>>(
  con: &BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>>,
) -> BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>> {
  let mut ess = BTreeMap::<QueryId, STMPaxos2PCTMOuter<T, InnerT>>::new();
  for (qid, es) in con {
    if let Some(es) = es.reconfig_snapshot() {
      ess.insert(qid.clone(), es);
    }
  }
  ess
}
