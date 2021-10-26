use crate::common::{BasicIOCtx, RemoteLeaderChangedPLm};
use crate::model::common::{proc, PaxosGroupId, QueryId};
use crate::stmpaxos2pc_rm::{STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::Hash;

// -----------------------------------------------------------------------------------------------
//  RMServerContext
// -----------------------------------------------------------------------------------------------

pub trait RMServerContext<T: PayloadTypes> {
  fn push_plm(&mut self, plm: T::RMPLm);

  fn send_to_tm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    io_ctx: &mut IO,
    tm: &T::TMPath,
    msg: T::TMMessage,
  );

  fn mk_node_path(&self) -> T::RMPath;

  fn is_leader(&self) -> bool;
}

// -----------------------------------------------------------------------------------------------
//  TMServerContext
// -----------------------------------------------------------------------------------------------

pub trait TMServerContext<T: PayloadTypes> {
  fn push_plm(&mut self, plm: T::TMPLm);

  fn send_to_rm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    io_ctx: &mut IO,
    rm: &T::RMPath,
    msg: T::RMMessage,
  );

  fn mk_node_path(&self) -> T::TMPath;

  fn is_leader(&self) -> bool;
}

pub trait RMPathTrait {
  fn to_gid(&self) -> PaxosGroupId;
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PC
// -----------------------------------------------------------------------------------------------

pub trait PayloadTypes: Clone {
  // Meta
  type RMPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type TMPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type RMPath: Serialize
    + DeserializeOwned
    + Debug
    + Clone
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + RMPathTrait;
  type TMPath: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type RMMessage: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type TMMessage: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type NetworkMessageT;
  type RMContext: RMServerContext<Self>;
  type TMContext: TMServerContext<Self>;

  // TM PLm
  type TMPreparedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type TMCommittedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type TMAbortedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type TMClosedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;

  fn tm_plm(plm: TMPLm<Self>) -> Self::TMPLm;

  // RM PLm
  type RMPreparedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type RMCommittedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type RMAbortedPLm: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;

  fn rm_plm(plm: RMPLm<Self>) -> Self::RMPLm;

  // TM-to-RM Messages
  type Prepare: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type Abort: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type Commit: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;

  fn rm_msg(msg: RMMessage<Self>) -> Self::RMMessage;

  // RM-to-TM Messages
  type Prepared: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type Aborted: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;
  type Closed: Serialize + DeserializeOwned + Debug + Clone + PartialEq + Eq;

  fn tm_msg(msg: TMMessage<Self>) -> Self::TMMessage;
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TMPLm<T: PayloadTypes> {
  Prepared(TMPreparedPLm<T>),
  Committed(TMCommittedPLm<T>),
  Aborted(TMAbortedPLm<T>),
  Closed(TMClosedPLm<T>),
}

// RM PLm
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RMPreparedPLm<T: PayloadTypes> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RMPLm<T: PayloadTypes> {
  Prepared(RMPreparedPLm<T>),
  Committed(RMCommittedPLm<T>),
  Aborted(RMAbortedPLm<T>),
}

// TM-to-RM Messages
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Prepare<T: PayloadTypes> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
  pub payload: T::Prepare,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Abort<T: PayloadTypes> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
  pub payload: T::Abort,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Commit<T: PayloadTypes> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
  pub payload: T::Commit,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum RMMessage<T: PayloadTypes> {
  Prepare(Prepare<T>),
  Abort(Abort<T>),
  Commit(Commit<T>),
}

// RM-to-TM Messages
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Prepared<T: PayloadTypes> {
  pub query_id: QueryId,
  pub rm: T::RMPath,
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
  pub rm: T::RMPath,
  pub payload: T::Closed,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TMMessage<T: PayloadTypes> {
  Prepared(Prepared<T>),
  Aborted(Aborted<T>),
  Closed(Closed<T>),
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCTMInner
// -----------------------------------------------------------------------------------------------

pub trait STMPaxos2PCTMInner<T: PayloadTypes> {
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

  /// Called after all RMs have processed the `Commit` or or `Abort` message.
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

  // This is called when the node died.
  fn node_died<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  );
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCTMOuter
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct PreparingSt<T: PayloadTypes> {
  rms_remaining: BTreeSet<T::RMPath>,
  prepared: BTreeMap<T::RMPath, T::Prepared>,
}

#[derive(Debug)]
pub struct CommittedSt<T: PayloadTypes> {
  rms_remaining: BTreeSet<T::RMPath>,
}

#[derive(Debug)]
pub struct AbortedSt<T: PayloadTypes> {
  rms_remaining: BTreeSet<T::RMPath>,
}

#[derive(Debug)]
pub enum FollowerState<T: PayloadTypes> {
  Preparing(BTreeMap<T::RMPath, T::Prepare>),
  Committed(BTreeMap<T::RMPath, T::Commit>),
  Aborted(BTreeMap<T::RMPath, T::Abort>),
}

#[derive(Debug)]
pub enum State<T: PayloadTypes> {
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

#[derive(Debug)]
pub struct STMPaxos2PCTMOuter<T: PayloadTypes, InnerT> {
  pub query_id: QueryId,
  pub follower: Option<FollowerState<T>>,
  pub state: State<T>,
  pub inner: InnerT,
}

impl<T: PayloadTypes, InnerT: STMPaxos2PCTMInner<T>> STMPaxos2PCTMOuter<T, InnerT> {
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
            let committed_plm = T::tm_plm(TMPLm::Committed(TMCommittedPLm {
              query_id: self.query_id.clone(),
              payload: self.inner.mk_committed_plm(ctx, io_ctx, &preparing.prepared),
            }));
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
        let aborted_plm = T::tm_plm(TMPLm::Aborted(TMAbortedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_aborted_plm(ctx, io_ctx),
        }));
        ctx.push_plm(aborted_plm);
        self.state = State::InsertingTMAborted;
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  fn handle_close_confirmed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    closed: Closed<T>,
  ) -> STMPaxos2PCTMAction {
    match &mut self.state {
      State::Committed(committed) => {
        if committed.rms_remaining.remove(&closed.rm) {
          if committed.rms_remaining.is_empty() {
            // All RMs have committed
            let closed_plm = T::tm_plm(TMPLm::Closed(TMClosedPLm {
              query_id: self.query_id.clone(),
              payload: self.inner.mk_closed_plm(ctx, io_ctx),
            }));
            ctx.push_plm(closed_plm);
            self.state = State::InsertingTMClosed;
          }
        }
      }
      State::Aborted(aborted) => {
        if aborted.rms_remaining.remove(&closed.rm) {
          if aborted.rms_remaining.is_empty() {
            // All RMs have aborted
            let closed_plm = T::tm_plm(TMPLm::Closed(TMClosedPLm {
              query_id: self.query_id.clone(),
              payload: self.inner.mk_closed_plm(ctx, io_ctx),
            }));
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
      ctx.send_to_rm(io_ctx, &rm, T::rm_msg(RMMessage::Prepare(prepare)));
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
      ctx.send_to_rm(io_ctx, &rm, T::rm_msg(RMMessage::Commit(commit)));
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
        self.follower = Some(FollowerState::Committed(commit_payloads.clone()));

        // Change state to Committed
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
      ctx.send_to_rm(io_ctx, &rm, T::rm_msg(RMMessage::Abort(abort)));
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
        self.follower = Some(FollowerState::Aborted(abort_payloads.clone()));

        // Change state to Aborted
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

  pub fn start_inserting<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCTMAction {
    match &self.state {
      State::WaitingInsertTMPrepared => {
        let prepared = T::tm_plm(TMPLm::Prepared(TMPreparedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_prepared_plm(ctx, io_ctx),
        }));
        ctx.push_plm(prepared);
        self.state = State::InsertTMPreparing;
      }
      _ => {}
    }
    STMPaxos2PCTMAction::Wait
  }

  pub fn leader_changed<IO: BasicIOCtx<T::NetworkMessageT>>(
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
        self.inner.node_died(ctx, io_ctx);
        STMPaxos2PCTMAction::Exit
      }
      State::Preparing(_)
      | State::InsertingTMCommitted
      | State::Committed(_)
      | State::InsertingTMAborted
      | State::Aborted(_)
      | State::InsertingTMClosed => {
        self.state = State::Following;
        self.inner.node_died(ctx, io_ctx);
        STMPaxos2PCTMAction::Wait
      }
    }
  }

  pub fn remote_leader_changed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::TMContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> STMPaxos2PCTMAction {
    let follower = self.follower.as_ref().unwrap();
    match &self.state {
      State::Preparing(preparing) => {
        let prepare_payloads = cast!(FollowerState::Preparing, follower).unwrap();
        for rm in &preparing.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Prepare.
          if rm.to_gid() == remote_leader_changed.gid {
            let payload = prepare_payloads.get(rm).unwrap().clone();
            let prepare =
              Prepare { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
            ctx.send_to_rm(io_ctx, &rm, T::rm_msg(RMMessage::Prepare(prepare)));
          }
        }
      }
      State::Committed(committed) => {
        let commit_payloads = cast!(FollowerState::Committed, follower).unwrap();
        for rm in &committed.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Commit.
          if rm.to_gid() == remote_leader_changed.gid {
            let payload = commit_payloads.get(rm).unwrap().clone();
            let commit =
              Commit { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
            ctx.send_to_rm(io_ctx, &rm, T::rm_msg(RMMessage::Commit(commit)));
          }
        }
      }
      State::Aborted(aborted) => {
        let abort_payloads = cast!(FollowerState::Aborted, follower).unwrap();
        for rm in &aborted.rms_remaining {
          // If the RM has not responded and its Leadership changed, we resend Abort.
          if rm.to_gid() == remote_leader_changed.gid {
            let payload = abort_payloads.get(rm).unwrap().clone();
            let abort = Abort { query_id: self.query_id.clone(), tm: ctx.mk_node_path(), payload };
            ctx.send_to_rm(io_ctx, &rm, T::rm_msg(RMMessage::Abort(abort)));
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
pub trait AggregateContainer<T: PayloadTypes, InnerT: STMPaxos2PCTMInner<T>> {
  fn get_mut(&mut self, query_id: &QueryId) -> Option<&mut STMPaxos2PCTMOuter<T, InnerT>>;

  fn insert(&mut self, query_id: QueryId, es: STMPaxos2PCTMOuter<T, InnerT>);
}

/// Implementation for HashMap, which is the common case.
impl<T: PayloadTypes, InnerT: STMPaxos2PCTMInner<T>> AggregateContainer<T, InnerT>
  for BTreeMap<QueryId, STMPaxos2PCTMOuter<T, InnerT>>
{
  fn get_mut(&mut self, query_id: &QueryId) -> Option<&mut STMPaxos2PCTMOuter<T, InnerT>> {
    self.get_mut(query_id)
  }

  fn insert(&mut self, query_id: QueryId, es: STMPaxos2PCTMOuter<T, InnerT>) {
    self.insert(query_id, es);
  }
}

/// Function to handle the insertion of an `TMPLm` for a given `AggregateContainer`.
pub fn handle_tm_plm<
  T: PayloadTypes,
  InnerT: STMPaxos2PCTMInner<T>,
  ConT: AggregateContainer<T, InnerT>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::TMContext,
  io_ctx: &mut IO,
  con: &mut ConT,
  plm: TMPLm<T>,
) -> (QueryId, STMPaxos2PCTMAction) {
  match plm {
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
  }
}

/// Function to handle the arrive of an `TMMessage` for a given `AggregateContainer`.
pub fn handle_tm_msg<
  T: PayloadTypes,
  InnerT: STMPaxos2PCTMInner<T>,
  ConT: AggregateContainer<T, InnerT>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::TMContext,
  io_ctx: &mut IO,
  con: &mut ConT,
  msg: TMMessage<T>,
) -> (QueryId, STMPaxos2PCTMAction) {
  match msg {
    TMMessage::Prepared(prepared) => {
      let es = con.get_mut(&prepared.query_id).unwrap();
      (prepared.query_id.clone(), es.handle_prepared(ctx, io_ctx, prepared))
    }
    TMMessage::Aborted(aborted) => {
      let es = con.get_mut(&aborted.query_id).unwrap();
      (aborted.query_id, es.handle_aborted(ctx, io_ctx))
    }
    TMMessage::Closed(closed) => {
      let es = con.get_mut(&closed.query_id).unwrap();
      (closed.query_id.clone(), es.handle_close_confirmed(ctx, io_ctx, closed))
    }
  }
}
