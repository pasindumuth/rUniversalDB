use crate::common::BasicIOCtx;
use crate::model::common::QueryId;
use crate::paxos2pc_tm::Paxos2PCContainer;
use crate::stmpaxos2pc_tm::{
  Closed, Commit, PayloadTypes, Prepared, RMAbortedPLm, RMCommittedPLm, RMMessage, RMPLm,
  RMPreparedPLm, RMServerContext, TMMessage,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCRMInner
// -----------------------------------------------------------------------------------------------

pub trait STMPaxos2PCRMInner<T: PayloadTypes> {
  /// Constructs an instance of `STMPaxos2PCRMInner` from a Prepared PLm. This is used primarily
  /// by the Follower.
  fn new<IO: BasicIOCtx<T::NetworkMessageT>>(
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    payload: T::Prepare,
  ) -> Self;

  /// Constructs an instance of `STMPaxos2PCRMInner` from a Prepared PLm. This is used primarily
  /// by the Follower.
  fn new_follower<IO: BasicIOCtx<T::NetworkMessageT>>(
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    payload: T::RMPreparedPLm,
  ) -> Self;

  /// This is called at various times, like after `CommittedPLm` and `AbortedPLM` are inserted,
  /// and while in `WaitingInsertingPrepared`, when an `Abort` arrives.
  /// NOTE: this has to be a static method because it has to be sendable when the RM has cleaned
  /// up the ES, but then a late Commit/Abort message arrives.
  fn mk_closed() -> T::Closed;

  /// Called in order to get the `RMPreparedPLm` to insert.
  fn mk_prepared_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> T::RMPreparedPLm;

  /// Called after PreparedPLm is inserted.
  fn prepared_plm_inserted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> T::Prepared;

  /// Called after all RMs have Prepared.
  fn mk_committed_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    commit: &T::Commit,
  ) -> T::RMCommittedPLm;

  /// Called after CommittedPLm is inserted.
  fn committed_plm_inserted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    committed_plm: &RMCommittedPLm<T>,
  ) -> T::RMCommitActionData;

  /// Called if one of the RMs returned Aborted.
  fn mk_aborted_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> T::RMAbortedPLm;

  /// Called after AbortedPLm is inserted.
  fn aborted_plm_inserted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  );

  /// If this node is a Follower, a copy of this `Inner` is returned. If this node is
  /// a Leader, then the value of this `STMPaxos2PCRMInner` that would result from losing
  /// Leadership is returned (i.e. after the `Outer` calls `leader_changed`).
  fn reconfig_snapshot(&self) -> Self;
}

// -----------------------------------------------------------------------------------------------
//  STMPaxos2PCRMOuter
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum State<T: PayloadTypes> {
  Follower,
  WaitingInsertingPrepared,
  InsertingPrepared,
  Prepared(Prepared<T>),
  InsertingCommitted,
  InsertingPreparedAborted,
  InsertingAborted,
}

pub enum STMPaxos2PCRMAction<T: PayloadTypes> {
  Wait,
  Exit(Option<T::RMCommitActionData>),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct STMPaxos2PCRMOuter<T: PayloadTypes, InnerT> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
  pub follower: Option<Prepared<T>>,
  pub state: State<T>,
  pub inner: InnerT,
}

impl<T: PayloadTypes, InnerT: STMPaxos2PCRMInner<T>> STMPaxos2PCRMOuter<T, InnerT> {
  pub fn new(query_id: QueryId, tm: T::TMPath, inner: InnerT) -> STMPaxos2PCRMOuter<T, InnerT> {
    STMPaxos2PCRMOuter {
      query_id,
      tm,
      follower: None,
      state: State::WaitingInsertingPrepared,
      inner,
    }
  }

  /// This is only called when the `PreparedPLm` is insert at a Follower node.
  fn init_follower<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) {
    self._handle_prepared_plm(ctx, io_ctx);
    self.state = State::Follower;
  }

  // STMPaxos2PC messages

  fn handle_prepare<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCRMAction<T> {
    match &self.state {
      State::Prepared(prepared) => {
        // Populate with TM. Hold it here in the RM.
        ctx.send_to_tm(io_ctx, &self.tm, T::tm_msg(TMMessage::Prepared(prepared.clone())));
      }
      _ => {}
    }
    STMPaxos2PCRMAction::Wait
  }

  fn handle_commit<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    commit: Commit<T>,
  ) -> STMPaxos2PCRMAction<T> {
    match &self.state {
      State::Prepared(_) => {
        let committed_plm = T::rm_plm(RMPLm::Committed(RMCommittedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_committed_plm(ctx, io_ctx, &commit.payload),
        }));
        ctx.push_plm(committed_plm);
        self.state = State::InsertingCommitted;
      }
      _ => {}
    }
    STMPaxos2PCRMAction::Wait
  }

  fn handle_abort<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCRMAction<T> {
    match &self.state {
      State::WaitingInsertingPrepared => {
        self.send_closed(ctx, io_ctx);
        STMPaxos2PCRMAction::Exit(None)
      }
      State::InsertingPrepared => {
        self.state = State::InsertingPreparedAborted;
        STMPaxos2PCRMAction::Wait
      }
      State::Prepared(_) => {
        let aborted_plm = T::rm_plm(RMPLm::Aborted(RMAbortedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_aborted_plm(ctx, io_ctx),
        }));
        ctx.push_plm(aborted_plm);
        self.state = State::InsertingAborted;
        STMPaxos2PCRMAction::Wait
      }
      _ => STMPaxos2PCRMAction::Wait,
    }
  }

  // STMPaxos2PC PLm Insertions

  /// Construct the `Prepared` RM-to-TM message to send back, and hold it
  /// in the Follower state.
  fn _handle_prepared_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Prepared<T> {
    let this_node_path = ctx.mk_node_path();
    let prepared = Prepared {
      query_id: self.query_id.clone(),
      rm: this_node_path,
      payload: self.inner.prepared_plm_inserted(ctx, io_ctx),
    };
    self.follower = Some(prepared.clone());
    prepared
  }

  fn handle_prepared_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCRMAction<T> {
    match &self.state {
      State::InsertingPrepared => {
        let prepared = self._handle_prepared_plm(ctx, io_ctx);
        ctx.send_to_tm(io_ctx, &self.tm, T::tm_msg(TMMessage::Prepared(prepared.clone())));
        self.state = State::Prepared(prepared);
      }
      State::InsertingPreparedAborted => {
        self._handle_prepared_plm(ctx, io_ctx);
        let aborted_plm = T::rm_plm(RMPLm::Aborted(RMAbortedPLm {
          query_id: self.query_id.clone(),
          payload: self.inner.mk_aborted_plm(ctx, io_ctx),
        }));
        ctx.push_plm(aborted_plm);
        self.state = State::InsertingAborted;
      }
      _ => {}
    }
    STMPaxos2PCRMAction::Wait
  }

  fn handle_committed_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    committed_plm: RMCommittedPLm<T>,
  ) -> STMPaxos2PCRMAction<T> {
    match &self.state {
      State::Follower => {
        let action = self.inner.committed_plm_inserted(ctx, io_ctx, &committed_plm);
        STMPaxos2PCRMAction::Exit(Some(action))
      }
      State::InsertingCommitted => {
        let action = self.inner.committed_plm_inserted(ctx, io_ctx, &committed_plm);
        self.send_closed(ctx, io_ctx);
        STMPaxos2PCRMAction::Exit(Some(action))
      }
      _ => STMPaxos2PCRMAction::Wait,
    }
  }

  fn handle_aborted_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCRMAction<T> {
    match &self.state {
      State::Follower => {
        self.inner.aborted_plm_inserted(ctx, io_ctx);
        STMPaxos2PCRMAction::Exit(None)
      }
      State::InsertingAborted => {
        self.inner.aborted_plm_inserted(ctx, io_ctx);
        self.send_closed(ctx, io_ctx);
        STMPaxos2PCRMAction::Exit(None)
      }
      _ => STMPaxos2PCRMAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> STMPaxos2PCRMAction<T> {
    match &self.state {
      State::WaitingInsertingPrepared => {
        let prepared_plm = RMPreparedPLm {
          query_id: self.query_id.clone(),
          tm: self.tm.clone(),
          payload: self.inner.mk_prepared_plm(ctx, io_ctx),
        };
        ctx.push_plm(T::rm_plm(RMPLm::Prepared(prepared_plm)));
        self.state = State::InsertingPrepared;
      }
      _ => {}
    }
    STMPaxos2PCRMAction::Wait
  }

  pub fn leader_changed(&mut self, ctx: &mut T::RMContext) -> STMPaxos2PCRMAction<T> {
    match &self.state {
      State::Follower => {
        if ctx.is_leader() {
          let prepared = self.follower.as_ref().unwrap();
          self.state = State::Prepared(prepared.clone());
        }
        STMPaxos2PCRMAction::Wait
      }
      State::WaitingInsertingPrepared
      | State::InsertingPrepared
      | State::InsertingPreparedAborted => STMPaxos2PCRMAction::Exit(None),
      State::Prepared(_) | State::InsertingCommitted | State::InsertingAborted => {
        self.state = State::Follower;
        STMPaxos2PCRMAction::Wait
      }
    }
  }

  /// If this node is a Follower, a copy of this node is returned. If this node is
  /// a Leader, then the value of this `STMPaxos2PCRMOuter` that would result from losing
  /// Leadership is returned (i.e. after calling `leader_changed`).
  pub fn reconfig_snapshot(&self) -> Option<STMPaxos2PCRMOuter<T, InnerT>> {
    match &self.state {
      State::WaitingInsertingPrepared
      | State::InsertingPreparedAborted
      | State::InsertingPrepared => None,
      State::Follower
      | State::Prepared(_)
      | State::InsertingCommitted
      | State::InsertingAborted => Some(STMPaxos2PCRMOuter {
        query_id: self.query_id.clone(),
        tm: self.tm.clone(),
        follower: self.follower.clone(),
        state: State::Follower,
        inner: self.inner.reconfig_snapshot(),
      }),
    }
  }

  // Helpers
  fn send_closed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) {
    let this_node_path = ctx.mk_node_path();
    ctx.send_to_tm(
      io_ctx,
      &self.tm,
      T::tm_msg(TMMessage::Closed(Closed {
        query_id: self.query_id.clone(),
        rm: this_node_path,
        payload: InnerT::mk_closed(),
      })),
    );
  }
}

// -----------------------------------------------------------------------------------------------
//  Aggregate STM ES Management
// -----------------------------------------------------------------------------------------------
/// Function to handle the insertion of an `RMPLm` for a given `AggregateContainer`.
pub fn handle_rm_plm<
  T: PayloadTypes,
  InnerT: STMPaxos2PCRMInner<T>,
  ConT: Paxos2PCContainer<STMPaxos2PCRMOuter<T, InnerT>>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::RMContext,
  io_ctx: &mut IO,
  con: &mut ConT,
  plm: RMPLm<T>,
) -> (QueryId, STMPaxos2PCRMAction<T>) {
  match plm {
    RMPLm::Prepared(prepared) => {
      if ctx.is_leader() {
        let es = con.get_mut(&prepared.query_id).unwrap();
        (prepared.query_id, es.handle_prepared_plm(ctx, io_ctx))
      } else {
        // Recall that for a Follower, for a Prepared, we must contruct
        // the ES for the first time.
        let mut outer = STMPaxos2PCRMOuter::new(
          prepared.query_id.clone(),
          prepared.tm,
          InnerT::new_follower(ctx, io_ctx, prepared.payload),
        );
        outer.init_follower(ctx, io_ctx);
        con.insert(prepared.query_id.clone(), outer);
        (prepared.query_id, STMPaxos2PCRMAction::Wait)
      }
    }
    RMPLm::Committed(committed) => {
      let query_id = committed.query_id.clone();
      let es = con.get_mut(&query_id).unwrap();
      (query_id, es.handle_committed_plm(ctx, io_ctx, committed))
    }
    RMPLm::Aborted(aborted) => {
      let es = con.get_mut(&aborted.query_id).unwrap();
      (aborted.query_id, es.handle_aborted_plm(ctx, io_ctx))
    }
  }
}

/// Function to handle the arrive of an `RMMessage` for a given `AggregateContainer`.
pub fn handle_rm_msg<
  T: PayloadTypes,
  InnerT: STMPaxos2PCRMInner<T>,
  ConT: Paxos2PCContainer<STMPaxos2PCRMOuter<T, InnerT>>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::RMContext,
  io_ctx: &mut IO,
  con: &mut ConT,
  msg: RMMessage<T>,
) -> (QueryId, STMPaxos2PCRMAction<T>) {
  match msg {
    RMMessage::Prepare(prepare) => {
      if let Some(es) = con.get_mut(&prepare.query_id) {
        (prepare.query_id, es.handle_prepare(ctx, io_ctx))
      } else {
        let outer = STMPaxos2PCRMOuter::new(
          prepare.query_id.clone(),
          prepare.tm,
          InnerT::new(ctx, io_ctx, prepare.payload),
        );
        con.insert(prepare.query_id.clone(), outer);
        (prepare.query_id, STMPaxos2PCRMAction::Wait)
      }
    }
    RMMessage::Abort(abort) => {
      if let Some(es) = con.get_mut(&abort.query_id) {
        (abort.query_id, es.handle_abort(ctx, io_ctx))
      } else {
        let this_node_path = ctx.mk_node_path();
        ctx.send_to_tm(
          io_ctx,
          &abort.tm,
          T::tm_msg(TMMessage::Closed(Closed {
            query_id: abort.query_id.clone(),
            rm: this_node_path,
            payload: InnerT::mk_closed(),
          })),
        );
        (abort.query_id, STMPaxos2PCRMAction::Wait)
      }
    }
    RMMessage::Commit(commit) => {
      let query_id = commit.query_id.clone();
      if let Some(es) = con.get_mut(&query_id) {
        (query_id, es.handle_commit(ctx, io_ctx, commit.clone()))
      } else {
        let this_node_path = ctx.mk_node_path();
        ctx.send_to_tm(
          io_ctx,
          &commit.tm,
          T::tm_msg(TMMessage::Closed(Closed {
            query_id: query_id.clone(),
            rm: this_node_path,
            payload: InnerT::mk_closed(),
          })),
        );
        (query_id, STMPaxos2PCRMAction::Wait)
      }
    }
  }
}
