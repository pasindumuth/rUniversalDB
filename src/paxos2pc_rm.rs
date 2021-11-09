use crate::common::{BasicIOCtx, RemoteLeaderChangedPLm};
use crate::model::common::{LeadershipId, QueryId};
use crate::paxos2pc_tm::{
  Aborted, CheckPrepared, Commit, InformPrepared, PayloadTypes, Prepared, RMAbortedPLm,
  RMCommittedPLm, RMMessage, RMPLm, RMPreparedPLm, RMServerContext, TMMessage, TMPathTrait, Wait,
};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Paxos2PCRMInner
// -----------------------------------------------------------------------------------------------

pub trait Paxos2PCRMInner<T: PayloadTypes> {
  /// Constructs an instance of `Paxos2PCRMInner` from a Prepared PLm. This is used primarily
  /// by the Follower.
  fn new<IO: BasicIOCtx<T::NetworkMessageT>>(ctx: &mut T::RMContext, io_ctx: &mut IO) -> Self;

  /// Constructs an instance of `Paxos2PCRMInner` from a Prepared PLm. This is used primarily
  /// by the Follower.
  fn new_follower<IO: BasicIOCtx<T::NetworkMessageT>>(
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    payload: T::RMPreparedPLm,
  ) -> Self;

  /// Called if an `Abort` is received before any PL insertions.
  fn early_aborted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> T::RMAbortedPLm;

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
  );

  /// Called after all RMs have Prepared.
  fn mk_committed_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> T::RMCommittedPLm;

  /// Called after CommittedPLm is inserted.
  fn committed_plm_inserted<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  );

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
}

// -----------------------------------------------------------------------------------------------
//  Paxos2PCRMOuter
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct OrigTMLeadership {
  pub orig_tm_lid: LeadershipId,
}

#[derive(Debug)]
pub enum State {
  Follower,
  WaitingInsertingPrepared(OrigTMLeadership),
  InsertingPrepared(OrigTMLeadership),
  Prepared,
  InsertingCommitted,
  InsertingPreparedAborted,
  InsertingAborted,
}

#[derive(Debug)]
pub struct Paxos2PCRMExecOuter<T: PayloadTypes, InnerT> {
  pub query_id: QueryId,
  pub tm: T::TMPath,
  pub rms: Vec<T::RMPath>,

  pub state: State,
  pub inner: InnerT,
}

#[derive(Debug)]
pub enum Paxos2PCRMOuter<T: PayloadTypes, InnerT> {
  Committed,
  Aborted,
  Paxos2PCRMExecOuter(Paxos2PCRMExecOuter<T, InnerT>),
}

pub enum Paxos2PCRMAction {
  Wait,
  Exit,
}

impl<T: PayloadTypes, InnerT: Paxos2PCRMInner<T>> Paxos2PCRMOuter<T, InnerT> {
  pub fn new(
    ctx: &mut T::RMContext,
    query_id: QueryId,
    tm: T::TMPath,
    rms: Vec<T::RMPath>,
    inner: InnerT,
  ) -> Paxos2PCRMOuter<T, InnerT> {
    let state = State::WaitingInsertingPrepared(OrigTMLeadership {
      orig_tm_lid: ctx.leader_map().get(&tm.to_gid()).unwrap().clone(),
    });
    Paxos2PCRMOuter::Paxos2PCRMExecOuter(Paxos2PCRMExecOuter { query_id, tm, rms, state, inner })
  }

  /// This is only called when the `PreparedPLm` is insert at a Follower node.
  fn init_follower<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => {
        es.inner.prepared_plm_inserted(ctx, io_ctx);
        es.state = State::Follower;
      }
      _ => {}
    }
  }

  // Paxos2PC messages

  pub fn handle_prepare<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &es.state {
        State::Prepared => {
          es.send_prepared(ctx, io_ctx);
        }
        _ => {}
      },
      _ => {}
    }
    Paxos2PCRMAction::Wait
  }

  pub fn handle_commit<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &es.state {
        State::Prepared => {
          let committed_plm = T::rm_plm(RMPLm::Committed(RMCommittedPLm {
            query_id: es.query_id.clone(),
            payload: es.inner.mk_committed_plm(ctx, io_ctx),
          }));
          ctx.push_plm(committed_plm);
          es.state = State::InsertingCommitted;
        }
        _ => {}
      },
      _ => {}
    }
    Paxos2PCRMAction::Wait
  }

  pub fn handle_abort<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &es.state {
        State::WaitingInsertingPrepared(_) => {
          es.inner.early_aborted(ctx, io_ctx);
          Paxos2PCRMAction::Exit
        }
        State::InsertingPrepared(_) => {
          es.state = State::InsertingPreparedAborted;
          Paxos2PCRMAction::Wait
        }
        State::Prepared => {
          let aborted_plm = T::rm_plm(RMPLm::Aborted(RMAbortedPLm {
            query_id: es.query_id.clone(),
            payload: es.inner.mk_aborted_plm(ctx, io_ctx),
          }));
          ctx.push_plm(aborted_plm);
          es.state = State::InsertingAborted;
          Paxos2PCRMAction::Wait
        }
        _ => Paxos2PCRMAction::Wait,
      },
      _ => Paxos2PCRMAction::Wait,
    }
  }

  pub fn handle_check_prepared<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    check_prepared: CheckPrepared<T>,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &es.state {
        State::Follower => {}
        State::WaitingInsertingPrepared(_) => es.send_wait(ctx, io_ctx),
        State::InsertingPrepared(_) => es.send_wait(ctx, io_ctx),
        State::Prepared => es.send_prepared(ctx, io_ctx),
        State::InsertingCommitted => es.send_prepared(ctx, io_ctx),
        State::InsertingPreparedAborted => es.send_wait(ctx, io_ctx),
        State::InsertingAborted => es.send_prepared(ctx, io_ctx),
      },
      Paxos2PCRMOuter::Committed => {
        let this_node_path = ctx.mk_node_path();
        ctx.send_to_tm(
          io_ctx,
          &check_prepared.tm,
          T::tm_msg(TMMessage::Prepared(Prepared {
            query_id: check_prepared.query_id,
            rm: this_node_path,
          })),
        );
      }
      Paxos2PCRMOuter::Aborted => {
        let this_node_path = ctx.mk_node_path();
        ctx.send_to_tm(
          io_ctx,
          &check_prepared.tm,
          T::tm_msg(TMMessage::Aborted(Aborted {
            query_id: check_prepared.query_id,
            rm: this_node_path,
          })),
        );
      }
    }
    Paxos2PCRMAction::Wait
  }

  // Paxos2PC PLm Insertions

  pub fn handle_prepared_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &es.state {
        State::InsertingPrepared(OrigTMLeadership { orig_tm_lid }) => {
          let cur_tm_lid = ctx.leader_map().get(&es.tm.to_gid()).unwrap();
          if orig_tm_lid != cur_tm_lid {
            es.send_inform_prepared(ctx, io_ctx);
          } else {
            es.send_prepared(ctx, io_ctx);
          }

          es.inner.prepared_plm_inserted(ctx, io_ctx);
          es.state = State::Prepared;
        }
        State::InsertingPreparedAborted => {
          es.inner.prepared_plm_inserted(ctx, io_ctx);
          let aborted_plm = T::rm_plm(RMPLm::Aborted(RMAbortedPLm {
            query_id: es.query_id.clone(),
            payload: es.inner.mk_aborted_plm(ctx, io_ctx),
          }));
          ctx.push_plm(aborted_plm);
          es.state = State::InsertingAborted;
        }
        _ => {}
      },
      _ => {}
    }
    Paxos2PCRMAction::Wait
  }

  pub fn handle_committed_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &es.state {
        State::Follower | State::InsertingCommitted => {
          es.inner.committed_plm_inserted(ctx, io_ctx);
          *self = Paxos2PCRMOuter::Committed;
        }
        _ => {}
      },
      _ => {}
    }
    Paxos2PCRMAction::Wait
  }

  pub fn handle_aborted_plm<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &es.state {
        State::Follower | State::InsertingAborted => {
          es.inner.aborted_plm_inserted(ctx, io_ctx);
          *self = Paxos2PCRMOuter::Aborted;
        }
        _ => {}
      },
      _ => {}
    }
    Paxos2PCRMAction::Wait
  }

  // Other

  pub fn start_inserting<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &es.state {
        State::WaitingInsertingPrepared(orig_leadership) => {
          let prepared_plm = RMPreparedPLm {
            query_id: es.query_id.clone(),
            tm: es.tm.clone(),
            rms: es.rms.clone(),
            payload: es.inner.mk_prepared_plm(ctx, io_ctx),
          };
          ctx.push_plm(T::rm_plm(RMPLm::Prepared(prepared_plm)));
          es.state = State::InsertingPrepared(orig_leadership.clone());
        }
        _ => {}
      },
      _ => {}
    }
    Paxos2PCRMAction::Wait
  }

  pub fn remote_leader_changed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &mut es.state {
        State::Prepared => {
          if remote_leader_changed.gid == es.tm.to_gid() {
            // The TM Leadership changed
            es.send_inform_prepared(ctx, io_ctx);
          }
        }
        _ => {}
      },
      _ => {}
    }
    Paxos2PCRMAction::Wait
  }

  pub fn leader_changed<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) -> Paxos2PCRMAction {
    match self {
      Paxos2PCRMOuter::Paxos2PCRMExecOuter(es) => match &mut es.state {
        State::Follower => {
          if ctx.is_leader() {
            // This node gained Leadership
            es.send_inform_prepared(ctx, io_ctx);
            es.state = State::Prepared;
          }
          Paxos2PCRMAction::Wait
        }
        State::WaitingInsertingPrepared(_)
        | State::InsertingPrepared(_)
        | State::InsertingPreparedAborted => {
          es.inner.early_aborted(ctx, io_ctx);
          Paxos2PCRMAction::Exit
        }
        State::Prepared | State::InsertingCommitted | State::InsertingAborted => {
          es.state = State::Follower;
          Paxos2PCRMAction::Wait
        }
      },
      _ => Paxos2PCRMAction::Wait,
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  FinishQueryExecuting Implementation
// -----------------------------------------------------------------------------------------------

impl<T: PayloadTypes, InnerT: Paxos2PCRMInner<T>> Paxos2PCRMExecOuter<T, InnerT> {
  /// Send a `Prepared` to the TM
  fn send_prepared<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) {
    let this_node_path = ctx.mk_node_path();
    ctx.send_to_tm(
      io_ctx,
      &self.tm,
      T::tm_msg(TMMessage::Prepared(Prepared {
        query_id: self.query_id.clone(),
        rm: this_node_path,
      })),
    );
  }

  /// Send a `InformPrepared` to the TM
  fn send_inform_prepared<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) {
    ctx.send_to_tm(
      io_ctx,
      &self.tm,
      T::tm_msg(TMMessage::InformPrepared(InformPrepared {
        query_id: self.query_id.clone(),
        tm: self.tm.clone(),
        rms: self.rms.clone(),
      })),
    );
  }

  /// Send a `Wait` to the TM
  fn send_wait<IO: BasicIOCtx<T::NetworkMessageT>>(
    &mut self,
    ctx: &mut T::RMContext,
    io_ctx: &mut IO,
  ) {
    let this_node_path = ctx.mk_node_path();
    ctx.send_to_tm(
      io_ctx,
      &self.tm,
      T::tm_msg(TMMessage::Wait(Wait { query_id: self.query_id.clone(), rm: this_node_path })),
    );
  }
}

// -----------------------------------------------------------------------------------------------
//  Aggregate STM ES Management
// -----------------------------------------------------------------------------------------------
pub trait AggregateContainer<T: PayloadTypes, InnerT: Paxos2PCRMInner<T>> {
  fn get_mut(&mut self, query_id: &QueryId) -> Option<&mut Paxos2PCRMOuter<T, InnerT>>;

  fn insert(&mut self, query_id: QueryId, es: Paxos2PCRMOuter<T, InnerT>);
}

/// Implementation for BTreeMap, which is the common case.
impl<T: PayloadTypes, InnerT: Paxos2PCRMInner<T>> AggregateContainer<T, InnerT>
  for BTreeMap<QueryId, Paxos2PCRMOuter<T, InnerT>>
{
  fn get_mut(&mut self, query_id: &QueryId) -> Option<&mut Paxos2PCRMOuter<T, InnerT>> {
    self.get_mut(query_id)
  }

  fn insert(&mut self, query_id: QueryId, es: Paxos2PCRMOuter<T, InnerT>) {
    self.insert(query_id, es);
  }
}

/// Function to handle the insertion of an `RMPLm` for a given `AggregateContainer`.
pub fn handle_rm_plm<
  T: PayloadTypes,
  InnerT: Paxos2PCRMInner<T>,
  ConT: AggregateContainer<T, InnerT>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::RMContext,
  io_ctx: &mut IO,
  con: &mut ConT,
  plm: RMPLm<T>,
) -> (QueryId, Paxos2PCRMAction) {
  match plm {
    RMPLm::Prepared(prepared) => {
      if ctx.is_leader() {
        let es = con.get_mut(&prepared.query_id).unwrap();
        (prepared.query_id, es.handle_prepared_plm(ctx, io_ctx))
      } else {
        // Recall that for a Follower, for a Prepared, we must contruct
        // the ES for the first time.
        let inner = InnerT::new_follower(ctx, io_ctx, prepared.payload);
        let mut outer =
          Paxos2PCRMOuter::new(ctx, prepared.query_id.clone(), prepared.tm, prepared.rms, inner);
        outer.init_follower(ctx, io_ctx);
        con.insert(prepared.query_id.clone(), outer);
        (prepared.query_id, Paxos2PCRMAction::Wait)
      }
    }
    RMPLm::Committed(committed) => {
      let query_id = committed.query_id.clone();
      let es = con.get_mut(&query_id).unwrap();
      (query_id, es.handle_committed_plm(ctx, io_ctx))
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
  InnerT: Paxos2PCRMInner<T>,
  ConT: AggregateContainer<T, InnerT>,
  IO: BasicIOCtx<T::NetworkMessageT>,
>(
  ctx: &mut T::RMContext,
  io_ctx: &mut IO,
  con: &mut ConT,
  msg: RMMessage<T>,
) -> (QueryId, Paxos2PCRMAction) {
  match msg {
    RMMessage::Prepare(prepare) => {
      if let Some(es) = con.get_mut(&prepare.query_id) {
        (prepare.query_id, es.handle_prepare(ctx, io_ctx))
      } else {
        let inner = InnerT::new(ctx, io_ctx);
        let outer =
          Paxos2PCRMOuter::new(ctx, prepare.query_id.clone(), prepare.tm, prepare.rms, inner);
        con.insert(prepare.query_id.clone(), outer);
        (prepare.query_id, Paxos2PCRMAction::Wait)
      }
    }
    RMMessage::CheckPrepared(check_prepared) => {
      let query_id = check_prepared.query_id.clone();
      if let Some(es) = con.get_mut(&query_id) {
        (query_id, es.handle_check_prepared(ctx, io_ctx, check_prepared))
      } else {
        (query_id, Paxos2PCRMAction::Wait)
      }
    }
    RMMessage::Abort(abort) => {
      if let Some(es) = con.get_mut(&abort.query_id) {
        (abort.query_id, es.handle_abort(ctx, io_ctx))
      } else {
        (abort.query_id, Paxos2PCRMAction::Wait)
      }
    }
    RMMessage::Commit(commit) => {
      let query_id = commit.query_id.clone();
      if let Some(es) = con.get_mut(&query_id) {
        (query_id, es.handle_commit(ctx, io_ctx))
      } else {
        (query_id, Paxos2PCRMAction::Wait)
      }
    }
  }
}
