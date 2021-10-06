use crate::common::IOTypes;
use crate::common::RemoteLeaderChangedPLm;
use crate::finish_query_es::Paxos2PCRMState::Follower;
use crate::model::common::{CQueryPath, LeadershipId, QueryId, TQueryPath, Timestamp};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::storage::{commit_to_storage, GenericTable};
use crate::tablet::{plm, TabletPLm};
use crate::tablet::{ReadWriteRegion, TabletContext};

// -----------------------------------------------------------------------------------------------
//  FinishQueryES
// -----------------------------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct OrigTMLeadership {
  pub orig_tm_lid: LeadershipId,
}

#[derive(Debug)]
pub enum Paxos2PCRMState {
  Follower,
  WaitingInsertingPrepared(OrigTMLeadership),
  InsertingPrepared(OrigTMLeadership),
  Prepared,
  InsertingCommitted,
  InsertingPrepareAborted,
  InsertingAborted,
}

#[derive(Debug)]
pub struct FinishQueryExecuting {
  pub query_id: QueryId,
  pub tm: CQueryPath,
  pub all_rms: Vec<TQueryPath>,

  pub region_lock: ReadWriteRegion,
  pub timestamp: Timestamp,
  pub update_view: GenericTable,
  pub state: Paxos2PCRMState,
}

#[derive(Debug)]
pub enum FinishQueryES {
  Committed,
  Aborted,
  FinishQueryExecuting(FinishQueryExecuting),
}

pub enum FinishQueryAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl FinishQueryES {
  // Paxos2PC messages

  pub fn handle_prepare<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &es.state {
        Paxos2PCRMState::Prepared => {
          es.send_prepared(ctx);
        }
        _ => {}
      },
      _ => {}
    }
    FinishQueryAction::Wait
  }

  pub fn handle_abort<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &es.state {
        Paxos2PCRMState::WaitingInsertingPrepared(_) => {
          ctx.inserting_prepared_writes.remove(&es.timestamp);
          FinishQueryAction::Exit
        }
        Paxos2PCRMState::InsertingPrepared(_) => {
          es.state = Paxos2PCRMState::InsertingPrepareAborted;
          ctx.tablet_bundle.push(TabletPLm::FinishQueryAborted(plm::FinishQueryAborted {
            query_id: es.query_id.clone(),
          }));
          FinishQueryAction::Wait
        }
        Paxos2PCRMState::Prepared => {
          es.state = Paxos2PCRMState::InsertingAborted;
          ctx.tablet_bundle.push(TabletPLm::FinishQueryAborted(plm::FinishQueryAborted {
            query_id: es.query_id.clone(),
          }));
          FinishQueryAction::Wait
        }
        _ => FinishQueryAction::Wait,
      },
      _ => FinishQueryAction::Wait,
    }
  }

  pub fn handle_commit<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &es.state {
        Paxos2PCRMState::Prepared => {
          es.state = Paxos2PCRMState::InsertingCommitted;
          ctx.tablet_bundle.push(TabletPLm::FinishQueryCommitted(plm::FinishQueryCommitted {
            query_id: es.query_id.clone(),
          }));
        }
        _ => {}
      },
      _ => {}
    }
    FinishQueryAction::Wait
  }

  pub fn handle_check_prepared<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    check_prepared: msg::FinishQueryCheckPrepared,
  ) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &es.state {
        Paxos2PCRMState::WaitingInsertingPrepared(_) => es.send_wait(ctx),
        Paxos2PCRMState::InsertingPrepared(_) => es.send_wait(ctx),
        Paxos2PCRMState::Prepared => es.send_prepared(ctx),
        Paxos2PCRMState::InsertingCommitted => es.send_prepared(ctx),
        Paxos2PCRMState::InsertingPrepareAborted => es.send_wait(ctx),
        Paxos2PCRMState::InsertingAborted => es.send_prepared(ctx),
        _ => {}
      },
      FinishQueryES::Committed => {
        let this_query_path = ctx.mk_query_path(check_prepared.query_id.clone());
        ctx.ctx().send_to_c(
          check_prepared.tm.node_path.clone(),
          msg::CoordMessage::FinishQueryPrepared(msg::FinishQueryPrepared {
            return_qid: check_prepared.tm.query_id.clone(),
            rm_path: this_query_path,
          }),
        );
      }
      FinishQueryES::Aborted => {
        let this_query_path = ctx.mk_query_path(check_prepared.query_id.clone());
        ctx.ctx().send_to_c(
          check_prepared.tm.node_path.clone(),
          msg::CoordMessage::FinishQueryAborted(msg::FinishQueryAborted {
            return_qid: check_prepared.tm.query_id.clone(),
            rm_path: this_query_path,
          }),
        );
      }
    }
    FinishQueryAction::Wait
  }

  // Paxos2PC PLm Insertions

  pub fn handle_prepared_plm<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
  ) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &es.state {
        Paxos2PCRMState::InsertingPrepared(exec_state) => {
          let cur_tm_lid = ctx.leader_map.get(&es.tm.node_path.sid.to_gid()).unwrap();
          if &exec_state.orig_tm_lid != cur_tm_lid {
            es.send_inform_prepared(ctx);
          } else {
            es.send_prepared(ctx);
          }

          let region_lock = ctx.inserting_prepared_writes.remove(&es.timestamp).unwrap();
          ctx.prepared_writes.insert(es.timestamp.clone(), region_lock);
          es.state = Paxos2PCRMState::Prepared;
        }
        Paxos2PCRMState::InsertingPrepareAborted => {
          let region_lock = ctx.inserting_prepared_writes.remove(&es.timestamp).unwrap();
          ctx.prepared_writes.insert(es.timestamp.clone(), region_lock);
          es.state = Paxos2PCRMState::InsertingAborted;
        }
        _ => {}
      },
      _ => {}
    }
    FinishQueryAction::Wait
  }

  pub fn handle_aborted_plm<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
  ) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &es.state {
        Paxos2PCRMState::Follower | Paxos2PCRMState::InsertingAborted => {
          ctx.prepared_writes.remove(&es.timestamp).unwrap();
          *self = FinishQueryES::Aborted;
        }
        _ => {}
      },
      _ => {}
    }
    FinishQueryAction::Wait
  }

  pub fn handle_committed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
  ) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &es.state {
        Paxos2PCRMState::Follower | Paxos2PCRMState::InsertingCommitted => {
          commit_to_storage(&mut ctx.storage, &es.timestamp, es.update_view.clone());
          let region_lock = ctx.prepared_writes.remove(&es.timestamp).unwrap();
          ctx.committed_writes.insert(es.timestamp.clone(), region_lock);
          *self = FinishQueryES::Committed;
        }
        _ => {}
      },
      _ => {}
    }
    FinishQueryAction::Wait
  }

  // Other

  pub fn start_inserting<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &es.state {
        Paxos2PCRMState::WaitingInsertingPrepared(orig_leadership) => {
          es.state = Paxos2PCRMState::InsertingPrepared(orig_leadership.clone());
          ctx.tablet_bundle.push(TabletPLm::FinishQueryPrepared(plm::FinishQueryPrepared {
            query_id: es.query_id.clone(),
            tm: es.tm.clone(),
            all_rms: es.all_rms.clone(),
            region_lock: es.region_lock.clone(),
            timestamp: es.timestamp.clone(),
            update_view: es.update_view.clone(),
          }));
        }
        _ => {}
      },
      _ => {}
    }
    FinishQueryAction::Wait
  }

  pub fn remote_leader_changed<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &mut es.state {
        Paxos2PCRMState::Prepared => {
          if remote_leader_changed.gid == es.tm.node_path.sid.to_gid() {
            // The TM Leadership changed
            es.send_inform_prepared(ctx);
          }
        }
        _ => {}
      },
      _ => {}
    }
    FinishQueryAction::Wait
  }

  pub fn leader_changed<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> FinishQueryAction {
    match self {
      FinishQueryES::FinishQueryExecuting(es) => match &mut es.state {
        Paxos2PCRMState::Follower => {
          if ctx.is_leader() {
            // This node gained Leadership
            es.send_inform_prepared(ctx);
            es.state = Paxos2PCRMState::Prepared;
          }
          FinishQueryAction::Wait
        }
        Paxos2PCRMState::WaitingInsertingPrepared(_) => {
          ctx.inserting_prepared_writes.remove(&es.timestamp);
          FinishQueryAction::Exit
        }
        Paxos2PCRMState::InsertingPrepared(_) => {
          ctx.inserting_prepared_writes.remove(&es.timestamp);
          FinishQueryAction::Exit
        }
        Paxos2PCRMState::Prepared => {
          es.state = Follower;
          FinishQueryAction::Wait
        }
        Paxos2PCRMState::InsertingCommitted => {
          es.state = Follower;
          FinishQueryAction::Wait
        }
        Paxos2PCRMState::InsertingPrepareAborted => {
          ctx.inserting_prepared_writes.remove(&es.timestamp);
          FinishQueryAction::Exit
        }
        Paxos2PCRMState::InsertingAborted => {
          es.state = Follower;
          FinishQueryAction::Wait
        }
        _ => FinishQueryAction::Wait,
      },
      _ => FinishQueryAction::Wait,
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  FinishQueryExecuting Implementation
// -----------------------------------------------------------------------------------------------

impl FinishQueryExecuting {
  /// Send a `FinishQueryPrepared` to the TM
  fn send_prepared<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    let this_query_path = ctx.mk_query_path(self.query_id.clone());
    ctx.ctx().send_to_c(
      self.tm.node_path.clone(),
      msg::CoordMessage::FinishQueryPrepared(msg::FinishQueryPrepared {
        return_qid: self.tm.query_id.clone(),
        rm_path: this_query_path,
      }),
    );
  }

  /// Send a `FinishQueryInformPrepared` to the TM
  fn send_inform_prepared<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    ctx.ctx().send_to_c(
      self.tm.node_path.clone(),
      msg::CoordMessage::FinishQueryInformPrepared(msg::FinishQueryInformPrepared {
        tm: self.tm.clone(),
        all_rms: self.all_rms.clone(),
      }),
    );
  }

  /// Send a `FinishQueryWait` to the TM
  fn send_wait<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    let this_query_path = ctx.mk_query_path(self.query_id.clone());
    ctx.ctx().send_to_c(
      self.tm.node_path.clone(),
      msg::CoordMessage::FinishQueryWait(msg::FinishQueryWait {
        return_qid: self.tm.query_id.clone(),
        rm_path: this_query_path,
      }),
    );
  }
}
