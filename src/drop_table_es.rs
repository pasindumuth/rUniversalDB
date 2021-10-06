use crate::alter_table_es::State;
use crate::common::IOTypes;
use crate::model::common::{proc, QueryId, Timestamp};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::tablet::{plm, TabletContext, TabletPLm};
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  DropTableES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct DropTableES {
  pub query_id: QueryId,
  pub prepared_timestamp: Timestamp,
  pub state: State,
}

pub enum DropTableAction {
  Wait,
  Aborted,
  Committed(Timestamp),
}
// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl DropTableES {
  // STMPaxos2PC messages

  pub fn handle_prepare<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> DropTableAction {
    match &self.state {
      State::Prepared => {
        let this_node_path = ctx.mk_node_path();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::DropTablePrepared(
          msg::DropTablePrepared {
            query_id: self.query_id.clone(),
            rm: this_node_path,
            timestamp: self.prepared_timestamp.clone(),
          },
        ));
      }
      _ => {}
    }
    DropTableAction::Wait
  }

  pub fn handle_commit<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    commit: msg::DropTableCommit,
  ) -> DropTableAction {
    match &self.state {
      State::Prepared => {
        ctx.tablet_bundle.push(TabletPLm::DropTableCommitted(plm::DropTableCommitted {
          query_id: self.query_id.clone(),
          timestamp: commit.timestamp,
        }));
        self.state = State::InsertingCommitted;
      }
      _ => {}
    }
    DropTableAction::Wait
  }

  pub fn handle_abort<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> DropTableAction {
    match &self.state {
      State::WaitingInsertingPrepared => {
        let this_node_path = ctx.mk_node_path();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::DropTableCloseConfirm(
          msg::DropTableCloseConfirm { query_id: self.query_id.clone(), rm: this_node_path },
        ));
        DropTableAction::Aborted
      }
      State::InsertingPrepared => {
        ctx.tablet_bundle.push(TabletPLm::DropTableAborted(plm::DropTableAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = State::InsertingPreparedAborted;
        DropTableAction::Wait
      }
      State::Prepared => {
        ctx.tablet_bundle.push(TabletPLm::DropTableAborted(plm::DropTableAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = State::InsertingAborted;
        DropTableAction::Wait
      }
      _ => DropTableAction::Wait,
    }
  }

  // STMPaxos2PC PLm Insertions

  pub fn handle_prepared_plm<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> DropTableAction {
    match &self.state {
      State::InsertingPrepared => {
        let this_node_path = ctx.mk_node_path();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::DropTablePrepared(
          msg::DropTablePrepared {
            query_id: self.query_id.clone(),
            rm: this_node_path,
            timestamp: self.prepared_timestamp.clone(),
          },
        ));
        self.state = State::Prepared;
      }
      State::InsertingPreparedAborted => {
        self.state = State::InsertingAborted;
      }
      _ => {}
    }
    DropTableAction::Wait
  }

  pub fn handle_aborted_plm<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> DropTableAction {
    match &self.state {
      State::Follower => DropTableAction::Aborted,
      State::InsertingAborted => {
        let this_node_path = ctx.mk_node_path();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::DropTableCloseConfirm(
          msg::DropTableCloseConfirm { query_id: self.query_id.clone(), rm: this_node_path },
        ));
        DropTableAction::Aborted
      }
      _ => DropTableAction::Wait,
    }
  }

  pub fn handle_committed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    committed_plm: plm::DropTableCommitted,
  ) -> DropTableAction {
    match &self.state {
      State::Follower => DropTableAction::Committed(committed_plm.timestamp.clone()),
      State::InsertingCommitted => {
        let this_node_path = ctx.mk_node_path();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::DropTableCloseConfirm(
          msg::DropTableCloseConfirm { query_id: self.query_id.clone(), rm: this_node_path },
        ));
        DropTableAction::Committed(committed_plm.timestamp.clone())
      }
      _ => DropTableAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> DropTableAction {
    match &self.state {
      State::WaitingInsertingPrepared => {
        ctx.tablet_bundle.push(TabletPLm::DropTablePrepared(plm::DropTablePrepared {
          query_id: self.query_id.clone(),
          timestamp: self.prepared_timestamp.clone(),
        }));
        self.state = State::InsertingPrepared;
      }
      _ => {}
    }
    DropTableAction::Wait
  }

  pub fn leader_changed<T: IOTypes>(&mut self, _: &mut TabletContext<T>) -> DropTableAction {
    match &self.state {
      State::Follower => {
        self.state = State::Prepared;
        DropTableAction::Wait
      }
      State::WaitingInsertingPrepared => DropTableAction::Aborted,
      State::InsertingPrepared => DropTableAction::Aborted,
      State::Prepared => {
        self.state = State::Follower;
        DropTableAction::Wait
      }
      State::InsertingCommitted => {
        self.state = State::Follower;
        DropTableAction::Wait
      }
      State::InsertingPreparedAborted => DropTableAction::Aborted,
      State::InsertingAborted => {
        self.state = State::Follower;
        DropTableAction::Wait
      }
    }
  }
}
