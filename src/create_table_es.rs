use crate::alter_table_es::State;
use crate::common::{IOTypes, NetworkOut, RemoteLeaderChangedPLm};
use crate::model::common::{
  proc, ColName, ColType, Gen, QueryId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::slave::SlaveContext;
use crate::slave::{plm, SlavePLm};
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  CreateTableES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct CreateTableES {
  pub query_id: QueryId,

  pub tablet_group_id: TabletGroupId,
  pub table_path: TablePath,
  pub gen: Gen,

  pub key_range: TabletKeyRange,
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,

  pub state: State,
}

pub enum CreateTableAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl CreateTableES {
  // STMPaxos2PC messages

  pub fn handle_prepare<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> CreateTableAction {
    match &self.state {
      State::Prepared => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::CreateTablePrepared(
          msg::CreateTablePrepared { query_id: self.query_id.clone(), sid },
        ));
      }
      _ => {}
    }
    CreateTableAction::Wait
  }

  pub fn handle_commit<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> CreateTableAction {
    match &self.state {
      State::Prepared => {
        ctx.slave_bundle.plms.push(SlavePLm::CreateTableCommitted(plm::CreateTableCommitted {
          query_id: self.query_id.clone(),
        }));
        self.state = State::InsertingCommitted;
      }
      _ => {}
    }
    CreateTableAction::Wait
  }

  pub fn handle_abort<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> CreateTableAction {
    match &self.state {
      State::WaitingInsertingPrepared => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::CreateTableCloseConfirm(
          msg::CreateTableCloseConfirm { query_id: self.query_id.clone(), sid },
        ));
        CreateTableAction::Exit
      }
      State::InsertingPrepared => {
        ctx.slave_bundle.plms.push(SlavePLm::CreateTableAborted(plm::CreateTableAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = State::InsertingPreparedAborted;
        CreateTableAction::Wait
      }
      State::Prepared => {
        ctx.slave_bundle.plms.push(SlavePLm::CreateTableAborted(plm::CreateTableAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = State::InsertingAborted;
        CreateTableAction::Wait
      }
      _ => CreateTableAction::Wait,
    }
  }

  // STMPaxos2PC PLm Insertions

  pub fn handle_prepared_plm<T: IOTypes>(
    &mut self,
    ctx: &mut SlaveContext<T>,
  ) -> CreateTableAction {
    match &self.state {
      State::InsertingPrepared => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::CreateTablePrepared(
          msg::CreateTablePrepared { query_id: self.query_id.clone(), sid },
        ));
        self.state = State::Prepared;
      }
      State::InsertingPreparedAborted => {
        self.state = State::InsertingAborted;
      }
      _ => {}
    }
    CreateTableAction::Wait
  }

  pub fn handle_aborted_plm<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> CreateTableAction {
    match &self.state {
      State::Follower => CreateTableAction::Exit,
      State::InsertingAborted => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::CreateTableCloseConfirm(
          msg::CreateTableCloseConfirm { query_id: self.query_id.clone(), sid },
        ));
        CreateTableAction::Exit
      }
      _ => CreateTableAction::Wait,
    }
  }

  /// Create a Tablet as specified by this ES
  fn create_table<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) {
    // TODO: create the Tablet
  }

  pub fn handle_committed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut SlaveContext<T>,
  ) -> CreateTableAction {
    match &self.state {
      State::Follower => {
        self.create_table(ctx);
        CreateTableAction::Exit
      }
      State::InsertingCommitted => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx().send_to_master(msg::MasterRemotePayload::CreateTableCloseConfirm(
          msg::CreateTableCloseConfirm { query_id: self.query_id.clone(), sid },
        ));
        self.create_table(ctx);
        CreateTableAction::Exit
      }
      _ => CreateTableAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> CreateTableAction {
    match &self.state {
      State::WaitingInsertingPrepared => {
        ctx.slave_bundle.plms.push(SlavePLm::CreateTablePrepared(plm::CreateTablePrepared {
          query_id: self.query_id.clone(),
          tablet_group_id: self.tablet_group_id.clone(),
          table_path: self.table_path.clone(),
          gen: self.gen.clone(),
          key_range: self.key_range.clone(),
          key_cols: self.key_cols.clone(),
          val_cols: self.val_cols.clone(),
        }));
        self.state = State::InsertingPrepared;
      }
      _ => {}
    }
    CreateTableAction::Wait
  }

  pub fn leader_changed<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> CreateTableAction {
    match &self.state {
      State::Follower => {
        if ctx.is_leader() {
          self.state = State::Prepared;
        }
        CreateTableAction::Wait
      }
      State::WaitingInsertingPrepared => CreateTableAction::Exit,
      State::InsertingPrepared => CreateTableAction::Exit,
      State::Prepared => {
        self.state = State::Follower;
        CreateTableAction::Wait
      }
      State::InsertingCommitted => {
        self.state = State::Follower;
        CreateTableAction::Wait
      }
      State::InsertingPreparedAborted => CreateTableAction::Exit,
      State::InsertingAborted => {
        self.state = State::Follower;
        CreateTableAction::Wait
      }
    }
  }
}
