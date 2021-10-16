use crate::alter_table_es::State;
use crate::common::{SlaveIOCtx, TableSchema};
use crate::model::common::{
  proc, ColName, ColType, Gen, QueryId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::multiversion_map::MVM;
use crate::server::ServerContextBase;
use crate::slave::SlaveContext;
use crate::slave::{plm, SlavePLm};
use crate::tablet::TabletCreateHelper;
use rand::RngCore;

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

  pub fn handle_prepare<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
  ) -> CreateTableAction {
    match &self.state {
      State::Prepared => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::CreateTablePrepared(
          msg::CreateTablePrepared { query_id: self.query_id.clone(), sid },
        ));
      }
      _ => {}
    }
    CreateTableAction::Wait
  }

  pub fn handle_commit(&mut self, ctx: &mut SlaveContext) -> CreateTableAction {
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

  pub fn handle_abort<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
  ) -> CreateTableAction {
    match &self.state {
      State::WaitingInsertingPrepared => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::CreateTableCloseConfirm(
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

  pub fn handle_prepared_plm<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
  ) -> CreateTableAction {
    match &self.state {
      State::InsertingPrepared => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::CreateTablePrepared(
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

  pub fn handle_aborted_plm<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
  ) -> CreateTableAction {
    match &self.state {
      State::Follower => CreateTableAction::Exit,
      State::InsertingAborted => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::CreateTableCloseConfirm(
          msg::CreateTableCloseConfirm { query_id: self.query_id.clone(), sid },
        ));
        CreateTableAction::Exit
      }
      _ => CreateTableAction::Wait,
    }
  }

  /// Create a Tablet as specified by this ES
  fn create_table<IO: SlaveIOCtx>(&mut self, ctx: &mut SlaveContext, io_ctx: &mut IO) {
    // Construct the Tablet
    let mut rand_seed = [0; 16];
    io_ctx.rand().fill_bytes(&mut rand_seed);
    let helper = TabletCreateHelper {
      rand_seed,
      this_slave_group_id: ctx.this_slave_group_id.clone(),
      this_tablet_group_id: self.tablet_group_id.clone(),
      this_eid: ctx.this_eid.clone(),
      gossip: ctx.gossip.clone(),
      leader_map: ctx.leader_map.clone(),
      this_table_path: self.table_path.clone(),
      this_table_key_range: self.key_range.clone(),
      table_schema: TableSchema {
        key_cols: self.key_cols.clone(),
        val_cols: MVM::init(self.val_cols.clone().into_iter().collect()),
      },
    };
    io_ctx.create_tablet(helper);
  }

  pub fn handle_committed_plm<IO: SlaveIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
  ) -> CreateTableAction {
    match &self.state {
      State::Follower => {
        self.create_table(ctx, io_ctx);
        CreateTableAction::Exit
      }
      State::InsertingCommitted => {
        let sid = ctx.this_slave_group_id.clone();
        ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::CreateTableCloseConfirm(
          msg::CreateTableCloseConfirm { query_id: self.query_id.clone(), sid },
        ));
        self.create_table(ctx, io_ctx);
        CreateTableAction::Exit
      }
      _ => CreateTableAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<IO: SlaveIOCtx>(&mut self, ctx: &mut SlaveContext) -> CreateTableAction {
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

  pub fn leader_changed<IO: SlaveIOCtx>(&mut self, ctx: &mut SlaveContext) -> CreateTableAction {
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
