use crate::common::CoreIOCtx;
use crate::model::common::{proc, QueryId, Timestamp};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::tablet::{plm, TabletContext, TabletPLm};

// -----------------------------------------------------------------------------------------------
//  AlterTableES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum State {
  Follower,
  WaitingInsertingPrepared,
  InsertingPrepared,
  Prepared,
  InsertingCommitted,
  InsertingPreparedAborted,
  InsertingAborted,
}

#[derive(Debug)]
pub struct AlterTableES {
  pub query_id: QueryId,
  pub alter_op: proc::AlterOp,
  pub prepared_timestamp: Timestamp,
  pub state: State,
}

pub enum AlterTableAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl AlterTableES {
  // STMPaxos2PC messages

  pub fn handle_prepare<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> AlterTableAction {
    match &self.state {
      State::Prepared => {
        self.send_prepared(ctx, io_ctx);
      }
      _ => {}
    }
    AlterTableAction::Wait
  }

  pub fn handle_commit(
    &mut self,
    ctx: &mut TabletContext,
    commit: msg::AlterTableCommit,
  ) -> AlterTableAction {
    match &self.state {
      State::Prepared => {
        ctx.tablet_bundle.push(TabletPLm::AlterTableCommitted(plm::AlterTableCommitted {
          query_id: self.query_id.clone(),
          timestamp: commit.timestamp,
        }));
        self.state = State::InsertingCommitted;
      }
      _ => {}
    }
    AlterTableAction::Wait
  }

  pub fn handle_abort<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> AlterTableAction {
    match &self.state {
      State::WaitingInsertingPrepared => {
        self.send_close_confirm(ctx, io_ctx);
        AlterTableAction::Exit
      }
      State::InsertingPrepared => {
        ctx.tablet_bundle.push(TabletPLm::AlterTableAborted(plm::AlterTableAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = State::InsertingPreparedAborted;
        AlterTableAction::Wait
      }
      State::Prepared => {
        ctx.tablet_bundle.push(TabletPLm::AlterTableAborted(plm::AlterTableAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = State::InsertingAborted;
        AlterTableAction::Wait
      }
      _ => AlterTableAction::Wait,
    }
  }

  // STMPaxos2PC PLm Insertions

  pub fn handle_prepared_plm<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> AlterTableAction {
    match &self.state {
      State::InsertingPrepared => {
        self.send_prepared(ctx, io_ctx);
        self.state = State::Prepared;
      }
      State::InsertingPreparedAborted => {
        self.state = State::InsertingAborted;
      }
      _ => {}
    }
    AlterTableAction::Wait
  }

  pub fn handle_aborted_plm<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
  ) -> AlterTableAction {
    match &self.state {
      State::Follower => AlterTableAction::Exit,
      State::InsertingAborted => {
        self.send_close_confirm(ctx, io_ctx);
        AlterTableAction::Exit
      }
      _ => AlterTableAction::Wait,
    }
  }

  /// Apply the `alter_op` to this Tablet's `table_schema`.
  fn apply_alter_op(&mut self, ctx: &mut TabletContext, committed_plm: plm::AlterTableCommitted) {
    ctx.table_schema.val_cols.write(
      &self.alter_op.col_name,
      self.alter_op.maybe_col_type.clone(),
      committed_plm.timestamp,
    );
  }

  pub fn handle_committed_plm<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    committed_plm: plm::AlterTableCommitted,
  ) -> AlterTableAction {
    match &self.state {
      State::Follower => {
        self.apply_alter_op(ctx, committed_plm);
        AlterTableAction::Exit
      }
      State::InsertingCommitted => {
        self.send_close_confirm(ctx, io_ctx);
        self.apply_alter_op(ctx, committed_plm);
        AlterTableAction::Exit
      }
      _ => AlterTableAction::Wait,
    }
  }

  // Other

  pub fn start_inserting(&mut self, ctx: &mut TabletContext) -> AlterTableAction {
    match &self.state {
      State::WaitingInsertingPrepared => {
        ctx.tablet_bundle.push(TabletPLm::AlterTablePrepared(plm::AlterTablePrepared {
          query_id: self.query_id.clone(),
          alter_op: self.alter_op.clone(),
          timestamp: self.prepared_timestamp.clone(),
        }));
        self.state = State::InsertingPrepared;
      }
      _ => {}
    }
    AlterTableAction::Wait
  }

  pub fn leader_changed(&mut self, ctx: &mut TabletContext) -> AlterTableAction {
    match &self.state {
      State::Follower => {
        if ctx.is_leader() {
          self.state = State::Prepared;
        }
        AlterTableAction::Wait
      }
      State::WaitingInsertingPrepared => AlterTableAction::Exit,
      State::InsertingPrepared => AlterTableAction::Exit,
      State::Prepared => {
        self.state = State::Follower;
        AlterTableAction::Wait
      }
      State::InsertingCommitted => {
        self.state = State::Follower;
        AlterTableAction::Wait
      }
      State::InsertingPreparedAborted => AlterTableAction::Exit,
      State::InsertingAborted => {
        self.state = State::Follower;
        AlterTableAction::Wait
      }
    }
  }

  // Helpers

  fn send_prepared<IO: CoreIOCtx>(&self, ctx: &mut TabletContext, io_ctx: &mut IO) {
    let this_node_path = ctx.mk_node_path();
    ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::AlterTablePrepared(
      msg::AlterTablePrepared {
        query_id: self.query_id.clone(),
        rm: this_node_path,
        timestamp: self.prepared_timestamp.clone(),
      },
    ));
  }

  fn send_close_confirm<IO: CoreIOCtx>(&self, ctx: &mut TabletContext, io_ctx: &mut IO) {
    let this_node_path = ctx.mk_node_path();
    ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::AlterTableCloseConfirm(
      msg::AlterTableCloseConfirm { query_id: self.query_id.clone(), rm: this_node_path },
    ));
  }
}
