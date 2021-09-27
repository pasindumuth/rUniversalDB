use crate::common::IOTypes;
use crate::model::common::{proc, QueryId, Timestamp};
use crate::model::message as msg;
use crate::tablet::TabletContext;
use std::collections::HashMap;

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
  pub prepare_timestamp: Timestamp,
  pub state: State,
}

pub enum AlterTableAction {
  /// This tells the parent Server to wait.
  Wait,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl AlterTableES {
  pub fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> AlterTableAction {
    AlterTableAction::Wait
  }

  pub fn handle_prepare<T: IOTypes>(
    &mut self,
    prepare: msg::AlterTablePrepare,
    ctx: &mut TabletContext<T>,
  ) -> AlterTableAction {
    AlterTableAction::Wait
  }

  pub fn handle_commit<T: IOTypes>(
    &mut self,
    commit: msg::AlterTableCommit,
    ctx: &mut TabletContext<T>,
  ) -> AlterTableAction {
    AlterTableAction::Wait
  }

  pub fn handle_abort<T: IOTypes>(
    &mut self,
    abort: msg::AlterTableAbort,
    ctx: &mut TabletContext<T>,
  ) -> AlterTableAction {
    AlterTableAction::Wait
  }
}
