use crate::alter_table_es::State;
use crate::common::IOTypes;
use crate::model::common::{proc, QueryId, Timestamp};
use crate::model::message as msg;
use crate::tablet::TabletContext;
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  DropTableES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct DropExecuting {
  pub query_id: QueryId,
  pub prepare_timestamp: Timestamp,
  pub state: State,
}
#[derive(Debug)]
pub enum DropTableES {
  Committed(Timestamp),
  DropExecuting(DropExecuting),
}

pub enum DropTableAction {
  /// This tells the parent Server to wait.
  Wait,
}
// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl DropTableES {
  pub fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> DropTableAction {
    DropTableAction::Wait
  }

  pub fn handle_prepare<T: IOTypes>(
    &mut self,
    prepare: msg::DropTablePrepare,
    ctx: &mut TabletContext<T>,
  ) -> DropTableAction {
    DropTableAction::Wait
  }

  pub fn handle_commit<T: IOTypes>(
    &mut self,
    commit: msg::DropTableCommit,
    ctx: &mut TabletContext<T>,
  ) -> DropTableAction {
    DropTableAction::Wait
  }

  pub fn handle_abort<T: IOTypes>(
    &mut self,
    abort: msg::DropTableAbort,
    ctx: &mut TabletContext<T>,
  ) -> DropTableAction {
    DropTableAction::Wait
  }
}
