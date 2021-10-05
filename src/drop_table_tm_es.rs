use crate::common::IOTypes;
use crate::create_table_tm_es::{ResponseData, TMClosedState};
use crate::master::{plm, MasterContext, MasterPLm};
use crate::model::common::{EndpointId, QueryId, RequestId, TablePath, TabletGroupId, Timestamp};
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  DropTableTMES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum TMFollowerState {
  Preparing,
  Committed(Timestamp),
  Closed,
}

#[derive(Debug)]
pub enum DropTableTMS {
  Start,
  Follower(TMFollowerState),
  WaitingInsertTMPrepared,
  InsertTMPreparing,
  Preparing(HashMap<TabletGroupId, Option<Timestamp>>),
  InsertingTMCommitted,
  Committed(Timestamp, HashMap<TabletGroupId, Option<()>>),
  InsertingTMAborted,
  Aborted(HashMap<TabletGroupId, Option<()>>),
  InsertingTMClosed(TMClosedState),
}

#[derive(Debug)]
pub struct DropTableTMES {
  // Response data
  pub response_data: Option<ResponseData>,

  // DropTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,

  // STMPaxos2PCTM state
  pub state: DropTableTMS,
}

#[derive(Debug)]
pub enum DropTableTMAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementatiion
// -----------------------------------------------------------------------------------------------

impl DropTableTMES {
  pub fn handle_prepared_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> DropTableTMAction {
    DropTableTMAction::Wait
  }

  pub fn handle_aborted_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> DropTableTMAction {
    DropTableTMAction::Wait
  }

  pub fn handle_committed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> DropTableTMAction {
    DropTableTMAction::Wait
  }

  pub fn handle_closed_plm<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> DropTableTMAction {
    DropTableTMAction::Wait
  }

  pub fn starting_insert<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> DropTableTMAction {
    match self.state {
      DropTableTMS::WaitingInsertTMPrepared => {
        ctx.master_bundle.push(MasterPLm::DropTableTMPrepared(plm::DropTableTMPrepared {
          query_id: self.query_id.clone(),
          table_path: self.table_path.clone(),
        }));
        self.state = DropTableTMS::InsertTMPreparing;
      }
      _ => {}
    }
    DropTableTMAction::Wait
  }
}
