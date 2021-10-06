use crate::common::IOTypes;
use crate::master::{plm, MasterContext, MasterPLm};
use crate::model::common::{
  ColName, ColType, EndpointId, QueryId, RequestId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  General STMPaxos2PC TM Types
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct ResponseData {
  pub request_id: RequestId,
  pub sender_eid: EndpointId,
}

#[derive(Debug)]
pub enum TMClosedState {
  Committed(Timestamp),
  Aborted,
}

// -----------------------------------------------------------------------------------------------
//  CreateTableTMES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum CreateTableTMS {
  Start,
  Follower(TMFollowerState),
  WaitingInsertTMPrepared,
  InsertTMPreparing,
  Preparing(HashMap<TabletGroupId, Option<()>>),
  InsertingTMCommitted,
  Committed(HashMap<TabletGroupId, Option<()>>),
  InsertingTMAborted,
  Aborted(HashMap<TabletGroupId, Option<()>>),
  InsertingTMClosed(TMClosedState),
}

#[derive(Debug)]
pub enum TMFollowerState {
  Preparing,
  Committed,
  Closed,
}

#[derive(Debug)]
pub struct CreateTableTMES {
  // Response data
  pub response_data: Option<ResponseData>,

  // CreateTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,

  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,

  pub shards: Vec<(TabletKeyRange, TabletGroupId, SlaveGroupId)>,

  // STMPaxos2PCTM state
  pub state: CreateTableTMS,
}

#[derive(Debug)]
pub enum CreateTableTMAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl CreateTableTMES {
  pub fn handle_prepared_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> CreateTableTMAction {
    CreateTableTMAction::Wait
  }

  pub fn handle_aborted_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> CreateTableTMAction {
    CreateTableTMAction::Wait
  }

  pub fn handle_committed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> CreateTableTMAction {
    CreateTableTMAction::Wait
  }

  pub fn handle_closed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> CreateTableTMAction {
    CreateTableTMAction::Wait
  }

  pub fn start_inserting<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> CreateTableTMAction {
    match self.state {
      CreateTableTMS::WaitingInsertTMPrepared => {
        ctx.master_bundle.push(MasterPLm::CreateTableTMPrepared(plm::CreateTableTMPrepared {
          query_id: self.query_id.clone(),
          table_path: self.table_path.clone(),
          key_cols: self.key_cols.clone(),
          val_cols: self.val_cols.clone(),
          shards: self.shards.clone(),
        }));
        self.state = CreateTableTMS::InsertTMPreparing;
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }
}
