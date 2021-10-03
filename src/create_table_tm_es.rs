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
  Committed,
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
  pub tablet_group_id: TabletGroupId,
  pub table_path: TablePath,

  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,

  pub shards: Vec<(TabletKeyRange, TabletGroupId, SlaveGroupId)>,

  // STMPaxos2PCTM state
  pub state: CreateTableTMS,
}
