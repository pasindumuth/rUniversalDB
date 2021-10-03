use crate::create_table_tm_es::{ResponseData, TMClosedState};
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
