use crate::common::{GossipData, IOTypes, NetworkOut};
use crate::create_table_tm_es::{ResponseData, TMClosedState};
use crate::master::MasterContext;
use crate::model::common::{
  proc, EndpointId, Gen, QueryId, RequestId, TablePath, TabletGroupId, Timestamp,
};
use crate::model::message as msg;
use std::cmp::max;
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  AlterTableES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum TMFollowerState {
  Preparing,
  Committed(Timestamp),
  Closed,
}

#[derive(Debug)]
pub enum AlterTableTMS {
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
pub struct AlterTableTMES {
  // Response data
  pub response_data: Option<ResponseData>,

  // AlterTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,
  pub alter_op: proc::AlterOp,

  // STMPaxos2PCTM state
  pub state: AlterTableTMS,
}

pub enum AlterTableAction {
  /// This tells the parent Server to wait.
  Wait,
  /// Indicates the ES succeeded. This returns the Timestamp which the AlterOp was Committed.
  Success(Timestamp),
  /// Indicates the AlterOp was Column Invalid.
  ColumnInvalid,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl AlterTableTMES {}
