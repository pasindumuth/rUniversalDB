use crate::common::{BasicIOCtx, Timestamp};
use crate::common::{CNodePath, EndpointId, QueryId, RequestId, TNodePath, TQueryPath, TableView};
use crate::coord::CoordContext;
use crate::message as msg;
use crate::paxos2pc_tm::{
  Paxos2PCTMInner, Paxos2PCTMOuter, PayloadTypes, RMMessage, RMPLm, TMMessage,
};
use crate::sql_ast::proc;
use crate::storage::GenericTable;
use crate::tablet::{MSQueryES, ReadWriteRegion, TabletContext, TabletPLm};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

// RM PLm
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryRMPrepared {
  pub region_lock: ReadWriteRegion,
  pub timestamp: Timestamp,
  pub update_view: GenericTable,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryRMCommitted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryRMAborted {}

// TM-to-RM Messages
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryPrepare {
  /// Contains the QueryId of the MSQueryES that this `Prepare` has to take over
  pub query_id: QueryId,
}

// FinishQueryPayloadTypes

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FinishQueryPayloadTypes {}

impl PayloadTypes for FinishQueryPayloadTypes {
  // Master
  type RMPLm = TabletPLm;
  type RMPath = TNodePath;
  type TMPath = CNodePath;
  type RMMessage = msg::TabletMessage;
  type TMMessage = msg::CoordMessage;
  type NetworkMessageT = msg::NetworkMessage;
  type RMContext = TabletContext;
  type RMExtraData = BTreeMap<QueryId, MSQueryES>;
  type TMContext = CoordContext;

  // RM PLm
  type RMPreparedPLm = FinishQueryRMPrepared;
  type RMCommittedPLm = FinishQueryRMCommitted;
  type RMAbortedPLm = FinishQueryRMAborted;

  fn rm_plm(plm: RMPLm<Self>) -> Self::RMPLm {
    TabletPLm::FinishQuery(plm)
  }

  type Prepare = FinishQueryPrepare;

  fn rm_msg(msg: RMMessage<Self>) -> Self::RMMessage {
    msg::TabletMessage::FinishQuery(msg)
  }

  fn tm_msg(msg: TMMessage<Self>) -> Self::TMMessage {
    msg::CoordMessage::FinishQuery(msg)
  }
}

// -----------------------------------------------------------------------------------------------
//  FinishQueryTMES
// -----------------------------------------------------------------------------------------------

pub type FinishQueryTMES = Paxos2PCTMOuter<FinishQueryPayloadTypes, FinishQueryTMInner>;

#[derive(Debug)]
pub struct ResponseData {
  // Request values (values send in the original request)
  pub request_id: RequestId,
  pub sender_eid: EndpointId,
  /// We hold onto the original `MSQuery` in case of an Abort so that we can restart.
  pub sql_query: proc::MSQuery,

  // Result values (values computed by the MSCoordES)
  pub table_view: TableView,
  pub timestamp: Timestamp,
}

#[derive(Debug)]
pub struct FinishQueryTMInner {
  pub response_data: Option<ResponseData>,
  pub committed: bool,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl Paxos2PCTMInner<FinishQueryPayloadTypes> for FinishQueryTMInner {
  fn new_rec<IO: BasicIOCtx>(_: &mut CoordContext, _: &mut IO) -> FinishQueryTMInner {
    FinishQueryTMInner { response_data: None, committed: false }
  }

  fn committed<IO: BasicIOCtx>(&mut self, _: &mut CoordContext, _: &mut IO) {
    self.committed = true;
  }

  fn aborted<IO: BasicIOCtx>(&mut self, _: &mut CoordContext, _: &mut IO) {
    self.committed = false;
  }
}
