use crate::model::common::{EndpointId, QueryId, RequestId, TQueryPath, TableView, Timestamp};
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  FinishQueryTMES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct FinishQueryOrigTMES {
  pub request_id: RequestId,
  pub sender_path: EndpointId,
  pub tablet_view: TableView,
  pub timestamp: Timestamp,

  pub query_id: QueryId,
  pub all_rms: Vec<TQueryPath>,
  pub state: HashMap<TQueryPath, bool>,
}

#[derive(Debug)]
pub struct FinishQueryRecTMES {
  pub query_id: QueryId,
  pub all_rms: Vec<TQueryPath>,
  pub state: HashMap<TQueryPath, bool>,
}

#[derive(Debug)]
pub enum FinishQueryTMES {
  Orig(FinishQueryOrigTMES),
  Rec(FinishQueryRecTMES),
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------
