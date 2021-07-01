use crate::common::{GossipData, IOTypes, NetworkOut, OrigP};
use crate::model::common::{
  EndpointId, QueryId, QueryPath, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange,
};
use crate::model::message as msg;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ServerContext<'a, T: IOTypes> {
  /// IO Objects.
  pub rand: &'a mut T::RngCoreT,
  pub clock: &'a mut T::ClockT,
  pub network_output: &'a mut T::NetworkOutT,

  /// Metadata
  pub this_slave_group_id: &'a mut SlaveGroupId,
  pub master_eid: &'a mut EndpointId,

  /// Gossip
  pub gossip: &'a mut Arc<GossipData>,

  /// Distribution
  pub sharding_config: &'a mut HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: &'a mut HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: &'a mut HashMap<SlaveGroupId, EndpointId>,

  /// Child Queries
  pub master_query_map: &'a mut HashMap<QueryId, OrigP>,
}

impl<'a, T: IOTypes> ServerContext<'a, T> {
  /// This function infers weather the CommonQuery is destined for a Slave or a Tablet
  /// by using the `sender_path`, and then acts accordingly.
  pub fn send_to_path(self, sender_path: QueryPath, common_query: CommonQuery) {
    let sid = &sender_path.slave_group_id;
    let eid = self.slave_address_config.get(sid).unwrap();
    self.network_output.send(
      &eid,
      msg::NetworkMessage::Slave(
        if let Some(tablet_group_id) = sender_path.maybe_tablet_group_id {
          msg::SlaveMessage::TabletMessage(tablet_group_id.clone(), common_query.tablet_msg())
        } else {
          common_query.slave_msg()
        },
      ),
    );
  }
}

// -----------------------------------------------------------------------------------------------
//  Common Query
// -----------------------------------------------------------------------------------------------
// These enums are used for communication between algorithms.

/// This is a convenience enum to strealine the sending of PCSA messages to Tablets and Slaves.
pub enum CommonQuery {
  PerformQuery(msg::PerformQuery),
  CancelQuery(msg::CancelQuery),
  QueryAborted(msg::QueryAborted),
  QuerySuccess(msg::QuerySuccess),
}

impl CommonQuery {
  pub fn tablet_msg(self) -> msg::TabletMessage {
    match self {
      CommonQuery::PerformQuery(query) => msg::TabletMessage::PerformQuery(query),
      CommonQuery::CancelQuery(query) => msg::TabletMessage::CancelQuery(query),
      CommonQuery::QueryAborted(query) => msg::TabletMessage::QueryAborted(query),
      CommonQuery::QuerySuccess(query) => msg::TabletMessage::QuerySuccess(query),
    }
  }

  pub fn slave_msg(self) -> msg::SlaveMessage {
    match self {
      CommonQuery::PerformQuery(query) => msg::SlaveMessage::PerformQuery(query),
      CommonQuery::CancelQuery(query) => msg::SlaveMessage::CancelQuery(query),
      CommonQuery::QueryAborted(query) => msg::SlaveMessage::QueryAborted(query),
      CommonQuery::QuerySuccess(query) => msg::SlaveMessage::QuerySuccess(query),
    }
  }
}
