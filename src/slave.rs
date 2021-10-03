use crate::col_usage::{node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, lookup_pos, map_insert, merge_table_views, mk_qid, remove_item, Clock, GossipData,
  IOTypes, NetworkOut, OrigP, TMStatus, TabletForwardOut,
};
use crate::gr_query_es::{GRQueryAction, GRQueryES};
use crate::model::common::{
  iast, proc, CTQueryPath, ColName, ColType, Context, ContextRow, Gen, LeadershipId, NodeGroupId,
  PaxosGroupId, SlaveGroupId, TablePath, TableView, TabletGroupId, TabletKeyRange, TierMap,
  Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message as msg;
use crate::model::message::{GeneralQuery, QueryError, SlaveMessage, SlaveRemotePayload};
use crate::ms_query_coord_es::{
  FullMSCoordES, MSCoordES, MSQueryCoordAction, QueryPlanningES, QueryPlanningS,
};
use crate::query_converter::convert_to_msquery;
use crate::server::{CommonQuery, ServerContext};
use crate::sql_parser::convert_ast;
use crate::tablet::{GRQueryESWrapper, TransTableReadESWrapper};
use crate::trans_table_read_es::{
  TransExecutionS, TransTableAction, TransTableReadES, TransTableSource,
};
use serde::{Deserialize, Serialize};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  Slave State
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct SlaveState<T: IOTypes> {
  slave_context: SlaveContext<T>,
}

/// The SlaveState that holds all the state of the Slave
#[derive(Debug)]
pub struct SlaveContext<T: IOTypes> {
  /// IO Objects.
  pub rand: T::RngCoreT,
  pub clock: T::ClockT,
  pub network_output: T::NetworkOutT,
  pub tablet_forward_output: T::TabletForwardOutT,

  /// Metadata
  pub this_slave_group_id: SlaveGroupId,
  pub master_eid: EndpointId,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// Paxos
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  /// External Query Management
  pub external_request_id_map: HashMap<RequestId, QueryId>,
}

impl<T: IOTypes> SlaveState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    tablet_forward_output: T::TabletForwardOutT,
    gossip: Arc<GossipData>,
    this_slave_group_id: SlaveGroupId,
    master_eid: EndpointId,
  ) -> SlaveState<T> {
    SlaveState {
      slave_context: SlaveContext {
        rand,
        clock,
        network_output,
        tablet_forward_output,
        this_slave_group_id,
        master_eid,
        gossip,
        leader_map: Default::default(),
        external_request_id_map: Default::default(),
      },
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::SlaveMessage) {
    // self.slave_context.handle_incoming_message(&mut self.statuses, message);
  }
}

impl<T: IOTypes> SlaveContext<T> {
  /// Handles all messages, coming from Tablets, the Master, External, etc.
  pub fn handle_incoming_message(&mut self, message: msg::SlaveMessage) {
    match message {
      SlaveMessage::ExternalMessage(_) => {}
      SlaveMessage::RemoteMessage(remote_message) => match remote_message.payload {
        SlaveRemotePayload::RemoteLeaderChanged(_) => {}
        SlaveRemotePayload::CreateTablePrepare(_) => {}
        SlaveRemotePayload::CreateTableCommit(_) => {}
        SlaveRemotePayload::CreateTableAbort(_) => {}
        SlaveRemotePayload::MasterGossip(gossip) => {
          self.gossip = Arc::new(gossip.gossip_data);
        }
        SlaveRemotePayload::TabletMessage(_, _) => {}
        SlaveRemotePayload::CoordMessage(_, _) => {}
      },
      SlaveMessage::PaxosMessage(_) => {}
    }
  }
}
