use crate::col_usage::{node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, lookup_pos, map_insert, merge_table_views, mk_qid, remove_item, Clock, GossipData,
  IOTypes, NetworkOut, OrigP, TMStatus, TabletForwardOut,
};
use crate::gr_query_es::{GRQueryAction, GRQueryES};
use crate::model::common::{
  iast, proc, ColName, ColType, Context, ContextRow, Gen, NodeGroupId, NodePath, QueryPath,
  SlaveGroupId, TablePath, TableView, TabletGroupId, TabletKeyRange, TierMap, Timestamp,
  TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message as msg;
use crate::model::message::{GeneralQuery, QueryError, SlaveMessage};
use crate::ms_query_coord_es::{
  FullMSCoordES, MSCoordES, MSQueryCoordAction, MSQueryCoordReplanningES, MSQueryCoordReplanningS,
};
use crate::query_converter::convert_to_msquery;
use crate::server::{CommonQuery, ServerContext};
use crate::sql_parser::convert_ast;
use crate::tablet::{GRQueryESWrapper, TransTableReadESWrapper};
use crate::trans_table_read_es::{
  FullTransTableReadES, TransExecutionS, TransTableAction, TransTableSource,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  Slave Statuses
// -----------------------------------------------------------------------------------------------

/// A wrapper around MSCoordES that keeps track of the child queries it created. We
/// we use this for resource management
#[derive(Debug)]
struct MSCoordESWrapper {
  request_id: RequestId,
  sender_eid: EndpointId,
  child_queries: Vec<QueryId>,
  es: FullMSCoordES,
}

/// This contains every TabletStatus. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  ms_coord_ess: HashMap<QueryId, MSCoordESWrapper>,
  gr_query_ess: HashMap<QueryId, GRQueryESWrapper>,
  full_trans_table_read_ess: HashMap<QueryId, TransTableReadESWrapper>,
  tm_statuss: HashMap<QueryId, TMStatus>,
}

// -----------------------------------------------------------------------------------------------
//  Slave State
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct SlaveState<T: IOTypes> {
  slave_context: SlaveContext<T>,
  statuses: Statuses,
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
        external_request_id_map: Default::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::SlaveMessage) {
    // self.slave_context.handle_incoming_message(&mut self.statuses, message);
  }
}

impl<T: IOTypes> SlaveContext<T> {
  pub fn ctx(&mut self) -> ServerContext<T> {
    ServerContext {
      rand: &mut self.rand,
      clock: &mut self.clock,
      network_output: &mut self.network_output,
      this_slave_group_id: &self.this_slave_group_id,
      maybe_this_tablet_group_id: None,
      master_eid: &self.master_eid,
      gossip: &mut self.gossip,
    }
  }

  /// Handles all messages, coming from Tablets, the Master, External, etc.
  pub fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::SlaveMessage) {
    match message {
      SlaveMessage::ExternalMessage(_) => {}
      SlaveMessage::RemoteMessage(_) => {}
      SlaveMessage::PaxosMessage(_) => {}
    }
  }
}
