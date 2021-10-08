use crate::col_usage::{node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, lookup_pos, map_insert, merge_table_views, mk_qid, remove_item, Clock, CoordForwardOut,
  GossipData, IOTypes, NetworkOut, OrigP, RemoteLeaderChangedPLm, TMStatus, TabletForwardOut,
};
use crate::coord::CoordForwardMsg;
use crate::gr_query_es::{GRQueryAction, GRQueryES};
use crate::model::common::{
  iast, proc, CTQueryPath, ColName, ColType, Context, ContextRow, CoordGroupId, Gen, LeadershipId,
  NodeGroupId, PaxosGroupId, SlaveGroupId, TablePath, TableView, TabletGroupId, TabletKeyRange,
  TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message as msg;
use crate::model::message::{
  GeneralQuery, QueryError, SlaveExternalReq, SlaveMessage, SlaveRemotePayload,
};
use crate::ms_query_coord_es::{
  FullMSCoordES, MSCoordES, MSQueryCoordAction, QueryPlanningES, QueryPlanningS,
};
use crate::paxos::LeaderChanged;
use crate::query_converter::convert_to_msquery;
use crate::server::ServerContextBase;
use crate::server::{CommonQuery, SlaveServerContext};
use crate::sql_parser::convert_ast;
use crate::tablet::{GRQueryESWrapper, TransTableReadESWrapper};
use crate::trans_table_read_es::{
  TransExecutionS, TransTableAction, TransTableReadES, TransTableSource,
};
use serde::{Deserialize, Serialize};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  SlavePLm
// -----------------------------------------------------------------------------------------------

pub mod plm {
  use crate::common::GossipData;
  use crate::model::common::{
    proc, ColName, ColType, Gen, QueryId, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange,
    Timestamp,
  };
  use serde::{Deserialize, Serialize};

  // Gossip

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Gossip {
    pub ms_query: GossipData,
  }

  // CreateTable

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct CreateTablePrepared {
    pub query_id: QueryId,

    pub tablet_group_id: TabletGroupId,
    pub table_path: TablePath,
    pub gen: Gen,

    pub key_range: TabletKeyRange,
    pub key_cols: Vec<(ColName, ColType)>,
    pub val_cols: Vec<(ColName, ColType)>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct CreateTableCommitted {
    pub query_id: QueryId,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct CreateTableAborted {
    pub query_id: QueryId,
  }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlavePLm {
  CreateTablePrepared(plm::CreateTablePrepared),
  CreateTableCommitted(plm::CreateTableCommitted),
  CreateTableAborted(plm::CreateTableAborted),
  Gossip(plm::Gossip),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
}

// -----------------------------------------------------------------------------------------------
//  SlaveForwardMsg
// -----------------------------------------------------------------------------------------------

pub enum SlaveForwardMsg {
  SlaveBundle(Vec<SlavePLm>),
  SlaveExternalReq(msg::SlaveExternalReq),
  SlaveRemotePayload(msg::SlaveRemotePayload),
  GossipData(Arc<GossipData>),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
  LeaderChanged(LeaderChanged),
}

// -----------------------------------------------------------------------------------------------
//  Status
// -----------------------------------------------------------------------------------------------

/// This contains every Slave Status. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  create_table_ess: HashMap<QueryId, GRQueryESWrapper>,
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
  // IO Objects.
  pub rand: T::RngCoreT,
  pub clock: T::ClockT,
  pub network_output: T::NetworkOutT,
  pub tablet_forward_output: T::TabletForwardOutT,
  pub coord_forward_output: T::CoordForwardOutT,
  /// Maps integer values to Coords for the purpose of routing External requests.
  pub coord_positions: Vec<CoordGroupId>,

  /// Metadata
  pub this_slave_group_id: SlaveGroupId,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// LeaderMap
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  /// Paxos
  pub master_bundle: Vec<SlavePLm>,
}

impl<T: IOTypes> SlaveState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    tablet_forward_output: T::TabletForwardOutT,
    coord_forward_output: T::CoordForwardOutT,
    gossip: Arc<GossipData>,
    this_slave_group_id: SlaveGroupId,
  ) -> SlaveState<T> {
    SlaveState {
      slave_context: SlaveContext {
        rand,
        clock,
        network_output,
        tablet_forward_output,
        coord_forward_output,
        coord_positions: vec![],
        this_slave_group_id,
        gossip,
        leader_map: Default::default(),
        master_bundle: vec![],
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::SlaveMessage) {
    self.slave_context.handle_incoming_message(&mut self.statuses, message);
  }
}

impl<T: IOTypes> SlaveContext<T> {
  /// Handles all messages, coming from Tablets, the Slave, External, etc.
  pub fn handle_incoming_message(&mut self, _: &mut Statuses, message: msg::SlaveMessage) {
    match message {
      SlaveMessage::ExternalMessage(_) => {}
      SlaveMessage::RemoteMessage(remote_message) => match remote_message.payload {
        SlaveRemotePayload::RemoteLeaderChanged(_) => {}
        SlaveRemotePayload::CreateTablePrepare(_) => {}
        SlaveRemotePayload::CreateTableCommit(_) => {}
        SlaveRemotePayload::CreateTableAbort(_) => {}
        SlaveRemotePayload::MasterGossip(_) => {}
        SlaveRemotePayload::TabletMessage(_, _) => {}
        SlaveRemotePayload::CoordMessage(_, _) => {}
      },
      SlaveMessage::PaxosMessage(_) => {}
    }
  }

  /// Handles inputs the Slave Backend.
  fn handle_input(&mut self, statuses: &mut Statuses, slave_input: SlaveForwardMsg) {
    match slave_input {
      SlaveForwardMsg::SlaveBundle(_) => {}
      SlaveForwardMsg::SlaveExternalReq(request) => {
        // Compute the hash of the request Id.
        let request_id = match &request {
          SlaveExternalReq::PerformExternalQuery(perform) => perform.request_id.clone(),
          SlaveExternalReq::CancelExternalQuery(cancel) => cancel.request_id.clone(),
        };
        let mut hasher = DefaultHasher::new();
        request_id.hash(&mut hasher);
        let hash_value = hasher.finish();

        // Route the request to a Coord determined by the hash.
        let coord_index = hash_value % self.coord_positions.len() as u64;
        let cid = self.coord_positions.get(coord_index as usize).unwrap();
        self.coord_forward_output.forward(cid, CoordForwardMsg::ExternalMessage(request));
      }
      SlaveForwardMsg::SlaveRemotePayload(_) => {}
      SlaveForwardMsg::GossipData(_) => {}
      SlaveForwardMsg::RemoteLeaderChanged(_) => {}
      SlaveForwardMsg::LeaderChanged(_) => {}
    }
  }
}
