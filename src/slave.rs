use crate::alter_table_es::State;
use crate::col_usage::{node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, lookup_pos, map_insert, merge_table_views, mk_qid, remove_item, Clock, CoordForwardOut,
  GossipData, IOTypes, NetworkOut, OrigP, RemoteLeaderChangedPLm, TMStatus, TabletForwardOut,
};
use crate::coord::CoordForwardMsg;
use crate::create_table_es::{CreateTableAction, CreateTableES};
use crate::gr_query_es::{GRQueryAction, GRQueryES};
use crate::model::common::{
  iast, proc, CTQueryPath, ColName, ColType, Context, ContextRow, CoordGroupId, Gen, LeadershipId,
  NodeGroupId, PaxosGroupId, SlaveGroupId, TablePath, TableView, TabletGroupId, TabletKeyRange,
  TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message as msg;
use crate::ms_query_coord_es::{
  FullMSCoordES, MSCoordES, MSQueryCoordAction, QueryPlanningES, QueryPlanningS,
};
use crate::network_driver::{NetworkDriver, NetworkDriverContext};
use crate::paxos::LeaderChanged;
use crate::query_converter::convert_to_msquery;
use crate::server::{CommonQuery, SlaveServerContext};
use crate::server::{MainSlaveServerContext, ServerContextBase};
use crate::sql_parser::convert_ast;
use crate::sql_parser::DDLQuery::Create;
use crate::tablet::{GRQueryESWrapper, TabletForwardMsg, TransTableReadESWrapper};
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

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct SlaveBundle {
  remote_leader_changes: Vec<RemoteLeaderChangedPLm>,
  gossip_data: Option<GossipData>,
  pub plms: Vec<SlavePLm>,
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
  create_table_ess: HashMap<QueryId, CreateTableES>,
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
  pub this_gid: PaxosGroupId, // self.this_slave_group_id.to_gid()
  pub this_eid: EndpointId,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// LeaderMap
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  /// NetworkDriver
  pub network_driver: NetworkDriver<msg::SlaveRemotePayload>,

  /// Paxos
  pub slave_bundle: SlaveBundle,
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
    let this_gid = this_slave_group_id.to_gid();
    let mut leader_map = HashMap::<PaxosGroupId, LeadershipId>::new();
    for (sid, eids) in &gossip.slave_address_config {
      leader_map.insert(sid.to_gid(), LeadershipId { gen: Gen(0), eid: eids[0].clone() });
    }
    let all_gids = leader_map.keys().cloned().collect();
    SlaveState {
      slave_context: SlaveContext {
        rand,
        clock,
        network_output,
        tablet_forward_output,
        coord_forward_output,
        coord_positions: vec![],
        this_slave_group_id,
        this_gid,
        this_eid: EndpointId("".to_string()),
        gossip,
        leader_map,
        network_driver: NetworkDriver::new(all_gids),
        slave_bundle: SlaveBundle::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::SlaveMessage) {
    self.slave_context.handle_incoming_message(&mut self.statuses, message);
  }
}

impl<T: IOTypes> SlaveContext<T> {
  pub fn ctx(&mut self) -> MainSlaveServerContext<T> {
    MainSlaveServerContext {
      network_output: &mut self.network_output,
      this_slave_group_id: &self.this_slave_group_id,
      leader_map: &self.leader_map,
    }
  }

  /// Handles all messages, coming from Tablets, the Slave, External, etc.
  pub fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::SlaveMessage) {
    match message {
      msg::SlaveMessage::ExternalMessage(request) => {
        if self.is_leader() {
          self.handle_input(statuses, SlaveForwardMsg::SlaveExternalReq(request))
        }
      }
      msg::SlaveMessage::RemoteMessage(remote_message) => {
        if self.is_leader() {
          // Pass the message through the NetworkDriver
          let maybe_delivered = self.network_driver.receive(
            NetworkDriverContext {
              this_gid: &self.this_gid,
              this_eid: &self.this_eid,
              leader_map: &self.leader_map,
              remote_leader_changes: &mut self.slave_bundle.remote_leader_changes,
            },
            remote_message,
          );
          if let Some(payload) = maybe_delivered {
            // Deliver if the message passed through
            self.handle_input(statuses, SlaveForwardMsg::SlaveRemotePayload(payload));
          }
        }
      }
      msg::SlaveMessage::PaxosMessage(_) => {}
    }
  }

  /// Handles inputs the Slave Backend.
  fn handle_input(&mut self, statuses: &mut Statuses, slave_input: SlaveForwardMsg) {
    match slave_input {
      SlaveForwardMsg::SlaveBundle(bundle) => {
        if self.is_leader() {
          for paxos_log_msg in bundle {
            match paxos_log_msg {
              SlavePLm::CreateTablePrepared(prepared) => {
                let query_id = prepared.query_id;
                let es = statuses.create_table_ess.get_mut(&query_id).unwrap();
                let action = es.handle_prepared_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              SlavePLm::CreateTableCommitted(committed) => {
                let query_id = committed.query_id;
                let es = statuses.create_table_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              SlavePLm::CreateTableAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.create_table_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              SlavePLm::Gossip(_) => {}
              SlavePLm::RemoteLeaderChanged(_) => {}
            }
          }

          // Inform all ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.create_table_ess {
            es.start_inserting(self);
          }

          // TODO: dispatch bundle for insertion
        } else {
          for paxos_log_msg in bundle {
            match paxos_log_msg {
              SlavePLm::CreateTablePrepared(prepared) => {
                let query_id = prepared.query_id;
                map_insert(
                  &mut statuses.create_table_ess,
                  &query_id.clone(),
                  CreateTableES {
                    query_id,
                    tablet_group_id: prepared.tablet_group_id,
                    table_path: prepared.table_path,
                    gen: prepared.gen,
                    key_range: prepared.key_range,
                    key_cols: prepared.key_cols,
                    val_cols: prepared.val_cols,
                    state: State::Follower,
                  },
                );
              }
              SlavePLm::CreateTableCommitted(committed) => {
                let query_id = committed.query_id;
                let es = statuses.create_table_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              SlavePLm::CreateTableAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.create_table_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              SlavePLm::Gossip(_) => {}
              SlavePLm::RemoteLeaderChanged(_) => {}
            }
          }
        }
      }
      SlaveForwardMsg::SlaveExternalReq(request) => {
        // Compute the hash of the request Id.
        let request_id = match &request {
          msg::SlaveExternalReq::PerformExternalQuery(perform) => perform.request_id.clone(),
          msg::SlaveExternalReq::CancelExternalQuery(cancel) => cancel.request_id.clone(),
        };
        let mut hasher = DefaultHasher::new();
        request_id.hash(&mut hasher);
        let hash_value = hasher.finish();

        // Route the request to a Coord determined by the hash.
        let coord_index = hash_value % self.coord_positions.len() as u64;
        let cid = self.coord_positions.get(coord_index as usize).unwrap();
        self.coord_forward_output.forward(cid, CoordForwardMsg::ExternalMessage(request));
      }
      SlaveForwardMsg::SlaveRemotePayload(payload) => match payload {
        msg::SlaveRemotePayload::RemoteLeaderChanged(_) => {}
        msg::SlaveRemotePayload::CreateTablePrepare(prepare) => {
          let query_id = prepare.query_id;
          if let Some(es) = statuses.create_table_ess.get_mut(&query_id) {
            let action = es.handle_prepare(self);
            self.handle_create_table_es_action(statuses, query_id, action);
          } else {
            map_insert(
              &mut statuses.create_table_ess,
              &query_id.clone(),
              CreateTableES {
                query_id,
                tablet_group_id: prepare.tablet_group_id,
                table_path: prepare.table_path,
                gen: prepare.gen,
                key_range: prepare.key_range,
                key_cols: prepare.key_cols,
                val_cols: prepare.val_cols,
                state: State::WaitingInsertingPrepared,
              },
            );
          }
        }
        msg::SlaveRemotePayload::CreateTableCommit(commit) => {
          let query_id = commit.query_id;
          if let Some(es) = statuses.create_table_ess.get_mut(&query_id) {
            let action = es.handle_commit(self);
            self.handle_create_table_es_action(statuses, query_id, action);
          } else {
            // Send back a `CloseConfirm` to the Master.
            let sid = self.this_slave_group_id.clone();
            self.ctx().send_to_master(msg::MasterRemotePayload::CreateTableCloseConfirm(
              msg::CreateTableCloseConfirm { query_id, sid },
            ));
          }
        }
        msg::SlaveRemotePayload::CreateTableAbort(abort) => {
          let query_id = abort.query_id;
          if let Some(es) = statuses.create_table_ess.get_mut(&query_id) {
            let action = es.handle_abort(self);
            self.handle_create_table_es_action(statuses, query_id, action);
          } else {
            // Send back a `CloseConfirm` to the Master.
            let sid = self.this_slave_group_id.clone();
            self.ctx().send_to_master(msg::MasterRemotePayload::CreateTableCloseConfirm(
              msg::CreateTableCloseConfirm { query_id, sid },
            ));
          }
        }
        msg::SlaveRemotePayload::MasterGossip(master_gossip) => {
          let incoming_gossip = master_gossip.gossip_data;
          if self.gossip.gen < incoming_gossip.gen {
            if let Some(cur_next_gossip) = &self.slave_bundle.gossip_data {
              // Check if there is an existing GossipData about to be inserted.
              if cur_next_gossip.gen < incoming_gossip.gen {
                self.slave_bundle.gossip_data = Some(incoming_gossip);
              }
            } else {
              self.slave_bundle.gossip_data = Some(incoming_gossip);
            }
          }
        }
        msg::SlaveRemotePayload::TabletMessage(tid, tablet_msg) => {
          self.tablet_forward_output.forward(&tid, TabletForwardMsg::TabletMessage(tablet_msg))
        }
        msg::SlaveRemotePayload::CoordMessage(cid, coord_msg) => {
          self.coord_forward_output.forward(&cid, CoordForwardMsg::CoordMessage(coord_msg))
        }
      },
      SlaveForwardMsg::GossipData(gossip) => {
        self.gossip = gossip;
      }
      SlaveForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();
        self.leader_map.insert(gid.clone(), lid.clone()); // Update the LeadershipId
      }
      SlaveForwardMsg::LeaderChanged(leader_changed) => {
        let this_gid = self.this_slave_group_id.to_gid();
        self.leader_map.insert(this_gid, leader_changed.lid); // Update the LeadershipId

        // Inform CreateQueryES
        let query_ids: Vec<QueryId> = statuses.create_table_ess.keys().cloned().collect();
        for query_id in query_ids {
          let create_query_es = statuses.create_table_ess.get_mut(&query_id).unwrap();
          let action = create_query_es.leader_changed(self);
          self.handle_create_table_es_action(statuses, query_id.clone(), action);
        }

        if !self.is_leader() {
          // Clear SlaveBundle
          self.slave_bundle = SlaveBundle::default();
        }
      }
    }
  }

  /// Handles the actions produced by a CreateTableES.
  fn handle_create_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: CreateTableAction,
  ) {
    match action {
      CreateTableAction::Wait => {}
      CreateTableAction::Exit => {
        statuses.create_table_ess.remove(&query_id);
      }
    }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&self.this_slave_group_id.to_gid()).unwrap();
    lid.eid == self.this_eid
  }
}
