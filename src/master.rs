use crate::alter_table_tm_es::{
  AlterTableTMAction, AlterTableTMES, AlterTableTMS, Follower as AlterFollower,
};
use crate::col_usage::{
  collect_table_paths, compute_all_tier_maps, compute_query_plan_data, iterate_stage_ms_query,
  ColUsagePlanner, FrozenColUsageNode, GeneralStage,
};
use crate::common::{
  btree_map_insert, lookup, mk_qid, mk_sid, mk_tid, GossipData, IOTypes, NetworkOut, TableSchema,
  UUID,
};
use crate::common::{map_insert, RemoteLeaderChangedPLm};
use crate::create_table_tm_es::{
  CreateTableTMAction, CreateTableTMES, CreateTableTMS, Follower as CreateFollower, ResponseData,
};
use crate::drop_table_tm_es::{
  DropTableTMAction, DropTableTMES, DropTableTMS, Follower as DropFollower,
};
use crate::master_query_planning_es::{
  master_query_planning, master_query_planning_post, MasterQueryPlanningAction,
  MasterQueryPlanningES,
};
use crate::model::common::proc::{AlterTable, TableRef};
use crate::model::common::{
  proc, CQueryPath, ColName, EndpointId, Gen, LeadershipId, PaxosGroupId, QueryId, RequestId,
  SlaveGroupId, SlaveQueryPath, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
  TransTableName,
};
use crate::model::message as msg;
use crate::model::message::{
  ExternalDDLQueryAbortData, MasterExternalReq, MasterMessage, MasterRemotePayload,
};
use crate::multiversion_map::MVM;
use crate::network_driver::{NetworkDriver, NetworkDriverContext};
use crate::paxos::{PaxosContextBase, PaxosDriver};
use crate::server::{
  weak_contains_col, weak_contains_col_latest, CommonQuery, MasterServerContext, ServerContextBase,
};
use crate::sql_parser::{convert_ddl_ast, DDLQuery};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use sqlparser::test_utils::table;
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  MasterPLm
// -----------------------------------------------------------------------------------------------

pub mod plm {
  use crate::model::common::{
    proc, ColName, ColType, QueryId, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange,
    Timestamp,
  };
  use serde::{Deserialize, Serialize};

  // MasterQueryPlanning

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct MasterQueryPlanning {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
    pub ms_query: proc::MSQuery,
  }

  // CreateTable

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct CreateTableTMPrepared {
    pub query_id: QueryId,
    pub table_path: TablePath,

    pub key_cols: Vec<(ColName, ColType)>,
    pub val_cols: Vec<(ColName, ColType)>,
    pub shards: Vec<(TabletKeyRange, TabletGroupId, SlaveGroupId)>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct CreateTableTMCommitted {
    pub query_id: QueryId,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct CreateTableTMAborted {
    pub query_id: QueryId,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct CreateTableTMClosed {
    pub query_id: QueryId,
    pub timestamp_hint: Option<Timestamp>,
  }

  // AlterTable

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableTMPrepared {
    pub query_id: QueryId,
    pub table_path: TablePath,
    pub alter_op: proc::AlterOp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableTMCommitted {
    pub query_id: QueryId,
    pub timestamp_hint: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableTMAborted {
    pub query_id: QueryId,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableTMClosed {
    pub query_id: QueryId,
  }

  // DropTable

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct DropTableTMPrepared {
    pub query_id: QueryId,
    pub table_path: TablePath,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct DropTableTMCommitted {
    pub query_id: QueryId,
    pub timestamp_hint: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct DropTableTMAborted {
    pub query_id: QueryId,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct DropTableTMClosed {
    pub query_id: QueryId,
  }
}

// -----------------------------------------------------------------------------------------------
//  MasterBundle
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct MasterBundle {
  remote_leader_changes: Vec<RemoteLeaderChangedPLm>,
  pub plms: Vec<MasterPLm>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MasterPLm {
  MasterQueryPlanning(plm::MasterQueryPlanning),
  CreateTableTMPrepared(plm::CreateTableTMPrepared),
  CreateTableTMCommitted(plm::CreateTableTMCommitted),
  CreateTableTMAborted(plm::CreateTableTMAborted),
  CreateTableTMClosed(plm::CreateTableTMClosed),
  AlterTableTMPrepared(plm::AlterTableTMPrepared),
  AlterTableTMCommitted(plm::AlterTableTMCommitted),
  AlterTableTMAborted(plm::AlterTableTMAborted),
  AlterTableTMClosed(plm::AlterTableTMClosed),
  DropTableTMPrepared(plm::DropTableTMPrepared),
  DropTableTMCommitted(plm::DropTableTMCommitted),
  DropTableTMAborted(plm::DropTableTMAborted),
  DropTableTMClosed(plm::DropTableTMClosed),
}

// -----------------------------------------------------------------------------------------------
//  MasterPaxosContext
// -----------------------------------------------------------------------------------------------

pub struct MasterPaxosContext<'a, T: IOTypes> {
  /// IO Objects.
  pub network_output: &'a mut T::NetworkOutT,
  pub this_eid: &'a EndpointId,
}

impl<'a, T: IOTypes> PaxosContextBase<T, MasterBundle> for MasterPaxosContext<'a, T> {
  fn network_output(&mut self) -> &mut T::NetworkOutT {
    self.network_output
  }

  fn this_eid(&self) -> &EndpointId {
    self.this_eid
  }

  fn send(&mut self, eid: &EndpointId, message: msg::PaxosDriverMessage<MasterBundle>) {
    self
      .network_output
      .send(eid, msg::NetworkMessage::Master(msg::MasterMessage::PaxosDriverMessage(message)));
  }
}

// -----------------------------------------------------------------------------------------------
//  MasterForwardMsg
// -----------------------------------------------------------------------------------------------

pub enum MasterForwardMsg {
  MasterBundle(Vec<MasterPLm>),
  MasterExternalReq(msg::MasterExternalReq),
  MasterRemotePayload(msg::MasterRemotePayload),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
  LeaderChanged(msg::LeaderChanged),
  MasterTimerInput(MasterTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  FullMasterInput
// -----------------------------------------------------------------------------------------------

/// Messages deferred by the Slave to be run on the Slave.
pub enum MasterTimerInput {
  RetryInsert(UUID),
}

pub enum FullMasterInput {
  MasterMessage(msg::MasterMessage),
  MasterTimerInput(MasterTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  Master State
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct Statuses {
  create_table_tm_ess: BTreeMap<QueryId, CreateTableTMES>,
  alter_table_tm_ess: BTreeMap<QueryId, AlterTableTMES>,
  drop_table_tm_ess: BTreeMap<QueryId, DropTableTMES>,
  planning_ess: BTreeMap<QueryId, MasterQueryPlanningES>,
}

#[derive(Debug)]
pub struct MasterState<T: IOTypes> {
  context: MasterContext<T>,
  statuses: Statuses,
}

#[derive(Debug)]
pub struct MasterContext<T: IOTypes> {
  // IO Objects.
  pub rand: T::RngCoreT,
  pub clock: T::ClockT,
  pub network_output: T::NetworkOutT,

  /// Metadata
  pub this_eid: EndpointId,

  // Database Schema
  pub gen: Gen,
  pub db_schema: HashMap<(TablePath, Gen), TableSchema>,
  pub table_generation: MVM<TablePath, Gen>,

  // Distribution
  pub sharding_config: HashMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,

  /// LeaderMap
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  /// NetworkDriver
  pub network_driver: NetworkDriver<msg::MasterRemotePayload>,

  /// Request Management
  pub external_request_id_map: HashMap<RequestId, QueryId>,

  // Paxos
  pub master_bundle: MasterBundle,
  pub paxos_driver: PaxosDriver<MasterBundle>,
}

impl<T: IOTypes> MasterState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    db_schema: HashMap<(TablePath, Gen), TableSchema>,
    sharding_config: HashMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,
  ) -> MasterState<T> {
    let mut leader_map = HashMap::<PaxosGroupId, LeadershipId>::new();
    for (sid, eids) in &slave_address_config {
      leader_map.insert(sid.to_gid(), LeadershipId { gen: Gen(0), eid: eids[0].clone() });
    }
    let all_gids = leader_map.keys().cloned().collect();
    MasterState {
      context: MasterContext {
        rand,
        clock,
        network_output,
        this_eid: EndpointId("".to_string()),
        gen: Gen(0),
        db_schema,
        table_generation: MVM::new(),
        sharding_config,
        tablet_address_config,
        slave_address_config,
        leader_map,
        network_driver: NetworkDriver::new(all_gids),
        external_request_id_map: Default::default(),
        master_bundle: MasterBundle::default(),
        paxos_driver: PaxosDriver::new(),
      },
      statuses: Default::default(),
    }
  }

  // TODO: stop using this in favor of `handle_full_input`
  pub fn handle_incoming_message(&mut self, message: msg::MasterMessage) {
    self.context.handle_incoming_message(&mut self.statuses, message);
  }

  pub fn handle_full_input(&mut self, input: FullMasterInput) {
    match input {
      FullMasterInput::MasterMessage(message) => {
        self.context.handle_incoming_message(&mut self.statuses, message);
      }
      FullMasterInput::MasterTimerInput(timer_input) => {
        let forward_msg = MasterForwardMsg::MasterTimerInput(timer_input);
        self.context.handle_input(&mut self.statuses, forward_msg);
      }
    }
  }
}

impl<T: IOTypes> MasterContext<T> {
  pub fn ctx(&mut self) -> MasterServerContext<T> {
    MasterServerContext {
      network_output: &mut self.network_output,
      leader_map: &mut self.leader_map,
    }
  }

  pub fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::MasterMessage) {
    match message {
      MasterMessage::MasterExternalReq(request) => {
        if self.is_leader() {
          self.handle_input(statuses, MasterForwardMsg::MasterExternalReq(request))
        }
      }
      MasterMessage::RemoteMessage(remote_message) => {
        if self.is_leader() {
          // Pass the message through the NetworkDriver
          let maybe_delivered = self.network_driver.receive(
            NetworkDriverContext {
              this_gid: &PaxosGroupId::Master,
              this_eid: &self.this_eid,
              leader_map: &self.leader_map,
              remote_leader_changes: &mut self.master_bundle.remote_leader_changes,
            },
            remote_message,
          );
          if let Some(payload) = maybe_delivered {
            // Deliver if the message passed through
            self.handle_input(statuses, MasterForwardMsg::MasterRemotePayload(payload));
          }
        }
      }
      MasterMessage::PaxosDriverMessage(paxos_message) => {
        let bundles = self.paxos_driver.handle_paxos_message::<T>(
          &mut MasterPaxosContext {
            network_output: &mut self.network_output,
            this_eid: &self.this_eid,
          },
          paxos_message,
        );
        for shared_bundle in bundles {
          match shared_bundle {
            msg::PLEntry::Bundle(master_bundle) => {
              // Dispatch RemoteLeaderChanges
              for remote_change in master_bundle.remote_leader_changes.clone() {
                let forward_msg = MasterForwardMsg::RemoteLeaderChanged(remote_change.clone());
                self.handle_input(statuses, forward_msg);
              }

              // Dispatch the PaxosBundles
              self.handle_input(statuses, MasterForwardMsg::MasterBundle(master_bundle.plms));

              // Dispatch any messages that were buffered in the NetworkDriver.
              // Note: we must do this after RemoteLeaderChanges. Also note that there will
              // be no payloads in the NetworkBuffer if this nodes is a Follower.
              for remote_change in master_bundle.remote_leader_changes {
                let payloads = self
                  .network_driver
                  .deliver_blocked_messages(remote_change.gid, remote_change.lid);
                for payload in payloads {
                  self.handle_input(statuses, MasterForwardMsg::MasterRemotePayload(payload));
                }
              }
            }
            msg::PLEntry::LeaderChanged(leader_changed) => {
              // Forward to Master Backend
              self.handle_input(statuses, MasterForwardMsg::LeaderChanged(leader_changed.clone()));
            }
          }
        }
      }
    }
  }

  pub fn handle_input(&mut self, statuses: &mut Statuses, master_input: MasterForwardMsg) {
    match master_input {
      MasterForwardMsg::MasterTimerInput(timer_input) => match timer_input {
        MasterTimerInput::RetryInsert(uuid) => {
          self.paxos_driver.retry_insert(uuid);
        }
      },
      MasterForwardMsg::MasterBundle(bundle) => {
        if self.is_leader() {
          for paxos_log_msg in bundle {
            match paxos_log_msg {
              MasterPLm::MasterQueryPlanning(planning_plm) => {
                let query_id = planning_plm.query_id.clone();
                let result = master_query_planning_post(self, planning_plm);
                if let Some(es) = statuses.planning_ess.remove(&query_id) {
                  // If the ES still exists, we respond.
                  self.ctx().send_to_c(
                    es.sender_path.node_path,
                    msg::CoordMessage::MasterQueryPlanningSuccess(
                      msg::MasterQueryPlanningSuccess {
                        return_qid: es.sender_path.query_id,
                        query_id: es.query_id,
                        result,
                      },
                    ),
                  );
                }
              }
              // CreateTable
              MasterPLm::CreateTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_prepared_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMCommitted(committed) => {
                let query_id = committed.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMClosed(closed) => {
                let query_id = closed.query_id.clone();
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, closed);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              // AlterTable
              MasterPLm::AlterTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_prepared_plm(self);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMCommitted(committed) => {
                let query_id = committed.query_id.clone();
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self, committed);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMClosed(closed) => {
                let query_id = closed.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              // DropTable
              MasterPLm::DropTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_prepared_plm(self);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMCommitted(committed) => {
                let query_id = committed.query_id.clone();
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self, committed);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMClosed(closed) => {
                let query_id = closed.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
            }
          }

          // Run the Main Loop
          self.run_main_loop(statuses);

          // Inform all DDL ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.create_table_tm_ess {
            es.start_inserting(self);
          }
          for (_, es) in &mut statuses.alter_table_tm_ess {
            es.start_inserting(self);
          }
          for (_, es) in &mut statuses.drop_table_tm_ess {
            es.start_inserting(self);
          }

          // For every MasterQueryPlanningES, we add a PLm
          for (_, es) in &mut statuses.planning_ess {
            self.master_bundle.plms.push(MasterPLm::MasterQueryPlanning(
              plm::MasterQueryPlanning {
                query_id: es.query_id.clone(),
                timestamp: es.timestamp.clone(),
                ms_query: es.ms_query.clone(),
              },
            ));
          }

          // Dispatch the Master for insertion and start a new one.
          let master_bundle = std::mem::replace(&mut self.master_bundle, MasterBundle::default());
          self.paxos_driver.insert_bundle::<T>(
            &mut MasterPaxosContext {
              network_output: &mut self.network_output,
              this_eid: &self.this_eid,
            },
            master_bundle,
          );
        } else {
          for paxos_log_msg in bundle {
            match paxos_log_msg {
              MasterPLm::MasterQueryPlanning(planning_plm) => {
                master_query_planning_post(self, planning_plm);
              }
              // CreateTable
              MasterPLm::CreateTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                btree_map_insert(
                  &mut statuses.create_table_tm_ess,
                  &query_id.clone(),
                  CreateTableTMES {
                    response_data: None,
                    query_id,
                    table_path: prepared.table_path,
                    key_cols: prepared.key_cols,
                    val_cols: prepared.val_cols,
                    shards: prepared.shards,
                    state: CreateTableTMS::Follower(CreateFollower::Preparing),
                  },
                );
              }
              MasterPLm::CreateTableTMCommitted(committed) => {
                let query_id = committed.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMClosed(closed) => {
                let query_id = closed.query_id.clone();
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, closed);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              // AlterTable
              MasterPLm::AlterTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                btree_map_insert(
                  &mut statuses.alter_table_tm_ess,
                  &query_id.clone(),
                  AlterTableTMES {
                    response_data: None,
                    query_id,
                    table_path: prepared.table_path,
                    alter_op: prepared.alter_op,
                    state: AlterTableTMS::Follower(AlterFollower::Preparing),
                  },
                );
              }
              MasterPLm::AlterTableTMCommitted(committed) => {
                let query_id = committed.query_id.clone();
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self, committed);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMClosed(closed) => {
                let query_id = closed.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              // DropTable
              MasterPLm::DropTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                btree_map_insert(
                  &mut statuses.drop_table_tm_ess,
                  &query_id.clone(),
                  DropTableTMES {
                    response_data: None,
                    query_id,
                    table_path: prepared.table_path,
                    state: DropTableTMS::Follower(DropFollower::Preparing),
                  },
                );
              }
              MasterPLm::DropTableTMCommitted(committed) => {
                let query_id = committed.query_id.clone();
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self, committed);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMClosed(closed) => {
                let query_id = closed.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
            }
          }
        }
      }
      MasterForwardMsg::MasterExternalReq(message) => {
        match message {
          MasterExternalReq::PerformExternalDDLQuery(external_query) => {
            match self.validate_ddl_query(&external_query) {
              Ok(ddl_query) => {
                let query_id = mk_qid(&mut self.rand);
                let sender_eid = external_query.sender_eid;
                let request_id = external_query.request_id;
                self.external_request_id_map.insert(request_id.clone(), query_id.clone());
                match ddl_query {
                  DDLQuery::Create(create_table) => {
                    // Choose a random SlaveGroupId to place the Tablet
                    let sids = Vec::from_iter(self.slave_address_config.keys().into_iter());
                    let idx = self.rand.next_u32() as usize % sids.len();
                    let sid = sids[idx].clone();

                    // Construct shards
                    let shards = vec![(
                      TabletKeyRange { start: None, end: None },
                      mk_tid(&mut self.rand),
                      sid,
                    )];

                    // Construct ES
                    btree_map_insert(
                      &mut statuses.create_table_tm_ess,
                      &query_id,
                      CreateTableTMES {
                        response_data: Some(ResponseData { request_id, sender_eid }),
                        query_id: query_id.clone(),
                        table_path: create_table.table_path,
                        key_cols: create_table.key_cols,
                        val_cols: create_table.val_cols,
                        shards,
                        state: CreateTableTMS::Start,
                      },
                    );
                  }
                  DDLQuery::Alter(alter_table) => {
                    // Construct ES
                    btree_map_insert(
                      &mut statuses.alter_table_tm_ess,
                      &query_id,
                      AlterTableTMES {
                        response_data: Some(ResponseData { request_id, sender_eid }),
                        query_id: query_id.clone(),
                        table_path: alter_table.table_path,
                        alter_op: alter_table.alter_op,
                        state: AlterTableTMS::Start,
                      },
                    );
                  }
                  DDLQuery::Drop(drop_table) => {
                    // Construct ES
                    btree_map_insert(
                      &mut statuses.drop_table_tm_ess,
                      &query_id,
                      DropTableTMES {
                        response_data: Some(ResponseData { request_id, sender_eid }),
                        query_id: query_id.clone(),
                        table_path: drop_table.table_path,
                        state: DropTableTMS::Start,
                      },
                    );
                  }
                }
              }
              Err(payload) => {
                // We return an error because the RequestId is not unique.
                self.network_output.send(
                  &external_query.sender_eid,
                  msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
                    msg::ExternalDDLQueryAborted { request_id: external_query.request_id, payload },
                  )),
                );
              }
            }
          }
          MasterExternalReq::CancelExternalDDLQuery(_) => {}
        }

        // Run Main Loop
        self.run_main_loop(statuses);
      }
      MasterForwardMsg::MasterRemotePayload(payload) => match payload {
        MasterRemotePayload::RemoteLeaderChanged(_) => {}
        MasterRemotePayload::PerformMasterQueryPlanning(query_planning) => {
          let action = master_query_planning(self, query_planning.clone());
          match action {
            MasterQueryPlanningAction::Wait => {
              btree_map_insert(
                &mut statuses.planning_ess,
                &query_planning.query_id.clone(),
                MasterQueryPlanningES {
                  sender_path: query_planning.sender_path,
                  query_id: query_planning.query_id,
                  timestamp: query_planning.timestamp,
                  ms_query: query_planning.ms_query,
                },
              );
            }
            MasterQueryPlanningAction::Respond(result) => {
              self.ctx().send_to_c(
                query_planning.sender_path.node_path,
                msg::CoordMessage::MasterQueryPlanningSuccess(msg::MasterQueryPlanningSuccess {
                  return_qid: query_planning.sender_path.query_id,
                  query_id: query_planning.query_id,
                  result,
                }),
              );
            }
          }
        }
        MasterRemotePayload::CancelMasterQueryPlanning(cancel_query_planning) => {
          statuses.planning_ess.remove(&cancel_query_planning.query_id);
        }
        // CreateTable
        MasterRemotePayload::CreateTablePrepared(prepared) => {
          let query_id = prepared.query_id.clone();
          let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_prepared(self, prepared);
          self.handle_create_table_es_action(statuses, query_id, action);
        }
        MasterRemotePayload::CreateTableAborted(aborted) => {
          let query_id = aborted.query_id.clone();
          let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_aborted(self);
          self.handle_create_table_es_action(statuses, query_id, action);
        }
        MasterRemotePayload::CreateTableCloseConfirm(closed) => {
          let query_id = closed.query_id.clone();
          let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_close_confirmed(self, closed);
          self.handle_create_table_es_action(statuses, query_id, action);
        }
        // AlterTable
        MasterRemotePayload::AlterTablePrepared(prepared) => {
          let query_id = prepared.query_id.clone();
          let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_prepared(self, prepared);
          self.handle_alter_table_es_action(statuses, query_id, action);
        }
        MasterRemotePayload::AlterTableAborted(aborted) => {
          let query_id = aborted.query_id.clone();
          let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_aborted(self);
          self.handle_alter_table_es_action(statuses, query_id, action);
        }
        MasterRemotePayload::AlterTableCloseConfirm(closed) => {
          let query_id = closed.query_id.clone();
          let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_close_confirmed(self, closed);
          self.handle_alter_table_es_action(statuses, query_id, action);
        }
        // Drop
        MasterRemotePayload::DropTablePrepared(prepared) => {
          let query_id = prepared.query_id.clone();
          let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_prepared(self, prepared);
          self.handle_drop_table_es_action(statuses, query_id, action);
        }
        MasterRemotePayload::DropTableAborted(aborted) => {
          let query_id = aborted.query_id.clone();
          let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_aborted(self);
          self.handle_drop_table_es_action(statuses, query_id, action);
        }
        MasterRemotePayload::DropTableCloseConfirm(closed) => {
          let query_id = closed.query_id.clone();
          let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.handle_close_confirmed(self, closed);
          self.handle_drop_table_es_action(statuses, query_id, action);
        }
        MasterRemotePayload::MasterGossipRequest(_) => {}
      },
      MasterForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();
        self.leader_map.insert(gid.clone(), lid.clone()); // Update the LeadershipId

        // Create
        let query_ids: Vec<QueryId> = statuses.create_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.remote_leader_changed(self, remote_leader_changed.clone());
          self.handle_create_table_es_action(statuses, query_id, action);
        }

        // AlterTable
        let query_ids: Vec<QueryId> = statuses.alter_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.remote_leader_changed(self, remote_leader_changed.clone());
          self.handle_alter_table_es_action(statuses, query_id, action);
        }

        // DropTable
        let query_ids: Vec<QueryId> = statuses.drop_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.remote_leader_changed(self, remote_leader_changed.clone());
          self.handle_drop_table_es_action(statuses, query_id, action);
        }

        // For MasterQueryPlanningESs, if the sending PaxosGroup's Leadership changed,
        // we ECU (no response).
        let query_ids: Vec<QueryId> = statuses.planning_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.planning_ess.get_mut(&query_id).unwrap();
          if es.sender_path.node_path.sid.to_gid() == gid {
            statuses.planning_ess.remove(&query_id);
          }
        }
      }
      MasterForwardMsg::LeaderChanged(leader_changed) => {
        self.leader_map.insert(PaxosGroupId::Master, leader_changed.lid); // Update the LeadershipId

        // CreateTable
        let query_ids: Vec<QueryId> = statuses.create_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self);
          self.handle_create_table_es_action(statuses, query_id, action);
        }

        // AlterTable
        let query_ids: Vec<QueryId> = statuses.alter_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self);
          self.handle_alter_table_es_action(statuses, query_id, action);
        }

        // DropTable
        let query_ids: Vec<QueryId> = statuses.drop_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self);
          self.handle_drop_table_es_action(statuses, query_id, action);
        }

        // Check if this node just lost Leadership
        if !self.is_leader() {
          // Wink away all MasterQueryPlanningESs.
          statuses.planning_ess.clear();

          // Clear MasterBundle
          self.master_bundle = MasterBundle::default();
        }
      }
    }
  }

  /// Validate the uniqueness of `RequestId`, parse the SQL, ensure the `TablePath` exists, and
  /// check that the column being modified is not a key column before returning the `AlterTable`.
  fn validate_ddl_query(
    &self,
    external_query: &msg::PerformExternalDDLQuery,
  ) -> Result<DDLQuery, msg::ExternalDDLQueryAbortData> {
    if self.external_request_id_map.contains_key(&external_query.request_id) {
      // Duplicate RequestId; respond with an abort.
      Err(msg::ExternalDDLQueryAbortData::NonUniqueRequestId)
    } else {
      // Parse the SQL
      match Parser::parse_sql(&GenericDialect {}, &external_query.query) {
        Ok(parsed_ast) => Ok(convert_ddl_ast(&parsed_ast)),
        Err(parse_error) => {
          // Extract error string
          Err(msg::ExternalDDLQueryAbortData::ParseError(match parse_error {
            TokenizerError(err_msg) => err_msg,
            ParserError(err_msg) => err_msg,
          }))
        }
      }
    }
  }

  /// Runs the `run_main_loop_once` until it finally results in no states changes.
  fn run_main_loop(&mut self, statuses: &mut Statuses) {
    while !self.run_main_loop_once(statuses) {}
  }

  /// Thus runs one iteration of the Main Loop, returning `false` exactly when nothing changes.
  fn run_main_loop_once(&mut self, statuses: &mut Statuses) -> bool {
    // First, we accumulate all TablePaths that currently have a a DDL Query running for them.
    let mut tables_being_modified = HashSet::<TablePath>::new();
    for (_, es) in &statuses.create_table_tm_ess {
      if let CreateTableTMS::Start = &es.state {
      } else {
        tables_being_modified.insert(es.table_path.clone());
      }
    }

    for (_, es) in &statuses.alter_table_tm_ess {
      if let AlterTableTMS::Start = &es.state {
      } else {
        tables_being_modified.insert(es.table_path.clone());
      }
    }

    for (_, es) in &statuses.drop_table_tm_ess {
      if let DropTableTMS::Start = &es.state {
      } else {
        tables_being_modified.insert(es.table_path.clone());
      }
    }

    // Move CreateTableESs forward for TablePaths not in `tables_being_modified`
    {
      let mut ess_to_remove = Vec::<QueryId>::new();
      for (_, es) in &mut statuses.create_table_tm_ess {
        if let CreateTableTMS::Start = &es.state {
          if !tables_being_modified.contains(&es.table_path) {
            // Check Table Validity
            if self.table_generation.get_last_version(&es.table_path).is_none() {
              // If the table does not exist, we move the ES to WaitingInsertTMPrepared.
              es.state = CreateTableTMS::WaitingInsertTMPrepared;
              tables_being_modified.insert(es.table_path.clone());
              continue;
            }

            // Otherwise, we abort the CreateTableES.
            ess_to_remove.push(es.query_id.clone())
          }
        }
      }
      for query_id in ess_to_remove {
        let es = statuses.create_table_tm_ess.remove(&query_id).unwrap();
        if let Some(response_data) = &es.response_data {
          response_invalid_ddl::<T>(&mut self.network_output, response_data);
        }
      }
    }

    // Move AlterTableESs forward for TablePaths not in `tables_being_modified`
    {
      let mut ess_to_remove = Vec::<QueryId>::new();
      for (_, es) in &mut statuses.alter_table_tm_ess {
        if let AlterTableTMS::Start = &es.state {
          if !tables_being_modified.contains(&es.table_path) {
            // Check Column Validity
            if let Some(gen) = self.table_generation.get_last_version(&es.table_path) {
              // The Table Exists.
              let schema = &self.db_schema.get(&(es.table_path.clone(), gen.clone())).unwrap();
              let contains_col = weak_contains_col_latest(schema, &es.alter_op.col_name);
              let add_col = es.alter_op.maybe_col_type.is_some();
              if contains_col && !add_col || !contains_col && add_col {
                // We have Column Validity, so we move it to WaitingInsertTMPrepared.
                es.state = AlterTableTMS::WaitingInsertTMPrepared;
                tables_being_modified.insert(es.table_path.clone());
                continue;
              }
            }

            // We do not have Column Validity, so we abort.
            ess_to_remove.push(es.query_id.clone())
          }
        }
      }
      for query_id in ess_to_remove {
        let es = statuses.alter_table_tm_ess.remove(&query_id).unwrap();
        if let Some(response_data) = &es.response_data {
          response_invalid_ddl::<T>(&mut self.network_output, response_data);
        }
      }
    }

    // Move DropTableESs forward for TablePaths not in `tables_being_modified`
    {
      let mut ess_to_remove = Vec::<QueryId>::new();
      for (_, es) in &mut statuses.drop_table_tm_ess {
        if let DropTableTMS::Start = &es.state {
          if !tables_being_modified.contains(&es.table_path) {
            // Check Table Validity
            if self.table_generation.get_last_version(&es.table_path).is_some() {
              // If the table exists, we move the ES to WaitingInsertTMPrepared.
              es.state = DropTableTMS::WaitingInsertTMPrepared;
              tables_being_modified.insert(es.table_path.clone());
              continue;
            }

            // Otherwise, we abort the DropTableES.
            ess_to_remove.push(es.query_id.clone())
          }
        }
      }
      for query_id in ess_to_remove {
        let es = statuses.drop_table_tm_ess.remove(&query_id).unwrap();
        if let Some(response_data) = &es.response_data {
          response_invalid_ddl::<T>(&mut self.network_output, response_data);
        }
      }
    }

    return false;
  }

  /// Handles the actions specified by a CreateTableES.
  fn handle_create_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: CreateTableTMAction,
  ) {
    match action {
      CreateTableTMAction::Wait => {}
      CreateTableTMAction::Exit => {
        statuses.create_table_tm_ess.remove(&query_id);
      }
    }
  }

  /// Handles the actions specified by a AlterTableES.
  fn handle_alter_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: AlterTableTMAction,
  ) {
    match action {
      AlterTableTMAction::Wait => {}
      AlterTableTMAction::Exit => {
        statuses.alter_table_tm_ess.remove(&query_id);
      }
    }
  }

  /// Handles the actions specified by a AlterTableES.
  fn handle_drop_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: DropTableTMAction,
  ) {
    match action {
      DropTableTMAction::Wait => {}
      DropTableTMAction::Exit => {
        statuses.drop_table_tm_ess.remove(&query_id);
      }
    }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&PaxosGroupId::Master).unwrap();
    lid.eid == self.this_eid
  }

  /// Broadcast GossipData
  pub fn broadcast_gossip(&mut self) {
    let gossip_data = GossipData {
      gen: self.gen.clone(),
      db_schema: self.db_schema.clone(),
      table_generation: self.table_generation.clone(),
      sharding_config: self.sharding_config.clone(),
      tablet_address_config: self.tablet_address_config.clone(),
      slave_address_config: self.slave_address_config.clone(),
    };
    let sids: Vec<SlaveGroupId> = self.slave_address_config.keys().cloned().collect();
    for sid in sids {
      self.ctx().send_to_slave_common(
        sid,
        msg::SlaveRemotePayload::MasterGossip(msg::MasterGossip {
          gossip_data: gossip_data.clone(),
        }),
      )
    }
  }
}

/// Send `InvalidDDLQuery` to the given `ResponseData`
fn response_invalid_ddl<T: IOTypes>(
  network_output: &mut T::NetworkOutT,
  response_data: &ResponseData,
) {
  network_output.send(
    &response_data.sender_eid,
    msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
      msg::ExternalDDLQueryAborted {
        request_id: response_data.request_id.clone(),
        payload: ExternalDDLQueryAbortData::InvalidDDLQuery,
      },
    )),
  )
}
