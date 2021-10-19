use crate::alter_table_tm_es::{AlterTablePayloadTypes, AlterTableTMInner};
use crate::common::RemoteLeaderChangedPLm;
use crate::common::{
  btree_map_insert, lookup, mk_qid, mk_sid, mk_tid, GossipData, MasterIOCtx, TableSchema, UUID,
};
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
use crate::paxos::{PaxosContextBase, PaxosDriver, PaxosTimerEvent};
use crate::server::{
  weak_contains_col, weak_contains_col_latest, CommonQuery, MasterServerContext, ServerContextBase,
};
use crate::sql_parser::{convert_ddl_ast, DDLQuery};
use crate::stmpaxos2pc_tm as paxos2pc;
use crate::stmpaxos2pc_tm::{STMPaxos2PCTMAction, State};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::FromIterator;

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
  AlterTableTMPrepared2(paxos2pc::TMPreparedPLm<AlterTablePayloadTypes>),
  AlterTableTMCommitted2(paxos2pc::TMCommittedPLm<AlterTablePayloadTypes>),
  AlterTableTMAborted2(paxos2pc::TMAbortedPLm<AlterTablePayloadTypes>),
  AlterTableTMClosed2(paxos2pc::TMClosedPLm<AlterTablePayloadTypes>),
  DropTableTMPrepared(plm::DropTableTMPrepared),
  DropTableTMCommitted(plm::DropTableTMCommitted),
  DropTableTMAborted(plm::DropTableTMAborted),
  DropTableTMClosed(plm::DropTableTMClosed),
}

// -----------------------------------------------------------------------------------------------
//  MasterPaxosContext
// -----------------------------------------------------------------------------------------------

pub struct MasterPaxosContext<'a, IO: MasterIOCtx> {
  /// IO
  pub io_ctx: &'a mut IO,

  // Metadata
  pub this_eid: &'a EndpointId,
}

impl<'a, IO: MasterIOCtx> PaxosContextBase<MasterBundle> for MasterPaxosContext<'a, IO> {
  type RngCoreT = IO::RngCoreT;

  fn rand(&mut self) -> &mut Self::RngCoreT {
    self.io_ctx.rand()
  }

  fn this_eid(&self) -> &EndpointId {
    self.this_eid
  }

  fn send(&mut self, eid: &EndpointId, message: msg::PaxosDriverMessage<MasterBundle>) {
    self
      .io_ctx
      .send(eid, msg::NetworkMessage::Master(msg::MasterMessage::PaxosDriverMessage(message)));
  }

  fn defer(&mut self, defer_time: u128, timer_event: PaxosTimerEvent) {
    self.io_ctx.defer(defer_time, MasterTimerInput::PaxosTimerEvent(timer_event));
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
#[derive(Debug)]
pub enum MasterTimerInput {
  PaxosTimerEvent(PaxosTimerEvent),
}

pub enum FullMasterInput {
  MasterMessage(msg::MasterMessage),
  MasterTimerInput(MasterTimerInput),
}

// -----------------------------------------------------------------------------------------------
//  Master State
// -----------------------------------------------------------------------------------------------

type AlterTableTMES = paxos2pc::STMPaxos2PCTMOuter<AlterTablePayloadTypes, AlterTableTMInner>;

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
pub struct MasterState {
  context: MasterContext,
  statuses: Statuses,
}

#[derive(Debug)]
pub struct MasterContext {
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
  pub master_address_config: Vec<EndpointId>,

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

impl MasterState {
  pub fn new(context: MasterContext) -> MasterState {
    MasterState { context, statuses: Default::default() }
  }

  pub fn handle_input<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO, input: FullMasterInput) {
    match input {
      FullMasterInput::MasterMessage(message) => {
        self.context.handle_incoming_message(io_ctx, &mut self.statuses, message);
      }
      FullMasterInput::MasterTimerInput(timer_input) => {
        let forward_msg = MasterForwardMsg::MasterTimerInput(timer_input);
        self.context.handle_input(io_ctx, &mut self.statuses, forward_msg);
      }
    }
  }
}

impl MasterContext {
  pub fn new(
    this_eid: EndpointId,
    slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,
    master_address_config: Vec<EndpointId>,
    leader_map: HashMap<PaxosGroupId, LeadershipId>,
  ) -> MasterContext {
    let all_gids = leader_map.keys().cloned().collect();
    MasterContext {
      this_eid,
      gen: Gen(0),
      db_schema: Default::default(),
      table_generation: MVM::new(),
      sharding_config: Default::default(),
      tablet_address_config: Default::default(),
      slave_address_config,
      master_address_config: master_address_config.clone(),
      leader_map,
      network_driver: NetworkDriver::new(all_gids),
      external_request_id_map: Default::default(),
      master_bundle: MasterBundle::default(),
      paxos_driver: PaxosDriver::new(master_address_config),
    }
  }

  pub fn ctx<'a, IO: MasterIOCtx>(&'a mut self, io_ctx: &'a mut IO) -> MasterServerContext<'a, IO> {
    MasterServerContext { io_ctx, leader_map: &mut self.leader_map }
  }

  pub fn handle_incoming_message<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    message: msg::MasterMessage,
  ) {
    match message {
      MasterMessage::MasterExternalReq(request) => {
        if self.is_leader() {
          self.handle_input(io_ctx, statuses, MasterForwardMsg::MasterExternalReq(request))
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
            self.handle_input(io_ctx, statuses, MasterForwardMsg::MasterRemotePayload(payload));
          }
        }
      }
      MasterMessage::PaxosDriverMessage(paxos_message) => {
        let bundles = self.paxos_driver.handle_paxos_message(
          &mut MasterPaxosContext { io_ctx, this_eid: &self.this_eid },
          paxos_message,
        );
        for shared_bundle in bundles {
          match shared_bundle {
            msg::PLEntry::Bundle(master_bundle) => {
              // Dispatch RemoteLeaderChanges
              for remote_change in master_bundle.remote_leader_changes.clone() {
                let forward_msg = MasterForwardMsg::RemoteLeaderChanged(remote_change.clone());
                self.handle_input(io_ctx, statuses, forward_msg);
              }

              // Dispatch the PaxosBundles
              self.handle_input(
                io_ctx,
                statuses,
                MasterForwardMsg::MasterBundle(master_bundle.plms),
              );

              // Dispatch any messages that were buffered in the NetworkDriver.
              // Note: we must do this after RemoteLeaderChanges. Also note that there will
              // be no payloads in the NetworkBuffer if this nodes is a Follower.
              for remote_change in master_bundle.remote_leader_changes {
                let payloads = self
                  .network_driver
                  .deliver_blocked_messages(remote_change.gid, remote_change.lid);
                for payload in payloads {
                  self.handle_input(
                    io_ctx,
                    statuses,
                    MasterForwardMsg::MasterRemotePayload(payload),
                  );
                }
              }
            }
            msg::PLEntry::LeaderChanged(leader_changed) => {
              // Forward to Master Backend
              self.handle_input(
                io_ctx,
                statuses,
                MasterForwardMsg::LeaderChanged(leader_changed.clone()),
              );
            }
          }
        }
      }
    }
  }

  pub fn handle_input<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    master_input: MasterForwardMsg,
  ) {
    match master_input {
      MasterForwardMsg::MasterTimerInput(timer_input) => match timer_input {
        MasterTimerInput::PaxosTimerEvent(timer_event) => {
          self
            .paxos_driver
            .timer_event(&mut MasterPaxosContext { io_ctx, this_eid: &self.this_eid }, timer_event);
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
                  self.ctx(io_ctx).send_to_c(
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
                let action = es.handle_prepared_plm(self, io_ctx);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMCommitted(committed) => {
                let query_id = committed.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self, io_ctx);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self, io_ctx);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMClosed(closed) => {
                let query_id = closed.query_id.clone();
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, io_ctx, closed);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              // AlterTable
              MasterPLm::AlterTableTMPrepared2(prepared) => {
                let query_id = prepared.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, io_ctx);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMCommitted2(committed) => {
                let query_id = committed.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_prepared_plm(self, io_ctx);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMAborted2(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self, io_ctx);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMClosed2(closed) => {
                let query_id = closed.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, io_ctx);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              // DropTable
              MasterPLm::DropTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_prepared_plm(self, io_ctx);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMCommitted(committed) => {
                let query_id = committed.query_id.clone();
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self, io_ctx, committed);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self, io_ctx);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMClosed(closed) => {
                let query_id = closed.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, io_ctx);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
            }
          }

          // Run the Main Loop
          self.run_main_loop(io_ctx, statuses);

          // Inform all DDL ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.create_table_tm_ess {
            es.start_inserting(self, io_ctx);
          }
          for (_, es) in &mut statuses.alter_table_tm_ess {
            es.start_inserting(self, io_ctx);
          }
          for (_, es) in &mut statuses.drop_table_tm_ess {
            es.start_inserting(self, io_ctx);
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
          self.paxos_driver.insert_bundle(
            &mut MasterPaxosContext { io_ctx, this_eid: &self.this_eid },
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
                let action = es.handle_committed_plm(self, io_ctx);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self, io_ctx);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              MasterPLm::CreateTableTMClosed(closed) => {
                let query_id = closed.query_id.clone();
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, io_ctx, closed);
                self.handle_create_table_es_action(statuses, query_id, action);
              }
              // AlterTable
              MasterPLm::AlterTableTMPrepared2(prepared) => {
                let query_id = prepared.query_id;
                let mut es = AlterTableTMES::new(
                  query_id.clone(),
                  AlterTableTMInner {
                    response_data: None,
                    query_id: query_id.clone(),
                    table_path: prepared.payload.table_path,
                    alter_op: prepared.payload.alter_op,
                  },
                );
                es.init_follower(self, io_ctx);
                btree_map_insert(&mut statuses.alter_table_tm_ess, &query_id.clone(), es);
              }
              MasterPLm::AlterTableTMCommitted2(committed) => {
                let query_id = committed.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_prepared_plm(self, io_ctx);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMAborted2(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self, io_ctx);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMClosed2(closed) => {
                let query_id = closed.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, io_ctx);
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
                let action = es.handle_committed_plm(self, io_ctx, committed);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_aborted_plm(self, io_ctx);
                self.handle_drop_table_es_action(statuses, query_id, action);
              }
              MasterPLm::DropTableTMClosed(closed) => {
                let query_id = closed.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_closed_plm(self, io_ctx);
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
                let query_id = mk_qid(io_ctx.rand());
                let sender_eid = external_query.sender_eid;
                let request_id = external_query.request_id;
                self.external_request_id_map.insert(request_id.clone(), query_id.clone());
                match ddl_query {
                  DDLQuery::Create(create_table) => {
                    // Choose a random SlaveGroupId to place the Tablet
                    let sids = Vec::from_iter(self.slave_address_config.keys().into_iter());
                    let idx = io_ctx.rand().next_u32() as usize % sids.len();
                    let sid = sids[idx].clone();

                    // Construct shards
                    let shards =
                      vec![(TabletKeyRange { start: None, end: None }, mk_tid(io_ctx.rand()), sid)];

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
                      AlterTableTMES::new(
                        query_id.clone(),
                        AlterTableTMInner {
                          response_data: Some(ResponseData { request_id, sender_eid }),
                          query_id: query_id.clone(),
                          table_path: alter_table.table_path,
                          alter_op: alter_table.alter_op,
                        },
                      ),
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
                io_ctx.send(
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
        self.run_main_loop(io_ctx, statuses);
      }
      MasterForwardMsg::MasterRemotePayload(payload) => {
        match payload {
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
                self.ctx(io_ctx).send_to_c(
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
            let action = es.handle_prepared(self, io_ctx, prepared);
            self.handle_create_table_es_action(statuses, query_id, action);
          }
          MasterRemotePayload::CreateTableAborted(aborted) => {
            let query_id = aborted.query_id.clone();
            let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.handle_aborted(self, io_ctx);
            self.handle_create_table_es_action(statuses, query_id, action);
          }
          MasterRemotePayload::CreateTableCloseConfirm(closed) => {
            let query_id = closed.query_id.clone();
            let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.handle_close_confirmed(self, io_ctx, closed);
            self.handle_create_table_es_action(statuses, query_id, action);
          }
          // Advanced
          MasterRemotePayload::AlterTablePrepared(prepared) => {
            let query_id = prepared.query_id.clone();
            let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.handle_prepared(self, io_ctx, prepared);
            self.handle_alter_table_es_action(statuses, query_id, action);
          }
          MasterRemotePayload::AlterTableAborted(aborted) => {
            let query_id = aborted.query_id.clone();
            let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.handle_aborted(self, io_ctx);
            self.handle_alter_table_es_action(statuses, query_id, action);
          }
          MasterRemotePayload::AlterTableClosed(closed) => {
            let query_id = closed.query_id.clone();
            let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.handle_close_confirmed(self, io_ctx, closed);
            self.handle_alter_table_es_action(statuses, query_id, action);
          }
          // Drop
          MasterRemotePayload::DropTablePrepared(prepared) => {
            let query_id = prepared.query_id.clone();
            let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.handle_prepared(self, io_ctx, prepared);
            self.handle_drop_table_es_action(statuses, query_id, action);
          }
          MasterRemotePayload::DropTableAborted(aborted) => {
            let query_id = aborted.query_id.clone();
            let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.handle_aborted(self, io_ctx);
            self.handle_drop_table_es_action(statuses, query_id, action);
          }
          MasterRemotePayload::DropTableCloseConfirm(closed) => {
            let query_id = closed.query_id.clone();
            let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
            let action = es.handle_close_confirmed(self, io_ctx, closed);
            self.handle_drop_table_es_action(statuses, query_id, action);
          }
          MasterRemotePayload::MasterGossipRequest(_) => {}
        }

        // Run Main Loop
        self.run_main_loop(io_ctx, statuses);
      }
      MasterForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();
        self.leader_map.insert(gid.clone(), lid.clone()); // Update the LeadershipId

        // Create
        let query_ids: Vec<QueryId> = statuses.create_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
          self.handle_create_table_es_action(statuses, query_id, action);
        }

        // AlterTable
        let query_ids: Vec<QueryId> = statuses.alter_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
          self.handle_alter_table_es_action(statuses, query_id, action);
        }

        // DropTable
        let query_ids: Vec<QueryId> = statuses.drop_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
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
          let action = es.leader_changed(self, io_ctx);
          self.handle_create_table_es_action(statuses, query_id, action);
        }

        // AlterTable
        let query_ids: Vec<QueryId> = statuses.alter_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self, io_ctx);
          self.handle_alter_table_es_action(statuses, query_id, action);
        }

        // DropTable
        let query_ids: Vec<QueryId> = statuses.drop_table_tm_ess.keys().cloned().collect();
        for query_id in query_ids {
          let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
          let action = es.leader_changed(self, io_ctx);
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

  /// Validate the uniqueness of `RequestId` and return the parsed SQL, or an
  /// appropriate error if this fails.
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

  /// Runs the `run_main_loop_once` until it finally results in changes to the node's state.
  fn run_main_loop<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO, statuses: &mut Statuses) {
    while !self.run_main_loop_once(io_ctx, statuses) {}
  }

  /// Thus runs one iteration of the Main Loop, returning `false` exactly when nothing changes.
  fn run_main_loop_once<IO: MasterIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
  ) -> bool {
    // First, we accumulate all TablePaths that currently have a a DDL Query running for them.
    let mut tables_being_modified = HashSet::<TablePath>::new();
    for (_, es) in &statuses.create_table_tm_ess {
      if let CreateTableTMS::Start = &es.state {
      } else {
        tables_being_modified.insert(es.table_path.clone());
      }
    }

    for (_, es) in &statuses.alter_table_tm_ess {
      if let paxos2pc::State::Start = &es.state {
      } else {
        tables_being_modified.insert(es.inner.table_path.clone());
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
          respond_invalid_ddl(io_ctx, response_data);
        }
      }
    }

    // Move AlterTableESs forward for TablePaths not in `tables_being_modified`
    {
      let mut ess_to_remove = Vec::<QueryId>::new();
      for (_, es) in &mut statuses.alter_table_tm_ess {
        if let paxos2pc::State::Start = &es.state {
          if !tables_being_modified.contains(&es.inner.table_path) {
            // Check Column Validity (which is where the Table exists and the column exists
            // or does not exist, depending on if alter_op is a DROP COLUMN or ADD COLUMN).
            if let Some(gen) = self.table_generation.get_last_version(&es.inner.table_path) {
              // The Table Exists.
              let schema =
                &self.db_schema.get(&(es.inner.table_path.clone(), gen.clone())).unwrap();
              let contains_col = weak_contains_col_latest(schema, &es.inner.alter_op.col_name);
              let is_add_col = es.inner.alter_op.maybe_col_type.is_some();
              if contains_col && !is_add_col || !contains_col && is_add_col {
                // We have Column Validity, so we move it to WaitingInsertTMPrepared.
                es.state = paxos2pc::State::WaitingInsertTMPrepared;
                tables_being_modified.insert(es.inner.table_path.clone());
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
        if let Some(response_data) = &es.inner.response_data {
          respond_invalid_ddl(io_ctx, response_data);
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
          respond_invalid_ddl(io_ctx, response_data);
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
    action: STMPaxos2PCTMAction,
  ) {
    match action {
      STMPaxos2PCTMAction::Wait => {}
      STMPaxos2PCTMAction::Exit => {
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
  pub fn broadcast_gossip<IO: MasterIOCtx>(&mut self, io_ctx: &mut IO) {
    let gossip_data = GossipData {
      gen: self.gen.clone(),
      db_schema: self.db_schema.clone(),
      table_generation: self.table_generation.clone(),
      sharding_config: self.sharding_config.clone(),
      tablet_address_config: self.tablet_address_config.clone(),
      slave_address_config: self.slave_address_config.clone(),
      master_address_config: self.master_address_config.clone(),
    };
    let sids: Vec<SlaveGroupId> = self.slave_address_config.keys().cloned().collect();
    for sid in sids {
      self.ctx(io_ctx).send_to_slave_common(
        sid,
        msg::SlaveRemotePayload::MasterGossip(msg::MasterGossip {
          gossip_data: gossip_data.clone(),
        }),
      )
    }
  }
}

/// Send `InvalidDDLQuery` to the given `ResponseData`
fn respond_invalid_ddl<IO: MasterIOCtx>(io_ctx: &mut IO, response_data: &ResponseData) {
  io_ctx.send(
    &response_data.sender_eid,
    msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
      msg::ExternalDDLQueryAborted {
        request_id: response_data.request_id.clone(),
        payload: ExternalDDLQueryAbortData::InvalidDDLQuery,
      },
    )),
  )
}
