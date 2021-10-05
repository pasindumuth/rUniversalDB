use crate::alter_table_tm_es::{AlterTableTMAction, AlterTableTMES, AlterTableTMS};
use crate::col_usage::{ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  btree_map_insert, lookup, mk_qid, mk_sid, mk_tid, GossipData, IOTypes, NetworkOut, TableSchema,
};
use crate::common::{map_insert, RemoteLeaderChangedPLm};
use crate::create_table_tm_es::{CreateTableTMES, CreateTableTMS, ResponseData};
use crate::drop_table_tm_es::{DropTableTMES, DropTableTMS};
use crate::model::common::proc::{AlterTable, TableRef};
use crate::model::common::{
  proc, CQueryPath, ColName, EndpointId, Gen, LeadershipId, PaxosGroupId, QueryId, RequestId,
  SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::model::message::{
  ExternalDDLQueryAbortData, FrozenColUsageTree, MasterExternalReq, MasterMessage,
};
use crate::multiversion_map::MVM;
use crate::paxos::LeaderChanged;
use crate::server::{
  weak_contains_col, weak_contains_col_latest, CommonQuery, MasterServerContext,
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
    pub timestamp_hint: Timestamp,
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
//  MasterForwardMsg
// -----------------------------------------------------------------------------------------------

pub enum MasterForwardMsg {
  MasterBundle(Vec<MasterPLm>),
  MasterExternalReq(msg::MasterExternalReq),
  MasterRemotePayload(msg::MasterRemotePayload),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
  LeaderChanged(LeaderChanged),
}

// -----------------------------------------------------------------------------------------------
//  Master MasterQueryPlanningES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum MasterQueryPlanningS {
  WaitingInserting,
  Inserting,
}

#[derive(Debug)]
struct MasterQueryPlanningES {
  sender_path: CQueryPath,
  query_id: QueryId,
  timestamp: Timestamp,
  ms_query: proc::MSQuery,
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
  /// IO Objects.
  pub rand: T::RngCoreT,
  pub clock: T::ClockT,
  pub network_output: T::NetworkOutT,

  /// Metadata
  pub this_eid: EndpointId,

  /// Database Schema
  pub gen: Gen,
  pub db_schema: HashMap<(TablePath, Gen), TableSchema>,
  pub table_generation: MVM<TablePath, Gen>,

  /// Distribution
  pub sharding_config: HashMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,

  /// LeaderMap
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  /// Request Management
  pub external_request_id_map: HashMap<RequestId, QueryId>,

  /// Paxos
  pub master_bundle: Vec<MasterPLm>,
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
        leader_map: Default::default(),
        external_request_id_map: Default::default(),
        master_bundle: vec![],
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::MasterMessage) {
    // self.context.handle_incoming_message(&mut self.statuses, message);
  }
}

impl<T: IOTypes> MasterContext<T> {
  pub fn ctx(&mut self) -> MasterServerContext<T> {
    MasterServerContext {
      rand: &mut self.rand,
      clock: &mut self.clock,
      network_output: &mut self.network_output,
      sharding_config: &mut self.sharding_config,
      tablet_address_config: &mut self.tablet_address_config,
      slave_address_config: &mut self.slave_address_config,
      leader_map: &mut self.leader_map,
    }
  }

  pub fn handle_input(&mut self, statuses: &mut Statuses, master_input: MasterForwardMsg) {
    match master_input {
      MasterForwardMsg::MasterBundle(bundle) => {
        if self.is_leader() {
          for paxos_log_msg in bundle {
            match paxos_log_msg {
              MasterPLm::MasterQueryPlanning(_) => {}
              // CreateTable
              MasterPLm::CreateTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                es.handle_prepared_plm(self);
              }
              MasterPLm::CreateTableTMCommitted(committed) => {
                let query_id = committed.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                es.handle_committed_plm(self);
              }
              MasterPLm::CreateTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                es.handle_aborted_plm(self);
              }
              MasterPLm::CreateTableTMClosed(closed) => {
                let query_id = closed.query_id;
                let es = statuses.create_table_tm_ess.get_mut(&query_id).unwrap();
                es.handle_closed_plm(self);
              }
              // AlterTable
              MasterPLm::AlterTableTMPrepared(prepared) => {
                let query_id = prepared.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_prepared_plm(self);
                self.handle_alter_table_es_action(statuses, query_id, action);
              }
              MasterPLm::AlterTableTMCommitted(committed) => {
                let query_id = committed.query_id;
                let es = statuses.alter_table_tm_ess.get_mut(&query_id).unwrap();
                let action = es.handle_committed_plm(self);
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
                es.handle_prepared_plm(self);
              }
              MasterPLm::DropTableTMCommitted(committed) => {
                let query_id = committed.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                es.handle_committed_plm(self);
              }
              MasterPLm::DropTableTMAborted(aborted) => {
                let query_id = aborted.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                es.handle_aborted_plm(self);
              }
              MasterPLm::DropTableTMClosed(closed) => {
                let query_id = closed.query_id;
                let es = statuses.drop_table_tm_ess.get_mut(&query_id).unwrap();
                es.handle_closed_plm(self);
              }
            }
          }

          // Run the Main Loop
          self.run_main_loop(statuses);

          // Inform all ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.create_table_tm_ess {
            es.starting_insert(self);
          }
          for (_, es) in &mut statuses.alter_table_tm_ess {
            if let AlterTableTMS::WaitingInsertTMPrepared = &es.state {
              self.master_bundle.push(MasterPLm::AlterTableTMPrepared(plm::AlterTableTMPrepared {
                query_id: es.query_id.clone(),
                table_path: es.table_path.clone(),
                alter_op: es.alter_op.clone(),
              }));
              es.starting_insert(self);
            }
          }
          for (_, es) in &mut statuses.drop_table_tm_ess {
            if let DropTableTMS::WaitingInsertTMPrepared = &es.state {
              self.master_bundle.push(MasterPLm::DropTableTMPrepared(plm::DropTableTMPrepared {
                query_id: es.query_id.clone(),
                table_path: es.table_path.clone(),
              }));
              es.starting_insert(self);
            }
          }
        } else {
          // TODO: handle Follower case
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
      MasterForwardMsg::MasterRemotePayload(_) => {}
      MasterForwardMsg::RemoteLeaderChanged(_) => {}
      MasterForwardMsg::LeaderChanged(_) => {}
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

  /// For each node in the `frozen_col_usage_tree`, we take the union of `safe_present_cols`
  /// and `external_col`, and then increase their lat to the `timestamp`. This makes computing
  /// a FrozelColUsageTree at this Timestamp idemptotent.
  fn freeze_schema(&mut self, frozen_col_usage_tree: &FrozenColUsageTree, timestamp: Timestamp) {
    fn freeze_schema_r(
      db_schema: &mut HashMap<TablePath, TableSchema>,
      node: &FrozenColUsageNode,
      timestamp: Timestamp,
    ) {
      match &node.table_ref {
        TableRef::TablePath(table_path) => {
          let table_schema = db_schema.get_mut(table_path).unwrap();
          for col in &node.safe_present_cols {
            // Update the LAT for non-Key Columns.
            if lookup(&table_schema.key_cols, col).is_none() {
              table_schema.val_cols.update_lat(col, timestamp);
            }
          }
          for col in &node.external_cols {
            // Recall that no External Column should be a Key Column.
            assert!(lookup(&table_schema.key_cols, col).is_none());
            table_schema.val_cols.update_lat(col, timestamp);
          }
        }
        TableRef::TransTableName(_) => {}
      }

      // Recurse through the child nodes.
      for child in &node.children {
        for (_, (_, node)) in child {
          freeze_schema_r(db_schema, node, timestamp);
        }
      }
    }
    //
    // match frozen_col_usage_tree {
    //   FrozenColUsageTree::ColUsageNodes(nodes) => {
    //     for (_, (_, node)) in nodes {
    //       freeze_schema_r(&mut self.db_schema, node, timestamp);
    //     }
    //   }
    //   FrozenColUsageTree::ColUsageNode((_, node)) => {
    //     freeze_schema_r(&mut self.db_schema, node, timestamp);
    //   }
    // }
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

  /// Removes the `query_id` from the Master, cleaning up any remaining resources as well.
  fn exit_and_clean_up(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    // if let Some(mut es) = statuses.alter_table_ess.remove(&query_id) {
    //   self.external_request_id_map.remove(&es.request_id);
    //   es.exit_and_clean_up(self);
    // }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&PaxosGroupId::Master).unwrap();
    lid.eid == self.this_eid
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
