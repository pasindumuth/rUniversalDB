use crate::alter_table_tm_es::{AlterTableAction, AlterTableTMES, AlterTableTMS};
use crate::col_usage::{ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, mk_qid, GossipData, GossipDataSer, IOTypes, NetworkOut, TableSchema, TableSchemaSer,
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
use crate::paxos::LeaderChanged;
use crate::server::{CommonQuery, CoreServerContext};
use crate::sql_parser::{convert_ddl_ast, DDLQuery};
use crate::test_utils::mk_tid;
use serde::{Deserialize, Serialize};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use sqlparser::test_utils::table;
use std::cmp::max;
use std::collections::{HashMap, HashSet};

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
    pub tablet_group_id: TabletGroupId,
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
  create_table_tm_ess: HashMap<QueryId, CreateTableTMES>,
  alter_table_tm_ess: HashMap<QueryId, AlterTableTMES>,
  drop_table_tm_ess: HashMap<QueryId, DropTableTMES>,
  planning_ess: HashMap<QueryId, MasterQueryPlanningES>,
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
  pub table_generation: HashMap<TablePath, Gen>,

  /// Distribution
  pub sharding_config: HashMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: HashMap<SlaveGroupId, Vec<EndpointId>>,

  /// LeaderMap
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  /// Request Management
  pub external_request_id_map: HashMap<RequestId, QueryId>,
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
        table_generation: Default::default(),
        sharding_config,
        tablet_address_config,
        slave_address_config,
        leader_map: Default::default(),
        external_request_id_map: Default::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::MasterMessage) {
    // self.context.handle_incoming_message(&mut self.statuses, message);
  }
}

impl<T: IOTypes> MasterContext<T> {
  pub fn ctx(&mut self) -> CoreServerContext<T> {
    CoreServerContext {
      rand: &mut self.rand,
      clock: &mut self.clock,
      network_output: &mut self.network_output,
      sharding_config: &mut self.sharding_config,
      tablet_address_config: &mut self.tablet_address_config,
      slave_address_config: &mut self.slave_address_config,
    }
  }

  pub fn handle_input(&mut self, statuses: &mut Statuses, master_input: MasterForwardMsg) {
    match master_input {
      MasterForwardMsg::MasterBundle(_) => {}
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
                    // Construct shards
                    // pub shards: Vec<(TabletKeyRange, TabletGroupId, SlaveGroupId)>,

                    // TODO: continue
                    // let tablet_group_id = mk_tid()
                    // let shards = vec

                    // Construct ES
                    map_insert(
                      &mut statuses.create_table_tm_ess,
                      &query_id,
                      CreateTableTMES {
                        response_data: Some(ResponseData { request_id, sender_eid }),
                        query_id: query_id.clone(),
                        tablet_group_id: TabletGroupId("".to_string()),
                        table_path: create_table.table_path,
                        key_cols: create_table.key_cols,
                        val_cols: create_table.val_cols,
                        shards: vec![],
                        state: CreateTableTMS::Start,
                      },
                    );
                  }
                  DDLQuery::Alter(alter_table) => {
                    // Construct ES
                    map_insert(
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
                    map_insert(
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
    // First, figure out which (TablePath, ColName)s are used by `alter_table_ess`.
    // let mut used_col_names = HashSet::<(TablePath, ColName)>::new();
    // for (_, es) in &statuses.alter_table_ess {
    //   used_col_names.insert((es.table_path.clone(), es.alter_op.col_name.clone()));
    // }
    //
    // // Then, see if there is AlterTableES that is still in the `Start` state that
    // // does not use the above, and start it if it exists.
    // for (query_id, es) in &mut statuses.alter_table_ess {
    //   if matches!(es.state, AlterTableS::Start) {
    //     if !used_col_names.contains(&(es.table_path.clone(), es.alter_op.col_name.clone())) {
    //       let query_id = query_id.clone();
    //       let action = es.start(self);
    //       self.handle_alter_table_es_action(statuses, query_id, action);
    //       return true;
    //     }
    //   }
    // }

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
    action: AlterTableAction,
  ) {
    match action {
      AlterTableAction::Wait => {}
      AlterTableAction::Success(new_timestamp) => {
        // let es = statuses.alter_table_ess.remove(&query_id).unwrap();
        // self.external_request_id_map.remove(&es.request_id);
        // // Send off a success message to the Externa.
        // self.network_output.send(
        //   &es.sender_eid,
        //   msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(
        //     msg::ExternalDDLQuerySuccess {
        //       request_id: es.request_id.clone(),
        //       timestamp: new_timestamp,
        //     },
        //   )),
        // );
      }
      AlterTableAction::ColumnInvalid => {
        // let es = statuses.alter_table_ess.remove(&query_id).unwrap();
        // self.external_request_id_map.remove(&es.request_id);
        // // Send off a failure message to the External.
        // self.network_output.send(
        //   &es.sender_eid,
        //   msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
        //     msg::ExternalDDLQueryAborted {
        //       request_id: es.request_id.clone(),
        //       payload: ExternalDDLQueryAbortData::InvalidAlterOp,
        //     },
        //   )),
        // );
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
}
