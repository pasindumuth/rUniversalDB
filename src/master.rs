use crate::alter_table_es::{AlterTableAction, AlterTableES, AlterTableS};
use crate::col_usage::{ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, mk_qid, GossipData, GossipDataSer, IOTypes, NetworkOut, TableSchema, TableSchemaSer,
};
use crate::model::common::proc::{AlterTable, TableRef};
use crate::model::common::{
  proc, ColName, EndpointId, Gen, QueryId, RequestId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::model::message::{
  ColUsageTree, ExternalDDLQueryAbortData, FrozenColUsageTree, MasterMessage,
};
use crate::server::{CommonQuery, CoreServerContext};
use crate::sql_parser::convert_ddl_ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use sqlparser::test_utils::table;
use std::cmp::max;
use std::collections::{HashMap, HashSet};

// -----------------------------------------------------------------------------------------------
//  Master State
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct Statuses {
  alter_table_ess: HashMap<QueryId, AlterTableES>,
}

#[derive(Debug)]
pub struct MasterState<T: IOTypes> {
  context: MasterContext<T>,
  statuses: Statuses,
}

impl<T: IOTypes> MasterState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    db_schema: HashMap<TablePath, TableSchema>,
    sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, EndpointId>,
  ) -> MasterState<T> {
    MasterState {
      context: MasterContext {
        rand,
        clock,
        network_output,
        gen: Gen(0),
        db_schema,
        table_generation: Default::default(),
        sharding_config,
        tablet_address_config,
        slave_address_config,
        external_request_id_map: Default::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::MasterMessage) {
    self.context.handle_incoming_message(&mut self.statuses, message);
  }
}

#[derive(Debug)]
pub struct MasterContext<T: IOTypes> {
  /// IO Objects.
  pub rand: T::RngCoreT,
  pub clock: T::ClockT,
  pub network_output: T::NetworkOutT,

  /// Database Schema
  pub gen: Gen,
  pub db_schema: HashMap<TablePath, TableSchema>,
  // TODO: make use of this.
  pub table_generation: HashMap<TablePath, Gen>,

  /// Distribution
  pub sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  /// Request Management
  pub external_request_id_map: HashMap<RequestId, QueryId>,
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

  pub fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::MasterMessage) {
    match message {
      MasterMessage::PerformExternalDDLQuery(external_alter) => {
        match self.validate_ddl_query(&external_alter) {
          Ok(alter_table) => {
            let query_id = mk_qid(&mut self.rand);
            let request_id = external_alter.request_id;
            self.external_request_id_map.insert(request_id.clone(), query_id.clone());
            statuses.alter_table_ess.insert(
              query_id.clone(),
              AlterTableES {
                request_id,
                sender_eid: external_alter.sender_eid,
                query_id,
                table_path: alter_table.table_path,
                alter_op: alter_table.alter_op,
                state: AlterTableS::Start,
              },
            );
          }
          Err(payload) => {
            // We return an error because the RequestId is not unique.
            self.network_output.send(
              &external_alter.sender_eid,
              msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
                msg::ExternalDDLQueryAborted { request_id: external_alter.request_id, payload },
              )),
            );
          }
        }
      }
      MasterMessage::CancelExternalDDLQuery(cancel) => {
        if let Some(query_id) = self.external_request_id_map.get(&cancel.request_id) {
          self.exit_and_clean_up(statuses, query_id.clone())
        }
        // Send off a cancellation confirmation to the External.
        self.network_output.send(
          &cancel.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
            msg::ExternalDDLQueryAborted {
              request_id: cancel.request_id,
              payload: ExternalDDLQueryAbortData::ConfirmCancel,
            },
          )),
        );
      }
      MasterMessage::AlterTablePrepared(prepared) => {
        let query_id = prepared.query_id.clone();
        if let Some(es) = statuses.alter_table_ess.get_mut(&query_id) {
          let action = es.handle_prepared(self, prepared);
          self.handle_alter_table_es_action(statuses, query_id, action);
        }
      }
      MasterMessage::AlterTableAborted(_) => {
        // The Tablets never send this.
        panic!()
      }
      MasterMessage::PerformMasterFrozenColUsage(request) => {
        // Construct the FrozenColUsageTree with the current database schema in the Master.
        let timestamp = request.timestamp.clone();
        let mut planner = ColUsagePlanner { db_schema: &self.db_schema, timestamp };
        let frozen_col_usage_tree = match request.col_usage_tree {
          ColUsageTree::MSQuery(ms_query) => {
            msg::FrozenColUsageTree::ColUsageNodes(planner.plan_ms_query(&ms_query))
          }
          ColUsageTree::GRQuery(gr_query) => {
            let mut trans_table_schemas = request.trans_table_schemas;
            msg::FrozenColUsageTree::ColUsageNodes(
              planner.plan_gr_query(&mut trans_table_schemas, &gr_query),
            )
          }
          ColUsageTree::MSQueryStage(stage_query) => {
            let mut trans_table_schemas = request.trans_table_schemas;
            msg::FrozenColUsageTree::ColUsageNode(
              planner.plan_ms_query_stage(&mut trans_table_schemas, &stage_query),
            )
          }
        };

        // Freeze the `safe_present_cols` and `external_cols` used for every node in the
        // FrozenColUsageTree computed above.
        self.freeze_schema(&frozen_col_usage_tree, timestamp);

        // Send the response to the originator.
        // TODO: send this to the coord properly
        // let response = CommonQuery::MasterFrozenColUsageSuccess(msg::MasterFrozenColUsageSuccess {
        //   return_qid: request.sender_path.query_id.clone(),
        //   frozen_col_usage_tree,
        //   gossip: GossipDataSer::from_gossip(GossipData {
        //     gen: self.gen.clone(),
        //     db_schema: self.db_schema.clone(),
        //     table_generation: self.table_generation.clone(),
        //     sharding_config: self.sharding_config.clone(),
        //     tablet_address_config: self.tablet_address_config.clone(),
        //     slave_address_config: self.slave_address_config.clone(),
        //   }),
        // });
        // self.ctx().send_to_path(request.sender_path, response);
      }
      MasterMessage::CancelMasterFrozenColUsage(_) => panic!(),
      MasterMessage::CreateTablePrepared(_) => panic!(),
      MasterMessage::CreateTableAborted(_) => panic!(),
      MasterMessage::CreateTableInformPrepared(_) => panic!(),
      MasterMessage::CreateTableWait(_) => panic!(),
      MasterMessage::AlterTableInformPrepared(_) => panic!(),
      MasterMessage::AlterTableWait(_) => panic!(),
      MasterMessage::DropTablePrepared(_) => panic!(),
      MasterMessage::DropTableAborted(_) => panic!(),
      MasterMessage::DropTableInformPrepared(_) => panic!(),
      MasterMessage::DropTableWait(_) => panic!(),
      MasterMessage::CreateTableCloseConfirm(_) => panic!(),
      MasterMessage::AlterTableCloseConfirm(_) => panic!(),
      MasterMessage::DropTableCloseConfirm(_) => panic!(),
    }

    self.run_main_loop(statuses);
  }

  /// Validate the uniqueness of `RequestId`, parse the SQL, ensure the `TablePath` exists, and
  /// check that the column being modified is not a key column before returning the `AlterTable`.
  fn validate_ddl_query(
    &self,
    external_query: &msg::PerformExternalDDLQuery,
  ) -> Result<proc::AlterTable, msg::ExternalDDLQueryAbortData> {
    if self.external_request_id_map.contains_key(&external_query.request_id) {
      // Duplicate RequestId; respond with an abort.
      Err(msg::ExternalDDLQueryAbortData::NonUniqueRequestId)
    } else {
      // Parse the SQL
      match Parser::parse_sql(&GenericDialect {}, &external_query.query) {
        Ok(parsed_ast) => {
          let alter_table = convert_ddl_ast(&parsed_ast);
          // Do several more checks on `alter_table` before returning.
          if let Some(table_schema) = self.db_schema.get(&alter_table.table_path) {
            if lookup(&table_schema.key_cols, &alter_table.alter_op.col_name).is_none() {
              return Ok(alter_table);
            }
          }
          Err(msg::ExternalDDLQueryAbortData::InvalidAlterOp)
        }
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
    while self.run_main_loop_once(statuses) {}
  }

  /// Thus runs one iteration of the Main Loop, returning `false` exactly when nothing changes.
  fn run_main_loop_once(&mut self, statuses: &mut Statuses) -> bool {
    // First, figure out which (TablePath, ColName)s are used by `alter_table_ess`.
    let mut used_col_names = HashSet::<(TablePath, ColName)>::new();
    for (_, es) in &statuses.alter_table_ess {
      used_col_names.insert((es.table_path.clone(), es.alter_op.col_name.clone()));
    }

    // Then, see if there is AlterTableES that is still in the `Start` state that
    // does not use the above, and start it if it exists.
    for (query_id, es) in &mut statuses.alter_table_ess {
      if matches!(es.state, AlterTableS::Start) {
        if !used_col_names.contains(&(es.table_path.clone(), es.alter_op.col_name.clone())) {
          let query_id = query_id.clone();
          let action = es.start(self);
          self.handle_alter_table_es_action(statuses, query_id, action);
          return true;
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

    match frozen_col_usage_tree {
      FrozenColUsageTree::ColUsageNodes(nodes) => {
        for (_, (_, node)) in nodes {
          freeze_schema_r(&mut self.db_schema, node, timestamp);
        }
      }
      FrozenColUsageTree::ColUsageNode((_, node)) => {
        freeze_schema_r(&mut self.db_schema, node, timestamp);
      }
    }
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
        let es = statuses.alter_table_ess.remove(&query_id).unwrap();
        self.external_request_id_map.remove(&es.request_id);
        // Send off a success message to the Externa.
        self.network_output.send(
          &es.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(
            msg::ExternalDDLQuerySuccess {
              request_id: es.request_id.clone(),
              timestamp: new_timestamp,
            },
          )),
        );
      }
      AlterTableAction::ColumnInvalid => {
        let es = statuses.alter_table_ess.remove(&query_id).unwrap();
        self.external_request_id_map.remove(&es.request_id);
        // Send off a failure message to the External.
        self.network_output.send(
          &es.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
            msg::ExternalDDLQueryAborted {
              request_id: es.request_id.clone(),
              payload: ExternalDDLQueryAbortData::InvalidAlterOp,
            },
          )),
        );
      }
    }
  }

  /// Removes the `query_id` from the Master, cleaning up any remaining resources as well.
  fn exit_and_clean_up(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    if let Some(mut es) = statuses.alter_table_ess.remove(&query_id) {
      self.external_request_id_map.remove(&es.request_id);
      es.exit_and_clean_up(self);
    }
  }
}
