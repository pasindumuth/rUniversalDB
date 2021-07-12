use crate::common::{lookup, mk_qid, IOTypes, NetworkOut, TableSchema};
use crate::model::common::proc::AlterTable;
use crate::model::common::{
  proc, EndpointId, Gen, QueryId, RequestId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::model::message::{ExternalDDLQueryAbortData, MasterMessage};
use crate::sql_parser::convert_ddl_ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use sqlparser::test_utils::table;
use std::collections::HashMap;

#[derive(Debug)]
pub struct AlterTableTMStatus {
  table_path: TablePath,
  alter_op: proc::AlterOp,
  request_id: RequestId,
  tm_state: HashMap<TabletGroupId, Option<Timestamp>>,
}

#[derive(Debug)]
pub struct MasterState<T: IOTypes> {
  /// IO Objects.
  rand: T::RngCoreT,
  clock: T::ClockT,
  network_output: T::NetworkOutT,
  tablet_forward_output: T::TabletForwardOutT,

  /// Database Schema
  gen: Gen,
  db_schema: HashMap<TablePath, TableSchema>,

  /// Distribution
  sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  /// AlterTable
  external_request_id_map: HashMap<RequestId, QueryId>,
  pending_alter_table_requests: HashMap<QueryId, (TablePath, proc::AlterOp, RequestId)>,
  alter_table_tm_status: HashMap<QueryId, AlterTableTMStatus>,
}

impl<T: IOTypes> MasterState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    tablet_forward_output: T::TabletForwardOutT,
    db_schema: HashMap<TablePath, TableSchema>,
    sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, EndpointId>,
  ) -> MasterState<T> {
    MasterState {
      rand,
      clock,
      network_output,
      tablet_forward_output,
      gen: Gen(0),
      db_schema,
      sharding_config,
      tablet_address_config,
      slave_address_config,
      external_request_id_map: Default::default(),
      pending_alter_table_requests: Default::default(),
      alter_table_tm_status: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::MasterMessage) {
    match message {
      MasterMessage::PerformExternalDDLQuery(external_alter) => {
        match self.validate_ddl_query(&external_alter) {
          Ok(alter_table) => {
            let query_id = mk_qid(&mut self.rand);
            let request_id = &external_alter.request_id;
            self.external_request_id_map.insert(request_id.clone(), query_id.clone());
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
      MasterMessage::CancelExternalDDLQuery(_) => {}
      MasterMessage::AlterTablePrepared(_) => {}
      MasterMessage::AlterTableAborted(_) => {}
      MasterMessage::PerformMasterFrozenColUsage(_) => {}
      MasterMessage::CancelMasterFrozenColUsage(_) => {}
    }
  }

  /// Validate the uniqueness of `RequestId`, parse the SQL, and do minor
  /// validations on it before returning the parsed output.
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
}
