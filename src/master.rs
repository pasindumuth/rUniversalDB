use crate::common::{IOTypes, TableSchema};
use crate::model::common::{
  EndpointId, Gen, QueryId, RequestId, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange,
  Timestamp,
};
use crate::model::message as msg;
use crate::model::message::MasterMessage;
use std::collections::HashMap;

#[derive(Debug)]
pub struct AlterTableTMStatus {
  table_path: TablePath,
  alter_op: msg::AlterOp,
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
  pending_alter_table_requests: HashMap<QueryId, (TablePath, msg::AlterOp, RequestId)>,
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
      MasterMessage::PerformExternalAlterTable(_) => {}
      MasterMessage::CancelExternalAlterTable(_) => {}
      MasterMessage::AlterTablePrepared(_) => {}
      MasterMessage::AlterTableAborted(_) => {}
      MasterMessage::PerformMasterFrozenColUsage(_) => {}
      MasterMessage::CancelMasterFrozenColUsage(_) => {}
    }
  }
}
