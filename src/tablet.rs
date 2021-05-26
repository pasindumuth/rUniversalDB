use crate::common::{IOTypes, TableSchema};
use crate::model::common::{EndpointId, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange};
use crate::model::message::TabletMessage;
use rand::RngCore;
use std::collections::HashMap;

#[derive(Debug)]
pub struct TabletState<T: IOTypes> {
  /// IO Objects.
  rand: T::RngCoreT,
  network_output: T::NetworkOutT,

  /// Distribution
  schema: HashMap<TablePath, TableSchema>,
  sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  /// Metadata
  this_tablet_group_id: TabletGroupId, // The SlaveGroupId of this Slave
}

impl<T: IOTypes> TabletState<T> {
  pub fn new(
    rand: T::RngCoreT,
    network_output: T::NetworkOutT,
    schema: HashMap<TablePath, TableSchema>,
    sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, EndpointId>,
    this_tablet_group_id: TabletGroupId,
  ) -> TabletState<T> {
    TabletState {
      rand,
      network_output,
      schema,
      sharding_config,
      tablet_address_config,
      slave_address_config,
      this_tablet_group_id,
    }
  }

  pub fn handle_incoming_message(&mut self, msg: TabletMessage) {
    unimplemented!();
  }
}
