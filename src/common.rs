use crate::model::common::{
  ColName, ColType, EndpointId, QueryId, TablePath, TabletGroupId, Timestamp,
};
use crate::model::message::{NetworkMessage, TabletMessage};
use crate::multiversion_map::MVM;
use rand::{Rng, RngCore};
use std::collections::HashMap;

pub trait Clock {
  fn now(&mut self) -> Timestamp;
}

pub trait NetworkOut {
  fn send(&mut self, eid: &EndpointId, msg: NetworkMessage);
}

pub trait TabletForwardOut {
  fn forward(&mut self, tablet_group_id: &TabletGroupId, msg: TabletMessage);
}

pub trait IOTypes {
  type RngCoreT: RngCore;
  type ClockT: Clock;
  type NetworkOutT: NetworkOut;
  type TabletForwardOutT: TabletForwardOut;
}

/// These are very low-level utilities where I consider
/// it a shortcoming of the language that there isn't something
/// I can already use.

pub fn rvec(i: i32, j: i32) -> Vec<i32> {
  (i..j).collect()
}

pub fn mk_qid<R: Rng>(rng: &mut R) -> QueryId {
  let mut bytes: [u8; 8] = [0; 8];
  rng.fill(&mut bytes);
  QueryId(bytes)
}

// -----------------------------------------------------------------------------------------------
//  Table Schema
// -----------------------------------------------------------------------------------------------

/// A struct to encode the Table Schema of a table. Recall that Key Columns (which forms
/// the PrimaryKey) can't change. However, Value Columns can change, and they do so in a
/// versioned fashion with an MVM.
#[derive(Debug, Clone)]
pub struct TableSchema {
  key_cols: Vec<(ColName, ColType)>,
  val_cols: MVM<ColName, ColType>,
}

impl TableSchema {
  pub fn new(key_cols: Vec<(ColName, ColType)>, val_cols: Vec<(ColName, ColType)>) -> TableSchema {
    // We construct the map underlying an MVM using the given `val_cols`. We just
    // set the version Timestamp to be 0, and also set the initial LAT to be 0.
    let mut mvm = MVM::<ColName, ColType>::new();
    for (col_name, col_type) in val_cols {
      mvm.write(&col_name, Some(col_type), Timestamp(0));
    }
    TableSchema { key_cols, val_cols: mvm }
  }
}

// -------------------------------------------------------------------------------------------------
// Gossip
// -------------------------------------------------------------------------------------------------

/// Holds Gossip Data in a node. It's accessible in both Tablets and Slaves.
#[derive(Debug, Clone)]
pub struct GossipData {
  pub gossip_gen: u32,
  pub gossiped_db_schema: HashMap<TablePath, TableSchema>,
}
