use crate::col_usage::FrozenColUsageNode;
use crate::model::common::{
  ColName, ColType, EndpointId, Gen, NodeGroupId, QueryId, TablePath, TableView, TabletGroupId,
  Timestamp, TransTableName,
};
use crate::model::message::{NetworkMessage, TabletMessage};
use crate::multiversion_map::MVM;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: MVM<ColName, ColType>,
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
  pub gossip_gen: Gen,
  pub gossiped_db_schema: HashMap<TablePath, TableSchema>,
}

// -----------------------------------------------------------------------------------------------
//  TMStatus
// -----------------------------------------------------------------------------------------------
// These are used to perform PCSA over the network for reads and writes.

#[derive(Debug)]
pub enum TMWaitValue {
  QueryId(QueryId),
  Result((Vec<ColName>, HashMap<u32, TableView>)),
}

#[derive(Debug)]
pub struct TMStatus {
  pub root_query_id: QueryId,
  pub new_rms: HashSet<TabletGroupId>,
  /// Holds the number of nodes that responded (used to decide when this TM is done).
  pub responded_count: usize,
  pub tm_state: HashMap<NodeGroupId, TMWaitValue>,
  pub orig_path: QueryId,
}

// -----------------------------------------------------------------------------------------------
//  Basic Utils
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum OrigP {
  MSCoordPath(QueryId),
  MSQueryWritePath(QueryId, u32),
  MSQueryReadPath(QueryId, QueryId),
  ReadPath(QueryId),
}

// -----------------------------------------------------------------------------------------------
//  Query Plan
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
  pub gossip_gen: Gen,
  pub trans_table_schemas: HashMap<TransTableName, Vec<ColName>>,
  pub col_usage_node: FrozenColUsageNode,
}
