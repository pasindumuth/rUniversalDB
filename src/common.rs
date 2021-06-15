use crate::col_usage::FrozenColUsageNode;
use crate::model::common::{
  ColName, ColType, ColVal, EndpointId, Gen, NodeGroupId, QueryId, QueryPath, TablePath, TableView,
  TabletGroupId, Timestamp, TransTableName,
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

pub fn lookup_pos<K: Eq, V>(assoc: &Vec<(K, V)>, key: &K) -> Option<usize> {
  assoc.iter().position(|(k, _)| k == key)
}

// NOTE: We run into annoying lifetime problems here. Figure out later.
// pub fn lookup<'a, K: Eq, V>(assoc: &Vec<(K, V)>, key: &K) -> Option<&'a V> {
//   assoc.iter().find(|(k, _)| k == key).map(|(_, v)| v)
// }

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
  Nothing,
  Result((Vec<ColName>, Vec<TableView>)),
}

#[derive(Debug)]
pub struct TMStatus {
  pub node_group_ids: HashMap<NodeGroupId, QueryId>,
  pub new_rms: HashSet<QueryPath>,
  /// Holds the number of nodes that responded (used to decide when this TM is done).
  pub responded_count: usize,
  pub tm_state: HashMap<QueryId, TMWaitValue>,
  pub orig_p: OrigP,
}

// -----------------------------------------------------------------------------------------------
//  Basic Utils
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OrigP {
  StatusPath(QueryId),
}

pub fn merge_table_views(
  mut results: Vec<(Vec<ColName>, Vec<TableView>)>,
) -> (Vec<ColName>, Vec<TableView>) {
  let (schema, mut views) = results.pop().unwrap();
  for (cur_schema, cur_views) in results {
    assert_eq!(cur_schema, schema);
    assert_eq!(cur_views.len(), views.len());
    for (idx, cur_view) in cur_views.into_iter().enumerate() {
      let mut view = views.get_mut(idx).unwrap();
      assert_eq!(view.col_names, cur_view.col_names);
      for (cur_row_val, cur_row_count) in cur_view.rows {
        if let Some(row_count) = view.rows.get_mut(&cur_row_val) {
          *row_count += cur_row_count;
        } else {
          view.rows.insert(cur_row_val, cur_row_count);
        }
      }
    }
  }
  (schema, views)
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

// -------------------------------------------------------------------------------------------------
//  Key Regions
// -------------------------------------------------------------------------------------------------

// Generic single-side bound for an orderable value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SingleBound<T> {
  Included(T),
  Excluded(T),
  Unbounded,
}

/// Here, the Variant that the `ColVal` in each `SingleBound` takes
/// on has to be the same (i.e. the `ColVal` has to be the same type).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColBound {
  pub start: SingleBound<ColVal>,
  pub end: SingleBound<ColVal>,
}

impl ColBound {
  pub fn new(start: SingleBound<ColVal>, end: SingleBound<ColVal>) -> ColBound {
    ColBound { start, end }
  }
}

/// A full Boundary for a `PrimaryKey`
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyBound {
  pub key_col_bounds: Vec<ColBound>,
}

/// TableRegion, used to represent both ReadRegions and WriteRegions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableRegion {
  pub col_region: Vec<ColName>,
  pub row_region: Vec<KeyBound>,
}
