use crate::col_usage::FrozenColUsageNode;
use crate::model::common::{
  ColName, ColType, EndpointId, Gen, NodeGroupId, QueryId, QueryPath, TablePath, TableView,
  TabletGroupId, Timestamp, TransTableName,
};
use crate::model::message as msg;
use crate::multiversion_map::MVM;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

pub trait Clock {
  fn now(&mut self) -> Timestamp;
}

pub trait NetworkOut {
  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage);
}

pub trait TabletForwardOut {
  fn forward(&mut self, tablet_group_id: &TabletGroupId, msg: msg::TabletMessage);
}

pub trait IOTypes {
  type RngCoreT: RngCore + Debug;
  type ClockT: Clock + Debug;
  type NetworkOutT: NetworkOut + Debug;
  type TabletForwardOutT: TabletForwardOut + Debug;
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

pub fn lookup<'a, K: Eq, V>(assoc: &'a Vec<(K, V)>, key: &K) -> Option<&'a V> {
  assoc.iter().find(|(k, _)| k == key).map(|(_, v)| v)
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

  pub fn get_key_cols(&self) -> Vec<ColName> {
    self.key_cols.iter().map(|(col, _)| col.clone()).collect()
  }
}

/// A Serializable version of `TableSchema`. This is needed since it's
/// not serializable automatically
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableSchemaSer {
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: HashMap<ColName, (Timestamp, Vec<(Timestamp, Option<ColType>)>)>,
}

impl TableSchemaSer {
  pub fn from_schema(schema: TableSchema) -> TableSchemaSer {
    TableSchemaSer { key_cols: schema.key_cols, val_cols: schema.val_cols.map }
  }

  pub fn to_schema(self) -> TableSchema {
    TableSchema { key_cols: self.key_cols, val_cols: MVM { map: self.val_cols } }
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

/// A Serializable version of `GossipData`. This is needed since it's
/// not serializable automatically
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct GossipDataSer {
  pub gossip_gen: Gen,
  pub gossiped_db_schema: HashMap<TablePath, TableSchemaSer>,
}

impl GossipDataSer {
  pub fn from_gossip(gossip: GossipData) -> GossipDataSer {
    GossipDataSer {
      gossip_gen: gossip.gossip_gen,
      gossiped_db_schema: gossip
        .gossiped_db_schema
        .into_iter()
        .map(|(table_path, schema)| (table_path, TableSchemaSer::from_schema(schema)))
        .collect(),
    }
  }

  pub fn to_gossip(self) -> GossipData {
    GossipData {
      gossip_gen: self.gossip_gen,
      gossiped_db_schema: self
        .gossiped_db_schema
        .into_iter()
        .map(|(table_path, schema)| (table_path, schema.to_schema()))
        .collect(),
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  TMStatus
// -----------------------------------------------------------------------------------------------
// These are used to perform PCSA over the network for reads and writes.

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TMWaitValue {
  Nothing,
  Result((Vec<ColName>, Vec<TableView>)),
}

#[derive(Debug)]
pub struct TMStatus {
  pub node_group_ids: HashMap<NodeGroupId, QueryId>,
  pub query_id: QueryId,
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
pub struct OrigP {
  pub query_id: QueryId,
}

impl OrigP {
  pub fn new(query_id: QueryId) -> OrigP {
    OrigP { query_id }
  }
}

pub fn merge_table_views(
  mut results: Vec<(Vec<ColName>, Vec<TableView>)>,
) -> (Vec<ColName>, Vec<TableView>) {
  let (schema, mut views) = results.pop().unwrap();
  for (cur_schema, cur_views) in results {
    assert_eq!(cur_schema, schema);
    assert_eq!(cur_views.len(), views.len());
    for (idx, cur_view) in cur_views.into_iter().enumerate() {
      let view = views.get_mut(idx).unwrap();
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

/// This is a simple insert-get operation for HashMaps. We usually want to create a value
/// in the same expression as the insert operation, but we also want to get a &mut to the
/// inserted value. This function does this.
pub fn map_insert<'a, K: Clone + Eq + Hash, V>(
  map: &'a mut HashMap<K, V>,
  key: &K,
  value: V,
) -> &'a mut V {
  map.insert(key.clone(), value);
  map.get_mut(key).unwrap()
}

/// Used to add elements form a BTree MultiMap
pub fn btree_multimap_insert<K: Ord + Clone, V: Ord>(
  map: &mut BTreeMap<K, BTreeSet<V>>,
  key: &K,
  value: V,
) {
  if let Some(set) = map.get_mut(key) {
    set.insert(value);
  } else {
    let mut set = BTreeSet::<V>::new();
    set.insert(value);
    map.insert(key.clone(), set);
  }
}

/// Used to removed elements form a BTree MultiMap
pub fn btree_multimap_remove<K: Ord, V: Ord>(
  map: &mut BTreeMap<K, BTreeSet<V>>,
  key: &K,
  value: &V,
) {
  if let Some(set) = map.get_mut(key) {
    set.remove(value);
    if set.is_empty() {
      map.remove(key);
    }
  }
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

// A Generic double-sided bound for an orderable value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColBound<T> {
  pub start: SingleBound<T>,
  pub end: SingleBound<T>,
}

impl<T> ColBound<T> {
  pub fn new(start: SingleBound<T>, end: SingleBound<T>) -> ColBound<T> {
    ColBound { start, end }
  }
}

// There is a Variant here for every ColType.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolyColBound {
  Int(ColBound<i32>),
  String(ColBound<String>),
  Bool(ColBound<bool>),
}

/// A full Boundary for a `PrimaryKey`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyBound {
  pub col_bounds: Vec<PolyColBound>,
}

/// TableRegion, used to represent both ReadRegions and WriteRegions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableRegion {
  pub col_region: Vec<ColName>,
  pub row_region: Vec<KeyBound>,
}
