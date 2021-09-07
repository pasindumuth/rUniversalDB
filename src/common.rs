use crate::col_usage::FrozenColUsageNode;
use crate::model::common::{
  proc, ColName, ColType, EndpointId, Gen, LeadershipId, NodeGroupId, NodePath, QueryId, QueryPath,
  SlaveGroupId, TablePath, TableView, TabletGroupId, TabletKeyRange, TierMap, Timestamp,
  TransTableName,
};
use crate::model::message as msg;
use crate::multiversion_map::MVM;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

// -----------------------------------------------------------------------------------------------
//  IOTypes
// -----------------------------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------------------------
//  Language
// -----------------------------------------------------------------------------------------------
/// These are very low-level utilities whose absence I consider a shortcoming of Rust.

/// Constructs a Vec out of the given indices.
pub fn rvec(i: i32, j: i32) -> Vec<i32> {
  (i..j).collect()
}

/// Lookup the position of a `key` in an associative list.
pub fn lookup_pos<K: Eq, V>(assoc: &Vec<(K, V)>, key: &K) -> Option<usize> {
  assoc.iter().position(|(k, _)| k == key)
}

/// Lookup the value, given the `key`, in an associative list.
pub fn lookup<'a, K: Eq, V>(assoc: &'a Vec<(K, V)>, key: &K) -> Option<&'a V> {
  assoc.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// Remove an item from the Vector
pub fn remove_item<V: Eq>(vec: &mut Vec<V>, item: &V) {
  if let Some(pos) = vec.iter().position(|x| x == item) {
    vec.remove(pos);
  }
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
  pub gen: Gen,
  pub db_schema: HashMap<TablePath, TableSchema>,
  pub table_generation: HashMap<TablePath, Gen>,

  // TODO: Change the keys here to (TablePath, Gen)
  /// Distribution
  pub sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: HashMap<SlaveGroupId, EndpointId>,
  // TODO: we should not need coord_address_config; any responder should remember
  //  the full sender_path
}

/// A Serializable version of `GossipData`. This is needed since it's
/// not serializable automatically
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct GossipDataSer {
  pub gen: Gen,
  pub db_schema: HashMap<TablePath, TableSchemaSer>,
  pub table_generation: HashMap<TablePath, Gen>,
  pub sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: HashMap<SlaveGroupId, EndpointId>,
}

impl GossipDataSer {
  pub fn from_gossip(gossip: GossipData) -> GossipDataSer {
    GossipDataSer {
      gen: gossip.gen,
      db_schema: gossip
        .db_schema
        .into_iter()
        .map(|(table_path, schema)| (table_path, TableSchemaSer::from_schema(schema)))
        .collect(),
      table_generation: gossip.table_generation,
      sharding_config: gossip.sharding_config,
      tablet_address_config: gossip.tablet_address_config,
      slave_address_config: gossip.slave_address_config,
    }
  }

  pub fn to_gossip(self) -> GossipData {
    GossipData {
      gen: self.gen,
      db_schema: self
        .db_schema
        .into_iter()
        .map(|(table_path, schema)| (table_path, schema.to_schema()))
        .collect(),
      table_generation: self.table_generation,
      sharding_config: self.sharding_config,
      tablet_address_config: self.tablet_address_config,
      slave_address_config: self.slave_address_config,
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  TMStatus
// -----------------------------------------------------------------------------------------------
// These are used to perform PCSA over the network for reads and writes.
#[derive(Debug)]
pub struct TMStatus {
  /// The QueryId of the TMStatus.
  pub query_id: QueryId,
  /// This is the QueryId of the PerformQuery. We keep this distinct from the TMStatus'
  /// QueryId, since one of the RMs might be this node.
  pub child_query_id: QueryId,
  pub new_rms: HashSet<QueryPath>,
  /// Holds the number of nodes that responded (used to decide when this TM is done).
  pub responded_count: usize,
  pub tm_state: HashMap<NodePath, Option<(Vec<ColName>, Vec<TableView>)>>,
  pub orig_p: OrigP,
}

// -----------------------------------------------------------------------------------------------
//  Basic Utils
// -----------------------------------------------------------------------------------------------

pub fn mk_qid<R: Rng>(rng: &mut R) -> QueryId {
  let mut bytes: [u8; 8] = [0; 8];
  rng.fill(&mut bytes);
  QueryId(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrigP {
  pub query_id: QueryId,
}

impl OrigP {
  pub fn new(query_id: QueryId) -> OrigP {
    OrigP { query_id }
  }
}

/// Here, every element of `results` has the same `Vec<ColName>`, and all `Vec<TableView>`s
/// have the same length. This function essentially merges together corresponding `TableView`.
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

// -----------------------------------------------------------------------------------------------
//  Query Plan
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
  pub tier_map: TierMap,
  pub query_leader_map: HashMap<SlaveGroupId, LeadershipId>,
  pub table_location_map: HashMap<TablePath, Gen>,
  /// These are additional required columns that the QueryPlan expects that these `TablePaths`
  /// to have. While a TP is happen is happening, we must must these `ColName` and verify
  /// their presence.
  ///
  /// Note: not all `TablePaths` used in the MSQuery needs to be here.
  pub extra_req_cols: HashMap<TablePath, Vec<ColName>>,
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

// -----------------------------------------------------------------------------------------------
//  Wrapper Utilities
// -----------------------------------------------------------------------------------------------

/// Contains the result of ESs like *Table*ES and TransTableReadES.
#[derive(Debug)]
pub struct QueryESResult {
  pub result: (Vec<ColName>, Vec<TableView>),
  pub new_rms: Vec<QueryPath>,
}
