use crate::col_usage::FrozenColUsageNode;
use crate::coord::CoordForwardMsg;
use crate::master::MasterTimerInput;
use crate::model::common::{
  CTNodePath, ColName, ColType, CoordGroupId, EndpointId, Gen, LeadershipId, PaxosGroupId, QueryId,
  SlaveGroupId, TQueryPath, TablePath, TableView, TabletGroupId, TabletKeyRange, TierMap,
  Timestamp,
};
use crate::model::message as msg;
use crate::multiversion_map::MVM;
use crate::slave::{SlaveBackMessage, SlaveTimerInput};
use crate::tablet::{TabletCreateHelper, TabletForwardMsg};
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::Hash;

// -----------------------------------------------------------------------------------------------
//  Basic
// -----------------------------------------------------------------------------------------------

pub trait BasicIOCtx<NetworkMessageT = msg::NetworkMessage> {
  type RngCoreT: RngCore + Debug;

  // Basic
  fn rand(&mut self) -> &mut Self::RngCoreT;
  fn now(&mut self) -> Timestamp;
  fn send(&mut self, eid: &EndpointId, msg: NetworkMessageT);
}

// -----------------------------------------------------------------------------------------------
//  SlaveIOCtx
// -----------------------------------------------------------------------------------------------

pub trait SlaveIOCtx: BasicIOCtx {
  // Tablet
  fn create_tablet(&mut self, helper: TabletCreateHelper);
  fn tablet_forward(&mut self, tablet_group_id: &TabletGroupId, forward_msg: TabletForwardMsg);
  fn all_tids(&self) -> Vec<TabletGroupId>;
  fn num_tablets(&self) -> usize;

  // Coord
  fn coord_forward(&mut self, coord_group_id: &CoordGroupId, forward_msg: CoordForwardMsg);
  fn all_cids(&self) -> Vec<CoordGroupId>;

  // Timer
  fn defer(&mut self, defer_time: Timestamp, timer_input: SlaveTimerInput);
}

// -----------------------------------------------------------------------------------------------
//  CoreIOCtx
// -----------------------------------------------------------------------------------------------

pub trait CoreIOCtx: BasicIOCtx {
  // Slave
  fn slave_forward(&mut self, forward_msg: SlaveBackMessage);
}

// -----------------------------------------------------------------------------------------------
//  MasterIOCtx
// -----------------------------------------------------------------------------------------------

pub trait MasterIOCtx: BasicIOCtx {
  // Timer
  fn defer(&mut self, defer_time: Timestamp, timer_input: MasterTimerInput);
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

/// This is a simple insert-get operation for BTreeMaps. We usually want to create a value
/// in the same expression as the insert operation, but we also want to get a &mut to the
/// inserted value. This function does this.
pub fn map_insert<'a, K: Clone + Eq + Ord, V>(
  map: &'a mut BTreeMap<K, V>,
  key: &K,
  value: V,
) -> &'a mut V {
  map.insert(key.clone(), value);
  map.get_mut(key).unwrap()
}

/// Same as above, except for BTrees
pub fn btree_map_insert<'a, K: Clone + Eq + Ord, V>(
  map: &'a mut BTreeMap<K, V>,
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

/// A basic UUID type
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UUID(String);

// -----------------------------------------------------------------------------------------------
//  Table Schema
// -----------------------------------------------------------------------------------------------

/// A struct to encode the Table Schema of a table. Recall that Key Columns (which forms
/// the PrimaryKey) can't change. However, Value Columns can change, and they do so in a
/// versioned fashion with an MVM.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
      // We start at Timestamp 1 because 0 is already used, where every key in
      // existance maps to None.
      mvm.write(&col_name, Some(col_type), 1);
    }
    TableSchema { key_cols, val_cols: mvm }
  }

  pub fn get_key_cols(&self) -> Vec<ColName> {
    self.key_cols.iter().map(|(col, _)| col.clone()).collect()
  }
}

// -------------------------------------------------------------------------------------------------
// Gossip
// -------------------------------------------------------------------------------------------------

/// Holds Gossip Data in a node. It's accessible in both Tablets and Slaves.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct GossipData {
  /// Database Schema
  pub gen: Gen,
  pub db_schema: BTreeMap<(TablePath, Gen), TableSchema>,
  pub table_generation: MVM<TablePath, Gen>,

  /// Distribution
  pub sharding_config: BTreeMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: BTreeMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
  pub master_address_config: Vec<EndpointId>,
}

// -----------------------------------------------------------------------------------------------
//  Common Paxos Messages
// -----------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RemoteLeaderChangedPLm {
  pub gid: PaxosGroupId,
  pub lid: LeadershipId,
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
  pub new_rms: BTreeSet<TQueryPath>,
  /// The current set of Leaderships that this TMStatus is waiting on. Thus, in order to
  /// contact an RM, we just use the `LeadershipId` found here.
  pub leaderships: BTreeMap<SlaveGroupId, LeadershipId>,
  /// Holds the number of nodes that responded (used to decide when this TM is done).
  pub responded_count: usize,
  pub tm_state: BTreeMap<CTNodePath, Option<(Vec<ColName>, Vec<TableView>)>>,
  pub orig_p: OrigP,
}

// -----------------------------------------------------------------------------------------------
//  Basic Utils
// -----------------------------------------------------------------------------------------------

fn rand_string<R: Rng>(rng: &mut R) -> String {
  rng.sample_iter(&Alphanumeric).take(12).map(char::from).collect()
}

pub fn mk_qid<R: Rng>(rng: &mut R) -> QueryId {
  QueryId(rand_string(rng))
}

pub fn mk_tid<R: Rng>(rng: &mut R) -> TabletGroupId {
  TabletGroupId(rand_string(rng))
}

pub fn mk_cid<R: Rng>(rng: &mut R) -> CoordGroupId {
  CoordGroupId(rand_string(rng))
}

pub fn mk_sid<R: Rng>(rng: &mut R) -> SlaveGroupId {
  SlaveGroupId(rand_string(rng))
}

pub fn mk_uuid<R: Rng>(rng: &mut R) -> UUID {
  UUID(rand_string(rng))
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
  pub query_leader_map: BTreeMap<SlaveGroupId, LeadershipId>,
  pub table_location_map: BTreeMap<TablePath, Gen>,
  /// These are additional required columns that the QueryPlan expects that these `TablePaths`
  /// to have. These are columns that are not already present in the `col_usage_node`, such as
  /// projected columns in SELECT queries or assigned columns in UPDATE queries. While a TP is
  /// happen is happening, we must verify the presence of these `ColName`.
  ///
  /// Note: not all `TablePaths` used in the MSQuery needs to be here.
  pub extra_req_cols: BTreeMap<TablePath, Vec<ColName>>,
  pub col_usage_node: FrozenColUsageNode,
}

// -------------------------------------------------------------------------------------------------
//  Key Regions
// -------------------------------------------------------------------------------------------------

// Generic single-side bound for an orderable value.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SingleBound<T> {
  Included(T),
  Excluded(T),
  Unbounded,
}

// A Generic double-sided bound for an orderable value.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolyColBound {
  Int(ColBound<i32>),
  String(ColBound<String>),
  Bool(ColBound<bool>),
}

/// A full Boundary for a `PrimaryKey`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyBound {
  pub col_bounds: Vec<PolyColBound>,
}

/// TableRegion, used to represent both ReadRegions and WriteRegions.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableRegion {
  pub col_region: Vec<ColName>,
  pub row_region: Vec<KeyBound>,
}

// -----------------------------------------------------------------------------------------------
//  Wrapper Utilities
// -----------------------------------------------------------------------------------------------

/// Contains the result of ESs like *Table*ES and TransTableReadES.
#[derive(Debug, Clone)]
pub struct QueryESResult {
  pub result: (Vec<ColName>, Vec<TableView>),
  pub new_rms: Vec<TQueryPath>,
}
