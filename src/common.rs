use crate::col_usage::ColUsageNode;
use crate::coord::{CoordContext, CoordForwardMsg, CoordState};
use crate::master::MasterTimerInput;
use crate::master_query_planning_es::ColPresenceReq;
use crate::model::common::{
  proc, CQueryPath, CTNodePath, ColName, ColType, ColVal, ColValN, CoordGroupId, EndpointId, Gen,
  LeadershipId, PaxosGroupId, PaxosGroupIdTrait, QueryId, RequestId, SlaveGroupId, TQueryPath,
  TablePath, TableView, TabletGroupId, TabletKeyRange, TierMap, TransTableLocationPrefix,
};
use crate::model::message as msg;
use crate::model::message::NetworkMessage;
use crate::multiversion_map::MVM;
use crate::node::{GenericInput, GenericTimerInput};
use crate::server::{CTServerContext, CommonQuery};
use crate::slave::{SlaveBackMessage, SlaveTimerInput};
use crate::tablet::{
  TabletConfig, TabletContext, TabletCreateHelper, TabletForwardMsg, TabletSnapshot, TabletState,
};
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

#[path = "./test/common_test.rs"]
pub mod common_test;

// -----------------------------------------------------------------------------------------------
//  Basic
// -----------------------------------------------------------------------------------------------

/// These messages are primarily for testing purposes.
pub enum GeneralTraceMessage {
  /// This should be called every time a `external_request_id_map` is updated.
  RequestIdQueryId(RequestId, QueryId),
  /// This should be called every time a MSQuery or DDLQuery is committed. There can
  /// be duplicates of this message.
  CommittedQueryId(QueryId, Timestamp),
  /// This indicates a reconfiguration happened at `PaxosGroupId`. Recall that such events can
  /// be identified uniquely globally by the new nodes they introduced.
  Reconfig(PaxosGroupId, Vec<EndpointId>),
}

pub trait BasicIOCtx<NetworkMessageT = msg::NetworkMessage> {
  type RngCoreT: RngCore + Debug;

  // Basic
  fn rand(&mut self) -> &mut Self::RngCoreT;
  fn now(&mut self) -> Timestamp;
  fn send(&mut self, eid: &EndpointId, msg: NetworkMessageT);

  /// Tracer
  fn general_trace(&mut self, trace_msg: GeneralTraceMessage);
}

// -----------------------------------------------------------------------------------------------
//  FreeNodeIOCtx
// -----------------------------------------------------------------------------------------------

pub trait FreeNodeIOCtx: BasicIOCtx {
  // Tablet
  fn create_tablet_full(
    &mut self,
    gossip: Arc<GossipData>,
    snapshot: TabletSnapshot,
    this_eid: EndpointId,
    tablet_config: TabletConfig,
  );

  // Coord
  fn create_coord_full(&mut self, ctx: CoordContext);

  // Timer
  fn defer(&mut self, defer_time: Timestamp, deferred_time: GenericTimerInput);
}

// -----------------------------------------------------------------------------------------------
//  SlaveIOCtx
// -----------------------------------------------------------------------------------------------

pub enum SlaveTraceMessage {
  LeaderChanged(PaxosGroupId, LeadershipId),
}

pub trait SlaveIOCtx: BasicIOCtx {
  /// This marks that the node should exit (which happens if the PaxosGroup ejects a node).
  fn mark_exit(&mut self);
  fn did_exit(&mut self) -> bool;

  // Tablet
  fn create_tablet(&mut self, helper: TabletCreateHelper);
  /// Forwards `forward_msg` to the Tablet. This function asserts that the Tablet exists.
  fn tablet_forward(&mut self, tablet_group_id: &TabletGroupId, forward_msg: TabletForwardMsg);
  fn all_tids(&self) -> Vec<TabletGroupId>;
  fn num_tablets(&self) -> usize;

  // Coord
  /// Forwards `forward_msg` to the Coord. This function asserts that the Coord exists.
  fn coord_forward(&mut self, coord_group_id: &CoordGroupId, forward_msg: CoordForwardMsg);
  fn all_cids(&self) -> Vec<CoordGroupId>;

  // Timer
  fn defer(&mut self, defer_time: Timestamp, timer_input: SlaveTimerInput);

  // Tracer
  fn trace(&mut self, trace_msg: SlaveTraceMessage);
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

pub enum MasterTraceMessage {
  LeaderChanged(LeadershipId),
  /// This is traced when the Master is first created.
  MasterCreation(LeadershipId),
  /// This is traced when a new SlaveGroup is created, where we also
  /// include the first `LeadershipId`.
  SlaveCreated(SlaveGroupId, LeadershipId),
}

pub trait MasterIOCtx: BasicIOCtx {
  /// This marks that the node should exit (which happens if the PaxosGroup ejects a node).
  fn mark_exit(&mut self);
  fn did_exit(&mut self) -> bool;

  // Timer
  fn defer(&mut self, defer_time: Timestamp, timer_input: MasterTimerInput);

  // Tracer
  fn trace(&mut self, trace_msg: MasterTraceMessage);
}

// -----------------------------------------------------------------------------------------------
//  NodeIOCtx
// -----------------------------------------------------------------------------------------------

pub trait NodeIOCtx: BasicIOCtx + FreeNodeIOCtx + SlaveIOCtx + MasterIOCtx {}

// -----------------------------------------------------------------------------------------------
//  Language
// -----------------------------------------------------------------------------------------------
/// These are very low-level utilities whose absence I consider a shortcoming of Rust.

pub trait RangeEnds: Sized {
  /// Constructs a Vec out of the given endpoints.
  fn rvec(i: Self, j: Self) -> Vec<Self>;
}

impl RangeEnds for i32 {
  fn rvec(i: i32, j: i32) -> Vec<i32> {
    (i..j).collect()
  }
}

impl RangeEnds for u32 {
  fn rvec(i: u32, j: u32) -> Vec<u32> {
    (i..j).collect()
  }
}

// FlatMap Interface

/// Lookup the position of a `key` in an associative list.
pub fn lookup_pos<K: Eq, V>(assoc: &Vec<(K, V)>, key: &K) -> Option<usize> {
  assoc.iter().position(|(k, _)| k == key)
}

/// Lookup the value, given the `key`, in an associative list.
pub fn lookup<'a, K: Eq, V>(assoc: &'a Vec<(K, V)>, key: &K) -> Option<&'a V> {
  assoc.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

// FlatSet Interface

/// Add an item to the `vec`
pub fn add_item<V: Eq + Clone>(vec: &mut Vec<V>, item: &V) {
  if !vec.contains(item) {
    vec.push(item.clone());
  }
}

/// Remove an item from the `vec`
pub fn remove_item<V: Eq>(vec: &mut Vec<V>, item: &V) {
  if let Some(pos) = vec.iter().position(|x| x == item) {
    vec.remove(pos);
  }
}

// Map Utils

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

/// Looks up the `key` in the `map`, and returns the value, if it is present. Otherwise,
/// we insert a default constructed value at that `key` and return that.
pub fn default_get_mut<'a, K: Clone + Eq + Ord, V: Default>(
  map: &'a mut BTreeMap<K, V>,
  key: &K,
) -> &'a mut V {
  if !map.contains_key(key) {
    map.insert(key.clone(), V::default());
  }
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

/// Gets the key at the given index
pub fn read_index<K: Ord, V>(map: &BTreeMap<K, V>, idx: usize) -> Option<(&K, &V)> {
  let mut it = map.iter();
  for _ in 0..idx {
    it.next();
  }
  it.next()
}

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
      mvm.write(&col_name, Some(col_type), mk_t(1));
    }
    TableSchema { key_cols, val_cols: mvm }
  }

  pub fn get_key_col_refs(&self) -> Vec<proc::ColumnRef> {
    self
      .key_cols
      .iter()
      .cloned()
      .map(|(col_name, _)| proc::ColumnRef { table_name: None, col_name })
      .collect()
  }

  /// Gets the `ColNames` that are present at the `timestamp` in their canonical
  /// order. Importantly, this implies that Table columns have a well defined order
  /// (for every `Timestamp`).
  pub fn get_schema_static(&self, timestamp: &Timestamp) -> Vec<ColName> {
    let mut all_cols = Vec::<ColName>::new();
    for (col, _) in &self.key_cols {
      all_cols.push(col.clone());
    }
    for (col, _) in self.val_cols.static_snapshot_read(timestamp) {
      // In practice, a ColName should never be a ValCol if it is already a KeyCol.
      debug_assert!(!all_cols.contains(&col));
      add_item(&mut all_cols, &col);
    }
    all_cols
  }

  /// Gets the `ColNames` that are present at the `timestamp` in their canonical
  /// order. Importantly, this implies that Table columns have a well defined order
  /// (for every `Timestamp`).
  pub fn get_schema_val_cols_static(&self, timestamp: &Timestamp) -> Vec<ColName> {
    self.val_cols.static_snapshot_read(timestamp).into_keys().collect()
  }
}

// -------------------------------------------------------------------------------------------------
// Gossip
// -------------------------------------------------------------------------------------------------

/// Holds system Metadata that is Gossiped out from the Master to the Slaves. It is very
/// important, containing the database schema, Paxos configuration, etc.
///
/// Properties:
///   1. The set of keys in `db_schema` is equal to the key-value pairs in `table_generation`.
///   2. The set of keys in `db_schema` is equal to keys in `sharding_config`.
///   3. The `PrimaryKey`s in `TabletKeyRange` have the right schema according to `db_schema`.
///   4. Every `TabletGroupId` in `sharding_config` is a key in `tablet_address_config`.
///   5. Every `SlaveGroupId` in `tablet_address_config` is a key in `slave_address_config`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct GossipData {
  /// Database Schema
  gen: Gen,
  db_schema: BTreeMap<(TablePath, Gen), TableSchema>,
  table_generation: MVM<TablePath, Gen>,

  /// Distribution
  sharding_config: BTreeMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: BTreeMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
  master_address_config: Vec<EndpointId>,
}

impl GossipData {
  pub fn new(
    slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>>,
    master_address_config: Vec<EndpointId>,
  ) -> GossipData {
    GossipData {
      gen: Gen(0),
      db_schema: Default::default(),
      table_generation: MVM::new(),
      sharding_config: Default::default(),
      tablet_address_config: Default::default(),
      slave_address_config,
      master_address_config,
    }
  }

  /// This is the only way to modify a `GossipData` instance, which makes sure to
  /// increment the `gen` accordingly.
  pub fn update<T, F: FnOnce(GossipDataMutView) -> T>(&mut self, func: F) -> T {
    self.gen.inc();
    func(GossipDataMutView {
      db_schema: &mut self.db_schema,
      table_generation: &mut self.table_generation,
      sharding_config: &mut self.sharding_config,
      tablet_address_config: &mut self.tablet_address_config,
      slave_address_config: &mut self.slave_address_config,
      master_address_config: &mut self.master_address_config,
    })
  }

  pub fn get_gen(&self) -> &Gen {
    &self.gen
  }

  pub fn get(&self) -> GossipDataView {
    GossipDataView {
      db_schema: &self.db_schema,
      table_generation: &self.table_generation,
      sharding_config: &self.sharding_config,
      tablet_address_config: &self.tablet_address_config,
      slave_address_config: &self.slave_address_config,
      master_address_config: &self.master_address_config,
    }
  }
}

/// An immutable view of GossipData, useful for read-only access.
#[derive(Debug)]
pub struct GossipDataView<'a> {
  /// Database Schema
  pub db_schema: &'a BTreeMap<(TablePath, Gen), TableSchema>,
  pub table_generation: &'a MVM<TablePath, Gen>,

  /// Distribution
  pub sharding_config: &'a BTreeMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: &'a BTreeMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: &'a BTreeMap<SlaveGroupId, Vec<EndpointId>>,
  pub master_address_config: &'a Vec<EndpointId>,
}

/// A mutable view of GossipData, useful when we want to update it (and have `gen`) be
/// automatically updated.
#[derive(Debug)]
pub struct GossipDataMutView<'a> {
  /// Database Schema
  pub db_schema: &'a mut BTreeMap<(TablePath, Gen), TableSchema>,
  pub table_generation: &'a mut MVM<TablePath, Gen>,

  /// Distribution
  pub sharding_config: &'a mut BTreeMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: &'a mut BTreeMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: &'a mut BTreeMap<SlaveGroupId, Vec<EndpointId>>,
  pub master_address_config: &'a mut Vec<EndpointId>,
}

impl<'a> GossipDataMutView<'a> {
  /// A convenience function to convert this mutable container into the immutable one.
  pub fn get(&self) -> GossipDataView {
    GossipDataView {
      db_schema: &self.db_schema,
      table_generation: &self.table_generation,
      sharding_config: &self.sharding_config,
      tablet_address_config: &self.tablet_address_config,
      slave_address_config: &self.slave_address_config,
      master_address_config: &self.master_address_config,
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  LeaderMap
// -----------------------------------------------------------------------------------------------

/// This contains every `PaxosGroupId` in `GossipData` (i.e. all Slaves and the Master).
/// Inside of Slaves, recall that `GossipData` might not yet contain the Slave (this happens
/// when the SlaveGroup is freshly created). This `LeaderMap` will contain that as well.
pub type LeaderMap = BTreeMap<PaxosGroupId, LeadershipId>;

/// Amend the local LeaderMap to refect the new GossipData.
pub fn update_leader_map(
  leader_map: &mut VersionedValue<LeaderMap>,
  old_gossip: &GossipData,
  some_leader_map: &LeaderMap,
  new_gossip: &GossipData,
) {
  // Add new PaxosGroupIds
  for (sid, _) in new_gossip.get().slave_address_config {
    if !old_gossip.get().slave_address_config.contains_key(sid) {
      let gid = sid.to_gid();
      let lid = some_leader_map.get(&gid).unwrap().clone();
      leader_map.update(move |leader_map| {
        leader_map.insert(gid, lid);
      });
    }
  }

  // Remove old PaxosGroupIds
  for (sid, _) in old_gossip.get().slave_address_config {
    if !new_gossip.get().slave_address_config.contains_key(sid) {
      leader_map.update(move |leader_map| {
        leader_map.remove(&sid.to_gid());
      });
    }
  }
}

/// Amend the local LeaderMap to refect the new GossipData.
pub fn update_leader_map_unversioned(
  leader_map: &mut LeaderMap,
  old_gossip: &GossipData,
  some_leader_map: &LeaderMap,
  new_gossip: &GossipData,
) {
  // Add new PaxosGroupIds
  for (sid, _) in new_gossip.get().slave_address_config {
    if !old_gossip.get().slave_address_config.contains_key(sid) {
      let gid = sid.to_gid();
      let lid = some_leader_map.get(&gid).unwrap().clone();
      leader_map.insert(gid, lid);
    }
  }

  // Remove old PaxosGroupIds
  for (sid, _) in old_gossip.get().slave_address_config {
    if !new_gossip.get().slave_address_config.contains_key(sid) {
      leader_map.remove(&sid.to_gid());
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  All EndpointIds
// -----------------------------------------------------------------------------------------------

pub fn update_all_eids(
  all_eids: &mut VersionedValue<BTreeSet<EndpointId>>,
  rem_eids: &Vec<EndpointId>,
  new_eids: Vec<EndpointId>,
) {
  all_eids.update(|all_eids| {
    for rem_eid in rem_eids {
      all_eids.remove(rem_eid);
    }
    for new_eid in new_eids {
      all_eids.insert(new_eid);
    }
  });
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
//  Basic Utils
// -----------------------------------------------------------------------------------------------

fn rand_string<R: Rng>(rng: &mut R) -> String {
  rng.sample_iter(&Alphanumeric).take(12).map(char::from).collect()
}

pub fn mk_qid<R: Rng>(rng: &mut R) -> QueryId {
  QueryId(rand_string(rng))
}

pub fn mk_rid<R: Rng>(rng: &mut R) -> RequestId {
  RequestId(rand_string(rng))
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
  mut results: Vec<(Vec<Option<ColName>>, Vec<TableView>)>,
) -> Vec<TableView> {
  let mut it = results.into_iter();
  if let Some((schema, mut views)) = it.next() {
    while let Some((cur_schema, cur_views)) = it.next() {
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
    views
  } else {
    vec![]
  }
}

pub fn to_table_path(source: &proc::GeneralSource) -> &TablePath {
  cast!(proc::GeneralSourceRef::TablePath, &source.source_ref).unwrap()
}

/// An immutable value of type `T` with an associated version to easily tell
/// when it has been updated.
#[derive(Debug, Clone)]
pub struct VersionedValue<T> {
  gen: Gen,
  value: T,
}

impl<T> VersionedValue<T> {
  pub fn new(value: T) -> VersionedValue<T> {
    VersionedValue { gen: Gen(0), value }
  }

  pub fn set(&mut self, value: T) {
    self.gen.inc();
    self.value = value;
  }

  pub fn gen(&self) -> &Gen {
    &self.gen
  }

  pub fn value(&self) -> &T {
    &self.value
  }

  pub fn update<F: FnOnce(&mut T)>(&mut self, f: F) {
    f(&mut self.value);
    self.gen.inc();
  }
}

// -----------------------------------------------------------------------------------------------
//  Timestamp
// -----------------------------------------------------------------------------------------------

/// We use this type whenever we need to represent time. The `time_ms` is a time in
/// milliseconds. The `suffix` is often randomly generated to reduce the probability
/// of collisions. We can think of it as a decimal value, for arithemetic as well as
/// the ordering relation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
  pub time_ms: u128,
  pub suffix: u64,
}

impl Timestamp {
  pub fn new(time_ms: u128, suffix: u64) -> Timestamp {
    Timestamp { time_ms, suffix }
  }

  /// We use grade school addition.
  pub fn add(&self, that: Timestamp) -> Timestamp {
    let sum = self.suffix as u128 + that.suffix as u128;
    let new_suffix = sum as u64;
    let borrow = if sum > u64::MAX as u128 { 1 } else { 0 };
    Timestamp { time_ms: self.time_ms + that.time_ms + borrow, suffix: new_suffix }
  }
}

pub fn mk_t(time_ms: u128) -> Timestamp {
  Timestamp { time_ms, suffix: 0 }
}

/// Add some noise to the `Timestamp` returned by the clock to help avoid collisions
/// between transactions. Importantly, a server should not expect that consecutive calls
/// to `cur_timestamp` would result in non-decreasing `Timestamp`; only the `time_ms`
/// will be non-decreasing
pub fn cur_timestamp<IO: BasicIOCtx>(io_ctx: &mut IO, timestamp_suffix_divisor: u64) -> Timestamp {
  let mut now_timestamp = io_ctx.now();
  now_timestamp.suffix = io_ctx.rand().next_u64() % timestamp_suffix_divisor;
  now_timestamp
}

// -----------------------------------------------------------------------------------------------
//  Query Plan
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
  pub tier_map: TierMap,
  /// See the `compute_query_leader_map` in `QueryPlanningES`.
  pub query_leader_map: BTreeMap<SlaveGroupId, LeadershipId>,
  pub table_location_map: BTreeMap<TablePath, Gen>,
  /// These are additional required columns that the QueryPlan expects that these `TablePaths`
  /// to have. These are columns that are not already present in the `col_usage_node`, such as
  /// projected columns in SELECT queries or assigned columns in UPDATE queries. While a TP is
  /// happen is happening, we must verify the presence of these `ColName`.
  ///
  /// Note: not all `TablePaths` used in the MSQuery needs to be here.
  pub col_presence_req: BTreeMap<TablePath, ColPresenceReq>,
  pub col_usage_node: ColUsageNode,
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

/// Represents a ReadRegions in the Region Isolation Algorithm.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadRegion {
  pub row_region: Vec<KeyBound>,
  pub val_col_region: Vec<ColName>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WriteRegionType {
  FixedRowsVarCols { val_col_region: Vec<ColName> },
  VarRows,
}

/// Represents a ReadRegions in the Region Isolation Algorithm.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteRegion {
  pub row_region: Vec<KeyBound>,
  pub presence: bool,
  pub val_col_region: Vec<ColName>,
}

// -----------------------------------------------------------------------------------------------
//  Wrapper Utilities
// -----------------------------------------------------------------------------------------------

/// Contains the result of ESs like *Table*ES and TransTableReadES.
#[derive(Debug, Clone)]
pub struct QueryESResult {
  pub result: (Vec<Option<ColName>>, Vec<TableView>),
  pub new_rms: Vec<TQueryPath>,
}

// -------------------------------------------------------------------------------------------------
//  BoundType
// -------------------------------------------------------------------------------------------------

/// This trait is used to cast `ColVal` and `ColValN` values down to their underlying
/// types. This is necessary for expression evaluation.
///
/// NOTE: We need `Sized` since we return `Option<Self>`.
pub trait BoundType: Sized {
  fn col_val_cast(col_val: ColVal) -> Option<Self>;
  fn col_valn_cast(col_val: ColValN) -> Option<Self> {
    Self::col_val_cast(col_val?)
  }

  fn col_val_cast_ref(col_val: &ColVal) -> Option<&Self>;
  fn col_valn_cast_ref(col_val: &ColValN) -> Option<&Self> {
    Self::col_val_cast_ref(col_val.as_ref()?)
  }
}

/// `BoundType` for `i32`
impl BoundType for i32 {
  fn col_val_cast(col_val: ColVal) -> Option<Self> {
    if let ColVal::Int(val) = col_val {
      Some(val)
    } else {
      None
    }
  }

  fn col_val_cast_ref(col_val: &ColVal) -> Option<&Self> {
    if let ColVal::Int(val) = col_val {
      Some(val)
    } else {
      None
    }
  }
}

/// `BoundType` for `bool`
impl BoundType for bool {
  fn col_val_cast(col_val: ColVal) -> Option<Self> {
    if let ColVal::Bool(val) = col_val {
      Some(val)
    } else {
      None
    }
  }

  fn col_val_cast_ref(col_val: &ColVal) -> Option<&Self> {
    if let ColVal::Bool(val) = col_val {
      Some(val)
    } else {
      None
    }
  }
}

/// `BoundType` for `String`
impl BoundType for String {
  fn col_val_cast(col_val: ColVal) -> Option<Self> {
    if let ColVal::String(val) = col_val {
      Some(val)
    } else {
      None
    }
  }

  fn col_val_cast_ref(col_val: &ColVal) -> Option<&Self> {
    if let ColVal::String(val) = col_val {
      Some(val)
    } else {
      None
    }
  }
}
