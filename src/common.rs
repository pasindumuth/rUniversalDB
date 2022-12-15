use crate::coord::{CoordContext, CoordForwardMsg, CoordState};
use crate::expression::does_types_match;
use crate::master::MasterTimerInput;
use crate::master_query_planning_es::ColPresenceReq;
use crate::message as msg;
use crate::multiversion_map::MVM;
use crate::node::{GenericInput, GenericTimerInput};
use crate::server::{CTServerContext, CommonQuery};
use crate::slave::{SlaveBackMessage, SlaveTimerInput};
use crate::sql_ast::proc;
use crate::tablet::{TabletConfig, TabletContext, TabletForwardMsg, TabletSnapshot, TabletState};
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

#[path = "test/common_test.rs"]
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
  fn create_tablet(&mut self, tablet_ctx: TabletContext);
  /// Forwards `forward_msg` to the Tablet. This function returns an error if the Tablet
  /// does not exist, and with it, it will return the `forward_msg` that failed to be processed.
  fn tablet_forward(
    &mut self,
    tablet_group_id: &TabletGroupId,
    forward_msg: TabletForwardMsg,
  ) -> Result<(), TabletForwardMsg>;
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

pub const ALPHABET: &'static str = "abcdefghijklmnopqrstuvwxyz";

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

/// Generic function to call when an ES reaches an expected branch.
pub fn unexpected_branch<T>() -> Option<T> {
  debug_assert!(false);
  None
}

// -------------------------------------------------------------------------------------------------
//  ReadOnlySet
// -------------------------------------------------------------------------------------------------

pub trait ReadOnlySet<T> {
  fn contains(&self, val: &T) -> bool;
}

impl<K: Ord, V> ReadOnlySet<K> for BTreeMap<K, V> {
  fn contains(&self, val: &K) -> bool {
    self.contains_key(val)
  }
}

// -------------------------------------------------------------------------------------------------
//  Relational Tablet
// -------------------------------------------------------------------------------------------------

/// A global identifier of a Table (across tables and databases).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TablePath(pub String);

/// A global identifier of a TransTable (across tables and databases).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransTableName(pub String);

/// The types that the columns of a Relational Tablet can take on.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ColType {
  Int,
  Bool,
  String,
}

/// The values that the columns of a Relational Tablet can take on.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColVal {
  Int(i32),
  Bool(bool),
  String(String),
}

/// This is a nullable `ColVal`. We use this alias for self-documentation
pub type ColValN = Option<ColVal>;

/// The name of a column.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColName(pub String);

/// The Primary Key of a Relational Tablet. Note that we don't use
/// Vec<Option<ColValue>> because values of a key column can't be NULL.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrimaryKey {
  pub cols: Vec<ColVal>,
}

impl PrimaryKey {
  pub fn new(cols: Vec<ColVal>) -> PrimaryKey {
    PrimaryKey { cols }
  }
}

/// Represents a contiguous subset of keys in a Table. Here, if `start` or `end`
/// is present, then the `ColType` agrees with the first KeyCol, and they will
/// be used to partition the KeySpace by partitioning the first KeyCol.
/// Here, `start` is inclusive and `end` is exlusive. If either are `None`,
/// that side is unbounded.
///
/// NOTE: The reason we ceased to use `PrimaryKey` here was that when building
/// `(Read/Write)Region`s, slicing the `row_region` would be quite a bit more
/// inefficient. The only shortcoming with the below is that `PrimaryKey` that
/// start with a `bool` cannot be sharded well. One solution is to use `PrimaryKey`
/// prefixes (instead of the whole `PrimaryKey`). (Notice that this would not make
/// `row_region` slicing any more expensive if we continue to use the first KeyCol
/// most of the time).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TabletKeyRange {
  pub start: Option<ColVal>,
  pub end: Option<ColVal>,
}

impl TabletKeyRange {
  /// This function returns `false` when `range_key` false outside of `Self`.
  /// Importantly, the `ColType` of the `range_key` provided must be the same as
  /// `Self`. Recall that `Self` is inclusive of `start` and exclusive of `end`.
  pub fn contains(&self, range_key: &ColVal) -> bool {
    if let Some(start_key) = &self.start {
      if range_key < start_key {
        return false;
      }
    }
    if let Some(end_key) = &self.end {
      if end_key <= range_key {
        return false;
      }
    }
    true
  }

  /// This function returns `false` when `pkey` falls outside of `Self`. Importantly,
  /// the `ColType` of the first key in `pkey` (if it exists) must match that of `Self`.
  pub fn contains_pkey(&self, pkey: &PrimaryKey) -> bool {
    if let Some(first_key) = pkey.cols.first() {
      self.contains(first_key)
    } else {
      true
    }
  }

  /// This function returns `None` if this conversion fails (due to a type incompatibility).
  pub fn into_col_bound<T: BoundType>(self) -> Option<ColBound<T>> {
    match (self.start, self.end) {
      (None, None) => Some(ColBound { start: SingleBound::Unbounded, end: SingleBound::Unbounded }),
      (Some(start), None) => Some(ColBound {
        start: SingleBound::Included(T::col_val_cast(start)?),
        end: SingleBound::Unbounded,
      }),
      (None, Some(end)) => Some(ColBound {
        start: SingleBound::Unbounded,
        end: SingleBound::Excluded(T::col_val_cast(end)?),
      }),
      (Some(start), Some(end)) => Some(ColBound {
        start: SingleBound::Included(T::col_val_cast(start)?),
        end: SingleBound::Excluded(T::col_val_cast(end)?),
      }),
    }
  }
}

/// A Type used to represent a generation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct Gen(pub u64);

/// A Type used to represent a generation of a `sharding_config` for a Table.
pub type ShardingGen = Gen;

// -------------------------------------------------------------------------------------------------
//  Transaction Data Structures
// -------------------------------------------------------------------------------------------------

/// See `compute_all_tier_maps` to see how this is used in the `QueryPlan`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TierMap {
  pub map: BTreeMap<TablePath, u32>,
}

// -------------------------------------------------------------------------------------------------
//  ID Types
// -------------------------------------------------------------------------------------------------

/// This is used to indicate if an `EndpointId` is Internal or External. To facilitate
/// reconnection, we have a randomly generated `salt` as a part of `External`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InternalMode {
  Internal,
  External { salt: String },
}

/// A global identifer of a network node. This includes Slaves, Clients, Admin
/// clients, and other nodes in the network.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EndpointId {
  pub ip: String,
  /// Internal `EndpointId` are network endpoints that belong to processes within the
  /// system. In particular, these are the Slave Nodes, Master Nodes, Free Nodes, etc.
  /// Processes outside of the system includes the user. In practice, `mode` is
  /// used to decide whether to reconnect to an `EndpointId` once its connection goes down.
  /// We do not do this for Internal `EndpointId`s to preserve FIFO behavior.
  pub mode: InternalMode,
}

impl EndpointId {
  pub fn new(ip: String, is_internal: InternalMode) -> EndpointId {
    EndpointId { ip, mode: is_internal }
  }
}

/// A global identfier of a Tablet.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TabletGroupId(pub String);

/// A global identfier of a Tablet.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(pub String);

/// A global identfier of a Slave.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlaveGroupId(pub String);

/// A global identfier of a Coord.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CoordGroupId(pub String);

/// A request ID used to help the sender cancel the query if they wanted to.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(pub String);

// -------------------------------------------------------------------------------------------------
//  PaxosGroupIdTrait
// -------------------------------------------------------------------------------------------------

pub trait PaxosGroupIdTrait {
  fn to_gid(&self) -> PaxosGroupId;
}

impl PaxosGroupIdTrait for SlaveGroupId {
  fn to_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.clone())
  }
}

impl PaxosGroupIdTrait for TNodePath {
  fn to_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.sid.clone())
  }
}

impl PaxosGroupIdTrait for CNodePath {
  fn to_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.sid.clone())
  }
}

// -------------------------------------------------------------------------------------------------
//  QueryPath
// -------------------------------------------------------------------------------------------------

// SubNodePaths

// SubNodePaths point to a specific thread besides the main thread that is running in a
// server. Here, we define the `main` thread to be the one that messages come into.

/// `CTSubNodePath`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CTSubNodePath {
  Tablet(TabletGroupId),
  Coord(CoordGroupId),
}

pub trait IntoCTSubNodePath {
  fn into_ct(self) -> CTSubNodePath;
}

/// `TSubNodePath`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TSubNodePath {
  Tablet(TabletGroupId),
}

impl IntoCTSubNodePath for TSubNodePath {
  fn into_ct(self) -> CTSubNodePath {
    match self {
      TSubNodePath::Tablet(tid) => CTSubNodePath::Tablet(tid),
    }
  }
}

/// `CSubNodePath`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CSubNodePath {
  Coord(CoordGroupId),
}

impl IntoCTSubNodePath for CSubNodePath {
  fn into_ct(self) -> CTSubNodePath {
    match self {
      CSubNodePath::Coord(cid) => CTSubNodePath::Coord(cid),
    }
  }
}

// SlaveNodePath

/// `SlaveNodePath`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlaveNodePath<SubNodePathT> {
  pub sid: SlaveGroupId,
  pub sub: SubNodePathT,
}

impl<SubNodePathT: IntoCTSubNodePath> SlaveNodePath<SubNodePathT> {
  pub fn into_ct(self) -> CTNodePath {
    SlaveNodePath { sid: self.sid, sub: self.sub.into_ct() }
  }
}

/// `CTNodePath`
pub type CTNodePath = SlaveNodePath<CTSubNodePath>;

/// `TNodePath`
pub type TNodePath = SlaveNodePath<TSubNodePath>;

/// `CNodePath`
pub type CNodePath = SlaveNodePath<CSubNodePath>;

// QueryPaths

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlaveQueryPath<SubNodePathT> {
  pub node_path: SlaveNodePath<SubNodePathT>,
  pub query_id: QueryId,
}

impl<SubNodePathT: IntoCTSubNodePath> SlaveQueryPath<SubNodePathT> {
  pub fn into_ct(self) -> CTQueryPath {
    CTQueryPath {
      node_path: CTNodePath { sid: self.node_path.sid, sub: self.node_path.sub.into_ct() },
      query_id: self.query_id,
    }
  }
}

/// `CTQueryPath`
pub type CTQueryPath = SlaveQueryPath<CTSubNodePath>;

/// `TQueryPath`
pub type TQueryPath = SlaveQueryPath<TSubNodePath>;

/// `CQueryPath`
pub type CQueryPath = SlaveQueryPath<CSubNodePath>;

// Gen
impl Gen {
  pub fn next(&self) -> Gen {
    Gen(self.0 + 1)
  }

  pub fn inc(&mut self) {
    self.0 += 1;
  }
}

// -------------------------------------------------------------------------------------------------
//  Paxos
// -------------------------------------------------------------------------------------------------

/// Used to identify a Leadership in a given PaxosGroup.
/// The default total ordering is works, since `Gen` is compared first.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeadershipId {
  pub gen: Gen,
  pub eid: EndpointId,
}

impl LeadershipId {
  pub fn mk_first(eid: EndpointId) -> LeadershipId {
    LeadershipId { gen: Gen(0), eid }
  }
}

/// Used to identify a PaxosGroup in the system.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PaxosGroupId {
  Master,
  Slave(SlaveGroupId),
}

// -------------------------------------------------------------------------------------------------
//  Subquery Context
// -------------------------------------------------------------------------------------------------

/// `Context` to send subqueries
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Context {
  pub context_schema: ContextSchema,
  pub context_rows: Vec<ContextRow>,
}

impl Context {
  pub fn new(context_schema: ContextSchema) -> Context {
    Context { context_schema, context_rows: vec![] }
  }
}

/// The schema of the `Context`.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextSchema {
  pub column_context_schema: Vec<proc::ColumnRef>,
  pub trans_table_context_schema: Vec<TransTableLocationPrefix>,
}

impl ContextSchema {
  pub fn trans_table_names(&self) -> Vec<TransTableName> {
    self.trans_table_context_schema.iter().map(|prefix| prefix.trans_table_name.clone()).collect()
  }
}

/// A Row in the `Context`
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextRow {
  pub column_context_row: Vec<ColValN>,
  pub trans_table_context_row: Vec<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransTableLocationPrefix {
  pub source: CTQueryPath,
  pub trans_table_name: TransTableName,
}

// -----------------------------------------------------------------------------------------------
//  Table View
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableView {
  /// The keys are the rows, and the values are the number of repetitions.
  pub rows: BTreeMap<Vec<ColValN>, u64>,
}

impl TableView {
  pub fn new() -> TableView {
    TableView { rows: Default::default() }
  }

  pub fn add_row(&mut self, row: Vec<ColValN>) {
    self.add_row_multi(row, 1);
  }

  pub fn add_row_multi(&mut self, row: Vec<ColValN>, row_count: u64) {
    if let Some(count) = self.rows.get_mut(&row) {
      *count += row_count;
    } else if row_count > 0 {
      self.rows.insert(row, row_count);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Query Result
// -----------------------------------------------------------------------------------------------

/// This is a wrapper for `TableView`, except where the schema is attached.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
  pub schema: Vec<Option<ColName>>,
  pub data: TableView,
}

impl QueryResult {
  pub fn new(result_schema: Vec<Option<ColName>>) -> QueryResult {
    QueryResult { schema: result_schema, data: TableView::new() }
  }

  pub fn add_row(&mut self, row: Vec<ColValN>) {
    self.data.add_row(row);
  }
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

  pub fn get_key_col_refs(&self, table_name: &String) -> Vec<proc::ColumnRef> {
    self
      .key_cols
      .iter()
      .cloned()
      .map(|(col_name, _)| proc::ColumnRef { table_name: table_name.clone(), col_name })
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

  /// Checks whether this `range_key` can appear in a `TabletKeyRange` that is used
  /// to shard this `TableSchema`. Since this logic only depends on the `key_cols`, that
  /// means the results of this function does not change over time.
  pub fn is_valid_range_key(&self, range_key: &Option<ColVal>) -> bool {
    if let Some(range_key) = range_key {
      if let Some((_, first_key_type)) = &self.key_cols.first() {
        does_types_match(first_key_type, Some(range_key))
      } else {
        false
      }
    } else {
      true
    }
  }
}

// -------------------------------------------------------------------------------------------------
// Gossip
// -------------------------------------------------------------------------------------------------

/// This is the `TablePath` `Gen` combined with a particular `ShardingGen`.
pub type FullGen = (Gen, ShardingGen);

/// Holds system Metadata that is Gossiped out from the Master to the Slaves. It is very
/// important, containing the database schema, Paxos configuration, etc.
///
/// Properties:
///   1. The set of keys in `db_schema` is equal to the set key-value pairs in `table_generation`
///      where we remove the `ShardingGen` from the latter.
///   2. The set of keys in `db_schema` is equal to set of keys in `sharding_config`
///      where we remove the `ShardingGen` from the latter.
///   3. The `PrimaryKey`s in `TabletKeyRange` have the right schema according to `db_schema`.
///   4. Every `TabletGroupId` in `sharding_config` is a key in `tablet_address_config`.
///   5. Every `SlaveGroupId` in `tablet_address_config` is a key in `slave_address_config`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct GossipData {
  /// Database Schema
  gen: Gen,
  db_schema: BTreeMap<(TablePath, Gen), TableSchema>,
  table_generation: MVM<TablePath, FullGen>,

  /// Distribution
  sharding_config: BTreeMap<(TablePath, FullGen), Vec<(TabletKeyRange, TabletGroupId)>>,
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
  pub table_generation: &'a MVM<TablePath, FullGen>,

  /// Distribution
  pub sharding_config: &'a BTreeMap<(TablePath, FullGen), Vec<(TabletKeyRange, TabletGroupId)>>,
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
  pub table_generation: &'a mut MVM<TablePath, FullGen>,

  /// Distribution
  pub sharding_config: &'a mut BTreeMap<(TablePath, FullGen), Vec<(TabletKeyRange, TabletGroupId)>>,
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

pub fn rand_string<R: Rng>(rng: &mut R) -> String {
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

/// Here, every element of `results` has the same schema; all `Vec<ColValN>`s in
/// across all `TableView`s have the same number of elements and the column types correspond.
/// In addition all `Vec<TableView>`s have the same length. This function essentially
/// merges together corresponding `TableView`.
pub fn merge_table_views(mut results: Vec<Vec<TableView>>) -> Vec<TableView> {
  let mut it = results.into_iter();
  if let Some(mut views) = it.next() {
    while let Some(cur_views) = it.next() {
      assert_eq!(cur_views.len(), views.len());
      for (idx, cur_view) in cur_views.into_iter().enumerate() {
        let view = views.get_mut(idx).unwrap();
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
/// TODO: make this a method of the `BasicIOCtx`, where structs that implement
///  `BasicIOCTx` will contain the `timestmap_suffix_divisor` underneath.
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
  pub table_location_map: BTreeMap<TablePath, FullGen>,
  /// These are the presence/absence requirements of columns in TableSchemas that TP
  /// needs to verify and lock in order for the query to execute properly.
  ///
  /// Note: not all `TablePaths` used in the MSQuery needs to be here.
  pub col_presence_req: BTreeMap<TablePath, ColPresenceReq>,
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
  pub result: Vec<TableView>,
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

  fn from_poly(poly_col_bound: &PolyColBound) -> Option<&ColBound<Self>>;
  fn to_poly(col_bound: ColBound<Self>) -> PolyColBound;
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

  fn from_poly(poly_col_bound: &PolyColBound) -> Option<&ColBound<Self>> {
    if let PolyColBound::Int(col_bound) = poly_col_bound {
      Some(col_bound)
    } else {
      None
    }
  }

  fn to_poly(col_bound: ColBound<Self>) -> PolyColBound {
    PolyColBound::Int(col_bound)
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

  fn from_poly(poly_col_bound: &PolyColBound) -> Option<&ColBound<Self>> {
    if let PolyColBound::Bool(col_bound) = poly_col_bound {
      Some(col_bound)
    } else {
      None
    }
  }

  fn to_poly(col_bound: ColBound<Self>) -> PolyColBound {
    PolyColBound::Bool(col_bound)
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

  fn from_poly(poly_col_bound: &PolyColBound) -> Option<&ColBound<Self>> {
    if let PolyColBound::String(col_bound) = poly_col_bound {
      Some(col_bound)
    } else {
      None
    }
  }

  fn to_poly(col_bound: ColBound<Self>) -> PolyColBound {
    PolyColBound::String(col_bound)
  }
}
