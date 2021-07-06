use crate::col_usage::{
  collect_select_subqueries, collect_top_level_cols, collect_update_subqueries,
  node_external_trans_tables, nodes_external_trans_tables, ColUsagePlanner, FrozenColUsageNode,
};
use crate::common::{
  btree_multimap_insert, btree_multimap_remove, lookup, lookup_pos, map_insert, merge_table_views,
  mk_qid, GossipData, IOTypes, KeyBound, NetworkOut, OrigP, QueryPlan, TMStatus, TMWaitValue,
  TableRegion, TableSchema,
};
use crate::expression::{
  compress_row_region, compute_key_region, compute_poly_col_bounds, construct_cexpr,
  construct_kb_expr, does_intersect, evaluate_c_expr, is_true, CExpr, EvalError,
};
use crate::gr_query_es::{
  GRExecutionS, GRQueryAction, GRQueryConstructorView, GRQueryES, GRQueryPlan, ReadStage,
  SubqueryComputableSql,
};
use crate::model::common::{
  proc, ColType, ColValN, Context, ContextRow, ContextSchema, Gen, NodeGroupId, QueryPath,
  TableView, TierMap, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{
  ColName, ColVal, EndpointId, PrimaryKey, QueryId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::model::message::AbortedData;
use crate::multiversion_map::MVM;
use crate::server::{
  contains_col, evaluate_super_simple_select, evaluate_update, mk_eval_error, CommonQuery,
  ContextConstructor, LocalTable, ServerContext,
};
use crate::trans_read_es::{
  FullTransTableReadES, TransQueryReplanningES, TransQueryReplanningS, TransTableAction,
  TransTableSource,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;
use std::ops::{Add, Bound, Deref, Sub};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  CommonQueryReplanningES
// -----------------------------------------------------------------------------------------------

pub trait QueryReplanningSqlView {
  /// Get the projected Columns of the query.
  fn projected_cols(&self, table_schema: &TableSchema) -> Vec<ColName>;
  /// Get the TablePath that the query reads.
  fn table(&self) -> &TablePath;
  /// Get all expressions in the Query (in some deterministic order)
  fn exprs(&self) -> Vec<proc::ValExpr>;
  /// This converts the underlying SQL into an MSQueryStage, useful
  /// for sending it out over the network.
  fn ms_query_stage(&self) -> proc::MSQueryStage;
}

impl QueryReplanningSqlView for proc::SuperSimpleSelect {
  fn projected_cols(&self, _: &TableSchema) -> Vec<ColName> {
    return self.projection.clone();
  }

  fn table(&self) -> &TablePath {
    cast!(proc::TableRef::TablePath, &self.from).unwrap()
  }

  fn exprs(&self) -> Vec<proc::ValExpr> {
    vec![self.selection.clone()]
  }

  fn ms_query_stage(&self) -> proc::MSQueryStage {
    proc::MSQueryStage::SuperSimpleSelect(self.clone())
  }
}

impl QueryReplanningSqlView for proc::Update {
  fn projected_cols(&self, table_schema: &TableSchema) -> Vec<ColName> {
    let mut projected_cols = Vec::from_iter(self.assignment.iter().map(|(col, _)| col.clone()));
    projected_cols.extend(table_schema.get_key_cols());
    return projected_cols;
  }

  fn table(&self) -> &TablePath {
    &self.table
  }

  fn exprs(&self) -> Vec<proc::ValExpr> {
    let mut exprs = Vec::new();
    exprs.push(self.selection.clone());
    for (_, expr) in &self.assignment {
      exprs.push(expr.clone());
    }
    exprs
  }

  fn ms_query_stage(&self) -> proc::MSQueryStage {
    proc::MSQueryStage::Update(self.clone())
  }
}

#[derive(Debug)]
enum CommonQueryReplanningS {
  Start,
  /// Used to lock the columsn in the SELECT clause or SET clause.
  ProjectedColumnLocking {
    locked_columns_query_id: QueryId,
  },
  /// Used to lock the query plan's columns
  ColumnLocking {
    locked_columns_query_id: QueryId,
  },
  /// Used to lock the query plan's columns after recomputation.
  RecomputeQueryPlan {
    locked_columns_query_id: QueryId,
    // The `ColName`s being locked. This is a minor optimization to save a few computations.
    locking_cols: Vec<ColName>,
  },
  /// Used to wait on the master
  MasterQueryReplanning {
    master_query_id: QueryId,
  },
  Done(bool),
}

#[derive(Debug)]
struct CommonQueryReplanningES<T: QueryReplanningSqlView> {
  // These members are parallel to the messages in `msg::GeneralQuery`.
  pub timestamp: Timestamp,
  pub context: Rc<Context>,
  pub sql_view: T,
  pub query_plan: QueryPlan,

  /// Path of the original sender (needed for responding with errors).
  pub sender_path: QueryPath,
  /// The OrigP of the Task holding this CommonQueryReplanningES
  pub query_id: QueryId,
  /// The state of the CommonQueryReplanningES
  pub state: CommonQueryReplanningS,
}

// -----------------------------------------------------------------------------------------------
//  SubqueryStatus
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct SubqueryLockingSchemas {
  // Recall that we only get to this State if a Subquery had failed. We hold onto
  // the prior ColNames and TransTableNames (rather than computating from the QueryPlan
  // again) so that we don't potentially lose prior ColName amendments.
  old_columns: Vec<ColName>,
  trans_table_names: Vec<TransTableName>,
  new_cols: Vec<ColName>,
  query_id: QueryId,
}

#[derive(Debug)]
pub struct SubqueryPendingReadRegion {
  new_columns: Vec<ColName>,
  trans_table_names: Vec<TransTableName>,
  read_region: TableRegion,
  query_id: QueryId,
}

#[derive(Debug)]
pub struct SubqueryPending {
  pub context: Rc<Context>,
  pub query_id: QueryId,
}

#[derive(Debug)]
pub struct SubqueryFinished {
  pub context: Rc<Context>,
  pub result: Vec<TableView>,
}

#[derive(Debug)]
pub enum SingleSubqueryStatus {
  LockingSchemas(SubqueryLockingSchemas),
  PendingReadRegion(SubqueryPendingReadRegion),
  Pending(SubqueryPending),
  Finished(SubqueryFinished),
}

// -----------------------------------------------------------------------------------------------
//  TableReadES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
struct Pending {
  read_region: TableRegion,
  query_id: QueryId,
}

#[derive(Debug)]
pub struct Executing {
  pub completed: usize,
  /// Here, the position of every SingleSubqueryStatus corresponds to the position
  /// of the subquery in the SQL query.
  pub subqueries: Vec<SingleSubqueryStatus>,
  /// We remember the row_region we had computed previously. If we have to protected
  /// more ReadRegions due to InternalColumnsDNEs, the `row_region` will be the same.
  pub row_region: Vec<KeyBound>,
}

impl Executing {
  pub fn find_subquery(&self, qid: &QueryId) -> Option<usize> {
    for i in 1..self.subqueries.len() {
      match &self.subqueries.get(i).unwrap() {
        SingleSubqueryStatus::LockingSchemas(SubqueryLockingSchemas { query_id, .. })
        | SingleSubqueryStatus::PendingReadRegion(SubqueryPendingReadRegion { query_id, .. })
        | SingleSubqueryStatus::Pending(SubqueryPending { query_id, .. }) => {
          if query_id == qid {
            return Some(i);
          }
        }
        SingleSubqueryStatus::Finished(_) => {}
      }
    }
    None
  }
}

#[derive(Debug)]
enum ExecutionS {
  Start,
  Pending(Pending),
  Executing(Executing),
}

#[derive(Debug)]
struct TableReadES {
  root_query_path: QueryPath,
  tier_map: TierMap,
  timestamp: Timestamp,
  context: Rc<Context>,

  // Fields needed for responding.
  sender_path: QueryPath,
  query_id: QueryId,

  // Query-related fields.
  sql_query: proc::SuperSimpleSelect,
  query_plan: QueryPlan,

  // Dynamically evolving fields.
  new_rms: HashSet<QueryPath>,
  state: ExecutionS,
}

#[derive(Debug)]
struct QueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to TableReadES.
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES<proc::SuperSimpleSelect>,
}

#[derive(Debug)]
enum FullTableReadES {
  QueryReplanning(QueryReplanningES),
  Executing(TableReadES),
}

// -----------------------------------------------------------------------------------------------
//  MSQueryES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct MSQueryES {
  query_id: QueryId,
  timestamp: Timestamp,
  update_views: BTreeMap<u32, GenericTable>,
  pending_queries: HashSet<QueryId>,
}

// -----------------------------------------------------------------------------------------------
//  MSTableWriteES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
enum MSWriteExecutionS {
  Start,
  Pending(Pending),
  Executing(Executing),
}

#[derive(Debug)]
struct MSTableWriteES {
  root_query_path: QueryPath,
  tier_map: TierMap,
  timestamp: Timestamp,
  tier: u32,
  context: Rc<Context>,

  // Fields needed for responding.
  sender_path: QueryPath,
  query_id: QueryId,

  // Query-related fields.
  sql_query: proc::Update,
  query_plan: QueryPlan,

  // MSQuery fields
  ms_query_id: QueryId,

  // Dynamically evolving fields.
  new_rms: HashSet<QueryPath>,
  state: MSWriteExecutionS,
}

#[derive(Debug)]
struct MSWriteQueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to MSTableWriteES.
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  pub ms_query_id: QueryId,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES<proc::Update>,
}

#[derive(Debug)]
enum FullMSTableWriteES {
  QueryReplanning(MSWriteQueryReplanningES),
  Executing(MSTableWriteES),
}

impl FullMSTableWriteES {
  fn ms_query_id(&self) -> &QueryId {
    match self {
      Self::QueryReplanning(es) => &es.ms_query_id,
      Self::Executing(es) => &es.ms_query_id,
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  MSTableReadES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
enum MSReadExecutionS {
  Start,
  Pending(Pending),
  Executing(Executing),
}

#[derive(Debug)]
struct MSTableReadES {
  root_query_path: QueryPath,
  tier_map: TierMap,
  timestamp: Timestamp,
  tier: u32,
  context: Rc<Context>,

  // Fields needed for responding.
  sender_path: QueryPath,
  query_id: QueryId,

  // Query-related fields.
  sql_query: proc::SuperSimpleSelect,
  query_plan: QueryPlan,

  // MSQuery fields
  ms_query_id: QueryId,

  // Dynamically evolving fields.
  new_rms: HashSet<QueryPath>,
  state: MSReadExecutionS,
}

#[derive(Debug)]
struct MSReadQueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to MSTableReadES.
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  pub ms_query_id: QueryId,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES<proc::SuperSimpleSelect>,
}

#[derive(Debug)]
enum FullMSTableReadES {
  QueryReplanning(MSReadQueryReplanningES),
  Executing(MSTableReadES),
}

impl FullMSTableReadES {
  fn ms_query_id(&self) -> &QueryId {
    match self {
      Self::QueryReplanning(es) => &es.ms_query_id,
      Self::Executing(es) => &es.ms_query_id,
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  ReadStatus
// -----------------------------------------------------------------------------------------------

/// This contains every TabletStatus. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  gr_query_ess: HashMap<QueryId, GRQueryES>,
  full_table_read_ess: HashMap<QueryId, FullTableReadES>,
  full_trans_table_read_ess: HashMap<QueryId, FullTransTableReadES>,
  tm_statuss: HashMap<QueryId, TMStatus>,
  ms_query_ess: HashMap<QueryId, MSQueryES>,
  full_ms_table_read_ess: HashMap<QueryId, FullMSTableReadES>,
  full_ms_table_write_ess: HashMap<QueryId, FullMSTableWriteES>,
}

impl Statuses {
  // A convenient polymorphic remove function.
  fn remove(&mut self, query_id: &QueryId) {
    if self.gr_query_ess.remove(query_id).is_some() {
      return;
    };
    if self.full_table_read_ess.remove(query_id).is_some() {
      return;
    };
    if self.full_trans_table_read_ess.remove(query_id).is_some() {
      return;
    };
    if self.tm_statuss.remove(query_id).is_some() {
      return;
    };
    if self.ms_query_ess.remove(query_id).is_some() {
      return;
    };
    if self.full_ms_table_read_ess.remove(query_id).is_some() {
      return;
    };
    if self.full_ms_table_write_ess.remove(query_id).is_some() {
      return;
    };
  }
}

// -----------------------------------------------------------------------------------------------
//  Region Isolation Algorithm
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct VerifyingReadWriteRegion {
  orig_p: OrigP,
  m_waiting_read_protected: BTreeSet<(OrigP, QueryId, TableRegion)>,
  m_read_protected: BTreeSet<TableRegion>,
  m_write_protected: BTreeSet<TableRegion>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ReadWriteRegion {
  orig_p: OrigP,
  m_read_protected: BTreeSet<TableRegion>,
  m_write_protected: BTreeSet<TableRegion>,
}

// -----------------------------------------------------------------------------------------------
//  Internal Communication
// -----------------------------------------------------------------------------------------------
// These enums are used for communication between algorithms.

// TODO: sync the `TableRegion`.
enum ReadProtectionGrant {
  Read { timestamp: Timestamp, protect_request: (OrigP, QueryId, TableRegion) },
  MRead { timestamp: Timestamp, protect_request: (OrigP, QueryId, TableRegion) },
  DeadlockSafetyReadAbort { timestamp: Timestamp, protect_request: (OrigP, QueryId, TableRegion) },
  DeadlockSafetyWriteAbort { timestamp: Timestamp, orig_p: OrigP },
}

// -----------------------------------------------------------------------------------------------
//  Storage
// -----------------------------------------------------------------------------------------------

/// A trait for reading subtables from some kind of underlying table data view. The `key_region`
/// indicates the set of rows to read from the Table, and the `col_region` are the columns to read.
///
/// This function must a pure function; the order of the returned vectors matter.
trait StorageView {
  fn compute_subtable(
    &self,
    key_region: &Vec<KeyBound>,
    col_region: &Vec<ColName>,
  ) -> (Vec<ColName>, Vec<(Vec<ColValN>, u64)>);
}

/// This is used to directly read data from persistent storage.
struct SimpleStorageView<'a> {
  storage: &'a GenericMVTable,
}

impl<'a> SimpleStorageView<'a> {
  fn new(storage: &GenericMVTable) -> SimpleStorageView {
    SimpleStorageView { storage }
  }
}

impl<'a> StorageView for SimpleStorageView<'a> {
  fn compute_subtable(
    &self,
    key_region: &Vec<KeyBound>,
    column_region: &Vec<ColName>,
  ) -> (Vec<ColName>, Vec<(Vec<ColValN>, u64)>) {
    // TODO: We can probably change the key type used in the GenericMVTable so that we
    // can do range querys in BTreeMaps, where we can directly use the KeyBound bounds
    // (inclusive, exclusive, and unbounded bounds) to get what we need directly. This
    // is an implementation detail.
    unimplemented!()
  }
}

/// This reads data by first replaying the Update Views on top of the persistant data.
struct MSStorageView<'a> {
  storage: &'a GenericMVTable,
  update_views: &'a BTreeMap<u32, GenericTable>,
  tier: u32,
}

impl<'a> MSStorageView<'a> {
  /// Here, in addition to the persistent storage, we pass in the `update_views` that
  /// we want to apply on top before reading data, and a `tier` to indicate up to which
  /// update should be applied.
  fn new(
    storage: &'a GenericMVTable,
    update_views: &'a BTreeMap<u32, GenericTable>,
    tier: u32,
  ) -> MSStorageView<'a> {
    MSStorageView { storage, update_views, tier }
  }
}

impl<'a> StorageView for MSStorageView<'a> {
  fn compute_subtable(
    &self,
    key_region: &Vec<KeyBound>,
    column_region: &Vec<ColName>,
  ) -> (Vec<ColName>, Vec<(Vec<ColValN>, u64)>) {
    unimplemented!()
  }
}

// -----------------------------------------------------------------------------------------------
//  StorageLocalTable
// -----------------------------------------------------------------------------------------------

struct StorageLocalTable<'a, StorageViewT: StorageView> {
  table_schema: &'a TableSchema,
  /// The Timestamp which we are reading data at.
  timestamp: &'a Timestamp,
  /// The row-filtering expression (i.e. WHERE clause) to compute subtables with.
  selection: &'a proc::ValExpr,
  /// This is used to compute the KeyBound
  storage: StorageViewT,
}

impl<'a, StorageViewT: StorageView> StorageLocalTable<'a, StorageViewT> {
  fn new(
    table_schema: &'a TableSchema,
    timestamp: &'a Timestamp,
    selection: &'a proc::ValExpr,
    storage: StorageViewT,
  ) -> StorageLocalTable<'a, StorageViewT> {
    StorageLocalTable { table_schema, timestamp, selection, storage }
  }
}

impl<'a, StorageViewT: StorageView> LocalTable for StorageLocalTable<'a, StorageViewT> {
  fn contains_col(&self, col: &ColName) -> bool {
    contains_col(self.table_schema, col, self.timestamp)
  }

  fn get_rows(
    &self,
    parent_context_schema: &ContextSchema,
    parent_context_row: &ContextRow,
    col_names: &Vec<ColName>,
  ) -> Result<Vec<(Vec<ColValN>, u64)>, EvalError> {
    // We extract all `ColNames` in `parent_context_schema` that aren't shadowed by the LocalTable,
    // and then map them to their values in `parent_context_row`.
    let mut col_map = HashMap::<ColName, ColValN>::new();
    let context_col_names = &parent_context_schema.column_context_schema;
    let context_col_vals = &parent_context_row.column_context_row;
    for i in 0..context_col_names.len() {
      if !self.contains_col(context_col_names.get(i).unwrap()) {
        col_map.insert(
          context_col_names.get(i).unwrap().clone(),
          context_col_vals.get(i).unwrap().clone(),
        );
      }
    }

    // Compute the KeyBound, the subtable, and return it.
    let key_bounds = compute_key_region(&self.selection, col_map, &self.table_schema.key_cols)?;
    let (_, subtable) = self.storage.compute_subtable(&key_bounds, col_names);
    Ok(subtable)
  }
}

// -----------------------------------------------------------------------------------------------
//  Tablet State
// -----------------------------------------------------------------------------------------------

// The best method is to have a method for every transition out of a state in an ES.
// This function will digs down from the root. The function will make decision
// on how to best short-circuit to other states, or even finish/respond to the client.
// We refer to the code that short-circuits from state A to B as the "A to B Transition Logic".
// Note that if we Immediately transition to another state, then we have re-lookup the ES
// from the root. In the future, if this lookup is too expensive, we can create
// a shared auxiliary function called by both. It will have many args.
// ...
// The big issue with the above solution is that Transition Logic will often be common
// when transition to some State (regardless of what the prior state was). We have to
// pull this logic into their own functions, as usual, but I think the right approach will
// be to first write out all Transition Logics, and then pull out common parts later.
// It's good to generally have a Start State to instatiate things like `CommonQueryReplanningES`,
// Since I don't want to duplicate the logic for instiating it in when first constructing
// the FullTableReadES. If we construct from outside in at the top, then we should be good.
//
// What if driving the GRQuery fails? We have these valid states of the system. While
// one ES is being modified, no other ES should be in an invalid state. This can happen
// if one ES is trying to create another ES, and simply puses it's own execution to start
// executing the new ES. Instead, we should define our ESs in a way that's default constructible.
// Thus, we finish modifying the parent ES + add in the new ESs into `read_statuses`, which gets
// us back to a valid state, and then we can execute the new ESs one by one. Note that the
// execution of these ESs are still driven from code that modified the parent (i.e. not from
// the main loop or an incoming message). This is totally okay.

// We also can't do the "shared auxiliary function" part, because the code we pull out
// will call functions that use `self`.
//
// Alternatively, we can stop trying to short-circuit forward and just always move onto
// the next state. Design is such a way that that's possible. Use the Main Loop.

// Naming: ES for structs, S for enums, names consistent with type (potentially shorter),
// perform_query, query, sql_query.
// `query_id` for main Query's one in question.
// We use "Main" to prefix to distinguish it from a child that we are trying to build (i.e.
// the "Main" Context).

// Every method call to a deep object will be driven from the top here, and all data
// will be looked up and passed in as references. This allows for many different
// sub-objects to have (potentially mutable) access to other sub-objects.

// The sender has to send the full SenderPath. The responder
// just needs to send the SenderStatePath, since it will use the
// other data to figure out the node and back that into the NetworkMessage
// separately.

type GenericMVTable = MVM<(PrimaryKey, Option<ColName>), ColValN>;
type GenericTable = HashMap<(PrimaryKey, Option<ColName>), ColValN>;

#[derive(Debug)]
pub struct TabletState<T: IOTypes> {
  tablet_context: TabletContext<T>,
  statuses: Statuses,
}

#[derive(Debug)]
pub struct TabletContext<T: IOTypes> {
  /// IO Objects.
  rand: T::RngCoreT,
  clock: T::ClockT,
  network_output: T::NetworkOutT,

  /// Metadata
  this_slave_group_id: SlaveGroupId,
  this_tablet_group_id: TabletGroupId,
  master_eid: EndpointId,

  /// Gossip
  gossip: Arc<GossipData>,

  /// Distribution
  sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  // Storage
  storage: GenericMVTable,
  this_table_path: TablePath,
  this_table_key_range: TabletKeyRange,
  table_schema: TableSchema,

  // Region Isolation Algorithm
  verifying_writes: BTreeMap<Timestamp, VerifyingReadWriteRegion>,
  prepared_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  committed_writes: BTreeMap<Timestamp, ReadWriteRegion>,

  // TODO: make (OrigP, QueryId, TableRegion) into a proper type.
  waiting_read_protected: BTreeMap<Timestamp, BTreeSet<(OrigP, QueryId, TableRegion)>>,
  read_protected: BTreeMap<Timestamp, BTreeSet<TableRegion>>,

  // Schema Change and Locking
  prepared_schema_change: BTreeMap<Timestamp, HashMap<ColName, Option<ColType>>>,
  request_index: BTreeMap<Timestamp, BTreeSet<QueryId>>, // Used to help iterate requests in order.
  requested_locked_columns: HashMap<QueryId, (OrigP, Timestamp, Vec<ColName>)>,

  /// Child Queries
  ms_root_query_map: HashMap<QueryId, QueryId>,
  master_query_map: HashMap<QueryId, OrigP>,
}

impl<T: IOTypes> TabletState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    gossip: Arc<GossipData>,
    sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, EndpointId>,
    this_slave_group_id: SlaveGroupId,
    this_tablet_group_id: TabletGroupId,
    master_eid: EndpointId,
  ) -> TabletState<T> {
    let (this_table_path, this_table_key_range) = (|| {
      // Search the sharding config, which should contain this data.
      for (path, shards) in &sharding_config {
        for (key_range, tid) in shards {
          if tid == &this_tablet_group_id {
            return (path.clone(), key_range.clone());
          }
        }
      }
      panic!();
    })();
    let table_schema = gossip.gossiped_db_schema.get(&this_table_path).unwrap().clone();
    TabletState {
      tablet_context: TabletContext::<T> {
        rand,
        clock,
        network_output,
        this_slave_group_id,
        this_tablet_group_id,
        master_eid,
        gossip,
        sharding_config,
        tablet_address_config,
        slave_address_config,
        storage: GenericMVTable::new(),
        this_table_path,
        this_table_key_range,
        table_schema,
        verifying_writes: Default::default(),
        prepared_writes: Default::default(),
        committed_writes: Default::default(),
        waiting_read_protected: Default::default(),
        read_protected: Default::default(),
        prepared_schema_change: Default::default(),
        request_index: Default::default(),
        requested_locked_columns: Default::default(),
        ms_root_query_map: Default::default(),
        master_query_map: Default::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::TabletMessage) {
    self.tablet_context.handle_incoming_message(&mut self.statuses, message);
  }
}

impl<T: IOTypes> TabletContext<T> {
  fn ctx(&mut self) -> ServerContext<T> {
    ServerContext {
      rand: &mut self.rand,
      clock: &mut self.clock,
      network_output: &mut self.network_output,
      this_slave_group_id: &self.this_slave_group_id,
      maybe_this_tablet_group_id: Some(&self.this_tablet_group_id),
      master_eid: &self.master_eid,
      gossip: &mut self.gossip,
      sharding_config: &mut self.sharding_config,
      tablet_address_config: &mut self.tablet_address_config,
      slave_address_config: &mut self.slave_address_config,
      master_query_map: &mut self.master_query_map,
    }
  }

  fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::TabletMessage) {
    match message {
      msg::TabletMessage::PerformQuery(perform_query) => {
        match perform_query.query {
          msg::GeneralQuery::SuperSimpleTransTableSelectQuery(query) => {
            // First, we check if the GRQueryES still exists in the Statuses, continuing
            // if so and aborting if not.
            if let Some(gr_query_es) = statuses.gr_query_ess.get(&query.location_prefix.query_id) {
              // Construct and start the TransQueryReplanningES
              let full_trans_table_es = map_insert(
                &mut statuses.full_trans_table_read_ess,
                &perform_query.query_id,
                FullTransTableReadES::QueryReplanning(TransQueryReplanningES {
                  root_query_path: perform_query.root_query_path,
                  tier_map: perform_query.tier_map,
                  query_id: perform_query.query_id.clone(),
                  location_prefix: query.location_prefix,
                  context: Rc::new(query.context),
                  sql_query: query.sql_query,
                  query_plan: query.query_plan,
                  sender_path: perform_query.sender_path,
                  orig_p: OrigP::new(perform_query.query_id.clone()),
                  state: TransQueryReplanningS::Start,
                  timestamp: gr_query_es.timestamp.clone(),
                }),
              );

              let action = full_trans_table_es.start(&mut self.ctx(), gr_query_es);
              self.handle_trans_es_action(statuses, perform_query.query_id, action);
            } else {
              // This means that the target GRQueryES was deleted, so we send back
              // an Abort with LateralError.
              self.ctx().send_query_error(
                perform_query.sender_path,
                perform_query.query_id,
                msg::QueryError::LateralError,
              );
              return;
            }
          }
          msg::GeneralQuery::SuperSimpleTableSelectQuery(query) => {
            // We inspect the TierMap to see what kind of ES to create
            let table_path = cast!(proc::TableRef::TablePath, &query.sql_query.from).unwrap();
            if perform_query.tier_map.map.contains_key(table_path) {
              // Here, we create an MSTableReadQueryES.

              // Lookup the MSQueryES
              let root_query_id = perform_query.root_query_path.query_id.clone();
              if !self.ms_root_query_map.contains_key(&root_query_id) {
                // If it doesn't exist, we try adding one. Of course, we must first
                // check whether the Timestamp is available or not.
                let timestamp = query.timestamp;
                if self.verifying_writes.contains_key(&timestamp)
                  || self.prepared_writes.contains_key(&timestamp)
                  || self.committed_writes.contains_key(&timestamp)
                {
                  // This means the Timestamp is already in use, so we have to Abort.
                  self.ctx().send_query_error(
                    perform_query.sender_path,
                    perform_query.query_id,
                    msg::QueryError::TimestampConflict,
                  );
                  return;
                } else {
                  // This means that we can add an MSQueryES at the Timestamp
                  let ms_query_id = mk_qid(&mut self.rand);
                  statuses.ms_query_ess.insert(
                    ms_query_id.clone(),
                    MSQueryES {
                      query_id: ms_query_id.clone(),
                      timestamp: query.timestamp,
                      update_views: Default::default(),
                      pending_queries: Default::default(),
                    },
                  );

                  // We also amend the `ms_root_query_map` to associate the root query.
                  self.ms_root_query_map.insert(root_query_id.clone(), ms_query_id.clone());

                  // Send a register message back to the root.
                  let register_query = msg::RegisterQuery {
                    root_query_id: root_query_id.clone(),
                    query_path: QueryPath {
                      slave_group_id: self.this_slave_group_id.clone(),
                      maybe_tablet_group_id: Some(self.this_tablet_group_id.clone()),
                      query_id: ms_query_id,
                    },
                  };
                  let sid = perform_query.sender_path.slave_group_id.clone();
                  let eid = self.slave_address_config.get(&sid).unwrap();
                  self.network_output.send(
                    eid,
                    msg::NetworkMessage::Slave(msg::SlaveMessage::RegisterQuery(register_query)),
                  );

                  // Finally, add an empty VerifyingReadWriteRegion
                  self.verifying_writes.insert(
                    timestamp,
                    VerifyingReadWriteRegion {
                      orig_p: OrigP::new(perform_query.query_id.clone()),
                      m_waiting_read_protected: BTreeSet::new(),
                      m_read_protected: BTreeSet::new(),
                      m_write_protected: BTreeSet::new(),
                    },
                  );
                }
              }

              // Lookup the MSQuery and add the QueryId of the new Query into `pending_queries`.
              let ms_query_id = self.ms_root_query_map.get(&root_query_id).unwrap().clone();
              let ms_query = statuses.ms_query_ess.get_mut(&ms_query_id).unwrap();
              ms_query.pending_queries.insert(perform_query.query_id.clone());

              // Create an MSWriteTableES in the QueryReplanning state, and add it to
              // the MSQueryES.
              let mut comm_plan_es = CommonQueryReplanningES {
                timestamp: query.timestamp,
                context: Rc::new(query.context),
                sql_view: query.sql_query.clone(),
                query_plan: query.query_plan,
                sender_path: perform_query.sender_path,
                query_id: perform_query.query_id.clone(),
                state: CommonQueryReplanningS::Start,
              };
              comm_plan_es.start::<T>(self);
              statuses.full_ms_table_read_ess.insert(
                perform_query.query_id.clone(),
                FullMSTableReadES::QueryReplanning(MSReadQueryReplanningES {
                  root_query_path: perform_query.root_query_path,
                  tier_map: perform_query.tier_map,
                  ms_query_id,
                  status: comm_plan_es,
                }),
              );
            } else {
              let mut comm_plan_es = CommonQueryReplanningES {
                timestamp: query.timestamp,
                context: Rc::new(query.context),
                sql_view: query.sql_query.clone(),
                query_plan: query.query_plan,
                sender_path: perform_query.sender_path,
                query_id: perform_query.query_id.clone(),
                state: CommonQueryReplanningS::Start,
              };
              comm_plan_es.start::<T>(self);
              // Add ReadStatus
              statuses.full_table_read_ess.insert(
                perform_query.query_id.clone(),
                FullTableReadES::QueryReplanning(QueryReplanningES {
                  root_query_path: perform_query.root_query_path,
                  tier_map: perform_query.tier_map,
                  status: comm_plan_es,
                }),
              );
            }
          }
          msg::GeneralQuery::UpdateQuery(query) => {
            // We first do some basic verification of the SQL query, namely assert that the
            // assigned columns aren't key columns.e
            for (col, _) in &query.sql_query.assignment {
              assert!(!lookup_pos(&self.table_schema.key_cols, col).is_some())
            }

            // Lookup the MSQueryES
            let root_query_id = perform_query.root_query_path.query_id.clone();
            if !self.ms_root_query_map.contains_key(&root_query_id) {
              // If it doesn't exist, we try adding one. Of course, we must first
              // check whether the Timestamp is available or not.
              let timestamp = query.timestamp;
              if self.verifying_writes.contains_key(&timestamp)
                || self.prepared_writes.contains_key(&timestamp)
                || self.committed_writes.contains_key(&timestamp)
              {
                // This means the Timestamp is already in use, so we have to Abort.
                self.ctx().send_query_error(
                  perform_query.sender_path,
                  perform_query.query_id,
                  msg::QueryError::TimestampConflict,
                );
                return;
              } else {
                // This means that we can add an MSQueryES at the Timestamp
                let ms_query_id = mk_qid(&mut self.rand);
                statuses.ms_query_ess.insert(
                  ms_query_id.clone(),
                  MSQueryES {
                    query_id: ms_query_id.clone(),
                    timestamp: query.timestamp,
                    update_views: Default::default(),
                    pending_queries: Default::default(),
                  },
                );

                // We also amend the `ms_root_query_map` to associate the root query.
                self.ms_root_query_map.insert(root_query_id.clone(), ms_query_id.clone());

                // Send a register message back to the root.
                let register_query = msg::RegisterQuery {
                  root_query_id: root_query_id.clone(),
                  query_path: QueryPath {
                    slave_group_id: self.this_slave_group_id.clone(),
                    maybe_tablet_group_id: Some(self.this_tablet_group_id.clone()),
                    query_id: ms_query_id,
                  },
                };
                let sid = perform_query.sender_path.slave_group_id.clone();
                let eid = self.slave_address_config.get(&sid).unwrap();
                self.network_output.send(
                  eid,
                  msg::NetworkMessage::Slave(msg::SlaveMessage::RegisterQuery(register_query)),
                );

                // Finally, add an empty VerifyingReadWriteRegion
                self.verifying_writes.insert(
                  timestamp,
                  VerifyingReadWriteRegion {
                    orig_p: OrigP::new(perform_query.query_id.clone()),
                    m_waiting_read_protected: BTreeSet::new(),
                    m_read_protected: BTreeSet::new(),
                    m_write_protected: BTreeSet::new(),
                  },
                );
              }
            }

            // Lookup the MSQuery and add the QueryId of the new Query into `pending_queries`.
            let ms_query_id = self.ms_root_query_map.get(&root_query_id).unwrap().clone();
            let ms_query = statuses.ms_query_ess.get_mut(&ms_query_id).unwrap();
            ms_query.pending_queries.insert(perform_query.query_id.clone());

            // Create an MSWriteTableES in the QueryReplanning state, and add it to
            // the MSQueryES.
            // TODO: if we wanted to do this Consistently, we would construct the whole
            // FullMSTableWriteES, and then have a function that looks up ESs and performs
            // Immediate computations if they are in a state to do so (or throw an error otherwise).
            let mut comm_plan_es = CommonQueryReplanningES {
              timestamp: query.timestamp,
              context: Rc::new(query.context),
              sql_view: query.sql_query.clone(),
              query_plan: query.query_plan,
              sender_path: perform_query.sender_path,
              query_id: perform_query.query_id.clone(),
              state: CommonQueryReplanningS::Start,
            };
            comm_plan_es.start::<T>(self);
            statuses.full_ms_table_write_ess.insert(
              perform_query.query_id.clone(),
              FullMSTableWriteES::QueryReplanning(MSWriteQueryReplanningES {
                root_query_path: perform_query.root_query_path,
                tier_map: perform_query.tier_map,
                ms_query_id,
                status: comm_plan_es,
              }),
            );
          }
        }
      }
      msg::TabletMessage::CancelQuery(cancel_query) => {
        self.exit_and_clean_up(statuses, cancel_query.query_id);
      }
      msg::TabletMessage::QueryAborted(query_aborted) => {
        self.handle_query_aborted(statuses, query_aborted);
      }
      msg::TabletMessage::QuerySuccess(query_success) => {
        self.handle_query_success(statuses, query_success);
      }
      msg::TabletMessage::Query2PCPrepare(_) => unimplemented!(),
      msg::TabletMessage::Query2PCAbort(_) => unimplemented!(),
      msg::TabletMessage::Query2PCCommit(_) => unimplemented!(),
      msg::TabletMessage::MasterFrozenColUsageAborted(_) => unimplemented!(),
      msg::TabletMessage::MasterFrozenColUsageSuccess(_) => unimplemented!(),
    }

    self.run_main_loop(statuses);
  }

  /// Adds the following triple into `requested_locked_columns`, making sure to update
  /// `request_index` as well. Here, `orig_p` is the origiator who should get the locking
  /// result.
  fn add_requested_locked_columns(
    &mut self,
    orig_p: OrigP,
    timestamp: Timestamp,
    cols: Vec<ColName>,
  ) -> QueryId {
    let locked_cols_qid = mk_qid(&mut self.rand);
    // Add column locking
    self.requested_locked_columns.insert(locked_cols_qid.clone(), (orig_p, timestamp, cols));
    // Update index
    btree_multimap_insert(&mut self.request_index, &timestamp, locked_cols_qid.clone());
    locked_cols_qid
  }

  /// This removes elements from `requested_locked_columns`, maintaining
  /// `request_index` as well, if the request is present.
  fn remove_col_locking_request(
    &mut self,
    query_id: QueryId,
  ) -> Option<(OrigP, Timestamp, Vec<ColName>)> {
    // Remove if present
    if let Some((orig_p, timestamp, cols)) = self.requested_locked_columns.remove(&query_id) {
      // Maintain the request_index.
      btree_multimap_remove(&mut self.request_index, &timestamp, &query_id);
      return Some((orig_p, timestamp, cols));
    }
    return None;
  }

  /// This removes the Read Protection request from `waiting_read_protected` with the given
  /// `query_id` at the given `timestamp`, if it exists, and returns it.
  fn remove_read_protected_request(
    &mut self,
    timestamp: Timestamp,
    query_id: QueryId,
  ) -> Option<(OrigP, QueryId, TableRegion)> {
    if let Some(waiting) = self.waiting_read_protected.get_mut(&timestamp) {
      for protect_request in waiting.iter() {
        let (_, cur_query_id, _) = protect_request;
        if cur_query_id == &query_id {
          // Here, we found a request with matching QueryId, so we remove it.
          let protect_request = protect_request.clone();
          waiting.remove(&protect_request);
          if waiting.is_empty() {
            self.waiting_read_protected.remove(&timestamp);
          }
          return Some(protect_request);
        }
      }
    }
    return None;
  }

  /// This removes the Read Protection request from `waiting_read_protected` with the given
  /// `query_id` at the given `timestamp`, if it exists, and returns it.
  fn remove_m_read_protected_request(
    &mut self,
    timestamp: Timestamp,
    query_id: QueryId,
  ) -> Option<(OrigP, QueryId, TableRegion)> {
    if let Some(verifying_write) = self.verifying_writes.get_mut(&timestamp) {
      for protect_request in verifying_write.m_waiting_read_protected.iter() {
        let (_, cur_query_id, _) = protect_request;
        if cur_query_id == &query_id {
          // Here, we found a request with matching QueryId, so we remove it.
          let protect_request = protect_request.clone();
          verifying_write.m_waiting_read_protected.remove(&protect_request);
          return Some(protect_request);
        }
      }
    }
    return None;
  }

  fn check_write_region_isolation(
    &self,
    write_region: &TableRegion,
    timestamp: &Timestamp,
  ) -> bool {
    // We iterate through every subsequent Reads that this `write_region` can conflict
    // with, and check if there is indeed a conflict.

    // First, verify Region Isolation with ReadRegions of subsequent *_writes.
    let bound = (Bound::Excluded(timestamp), Bound::Unbounded);
    for (_, verifying_write) in self.verifying_writes.range(bound) {
      if does_intersect(write_region, &verifying_write.m_read_protected) {
        return false;
      }
    }
    for (_, prepared_write) in self.prepared_writes.range(bound) {
      if does_intersect(write_region, &prepared_write.m_read_protected) {
        return false;
      }
    }
    for (_, committed_write) in self.committed_writes.range(bound) {
      if does_intersect(write_region, &committed_write.m_read_protected) {
        return false;
      }
    }

    // Then, verify Region Isolation with ReadRegions of subsequent Reads.
    let bound = (Bound::Included(timestamp), Bound::Unbounded);
    for (_, read_regions) in self.read_protected.range(bound) {
      if does_intersect(write_region, read_regions) {
        return false;
      }
    }

    // If we get here, it means we have Region Isolation.
    return false;
  }

  fn run_main_loop(&mut self, statuses: &mut Statuses) {
    let mut change_occurred = true;
    while change_occurred {
      // We set this to false, and the below code will set it back to true if need be.
      change_occurred = false;

      // First, we see if we can satisfy any `requested_locked_cols`.

      // First, compute the Timestamp that each ColName is going to change.
      let mut col_prepare_timestamps = HashMap::<ColName, Timestamp>::new();
      for (timestamp, schema_change) in &self.prepared_schema_change {
        for (col, _) in schema_change {
          col_prepare_timestamps.insert(col.clone(), timestamp.clone());
        }
      }

      // Iterate through every `request_locked_columns` and see if that can be satisfied.
      'outer: for (_, query_set) in &self.request_index {
        for query_id in query_set {
          let (_, timestamp, cols) = self.requested_locked_columns.get(query_id).unwrap();
          let mut not_preparing = true;
          for col in cols {
            // Check if the `col` is being Prepared.
            if let Some(prep_timestamp) = col_prepare_timestamps.get(col) {
              if timestamp >= prep_timestamp {
                not_preparing = false;
                break;
              }
            }
          }
          if not_preparing {
            // This means that this task is done. We still need to enforce the lats
            // to be high enough in `val_cols`.
            for col in cols {
              if lookup_pos(&self.table_schema.key_cols, col).is_none() {
                self.table_schema.val_cols.read(col, timestamp.clone());
              }
            }

            // Remove the column locking request.
            let query_id = query_id.clone();
            let (orig_p, _, _) = self.remove_col_locking_request(query_id.clone()).unwrap();

            // Process
            self.columns_locked_for_query(statuses, orig_p, query_id);
            change_occurred = true;

            break 'outer;
          }
        }
      }

      // Next, we see if we can provide Region
      let maybe_first_done = (|| {
        // To account for both `verifying_writes` and `prepared_writes`, we merge
        // them into a single container similar to `verifying_writes`. This is just
        // for expedience; it should be optimized later. (We can probably change
        // `prepared_writes` to `VerifyingReadWriteRegion` and then zip 2 iterators).
        let mut all_cur_writes = BTreeMap::<Timestamp, VerifyingReadWriteRegion>::new();
        for (cur_timestamp, verifying_write) in &self.verifying_writes {
          all_cur_writes.insert(*cur_timestamp, verifying_write.clone());
        }
        for (cur_timestamp, verifying_write) in &self.prepared_writes {
          all_cur_writes.insert(
            *cur_timestamp,
            VerifyingReadWriteRegion {
              orig_p: verifying_write.orig_p.clone(),
              m_waiting_read_protected: Default::default(),
              m_read_protected: verifying_write.m_read_protected.clone(),
              m_write_protected: verifying_write.m_write_protected.clone(),
            },
          );
        }

        // First, we see if any `(m_)waiting_read_protected`s can be moved to `(m_)read_protected`.
        if !all_cur_writes.is_empty() {
          let (first_write_timestamp, verifying_write) = all_cur_writes.first_key_value().unwrap();

          // First, process all `read_protected`s before the `first_write_timestamp`
          let bound = (Bound::Unbounded, Bound::Excluded(first_write_timestamp));
          for (timestamp, set) in self.waiting_read_protected.range(bound) {
            let protect_request = set.first().unwrap().clone();
            return Some(ReadProtectionGrant::Read { timestamp: *timestamp, protect_request });
          }

          // Next, process all `m_read_protected`s for the first `verifying_write`
          for protect_request in &verifying_write.m_waiting_read_protected {
            return Some(ReadProtectionGrant::MRead {
              timestamp: *first_write_timestamp,
              protect_request: protect_request.clone(),
            });
          }

          // Next, accumulate the WriteRegions, and then search for region protection with
          // all subsequent `(m_)waiting_read_protected`s.
          let mut cum_write_regions = verifying_write.m_write_protected.clone();
          let mut prev_write_timestamp = first_write_timestamp;
          let bound = (Bound::Excluded(first_write_timestamp), Bound::Unbounded);
          for (cur_timestamp, verifying_write) in all_cur_writes.range(bound) {
            // The loop state is that `cum_write_regions` contains all WriteRegions <=
            // `prev_write_timestamp`, all `m_read_protected` <= have been processed, and
            // all `read_protected`s < have been processed.

            // Process `m_read_protected`
            for protect_request in &verifying_write.m_waiting_read_protected {
              let (_, _, read_region) = protect_request;
              if does_intersect(read_region, &cum_write_regions) {
                return Some(ReadProtectionGrant::MRead {
                  timestamp: *first_write_timestamp,
                  protect_request: protect_request.clone(),
                });
              }
            }

            // Process `read_protected`
            let bound = (Bound::Included(prev_write_timestamp), Bound::Excluded(cur_timestamp));
            for (timestamp, set) in self.waiting_read_protected.range(bound) {
              for protect_request in set {
                let (_, _, read_region) = protect_request;
                if does_intersect(read_region, &cum_write_regions) {
                  return Some(ReadProtectionGrant::Read {
                    timestamp: *timestamp,
                    protect_request: protect_request.clone(),
                  });
                }
              }
            }

            // Add the WriteRegions into `cur_write_regions`.
            for write_region in &verifying_write.m_write_protected {
              cum_write_regions.insert(write_region.clone());
            }
            prev_write_timestamp = cur_timestamp;
          }

          // Finally, finish processing any remaining `read_protected`s
          let bound = (Bound::Included(prev_write_timestamp), Bound::Unbounded);
          for (timestamp, set) in self.waiting_read_protected.range(bound) {
            for protect_request in set {
              let (_, _, read_region) = protect_request;
              if does_intersect(read_region, &cum_write_regions) {
                return Some(ReadProtectionGrant::Read {
                  timestamp: *timestamp,
                  protect_request: protect_request.clone(),
                });
              }
            }
          }
        } else {
          for (timestamp, set) in &self.waiting_read_protected {
            let protect_request = set.first().unwrap().clone();
            return Some(ReadProtectionGrant::Read { timestamp: *timestamp, protect_request });
          }
        }

        // Next, we search for any DeadlockSafetyWriteAbort.
        for (timestamp, set) in &self.waiting_read_protected {
          if let Some(verifying_write) = self.verifying_writes.get(timestamp) {
            for protect_request in set {
              let (orig_p, _, read_region) = protect_request;
              if does_intersect(read_region, &verifying_write.m_write_protected) {
                return Some(ReadProtectionGrant::DeadlockSafetyWriteAbort {
                  timestamp: *timestamp,
                  orig_p: orig_p.clone(),
                });
              }
            }
          }
        }

        // Finally, we search for any DeadlockSafetyWriteAbort.
        for (timestamp, set) in &self.waiting_read_protected {
          if let Some(prepared_write) = self.prepared_writes.get(timestamp) {
            for protect_request in set {
              let (_, _, read_region) = protect_request;
              if does_intersect(read_region, &prepared_write.m_write_protected) {
                return Some(ReadProtectionGrant::DeadlockSafetyReadAbort {
                  timestamp: *timestamp,
                  protect_request: protect_request.clone(),
                });
              }
            }
          }
        }

        return None;
      })();
      if let Some(read_protected) = maybe_first_done {
        match read_protected {
          ReadProtectionGrant::Read { timestamp, protect_request } => {
            // Remove the protect_request, doing any extra cleanups too.
            let (_, query_id, _) = protect_request;
            let (orig_p, query_id, read_region) =
              self.remove_read_protected_request(timestamp, query_id).unwrap();

            // Add the ReadRegion to `read_protected`.
            btree_multimap_insert(&mut self.read_protected, &timestamp, read_region.clone());

            // Inform the originator.
            self.read_protected_for_query(statuses, orig_p, query_id);
          }
          ReadProtectionGrant::MRead { timestamp, protect_request } => {
            // Remove the protect_request, adding the ReadRegion to `m_read_protected`.
            let verifying_write = self.verifying_writes.get_mut(&timestamp).unwrap();
            verifying_write.m_waiting_read_protected.remove(&protect_request);

            let (orig_p, query_id, read_region) = protect_request;
            verifying_write.m_read_protected.insert(read_region.clone());

            // Inform the originator.
            self.read_protected_for_query(statuses, orig_p, query_id);
          }
          ReadProtectionGrant::DeadlockSafetyReadAbort { timestamp, protect_request } => {
            // Remove the protect_request, doing any extra cleanups too.
            let (_, query_id, _) = protect_request;
            let (orig_p, _, read_region) =
              self.remove_read_protected_request(timestamp, query_id).unwrap();

            // Inform the originator
            self.deadlock_safety_read_abort(statuses, orig_p, read_region);
          }
          ReadProtectionGrant::DeadlockSafetyWriteAbort { orig_p, timestamp } => {
            // Remove the `verifying_write` at `timestamp`.
            self.verifying_writes.remove(&timestamp);

            // Inform the originator.
            self.deadlock_safety_write_abort(statuses, orig_p);
          }
        }
        change_occurred = true;
      }
    }
  }

  fn columns_locked_for_query(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    locked_cols_qid: QueryId,
  ) {
    let query_id = orig_p.query_id;
    if let Some(read_es) = statuses.full_table_read_ess.get_mut(&query_id) {
      match read_es {
        FullTableReadES::QueryReplanning(plan_es) => {
          // Advance the QueryReplanning now that the desired columns have been locked.
          let comm_plan_es = &mut plan_es.status;
          comm_plan_es.columns_locked::<T>(self);
          // We check if the QueryReplanning is done.
          match comm_plan_es.state {
            CommonQueryReplanningS::Done(success) => {
              if success {
                // If the QueryReplanning was successful, we move the FullTableReadES
                // to Executing in the Start state, and immediately start executing it.
                *read_es = FullTableReadES::Executing(TableReadES {
                  root_query_path: plan_es.root_query_path.clone(),
                  tier_map: plan_es.tier_map.clone(),
                  timestamp: comm_plan_es.timestamp,
                  context: comm_plan_es.context.clone(),
                  sender_path: comm_plan_es.sender_path.clone(),
                  query_id: comm_plan_es.query_id.clone(),
                  sql_query: comm_plan_es.sql_view.clone(),
                  query_plan: comm_plan_es.query_plan.clone(),
                  new_rms: Default::default(),
                  state: ExecutionS::Start,
                });
                self.start_table_read_es(statuses, &query_id);
              } else {
                // Recall that if QueryReplanning had ended in a failure (i.e.
                // having missing columns), then `CommonQueryReplanningES` will
                // have send back the necessary responses. Thus, we only need to
                // Exit the ES here.
                statuses.remove(&query_id);
              }
            }
            _ => {}
          }
        }
        FullTableReadES::Executing(es) => {
          let executing = cast!(ExecutionS::Executing, &mut es.state).unwrap();

          // Find the SingleSubqueryStatus that sent out this requested_locked_columns.
          // There should always be such a Subquery.
          let subquery_idx = executing.find_subquery(&locked_cols_qid).unwrap();
          let single_status = executing.subqueries.get_mut(subquery_idx).unwrap();
          let locking_status = cast!(SingleSubqueryStatus::LockingSchemas, single_status).unwrap();

          // None of the `new_cols` should already exist in the old subquery Context schema
          // (since they didn't exist when the GRQueryES complained).
          for col in &locking_status.new_cols {
            assert!(!locking_status.old_columns.contains(col));
          }

          // Next, we compute the subset of `new_cols` that aren't in the Table
          // Schema or the Context.
          let mut missing_cols = Vec::<ColName>::new();
          for col in &locking_status.new_cols {
            if !contains_col(&self.table_schema, col, &es.timestamp) {
              if !es.context.context_schema.column_context_schema.contains(col) {
                missing_cols.push(col.clone());
              }
            }
          }

          if !missing_cols.is_empty() {
            // If there are missing columns, we Exit and clean up, and propagate
            // the Abort to the originator.

            // Construct a ColumnsDNE containing `missing_cols` and send it
            // back to the originator.
            self.ctx().send_abort_data(
              es.sender_path.clone(),
              query_id.clone(),
              msg::AbortedData::ColumnsDNE { missing_cols },
            );

            // Finally, Exit and Clean Up this TableReadES.
            self.exit_and_clean_up(statuses, query_id);
          } else {
            // Here, we know all `new_cols` are in this Context, and so we can continue
            // trying to evaluate the subquery.

            // Now, add the `new_cols` to the schema
            let mut new_columns = locking_status.old_columns.clone();
            new_columns.extend(locking_status.new_cols.clone());

            // For ColNames in `new_cols` that belong to this Table, we need to
            // lock the region, so we compute a TableRegion accordingly.
            let mut new_col_region = Vec::<ColName>::new();
            for col in &locking_status.new_cols {
              if contains_col(&self.table_schema, col, &es.timestamp) {
                new_col_region.push(col.clone());
              }
            }
            let new_read_region =
              TableRegion { col_region: new_col_region, row_region: executing.row_region.clone() };

            // Add a read protection requested
            let protect_query_id = mk_qid(&mut self.rand);
            let protect_request =
              (OrigP::new(es.query_id.clone()), protect_query_id.clone(), new_read_region.clone());
            btree_multimap_insert(&mut self.waiting_read_protected, &es.timestamp, protect_request);

            // Finally, update the SingleSubqueryStatus to wait for the Region Protection.
            *single_status = SingleSubqueryStatus::PendingReadRegion(SubqueryPendingReadRegion {
              new_columns,
              trans_table_names: locking_status.trans_table_names.clone(),
              read_region: new_read_region,
              query_id: protect_query_id,
            })
          }
        }
      }
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      match ms_write_es {
        FullMSTableWriteES::QueryReplanning(plan_es) => {
          // Advance the QueryReplanning now that the desired columns have been locked.
          let comm_plan_es = &mut plan_es.status;
          comm_plan_es.columns_locked::<T>(self);

          // We check if the QueryReplanning is done.
          let ms_query_id = plan_es.ms_query_id.clone();
          match comm_plan_es.state {
            CommonQueryReplanningS::Done(success) => {
              if success {
                // If the QueryReplanning was successful, we move the FullMSTableWriteES
                // to Executing in the Start state, and immediately start executing it.

                // First, we look up the `tier` of this Table being
                // written, update the `tier_map`.
                let sql_query = comm_plan_es.sql_view.clone();
                let mut tier_map = plan_es.tier_map.clone();
                let tier = tier_map.map.get(sql_query.table()).unwrap().clone();
                *tier_map.map.get_mut(sql_query.table()).unwrap() += 1;

                // Then, we construct the MSTableWriteES.
                *ms_write_es = FullMSTableWriteES::Executing(MSTableWriteES {
                  root_query_path: plan_es.root_query_path.clone(),
                  tier_map,
                  timestamp: comm_plan_es.timestamp,
                  tier,
                  context: comm_plan_es.context.clone(),
                  sender_path: comm_plan_es.sender_path.clone(),
                  query_id: comm_plan_es.query_id.clone(),
                  sql_query,
                  query_plan: comm_plan_es.query_plan.clone(),
                  ms_query_id,
                  new_rms: Default::default(),
                  state: MSWriteExecutionS::Start,
                });

                // Finally, we start the MSWriteTableES.
                self.start_ms_table_write_es(statuses, &query_id);
              } else {
                // Since `CommonQueryReplanningES` will have already sent back the necessary
                // responses. Thus, we only need to Exit and Clean Up the ES.
                self.exit_and_clean_up(statuses, query_id);
              }
            }
            _ => {}
          }
        }
        FullMSTableWriteES::Executing(es) => {
          let executing = cast!(MSWriteExecutionS::Executing, &mut es.state).unwrap();

          // Find the SingleSubqueryStatus that sent out this requested_locked_columns.
          // There should always be such a Subquery.
          let subquery_idx = executing.find_subquery(&locked_cols_qid).unwrap();
          let single_status = executing.subqueries.get_mut(subquery_idx).unwrap();
          let locking_status = cast!(SingleSubqueryStatus::LockingSchemas, single_status).unwrap();

          // None of the `new_cols` should already exist in the old subquery Context schema
          // (since they didn't exist when the GRQueryES complained).
          for col in &locking_status.new_cols {
            assert!(!locking_status.old_columns.contains(col));
          }

          // Next, we compute the subset of `new_cols` that aren't in the Table
          // Schema or the Context.
          let mut missing_cols = Vec::<ColName>::new();
          for col in &locking_status.new_cols {
            if !contains_col(&self.table_schema, col, &es.timestamp) {
              if !es.context.context_schema.column_context_schema.contains(col) {
                missing_cols.push(col.clone());
              }
            }
          }

          if !missing_cols.is_empty() {
            // If there are missing columns, we Exit and clean up, and propagate
            // the Abort to the originator.

            // Construct a ColumnsDNE containing `missing_cols` and send it
            // back to the originator.
            self.ctx().send_abort_data(
              es.sender_path.clone(),
              query_id.clone(),
              msg::AbortedData::ColumnsDNE { missing_cols },
            );

            // Finally, Exit and Clean Up this MSWriteTableES.
            self.exit_and_clean_up(statuses, query_id);
          } else {
            // Here, we know all `new_cols` are in this Context, and so we can continue
            // trying to evaluate the subquery.

            // Now, add the `new_cols` to the schema
            let mut new_columns = locking_status.old_columns.clone();
            new_columns.extend(locking_status.new_cols.clone());

            // For ColNames in `new_cols` that belong to this Table, we need to
            // lock the region, so we compute a TableRegion accordingly.
            let mut new_col_region = Vec::<ColName>::new();
            for col in &locking_status.new_cols {
              if contains_col(&self.table_schema, col, &es.timestamp) {
                new_col_region.push(col.clone());
              }
            }
            let new_read_region =
              TableRegion { col_region: new_col_region, row_region: executing.row_region.clone() };

            // Add a read protection requested
            let protect_query_id = mk_qid(&mut self.rand);
            let orig_p = OrigP::new(es.query_id.clone());
            let protect_request = (orig_p, protect_query_id.clone(), new_read_region.clone());
            // Note: this part is the main difference between this and TableReadES.
            let verifying_write = self.verifying_writes.get_mut(&es.timestamp).unwrap();
            verifying_write.m_waiting_read_protected.insert(protect_request);

            // Finally, update the SingleSubqueryStatus to wait for the Region Protection.
            *single_status = SingleSubqueryStatus::PendingReadRegion(SubqueryPendingReadRegion {
              new_columns,
              trans_table_names: locking_status.trans_table_names.clone(),
              read_region: new_read_region,
              query_id: protect_query_id,
            })
          }
        }
      }
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      match ms_read_es {
        FullMSTableReadES::QueryReplanning(plan_es) => {
          // Advance the QueryReplanning now that the desired columns have been locked.
          let comm_plan_es = &mut plan_es.status;
          comm_plan_es.columns_locked::<T>(self);

          // We check if the QueryReplanning is done.
          let ms_query_id = plan_es.ms_query_id.clone();
          match comm_plan_es.state {
            CommonQueryReplanningS::Done(success) => {
              if success {
                // If the QueryReplanning was successful, we move the FullMSTableReadES
                // to Executing in the Start state, and immediately start executing it.

                // First, we look up the `tier` of this Table being read.
                let sql_query = comm_plan_es.sql_view.clone();
                let tier_map = plan_es.tier_map.clone();
                let tier = tier_map.map.get(sql_query.table()).unwrap().clone();

                // Then, we construct the MSTableReadES.
                *ms_read_es = FullMSTableReadES::Executing(MSTableReadES {
                  root_query_path: plan_es.root_query_path.clone(),
                  tier_map,
                  timestamp: comm_plan_es.timestamp,
                  tier,
                  context: comm_plan_es.context.clone(),
                  sender_path: comm_plan_es.sender_path.clone(),
                  query_id: comm_plan_es.query_id.clone(),
                  sql_query,
                  query_plan: comm_plan_es.query_plan.clone(),
                  ms_query_id,
                  new_rms: Default::default(),
                  state: MSReadExecutionS::Start,
                });

                // Finally, we start the MSReadTableES.
                self.start_ms_table_read_es(statuses, &query_id);
              } else {
                // Since `CommonQueryReplanningES` will have already sent back the necessary
                // responses. Thus, we only need to Exit and Clean Up the ES.
                self.exit_and_clean_up(statuses, query_id);
              }
            }
            _ => {}
          }
        }
        FullMSTableReadES::Executing(es) => {
          let executing = cast!(MSReadExecutionS::Executing, &mut es.state).unwrap();

          // Find the SingleSubqueryStatus that sent out this requested_locked_columns.
          // There should always be such a Subquery.
          let subquery_idx = executing.find_subquery(&locked_cols_qid).unwrap();
          let single_status = executing.subqueries.get_mut(subquery_idx).unwrap();
          let locking_status = cast!(SingleSubqueryStatus::LockingSchemas, single_status).unwrap();

          // None of the `new_cols` should already exist in the old subquery Context schema
          // (since they didn't exist when the GRQueryES complained).
          for col in &locking_status.new_cols {
            assert!(!locking_status.old_columns.contains(col));
          }

          // Next, we compute the subset of `new_cols` that aren't in the Table
          // Schema or the Context.
          let mut missing_cols = Vec::<ColName>::new();
          for col in &locking_status.new_cols {
            if !contains_col(&self.table_schema, col, &es.timestamp) {
              if !es.context.context_schema.column_context_schema.contains(col) {
                missing_cols.push(col.clone());
              }
            }
          }

          if !missing_cols.is_empty() {
            // If there are missing columns, we Exit and clean up, and propagate
            // the Abort to the originator.

            // Construct a ColumnsDNE containing `missing_cols` and send it
            // back to the originator.
            self.ctx().send_abort_data(
              es.sender_path.clone(),
              query_id.clone(),
              msg::AbortedData::ColumnsDNE { missing_cols },
            );

            // Finally, Exit and Clean Up this MSWriteTableES.
            self.exit_and_clean_up(statuses, query_id);
          } else {
            // Here, we know all `new_cols` are in this Context, and so we can continue
            // trying to evaluate the subquery.

            // Now, add the `new_cols` to the schema
            let mut new_columns = locking_status.old_columns.clone();
            new_columns.extend(locking_status.new_cols.clone());

            // For ColNames in `new_cols` that belong to this Table, we need to
            // lock the region, so we compute a TableRegion accordingly.
            let mut new_col_region = Vec::<ColName>::new();
            for col in &locking_status.new_cols {
              if contains_col(&self.table_schema, col, &es.timestamp) {
                new_col_region.push(col.clone());
              }
            }
            let new_read_region =
              TableRegion { col_region: new_col_region, row_region: executing.row_region.clone() };

            // Add a read protection requested
            let protect_query_id = mk_qid(&mut self.rand);
            let orig_p = OrigP::new(es.query_id.clone());
            let protect_request = (orig_p, protect_query_id.clone(), new_read_region.clone());
            // Note: this part is the main difference between this and TableReadES.
            let verifying_write = self.verifying_writes.get_mut(&es.timestamp).unwrap();
            verifying_write.m_waiting_read_protected.insert(protect_request);

            // Finally, update the SingleSubqueryStatus to wait for the Region Protection.
            *single_status = SingleSubqueryStatus::PendingReadRegion(SubqueryPendingReadRegion {
              new_columns,
              trans_table_names: locking_status.trans_table_names.clone(),
              read_region: new_read_region,
              query_id: protect_query_id,
            })
          }
        }
      }
    }
  }

  /// Processes the Start state of TableReadES.
  fn start_table_read_es(&mut self, statuses: &mut Statuses, query_id: &QueryId) {
    let read_es = statuses.full_table_read_ess.get_mut(&query_id).unwrap();
    let es = cast!(FullTableReadES::Executing, read_es).unwrap();

    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &es.sql_query.selection,
      &self.table_schema,
      &es.timestamp,
      &es.context.context_schema,
    );

    // Compute the Row Region by taking the union across all ContextRows
    let mut row_region = Vec::<KeyBound>::new();
    for context_row in &es.context.context_rows {
      match keybound_computer.compute_keybounds(&context_row) {
        Ok(key_bounds) => {
          for key_bound in key_bounds {
            row_region.push(key_bound);
          }
        }
        Err(eval_error) => {
          self.abort_with_query_error(statuses, &query_id, mk_eval_error(eval_error));
          return;
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(es.sql_query.projection.clone());
    col_region.extend(es.query_plan.col_usage_node.safe_present_cols.clone());

    // Move the TableReadES to the Pending state with the given ReadRegion.
    let protect_query_id = mk_qid(&mut self.rand);
    let col_region = Vec::from_iter(col_region.into_iter());
    let read_region = TableRegion { col_region, row_region };
    es.state = ExecutionS::Pending(Pending {
      read_region: read_region.clone(),
      query_id: protect_query_id.clone(),
    });

    // Add a read protection requested
    let protect_request = (OrigP::new(es.query_id.clone()), protect_query_id, read_region);
    btree_multimap_insert(&mut self.waiting_read_protected, &es.timestamp, protect_request);
  }

  /// Processes the Start state of MSTableWrite.
  fn start_ms_table_write_es(&mut self, statuses: &mut Statuses, query_id: &QueryId) {
    let ms_write_es = statuses.full_ms_table_write_ess.get_mut(&query_id).unwrap();
    let es = cast!(FullMSTableWriteES::Executing, ms_write_es).unwrap();

    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &es.sql_query.selection,
      &self.table_schema,
      &es.timestamp,
      &es.context.context_schema,
    );

    // Compute the Row Region by taking the union across all ContextRows
    let mut row_region = Vec::<KeyBound>::new();
    for context_row in &es.context.context_rows {
      match keybound_computer.compute_keybounds(&context_row) {
        Ok(key_bounds) => {
          for key_bound in key_bounds {
            row_region.push(key_bound);
          }
        }
        Err(eval_error) => {
          self.abort_with_query_error(statuses, &query_id, mk_eval_error(eval_error));
          return;
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Write Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(es.sql_query.assignment.iter().map(|(col, _)| col.clone()));

    // Compute the Write Region
    let col_region = Vec::from_iter(col_region.into_iter());
    let write_region = TableRegion { col_region, row_region: row_region.clone() };

    // Verify that we have WriteRegion Isolation with Subsequent Reads. We abort
    // if we don't, and we amend this MSQuery's VerifyingReadWriteRegions if we do.
    let timestamp = es.timestamp.clone();
    if !self.check_write_region_isolation(&write_region, &timestamp) {
      // Send an abortion.
      self.ctx().send_query_error(
        es.sender_path.clone(),
        query_id.clone(),
        msg::QueryError::WriteRegionConflictWithSubsequentRead,
      );
      self.exit_and_clean_up(statuses, query_id.clone());
    } else {
      // Compute the Read Column Region.
      let col_region = HashSet::<ColName>::from_iter(
        es.query_plan.col_usage_node.safe_present_cols.iter().cloned(),
      );

      // Move the MSTableWriteES to the Pending state with the given ReadRegion.
      let protect_query_id = mk_qid(&mut self.rand);
      let col_region = Vec::from_iter(col_region.into_iter());
      let read_region = TableRegion { col_region, row_region };
      es.state = MSWriteExecutionS::Pending(Pending {
        read_region: read_region.clone(),
        query_id: protect_query_id.clone(),
      });

      // Add a ReadRegion to the m_waiting_read_protected.
      let orig_p = OrigP::new(es.query_id.clone());
      let protect_request = (orig_p, protect_query_id, read_region);
      let verifying = self.verifying_writes.get_mut(&es.timestamp).unwrap();
      verifying.m_waiting_read_protected.insert(protect_request);
    }
  }

  /// Processes the Start state of MSTableRead.
  fn start_ms_table_read_es(&mut self, statuses: &mut Statuses, query_id: &QueryId) {
    let ms_read_es = statuses.full_ms_table_read_ess.get_mut(&query_id).unwrap();
    let es = cast!(FullMSTableReadES::Executing, ms_read_es).unwrap();

    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &es.sql_query.selection,
      &self.table_schema,
      &es.timestamp,
      &es.context.context_schema,
    );

    // Compute the Row Region by taking the union across all ContextRows
    let mut row_region = Vec::<KeyBound>::new();
    for context_row in &es.context.context_rows {
      match keybound_computer.compute_keybounds(&context_row) {
        Ok(key_bounds) => {
          for key_bound in key_bounds {
            row_region.push(key_bound);
          }
        }
        Err(eval_error) => {
          self.abort_with_query_error(statuses, &query_id, mk_eval_error(eval_error));
          return;
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Read Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(es.sql_query.projection.clone());
    col_region.extend(es.query_plan.col_usage_node.safe_present_cols.clone());

    // Move the MSTableReadES to the Pending state with the given ReadRegion.
    let protect_query_id = mk_qid(&mut self.rand);
    let col_region = Vec::from_iter(col_region.into_iter());
    let read_region = TableRegion { col_region, row_region };
    es.state = MSReadExecutionS::Pending(Pending {
      read_region: read_region.clone(),
      query_id: protect_query_id.clone(),
    });

    // Add a ReadRegion to the m_waiting_read_protected.
    let orig_p = OrigP::new(es.query_id.clone());
    let protect_request = (orig_p, protect_query_id, read_region);
    let verifying = self.verifying_writes.get_mut(&es.timestamp).unwrap();
    verifying.m_waiting_read_protected.insert(protect_request);
  }

  fn get_path(&self, statuses: &Statuses, query_id: &QueryId) -> QueryPath {
    if let Some(read_es) = statuses.full_table_read_ess.get(query_id) {
      // TODO: shouldn't we also switch over the QueryReplanning state?
      let es = cast!(FullTableReadES::Executing, read_es).unwrap();
      es.sender_path.clone()
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get(&query_id) {
      trans_read_es.sender_path()
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get(&query_id) {
      let es = cast!(FullMSTableWriteES::Executing, ms_write_es).unwrap();
      es.sender_path.clone()
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get(&query_id) {
      let es = cast!(FullMSTableReadES::Executing, ms_read_es).unwrap();
      es.sender_path.clone()
    } else {
      panic!()
    }
  }

  /// This looks up the ES of `query_id` and sends the `query_error` back to that ES's
  /// sender_path, and also Exit and Cleans Up the ES.
  fn abort_with_query_error(
    &mut self,
    statuses: &mut Statuses,
    query_id: &QueryId,
    query_error: msg::QueryError,
  ) {
    let sender_path = self.get_path(statuses, query_id);
    self.ctx().send_query_error(sender_path, query_id.clone(), query_error);
    self.exit_and_clean_up(statuses, query_id.clone());
  }

  /// We call this when a DeadlockSafetyReadAbort happens for a `waiting_read_protected`.
  /// Recall this behavior is distinct than if a `verifying_writes` aborts.
  fn deadlock_safety_read_abort(&mut self, statuses: &mut Statuses, orig_p: OrigP, _: TableRegion) {
    self.abort_with_query_error(
      statuses,
      &orig_p.query_id,
      msg::QueryError::DeadlockSafetyAbortion,
    );
  }

  /// This function simply removes the the MSQueryES from `statuses`, accesses all ESs in
  /// `pending_queries`, Exit sthen and cleans up and sends back `query_error` to their senders.
  fn abort_ms_with_query_error(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    query_error: msg::QueryError,
  ) {
    let ms_query_es = statuses.ms_query_ess.remove(&query_id).unwrap();
    for query_id in ms_query_es.pending_queries {
      self.abort_with_query_error(statuses, &query_id, query_error.clone());
    }
    self.verifying_writes.remove(&ms_query_es.timestamp);
  }

  /// We call this when a DeadlockSafetyReadAbort happens for a `verifying_writes`.
  fn deadlock_safety_write_abort(&mut self, statuses: &mut Statuses, orig_p: OrigP) {
    self.abort_ms_with_query_error(
      statuses,
      orig_p.query_id,
      msg::QueryError::DeadlockSafetyAbortion,
    );
  }

  /// Here, the subquery GRQueryES at `subquery_id` must already be cleaned up. This
  /// function informs the ES at OrigP and updates its state accordingly.
  fn common_handle_internal_columns_dne(
    &mut self,
    executing: &mut Executing,
    query_id: QueryId,
    timestamp: Timestamp,
    subquery_id: QueryId,
    rem_cols: Vec<ColName>,
  ) {
    // Insert a requested_locked_columns for the missing columns.
    let locked_cols_qid =
      self.add_requested_locked_columns(OrigP::new(query_id.clone()), timestamp, rem_cols.clone());

    // Get the SingleStatus
    let pos = executing.find_subquery(&subquery_id).unwrap();
    let single_status = executing.subqueries.get_mut(pos).unwrap();

    // Replace the the new SingleSubqueryStatus.
    let old_pending = cast!(SingleSubqueryStatus::Pending, single_status).unwrap();
    let old_context_schema = old_pending.context.context_schema.clone();
    *single_status = SingleSubqueryStatus::LockingSchemas(SubqueryLockingSchemas {
      old_columns: old_context_schema.column_context_schema.clone(),
      trans_table_names: old_context_schema.trans_table_names(),
      new_cols: rem_cols,
      query_id: locked_cols_qid,
    });
  }

  /// This function just routes the InternalColumnsDNE notification from the GRQueryES.
  /// Recall that the originator must exist (since the GRQueryES had existed).
  fn handle_internal_columns_dne(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    rem_cols: Vec<ColName>,
  ) {
    let query_id = orig_p.query_id;
    if let Some(read_es) = statuses.full_table_read_ess.get_mut(&query_id) {
      let es = cast!(FullTableReadES::Executing, read_es).unwrap();
      let executing = cast!(ExecutionS::Executing, &mut es.state).unwrap();
      self.common_handle_internal_columns_dne(
        executing,
        query_id,
        es.timestamp.clone(),
        subquery_id,
        rem_cols,
      );
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      let prefix = trans_read_es.location_prefix();
      let action = if let Some(gr_query_es) = statuses.gr_query_ess.get(&prefix.query_id) {
        trans_read_es.handle_internal_columns_dne(
          &mut self.ctx(),
          gr_query_es,
          subquery_id.clone(),
          rem_cols,
        )
      } else {
        trans_read_es.handle_internal_query_error(&mut self.ctx(), msg::QueryError::LateralError)
      };
      self.handle_trans_es_action(statuses, query_id, action);
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      let es = cast!(FullMSTableWriteES::Executing, ms_write_es).unwrap();
      let executing = cast!(MSWriteExecutionS::Executing, &mut es.state).unwrap();
      self.common_handle_internal_columns_dne(
        executing,
        query_id,
        es.timestamp.clone(),
        subquery_id,
        rem_cols,
      );
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      let es = cast!(FullMSTableReadES::Executing, ms_read_es).unwrap();
      let executing = cast!(MSReadExecutionS::Executing, &mut es.state).unwrap();
      self.common_handle_internal_columns_dne(
        executing,
        query_id,
        es.timestamp.clone(),
        subquery_id,
        rem_cols,
      );
    }
  }

  /// We get this if a ReadProtection was granted by the Main Loop. This includes standard
  /// read_protected, or m_read_protected.
  fn read_protected_for_query(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    protect_query_id: QueryId,
  ) {
    let query_id = orig_p.query_id;
    if let Some(read_es) = statuses.full_table_read_ess.get_mut(&query_id) {
      let es = cast!(FullTableReadES::Executing, read_es).unwrap();
      match &mut es.state {
        ExecutionS::Start => panic!(),
        ExecutionS::Pending(pending) => {
          let gr_query_statuses = match compute_subqueries::<T, _, _>(
            GRQueryConstructorView {
              root_query_path: &es.root_query_path,
              tier_map: &es.tier_map,
              timestamp: &es.timestamp,
              sql_query: &es.sql_query,
              query_plan: &es.query_plan,
              query_id: &es.query_id,
              context: &es.context,
            },
            &mut self.rand,
            StorageLocalTable::new(
              &self.table_schema,
              &es.timestamp,
              &es.sql_query.selection,
              SimpleStorageView::new(&self.storage),
            ),
          ) {
            Ok(gr_query_statuses) => gr_query_statuses,
            Err(eval_error) => {
              self.abort_with_query_error(statuses, &query_id, mk_eval_error(eval_error));
              return;
            }
          };

          // Here, we have computed all GRQueryESs, and we can now add them to Executing.
          let mut gr_query_ids = Vec::<QueryId>::new();
          let mut subqueries = Vec::<SingleSubqueryStatus>::new();
          for gr_query_es in gr_query_statuses {
            let query_id = gr_query_es.query_id.clone();
            gr_query_ids.push(query_id.clone());
            subqueries.push(SingleSubqueryStatus::Pending(SubqueryPending {
              context: gr_query_es.context.clone(),
              query_id: query_id.clone(),
            }));
            statuses.gr_query_ess.insert(query_id, gr_query_es);
          }

          // Move the ES to the Executing state.
          es.state = ExecutionS::Executing(Executing {
            completed: 0,
            subqueries,
            row_region: pending.read_region.row_region.clone(),
          });

          // Drive GRQueries
          for query_id in gr_query_ids {
            if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
              // Generally, we use an `if` guard in case one child Query aborts the parent and
              // thus all other children. (This won't happen for GRQueryESs, though)
              let action = es.start::<T>(&mut self.ctx());
              self.handle_gr_query_es_action(statuses, query_id, action);
            }
          }
        }
        ExecutionS::Executing(executing) => {
          let (subquery_id, gr_query_es) = match recompute_subquery::<T, _, _>(
            GRQueryConstructorView {
              root_query_path: &es.root_query_path,
              tier_map: &es.tier_map,
              timestamp: &es.timestamp,
              sql_query: &es.sql_query,
              query_plan: &es.query_plan,
              query_id: &es.query_id,
              context: &es.context,
            },
            &mut self.rand,
            StorageLocalTable::new(
              &self.table_schema,
              &es.timestamp,
              &es.sql_query.selection,
              SimpleStorageView::new(&self.storage),
            ),
            executing,
            &protect_query_id,
          ) {
            Ok(gr_query_statuses) => gr_query_statuses,
            Err(eval_error) => {
              self.abort_with_query_error(statuses, &query_id, mk_eval_error(eval_error));
              return;
            }
          };

          // Add in the GRQueryES to `table_statuses`, and start evaluating the it.
          let es = map_insert(&mut statuses.gr_query_ess, &subquery_id, gr_query_es);
          let action = es.start::<T>(&mut self.ctx());
          self.handle_gr_query_es_action(statuses, subquery_id, action);
        }
      }
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      let es = cast!(FullMSTableWriteES::Executing, ms_write_es).unwrap();
      match &mut es.state {
        MSWriteExecutionS::Start => panic!(),
        MSWriteExecutionS::Pending(pending) => {
          let ms_query_es = statuses.ms_query_ess.get(&es.ms_query_id).unwrap();
          let gr_query_statuses = match compute_subqueries::<T, _, _>(
            GRQueryConstructorView {
              root_query_path: &es.root_query_path,
              tier_map: &es.tier_map,
              timestamp: &es.timestamp,
              sql_query: &es.sql_query,
              query_plan: &es.query_plan,
              query_id: &es.query_id,
              context: &es.context,
            },
            &mut self.rand,
            StorageLocalTable::new(
              &self.table_schema,
              &es.timestamp,
              &es.sql_query.selection,
              MSStorageView::new(&self.storage, &ms_query_es.update_views, es.tier.clone()),
            ),
          ) {
            Ok(gr_query_statuses) => gr_query_statuses,
            Err(eval_error) => {
              let ms_query_id = es.ms_query_id.clone();
              self.abort_ms_with_query_error(statuses, ms_query_id, mk_eval_error(eval_error));
              return;
            }
          };

          // Here, we have computed all GRQueryESs, and we can now add them to Executing.
          let mut gr_query_ids = Vec::<QueryId>::new();
          let mut subqueries = Vec::<SingleSubqueryStatus>::new();
          for gr_query_es in gr_query_statuses {
            let query_id = gr_query_es.query_id.clone();
            gr_query_ids.push(query_id.clone());
            subqueries.push(SingleSubqueryStatus::Pending(SubqueryPending {
              context: gr_query_es.context.clone(),
              query_id: query_id.clone(),
            }));
            statuses.gr_query_ess.insert(query_id, gr_query_es);
          }

          // Move the ES to the Executing state.
          es.state = MSWriteExecutionS::Executing(Executing {
            completed: 0,
            subqueries,
            row_region: pending.read_region.row_region.clone(),
          });

          // Drive GRQueries
          for query_id in gr_query_ids {
            if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
              // Generally, we use an `if` guard in case one child Query aborts the parent and
              // thus all other children. (This won't happen for GRQueryESs, though)
              let action = es.start::<T>(&mut self.ctx());
              self.handle_gr_query_es_action(statuses, query_id, action);
            }
          }
        }
        MSWriteExecutionS::Executing(executing) => {
          let ms_query_es = statuses.ms_query_ess.get(&es.ms_query_id).unwrap();
          let (subquery_id, gr_query_es) = match recompute_subquery::<T, _, _>(
            GRQueryConstructorView {
              root_query_path: &es.root_query_path,
              tier_map: &es.tier_map,
              timestamp: &es.timestamp,
              sql_query: &es.sql_query,
              query_plan: &es.query_plan,
              query_id: &es.query_id,
              context: &es.context,
            },
            &mut self.rand,
            StorageLocalTable::new(
              &self.table_schema,
              &es.timestamp,
              &es.sql_query.selection,
              MSStorageView::new(&self.storage, &ms_query_es.update_views, es.tier.clone()),
            ),
            executing,
            &protect_query_id,
          ) {
            Ok(gr_query_statuses) => gr_query_statuses,
            Err(eval_error) => {
              self.abort_ms_with_query_error(statuses, query_id, mk_eval_error(eval_error));
              return;
            }
          };

          // Add in the GRQueryES to `table_statuses`, and start evaluating the it.
          let es = map_insert(&mut statuses.gr_query_ess, &subquery_id, gr_query_es);
          let action = es.start::<T>(&mut self.ctx());
          self.handle_gr_query_es_action(statuses, subquery_id, action);
        }
      }
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      let es = cast!(FullMSTableReadES::Executing, ms_read_es).unwrap();
      match &mut es.state {
        MSReadExecutionS::Start => panic!(),
        MSReadExecutionS::Pending(pending) => {
          let ms_query_es = statuses.ms_query_ess.get(&es.ms_query_id).unwrap();
          let gr_query_statuses = match compute_subqueries::<T, _, _>(
            GRQueryConstructorView {
              root_query_path: &es.root_query_path,
              tier_map: &es.tier_map,
              timestamp: &es.timestamp,
              sql_query: &es.sql_query,
              query_plan: &es.query_plan,
              query_id: &es.query_id,
              context: &es.context,
            },
            &mut self.rand,
            StorageLocalTable::new(
              &self.table_schema,
              &es.timestamp,
              &es.sql_query.selection,
              MSStorageView::new(&self.storage, &ms_query_es.update_views, es.tier.clone()),
            ),
          ) {
            Ok(gr_query_statuses) => gr_query_statuses,
            Err(eval_error) => {
              let ms_query_id = es.ms_query_id.clone();
              self.abort_ms_with_query_error(statuses, ms_query_id, mk_eval_error(eval_error));
              return;
            }
          };

          // Here, we have computed all GRQueryESs, and we can now add them to Executing.
          let mut gr_query_ids = Vec::<QueryId>::new();
          let mut subqueries = Vec::<SingleSubqueryStatus>::new();
          for gr_query_es in gr_query_statuses {
            let query_id = gr_query_es.query_id.clone();
            gr_query_ids.push(query_id.clone());
            subqueries.push(SingleSubqueryStatus::Pending(SubqueryPending {
              context: gr_query_es.context.clone(),
              query_id: query_id.clone(),
            }));
            statuses.gr_query_ess.insert(query_id, gr_query_es);
          }

          // Move the ES to the Executing state.
          es.state = MSReadExecutionS::Executing(Executing {
            completed: 0,
            subqueries,
            row_region: pending.read_region.row_region.clone(),
          });

          // Drive GRQueries
          for query_id in gr_query_ids {
            if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
              // Generally, we use an `if` guard in case one child Query aborts the parent and
              // thus all other children. (This won't happen for GRQueryESs, though)
              let action = es.start::<T>(&mut self.ctx());
              self.handle_gr_query_es_action(statuses, query_id, action);
            }
          }
        }
        MSReadExecutionS::Executing(executing) => {
          let ms_query_es = statuses.ms_query_ess.get(&es.ms_query_id).unwrap();
          let (subquery_id, gr_query_es) = match recompute_subquery::<T, _, _>(
            GRQueryConstructorView {
              root_query_path: &es.root_query_path,
              tier_map: &es.tier_map,
              timestamp: &es.timestamp,
              sql_query: &es.sql_query,
              query_plan: &es.query_plan,
              query_id: &es.query_id,
              context: &es.context,
            },
            &mut self.rand,
            StorageLocalTable::new(
              &self.table_schema,
              &es.timestamp,
              &es.sql_query.selection,
              MSStorageView::new(&self.storage, &ms_query_es.update_views, es.tier.clone()),
            ),
            executing,
            &protect_query_id,
          ) {
            Ok(gr_query_statuses) => gr_query_statuses,
            Err(eval_error) => {
              self.abort_ms_with_query_error(statuses, query_id, mk_eval_error(eval_error));
              return;
            }
          };

          // Add in the GRQueryES to `table_statuses`, and start evaluating the it.
          let es = map_insert(&mut statuses.gr_query_ess, &subquery_id, gr_query_es);
          let action = es.start::<T>(&mut self.ctx());
          self.handle_gr_query_es_action(statuses, subquery_id, action);
        }
      }
    }
  }

  fn handle_query_success(&mut self, statuses: &mut Statuses, query_success: msg::QuerySuccess) {
    let tm_query_id = &query_success.query_id;
    if let Some(tm_status) = statuses.tm_statuss.get_mut(tm_query_id) {
      // We just add the result of the `query_success` here.
      let tm_wait_value = tm_status.tm_state.get_mut(&query_success.query_id).unwrap();
      *tm_wait_value = TMWaitValue::Result(query_success.result.clone());
      tm_status.new_rms.extend(query_success.new_rms);
      tm_status.responded_count += 1;
      if tm_status.responded_count == tm_status.tm_state.len() {
        // Remove the `TMStatus` and take ownership
        let tm_status = statuses.tm_statuss.remove(&query_success.query_id).unwrap();
        // Merge there TableViews together
        let mut results = Vec::<(Vec<ColName>, Vec<TableView>)>::new();
        for (_, tm_wait_value) in tm_status.tm_state {
          results.push(cast!(TMWaitValue::Result, tm_wait_value).unwrap());
        }
        let merged_result = merge_table_views(results);
        let gr_query_id = tm_status.orig_p.query_id;
        let es = statuses.gr_query_ess.get_mut(&gr_query_id).unwrap();
        let action = es.handle_tm_success(
          &mut self.ctx(),
          tm_query_id.clone(),
          tm_status.new_rms,
          merged_result,
        );
        self.handle_gr_query_es_action(statuses, gr_query_id, action);
      }
    }
  }

  /// This function processes the result of a GRQueryES.
  fn handle_gr_query_done(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    subquery_new_rms: HashSet<QueryPath>,
    (table_schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    let query_id = orig_p.query_id;
    if let Some(read_es) = statuses.full_table_read_ess.get_mut(&query_id) {
      let es = cast!(FullTableReadES::Executing, read_es).unwrap();

      // Add the subquery results into the TableReadES.
      es.new_rms.extend(subquery_new_rms);
      let executing_state = cast!(ExecutionS::Executing, &mut es.state).unwrap();
      let subquery_idx = executing_state.find_subquery(&subquery_id).unwrap();
      let single_status = executing_state.subqueries.get_mut(subquery_idx).unwrap();
      let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
      *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
        context: context.clone(),
        result: table_views,
      });
      executing_state.completed += 1;

      // If all subqueries have been evaluated, finish the TableReadES
      // and respond to the client.
      if executing_state.completed == executing_state.subqueries.len() {
        // Compute children.
        let mut children = Vec::<(Vec<ColName>, Vec<TransTableName>)>::new();
        for single_status in &executing_state.subqueries {
          let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
          let context_schema = &result.context.context_schema;
          children.push((
            context_schema.column_context_schema.clone(),
            context_schema.trans_table_names(),
          ));
        }

        // Create the ContextConstructor.
        let context_constructor = ContextConstructor::new(
          es.context.context_schema.clone(),
          StorageLocalTable::new(
            &self.table_schema,
            &es.timestamp,
            &es.sql_query.selection,
            SimpleStorageView::new(&self.storage),
          ),
          children,
        );

        // These are all of the `ColNames` we need to evaluate things.
        let mut top_level_cols_set = HashSet::<ColName>::new();
        top_level_cols_set.extend(collect_top_level_cols(&es.sql_query.selection));
        top_level_cols_set.extend(es.sql_query.projection.clone());
        let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

        // Finally, iterate over the Context Rows of the subqueries and compute the final values.
        let mut res_table_views = Vec::<TableView>::new();
        for _ in 0..es.context.context_rows.len() {
          res_table_views.push(TableView::new(es.sql_query.projection.clone()));
        }

        let eval_res = context_constructor.run(
          &es.context.context_rows,
          top_level_col_names.clone(),
          &mut |context_row_idx: usize,
                top_level_col_vals: Vec<ColValN>,
                contexts: Vec<(ContextRow, usize)>,
                count: u64| {
            // First, we extract the subquery values using the child Context indices.
            let mut subquery_vals = Vec::<TableView>::new();
            for index in 0..contexts.len() {
              let (_, child_context_idx) = contexts.get(index).unwrap();
              let executing_state = cast!(ExecutionS::Executing, &es.state).unwrap();
              let single_status = executing_state.subqueries.get(index).unwrap();
              let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
              subquery_vals.push(result.result.get(*child_context_idx).unwrap().clone());
            }

            // Now, we evaluate all expressions in the SQL query and amend the
            // result to this TableView (if the WHERE clause evaluates to true).
            let evaluated_select = evaluate_super_simple_select(
              &es.sql_query,
              &top_level_col_names,
              &top_level_col_vals,
              &subquery_vals,
            )?;
            if is_true(&evaluated_select.selection)? {
              // This means that the current row should be selected for the result. Thus, we take
              // the values of the project columns and insert it into the appropriate TableView.
              let mut res_row = Vec::<ColValN>::new();
              for res_col_name in &es.sql_query.projection {
                let idx = top_level_col_names.iter().position(|k| res_col_name == k).unwrap();
                res_row.push(top_level_col_vals.get(idx).unwrap().clone());
              }

              res_table_views[context_row_idx].add_row_multi(res_row, count);
            };
            Ok(())
          },
        );

        if let Err(eval_error) = eval_res {
          self.abort_with_query_error(statuses, &query_id, mk_eval_error(eval_error));
          return;
        }

        // Build the success message and respond.
        let success_msg = msg::QuerySuccess {
          return_path: es.sender_path.query_id.clone(),
          query_id: query_id.clone(),
          result: (es.sql_query.projection.clone(), res_table_views),
          new_rms: es.new_rms.iter().cloned().collect(),
        };
        let sender_path = es.sender_path.clone();
        self.ctx().send_to_path(sender_path, CommonQuery::QuerySuccess(success_msg));

        // Remove the TableReadES.
        statuses.remove(&query_id);
      }
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      let prefix = trans_read_es.location_prefix();
      let action = if let Some(gr_query_es) = statuses.gr_query_ess.get(&prefix.query_id) {
        trans_read_es.handle_subquery_done(
          &mut self.ctx(),
          gr_query_es,
          subquery_id,
          subquery_new_rms,
          (table_schema, table_views),
        )
      } else {
        trans_read_es.handle_internal_query_error(&mut self.ctx(), msg::QueryError::LateralError)
      };
      self.handle_trans_es_action(statuses, query_id, action);
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      let es = cast!(FullMSTableWriteES::Executing, ms_write_es).unwrap();

      // Add the subquery results into the MSTableWriteES.
      es.new_rms.extend(subquery_new_rms);
      let executing_state = cast!(MSWriteExecutionS::Executing, &mut es.state).unwrap();
      let subquery_idx = executing_state.find_subquery(&subquery_id).unwrap();
      let single_status = executing_state.subqueries.get_mut(subquery_idx).unwrap();
      let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
      *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
        context: context.clone(),
        result: table_views,
      });
      executing_state.completed += 1;

      // If all subqueries have been evaluated, finish the MSTableWriteES
      // and respond to the client.
      if executing_state.completed == executing_state.subqueries.len() {
        // Compute children.
        let mut children = Vec::<(Vec<ColName>, Vec<TransTableName>)>::new();
        for single_status in &executing_state.subqueries {
          let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
          let context_schema = &result.context.context_schema;
          children.push((
            context_schema.column_context_schema.clone(),
            context_schema.trans_table_names(),
          ));
        }

        // Create the ContextConstructor.
        let update_views = &statuses.ms_query_ess.get_mut(&es.ms_query_id).unwrap().update_views;
        let storage_view = MSStorageView::new(&self.storage, update_views, es.tier.clone() - 1);
        let context_constructor = ContextConstructor::new(
          es.context.context_schema.clone(),
          StorageLocalTable::new(
            &self.table_schema,
            &es.timestamp,
            &es.sql_query.selection,
            storage_view,
          ),
          children,
        );

        // These are all of the `ColNames` we need to evaluate the Update. This consists of all
        // Top-Level Columns for every expression, as well as all Key Columns (since they are
        // included in the projected columns).
        let mut top_level_cols_set = HashSet::<ColName>::new();
        top_level_cols_set.extend(self.table_schema.get_key_cols());
        top_level_cols_set.extend(collect_top_level_cols(&es.sql_query.selection));
        for (_, expr) in &es.sql_query.assignment {
          top_level_cols_set.extend(collect_top_level_cols(expr));
        }
        let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

        // Setup the TableView that we are going to return and the UpdateView that we're going
        // to hold in the MSQueryES.
        let mut res_col_names = Vec::<ColName>::new();
        res_col_names.extend(self.table_schema.get_key_cols());
        res_col_names.extend(es.sql_query.assignment.iter().map(|(name, _)| name.clone()));
        let mut res_table_view = TableView::new(res_col_names.clone());
        let mut update_view = GenericTable::new();

        // Finally, iterate over the Context Rows of the subqueries and compute the final values.
        let eval_res = context_constructor.run(
          &es.context.context_rows,
          top_level_col_names.clone(),
          &mut |context_row_idx: usize,
                top_level_col_vals: Vec<ColValN>,
                contexts: Vec<(ContextRow, usize)>,
                count: u64| {
            assert_eq!(context_row_idx, 0); // Recall there is only one ContextRow for Updates.

            // First, we extract the subquery values using the child Context indices.
            let mut subquery_vals = Vec::<TableView>::new();
            for index in 0..contexts.len() {
              let (_, child_context_idx) = contexts.get(index).unwrap();
              let executing_state = cast!(MSWriteExecutionS::Executing, &es.state).unwrap();
              let single_status = executing_state.subqueries.get(index).unwrap();
              let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
              subquery_vals.push(result.result.get(*child_context_idx).unwrap().clone());
            }

            // Now, we evaluate all expressions in the SQL query and amend the
            // result to this TableView (if the WHERE clause evaluates to true).
            let evaluated_update = evaluate_update(
              &es.sql_query,
              &top_level_col_names,
              &top_level_col_vals,
              &subquery_vals,
            )?;
            if is_true(&evaluated_update.selection)? {
              // This means that the current row should be selected for the result.
              let mut res_row = Vec::<ColValN>::new();

              // First we add in the Key Columns
              let mut primary_key = PrimaryKey { cols: vec![] };
              for (key_col, _) in &self.table_schema.key_cols {
                let idx = top_level_col_names.iter().position(|col| key_col == col).unwrap();
                let col_val = top_level_col_vals.get(idx).unwrap().clone();
                res_row.push(col_val.clone());
                primary_key.cols.push(col_val.unwrap());
              }

              // Then, iterate through the assignment, updating `res_row` and `update_view`.
              for (col_name, col_val) in evaluated_update.assignment {
                res_row.push(col_val.clone());
                update_view.insert((primary_key.clone(), Some(col_name)), col_val);
              }

              // Finally, we add the `res_row` into the TableView.
              res_table_view.add_row_multi(res_row, count);
            };
            Ok(())
          },
        );

        if let Err(eval_error) = eval_res {
          let ms_query_id = es.ms_query_id.clone();
          self.abort_ms_with_query_error(statuses, ms_query_id, mk_eval_error(eval_error));
          return;
        }

        // Build the success message and respond.
        let success_msg = msg::QuerySuccess {
          return_path: es.sender_path.query_id.clone(),
          query_id: query_id.clone(),
          result: (res_col_names, vec![res_table_view]),
          new_rms: es.new_rms.iter().cloned().collect(),
        };
        let sender_path = es.sender_path.clone();
        self.ctx().send_to_path(sender_path, CommonQuery::QuerySuccess(success_msg));

        // Update the MSQuery. In particular, amend the `update_view` and remove this
        // MSTableWriteES from the pending queries.
        let ms_query = statuses.ms_query_ess.get_mut(&es.ms_query_id).unwrap();
        ms_query.pending_queries.remove(&query_id);
        ms_query.update_views.insert(es.tier.clone(), update_view);

        // Remove the MSTableWriteES.
        statuses.remove(&query_id);
      }
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      let es = cast!(FullMSTableReadES::Executing, ms_read_es).unwrap();

      // Add the subquery results into the MSTableReadES.
      es.new_rms.extend(subquery_new_rms);
      let executing_state = cast!(MSReadExecutionS::Executing, &mut es.state).unwrap();
      let subquery_idx = executing_state.find_subquery(&subquery_id).unwrap();
      let single_status = executing_state.subqueries.get_mut(subquery_idx).unwrap();
      let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
      *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
        context: context.clone(),
        result: table_views,
      });
      executing_state.completed += 1;

      // If all subqueries have been evaluated, finish the MSTableReadES
      // and respond to the client.
      if executing_state.completed == executing_state.subqueries.len() {
        // Compute children.
        let mut children = Vec::<(Vec<ColName>, Vec<TransTableName>)>::new();
        for single_status in &executing_state.subqueries {
          let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
          let context_schema = &result.context.context_schema;
          children.push((
            context_schema.column_context_schema.clone(),
            context_schema.trans_table_names(),
          ));
        }

        // Create the ContextConstructor.
        let update_views = &statuses.ms_query_ess.get_mut(&es.ms_query_id).unwrap().update_views;
        let storage_view = MSStorageView::new(&self.storage, update_views, es.tier.clone() - 1);
        let context_constructor = ContextConstructor::new(
          es.context.context_schema.clone(),
          StorageLocalTable::new(
            &self.table_schema,
            &es.timestamp,
            &es.sql_query.selection,
            storage_view,
          ),
          children,
        );

        // These are all of the `ColNames` we need to evaluate things.
        let mut top_level_cols_set = HashSet::<ColName>::new();
        top_level_cols_set.extend(collect_top_level_cols(&es.sql_query.selection));
        top_level_cols_set.extend(es.sql_query.projection.clone());
        let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

        // Finally, iterate over the Context Rows of the subqueries and compute the final values.
        let mut res_table_views = Vec::<TableView>::new();
        for _ in 0..es.context.context_rows.len() {
          res_table_views.push(TableView::new(es.sql_query.projection.clone()));
        }

        let eval_res = context_constructor.run(
          &es.context.context_rows,
          top_level_col_names.clone(),
          &mut |context_row_idx: usize,
                top_level_col_vals: Vec<ColValN>,
                contexts: Vec<(ContextRow, usize)>,
                count: u64| {
            // First, we extract the subquery values using the child Context indices.
            let mut subquery_vals = Vec::<TableView>::new();
            for index in 0..contexts.len() {
              let (_, child_context_idx) = contexts.get(index).unwrap();
              let executing_state = cast!(MSReadExecutionS::Executing, &es.state).unwrap();
              let single_status = executing_state.subqueries.get(index).unwrap();
              let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
              subquery_vals.push(result.result.get(*child_context_idx).unwrap().clone());
            }

            // Now, we evaluate all expressions in the SQL query and amend the
            // result to this TableView (if the WHERE clause evaluates to true).
            let evaluated_select = evaluate_super_simple_select(
              &es.sql_query,
              &top_level_col_names,
              &top_level_col_vals,
              &subquery_vals,
            )?;
            if is_true(&evaluated_select.selection)? {
              // This means that the current row should be selected for the result. Thus, we take
              // the values of the project columns and insert it into the appropriate TableView.
              let mut res_row = Vec::<ColValN>::new();
              for res_col_name in &es.sql_query.projection {
                let idx = top_level_col_names.iter().position(|k| res_col_name == k).unwrap();
                res_row.push(top_level_col_vals.get(idx).unwrap().clone());
              }

              res_table_views[context_row_idx].add_row_multi(res_row, count);
            };
            Ok(())
          },
        );

        if let Err(eval_error) = eval_res {
          let ms_query_id = es.ms_query_id.clone();
          self.abort_ms_with_query_error(statuses, ms_query_id, mk_eval_error(eval_error));
          return;
        }

        // Build the success message and respond.
        let success_msg = msg::QuerySuccess {
          return_path: es.sender_path.query_id.clone(),
          query_id: query_id.clone(),
          result: (es.sql_query.projection.clone(), res_table_views),
          new_rms: es.new_rms.iter().cloned().collect(),
        };
        let sender_path = es.sender_path.clone();
        self.ctx().send_to_path(sender_path, CommonQuery::QuerySuccess(success_msg));

        // Remove the TableReadES.
        statuses.remove(&query_id);
      }
    }
  }

  fn handle_query_aborted(&mut self, statuses: &mut Statuses, query_aborted: msg::QueryAborted) {
    if let Some(tm_status) = statuses.tm_statuss.remove(&query_aborted.return_path) {
      // We Exit and Clean up this TMStatus (sending CancelQuery to all
      // remaining participants) and send the QueryAborted back to the orig_p
      for (node_group_id, child_query_id) in tm_status.node_group_ids {
        if tm_status.tm_state.get(&child_query_id).unwrap() == &TMWaitValue::Nothing
          && child_query_id != query_aborted.query_id
        {
          // If the child Query hasn't responded yet, and isn't also the Query that
          // just aborted, then we send it a CancelQuery
          self.ctx().send_to_node(
            node_group_id,
            CommonQuery::CancelQuery(msg::CancelQuery { query_id: child_query_id }),
          );
        }
      }

      // Finally, we propagate up the AbortData to the GRQueryES that owns this TMStatus
      let gr_query_id = tm_status.orig_p.query_id;
      let es = statuses.gr_query_ess.get_mut(&gr_query_id).unwrap();
      let action = es.handle_tm_aborted(&mut self.ctx(), query_aborted.payload);
      self.handle_gr_query_es_action(statuses, gr_query_id, action);
    }
  }

  /// This routes the QueryError propagated by a GRQueryES up to the appropriate top-level ES.
  fn handle_internal_query_error(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    query_error: msg::QueryError,
  ) {
    let query_id = orig_p.query_id;
    if statuses.full_table_read_ess.contains_key(&query_id) {
      self.abort_with_query_error(statuses, &query_id, query_error)
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      let action = trans_read_es.handle_internal_query_error(&mut self.ctx(), query_error);
      self.handle_trans_es_action(statuses, query_id, action);
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get(&query_id) {
      let ms_query_id = ms_write_es.ms_query_id().clone();
      self.abort_ms_with_query_error(statuses, ms_query_id, query_error);
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get(&query_id) {
      let ms_query_id = ms_read_es.ms_query_id().clone();
      self.abort_ms_with_query_error(statuses, ms_query_id, query_error);
    }
  }

  /// Handles the actions specified by a TransTableReadES.
  fn handle_trans_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: TransTableAction,
  ) {
    match action {
      TransTableAction::Wait => {}
      TransTableAction::SendSubqueries(gr_query_ess) => {
        /// Here, we have to add in the GRQueryESs and start them.
        let mut subquery_ids = Vec::<QueryId>::new();
        for gr_query_es in gr_query_ess {
          let subquery_id = gr_query_es.query_id.clone();
          statuses.gr_query_ess.insert(subquery_id.clone(), gr_query_es);
          subquery_ids.push(subquery_id);
        }

        // Drive GRQueries
        for query_id in subquery_ids {
          if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
            // Generally, we use an `if` guard in case one child Query aborts the parent and
            // thus all other children. (This won't happen for GRQueryESs, though)
            let action = es.start::<T>(&mut self.ctx());
            self.handle_gr_query_es_action(statuses, query_id, action);
          }
        }
      }
      TransTableAction::Done => {
        // Recall that all responses will have been sent. Here, the ES should not be
        // holding onto any other resources, so we can simply remove it.
        statuses.full_trans_table_read_ess.remove(&query_id);
      }
      TransTableAction::ExitAndCleanUp(subquery_ids) => {
        // Recall that all responses will have been sent. There only resources that the ES
        // has are subqueries, so we Exit and Clean Up them here.
        statuses.full_trans_table_read_ess.remove(&query_id);
        for subquery_id in subquery_ids {
          self.exit_and_clean_up(statuses, subquery_id);
        }
      }
    }
  }

  /// Handles the actions specified by a GRQueryES.
  fn handle_gr_query_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: GRQueryAction,
  ) {
    match action {
      GRQueryAction::ExecuteTMStatus(tm_status) => {
        statuses.tm_statuss.insert(tm_status.query_id.clone(), tm_status);
      }
      GRQueryAction::Done(res) => {
        let es = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_gr_query_done(
          statuses,
          es.orig_p,
          es.query_id,
          res.new_rms,
          (res.schema, res.result),
        );
      }
      GRQueryAction::InternalColumnsDNE(rem_cols) => {
        let es = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_internal_columns_dne(statuses, es.orig_p, es.query_id, rem_cols);
      }
      GRQueryAction::QueryError(query_error) => {
        let es = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_internal_query_error(statuses, es.orig_p, query_error);
      }
      GRQueryAction::ExitAndCleanUp(subquery_ids) => {
        // Recall that all responses will have been sent. There only resources that the ES
        // has are subqueries, so we Exit and Clean Up them here.
        statuses.gr_query_ess.remove(&query_id);
        for subquery_id in subquery_ids {
          self.exit_and_clean_up(statuses, subquery_id);
        }
      }
    }
  }

  /// This function is used to initiate an Exit and Clean Up of ESs. This is needed to handle
  /// CancelQuery's, as well as when one on ES wants to Exit and Clean Up another ES. Note that
  /// We allow the ES at `query_id` to be in any state, and to not even exist.
  fn exit_and_clean_up(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
      // Here, we only take a `&mut` to the GRQueryES, since `handle_trans_es_action`
      // needs it to be present before deleting it.
      let action = es.exit_and_clean_up(&mut self.ctx());
      self.handle_gr_query_es_action(statuses, query_id, action);
    } else if let Some(read_es) = statuses.full_table_read_ess.remove(&query_id) {
      match read_es {
        FullTableReadES::QueryReplanning(es) => self.exit_planning(es.status.state),
        FullTableReadES::Executing(es) => match es.state {
          ExecutionS::Start => {}
          ExecutionS::Pending(pending) => {
            // Here, we remove the ReadRegion from `waiting_read_protected`, if it still exists.
            self.remove_read_protected_request(es.timestamp.clone(), pending.query_id);
          }
          ExecutionS::Executing(executing) => {
            // Here, we need to cancel every Subquery. Depending on the state of the
            // SingleSubqueryStatus, we either need to either clean up the column locking request,
            // the ReadRegion from read protection, or abort the underlying GRQueryES.
            for single_status in executing.subqueries {
              match single_status {
                SingleSubqueryStatus::LockingSchemas(locking_status) => {
                  self.remove_col_locking_request(locking_status.query_id);
                }
                SingleSubqueryStatus::PendingReadRegion(protect_status) => {
                  let protect_query_id = protect_status.query_id;
                  self.remove_read_protected_request(es.timestamp.clone(), protect_query_id);
                }
                SingleSubqueryStatus::Pending(pending_status) => {
                  self.exit_and_clean_up(statuses, pending_status.query_id);
                }
                SingleSubqueryStatus::Finished(_) => {}
              }
            }
          }
        },
      }
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      // Here, we only take a `&mut` to the TransTableRead, since `handle_gr_query_es_action`
      // needs it to be present before deleting it.
      let action = trans_read_es.exit_and_clean_up(&mut self.ctx());
      self.handle_trans_es_action(statuses, query_id, action);
    } else if let Some(tm_status) = statuses.tm_statuss.remove(&query_id) {
      // We Exit and Clean up this TMStatus (sending CancelQuery to all remaining participants)
      for (node_group_id, child_query_id) in tm_status.node_group_ids {
        if tm_status.tm_state.get(&child_query_id).unwrap() == &TMWaitValue::Nothing {
          // If the child Query hasn't responded, then sent it a CancelQuery
          self.ctx().send_to_node(
            node_group_id,
            CommonQuery::CancelQuery(msg::CancelQuery { query_id: child_query_id }),
          );
        }
      }
    } else if let Some(ms_query_es) = statuses.ms_query_ess.remove(&query_id) {
      // Note: this code is only run when a CancelQuery comes in (from the Slave) for the
      // MSQueryES. We don't remove the MSQueryES via this code otherwise, since the Abort
      // message for the `pending_queries` to propagate up will generally vary. This anomaly is
      // rooted in the fact that MSQueryES does not own the MTable*ESs.
      for query_id in ms_query_es.pending_queries {
        self.abort_with_query_error(statuses, &query_id, msg::QueryError::LateralError);
      }
      self.verifying_writes.remove(&ms_query_es.timestamp);
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.remove(&query_id) {
      // Remove the MSTableWriteES from the MSQuery::pending_queries.
      let ms_query = statuses.ms_query_ess.get_mut(ms_write_es.ms_query_id()).unwrap();
      ms_query.pending_queries.remove(&query_id);

      match ms_write_es {
        FullMSTableWriteES::QueryReplanning(es) => {
          // Exit the query Replanning
          self.exit_planning(es.status.state)
        }
        FullMSTableWriteES::Executing(es) => {
          // Exit and Clean Up state-specific resources.
          match es.state {
            MSWriteExecutionS::Start => {}
            MSWriteExecutionS::Pending(pending) => {
              // Here, we remove the ReadRegion from `m_waiting_read_protected`, if it still
              // exists.

              // Note that we leave `m_read_protected` and `m_write_protected` in-tact
              // since they are inconvenient to change (since they don't have `query_id`.
              self.remove_m_read_protected_request(es.timestamp.clone(), pending.query_id);
            }
            MSWriteExecutionS::Executing(executing) => {
              // Here, we need to cancel every Subquery. Depending on the state of the
              // SingleSubqueryStatus, we either need to either clean up the column locking request,
              // the ReadRegion from m_waiting_read_protected, or abort the underlying GRQueryES.

              // Note that we leave `m_read_protected` and `m_write_protected` in-tact
              // since they are inconvenient to change (since they don't have `query_id`.
              for single_status in executing.subqueries {
                match single_status {
                  SingleSubqueryStatus::LockingSchemas(locking_status) => {
                    self.remove_col_locking_request(locking_status.query_id);
                  }
                  SingleSubqueryStatus::PendingReadRegion(protect_status) => {
                    let protect_query_id = protect_status.query_id;
                    self.remove_m_read_protected_request(es.timestamp.clone(), protect_query_id);
                  }
                  SingleSubqueryStatus::Pending(pending_status) => {
                    self.exit_and_clean_up(statuses, pending_status.query_id);
                  }
                  SingleSubqueryStatus::Finished(_) => {}
                }
              }
            }
          }
        }
      }
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.remove(&query_id) {
      // Remove the MSTableReadES from the MSQuery::pending_queries.
      let ms_query = statuses.ms_query_ess.get_mut(ms_read_es.ms_query_id()).unwrap();
      ms_query.pending_queries.remove(&query_id);

      match ms_read_es {
        FullMSTableReadES::QueryReplanning(es) => {
          // Exit the query Replanning
          self.exit_planning(es.status.state)
        }
        FullMSTableReadES::Executing(es) => {
          // Exit and Clean Up state-specific resources.
          match es.state {
            MSReadExecutionS::Start => {}
            MSReadExecutionS::Pending(pending) => {
              // Here, we remove the ReadRegion from `m_waiting_read_protected`, if it still
              // exists.

              // Note that we leave `m_read_protected` and `m_write_protected` in-tact
              // since they are inconvenient to change (since they don't have `query_id`.
              self.remove_m_read_protected_request(es.timestamp.clone(), pending.query_id);
            }
            MSReadExecutionS::Executing(executing) => {
              // Here, we need to cancel every Subquery. Depending on the state of the
              // SingleSubqueryStatus, we either need to either clean up the column locking request,
              // the ReadRegion from m_waiting_read_protected, or abort the underlying GRQueryES.

              // Note that we leave `m_read_protected` and `m_write_protected` in-tact
              // since they are inconvenient to change (since they don't have `query_id`.
              for single_status in executing.subqueries {
                match single_status {
                  SingleSubqueryStatus::LockingSchemas(locking_status) => {
                    self.remove_col_locking_request(locking_status.query_id);
                  }
                  SingleSubqueryStatus::PendingReadRegion(protect_status) => {
                    let protect_query_id = protect_status.query_id;
                    self.remove_m_read_protected_request(es.timestamp.clone(), protect_query_id);
                  }
                  SingleSubqueryStatus::Pending(pending_status) => {
                    self.exit_and_clean_up(statuses, pending_status.query_id);
                  }
                  SingleSubqueryStatus::Finished(_) => {}
                }
              }
            }
          }
        }
      }
    }
  }

  fn exit_planning(&mut self, state: CommonQueryReplanningS) {
    match state {
      CommonQueryReplanningS::Start => {}
      CommonQueryReplanningS::ProjectedColumnLocking { locked_columns_query_id } => {
        self.remove_col_locking_request(locked_columns_query_id.clone());
      }
      CommonQueryReplanningS::ColumnLocking { locked_columns_query_id } => {
        self.remove_col_locking_request(locked_columns_query_id.clone());
      }
      CommonQueryReplanningS::RecomputeQueryPlan { locked_columns_query_id, .. } => {
        self.remove_col_locking_request(locked_columns_query_id.clone());
      }
      CommonQueryReplanningS::MasterQueryReplanning { master_query_id } => {
        // Remove if present
        if self.master_query_map.remove(&master_query_id).is_some() {
          // If the removal was successful, we should also send a Cancellation
          // message to the Master.
          self.network_output.send(
            &self.master_eid,
            msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
              msg::CancelMasterFrozenColUsage { query_id: master_query_id },
            )),
          );
        }
      }
      CommonQueryReplanningS::Done(_) => {}
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Old Table Subquery Construction
// -----------------------------------------------------------------------------------------------

/// This is used to compute the Keybound of a selection expression for
/// a given ContextRow containing external columns. Recall that the
/// selection expression might have ColumnRefs that aren't part of
/// the TableSchema (i.e. part of the external Context), and we can use
/// that to compute a tigher Keybound.
struct ContextKeyboundComputer {
  /// The `ColName`s here are the ones we must read from the Context, and the `usize`
  /// points to them in the ContextRows.
  context_col_index: HashMap<ColName, usize>,
  selection: proc::ValExpr,
  /// These are the KeyColumns we are trying to tighten.
  key_cols: Vec<(ColName, ColType)>,
}

impl ContextKeyboundComputer {
  /// Here, `selection` is the the filtering expression. `parent_context_schema` is the set
  /// of External columns whose values we can fill in (during `compute_keybounds` calls)
  /// to help reduce the KeyBound. However, we need `table_schema` and the `timestamp` of
  /// the Query to determine which of these External columns are shadowed so we can avoid
  /// using them.
  ///
  /// Formally, `selection`'s Top-Level Cols must all be locked in `table_schema` at `timestamp`.
  /// They must either be in the `table_schema` or in the `parent_context_schema`.
  fn new(
    selection: &proc::ValExpr,
    table_schema: &TableSchema,
    timestamp: &Timestamp,
    parent_context_schema: &ContextSchema,
  ) -> ContextKeyboundComputer {
    // Compute the ColNames directly used in the `selection` that's not part of
    // the TableSchema (i.e. part of the external Context).
    let mut external_cols = HashSet::<ColName>::new();
    for col in collect_top_level_cols(selection) {
      if !contains_col(table_schema, &col, timestamp) {
        external_cols.insert(col);
      }
    }

    /// Map the `external_cols` to their position in the parent context. Recall that
    /// every element `external_cols` should exist in the parent_context, so we
    /// assert as such.
    let mut context_col_index = HashMap::<ColName, usize>::new();
    for (index, col) in parent_context_schema.column_context_schema.iter().enumerate() {
      if external_cols.contains(col) {
        context_col_index.insert(col.clone(), index);
      }
    }
    assert_eq!(context_col_index.len(), external_cols.len());

    ContextKeyboundComputer {
      context_col_index,
      selection: selection.clone(),
      key_cols: table_schema.key_cols.clone(),
    }
  }

  /// Compute the tightest keybound for the given `parent_context_row`.
  ///
  /// Formally, these `parent_context_row` must correspond to the `parent_context_schema`
  /// provided in the constructor.
  fn compute_keybounds(&self, parent_context_row: &ContextRow) -> Result<Vec<KeyBound>, EvalError> {
    // First, map all External Columns names to the corresponding values
    // in this ContextRow
    let mut col_context = HashMap::<ColName, ColValN>::new();
    for (col, index) in &self.context_col_index {
      let col_val = parent_context_row.column_context_row.get(*index).unwrap().clone();
      col_context.insert(col.clone(), col_val);
    }

    // Then, compute the keybound
    compute_key_region(&self.selection, col_context, &self.key_cols)
  }
}

// -----------------------------------------------------------------------------------------------
//  New Table Subquery Construction
// -----------------------------------------------------------------------------------------------

/// This runs the `ContextConstructor` with the given inputs and simply accumulates the
/// `ContextRow` to produce a `Context` for each element in `children`.
pub fn compute_contexts<LocalTableT: LocalTable>(
  parent_context: &Context,
  local_table: LocalTableT,
  children: Vec<(Vec<ColName>, Vec<TransTableName>)>,
) -> Result<Vec<Context>, EvalError> {
  // Create the ContextConstruct.
  let context_constructor =
    ContextConstructor::new(parent_context.context_schema.clone(), local_table, children);

  // Initialize the child Contexts
  let mut child_contexts = Vec::<Context>::new();
  for schema in context_constructor.get_schemas() {
    child_contexts.push(Context::new(schema));
  }

  // Create the child Contexts.
  let callback = &mut |context_row_idx: usize,
                       top_level_col_vals: Vec<ColValN>,
                       contexts: Vec<(ContextRow, usize)>,
                       count: u64| {
    for (subquery_idx, (context_row, idx)) in contexts.into_iter().enumerate() {
      let child_context = child_contexts.get_mut(subquery_idx).unwrap();
      if idx == child_context.context_rows.len() {
        // This is a new ContextRow, so add it in.
        child_context.context_rows.push(context_row);
      }
    }

    Ok(())
  };
  context_constructor.run(&parent_context.context_rows, Vec::new(), callback)?;
  Ok(child_contexts)
}

/// This computes GRQueryESs corresponding to every element in `subqueries`.
fn compute_subqueries<T: IOTypes, LocalTableT: LocalTable, SqlQueryT: SubqueryComputableSql>(
  subquery_view: GRQueryConstructorView<SqlQueryT>,
  rand: &mut T::RngCoreT,
  local_table: LocalTableT,
) -> Result<Vec<GRQueryES>, EvalError> {
  // Here, we construct first construct all of the subquery Contexts using the
  // ContextConstructor, and then we construct GRQueryESs.

  // Compute children.
  let mut children = Vec::<(Vec<ColName>, Vec<TransTableName>)>::new();
  for child in &subquery_view.query_plan.col_usage_node.children {
    let mut col_name_set = HashSet::<ColName>::new();
    for (_, (_, node)) in child {
      col_name_set.extend(node.external_cols.clone())
    }
    children.push((col_name_set.iter().cloned().collect(), nodes_external_trans_tables(child)));
  }

  // Create the child context.
  let child_contexts = compute_contexts(subquery_view.context, local_table, children)?;

  // We compute all GRQueryESs.
  let mut gr_query_statuses = Vec::<GRQueryES>::new();
  for (subquery_idx, child_context) in child_contexts.into_iter().enumerate() {
    gr_query_statuses.push(subquery_view.mk_gr_query_es(
      mk_qid(rand),
      Rc::new(child_context),
      subquery_idx,
    ));
  }

  Ok(gr_query_statuses)
}

/// This recomputes GRQueryESs that corresponds to `protect_query_id`.
fn recompute_subquery<T: IOTypes, LocalTableT: LocalTable, SqlQueryT: SubqueryComputableSql>(
  subquery_view: GRQueryConstructorView<SqlQueryT>,
  rand: &mut T::RngCoreT,
  local_table: LocalTableT,
  executing: &mut Executing,
  protect_query_id: &QueryId,
) -> Result<(QueryId, GRQueryES), EvalError> {
  // Find the subquery that this `protect_query_id` is referring to. There should
  // always be such a Subquery.
  let subquery_idx = executing.find_subquery(protect_query_id).unwrap();
  let single_status = executing.subqueries.get_mut(subquery_idx).unwrap();
  let protect_status = cast!(SingleSubqueryStatus::PendingReadRegion, single_status).unwrap();

  // Create the child context.
  let child_contexts = compute_contexts(
    subquery_view.context,
    local_table,
    vec![(protect_status.new_columns.clone(), protect_status.trans_table_names.clone())],
  )?;
  assert_eq!(child_contexts.len(), 1);

  // Construct the GRQueryES
  let context = Rc::new(child_contexts.into_iter().next().unwrap());
  let gr_query_id = mk_qid(rand);
  let gr_query_es =
    subquery_view.mk_gr_query_es(gr_query_id.clone(), context.clone(), subquery_idx);

  // Advance the SingleSubqueryStatus.
  *single_status =
    SingleSubqueryStatus::Pending(SubqueryPending { context, query_id: gr_query_id.clone() });

  Ok((gr_query_id, gr_query_es))
}

// -----------------------------------------------------------------------------------------------
//  Query Replanning
// -----------------------------------------------------------------------------------------------

impl<R: QueryReplanningSqlView> CommonQueryReplanningES<R> {
  fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    matches!(self.state, CommonQueryReplanningS::Start);
    // Add column locking
    let locked_cols_qid = ctx.add_requested_locked_columns(
      OrigP::new(self.query_id.clone()),
      self.timestamp,
      self.sql_view.projected_cols(&ctx.table_schema),
    );
    self.state =
      CommonQueryReplanningS::ProjectedColumnLocking { locked_columns_query_id: locked_cols_qid };
  }

  /// This is called when Columns have been locked, and the originator was this ES.
  /// Note: The reason that `start` above is not just another case statement in this function
  /// because functions are generally  suppose to take in arguments specific to the states
  /// that they are transitioning. however, many of the other states do wait for column locking.
  fn columns_locked<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    match &self.state {
      CommonQueryReplanningS::ProjectedColumnLocking { .. } => {
        // We need to verify that the projected columns are present.
        for col in &self.sql_view.projected_cols(&ctx.table_schema) {
          if !contains_col(&ctx.table_schema, col, &self.timestamp) {
            // This means a projected column doesn't exist. Thus, we Exit and Clean Up.
            ctx.ctx().send_query_error(
              self.sender_path.clone(),
              self.query_id.clone(),
              msg::QueryError::ProjectedColumnsDNE { msg: String::new() },
            );
            self.state = CommonQueryReplanningS::Done(false);
            return;
          }
        }

        // Next, we check if the `gen` in the query plan is more recent than the
        // current Tablet's `gen`, and we act accordingly.
        if ctx.gossip.gossip_gen <= self.query_plan.gossip_gen {
          // Lock all of the `external_cols` and `safe_present` columns in this query plan.
          let node = &self.query_plan.col_usage_node;
          let mut all_cols = node.external_cols.clone();
          all_cols.extend(node.safe_present_cols.iter().cloned());

          // Add column locking
          let locked_cols_qid = ctx.add_requested_locked_columns(
            OrigP::new(self.query_id.clone()),
            self.timestamp.clone(),
            all_cols,
          );

          // Advance Read Status
          self.state =
            CommonQueryReplanningS::ColumnLocking { locked_columns_query_id: locked_cols_qid };
        } else {
          // Replan the query. First, we need to gather the set of `ColName`s touched by the
          // query, and then we need to lock them.
          let mut planner = ColUsagePlanner {
            gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
            timestamp: self.timestamp,
          };

          let all_cols = planner
            .get_all_cols(&mut self.query_plan.trans_table_schemas.clone(), &self.sql_view.exprs());

          // Add column locking
          let locked_cols_qid = ctx.add_requested_locked_columns(
            OrigP::new(self.query_id.clone()),
            self.timestamp.clone(),
            all_cols.clone(),
          );

          // Advance Read Status
          self.state = CommonQueryReplanningS::RecomputeQueryPlan {
            locked_columns_query_id: locked_cols_qid,
            locking_cols: all_cols,
          };
        }
      }
      CommonQueryReplanningS::ColumnLocking { .. } => {
        // Now, we need to verify that the `external_cols` are all absent and the
        // `safe_present_cols` are all present.

        let does_query_plan_align = (|| {
          // First, check that `external_cols are absent.
          for col in &self.query_plan.col_usage_node.external_cols {
            // Since the key_cols are static, no query plan should have a
            // key_col as an external col. Thus, we assert.
            assert!(lookup_pos(&ctx.table_schema.key_cols, col).is_none());
            if ctx.table_schema.val_cols.strong_static_read(&col, self.timestamp).is_some() {
              return false;
            }
          }
          // Next, check that `safe_present_cols` are present.
          for col in &self.query_plan.col_usage_node.safe_present_cols {
            if !contains_col(&ctx.table_schema, col, &self.timestamp) {
              return false;
            }
          }
          return true;
        })();

        if does_query_plan_align {
          // Here, QueryReplanning is finished and we may start executing the query.
          self.state = CommonQueryReplanningS::Done(true);
        } else {
          // This means the current schema didn't line up with the QueryPlan.
          // The only explanation is that the schema had changed very recently.
          // Thus, we re-plan the query.
          assert!(self.query_plan.gossip_gen > ctx.gossip.gossip_gen);
          let mut planner = ColUsagePlanner {
            gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
            timestamp: self.timestamp,
          };

          let all_cols = planner
            .get_all_cols(&mut self.query_plan.trans_table_schemas.clone(), &self.sql_view.exprs());

          // Add column locking
          let locked_cols_qid = ctx.add_requested_locked_columns(
            OrigP::new(self.query_id.clone()),
            self.timestamp.clone(),
            all_cols.clone(),
          );

          // Advance Read Status
          self.state = CommonQueryReplanningS::RecomputeQueryPlan {
            locked_columns_query_id: locked_cols_qid,
            locking_cols: all_cols,
          };
        }
      }
      CommonQueryReplanningS::RecomputeQueryPlan { locking_cols, .. } => {
        // This means have provisinally locked the necessary columns. However,
        // while we were locking, the current Gossip could have updated. Thus,
        // we need to make sure we actually have indeed locked the necessary
        // columns, and re-lock otherwise.
        let mut planner = ColUsagePlanner {
          gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
          timestamp: self.timestamp,
        };

        let all_cols = planner
          .get_all_cols(&mut self.query_plan.trans_table_schemas.clone(), &self.sql_view.exprs());

        let mut did_lock_all = true;
        for col in all_cols.clone() {
          did_lock_all |= locking_cols.contains(&col);
        }

        if !did_lock_all {
          // Add column locking
          let locked_cols_qid = ctx.add_requested_locked_columns(
            OrigP::new(self.query_id.clone()),
            self.timestamp.clone(),
            all_cols.clone(),
          );
          // Repeat this state
          self.state = CommonQueryReplanningS::RecomputeQueryPlan {
            locked_columns_query_id: locked_cols_qid,
            locking_cols: all_cols,
          };
          return;
        }

        // Here, we have locked all necessary columns and we can finish the QueryPlan. However,
        // if we find that the context is insufficient, we need to contact the Master.

        // Compute `schema_cols`, the cols that are present among all relevent cols.
        let mut schema_cols = Vec::<ColName>::new();
        for col in locking_cols {
          if contains_col(&ctx.table_schema, col, &self.timestamp) {
            schema_cols.push(col.clone());
          }
        }

        // Construct a new QueryPlan using the above `schema_cols`.
        let mut planner = ColUsagePlanner {
          gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
          timestamp: self.timestamp,
        };

        let (_, col_usage_node) = planner.plan_stage_query_with_schema(
          &mut self.query_plan.trans_table_schemas.clone(),
          &self.sql_view.projected_cols(&ctx.table_schema), // TODO: we shouldn't have to pass this in.
          &proc::TableRef::TablePath(self.sql_view.table().clone()),
          schema_cols,
          &self.sql_view.exprs(),
        );

        let external_cols = col_usage_node.external_cols.clone();
        self.query_plan.gossip_gen = ctx.gossip.gossip_gen;
        self.query_plan.col_usage_node = col_usage_node;

        // Next, we check to see if all ColNames in `external_cols` is contaiend
        // in the context. If not, we have to consult the Master.
        for col in external_cols {
          if !self.context.context_schema.column_context_schema.contains(&col) {
            // This means we need to consult the Master.
            let master_query_id = mk_qid(&mut ctx.rand);

            ctx.network_output.send(
              &ctx.master_eid,
              msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
                msg::PerformMasterFrozenColUsage {
                  query_id: master_query_id.clone(),
                  timestamp: self.timestamp,
                  trans_table_schemas: self.query_plan.trans_table_schemas.clone(),
                  col_usage_tree: msg::ColUsageTree::MSQueryStage(self.sql_view.ms_query_stage()),
                },
              )),
            );
            ctx.master_query_map.insert(master_query_id.clone(), OrigP::new(self.query_id.clone()));

            // Advance Read Status
            self.state = CommonQueryReplanningS::MasterQueryReplanning { master_query_id };
            return;
          }
        }

        // If we get here that means we have successfully computed a valid QueryPlan,
        // and we are done.
        self.state = CommonQueryReplanningS::Done(true);
      }
      _ => panic!(),
    }
  }
}
