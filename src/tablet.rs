use crate::col_usage::{
  collect_select_subqueries, collect_top_level_cols, collect_update_subqueries,
  node_external_trans_tables, nodes_external_cols, nodes_external_trans_tables, ColUsagePlanner,
  FrozenColUsageNode,
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
use crate::ms_table_read_es::{FullMSTableReadES, MSReadQueryReplanningES, MSTableReadAction};
use crate::ms_table_write_es::{FullMSTableWriteES, MSTableWriteAction, MSWriteQueryReplanningES};
use crate::multiversion_map::MVM;
use crate::server::{
  contains_col, evaluate_super_simple_select, evaluate_update, mk_eval_error, CommonQuery,
  ContextConstructor, LocalTable, ServerContext,
};
use crate::table_read_es::{FullTableReadES, QueryReplanningES, TableAction};
use crate::trans_table_read_es::{
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
pub enum CommonQueryReplanningS {
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
pub struct CommonQueryReplanningES<T: QueryReplanningSqlView> {
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
  pub old_columns: Vec<ColName>,
  pub trans_table_names: Vec<TransTableName>,
  pub new_cols: Vec<ColName>,
  pub query_id: QueryId,
}

#[derive(Debug)]
pub struct SubqueryPendingReadRegion {
  pub new_columns: Vec<ColName>,
  pub trans_table_names: Vec<TransTableName>,
  pub read_region: TableRegion,
  pub query_id: QueryId,
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
//  Common Execution States
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct Pending {
  pub read_region: TableRegion,
  pub query_id: QueryId,
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

// -----------------------------------------------------------------------------------------------
//  MSQueryES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct MSQueryES {
  pub root_query_id: QueryId,
  pub query_id: QueryId,
  pub timestamp: Timestamp,
  pub update_views: BTreeMap<u32, GenericTable>,
  pub pending_queries: HashSet<QueryId>,
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
pub struct VerifyingReadWriteRegion {
  pub orig_p: OrigP,
  pub m_waiting_read_protected: BTreeSet<(OrigP, QueryId, TableRegion)>,
  pub m_read_protected: BTreeSet<TableRegion>,
  pub m_write_protected: BTreeSet<TableRegion>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadWriteRegion {
  pub orig_p: OrigP,
  pub m_read_protected: BTreeSet<TableRegion>,
  pub m_write_protected: BTreeSet<TableRegion>,
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
pub trait StorageView {
  fn compute_subtable(
    &self,
    key_region: &Vec<KeyBound>,
    col_region: &Vec<ColName>,
  ) -> (Vec<ColName>, Vec<(Vec<ColValN>, u64)>);
}

/// This is used to directly read data from persistent storage.
pub struct SimpleStorageView<'a> {
  storage: &'a GenericMVTable,
}

impl<'a> SimpleStorageView<'a> {
  pub fn new(storage: &GenericMVTable) -> SimpleStorageView {
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
pub struct MSStorageView<'a> {
  storage: &'a GenericMVTable,
  update_views: &'a BTreeMap<u32, GenericTable>,
  tier: u32,
}

impl<'a> MSStorageView<'a> {
  /// Here, in addition to the persistent storage, we pass in the `update_views` that
  /// we want to apply on top before reading data, and a `tier` to indicate up to which
  /// update should be applied.
  pub fn new(
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

pub struct StorageLocalTable<'a, StorageViewT: StorageView> {
  table_schema: &'a TableSchema,
  /// The Timestamp which we are reading data at.
  timestamp: &'a Timestamp,
  /// The row-filtering expression (i.e. WHERE clause) to compute subtables with.
  selection: &'a proc::ValExpr,
  /// This is used to compute the KeyBound
  storage: StorageViewT,
}

impl<'a, StorageViewT: StorageView> StorageLocalTable<'a, StorageViewT> {
  pub fn new(
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

pub type GenericMVTable = MVM<(PrimaryKey, Option<ColName>), ColValN>;
pub type GenericTable = HashMap<(PrimaryKey, Option<ColName>), ColValN>;

#[derive(Debug)]
pub struct TabletState<T: IOTypes> {
  tablet_context: TabletContext<T>,
  statuses: Statuses,
}

#[derive(Debug)]
pub struct TabletContext<T: IOTypes> {
  /// IO Objects.
  pub rand: T::RngCoreT,
  pub clock: T::ClockT,
  pub network_output: T::NetworkOutT,

  /// Metadata
  pub this_slave_group_id: SlaveGroupId,
  pub this_tablet_group_id: TabletGroupId,
  pub master_eid: EndpointId,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// Distribution
  pub sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  // Storage
  pub storage: GenericMVTable,
  pub this_table_path: TablePath,
  pub this_table_key_range: TabletKeyRange,
  pub table_schema: TableSchema,

  // Region Isolation Algorithm
  pub verifying_writes: BTreeMap<Timestamp, VerifyingReadWriteRegion>,
  pub prepared_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub committed_writes: BTreeMap<Timestamp, ReadWriteRegion>,

  // TODO: make (OrigP, QueryId, TableRegion) into a proper type.
  pub waiting_read_protected: BTreeMap<Timestamp, BTreeSet<(OrigP, QueryId, TableRegion)>>,
  pub read_protected: BTreeMap<Timestamp, BTreeSet<TableRegion>>,

  // Schema Change and Locking
  pub prepared_schema_change: BTreeMap<Timestamp, HashMap<ColName, Option<ColType>>>,
  pub request_index: BTreeMap<Timestamp, BTreeSet<QueryId>>, // Used to help iterate requests in order.
  pub requested_locked_columns: HashMap<QueryId, (OrigP, Timestamp, Vec<ColName>)>,

  /// Child Queries
  pub ms_root_query_map: HashMap<QueryId, QueryId>,
  pub master_query_map: HashMap<QueryId, OrigP>,
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
  pub fn ctx(&mut self) -> ServerContext<T> {
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
                      root_query_id: root_query_id.clone(),
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

              // Create an MSReadTableES in the QueryReplanning state, and add it to
              // the MSQueryES.
              let ms_read_es = map_insert(
                &mut statuses.full_ms_table_read_ess,
                &perform_query.query_id,
                FullMSTableReadES::QueryReplanning(MSReadQueryReplanningES {
                  root_query_path: perform_query.root_query_path,
                  tier_map: perform_query.tier_map,
                  ms_query_id,
                  status: CommonQueryReplanningES {
                    timestamp: query.timestamp,
                    context: Rc::new(query.context),
                    sql_view: query.sql_query.clone(),
                    query_plan: query.query_plan,
                    sender_path: perform_query.sender_path,
                    query_id: perform_query.query_id.clone(),
                    state: CommonQueryReplanningS::Start,
                  },
                }),
              );
              let action = ms_read_es.start(self);
              self.handle_ms_read_es_action(statuses, perform_query.query_id, action);
            } else {
              let read_es = map_insert(
                &mut statuses.full_table_read_ess,
                &perform_query.query_id,
                FullTableReadES::QueryReplanning(QueryReplanningES {
                  root_query_path: perform_query.root_query_path,
                  tier_map: perform_query.tier_map,
                  status: CommonQueryReplanningES {
                    timestamp: query.timestamp,
                    context: Rc::new(query.context),
                    sql_view: query.sql_query.clone(),
                    query_plan: query.query_plan,
                    sender_path: perform_query.sender_path,
                    query_id: perform_query.query_id.clone(),
                    state: CommonQueryReplanningS::Start,
                  },
                }),
              );
              let action = read_es.start(self);
              self.handle_read_es_action(statuses, perform_query.query_id, action);
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
                    root_query_id: root_query_id.clone(),
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
            let ms_write_es = map_insert(
              &mut statuses.full_ms_table_write_ess,
              &perform_query.query_id,
              FullMSTableWriteES::QueryReplanning(MSWriteQueryReplanningES {
                root_query_path: perform_query.root_query_path,
                tier_map: perform_query.tier_map,
                ms_query_id,
                status: CommonQueryReplanningES {
                  timestamp: query.timestamp,
                  context: Rc::new(query.context),
                  sql_view: query.sql_query.clone(),
                  query_plan: query.query_plan,
                  sender_path: perform_query.sender_path,
                  query_id: perform_query.query_id.clone(),
                  state: CommonQueryReplanningS::Start,
                },
              }),
            );
            let action = ms_write_es.start(self);
            self.handle_ms_write_es_action(statuses, perform_query.query_id, action);
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
  pub fn add_requested_locked_columns(
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
  pub fn remove_col_locking_request(
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
  pub fn remove_read_protected_request(
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
  pub fn remove_m_read_protected_request(
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

  pub fn check_write_region_isolation(
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
      let action = read_es.columns_locked(self, locked_cols_qid);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      let action = ms_write_es.columns_locked(self, locked_cols_qid);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      let action = ms_read_es.columns_locked(self, locked_cols_qid);
      self.handle_ms_read_es_action(statuses, query_id, action);
    }
  }

  fn get_path(&self, statuses: &Statuses, query_id: &QueryId) -> QueryPath {
    if let Some(read_es) = statuses.full_table_read_ess.get(query_id) {
      read_es.sender_path()
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get(&query_id) {
      trans_read_es.sender_path()
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get(&query_id) {
      ms_write_es.sender_path().clone()
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get(&query_id) {
      ms_read_es.sender_path().clone()
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
    // TODO: address this properly, then delete the above.
    self.abort_ms_with_query_error(
      statuses,
      orig_p.query_id,
      msg::QueryError::DeadlockSafetyAbortion,
    );
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
      let action = read_es.handle_internal_columns_dne(self, subquery_id, rem_cols);
      self.handle_read_es_action(statuses, query_id, action);
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
      let action = ms_write_es.handle_internal_columns_dne(self, subquery_id, rem_cols);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      let action = ms_read_es.handle_internal_columns_dne(self, subquery_id, rem_cols);
      self.handle_ms_read_es_action(statuses, query_id, action);
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
      let action = read_es.read_protected(self, protect_query_id);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      let action = ms_write_es.read_protected(self, &statuses.ms_query_ess, protect_query_id);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      let action = ms_read_es.read_protected(self, &statuses.ms_query_ess, protect_query_id);
      self.handle_ms_read_es_action(statuses, query_id, action);
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
      let action = read_es.handle_subquery_done(
        self,
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      );
      self.handle_read_es_action(statuses, query_id, action);
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
      let action = ms_write_es.handle_subquery_done(
        self,
        &mut statuses.ms_query_ess,
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      );
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      let action = ms_read_es.handle_subquery_done(
        self,
        &statuses.ms_query_ess,
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      );
      self.handle_ms_read_es_action(statuses, query_id, action);
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
    if let Some(read_es) = statuses.full_table_read_ess.get_mut(&query_id) {
      // Send InternalQueryError to TableReadES
      let action = read_es.handle_internal_query_error(self, query_error);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      // Send InternalQueryError to TransTableReadES
      let action = trans_read_es.handle_internal_query_error(&mut self.ctx(), query_error);
      self.handle_trans_es_action(statuses, query_id, action);
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      // Send InternalQueryError to MSTableWriteES
      let action = ms_write_es.handle_internal_query_error(self, query_error);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      // Send InternalQueryError to MSTableReadES
      let action = ms_read_es.handle_internal_query_error(self, query_error);
      self.handle_ms_read_es_action(statuses, query_id, action);
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

  /// Handles the actions specified by a TableReadES.
  fn handle_read_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: TableAction,
  ) {
    match action {
      TableAction::Wait => {}
      TableAction::SendSubqueries(gr_query_ess) => {
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
      TableAction::Done => {
        // Recall that all responses will have been sent. Here, the ES should not be
        // holding onto any other resources, so we can simply remove it.
        statuses.full_table_read_ess.remove(&query_id);
      }
      TableAction::ExitAndCleanUp(subquery_ids) => {
        // Recall that all responses will have been sent. There only resources that the ES
        // has are subqueries, so we Exit and Clean Up them here.
        statuses.full_table_read_ess.remove(&query_id);
        for subquery_id in subquery_ids {
          self.exit_and_clean_up(statuses, subquery_id);
        }
      }
    }
  }

  /// Handles the actions specified by a MSTableWriteES.
  fn handle_ms_write_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: MSTableWriteAction,
  ) {
    match action {
      MSTableWriteAction::Wait => {}
      MSTableWriteAction::SendSubqueries(gr_query_ess) => {
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
      MSTableWriteAction::Done => {
        // Simply remove the MSTableReadES.
        let ms_write_es = statuses.full_ms_table_write_ess.remove(&query_id).unwrap();
        // Remove the MSTableWriteES from the MSQuery::pending_queries.
        let ms_query_es = statuses.ms_query_ess.get_mut(ms_write_es.ms_query_id()).unwrap();
        ms_query_es.pending_queries.remove(&query_id);
      }
      MSTableWriteAction::ExitAll(query_error) => {
        // First, we get the associate MSQueryES.
        let ms_write_es = statuses.full_ms_table_write_ess.get(&query_id).unwrap();
        let ms_query_id = ms_write_es.ms_query_id().clone();
        let ms_query_es = statuses.ms_query_ess.get(&ms_query_id).unwrap();

        // Then, we Exit and Clean Up all ESs in MSQuery::pending_queries.
        for query_id in ms_query_es.pending_queries.clone() {
          if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
            // Here, this `query_id` is an MSTableReadES, and we abort it.
            let action = ms_read_es.handle_internal_query_error(self, query_error.clone());
            self.handle_ms_read_es_action(statuses, query_id.clone(), action);
          } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
            // Here, this `query_id` is an MSTableWriteES, and we abort it.
            let action = ms_write_es.handle_internal_query_error(self, query_error.clone());
            self.handle_ms_write_es_action(statuses, query_id.clone(), action);
          }
        }

        // Finally, we remove  Clean the MSQueryES, making sure to clean up TabletContext too.
        let ms_query_es = statuses.ms_query_ess.remove(&ms_query_id).unwrap();
        assert!(ms_query_es.pending_queries.is_empty()); // All ES should have been removed.
        self.verifying_writes.remove(&ms_query_es.timestamp).unwrap();
        self.ms_root_query_map.remove(&ms_query_es.root_query_id).unwrap();
      }
      MSTableWriteAction::ExitAndCleanUp(subquery_ids) => {
        // Recall that all responses will have been sent. We clean up all Subqueries here
        // and also update the MSQueryES.
        let ms_write_es = statuses.full_ms_table_write_ess.remove(&query_id).unwrap();
        // Remove the MSTableWriteES from the MSQuery::pending_queries.
        let ms_query = statuses.ms_query_ess.get_mut(ms_write_es.ms_query_id()).unwrap();
        ms_query.pending_queries.remove(&query_id);
        // Abort all of the subqueries.
        for subquery_id in subquery_ids {
          self.exit_and_clean_up(statuses, subquery_id);
        }
      }
    }
  }

  /// Handles the actions specified by a MSTableReadES.
  fn handle_ms_read_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: MSTableReadAction,
  ) {
    match action {
      MSTableReadAction::Wait => {}
      MSTableReadAction::SendSubqueries(gr_query_ess) => {
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
      MSTableReadAction::Done => {
        // Simply remove the MSTableReadES.
        let ms_read_es = statuses.full_ms_table_read_ess.remove(&query_id).unwrap();
        // Remove the MSTableReadES from the MSQuery::pending_queries.
        let ms_query_es = statuses.ms_query_ess.get_mut(ms_read_es.ms_query_id()).unwrap();
        ms_query_es.pending_queries.remove(&query_id);
      }
      MSTableReadAction::ExitAll(query_error) => {
        // First, we get the associate MSQueryES.
        let ms_read_es = statuses.full_ms_table_read_ess.get(&query_id).unwrap();
        let ms_query_id = ms_read_es.ms_query_id().clone();
        let ms_query_es = statuses.ms_query_ess.get(&ms_query_id).unwrap();

        // Then, we Exit and Clean Up all ESs in MSQuery::pending_queries.
        for query_id in ms_query_es.pending_queries.clone() {
          if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
            // Here, this `query_id` is an MSTableReadES, and we abort it.
            let action = ms_read_es.handle_internal_query_error(self, query_error.clone());
            self.handle_ms_read_es_action(statuses, query_id.clone(), action);
          } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
            // Here, this `query_id` is an MSTableWriteES, and we abort it.
            let action = ms_write_es.handle_internal_query_error(self, query_error.clone());
            self.handle_ms_write_es_action(statuses, query_id.clone(), action);
          }
        }

        // Finally, we remove  Clean the MSQueryES, making sure to clean up TabletContext too.
        let ms_query_es = statuses.ms_query_ess.remove(&ms_query_id).unwrap();
        assert!(ms_query_es.pending_queries.is_empty()); // All ES should have been removed.
        self.verifying_writes.remove(&ms_query_es.timestamp).unwrap();
        self.ms_root_query_map.remove(&ms_query_es.root_query_id).unwrap();
      }
      MSTableReadAction::ExitAndCleanUp(subquery_ids) => {
        // Recall that all responses will have been sent. We clean up all Subqueries here
        // and also update the MSQueryES.
        let ms_read_es = statuses.full_ms_table_read_ess.remove(&query_id).unwrap();
        // Remove the MSTableReadES from the MSQuery::pending_queries.
        let ms_query = statuses.ms_query_ess.get_mut(ms_read_es.ms_query_id()).unwrap();
        ms_query.pending_queries.remove(&query_id);
        // Abort all of the subqueries.
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
    // Here, we only take &mut to all ESs, since their `handle_*_action` functions require
    // them to exist when aborting.
    if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
      // Abort the GRQueryES
      let action = es.exit_and_clean_up(&mut self.ctx());
      self.handle_gr_query_es_action(statuses, query_id, action);
    } else if let Some(read_es) = statuses.full_table_read_ess.get_mut(&query_id) {
      // Abort the TableReadES
      let action = read_es.exit_and_clean_up(self);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      // Abort the TransTableReadES
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
      // TODO: do what we do in the action handlers for MSReadES and MSWriteES.
      for query_id in ms_query_es.pending_queries {
        self.abort_with_query_error(statuses, &query_id, msg::QueryError::LateralError);
      }
      self.verifying_writes.remove(&ms_query_es.timestamp);
    } else if let Some(ms_write_es) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      // Abort the MSTableWriteES. Recall this only returns the subqueries we need to abort.
      // Also, recall that `handle_ms_read_es_action` will maintain the MSQueryES.
      let action = ms_write_es.exit_and_clean_up(self);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read_es) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      // Abort the MSTableReadES. Recall this only returns the subqueries we need to abort.
      // Also, recall that `handle_ms_read_es_action` will maintain the MSQueryES.
      let action = ms_read_es.exit_and_clean_up(self);
      self.handle_ms_read_es_action(statuses, query_id, action);
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
pub struct ContextKeyboundComputer {
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
  pub fn new(
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
  pub fn compute_keybounds(
    &self,
    parent_context_row: &ContextRow,
  ) -> Result<Vec<KeyBound>, EvalError> {
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
pub fn compute_subqueries<T: IOTypes, LocalTableT: LocalTable, SqlQueryT: SubqueryComputableSql>(
  subquery_view: GRQueryConstructorView<SqlQueryT>,
  rand: &mut T::RngCoreT,
  local_table: LocalTableT,
) -> Result<Vec<GRQueryES>, EvalError> {
  // Here, we construct first construct all of the subquery Contexts using the
  // ContextConstructor, and then we construct GRQueryESs.

  // Compute children.
  let mut children = Vec::<(Vec<ColName>, Vec<TransTableName>)>::new();
  for child in &subquery_view.query_plan.col_usage_node.children {
    children.push((nodes_external_cols(child), nodes_external_trans_tables(child)));
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
pub fn recompute_subquery<T: IOTypes, LocalTableT: LocalTable, SqlQueryT: SubqueryComputableSql>(
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
  pub fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
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
  pub fn columns_locked<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
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

  /// This Exits and Cleans up this QueryReplanningES
  pub fn exit_and_clean_up<T: IOTypes>(&self, ctx: &mut TabletContext<T>) {
    match &self.state {
      CommonQueryReplanningS::Start => {}
      CommonQueryReplanningS::ProjectedColumnLocking { locked_columns_query_id } => {
        ctx.remove_col_locking_request(locked_columns_query_id.clone());
      }
      CommonQueryReplanningS::ColumnLocking { locked_columns_query_id } => {
        ctx.remove_col_locking_request(locked_columns_query_id.clone());
      }
      CommonQueryReplanningS::RecomputeQueryPlan { locked_columns_query_id, .. } => {
        ctx.remove_col_locking_request(locked_columns_query_id.clone());
      }
      CommonQueryReplanningS::MasterQueryReplanning { master_query_id } => {
        // Remove if present
        if ctx.master_query_map.remove(&master_query_id).is_some() {
          // If the removal was successful, we should also send a Cancellation
          // message to the Master.
          ctx.network_output.send(
            &ctx.master_eid,
            msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
              msg::CancelMasterFrozenColUsage { query_id: master_query_id.clone() },
            )),
          );
        }
      }
      CommonQueryReplanningS::Done(_) => {}
    }
  }
}
