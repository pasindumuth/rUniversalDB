use crate::col_usage::{
  collect_select_subqueries, collect_top_level_cols, collect_update_subqueries,
  node_external_trans_tables, nodes_external_cols, nodes_external_trans_tables, ColUsagePlanner,
  FrozenColUsageNode,
};
use crate::common::{
  btree_multimap_insert, btree_multimap_remove, lookup, lookup_pos, map_insert, merge_table_views,
  mk_qid, remove_item, ColBound, GossipData, IOTypes, KeyBound, NetworkOut, OrigP, PolyColBound,
  SingleBound, TMStatus, TableRegion, TableSchema,
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
  proc, ColType, ColValN, Context, ContextRow, ContextSchema, Gen, NodeGroupId, NodePath,
  QueryPath, TableView, TierMap, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{
  ColName, ColVal, EndpointId, PrimaryKey, QueryId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::ms_table_read_es::{MSReadExecutionS, MSTableReadAction, MSTableReadES};
use crate::ms_table_write_es::{MSTableWriteAction, MSTableWriteES, MSWriteExecutionS};
use crate::multiversion_map::MVM;
use crate::paxos::LeaderChanged;
use crate::server::{
  are_cols_locked, contains_col, evaluate_super_simple_select, evaluate_update, mk_eval_error,
  CommonQuery, ContextConstructor, LocalTable, ServerContext,
};
use crate::storage::{commit_to_storage, GenericMVTable, GenericTable, StorageView};
use crate::table_read_es::{ExecutionS, TableAction, TableReadES};
use crate::trans_table_read_es::{
  TransExecutionS, TransTableAction, TransTableReadES, TransTableSource,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;
use std::ops::{Add, Bound, Deref, Sub};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  QueryReplanningSqlView
// -----------------------------------------------------------------------------------------------

pub trait QueryReplanningSqlView {
  /// Get the ColNames that must exist in the TableSchema (where it's not sufficient
  /// for the Context to have these columns instead).
  fn required_local_cols(&self) -> Vec<ColName>;
  /// Get the TablePath that the query reads.
  fn table(&self) -> &TablePath;
  /// Get all expressions in the Query (in some deterministic order)
  fn exprs(&self) -> Vec<proc::ValExpr>;
  /// This converts the underlying SQL into an MSQueryStage, useful
  /// for sending it out over the network.
  fn ms_query_stage(&self) -> proc::MSQueryStage;
}

/// Implementation for `SuperSimpleSelect`.
impl QueryReplanningSqlView for proc::SuperSimpleSelect {
  fn required_local_cols(&self) -> Vec<ColName> {
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

/// Implementation for `Update`.
impl QueryReplanningSqlView for proc::Update {
  fn required_local_cols(&self) -> Vec<ColName> {
    self.assignment.iter().map(|(col, _)| col.clone()).collect()
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

// -----------------------------------------------------------------------------------------------
//  SubqueryStatus
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct SubqueryLockingSchemas {
  // Recall that we only get to this State if a Subquery had failed. We hold onto
  // the prior ColNames and TransTableNames (rather than computating from the QueryPlan
  // again) so that we don't potentially lose prior ColName amendments.
  pub old_columns: Vec<ColName>,
  /// Recall that the set of TransTables required by a subquery never changes.
  /// This is only here to construct the QueryPlan later.
  pub trans_table_names: Vec<TransTableName>,
  /// The `ColName`s from the InternalQueryError that was returned from the GRQueryES.
  pub new_cols: Vec<ColName>,
  /// The QueryId of the Column Locking request that this State is waiting for.
  pub query_id: QueryId,
}

#[derive(Debug)]
pub struct SubqueryPendingReadRegion {
  /// This contains all columns from `old_columns`, as well as any new ones from
  /// `new_cols` that's present in this Table that need to be locked.
  pub new_columns: Vec<ColName>,
  /// Recall that the set of TransTables required by a subquery never changes.
  /// This is only here to construct the QueryPlan later.
  pub trans_table_names: Vec<TransTableName>,
  /// The QueryId of the ReadRegion protection request that this State is waiting for.
  pub query_id: QueryId,
}

#[derive(Debug)]
pub struct SubqueryPending {
  pub context: Rc<Context>,
  /// The QueryId of GRQueryES we are waiting for.
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
pub struct ColumnsLocking {
  pub locked_cols_qid: QueryId,
}

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
    for (i, single_status) in self.subqueries.iter().enumerate() {
      match single_status {
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
//  Wrappers
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct TableReadESWrapper {
  sender_path: QueryPath,
  child_queries: Vec<QueryId>,
  es: TableReadES,
}

#[derive(Debug)]
pub struct TransTableReadESWrapper {
  pub sender_path: QueryPath,
  pub child_queries: Vec<QueryId>,
  pub es: TransTableReadES,
}

#[derive(Debug)]
struct MSTableReadESWrapper {
  sender_path: QueryPath,
  child_queries: Vec<QueryId>,
  es: MSTableReadES,
}

#[derive(Debug)]
struct MSTableWriteESWrapper {
  sender_path: QueryPath,
  child_queries: Vec<QueryId>,
  es: MSTableWriteES,
}

#[derive(Debug)]
pub struct GRQueryESWrapper {
  pub child_queries: Vec<QueryId>,
  pub es: GRQueryES,
}

// -----------------------------------------------------------------------------------------------
//  Status
// -----------------------------------------------------------------------------------------------

/// This contains every TabletStatus. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  gr_query_ess: HashMap<QueryId, GRQueryESWrapper>,
  full_table_read_ess: HashMap<QueryId, TableReadESWrapper>,
  full_trans_table_read_ess: HashMap<QueryId, TransTableReadESWrapper>,
  tm_statuss: HashMap<QueryId, TMStatus>,
  ms_query_ess: HashMap<QueryId, MSQueryES>,
  full_ms_table_read_ess: HashMap<QueryId, MSTableReadESWrapper>,
  full_ms_table_write_ess: HashMap<QueryId, MSTableWriteESWrapper>,
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
pub struct ProtectRequest {
  pub orig_p: OrigP,
  pub protect_qid: QueryId,
  pub read_region: TableRegion,
}

#[derive(Debug, Clone)]
pub struct VerifyingReadWriteRegion {
  pub orig_p: OrigP,
  pub m_waiting_read_protected: BTreeSet<ProtectRequest>,
  pub m_read_protected: BTreeSet<TableRegion>,
  pub m_write_protected: BTreeSet<TableRegion>,
}

#[derive(Debug, Clone)]
pub struct ReadWriteRegion {
  pub orig_p: OrigP,
  pub m_read_protected: BTreeSet<TableRegion>,
  pub m_write_protected: BTreeSet<TableRegion>,
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
    Ok(self.storage.compute_subtable(&key_bounds, col_names, self.timestamp))
  }
}

// -----------------------------------------------------------------------------------------------
//  TabletBundle
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePrepared {
  query_id: QueryId,
  alter_op: proc::AlterOp,
  timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableCommitted {
  query_id: QueryId,
  timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAborted {
  query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTablePrepared {
  query_id: QueryId,
  timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableCommitted {
  query_id: QueryId,
  timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableAborted {
  query_id: QueryId,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LockedCols {
  query_id: QueryId,
  timestamp: Timestamp,
  cols: Vec<ColName>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TabletBundle {
  AlterTablePrepared(AlterTablePrepared),
  AlterTableCommitted(AlterTableCommitted),
  AlterTableAborted(AlterTableAborted),
  DropTablePrepared(DropTablePrepared),
  DropTableCommitted(DropTableCommitted),
  DropTableAborted(DropTableAborted),
  LockedCols(LockedCols),
}

// -----------------------------------------------------------------------------------------------
//  TabletForwardMsg
// -----------------------------------------------------------------------------------------------

pub enum TabletForwardMsg {
  TabletBundle(TabletBundle),
  TabletMessage(msg::TabletMessage),
  GossipData(Arc<GossipData>),
  RemoteLeaderChanged(msg::RemoteLeaderChanged),
  LeaderChanged(LeaderChanged),
}

// -----------------------------------------------------------------------------------------------
//  Tablet State
// -----------------------------------------------------------------------------------------------

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

  // Storage
  pub storage: GenericMVTable,
  pub this_table_path: TablePath,
  pub this_table_key_range: TabletKeyRange,
  pub table_schema: TableSchema,

  // Region Isolation Algorithm
  pub verifying_writes: BTreeMap<Timestamp, VerifyingReadWriteRegion>,
  pub prepared_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub committed_writes: BTreeMap<Timestamp, ReadWriteRegion>,

  pub waiting_read_protected: BTreeMap<Timestamp, BTreeSet<ProtectRequest>>,
  pub read_protected: BTreeMap<Timestamp, BTreeSet<TableRegion>>,

  // Schema Change and Locking
  pub alter_table_rm_state: HashMap<QueryId, (proc::AlterOp, Timestamp)>,
  /// This is directly derived from the `alter_table_rm_state`, containing the `lat`
  /// of the given `ColName`. We may not read > this timestamp for ColumnLocking.
  pub prepared_scheme_change: HashMap<ColName, Timestamp>,
  /// This is used to help iterate requests in timestamp order.
  pub request_index: BTreeMap<Timestamp, BTreeSet<QueryId>>,
  pub requested_locked_columns: HashMap<QueryId, (OrigP, Timestamp, Vec<ColName>)>,

  /// Child Queries
  pub ms_root_query_map: HashMap<QueryId, QueryId>,
}

impl<T: IOTypes> TabletState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    gossip: Arc<GossipData>,
    this_slave_group_id: SlaveGroupId,
    this_tablet_group_id: TabletGroupId,
    master_eid: EndpointId,
  ) -> TabletState<T> {
    let (this_table_path, this_table_key_range) = (|| {
      // Search the sharding config, which should contain this data.
      for (path, shards) in &gossip.sharding_config {
        for (key_range, tid) in shards {
          if tid == &this_tablet_group_id {
            return (path.clone(), key_range.clone());
          }
        }
      }
      panic!();
    })();
    let table_schema = gossip.db_schema.get(&this_table_path).unwrap().clone();
    TabletState {
      tablet_context: TabletContext::<T> {
        rand,
        clock,
        network_output,
        this_slave_group_id,
        this_tablet_group_id,
        master_eid,
        gossip,
        storage: GenericMVTable::new(),
        this_table_path,
        this_table_key_range,
        table_schema,
        verifying_writes: Default::default(),
        prepared_writes: Default::default(),
        committed_writes: Default::default(),
        waiting_read_protected: Default::default(),
        read_protected: Default::default(),
        alter_table_rm_state: Default::default(),
        prepared_scheme_change: Default::default(),
        request_index: Default::default(),
        requested_locked_columns: Default::default(),
        ms_root_query_map: Default::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_input(&mut self, coord_input: TabletForwardMsg) {
    self.tablet_context.handle_input(&mut self.statuses, coord_input);
  }

  // TODO: remove
  pub fn handle_incoming_message(&mut self, message: msg::TabletMessage) {
    // self.tablet_context.handle_incoming_message(&mut self.statuses, message);
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
    }
  }

  fn handle_input(&mut self, statuses: &mut Statuses, tablet_input: TabletForwardMsg) {
    match tablet_input {
      TabletForwardMsg::TabletBundle(bundle) => match bundle {
        TabletBundle::AlterTablePrepared(_) => panic!(),
        TabletBundle::AlterTableCommitted(_) => panic!(),
        TabletBundle::AlterTableAborted(_) => panic!(),
        TabletBundle::DropTablePrepared(_) => panic!(),
        TabletBundle::DropTableCommitted(_) => panic!(),
        TabletBundle::DropTableAborted(_) => panic!(),
        TabletBundle::LockedCols(_) => panic!(),
      },
      TabletForwardMsg::TabletMessage(message) => {
        match message {
          msg::TabletMessage::PerformQuery(perform_query) => {
            match perform_query.query {
              msg::GeneralQuery::SuperSimpleTransTableSelectQuery(query) => {
                // First, we check if the GRQueryES still exists in the Statuses, continuing
                // if so and aborting if not.
                if let Some(gr_query) = statuses.gr_query_ess.get(&query.location_prefix.query_id) {
                  // Construct and start the TransQueryReplanningES
                  let full_trans_table = map_insert(
                    &mut statuses.full_trans_table_read_ess,
                    &perform_query.query_id,
                    TransTableReadESWrapper {
                      sender_path: perform_query.sender_path.clone(),
                      child_queries: vec![],
                      es: TransTableReadES {
                        root_query_path: perform_query.root_query_path,
                        location_prefix: query.location_prefix,
                        context: Rc::new(query.context),
                        sender_path: perform_query.sender_path,
                        query_id: perform_query.query_id.clone(),
                        sql_query: query.sql_query,
                        query_plan: query.query_plan,
                        new_rms: Default::default(),
                        state: TransExecutionS::Start,
                        timestamp: gr_query.es.timestamp.clone(),
                      },
                    },
                  );

                  let action = full_trans_table.es.start(&mut self.ctx(), &gr_query.es);
                  self.handle_trans_read_es_action(statuses, perform_query.query_id, action);
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
                if query.query_plan.tier_map.map.contains_key(table_path) {
                  // Here, we create an MSTableReadES.
                  let root_query_path = perform_query.root_query_path;
                  match self.get_msquery_id(statuses, root_query_path.clone(), query.timestamp) {
                    Ok(ms_query_id) => {
                      // Lookup the MSQueryES and add the new Query into `pending_queries`.
                      let ms_query_es = statuses.ms_query_ess.get_mut(&ms_query_id).unwrap();
                      ms_query_es.pending_queries.insert(perform_query.query_id.clone());

                      // Create an MSReadTableES in the QueryReplanning state, and start it.
                      let ms_read = map_insert(
                        &mut statuses.full_ms_table_read_ess,
                        &perform_query.query_id,
                        MSTableReadESWrapper {
                          sender_path: perform_query.sender_path.clone(),
                          child_queries: vec![],
                          es: MSTableReadES {
                            root_query_path,
                            timestamp: query.timestamp,
                            tier: 0,
                            context: Rc::new(query.context),
                            query_id: perform_query.query_id.clone(),
                            sql_query: query.sql_query,
                            query_plan: query.query_plan,
                            ms_query_id,
                            new_rms: Default::default(),
                            state: MSReadExecutionS::Start,
                          },
                        },
                      );
                      let action = ms_read.es.start(self);
                      self.handle_ms_read_es_action(statuses, perform_query.query_id, action);
                    }
                    Err(query_error) => {
                      // The MSQueryES couldn't be constructed.
                      self.ctx().send_query_error(
                        perform_query.sender_path,
                        perform_query.query_id,
                        query_error,
                      );
                    }
                  }
                } else {
                  // Here, we create a standard TableReadES.
                  let read = map_insert(
                    &mut statuses.full_table_read_ess,
                    &perform_query.query_id,
                    TableReadESWrapper {
                      sender_path: perform_query.sender_path.clone(),
                      child_queries: vec![],
                      es: TableReadES {
                        root_query_path: perform_query.root_query_path,
                        timestamp: query.timestamp,
                        context: Rc::new(query.context),
                        query_id: perform_query.query_id.clone(),
                        sql_query: query.sql_query,
                        query_plan: query.query_plan,
                        new_rms: Default::default(),
                        state: ExecutionS::Start,
                      },
                    },
                  );
                  let action = read.es.start(self);
                  self.handle_read_es_action(statuses, perform_query.query_id, action);
                }
              }
              msg::GeneralQuery::UpdateQuery(query) => {
                // We first do some basic verification of the SQL query, namely assert that the
                // assigned columns aren't key columns. (Recall this should be enforced by the Slave.)
                for (col, _) in &query.sql_query.assignment {
                  assert!(lookup(&self.table_schema.key_cols, col).is_none())
                }

                // Here, we create an MSTableWriteES.
                let root_query_path = perform_query.root_query_path;
                match self.get_msquery_id(statuses, root_query_path.clone(), query.timestamp) {
                  Ok(ms_query_id) => {
                    // Lookup the MSQueryES and add the new Query into `pending_queries`.
                    let ms_query_es = statuses.ms_query_ess.get_mut(&ms_query_id).unwrap();
                    ms_query_es.pending_queries.insert(perform_query.query_id.clone());

                    // First, we look up the `tier` of this Table being
                    // written, update the `tier_map`.
                    let sql_query = query.sql_query;
                    let mut query_plan = query.query_plan;
                    let tier = query_plan.tier_map.map.get(sql_query.table()).unwrap().clone();
                    *query_plan.tier_map.map.get_mut(sql_query.table()).unwrap() += 1;

                    // Create an MSWriteTableES in the QueryReplanning state, and add it to
                    // the MSQueryES.
                    let ms_write = map_insert(
                      &mut statuses.full_ms_table_write_ess,
                      &perform_query.query_id,
                      MSTableWriteESWrapper {
                        sender_path: perform_query.sender_path.clone(),
                        child_queries: vec![],
                        es: MSTableWriteES {
                          root_query_path,
                          timestamp: query.timestamp,
                          tier,
                          context: Rc::new(query.context),
                          query_id: perform_query.query_id.clone(),
                          sql_query,
                          query_plan,
                          ms_query_id,
                          new_rms: Default::default(),
                          state: MSWriteExecutionS::Start,
                        },
                      },
                    );
                    let action = ms_write.es.start(self);
                    self.handle_ms_write_es_action(statuses, perform_query.query_id, action);
                  }
                  Err(query_error) => {
                    // The MSQueryES couldn't be constructed.
                    self.ctx().send_query_error(
                      perform_query.sender_path,
                      perform_query.query_id,
                      query_error,
                    );
                  }
                }
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
          msg::TabletMessage::FinishQueryPrepare(prepare) => {
            // Recall that an MSQueryES can randomly be aborted due to a DeadlockSafetyWriteAbort.
            // If it's present, we send back Prepared, and if not, we send back Aborted.
            if let Some(ms_query_es) = statuses.ms_query_ess.get_mut(&prepare.ms_query_id) {
              assert!(ms_query_es.pending_queries.is_empty());
              assert_eq!(prepare.sender_path.query_id, ms_query_es.root_query_id);

              // The VerifyingReadWriteRegion must also exist, so we move it to prepared_*.
              let verifying_write = self.verifying_writes.remove(&ms_query_es.timestamp).unwrap();
              assert!(verifying_write.m_waiting_read_protected.is_empty());
              self.prepared_writes.insert(
                ms_query_es.timestamp,
                ReadWriteRegion {
                  orig_p: verifying_write.orig_p,
                  m_read_protected: verifying_write.m_read_protected,
                  m_write_protected: verifying_write.m_write_protected,
                },
              );

              // Send back Prepared
              let return_qid = prepare.sender_path.query_id.clone();
              let rm_path = self.mk_query_path(prepare.ms_query_id);
              self.ctx().core_ctx().send_to_coord(
                prepare.sender_path,
                msg::CoordMessage::FinishQueryPrepared(msg::FinishQueryPrepared {
                  return_qid,
                  rm_path,
                }),
              );
            } else {
              // Send back Aborted. The only reason for the MSQueryES to no longer exist must be
              // because it was aborted due to a DeadlockSafetyWriteAbort. Thus, we respond to
              // the Slave as such.
              let return_qid = prepare.sender_path.query_id.clone();
              let rm_path = self.mk_query_path(prepare.ms_query_id);
              self.ctx().core_ctx().send_to_coord(
                prepare.sender_path,
                msg::CoordMessage::FinishQueryAborted(msg::FinishQueryAborted {
                  return_qid,
                  rm_path,
                  reason: msg::FinishQueryAbortReason::DeadlockSafetyAbortion,
                }),
              );
            }
          }
          msg::TabletMessage::FinishQueryAbort(abort) => {
            if let Some(ms_query_es) = statuses.ms_query_ess.remove(&abort.ms_query_id) {
              // Recall that in order to get a FinishQueryAbort, the Slave must already have sent a
              // FinishQueryPrepare. Since the network is FIFO, we must have received it, and since
              // the MSQueryES exists, we must have responded with Prepared. Recall that we cannot
              // call `exit_and_clean_up` when MSQueryES is Prepared.
              self.prepared_writes.remove(&ms_query_es.timestamp).unwrap();
              self.ms_root_query_map.remove(&ms_query_es.root_query_id);
            }
          }
          msg::TabletMessage::FinishQueryCommit(commit) => {
            // Remove the MSQueryES. It must exist, since we had Prepared.
            let ms_query_es = statuses.ms_query_ess.remove(&commit.ms_query_id).unwrap();

            // Move the ReadWriteRegion to Committed, and cleanup `ms_root_query_map`.
            let read_write_region = self.prepared_writes.remove(&ms_query_es.timestamp).unwrap();
            self.committed_writes.insert(ms_query_es.timestamp.clone(), read_write_region);
            self.ms_root_query_map.remove(&ms_query_es.root_query_id);

            // Apply the UpdateViews to the storage container.
            commit_to_storage(&mut self.storage, &ms_query_es.timestamp, &ms_query_es.update_views);
          }
          msg::TabletMessage::AlterTablePrepare(prepare) => {
            // Check a few guaranteed properties.
            let col_name = &prepare.alter_op.col_name;
            assert!(!self.prepared_scheme_change.contains_key(col_name));
            assert!(lookup(&self.table_schema.key_cols, col_name).is_none());
            let maybe_col_type = self.table_schema.val_cols.get_last_version(col_name);
            // Assert Column Validness of the incoming request.
            assert!(
              (maybe_col_type.is_some() && prepare.alter_op.maybe_col_type.is_none())
                || (maybe_col_type.is_none() && prepare.alter_op.maybe_col_type.is_some())
            );

            // Populate `alter_table_rm_state`, update `prepared_scheme_change`,
            let lat = self.table_schema.val_cols.get_lat(col_name);
            self
              .alter_table_rm_state
              .insert(prepare.query_id.clone(), (prepare.alter_op.clone(), lat));
            self.prepared_scheme_change.insert(col_name.clone(), lat);

            // Send back a `Prepared`
            self.network_output.send(
              &self.master_eid,
              msg::NetworkMessage::Master(msg::MasterMessage::AlterTablePrepared(
                msg::AlterTablePrepared {
                  query_id: prepare.query_id,
                  tablet_group_id: self.this_tablet_group_id.clone(),
                  timestamp: lat,
                },
              )),
            );
          }
          msg::TabletMessage::AlterTableAbort(abort) => {
            // Recall that the `alter_table_rm_state` element must exist for this QueryId, since
            // Tablets never remove these on their own until a Commit or Abort.
            let (alter_op, _) = self.alter_table_rm_state.remove(&abort.query_id).unwrap();
            self.prepared_scheme_change.remove(&alter_op.col_name).unwrap();
          }
          msg::TabletMessage::AlterTableCommit(commit) => {
            // Update the TableSchema according to `alter_op`.
            let (alter_op, _) = self.alter_table_rm_state.remove(&commit.query_id).unwrap();
            self.prepared_scheme_change.remove(&alter_op.col_name).unwrap();
            self.table_schema.val_cols.write(
              &alter_op.col_name,
              alter_op.maybe_col_type,
              commit.timestamp,
            );

            // Use the GossipData sent by the Master to upate the local GossipData.
            // TODO: When a Slave receives gossip_data, dispatch it to all tablets,
            // not just the target tablet
            let gossip_data = commit.gossip_data.to_gossip();
            if self.gossip.gen < gossip_data.gen {
              self.gossip = Arc::new(gossip_data);
            }
          }
          msg::TabletMessage::FinishQueryCheckPrepared(_) => panic!(),
          msg::TabletMessage::AlterTableCheckPrepared(_) => panic!(),
          msg::TabletMessage::DropTablePrepare(_) => panic!(),
          msg::TabletMessage::DropTableAbort(_) => panic!(),
          msg::TabletMessage::DropTableCommit(_) => panic!(),
          msg::TabletMessage::DropTableCheckPrepared(_) => panic!(),
        }
        self.run_main_loop(statuses);
      }
      TabletForwardMsg::GossipData(_) => panic!(),
      TabletForwardMsg::RemoteLeaderChanged(_) => panic!(),
      TabletForwardMsg::LeaderChanged(_) => panic!(),
    }
  }

  /// Checks if the MSQueryES for `root_query_id` already exists, returning its
  /// `QueryId` if so. If not, we create an MSQueryES (populating `verifying_writes`,
  /// sending `RegisterQuery`, etc).
  fn get_msquery_id(
    &mut self,
    statuses: &mut Statuses,
    root_query_path: QueryPath,
    timestamp: Timestamp,
  ) -> Result<QueryId, msg::QueryError> {
    let root_query_id = root_query_path.query_id.clone();
    if let Some(ms_query_id) = self.ms_root_query_map.get(&root_query_id) {
      // Here, the MSQueryES already exists, so we just return it's QueryId.
      Ok(ms_query_id.clone())
    } else {
      // Otherwise, we need to create one. First check whether the Timestamp is available or not.
      if self.verifying_writes.contains_key(&timestamp)
        || self.prepared_writes.contains_key(&timestamp)
        || self.committed_writes.contains_key(&timestamp)
      {
        // This means the Timestamp is already in use, so we return an error.
        Err(msg::QueryError::TimestampConflict)
      } else {
        // This means that we can add an MSQueryES at the Timestamp
        let ms_query_id = mk_qid(&mut self.rand);
        statuses.ms_query_ess.insert(
          ms_query_id.clone(),
          MSQueryES {
            root_query_id: root_query_id.clone(),
            query_id: ms_query_id.clone(),
            timestamp: timestamp.clone(),
            update_views: Default::default(),
            pending_queries: Default::default(),
          },
        );

        // We also amend the `ms_root_query_map` to associate the root query.
        self.ms_root_query_map.insert(root_query_id.clone(), ms_query_id.clone());

        // Send a register message back to the root.
        let register_query = msg::RegisterQuery {
          root_query_id: root_query_id.clone(),
          query_path: self.mk_query_path(ms_query_id.clone()),
        };
        self
          .ctx()
          .core_ctx()
          .send_to_coord(root_query_path, msg::CoordMessage::RegisterQuery(register_query));

        // Finally, add an empty VerifyingReadWriteRegion
        self.verifying_writes.insert(
          timestamp,
          VerifyingReadWriteRegion {
            orig_p: OrigP::new(ms_query_id.clone()),
            m_waiting_read_protected: BTreeSet::new(),
            m_read_protected: BTreeSet::new(),
            m_write_protected: BTreeSet::new(),
          },
        );
        Ok(ms_query_id)
      }
    }
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
    self.requested_locked_columns.insert(locked_cols_qid.clone(), (orig_p, timestamp, cols));
    btree_multimap_insert(&mut self.request_index, &timestamp, locked_cols_qid.clone());
    locked_cols_qid
  }

  /// This removes elements from `requested_locked_columns`, maintaining
  /// `request_index` as well, if the request is present.
  pub fn remove_col_locking_request(
    &mut self,
    query_id: QueryId,
  ) -> Option<(OrigP, Timestamp, Vec<ColName>)> {
    if let Some((orig_p, timestamp, cols)) = self.requested_locked_columns.remove(&query_id) {
      btree_multimap_remove(&mut self.request_index, &timestamp, &query_id);
      return Some((orig_p, timestamp, cols));
    }
    return None;
  }

  /// This removes the Read Protection request from `waiting_read_protected` with the given
  /// `query_id` at the given `timestamp`, if it exists, and returns it.
  pub fn remove_read_protected_request(
    &mut self,
    timestamp: &Timestamp,
    query_id: &QueryId,
  ) -> Option<ProtectRequest> {
    if let Some(waiting) = self.waiting_read_protected.get_mut(&timestamp) {
      for protect_request in waiting.iter() {
        if &protect_request.protect_qid == query_id {
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
    timestamp: &Timestamp,
    query_id: &QueryId,
  ) -> Option<ProtectRequest> {
    if let Some(verifying_write) = self.verifying_writes.get_mut(timestamp) {
      for protect_request in verifying_write.m_waiting_read_protected.iter() {
        if &protect_request.protect_qid == query_id {
          // Here, we found a request with matching QueryId, so we remove it.
          let protect_request = protect_request.clone();
          verifying_write.m_waiting_read_protected.remove(&protect_request);
          return Some(protect_request);
        }
      }
    }
    return None;
  }

  /// Checks if the give `write_region` has a Region Isolation with subsequent reads.
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
    return true;
  }

  // The Main Loop
  fn run_main_loop(&mut self, statuses: &mut Statuses) {
    while !self.run_main_loop_once(statuses) {}
  }

  /// Thus runs one iteration of the Main Loop, returning `false` exactly when nothing changes.
  fn run_main_loop_once(&mut self, statuses: &mut Statuses) -> bool {
    // First, we see if we can satisfy Column Locking

    // Iterate through every `request_locked_columns` and see if that can be satisfied.
    for (_, query_set) in &self.request_index {
      for query_id in query_set {
        let (_, timestamp, cols) = self.requested_locked_columns.get(query_id).unwrap();
        // We check whether any of the Columns in `cols` is undergoing an AlterOp, i.e. Preparing.
        let mut preparing = false;
        for col in cols {
          if let Some(prep_timestamp) = self.prepared_scheme_change.get(col) {
            if timestamp > prep_timestamp {
              preparing = true;
              break;
            }
          }
        }
        if !preparing {
          // This means that we can lock the Columns and inform the originator
          // of the Locking Request.
          for col in cols {
            if lookup(&self.table_schema.key_cols, col).is_none() {
              self.table_schema.val_cols.update_lat(col, timestamp.clone());
            }
          }

          // Remove the column locking request.
          let query_id = query_id.clone();
          let (orig_p, _, _) = self.remove_col_locking_request(query_id.clone()).unwrap();

          // Process
          self.columns_locked_for_query(statuses, orig_p, query_id);
          return true;
        }
      }
    }

    // Next, we see if we can provide Region Protection

    // To account for both `verifying_writes` and `prepared_writes`, we merge
    // them into a single container similar to `verifying_writes`. This is just
    // for expedience; it should be optimized later. (We can probably change
    // `prepared_writes` to `VerifyingReadWriteRegion` and then zip 2 iterators).
    let mut all_cur_writes = BTreeMap::<Timestamp, VerifyingReadWriteRegion>::new();
    for (cur_timestamp, verifying_write) in &self.verifying_writes {
      all_cur_writes.insert(*cur_timestamp, verifying_write.clone());
    }
    for (cur_timestamp, prepared_write) in &self.prepared_writes {
      all_cur_writes.insert(
        *cur_timestamp,
        VerifyingReadWriteRegion {
          orig_p: prepared_write.orig_p.clone(),
          m_waiting_read_protected: Default::default(),
          m_read_protected: prepared_write.m_read_protected.clone(),
          m_write_protected: prepared_write.m_write_protected.clone(),
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
        self.grant_read_region(statuses, *timestamp, protect_request);
        return true;
      }

      // Next, process all `m_read_protected`s for the first `verifying_write`
      for protect_request in &verifying_write.m_waiting_read_protected {
        self.grant_m_read_region(statuses, *first_write_timestamp, protect_request.clone());
        return true;
      }

      // Next, accumulate the WriteRegions, and then search for region protection with
      // all subsequent `(m_)waiting_read_protected`s.
      let mut cum_write_regions = verifying_write.m_write_protected.clone();
      let mut prev_write_timestamp = first_write_timestamp;
      let bound = (Bound::Excluded(first_write_timestamp), Bound::Unbounded);
      for (cur_timestamp, verifying_write) in all_cur_writes.range(bound) {
        // The loop state is that `cum_write_regions` contains all WriteRegions <=
        // `prev_write_timestamp`, all `m_read_protected` <= `prev_write_timestamp` have been
        // processed, and all `read_protected`s < `prev_write_timestamp` have been processed.

        // Process `m_read_protected`
        for protect_request in &verifying_write.m_waiting_read_protected {
          if !does_intersect(&protect_request.read_region, &cum_write_regions) {
            self.grant_m_read_region(statuses, *cur_timestamp, protect_request.clone());
            return true;
          }
        }

        // Process `read_protected`
        let bound = (Bound::Included(prev_write_timestamp), Bound::Excluded(cur_timestamp));
        for (timestamp, set) in self.waiting_read_protected.range(bound) {
          for protect_request in set {
            if !does_intersect(&protect_request.read_region, &cum_write_regions) {
              self.grant_read_region(statuses, *timestamp, protect_request.clone());
              return true;
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
          if !does_intersect(&protect_request.read_region, &cum_write_regions) {
            self.grant_read_region(statuses, *timestamp, protect_request.clone());
            return true;
          }
        }
      }
    } else {
      for (timestamp, set) in &self.waiting_read_protected {
        for protect_request in set {
          self.grant_read_region(statuses, *timestamp, protect_request.clone());
          return true;
        }
      }
    }

    // Next, we search for any DeadlockSafetyWriteAbort.
    for (timestamp, set) in &self.waiting_read_protected {
      if let Some(verifying_write) = self.verifying_writes.get(timestamp) {
        for protect_request in set {
          if does_intersect(&protect_request.read_region, &verifying_write.m_write_protected) {
            self.deadlock_safety_write_abort(statuses, verifying_write.orig_p.clone(), *timestamp);
            return true;
          }
        }
      }
    }

    // Finally, we search for any DeadlockSafetyReadAbort.
    for (timestamp, set) in &self.waiting_read_protected {
      if let Some(prepared_write) = self.prepared_writes.get(timestamp) {
        for protect_request in set {
          if does_intersect(&protect_request.read_region, &prepared_write.m_write_protected) {
            self.deadlock_safety_read_abort(statuses, *timestamp, protect_request.clone());
            return true;
          }
        }
      }
    }

    return false;
  }

  /// Route the column locking to the appropriate ES.
  fn columns_locked_for_query(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    locked_cols_qid: QueryId,
  ) {
    let query_id = orig_p.query_id;
    if let Some(read) = statuses.full_table_read_ess.get_mut(&query_id) {
      // TableReadES
      let action = read.es.columns_locked(self, locked_cols_qid);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      let action = ms_write.es.columns_locked(self, locked_cols_qid);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      // MSTableReadES
      let action = ms_read.es.columns_locked(self, locked_cols_qid);
      self.handle_ms_read_es_action(statuses, query_id, action);
    }
  }

  /// Move the ProtectRequest in `waiting_read_protected` forward.
  fn grant_read_region(
    &mut self,
    statuses: &mut Statuses,
    timestamp: Timestamp,
    protect_request: ProtectRequest,
  ) {
    self.remove_read_protected_request(&timestamp, &protect_request.protect_qid).unwrap();
    btree_multimap_insert(&mut self.read_protected, &timestamp, protect_request.read_region);

    // Inform the originator.
    self.read_protected_for_query(statuses, protect_request.orig_p, protect_request.protect_qid);
  }

  /// Move the ProtectRequest in `m_waiting_read_protected` forward.
  fn grant_m_read_region(
    &mut self,
    statuses: &mut Statuses,
    timestamp: Timestamp,
    protect_request: ProtectRequest,
  ) {
    let verifying_write = self.verifying_writes.get_mut(&timestamp).unwrap();
    verifying_write.m_waiting_read_protected.remove(&protect_request);
    verifying_write.m_read_protected.insert(protect_request.read_region);

    // Inform the originator.
    self.read_protected_for_query(statuses, protect_request.orig_p, protect_request.protect_qid);
  }

  /// We call this when a DeadlockSafetyReadAbort happens for a `waiting_read_protected`.
  fn deadlock_safety_read_abort(
    &mut self,
    statuses: &mut Statuses,
    timestamp: Timestamp,
    protect_request: ProtectRequest,
  ) {
    // Remove the ProtectRequest.
    self.remove_read_protected_request(&timestamp, &protect_request.protect_qid).unwrap();

    // Send back Aborted and ECU.
    let query_id = protect_request.orig_p.query_id;
    if let Some(mut read) = statuses.full_table_read_ess.remove(&query_id) {
      let sender_path = read.sender_path;
      let responder_path = self.mk_query_path(query_id);
      self.ctx().send_to_path(
        sender_path.clone(),
        CommonQuery::QueryAborted(msg::QueryAborted {
          return_qid: sender_path.query_id,
          responder_path,
          payload: msg::AbortedData::QueryError(msg::QueryError::DeadlockSafetyAbortion),
        }),
      );
      read.es.exit_and_clean_up(self);
      self.exit_all(statuses, read.child_queries)
    }
  }

  /// Simply aborts the MSQueryES, which will clean up everything to do with it.
  fn deadlock_safety_write_abort(&mut self, statuses: &mut Statuses, orig_p: OrigP, _: Timestamp) {
    self.exit_ms_query_es(statuses, orig_p.query_id, msg::QueryError::DeadlockSafetyAbortion);
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
    if let Some(read) = statuses.full_table_read_ess.get_mut(&query_id) {
      // TableReadES
      let action = read.es.read_protected(self, protect_query_id);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      let action = ms_write.es.read_protected(
        self,
        statuses.ms_query_ess.get_mut(ms_write.es.ms_query_id()).unwrap(),
        protect_query_id,
      );
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      // MSTableReadES
      let action = ms_read.es.read_protected(
        self,
        statuses.ms_query_ess.get(ms_read.es.ms_query_id()).unwrap(),
        protect_query_id,
      );
      self.handle_ms_read_es_action(statuses, query_id, action);
    }
  }

  /// Handles an incoming QuerySuccess message.
  fn handle_query_success(&mut self, statuses: &mut Statuses, query_success: msg::QuerySuccess) {
    let tm_query_id = &query_success.return_qid;
    if let Some(tm_status) = statuses.tm_statuss.get_mut(tm_query_id) {
      // We just add the result of the `query_success` here.
      let node_path = query_success.responder_path.node_path;
      tm_status.tm_state.insert(node_path, Some(query_success.result.clone()));
      tm_status.new_rms.extend(query_success.new_rms);
      tm_status.responded_count += 1;
      if tm_status.responded_count == tm_status.tm_state.len() {
        // Remove the `TMStatus` and take ownership
        let tm_status = statuses.tm_statuss.remove(tm_query_id).unwrap();
        // Merge there TableViews together
        let mut results = Vec::<(Vec<ColName>, Vec<TableView>)>::new();
        for (_, rm_result) in tm_status.tm_state {
          results.push(rm_result.unwrap());
        }
        let merged_result = merge_table_views(results);
        let gr_query_id = tm_status.orig_p.query_id;
        let gr_query = statuses.gr_query_ess.get_mut(&gr_query_id).unwrap();
        remove_item(&mut gr_query.child_queries, tm_query_id);
        let action = gr_query.es.handle_tm_success(
          &mut self.ctx(),
          tm_query_id.clone(),
          tm_status.new_rms,
          merged_result,
        );
        self.handle_gr_query_es_action(statuses, gr_query_id, action);
      }
    }
  }

  /// Handles an incoming QueryAborted message.
  fn handle_query_aborted(&mut self, statuses: &mut Statuses, query_aborted: msg::QueryAborted) {
    let tm_query_id = &query_aborted.return_qid;
    if let Some(tm_status) = statuses.tm_statuss.remove(tm_query_id) {
      // We ECU this TMStatus by sending CancelQuery to all remaining participants.
      // Then, we send the QueryAborted back to the orig_p.
      for (rm_path, rm_result) in tm_status.tm_state {
        if rm_result.is_none() && rm_path != query_aborted.responder_path.node_path {
          // We avoid sending CancelQuery for the RM that just responded.
          self.ctx().send_to_node(
            rm_path.to_node_id(),
            CommonQuery::CancelQuery(msg::CancelQuery {
              query_id: tm_status.child_query_id.clone(),
            }),
          );
        }
      }

      // Finally, we propagate tge AbortData to the GRQueryES that owns this TMStatus
      let gr_query_id = tm_status.orig_p.query_id;
      let gr_query = statuses.gr_query_ess.get_mut(&gr_query_id).unwrap();
      remove_item(&mut gr_query.child_queries, tm_query_id);
      let action = gr_query.es.handle_tm_aborted(&mut self.ctx(), query_aborted.payload);
      self.handle_gr_query_es_action(statuses, gr_query_id, action);
    }
  }

  /// This function processes the result of a GRQueryES. Generally, it finds the ESWrapper
  /// to route the results to, cleans up its `child_queries`, and forwards the result.
  fn handle_gr_query_done(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    subquery_new_rms: HashSet<QueryPath>,
    (table_schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    let query_id = orig_p.query_id;
    if let Some(read) = statuses.full_table_read_ess.get_mut(&query_id) {
      // TableReadES
      remove_item(&mut read.child_queries, &subquery_id);
      let action = read.es.handle_subquery_done(
        self,
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      );
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(trans_read) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      // TransTableReadES
      let prefix = trans_read.es.location_prefix();
      remove_item(&mut trans_read.child_queries, &subquery_id);
      let action = if let Some(gr_query) = statuses.gr_query_ess.get(&prefix.query_id) {
        trans_read.es.handle_subquery_done(
          &mut self.ctx(),
          &gr_query.es,
          subquery_id,
          subquery_new_rms,
          (table_schema, table_views),
        )
      } else {
        trans_read.es.handle_internal_query_error(&mut self.ctx(), msg::QueryError::LateralError)
      };
      self.handle_trans_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      remove_item(&mut ms_write.child_queries, &subquery_id);
      let action = ms_write.es.handle_subquery_done(
        self,
        statuses.ms_query_ess.get_mut(ms_write.es.ms_query_id()).unwrap(),
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      );
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      // MSTableReadES
      remove_item(&mut ms_read.child_queries, &subquery_id);
      let action = ms_read.es.handle_subquery_done(
        self,
        statuses.ms_query_ess.get(ms_read.es.ms_query_id()).unwrap(),
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      );
      self.handle_ms_read_es_action(statuses, query_id, action);
    }
  }

  /// This routes the QueryError propagated by a GRQueryES up to the appropriate top-level ES.
  fn handle_internal_query_error(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    query_error: msg::QueryError,
  ) {
    let query_id = orig_p.query_id;
    if let Some(read) = statuses.full_table_read_ess.get_mut(&query_id) {
      // TableReadES
      remove_item(&mut read.child_queries, &subquery_id);
      let action = read.es.handle_internal_query_error(self, query_error);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(trans_read) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      // TransTableReadES
      remove_item(&mut trans_read.child_queries, &subquery_id);
      let action = trans_read.es.handle_internal_query_error(&mut self.ctx(), query_error);
      self.handle_trans_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write) = statuses.full_ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      remove_item(&mut ms_write.child_queries, &subquery_id);
      let action = ms_write.es.handle_internal_query_error(self, query_error);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.full_ms_table_read_ess.get_mut(&query_id) {
      // MSTableReadES
      remove_item(&mut ms_read.child_queries, &subquery_id);
      let action = ms_read.es.handle_internal_query_error(self, query_error);
      self.handle_ms_read_es_action(statuses, query_id, action);
    }
  }

  /// Adds the given `gr_query_ess` to `statuses`, executing them one at a time.
  fn launch_subqueries(&mut self, statuses: &mut Statuses, gr_query_ess: Vec<GRQueryES>) {
    /// Here, we have to add in the GRQueryESs and start them.
    let mut subquery_ids = Vec::<QueryId>::new();
    for gr_query_es in gr_query_ess {
      let subquery_id = gr_query_es.query_id.clone();
      let gr_query = GRQueryESWrapper { child_queries: vec![], es: gr_query_es };
      statuses.gr_query_ess.insert(subquery_id.clone(), gr_query);
      subquery_ids.push(subquery_id);
    }

    // Drive GRQueries
    for query_id in subquery_ids {
      if let Some(gr_query) = statuses.gr_query_ess.get_mut(&query_id) {
        // Generally, we use an `if` guard in case one child Query aborts the parent and
        // thus all other children. (This won't happen for GRQueryESs, though)
        let action = gr_query.es.start::<T>(&mut self.ctx());
        self.handle_gr_query_es_action(statuses, query_id, action);
      }
    }
  }

  /// Handles the actions specified by a TransTableReadES.
  fn handle_trans_read_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: TransTableAction,
  ) {
    match action {
      TransTableAction::Wait => {}
      TransTableAction::SendSubqueries(gr_query_ess) => {
        self.launch_subqueries(statuses, gr_query_ess);
      }
      TransTableAction::Success(success) => {
        // Remove the TableReadESWrapper and respond.
        let trans_read = statuses.full_trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = trans_read.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QuerySuccess(msg::QuerySuccess {
            return_qid: sender_path.query_id,
            responder_path,
            result: success.result,
            new_rms: success.new_rms,
          }),
        )
      }
      TransTableAction::QueryError(query_error) => {
        // Remove the TableReadESWrapper, abort subqueries, and respond.
        let trans_read = statuses.full_trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = trans_read.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        );
        self.exit_all(statuses, trans_read.child_queries)
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
        self.launch_subqueries(statuses, gr_query_ess);
      }
      TableAction::Success(success) => {
        // Remove the TableReadESWrapper and respond.
        let read = statuses.full_trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = read.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QuerySuccess(msg::QuerySuccess {
            return_qid: sender_path.query_id,
            responder_path,
            result: success.result,
            new_rms: success.new_rms,
          }),
        )
      }
      TableAction::QueryError(query_error) => {
        // Remove the TableReadESWrapper, abort subqueries, and respond.
        let read = statuses.full_trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = read.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        );
        self.exit_all(statuses, read.child_queries)
      }
    }
  }

  /// Cleans up the MSQueryES with QueryId of `query_id`. This can only be called
  /// if the MSQueryES hasn't Prepared yet.
  fn exit_ms_query_es(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    query_error: msg::QueryError,
  ) {
    let ms_query_es = statuses.ms_query_ess.remove(&query_id).unwrap();

    // Then, we ECU all ESs in `pending_queries`, and respond with an Abort.
    for query_id in ms_query_es.pending_queries {
      if let Some(mut ms_read) = statuses.full_ms_table_read_ess.remove(&query_id) {
        // MSTableReadES
        ms_read.es.exit_and_clean_up(self);
        let sender_path = ms_read.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        );
        self.exit_all(statuses, ms_read.child_queries);
      } else if let Some(mut ms_write) = statuses.full_ms_table_write_ess.remove(&query_id) {
        // MSTableWriteES
        ms_write.es.exit_and_clean_up(self);
        let sender_path = ms_write.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        );
        self.exit_all(statuses, ms_write.child_queries);
      }
    }

    // Cleanup the TableContext's
    self.verifying_writes.remove(&ms_query_es.timestamp);
    self.ms_root_query_map.remove(&ms_query_es.root_query_id);
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
        self.launch_subqueries(statuses, gr_query_ess);
      }
      MSTableWriteAction::Success(success) => {
        // Remove the MSWriteESWrapper, removing it from the MSQueryES, and respond.
        let ms_write = statuses.full_ms_table_read_ess.remove(&query_id).unwrap();
        let ms_query_es = statuses.ms_query_ess.get_mut(ms_write.es.ms_query_id()).unwrap();
        ms_query_es.pending_queries.remove(&query_id);
        let sender_path = ms_write.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QuerySuccess(msg::QuerySuccess {
            return_qid: sender_path.query_id,
            responder_path,
            result: success.result,
            new_rms: success.new_rms,
          }),
        )
      }
      MSTableWriteAction::QueryError(query_error) => {
        let ms_write = statuses.full_ms_table_read_ess.get(&query_id).unwrap();
        self.exit_ms_query_es(statuses, ms_write.es.ms_query_id().clone(), query_error);
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
        self.launch_subqueries(statuses, gr_query_ess);
      }
      MSTableReadAction::Success(success) => {
        // Remove the MSWriteESWrapper, removing it from the MSQueryES, and respond.
        let ms_read = statuses.full_ms_table_read_ess.remove(&query_id).unwrap();
        let ms_query_es = statuses.ms_query_ess.get_mut(ms_read.es.ms_query_id()).unwrap();
        ms_query_es.pending_queries.remove(&query_id);
        let sender_path = ms_read.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QuerySuccess(msg::QuerySuccess {
            return_qid: sender_path.query_id,
            responder_path,
            result: success.result,
            new_rms: success.new_rms,
          }),
        )
      }
      MSTableReadAction::QueryError(query_error) => {
        let ms_read = statuses.full_ms_table_read_ess.get(&query_id).unwrap();
        self.exit_ms_query_es(statuses, ms_read.es.ms_query_id().clone(), query_error);
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
        let gr_query = statuses.gr_query_ess.get_mut(&query_id).unwrap();
        gr_query.child_queries.push(tm_status.query_id.clone());
        statuses.tm_statuss.insert(tm_status.query_id.clone(), tm_status);
      }
      GRQueryAction::Success(res) => {
        let gr_query = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_gr_query_done(
          statuses,
          gr_query.es.orig_p,
          gr_query.es.query_id,
          res.new_rms,
          (res.schema, res.result),
        );
      }
      GRQueryAction::QueryError(query_error) => {
        let gr_query = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_internal_query_error(
          statuses,
          gr_query.es.orig_p,
          gr_query.es.query_id,
          query_error,
        );
      }
    }
  }

  /// Run `exit_and_clean_up` for all QueryIds in `query_ids`.
  fn exit_all(&mut self, statuses: &mut Statuses, query_ids: Vec<QueryId>) {
    for query_id in query_ids {
      self.exit_and_clean_up(statuses, query_id);
    }
  }

  /// This function is used to initiate an Exit and Clean Up of ESs. This is needed to handle
  /// CancelQuery's, as well as when one on ES wants to Exit and Clean Up another ES. Note that
  /// We allow the ES at `query_id` to be in any state, or to not even exist.
  fn exit_and_clean_up(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    if let Some(mut gr_query) = statuses.gr_query_ess.remove(&query_id) {
      // GRQueryES
      gr_query.es.exit_and_clean_up(&mut self.ctx());
      self.exit_all(statuses, gr_query.child_queries);
    } else if let Some(mut read) = statuses.full_table_read_ess.remove(&query_id) {
      // TableReadES
      read.es.exit_and_clean_up(self);
      self.exit_all(statuses, read.child_queries);
    } else if let Some(mut trans_read) = statuses.full_trans_table_read_ess.remove(&query_id) {
      // TransTableReadES
      trans_read.es.exit_and_clean_up(&mut self.ctx());
      self.exit_all(statuses, trans_read.child_queries);
    } else if let Some(tm_status) = statuses.tm_statuss.remove(&query_id) {
      // We ECU this TMStatus by sending CancelQuery to all remaining participants.
      for (rm_path, rm_result) in tm_status.tm_state {
        if rm_result.is_none() {
          self.ctx().send_to_node(
            rm_path.to_node_id(),
            CommonQuery::CancelQuery(msg::CancelQuery {
              query_id: tm_status.child_query_id.clone(),
            }),
          );
        }
      }
    } else if let Some(ms_query_es) = statuses.ms_query_ess.get(&query_id) {
      // MSQueryES.
      //
      // We should only run this code when a CancelQuery comes in (from the Slave) for
      // the MSQueryES. We shouldn't run this code in any other circumstance (e.g.
      // DeadlockSafetyAborted), since this only sends back `LateralError`s to the origiators
      // of the MSTable*ESs, which we should only do if an ancestor is known already have exit
      // (i..e the MSCoordES, in this case).
      //
      // The Slave should not send CancelQuery after it sends Prepare, since `exit_ms_query_es`
      // can't handle Prepared MSQueryESs. As a corollary, this function should not be used when
      // a FinishQueryAbort comes in.
      //
      // TODO: In the spirit of getting local safety, we shouldn't have the above expection.
      // Instead, we should give MSQueryES a state variable and have it react to CancelQuery
      // accordingly (ignore it if we have prepared).
      self.exit_ms_query_es(statuses, ms_query_es.query_id.clone(), msg::QueryError::LateralError);
    } else if let Some(mut ms_write) = statuses.full_ms_table_write_ess.remove(&query_id) {
      // MSTableWriteES.
      let ms_query_es = statuses.ms_query_ess.get_mut(ms_write.es.ms_query_id()).unwrap();
      ms_query_es.pending_queries.remove(&query_id);
      ms_write.es.exit_and_clean_up(self);
      self.exit_all(statuses, ms_write.child_queries);
    } else if let Some(mut ms_read) = statuses.full_ms_table_read_ess.remove(&query_id) {
      // MSTableReadES.
      let ms_query_es = statuses.ms_query_ess.get_mut(ms_read.es.ms_query_id()).unwrap();
      ms_query_es.pending_queries.remove(&query_id);
      ms_read.es.exit_and_clean_up(self);
      self.exit_all(statuses, ms_read.child_queries);
    }
  }

  /// Construct QueryPath for a given `query_id` that belongs to this Tablet.
  pub fn mk_query_path(&self, query_id: QueryId) -> QueryPath {
    QueryPath {
      node_path: NodePath {
        slave_group_id: self.this_slave_group_id.clone(),
        maybe_tablet_group_id: Some(self.this_tablet_group_id.clone()),
      },
      query_id,
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

  // Run the Constructor. Recall that this can return an error during to subtable computation.
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
  let mut gr_query_ess = Vec::<GRQueryES>::new();
  for (subquery_idx, child_context) in child_contexts.into_iter().enumerate() {
    gr_query_ess.push(subquery_view.mk_gr_query_es(
      mk_qid(rand),
      Rc::new(child_context),
      subquery_idx,
    ));
  }

  Ok(gr_query_ess)
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
