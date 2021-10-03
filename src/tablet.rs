use crate::alter_table_es::{AlterTableES, State};
use crate::col_usage::{
  collect_select_subqueries, collect_top_level_cols, collect_update_subqueries,
  node_external_trans_tables, nodes_external_cols, nodes_external_trans_tables, ColUsagePlanner,
  FrozenColUsageNode,
};
use crate::common::RemoteLeaderChangedPLm;
use crate::common::{
  btree_multimap_insert, btree_multimap_remove, lookup, lookup_pos, map_insert, merge_table_views,
  mk_qid, remove_item, Clock, ColBound, GossipData, IOTypes, KeyBound, NetworkOut, OrigP,
  PolyColBound, SingleBound, TMStatus, TableRegion, TableSchema,
};
use crate::drop_table_es::DropExecuting;
use crate::drop_table_es::DropTableES;
use crate::expression::{
  compress_row_region, compute_key_region, compute_poly_col_bounds, construct_cexpr,
  construct_kb_expr, does_intersect, evaluate_c_expr, is_true, CExpr, EvalError,
};
use crate::finish_query_es::{
  FinishQueryAction, FinishQueryES, FinishQueryExecuting, OrigTMLeadership, Paxos2PCRMState,
};
use crate::gr_query_es::{
  GRExecutionS, GRQueryAction, GRQueryConstructorView, GRQueryES, GRQueryPlan, ReadStage,
  SubqueryComputableSql,
};
use crate::model::common::{
  proc, CQueryPath, CSubNodePath, CTQueryPath, CTSubNodePath, ColType, ColValN, Context,
  ContextRow, ContextSchema, Gen, LeadershipId, NodeGroupId, PaxosGroupId, TNodePath, TQueryPath,
  TSubNodePath, TableView, TierMap, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{
  ColName, ColVal, EndpointId, PrimaryKey, QueryId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::model::message::SlaveMessage::PaxosMessage;
use crate::ms_table_read_es::{MSReadExecutionS, MSTableReadAction, MSTableReadES};
use crate::ms_table_write_es::{MSTableWriteAction, MSTableWriteES, MSWriteExecutionS};
use crate::multiversion_map::MVM;
use crate::paxos::LeaderChanged;
use crate::server::{
  are_cols_locked, contains_col, evaluate_super_simple_select, evaluate_update, mk_eval_error,
  CommonQuery, ContextConstructor, LocalTable, ServerContext,
};
use crate::storage::{
  commit_to_storage, compress_updates_views, GenericMVTable, GenericTable, StorageView,
};
use crate::table_read_es::{ExecutionS, TableAction, TableReadES};
use crate::trans_table_read_es::{
  TransExecutionS, TransTableAction, TransTableReadES, TransTableSource,
};
use serde::{Deserialize, Serialize};
use std::cmp::max;
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
        SingleSubqueryStatus::Pending(SubqueryPending { query_id, .. }) => {
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

/// When this exists, there will be a corresponding `VerifyingReadWriteRegion`.
#[derive(Debug)]
pub struct MSQueryES {
  pub root_query_path: CQueryPath,
  // The LeadershipId of the root PaxosNode.
  pub root_lid: LeadershipId,
  pub query_id: QueryId,
  pub timestamp: Timestamp,
  pub update_views: BTreeMap<u32, GenericTable>,
  /// This holds all `MSTable*ES`s that belong to this MSQueryES. We make sure
  /// that every ES reference here exist.
  pub pending_queries: HashSet<QueryId>,
}

// -----------------------------------------------------------------------------------------------
//  Wrappers
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct TableReadESWrapper {
  sender_path: CTQueryPath,
  child_queries: Vec<QueryId>,
  es: TableReadES,
}

impl TableReadESWrapper {
  fn sender_gid(&self) -> PaxosGroupId {
    self.sender_path.node_path.sid.to_gid()
  }
}

#[derive(Debug)]
pub struct TransTableReadESWrapper {
  pub sender_path: CTQueryPath,
  pub child_queries: Vec<QueryId>,
  pub es: TransTableReadES,
}

impl TransTableReadESWrapper {
  pub fn sender_gid(&self) -> PaxosGroupId {
    self.sender_path.node_path.sid.to_gid()
  }
}

#[derive(Debug)]
struct MSTableReadESWrapper {
  sender_path: CTQueryPath,
  child_queries: Vec<QueryId>,
  es: MSTableReadES,
}

impl MSTableReadESWrapper {
  fn sender_gid(&self) -> PaxosGroupId {
    self.sender_path.node_path.sid.to_gid()
  }
}

#[derive(Debug)]
struct MSTableWriteESWrapper {
  sender_path: CTQueryPath,
  child_queries: Vec<QueryId>,
  es: MSTableWriteES,
}

impl MSTableWriteESWrapper {
  fn sender_gid(&self) -> PaxosGroupId {
    self.sender_path.node_path.sid.to_gid()
  }
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
  // Paxos2PC
  finish_query_ess: HashMap<QueryId, FinishQueryES>,

  // TP
  gr_query_ess: HashMap<QueryId, GRQueryESWrapper>,
  table_read_ess: HashMap<QueryId, TableReadESWrapper>,
  trans_table_read_ess: HashMap<QueryId, TransTableReadESWrapper>,
  tm_statuss: HashMap<QueryId, TMStatus>,
  ms_query_ess: HashMap<QueryId, MSQueryES>,
  ms_table_read_ess: HashMap<QueryId, MSTableReadESWrapper>,
  ms_table_write_ess: HashMap<QueryId, MSTableWriteESWrapper>,

  // DDL
  ddl_es: Option<(QueryId, DDLES)>,
}

#[derive(Debug)]
pub enum DDLES {
  Alter(AlterTableES),
  Drop(DropTableES),
}

// -----------------------------------------------------------------------------------------------
//  Region Isolation Algorithm
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestedReadProtected {
  pub query_id: QueryId,
  pub read_region: TableRegion,
  pub orig_p: OrigP,
}

#[derive(Debug, Clone)]
pub struct VerifyingReadWriteRegion {
  pub orig_p: OrigP,
  pub m_waiting_read_protected: BTreeSet<RequestedReadProtected>,
  pub m_read_protected: BTreeSet<TableRegion>,
  pub m_write_protected: BTreeSet<TableRegion>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ReadWriteRegion {
  pub orig_p: OrigP,
  pub m_read_protected: BTreeSet<TableRegion>,
  pub m_write_protected: BTreeSet<TableRegion>,
}

// -----------------------------------------------------------------------------------------------
//  Schema Change and Locking
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RequestedLockedCols {
  query_id: QueryId,
  timestamp: Timestamp,
  cols: Vec<ColName>,
  orig_p: OrigP,
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

pub mod plm {
  use crate::common::TableRegion;
  use crate::model::common::{proc, CQueryPath, TQueryPath};
  use crate::model::common::{ColName, QueryId, Timestamp};
  use crate::storage::GenericTable;
  use crate::tablet::ReadWriteRegion;
  use serde::{Deserialize, Serialize};

  // LockedCols

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct LockedCols {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
    pub cols: Vec<ColName>,
  }

  // ReadProtected

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct ReadProtected {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
    pub region: TableRegion,
  }

  // FinishQuery

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct FinishQueryPrepared {
    pub query_id: QueryId,
    pub tm: CQueryPath,
    pub all_rms: Vec<TQueryPath>,

    pub region_lock: ReadWriteRegion,
    pub timestamp: Timestamp,
    pub update_view: GenericTable,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct FinishQueryCommitted {
    pub query_id: QueryId,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct FinishQueryAborted {
    pub query_id: QueryId,
  }

  // AlterTable

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTablePrepared {
    pub query_id: QueryId,
    pub alter_op: proc::AlterOp,
    pub timestamp: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableCommitted {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTableAborted {
    pub query_id: QueryId,
  }

  // DropTable

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct DropTablePrepared {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct DropTableCommitted {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct DropTableAborted {
    pub query_id: QueryId,
  }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TabletPLm {
  LockedCols(plm::LockedCols),
  ReadProtected(plm::ReadProtected),
  FinishQueryPrepared(plm::FinishQueryPrepared),
  FinishQueryCommitted(plm::FinishQueryCommitted),
  FinishQueryAborted(plm::FinishQueryAborted),
  AlterTablePrepared(plm::AlterTablePrepared),
  AlterTableCommitted(plm::AlterTableCommitted),
  AlterTableAborted(plm::AlterTableAborted),
  DropTablePrepared(plm::DropTablePrepared),
  DropTableCommitted(plm::DropTableCommitted),
  DropTableAborted(plm::DropTableAborted),
}

// -----------------------------------------------------------------------------------------------
//  TabletForwardMsg
// -----------------------------------------------------------------------------------------------

pub enum TabletForwardMsg {
  TabletBundle(Vec<TabletPLm>),
  TabletMessage(msg::TabletMessage),
  GossipData(Arc<GossipData>),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
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
  pub sub_node_path: CTSubNodePath, // Wraps `this_tablet_group_id` for expedience
  pub this_eid: EndpointId,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// LeaderMap
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  // Storage
  pub storage: GenericMVTable,
  pub this_table_path: TablePath,
  pub this_table_key_range: TabletKeyRange,
  pub table_schema: TableSchema,

  // Region Isolation Algorithm
  pub verifying_writes: BTreeMap<Timestamp, VerifyingReadWriteRegion>,
  pub inserting_prepared_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub prepared_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub committed_writes: BTreeMap<Timestamp, ReadWriteRegion>,

  pub waiting_read_protected: BTreeMap<Timestamp, BTreeSet<RequestedReadProtected>>,
  pub inserting_read_protected: BTreeMap<Timestamp, BTreeSet<RequestedReadProtected>>,
  pub read_protected: BTreeMap<Timestamp, BTreeSet<TableRegion>>,

  // Schema Change and Locking
  pub waiting_locked_cols: HashMap<QueryId, RequestedLockedCols>,
  pub inserting_locked_cols: HashMap<QueryId, RequestedLockedCols>,

  /// For every `MSQueryES`, this maps its corresponding `root_query_id` to its `query_id`.
  /// This is for use when a MSTable*ES is constructed. This needs to be updated whenever an
  /// `MSQueryES` is added or removed.
  pub ms_root_query_map: HashMap<QueryId, QueryId>,

  // Paxos
  pub tablet_bundle: Vec<TabletPLm>,
}

impl<T: IOTypes> TabletState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    gossip: Arc<GossipData>,
    this_slave_group_id: SlaveGroupId,
    this_tablet_group_id: TabletGroupId,
  ) -> TabletState<T> {
    // TODO fix this logic
    let ((this_table_path, _), this_table_key_range) = (|| {
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
    let table_schema = gossip.db_schema.get(&(this_table_path.clone(), Gen(0))).unwrap().clone();
    TabletState {
      tablet_context: TabletContext::<T> {
        rand,
        clock,
        network_output,
        this_slave_group_id,
        this_tablet_group_id,
        sub_node_path: CTSubNodePath::Tablet(TabletGroupId("".to_string())),
        this_eid: EndpointId("".to_string()),
        gossip: gossip,
        leader_map: Default::default(),
        storage: GenericMVTable::new(),
        this_table_path,
        this_table_key_range,
        table_schema,
        verifying_writes: Default::default(),
        inserting_prepared_writes: Default::default(),
        prepared_writes: Default::default(),
        committed_writes: Default::default(),
        waiting_read_protected: Default::default(),
        inserting_read_protected: Default::default(),
        read_protected: Default::default(),
        waiting_locked_cols: Default::default(),
        inserting_locked_cols: Default::default(),
        ms_root_query_map: Default::default(),
        tablet_bundle: vec![],
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
      sub_node_path: &self.sub_node_path,
      leader_map: &self.leader_map,
      gossip: &mut self.gossip,
    }
  }

  fn handle_input(&mut self, statuses: &mut Statuses, tablet_input: TabletForwardMsg) {
    match tablet_input {
      TabletForwardMsg::TabletBundle(bundle) => {
        for plm in bundle {
          // TODO: This code is only if this is the leader
          match plm {
            TabletPLm::LockedCols(locked_cols) => {
              // Increase TableSchema LATs
              for col_name in &locked_cols.cols {
                if lookup(&self.table_schema.key_cols, col_name).is_none() {
                  self.table_schema.val_cols.update_lat(col_name, locked_cols.timestamp);
                }
              }

              // Remove RequestedLockedCols and grant GlobalLockedCols
              let req = self.inserting_locked_cols.remove(&locked_cols.query_id).unwrap();
              self.grant_global_locked_cols(statuses, req.orig_p, req.query_id);
            }
            TabletPLm::ReadProtected(read_protected) => {
              let req = self
                .remove_inserting_read_protected_request(
                  &read_protected.timestamp,
                  &read_protected.query_id,
                )
                .unwrap();
              btree_multimap_insert(
                &mut self.read_protected,
                &read_protected.timestamp,
                read_protected.region,
              );

              // Inform the originator.
              let query_id = req.orig_p.query_id;
              if let Some(read) = statuses.table_read_ess.get_mut(&query_id) {
                // TableReadES
                let action = read.es.global_read_protected(self, req.query_id);
                self.handle_read_es_action(statuses, query_id, action);
              }
            }
            TabletPLm::FinishQueryPrepared(prepared) => {
              let query_id = prepared.query_id;
              let finish_query_es = statuses.finish_query_ess.get_mut(&query_id).unwrap();
              let action = finish_query_es.handle_prepared_plm(self);
              self.handle_finish_query_es_action(statuses, query_id, action);
            }
            TabletPLm::FinishQueryCommitted(committed) => {
              let query_id = committed.query_id;
              let finish_query_es = statuses.finish_query_ess.get_mut(&query_id).unwrap();
              let action = finish_query_es.handle_committed_plm(self);
              self.handle_finish_query_es_action(statuses, query_id, action);
            }
            TabletPLm::FinishQueryAborted(aborted) => {
              let query_id = aborted.query_id;
              let finish_query_es = statuses.finish_query_ess.get_mut(&query_id).unwrap();
              let action = finish_query_es.handle_aborted_plm(self);
              self.handle_finish_query_es_action(statuses, query_id, action);
            }
            TabletPLm::AlterTablePrepared(_) => panic!(),
            TabletPLm::AlterTableCommitted(_) => panic!(),
            TabletPLm::AlterTableAborted(_) => panic!(),
            TabletPLm::DropTablePrepared(_) => panic!(),
            TabletPLm::DropTableCommitted(_) => panic!(),
            TabletPLm::DropTableAborted(_) => panic!(),
          }
        }
      }
      TabletForwardMsg::TabletMessage(message) => {
        match message {
          msg::TabletMessage::PerformQuery(perform_query) => {
            match perform_query.query {
              msg::GeneralQuery::SuperSimpleTransTableSelectQuery(query) => {
                // First, we check if the GRQueryES still exists in the Statuses, continuing
                // if so and aborting if not.
                if let Some(gr_query) =
                  statuses.gr_query_ess.get(&query.location_prefix.source.query_id)
                {
                  // Construct and start the TransQueryReplanningES
                  let trans_table = map_insert(
                    &mut statuses.trans_table_read_ess,
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

                  let action = trans_table.es.start(&mut self.ctx(), &gr_query.es);
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
                  match self.get_msquery_id(
                    statuses,
                    root_query_path.clone(),
                    query.timestamp,
                    &query.query_plan.query_leader_map,
                  ) {
                    Ok(ms_query_id) => {
                      // Lookup the MSQueryES and add the new Query into `pending_queries`.
                      let ms_query_es = statuses.ms_query_ess.get_mut(&ms_query_id).unwrap();
                      ms_query_es.pending_queries.insert(perform_query.query_id.clone());

                      // Create an MSReadTableES in the QueryReplanning state, and start it.
                      let ms_read = map_insert(
                        &mut statuses.ms_table_read_ess,
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
                    &mut statuses.table_read_ess,
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
                        waiting_global_locks: Default::default(),
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
                match self.get_msquery_id(
                  statuses,
                  root_query_path.clone(),
                  query.timestamp,
                  &query.query_plan.query_leader_map,
                ) {
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
                      &mut statuses.ms_table_write_ess,
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
            let query_id = prepare.query_id.clone();
            if let Some(finish_query_es) = statuses.finish_query_ess.get_mut(&query_id) {
              // If a FinishQueryES already exists, we route the prepare message there.
              let action = finish_query_es.handle_prepare(self);
              self.handle_finish_query_es_action(statuses, query_id, action);
            } else {
              if let Some(ms_query_es) = statuses.ms_query_ess.remove(&query_id) {
                self.ms_root_query_map.remove(&ms_query_es.root_query_path.query_id);
                debug_assert!(ms_query_es.pending_queries.is_empty());

                let timestamp = ms_query_es.timestamp;

                // Move the VerifyingReadWrite to inserting.
                let verifying = self.verifying_writes.remove(&timestamp).unwrap();
                debug_assert!(verifying.m_waiting_read_protected.is_empty());
                let region_lock = ReadWriteRegion {
                  orig_p: verifying.orig_p,
                  m_read_protected: verifying.m_read_protected,
                  m_write_protected: verifying.m_write_protected,
                };
                self.inserting_prepared_writes.insert(timestamp.clone(), region_lock.clone());

                // Construct a FinishQueryES
                let tm_lid = self.leader_map.get(&prepare.tm.node_path.sid.to_gid()).unwrap();
                map_insert(
                  &mut statuses.finish_query_ess,
                  &query_id,
                  FinishQueryES::FinishQueryExecuting(FinishQueryExecuting {
                    query_id: query_id.clone(),
                    tm: prepare.tm,
                    all_rms: prepare.all_rms,
                    region_lock,
                    timestamp,
                    update_view: compress_updates_views(&ms_query_es.update_views),
                    state: Paxos2PCRMState::WaitingInsertingPrepared(OrigTMLeadership {
                      orig_tm_lid: tm_lid.clone(),
                    }),
                  }),
                );
              } else {
                // The MSQueryES might not be present because of a DeadlockSafetyWriteAbort.
                let this_query_path = self.mk_query_path(prepare.query_id);
                self.ctx().send_to_c(
                  prepare.tm.node_path,
                  msg::CoordMessage::FinishQueryAborted(msg::FinishQueryAborted {
                    return_qid: prepare.tm.query_id,
                    rm_path: this_query_path,
                  }),
                );
              }
            }
          }
          msg::TabletMessage::FinishQueryAbort(abort) => {
            let query_id = abort.query_id.clone();
            if let Some(finish_query_es) = statuses.finish_query_ess.get_mut(&query_id) {
              let action = finish_query_es.handle_abort(self);
              self.handle_finish_query_es_action(statuses, query_id, action);
            } else {
              if let Some(ms_query_es) = statuses.ms_query_ess.remove(&query_id) {
                self.ms_root_query_map.remove(&ms_query_es.root_query_path.query_id);
                debug_assert!(ms_query_es.pending_queries.is_empty());
                self.verifying_writes.remove(&ms_query_es.timestamp).unwrap();
              }
            }
          }
          msg::TabletMessage::FinishQueryCheckPrepared(check_prepared) => {
            let query_id = check_prepared.query_id.clone();
            if let Some(finish_query_es) = statuses.finish_query_ess.get_mut(&query_id) {
              let action = finish_query_es.handle_check_prepared(self, check_prepared);
              self.handle_finish_query_es_action(statuses, query_id, action);
            } else {
              let this_query_path = self.mk_query_path(check_prepared.query_id);
              self.ctx().send_to_c(
                check_prepared.tm.node_path,
                msg::CoordMessage::FinishQueryAborted(msg::FinishQueryAborted {
                  return_qid: check_prepared.tm.query_id,
                  rm_path: this_query_path,
                }),
              );
            }
          }
          msg::TabletMessage::FinishQueryCommit(commit) => {
            let query_id = commit.query_id.clone();
            if let Some(finish_query_es) = statuses.finish_query_ess.get_mut(&query_id) {
              let action = finish_query_es.handle_commit(self);
              self.handle_finish_query_es_action(statuses, query_id, action);
            }
          }
          msg::TabletMessage::AlterTablePrepare(prepare) => {
            if let Some((_, es)) = &mut statuses.ddl_es {
              let alter_table_es = cast!(DDLES::Alter, es).unwrap();
              alter_table_es.handle_prepare(prepare, self);
            } else {
              // Construct the `preparing_timestamp`
              let mut timestamp = self.clock.now();
              timestamp =
                max(timestamp, self.table_schema.val_cols.get_lat(&prepare.alter_op.col_name));
              for (_, req) in
                self.waiting_locked_cols.iter().chain(self.inserting_locked_cols.iter())
              {
                if req.cols.contains(&prepare.alter_op.col_name) {
                  timestamp = max(timestamp, req.timestamp);
                }
              }
              timestamp += 1;

              // Construct an AlterTableES set it to `ddl_es`.
              let query_id = prepare.query_id;
              statuses.ddl_es = Some((
                query_id.clone(),
                DDLES::Alter(AlterTableES {
                  query_id,
                  alter_op: prepare.alter_op,
                  prepare_timestamp: timestamp,
                  state: State::WaitingInsertingPrepared,
                }),
              ));
            }
          }
          msg::TabletMessage::AlterTableAbort(abort) => {
            if let Some((_, es)) = &mut statuses.ddl_es {
              let alter_table_es = cast!(DDLES::Alter, es).unwrap();
              alter_table_es.handle_abort(abort, self);
            } else {
              // Send back a `CloseConfirm` to the Master.
              let payload =
                msg::MasterRemotePayload::AlterTableCloseConfirm(msg::AlterTableCloseConfirm {
                  query_id: abort.query_id,
                  tablet_group_id: self.this_tablet_group_id.clone(),
                });
              self.ctx().send_to_master(payload);
            }
          }
          msg::TabletMessage::AlterTableCommit(commit) => {
            if let Some((_, es)) = &mut statuses.ddl_es {
              let alter_table_es = cast!(DDLES::Alter, es).unwrap();
              alter_table_es.handle_commit(commit, self);
            } else {
              // Send back a `CloseConfirm` to the Master.
              let payload =
                msg::MasterRemotePayload::AlterTableCloseConfirm(msg::AlterTableCloseConfirm {
                  query_id: commit.query_id,
                  tablet_group_id: self.this_tablet_group_id.clone(),
                });
              self.ctx().send_to_master(payload);
            }
          }
          msg::TabletMessage::DropTablePrepare(prepare) => {
            if let Some((_, es)) = &mut statuses.ddl_es {
              let drop_table_es = cast!(DDLES::Drop, es).unwrap();
              drop_table_es.handle_prepare(prepare, self);
            } else {
              // Construct the `preparing_timestamp`
              let mut timestamp = self.clock.now();
              timestamp = max(timestamp, self.table_schema.val_cols.get_latest_lat());
              for (_, req) in
                self.waiting_locked_cols.iter().chain(self.inserting_locked_cols.iter())
              {
                timestamp = max(timestamp, req.timestamp);
              }
              timestamp += 1;

              // Construct an DropTableES set it to `ddl_es`.
              let query_id = prepare.query_id;
              statuses.ddl_es = Some((
                query_id.clone(),
                DDLES::Drop(DropTableES::DropExecuting(DropExecuting {
                  query_id,
                  prepare_timestamp: timestamp,
                  state: State::WaitingInsertingPrepared,
                })),
              ));
            }
          }
          msg::TabletMessage::DropTableAbort(abort) => {
            if let Some((_, es)) = &mut statuses.ddl_es {
              let drop_table_es = cast!(DDLES::Drop, es).unwrap();
              drop_table_es.handle_abort(abort, self);
            } else {
              // Send back a `CloseConfirm` to the Master.
              let payload =
                msg::MasterRemotePayload::DropTableCloseConfirm(msg::DropTableCloseConfirm {
                  query_id: abort.query_id,
                  tablet_group_id: self.this_tablet_group_id.clone(),
                });
              self.ctx().send_to_master(payload);
            }
          }
          msg::TabletMessage::DropTableCommit(commit) => {
            if let Some((_, es)) = &mut statuses.ddl_es {
              let drop_table_es = cast!(DDLES::Drop, es).unwrap();
              drop_table_es.handle_commit(commit, self);
            } else {
              // Send back a `CloseConfirm` to the Master.
              let payload =
                msg::MasterRemotePayload::DropTableCloseConfirm(msg::DropTableCloseConfirm {
                  query_id: commit.query_id,
                  tablet_group_id: self.this_tablet_group_id.clone(),
                });
              self.ctx().send_to_master(payload);
            }
          }
        }

        // Run Main Loop
        self.run_main_loop(statuses);
      }
      TabletForwardMsg::GossipData(gossip) => {
        self.gossip = gossip;

        // Inform Top-Level ESs.
        let query_ids: Vec<QueryId> = statuses.table_read_ess.keys().cloned().collect();
        for query_id in query_ids {
          let read = statuses.table_read_ess.get_mut(&query_id).unwrap();
          let action = read.es.gossip_data_changed(self);
          self.handle_read_es_action(statuses, query_id, action);
        }

        let query_ids: Vec<QueryId> = statuses.ms_table_read_ess.keys().cloned().collect();
        for query_id in query_ids {
          let ms_read = statuses.ms_table_read_ess.get_mut(&query_id).unwrap();
          let action = ms_read.es.gossip_data_changed(self);
          self.handle_ms_read_es_action(statuses, query_id, action);
        }

        let query_ids: Vec<QueryId> = statuses.ms_table_write_ess.keys().cloned().collect();
        for query_id in query_ids {
          let ms_write = statuses.ms_table_write_ess.get_mut(&query_id).unwrap();
          let action = ms_write.es.gossip_data_changed(self);
          self.handle_ms_write_es_action(statuses, query_id, action);
        }

        // Run Main Loop
        self.run_main_loop(statuses);
      }
      TabletForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();
        self.leader_map.insert(gid.clone(), lid.clone()); // Update the LeadershipId

        // For Top-Level ESs, if the sending PaxosGroup's Leadership changed, we ECU (no response).
        let query_ids: Vec<QueryId> = statuses.table_read_ess.keys().cloned().collect();
        for query_id in query_ids {
          let read = statuses.table_read_ess.get_mut(&query_id).unwrap();
          if read.sender_gid() == gid {
            self.exit_and_clean_up(statuses, query_id);
          }
        }

        let query_ids: Vec<QueryId> = statuses.trans_table_read_ess.keys().cloned().collect();
        for query_id in query_ids {
          let trans_read = statuses.trans_table_read_ess.get_mut(&query_id).unwrap();
          if trans_read.sender_gid() == gid {
            self.exit_and_clean_up(statuses, query_id);
          }
        }

        let query_ids: Vec<QueryId> = statuses.ms_table_read_ess.keys().cloned().collect();
        for query_id in query_ids {
          let ms_read = statuses.ms_table_read_ess.get_mut(&query_id).unwrap();
          if ms_read.sender_gid() == gid {
            self.exit_and_clean_up(statuses, query_id);
          }
        }

        let query_ids: Vec<QueryId> = statuses.ms_table_write_ess.keys().cloned().collect();
        for query_id in query_ids {
          let ms_write = statuses.ms_table_write_ess.get_mut(&query_id).unwrap();
          if ms_write.sender_gid() == gid {
            self.exit_and_clean_up(statuses, query_id);
          }
        }

        // Inform TMStatus
        if let PaxosGroupId::Slave(sid) = gid {
          let query_ids: Vec<QueryId> = statuses.tm_statuss.keys().cloned().collect();
          for query_id in query_ids {
            let tm_status = statuses.tm_statuss.get_mut(&query_id).unwrap();
            if let Some(cur_lid) = tm_status.leaderships.get(&sid) {
              if cur_lid < &lid {
                // The new Leadership of a remote slave has changed beyond what the TMStatus
                // had contacted, so that RM will surely not respond. Thus we abort this
                // whole TMStatus and inform the GRQueryES so that it can retry the stage.
                let gr_query_id = tm_status.orig_p.query_id.clone();
                self.exit_and_clean_up(statuses, query_id.clone());

                // Inform the GRQueryES
                let gr_query = statuses.gr_query_ess.get_mut(&query_id).unwrap();
                remove_item(&mut gr_query.child_queries, &query_id);
                let action = gr_query.es.handle_tm_remote_leadership_changed(&mut self.ctx());
                self.handle_gr_query_es_action(statuses, gr_query_id, action);
              }
            }
          }

          // Inform MSQueryES
          let query_ids: Vec<QueryId> = statuses.ms_query_ess.keys().cloned().collect();
          for query_id in query_ids {
            let ms_query_es = statuses.ms_query_ess.get_mut(&query_id).unwrap();
            let root_sid = &ms_query_es.root_query_path.node_path.sid;
            if root_sid == &sid && ms_query_es.root_lid < lid {
              // Here, the root PaxosNode is dead, so we simply ECU the MSQueryES.
              self.exit_and_clean_up(statuses, query_id);
            }
          }

          // Inform FinishQueryES
          let query_ids: Vec<QueryId> = statuses.finish_query_ess.keys().cloned().collect();
          for query_id in query_ids {
            let finish_query_es = statuses.finish_query_ess.get_mut(&query_id).unwrap();
            let action = finish_query_es.remote_leader_changed(self, remote_leader_changed.clone());
            self.handle_finish_query_es_action(statuses, query_id.clone(), action);
          }

          // TODO: inform ddl_es

          // Run Main Loop
          self.run_main_loop(statuses);
        }
      }
      TabletForwardMsg::LeaderChanged(leader_changed) => {
        let this_gid = self.this_slave_group_id.to_gid();
        self.leader_map.insert(this_gid, leader_changed.lid);

        // Inform FinishQueryES
        let query_ids: Vec<QueryId> = statuses.finish_query_ess.keys().cloned().collect();
        for query_id in query_ids {
          let finish_query_es = statuses.finish_query_ess.get_mut(&query_id).unwrap();
          let action = finish_query_es.leader_changed(self);
          self.handle_finish_query_es_action(statuses, query_id.clone(), action);
        }

        // Inform FinishQueryES
        if let Some((query_id, es)) = &mut statuses.ddl_es {
          // TODO: inform ddl_es properly
        }

        if self.is_leader() {
          // This means this node lost Leadership.

          // Wink away all TM ESs.
          statuses.gr_query_ess.clear();
          statuses.table_read_ess.clear();
          statuses.trans_table_read_ess.clear();
          statuses.tm_statuss.clear();
          statuses.ms_query_ess.clear();
          statuses.ms_table_read_ess.clear();
          statuses.ms_table_write_ess.clear();

          // Wink away all unpersisted Region Isolation Algorithm data
          self.verifying_writes.clear();
          self.inserting_prepared_writes.clear();
          self.waiting_read_protected.clear();
          self.inserting_read_protected.clear();

          // Wink away all unpersisted Column Locking Algorithm data
          self.waiting_locked_cols.clear();
          self.inserting_locked_cols.clear();
        }
      }
    }
  }

  /// Checks if the MSQueryES for `root_query_id` already exists, returning its
  /// `QueryId` if so. If not, we create an MSQueryES (populating `verifying_writes`,
  /// sending `RegisterQuery`, etc).
  fn get_msquery_id(
    &mut self,
    statuses: &mut Statuses,
    root_query_path: CQueryPath,
    timestamp: Timestamp,
    query_leader_map: &HashMap<SlaveGroupId, LeadershipId>,
  ) -> Result<QueryId, msg::QueryError> {
    let root_query_id = root_query_path.query_id.clone();
    if let Some(ms_query_id) = self.ms_root_query_map.get(&root_query_id) {
      // Here, the MSQueryES already exists, so we just return it's QueryId.
      return Ok(ms_query_id.clone());
    }

    // Otherwise, we need to create one. First check whether the Timestamp is available or not.
    if self.verifying_writes.contains_key(&timestamp)
      || self.prepared_writes.contains_key(&timestamp)
      || self.committed_writes.contains_key(&timestamp)
    {
      // This means the Timestamp is already in use, so we return an error.
      return Err(msg::QueryError::TimestampConflict);
    }

    let ms_query_id = mk_qid(&mut self.rand);

    // We check that the original Leadership of root_query_path is not dead,
    // returning a QueryError if so.
    let root_sid = root_query_path.node_path.sid.clone();
    let root_lid = query_leader_map.get(&root_sid).unwrap().clone();
    if self.leader_map.get(&root_sid.to_gid()).unwrap() > &root_lid {
      return Err(msg::QueryError::InvalidLeadershipId);
    }

    // Otherwise, send a register message back to the root.
    let ms_query_path = self.mk_query_path(ms_query_id.clone());
    self.ctx().send_to_c_lid(
      root_query_path.node_path.clone(),
      msg::CoordMessage::RegisterQuery(msg::RegisterQuery {
        root_query_id: root_query_id.clone(),
        query_path: ms_query_path,
      }),
      root_lid.clone(),
    );

    // This means that we can add an MSQueryES at the Timestamp
    statuses.ms_query_ess.insert(
      ms_query_id.clone(),
      MSQueryES {
        root_query_path,
        root_lid,
        query_id: ms_query_id.clone(),
        timestamp: timestamp.clone(),
        update_views: Default::default(),
        pending_queries: Default::default(),
      },
    );

    // We also amend the `ms_root_query_map` to associate the root query.
    self.ms_root_query_map.insert(root_query_id.clone(), ms_query_id.clone());

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

  /// Adds the following triple into `waiting_locked_cols`. Here, `orig_p` is the origiator
  /// who should get the locking result.
  pub fn add_requested_locked_columns(
    &mut self,
    orig_p: OrigP,
    timestamp: Timestamp,
    cols: Vec<ColName>,
  ) -> QueryId {
    let locked_cols_qid = mk_qid(&mut self.rand);
    self.waiting_locked_cols.insert(
      locked_cols_qid.clone(),
      RequestedLockedCols { query_id: locked_cols_qid.clone(), timestamp, cols, orig_p },
    );
    locked_cols_qid
  }

  /// This removes the Read Protection request from `waiting_read_protected` with the given
  /// `query_id` at the given `timestamp`, if it exists, and returns it.
  pub fn remove_read_protected_request(
    &mut self,
    timestamp: &Timestamp,
    query_id: &QueryId,
  ) -> Option<RequestedReadProtected> {
    if let Some(waiting) = self.waiting_read_protected.get_mut(&timestamp) {
      for protect_request in waiting.iter() {
        if &protect_request.query_id == query_id {
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

  /// This removes the Read Protection request from `inserting_read_protected` with the given
  /// `query_id` at the given `timestamp`, if it exists, and returns it.
  pub fn remove_inserting_read_protected_request(
    &mut self,
    timestamp: &Timestamp,
    query_id: &QueryId,
  ) -> Option<RequestedReadProtected> {
    if let Some(inserting) = self.inserting_read_protected.get_mut(&timestamp) {
      for protect_request in inserting.iter() {
        if &protect_request.query_id == query_id {
          // Here, we found a request with matching QueryId, so we remove it.
          let protect_request = protect_request.clone();
          inserting.remove(&protect_request);
          if inserting.is_empty() {
            self.inserting_read_protected.remove(&timestamp);
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
  ) -> Option<RequestedReadProtected> {
    if let Some(verifying_write) = self.verifying_writes.get_mut(timestamp) {
      for protect_request in verifying_write.m_waiting_read_protected.iter() {
        if &protect_request.query_id == query_id {
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
    for (_, inserting_prepared_write) in self.inserting_prepared_writes.range(bound) {
      if does_intersect(write_region, &inserting_prepared_write.m_read_protected) {
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
    for (_, inserting_read_regions) in self.inserting_read_protected.range(bound) {
      let mut read_regions = BTreeSet::<TableRegion>::new();
      for req in inserting_read_regions {
        read_regions.insert(req.read_region.clone());
      }
      if does_intersect(write_region, &read_regions) {
        return false;
      }
    }

    // If we get here, it means we have Region Isolation.
    return true;
  }

  // The Main Loop
  fn run_main_loop(&mut self, statuses: &mut Statuses) {
    while !self.run_main_loop_iteration(statuses) {}
  }

  /// Thus runs one iteration of the Main Loop, returning `false` exactly when nothing changes.
  fn run_main_loop_iteration(&mut self, statuses: &mut Statuses) -> bool {
    // First, we see if we can satisfy Column Locking

    // Process `waiting_locked_cols`
    for (_, req) in &self.waiting_locked_cols {
      // First, we see if we can grant GlobalLockedCols immediately.
      let mut global_locked = true;
      for col_name in &req.cols {
        if lookup(&self.table_schema.key_cols, col_name).is_none() {
          if self.table_schema.val_cols.get_lat(col_name) < req.timestamp {
            global_locked = false;
            break;
          }
        }
      }

      if global_locked {
        let orig_p = req.orig_p.clone();
        let query_id = req.query_id.clone();
        self.waiting_locked_cols.remove(&query_id);
        self.grant_global_locked_cols(statuses, orig_p, query_id);
        return true;
      }

      // Next, we see if we can grant LocalLockedCols.
      match &statuses.ddl_es {
        None => {
          // Here, there is no DDL; we can always grant LocalLockedCols.
          let query_id = req.query_id.clone();
          self.grant_local_locked_cols(statuses, query_id);
          return true;
        }
        Some((_, ddl_es)) => {
          // When a DDL ES is present, we must verify the `req` does not conflict.
          match ddl_es {
            DDLES::Alter(es) => {
              let mut conflicts = false;
              for col_name in &req.cols {
                if &es.alter_op.col_name == col_name && req.timestamp >= es.prepare_timestamp {
                  conflicts = true;
                }
              }
              if !conflicts {
                // Grant LocalLockedCols
                let query_id = req.query_id.clone();
                self.grant_local_locked_cols(statuses, query_id);
                return true;
              }
            }
            DDLES::Drop(es) => match es {
              DropTableES::Committed(dropped_timestamp) => {
                if &req.timestamp < dropped_timestamp {
                  // Grant LocalLockedCols
                  let query_id = req.query_id.clone();
                  self.grant_local_locked_cols(statuses, query_id);
                } else {
                  // Grant TableDropped
                  let orig_p = req.orig_p.clone();
                  let query_id = req.query_id.clone();
                  self.waiting_locked_cols.remove(&query_id);
                  self.grant_table_dropped(statuses, orig_p);
                }
                return true;
              }
              DropTableES::DropExecuting(exec) => {
                if req.timestamp < exec.prepare_timestamp {
                  // Grant LocalLockedCols
                  let query_id = req.query_id.clone();
                  self.grant_local_locked_cols(statuses, query_id);
                  return true;
                }
              }
            },
          }
        }
      }
    }

    // Next, we see if we can provide Region Protection

    // To account for both `verifying_writes` and `prepared_writes`, we merge them into a
    // single container similar to `verifying_writes`. This should be optimized later.
    let mut all_cur_writes = BTreeMap::<Timestamp, VerifyingReadWriteRegion>::new();
    for (cur_timestamp, verifying_write) in &self.verifying_writes {
      all_cur_writes.insert(*cur_timestamp, verifying_write.clone());
    }
    let write_it = self.inserting_prepared_writes.iter().chain(self.prepared_writes.iter());
    for (cur_timestamp, prepared_write) in write_it {
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
        self.grant_local_read_protected(statuses, *timestamp, protect_request);
        return true;
      }

      // Next, process all `m_read_protected`s for the first `verifying_write`
      for protect_request in &verifying_write.m_waiting_read_protected {
        self.grant_m_local_read_protected(
          statuses,
          *first_write_timestamp,
          protect_request.clone(),
        );
        return true;
      }

      // Next, accumulate the WriteRegions, and then search for region protection with
      // all subsequent `(m_)waiting_read_protected`s.
      let mut cum_write_regions = verifying_write.m_write_protected.clone();
      let mut prev_write_timestamp = first_write_timestamp;
      let bound = (Bound::Excluded(first_write_timestamp), Bound::Unbounded);
      for (cur_timestamp, verifying_write) in all_cur_writes.range(bound) {
        // The loop state is that `cum_write_regions` contains all WriteRegions <=
        // `prev_write_timestamp`, all `m_waiting_read_protected` <= `prev_write_timestamp`
        // have been processed, and all `waiting_read_protected`s < `prev_write_timestamp`
        // have been processed.

        // Process `m_read_protected`
        for protect_request in &verifying_write.m_waiting_read_protected {
          if !does_intersect(&protect_request.read_region, &cum_write_regions) {
            self.grant_m_local_read_protected(statuses, *cur_timestamp, protect_request.clone());
            return true;
          }
        }

        // Process `read_protected`
        let bound = (Bound::Included(prev_write_timestamp), Bound::Excluded(cur_timestamp));
        for (timestamp, set) in self.waiting_read_protected.range(bound) {
          for protect_request in set {
            if !does_intersect(&protect_request.read_region, &cum_write_regions) {
              self.grant_local_read_protected(statuses, *timestamp, protect_request.clone());
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
            self.grant_local_read_protected(statuses, *timestamp, protect_request.clone());
            return true;
          }
        }
      }
    } else {
      for (timestamp, set) in &self.waiting_read_protected {
        for protect_request in set {
          self.grant_local_read_protected(statuses, *timestamp, protect_request.clone());
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

    return false;
  }

  /// Route the column locking to the appropriate ES. Here, `query_id` is that of
  /// the `waiting_locked_cols` that can be moved forward.
  fn grant_local_locked_cols(&mut self, statuses: &mut Statuses, locked_cols_qid: QueryId) {
    // Move the RequestedLockedCols to `inserting_*`
    let req = self.waiting_locked_cols.remove(&locked_cols_qid).unwrap();
    self.inserting_locked_cols.insert(locked_cols_qid.clone(), req.clone());
    self.tablet_bundle.push(TabletPLm::LockedCols(plm::LockedCols {
      query_id: req.query_id.clone(),
      timestamp: req.timestamp,
      cols: req.cols,
    }));

    // Inform the ES.
    let query_id = req.orig_p.query_id;
    if let Some(read) = statuses.table_read_ess.get_mut(&query_id) {
      // TableReadES
      let action = read.es.local_locked_cols(self, locked_cols_qid);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write) = statuses.ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      let action = ms_write.es.local_locked_cols(self, locked_cols_qid);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.ms_table_read_ess.get_mut(&query_id) {
      // MSTableReadES
      let action = ms_read.es.local_locked_cols(self, locked_cols_qid);
      self.handle_ms_read_es_action(statuses, query_id, action);
    }
  }

  /// Route the column locking to the appropriate ES.
  fn grant_global_locked_cols(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    locked_cols_qid: QueryId,
  ) {
    let query_id = orig_p.query_id;
    if let Some(read) = statuses.table_read_ess.get_mut(&query_id) {
      // TableReadES
      let action = read.es.global_locked_cols(self, locked_cols_qid);
      self.handle_read_es_action(statuses, query_id, action);
    }
  }

  /// Route the column locking to the appropriate ES.
  fn grant_table_dropped(&mut self, statuses: &mut Statuses, orig_p: OrigP) {
    let query_id = orig_p.query_id;
    if let Some(read) = statuses.table_read_ess.get_mut(&query_id) {
      // TableReadES
      let action = read.es.table_dropped(self);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write) = statuses.ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      let action = ms_write.es.table_dropped(self);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.ms_table_read_ess.get_mut(&query_id) {
      // MSTableReadES
      let action = ms_read.es.table_dropped(self);
      self.handle_ms_read_es_action(statuses, query_id, action);
    }
  }

  /// Move the ProtectRequest in `waiting_read_protected` forward.
  fn grant_local_read_protected(
    &mut self,
    statuses: &mut Statuses,
    timestamp: Timestamp,
    protect_request: RequestedReadProtected,
  ) {
    self.remove_read_protected_request(&timestamp, &protect_request.query_id).unwrap();
    btree_multimap_insert(&mut self.inserting_read_protected, &timestamp, protect_request.clone());
    self.tablet_bundle.push(TabletPLm::ReadProtected(plm::ReadProtected {
      query_id: protect_request.query_id.clone(),
      timestamp,
      region: protect_request.read_region,
    }));

    // Inform the originator.
    let query_id = protect_request.orig_p.query_id;
    if let Some(read) = statuses.table_read_ess.get_mut(&query_id) {
      // TableReadES
      let action = read.es.local_read_protected(self, protect_request.query_id);
      self.handle_read_es_action(statuses, query_id, action);
    }
  }

  /// Move the ProtectRequest in `m_waiting_read_protected` forward.
  fn grant_m_local_read_protected(
    &mut self,
    statuses: &mut Statuses,
    timestamp: Timestamp,
    protect_request: RequestedReadProtected,
  ) {
    let verifying_write = self.verifying_writes.get_mut(&timestamp).unwrap();
    verifying_write.m_waiting_read_protected.remove(&protect_request);
    verifying_write.m_read_protected.insert(protect_request.read_region);

    // Inform the originator.
    let query_id = protect_request.orig_p.query_id;
    if let Some(ms_write) = statuses.ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      let action = ms_write.es.local_read_protected(
        self,
        statuses.ms_query_ess.get_mut(&ms_write.es.ms_query_id).unwrap(),
        protect_request.query_id,
      );
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.ms_table_read_ess.get_mut(&query_id) {
      // MSTableReadES
      let action = ms_read.es.local_read_protected(
        self,
        statuses.ms_query_ess.get(&ms_read.es.ms_query_id).unwrap(),
        protect_request.query_id,
      );
      self.handle_ms_read_es_action(statuses, query_id, action);
    }
  }

  /// Simply aborts the MSQueryES, which will clean up everything to do with it.
  fn deadlock_safety_write_abort(&mut self, statuses: &mut Statuses, orig_p: OrigP, _: Timestamp) {
    self.exit_ms_query_es(statuses, orig_p.query_id, msg::QueryError::DeadlockSafetyAbortion);
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
    if let Some(tm_status) = statuses.tm_statuss.get(tm_query_id) {
      // We ECU this TMStatus by sending CancelQuery to all remaining participants.
      // Then, we propagate the QueryAborted back to the orig_p.
      let gr_query_id = tm_status.orig_p.query_id.clone();
      self.exit_and_clean_up(statuses, tm_query_id.clone());

      // Then, inform the GRQueryES
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
    subquery_new_rms: HashSet<TQueryPath>,
    result: (Vec<ColName>, Vec<TableView>),
  ) {
    let query_id = orig_p.query_id;
    if let Some(read) = statuses.table_read_ess.get_mut(&query_id) {
      // TableReadES
      remove_item(&mut read.child_queries, &subquery_id);
      let action = read.es.handle_subquery_done(self, subquery_id, subquery_new_rms, result);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(trans_read) = statuses.trans_table_read_ess.get_mut(&query_id) {
      // TransTableReadES
      let prefix = trans_read.es.location_prefix();
      remove_item(&mut trans_read.child_queries, &subquery_id);
      let action = if let Some(gr_query) = statuses.gr_query_ess.get(&prefix.source.query_id) {
        trans_read.es.handle_subquery_done(
          &mut self.ctx(),
          &gr_query.es,
          subquery_id,
          subquery_new_rms,
          result,
        )
      } else {
        trans_read.es.handle_internal_query_error(&mut self.ctx(), msg::QueryError::LateralError)
      };
      self.handle_trans_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write) = statuses.ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      remove_item(&mut ms_write.child_queries, &subquery_id);
      let action = ms_write.es.handle_subquery_done(
        self,
        statuses.ms_query_ess.get_mut(&ms_write.es.ms_query_id).unwrap(),
        subquery_id,
        subquery_new_rms,
        result,
      );
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.ms_table_read_ess.get_mut(&query_id) {
      // MSTableReadES
      remove_item(&mut ms_read.child_queries, &subquery_id);
      let action = ms_read.es.handle_subquery_done(
        self,
        statuses.ms_query_ess.get(&ms_read.es.ms_query_id).unwrap(),
        subquery_id,
        subquery_new_rms,
        result,
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
    if let Some(read) = statuses.table_read_ess.get_mut(&query_id) {
      // TableReadES
      remove_item(&mut read.child_queries, &subquery_id);
      let action = read.es.handle_internal_query_error(self, query_error);
      self.handle_read_es_action(statuses, query_id, action);
    } else if let Some(trans_read) = statuses.trans_table_read_ess.get_mut(&query_id) {
      // TransTableReadES
      remove_item(&mut trans_read.child_queries, &subquery_id);
      let action = trans_read.es.handle_internal_query_error(&mut self.ctx(), query_error);
      self.handle_trans_read_es_action(statuses, query_id, action);
    } else if let Some(ms_write) = statuses.ms_table_write_ess.get_mut(&query_id) {
      // MSTableWriteES
      remove_item(&mut ms_write.child_queries, &subquery_id);
      let action = ms_write.es.handle_internal_query_error(self, query_error);
      self.handle_ms_write_es_action(statuses, query_id, action);
    } else if let Some(ms_read) = statuses.ms_table_read_ess.get_mut(&query_id) {
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

  /// Handles the actions specified by a FinishQueryES.
  fn handle_finish_query_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: FinishQueryAction,
  ) {
    match action {
      FinishQueryAction::Wait => {}
      FinishQueryAction::Exit => {
        statuses.finish_query_ess.remove(&query_id);
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
        let trans_read = statuses.trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = trans_read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
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
        let trans_read = statuses.trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = trans_read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
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
        let read = statuses.trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
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
        let read = statuses.table_read_ess.remove(&query_id).unwrap();
        let sender_path = read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
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
    self.ms_root_query_map.remove(&ms_query_es.root_query_path.query_id);

    // Then, we ECU all ESs in `pending_queries`, and respond with an Abort.
    for query_id in ms_query_es.pending_queries {
      if let Some(mut ms_read) = statuses.ms_table_read_ess.remove(&query_id) {
        // MSTableReadES
        ms_read.es.exit_and_clean_up(self);
        let sender_path = ms_read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        );
        self.exit_all(statuses, ms_read.child_queries);
      } else if let Some(mut ms_write) = statuses.ms_table_write_ess.remove(&query_id) {
        // MSTableWriteES
        ms_write.es.exit_and_clean_up(self);
        let sender_path = ms_write.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
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
  }

  /// Handles the actions specified by an MSTableWriteES.
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
        let ms_write = statuses.ms_table_write_ess.remove(&query_id).unwrap();
        let ms_query_es = statuses.ms_query_ess.get_mut(&ms_write.es.ms_query_id).unwrap();
        ms_query_es.pending_queries.remove(&query_id);
        let sender_path = ms_write.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
          CommonQuery::QuerySuccess(msg::QuerySuccess {
            return_qid: sender_path.query_id,
            responder_path,
            result: success.result,
            new_rms: success.new_rms,
          }),
        )
      }
      MSTableWriteAction::QueryError(query_error) => {
        // Remove the MSWriteESWrapper, removing it from the MSQueryES, and respond.
        let ms_write = statuses.ms_table_write_ess.remove(&query_id).unwrap();
        let ms_query_es = statuses.ms_query_ess.get_mut(&ms_write.es.ms_query_id).unwrap();
        ms_query_es.pending_queries.remove(&query_id);
        let sender_path = ms_write.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        );
      }
    }
  }

  /// Handles the actions specified by an MSTableReadES.
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
        // Remove the MSReadESWrapper, removing it from the MSQueryES, and respond.
        let ms_read = statuses.ms_table_read_ess.remove(&query_id).unwrap();
        let ms_query_es = statuses.ms_query_ess.get_mut(&ms_read.es.ms_query_id).unwrap();
        ms_query_es.pending_queries.remove(&query_id);
        let sender_path = ms_read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
          CommonQuery::QuerySuccess(msg::QuerySuccess {
            return_qid: sender_path.query_id,
            responder_path,
            result: success.result,
            new_rms: success.new_rms,
          }),
        )
      }
      MSTableReadAction::QueryError(query_error) => {
        let ms_read = statuses.ms_table_read_ess.remove(&query_id).unwrap();
        let ms_query_es = statuses.ms_query_ess.get_mut(&ms_read.es.ms_query_id).unwrap();
        ms_query_es.pending_queries.remove(&query_id);
        let sender_path = ms_read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.ctx().send_to_ct(
          sender_path.node_path,
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        )
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
    // GRQueryES
    if let Some(mut gr_query) = statuses.gr_query_ess.remove(&query_id) {
      gr_query.es.exit_and_clean_up(&mut self.ctx());
      self.exit_all(statuses, gr_query.child_queries);
    }
    // TableReadES
    else if let Some(mut read) = statuses.table_read_ess.remove(&query_id) {
      read.es.exit_and_clean_up(self);
      self.exit_all(statuses, read.child_queries);
    }
    // TransTableReadES
    else if let Some(mut trans_read) = statuses.trans_table_read_ess.remove(&query_id) {
      trans_read.es.exit_and_clean_up(&mut self.ctx());
      self.exit_all(statuses, trans_read.child_queries);
    }
    // TMStatus
    else if let Some(tm_status) = statuses.tm_statuss.remove(&query_id) {
      // We ECU this TMStatus by sending CancelQuery to all remaining RMs.
      for (rm_path, rm_result) in tm_status.tm_state {
        if rm_result.is_none() {
          let orig_sid = &rm_path.sid;
          let orig_lid = tm_status.leaderships.get(&orig_sid).unwrap().clone();
          self.ctx().send_to_ct_lid(
            rm_path,
            CommonQuery::CancelQuery(msg::CancelQuery {
              query_id: tm_status.child_query_id.clone(),
            }),
            orig_lid,
          );
        }
      }
    }
    // MSQueryES
    else if let Some(ms_query_es) = statuses.ms_query_ess.get(&query_id) {
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
    }
    // MSTableWriteES
    else if let Some(mut ms_write) = statuses.ms_table_write_ess.remove(&query_id) {
      let ms_query_es = statuses.ms_query_ess.get_mut(&ms_write.es.ms_query_id).unwrap();
      ms_query_es.pending_queries.remove(&query_id);
      ms_write.es.exit_and_clean_up(self);
      self.exit_all(statuses, ms_write.child_queries);
    }
    // MSTableReadES
    else if let Some(mut ms_read) = statuses.ms_table_read_ess.remove(&query_id) {
      let ms_query_es = statuses.ms_query_ess.get_mut(&ms_read.es.ms_query_id).unwrap();
      ms_query_es.pending_queries.remove(&query_id);
      ms_read.es.exit_and_clean_up(self);
      self.exit_all(statuses, ms_read.child_queries);
    }
  }

  /// Construct QueryPath for a given `query_id` that belongs to this Tablet.
  pub fn mk_query_path(&self, query_id: QueryId) -> TQueryPath {
    TQueryPath {
      node_path: TNodePath {
        sid: self.this_slave_group_id.clone(),
        sub: TSubNodePath::Tablet(self.this_tablet_group_id.clone()),
      },
      query_id,
    }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&self.this_slave_group_id.to_gid()).unwrap();
    lid.eid == self.this_eid
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
