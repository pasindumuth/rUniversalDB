use crate::col_usage::{
  collect_select_subqueries, collect_top_level_cols, collect_update_subqueries,
  node_external_trans_tables, nodes_external_trans_tables, ColUsagePlanner, FrozenColUsageNode,
};
use crate::common::{
  lookup_pos, merge_table_views, mk_qid, GossipData, IOTypes, KeyBound, NetworkOut, OrigP,
  QueryPlan, TMStatus, TMWaitValue, TableRegion, TableSchema,
};
use crate::expression::{compute_bound, EvalError};
use crate::model::common::proc::{TableRef, ValExpr};
use crate::model::common::{
  iast, proc, ColType, ColValN, Context, ContextRow, ContextSchema, Gen, NodeGroupId, QueryPath,
  TableView, TierMap, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{
  ColName, ColVal, EndpointId, PrimaryKey, QueryId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::model::message::{AbortedData, TabletMessage};
use crate::multiversion_map::MVM;
use rand::RngCore;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::read;
use std::iter::FromIterator;
use std::ops::{Add, Bound, Deref, Sub};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  CommonQueryReplanningES
// -----------------------------------------------------------------------------------------------

trait QueryReplanningSqlView {
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

  fn exprs(&self) -> Vec<ValExpr> {
    vec![self.selection.clone()]
  }

  fn ms_query_stage(&self) -> proc::MSQueryStage {
    proc::MSQueryStage::SuperSimpleSelect(self.clone())
  }
}

impl QueryReplanningSqlView for proc::Update {
  fn projected_cols(&self, table_schema: &TableSchema) -> Vec<ColName> {
    let mut projected_cols = Vec::from_iter(self.assignment.iter().map(|(col, _)| col.clone()));
    projected_cols.extend(table_schema.key_cols.iter().map(|(col, _)| col.clone()));
    return projected_cols;
  }

  fn table(&self) -> &TablePath {
    &self.table
  }

  fn exprs(&self) -> Vec<ValExpr> {
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
  pub orig_p: OrigP,
  /// The state of the CommonQueryReplanningES
  pub state: CommonQueryReplanningS,
}

// -----------------------------------------------------------------------------------------------
//  SubqueryStatus
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
struct SubqueryLockingSchemas {
  // Recall that we only get to this State if a Subquery had failed. We hold onto
  // the prior ColNames and TransTableNames (rather than computating from the QueryPlan
  // again) so that we don't potentially lose prior ColName amendments.
  old_columns: Vec<ColName>,
  trans_table_names: Vec<TransTableName>,
  new_cols: Vec<ColName>,
  query_id: QueryId,
}

#[derive(Debug)]
struct SubqueryPendingReadRegion {
  new_columns: Vec<ColName>,
  trans_table_names: Vec<TransTableName>,
  read_region: TableRegion,
  query_id: QueryId,
}

#[derive(Debug)]
struct SubqueryPending {
  context: Rc<Context>,
}

#[derive(Debug)]
struct SubqueryFinished {
  context: Rc<Context>,
  result: Vec<TableView>,
}

#[derive(Debug)]
enum SingleSubqueryStatus {
  LockingSchemas(SubqueryLockingSchemas),
  PendingReadRegion(SubqueryPendingReadRegion),
  Pending(SubqueryPending),
  Finished(SubqueryFinished),
}

#[derive(Debug)]
struct SubqueryStatus {
  subqueries: HashMap<QueryId, SingleSubqueryStatus>,
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
struct Executing {
  completed: usize,
  /// This fields is used to associate a subquery's QueryId with the position
  /// that it appears in the SQL query.
  subquery_pos: Vec<QueryId>,
  subquery_status: SubqueryStatus,

  // We remember the row_region we had computed previously. If we have to protected
  // more ReadRegions due to InternalColumnsDNEs, the `row_region` will be the same.
  row_region: Vec<KeyBound>,
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
  query: proc::SuperSimpleSelect,
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
  pub query_id: QueryId,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES<proc::SuperSimpleSelect>,
}

#[derive(Debug)]
enum FullTableReadES {
  QueryReplanning(QueryReplanningES),
  Executing(TableReadES),
}

// -----------------------------------------------------------------------------------------------
//  GRQueryES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
struct ReadStage {
  stage_idx: usize,
  /// This fields maps the indices of the GRQueryES Context to that of the Context
  /// in this SubqueryStatus. We cache this here since it's computed when the child
  /// context is computed.
  parent_context_map: Vec<usize>,
  subquery_id: QueryId,
  single_subquery_status: SingleSubqueryStatus,
}

#[derive(Debug)]
struct MasterQueryReplanning {
  master_query_id: QueryId,
}

#[derive(Debug)]
enum GRExecutionS {
  Start,
  ReadStage(ReadStage),
  MasterQueryReplanning(MasterQueryReplanning),
}

// Recall that the elements don't need to preserve the order of the TransTables, since the
// sql_query does that for us (thus, we can use HashMaps).
#[derive(Debug)]
struct GRQueryPlan {
  pub gossip_gen: Gen,
  pub trans_table_schemas: HashMap<TransTableName, Vec<ColName>>,
  pub col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
}

#[derive(Debug)]
struct GRQueryES {
  root_query_path: QueryPath,
  tier_map: TierMap,
  timestamp: Timestamp,
  context: Rc<Context>,

  /// The elements of the outer Vec corresponds to every ContextRow in the
  /// `context`. The elements of the inner vec corresponds to the elements in
  /// `trans_table_view`. The `usize` indexes into an element in the corresponding
  /// Vec<TableView> inside the `trans_table_view`.
  new_trans_table_context: Vec<Vec<usize>>,

  // Fields needed for responding.
  query_id: QueryId,

  // Query-related fields.
  query: proc::GRQuery,
  query_plan: GRQueryPlan,

  // The dynamically evolving fields.
  new_rms: HashSet<QueryPath>,
  trans_table_view: Vec<(TransTableName, (Vec<ColName>, Vec<TableView>))>,
  state: GRExecutionS,

  /// This holds the path to the parent ES.
  orig_p: OrigP,
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
struct MSWritePending {
  read_region: TableRegion,
  query_id: QueryId,
}

#[derive(Debug)]
enum MSWriteExecutionS {
  Start,
  Pending(MSWritePending),
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
  query: proc::Update,
  query_plan: QueryPlan,

  // MSQuery fields
  ms_query_id: QueryId,

  // Dynamically evolving fields.
  new_rms: HashSet<QueryPath>,
  state: MSWriteExecutionS,
}

#[derive(Debug)]
struct MSWriteQueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to TableReadES.
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  pub query_id: QueryId,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES<proc::Update>,
}

#[derive(Debug)]
enum FullMSTableWriteES {
  QueryReplanning(MSWriteQueryReplanningES),
  Executing(MSTableWriteES),
}

// -----------------------------------------------------------------------------------------------
//  MSTableReadES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct MSTableReadES {}

#[derive(Debug)]
struct MSReadQueryReplanningES {
  pub tier_map: TierMap,
  pub sql_query: proc::SuperSimpleSelect,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES<proc::SuperSimpleSelect>,
}

#[derive(Debug)]
enum FullMSTableReadES {
  QueryReplanning(MSReadQueryReplanningES),
  Executing(MSTableReadES),
}

// -----------------------------------------------------------------------------------------------
//  ReadStatus
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum TabletStatus {
  GRQueryES(GRQueryES),
  FullTableReadES(FullTableReadES),
  TMStatus(TMStatus),
  MSQueryES(MSQueryES),
  FullMSTableReadES(FullMSTableReadES),
  FullMSTableWriteES(FullMSTableWriteES),
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

// TODO: sync the ProtectRequest with the Pseudocode
enum ReadProtectionGrant {
  Read { timestamp: Timestamp, protect_request: (OrigP, QueryId, TableRegion) },
  MRead { timestamp: Timestamp, protect_request: (OrigP, QueryId, TableRegion) },
  DeadlockSafetyReadAbort { timestamp: Timestamp, protect_request: (OrigP, QueryId, TableRegion) },
  DeadlockSafetyWriteAbort { timestamp: Timestamp, orig_p: OrigP },
}

/// This is a convenience enum to strealine the sending of PCSA messages to Tablets and Slaves.
enum CommonQuery {
  PerformQuery(msg::PerformQuery),
  CancelQuery(msg::CancelQuery),
  QueryAborted(msg::QueryAborted),
  QuerySuccess(msg::QuerySuccess),
}

impl CommonQuery {
  fn tablet_msg(self) -> msg::TabletMessage {
    match self {
      CommonQuery::PerformQuery(query) => msg::TabletMessage::PerformQuery(query),
      CommonQuery::CancelQuery(query) => msg::TabletMessage::CancelQuery(query),
      CommonQuery::QueryAborted(query) => msg::TabletMessage::QueryAborted(query),
      CommonQuery::QuerySuccess(query) => msg::TabletMessage::QuerySuccess(query),
    }
  }

  fn slave_msg(self) -> msg::SlaveMessage {
    match self {
      CommonQuery::PerformQuery(query) => msg::SlaveMessage::PerformQuery(query),
      CommonQuery::CancelQuery(query) => msg::SlaveMessage::CancelQuery(query),
      CommonQuery::QueryAborted(query) => msg::SlaveMessage::QueryAborted(query),
      CommonQuery::QuerySuccess(query) => msg::SlaveMessage::QuerySuccess(query),
    }
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

type GenericMVTable = MVM<(PrimaryKey, Option<ColName>), Option<ColVal>>;
type GenericTable = HashMap<(PrimaryKey, Option<ColName>), Option<ColVal>>;

/// A very minor alias to make the less verbose, since it's used so much.
type Statuses = HashMap<QueryId, TabletStatus>;

#[derive(Debug)]
pub struct TabletState<T: IOTypes> {
  tablet_context: TabletContext<T>,
  statuses: HashMap<QueryId, TabletStatus>,
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
  fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::TabletMessage) {
    match message {
      msg::TabletMessage::PerformQuery(perform_query) => {
        match perform_query.query {
          msg::GeneralQuery::SuperSimpleTransTableSelectQuery(_) => unimplemented!(),
          msg::GeneralQuery::SuperSimpleTableSelectQuery(query) => {
            // We inspect the TierMap to see what kind of ES to create
            let table_path = cast!(proc::TableRef::TablePath, &query.sql_query.from).unwrap();
            if perform_query.tier_map.map.contains_key(table_path) {
              // Create an MSTableReadQueryES
              unimplemented!()
            } else {
              let mut comm_plan_es = CommonQueryReplanningES {
                timestamp: query.timestamp,
                context: Rc::new(query.context),
                sql_view: query.sql_query.clone(),
                query_plan: query.query_plan,
                sender_path: perform_query.sender_path,
                orig_p: OrigP { status_path: perform_query.query_id.clone() },
                state: CommonQueryReplanningS::Start,
              };
              comm_plan_es.start::<T>(self);
              // Add ReadStatus
              statuses.insert(
                perform_query.query_id.clone(),
                TabletStatus::FullTableReadES(FullTableReadES::QueryReplanning(
                  QueryReplanningES {
                    root_query_path: perform_query.root_query_path,
                    tier_map: perform_query.tier_map,
                    query_id: perform_query.query_id.clone(),
                    status: comm_plan_es,
                  },
                )),
              );
            }
          }
          msg::GeneralQuery::UpdateQuery(query) => {
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
                let sender_path = perform_query.sender_path;
                let abort_msg = msg::QueryAborted {
                  return_path: sender_path.query_id.clone(),
                  query_id: perform_query.query_id.clone(),
                  payload: msg::AbortedData::QueryError(msg::QueryError::TimestampConflict),
                };
                self.send_to_path(sender_path, CommonQuery::QueryAborted(abort_msg));
                return;
              } else {
                // This means that we can add an MSQueryES at the Timestamp
                let ms_query_id = mk_qid(&mut self.rand);
                statuses.insert(
                  ms_query_id.clone(),
                  TabletStatus::MSQueryES(MSQueryES {
                    query_id: ms_query_id.clone(),
                    timestamp: query.timestamp,
                    update_views: Default::default(),
                    pending_queries: Default::default(),
                  }),
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

                // FInally, add an empty VerifyingReadWriteRegion
                self.verifying_writes.insert(
                  timestamp,
                  VerifyingReadWriteRegion {
                    orig_p: OrigP { status_path: perform_query.query_id.clone() },
                    m_waiting_read_protected: BTreeSet::new(),
                    m_read_protected: BTreeSet::new(),
                    m_write_protected: BTreeSet::new(),
                  },
                );
              }
            }

            // Lookup the MSQuery and the QueryId of the new Query into `pending_queries`.
            let ms_query_id = self.ms_root_query_map.get(&root_query_id).unwrap();
            let tablet_status = statuses.get_mut(ms_query_id).unwrap();
            let ms_query = cast!(TabletStatus::MSQueryES, tablet_status).unwrap();
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
              orig_p: OrigP { status_path: perform_query.query_id.clone() },
              state: CommonQueryReplanningS::Start,
            };
            comm_plan_es.start::<T>(self);
            statuses.insert(
              perform_query.query_id.clone(),
              TabletStatus::FullMSTableWriteES(FullMSTableWriteES::QueryReplanning(
                MSWriteQueryReplanningES {
                  root_query_path: perform_query.root_query_path,
                  tier_map: perform_query.tier_map,
                  query_id: perform_query.query_id,
                  status: comm_plan_es,
                },
              )),
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

  /// Add the following triple into `requested_locked_columns`, making sure to update
  /// `request_index` as well.
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
    if let Some(set) = self.request_index.get_mut(&timestamp) {
      set.insert(locked_cols_qid.clone());
    } else {
      let mut set = BTreeSet::<QueryId>::new();
      set.insert(locked_cols_qid.clone());
      self.request_index.insert(timestamp, set);
    };
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
      self.request_index.get_mut(&timestamp).unwrap().remove(&query_id);
      if self.request_index.get_mut(&timestamp).unwrap().is_empty() {
        self.request_index.remove(&timestamp);
      }
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

  /// This function infers weather the CommonQuery is destined for a Slave or a Tablet
  /// by using the `sender_path`, and then acts accordingly.
  fn send_to_path(&mut self, sender_path: QueryPath, common_query: CommonQuery) {
    let sid = &sender_path.slave_group_id;
    let eid = self.slave_address_config.get(sid).unwrap();
    self.network_output.send(
      &eid,
      msg::NetworkMessage::Slave(
        if let Some(tablet_group_id) = sender_path.maybe_tablet_group_id {
          msg::SlaveMessage::TabletMessage(tablet_group_id.clone(), common_query.tablet_msg())
        } else {
          common_query.slave_msg()
        },
      ),
    );
  }

  /// This is similar to the above, except uses a `node_group_id`.
  fn send_to_node(&mut self, node_group_id: NodeGroupId, common_query: CommonQuery) {
    match node_group_id {
      NodeGroupId::Tablet(tid) => {
        let sid = self.tablet_address_config.get(&tid).unwrap();
        let eid = self.slave_address_config.get(sid).unwrap();
        self.network_output.send(
          eid,
          msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
            tid,
            common_query.tablet_msg(),
          )),
        );
      }
      NodeGroupId::Slave(sid) => {
        let eid = self.slave_address_config.get(&sid).unwrap();
        self.network_output.send(eid, msg::NetworkMessage::Slave(common_query.slave_msg()));
      }
    };
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
            if let Some(set) = self.read_protected.get_mut(&timestamp) {
              set.insert(read_region.clone());
            } else {
              let read_regions = vec![read_region.clone()];
              self.read_protected.insert(timestamp, read_regions.into_iter().collect());
            }

            // Inform the originator.
            self.read_protected_for_query(statuses, orig_p, query_id);
          }
          ReadProtectionGrant::MRead { timestamp, protect_request } => {
            // Remove the protect_request, adding the ReadRegion to `m_read_protected`.
            let mut verifying_write = self.verifying_writes.get_mut(&timestamp).unwrap();
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
    let query_id = orig_p.status_path;
    if let Some(tablet_status) = statuses.get_mut(&query_id) {
      match tablet_status {
        TabletStatus::GRQueryES(_) => panic!(),
        TabletStatus::FullTableReadES(read_es) => {
          match read_es {
            FullTableReadES::QueryReplanning(plan_es) => {
              // Advance the QueryReplanning now that the desired columns have been locked.
              let comm_plan_es = &mut plan_es.status;
              comm_plan_es.column_locked::<T>(self, query_id.clone());
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
                      query_id: query_id.clone(),
                      query: comm_plan_es.sql_view.clone(),
                      query_plan: comm_plan_es.query_plan.clone(),
                      new_rms: Default::default(),
                      state: ExecutionS::Start,
                    });
                    self.start_table_read_es(statuses, &query_id);
                  } else {
                    // Recall that if QueryReplanning had ended in a failure (i.e.
                    // having missing columns), then `CommonQueryReplanningES` will
                    // have send back the necessary responses. Thus, we only need to
                    // Exist the ES here.
                    statuses.remove(&query_id);
                  }
                }
                _ => {}
              }
            }
            FullTableReadES::Executing(es) => {
              let executing = cast!(ExecutionS::Executing, &mut es.state).unwrap();

              // Find the Subquery that sent out this requested_locked_columns. There should
              // always be such a Subquery.
              let (subquery_id, locking_status) = (|| {
                for (subquery_id, state) in &executing.subquery_status.subqueries {
                  match state {
                    SingleSubqueryStatus::LockingSchemas(locking_status) => {
                      if locking_status.query_id == locked_cols_qid {
                        return Some((subquery_id, locking_status));
                      }
                    }
                    _ => {}
                  }
                }
                return None;
              })()
              .unwrap();

              // None of the `new_cols` should already exist in the old subquery Context schema
              // (since they didn't exist when the GRQueryES complained).
              for col in &locking_status.new_cols {
                assert!(!locking_status.old_columns.contains(col));
              }

              // Next, we compute the subset of `new_cols` that aren't in the Table
              // Schema or the Context.
              let mut rem_cols = Vec::<ColName>::new();
              for col in &locking_status.new_cols {
                if !contains_col(&self.table_schema, col, &es.timestamp) {
                  if !es.context.context_schema.column_context_schema.contains(col) {
                    rem_cols.push(col.clone());
                  }
                }
              }

              if !rem_cols.is_empty() {
                // If there are missing columns, we Exit and clean up, and propagate
                // the Abort to the originator.

                // Construct a ColumnsDNE containing `missing_cols` and send it
                // back to the originator.
                let columns_dne_msg = msg::QueryAborted {
                  return_path: es.sender_path.query_id.clone(),
                  query_id: query_id.clone(),
                  payload: msg::AbortedData::ColumnsDNE { missing_cols: rem_cols },
                };
                let sender_path = es.sender_path.clone();
                self.send_to_path(sender_path, CommonQuery::QueryAborted(columns_dne_msg));

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
                let new_read_region = TableRegion {
                  col_region: new_col_region,
                  row_region: executing.row_region.clone(),
                };

                // Add a read protection requested
                let protect_query_id = mk_qid(&mut self.rand);
                let orig_p = OrigP { status_path: es.query_id.clone() };
                let protect_request = (orig_p, protect_query_id.clone(), new_read_region.clone());
                if let Some(waiting) = self.waiting_read_protected.get_mut(&es.timestamp) {
                  waiting.insert(protect_request);
                } else {
                  self
                    .waiting_read_protected
                    .insert(es.timestamp, vec![protect_request].into_iter().collect());
                }

                // Finally, update the SingleSubqueryStatus to wait for the Region Protection.
                let subquery_id = subquery_id.clone();
                let trans_table_names = locking_status.trans_table_names.clone();
                let subqueries = &mut executing.subquery_status.subqueries;
                let single_status = subqueries.get_mut(&subquery_id).unwrap();
                *single_status =
                  SingleSubqueryStatus::PendingReadRegion(SubqueryPendingReadRegion {
                    new_columns,
                    trans_table_names,
                    read_region: new_read_region,
                    query_id: protect_query_id,
                  })
              }
            }
          }
        }
        TabletStatus::TMStatus(_) => panic!(),
        TabletStatus::MSQueryES(_) => panic!(),
        TabletStatus::FullMSTableReadES(_) => panic!(),
        TabletStatus::FullMSTableWriteES(ms_write_es) => {
          match ms_write_es {
            FullMSTableWriteES::QueryReplanning(plan_es) => {
              // Advance the QueryReplanning now that the desired columns have been locked.
              let comm_plan_es = &mut plan_es.status;
              comm_plan_es.column_locked::<T>(self, query_id.clone());

              let root_query_id = &plan_es.root_query_path.query_id;
              let ms_query_id = self.ms_root_query_map.get(root_query_id).unwrap();
              // We check if the QueryReplanning is done.
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
                    tier_map.map.get(sql_query.table()).unwrap().sub(1);

                    // Then, we construct the MSTableWriteES.
                    *ms_write_es = FullMSTableWriteES::Executing(MSTableWriteES {
                      root_query_path: plan_es.root_query_path.clone(),
                      tier_map,
                      timestamp: comm_plan_es.timestamp,
                      tier,
                      context: comm_plan_es.context.clone(),
                      sender_path: comm_plan_es.sender_path.clone(),
                      query_id: query_id.clone(),
                      query: sql_query,
                      query_plan: comm_plan_es.query_plan.clone(),
                      ms_query_id: ms_query_id.clone(),
                      new_rms: Default::default(),
                      state: MSWriteExecutionS::Start,
                    });

                    // Finally, we start the MSWriteTableES.
                    self.start_ms_table_write_es(statuses, &query_id);
                  } else {
                    // Since `CommonQueryReplanningES` will have already sent back the necessary
                    // responses. Thus, we only need to exit the ES here.

                    // Remove that FullMSTableWriteES
                    statuses.remove(&query_id);

                    // Update that MSQuery
                    let tablet_status = statuses.get_mut(ms_query_id).unwrap();
                    let ms_query = cast!(TabletStatus::MSQueryES, tablet_status).unwrap();
                    ms_query.pending_queries.remove(&query_id);
                  }
                }
                _ => {}
              }
            }
            FullMSTableWriteES::Executing(_) => {}
          }
        }
      }
    }
  }

  /// Processes the Start state of TableReadES.
  fn start_table_read_es(&mut self, statuses: &mut Statuses, query_id: &QueryId) {
    let tablet_status = statuses.get_mut(&query_id).unwrap();
    let read_es = cast!(TabletStatus::FullTableReadES, tablet_status).unwrap();
    let es = cast!(FullTableReadES::Executing, read_es).unwrap();

    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &es.query.selection,
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
          self.handle_eval_error_table_es(statuses, &query_id, eval_error);
          return;
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(es.query.projection.clone());
    col_region.extend(es.query_plan.col_usage_node.safe_present_cols.clone());

    // Move the TableReadES to the Pending state with the given ReadRegion.
    let protect_query_id = mk_qid(&mut self.rand);
    let col_region: Vec<ColName> = col_region.into_iter().collect();
    let read_region = TableRegion { col_region, row_region };
    es.state = ExecutionS::Pending(Pending {
      read_region: read_region.clone(),
      query_id: protect_query_id.clone(),
    });

    // Add a read protection requested
    let orig_p = OrigP { status_path: es.query_id.clone() };
    let protect_request = (orig_p, protect_query_id, read_region);
    if let Some(waiting) = self.waiting_read_protected.get_mut(&es.timestamp) {
      waiting.insert(protect_request);
    } else {
      self.waiting_read_protected.insert(es.timestamp, vec![protect_request].into_iter().collect());
    }
  }

  /// Processes the Start state of MSTableWrite.
  fn start_ms_table_write_es(&mut self, statuses: &mut Statuses, query_id: &QueryId) {
    let tablet_status = statuses.get_mut(&query_id).unwrap();
    let ms_write_es = cast!(TabletStatus::FullMSTableWriteES, tablet_status).unwrap();
    let es = cast!(FullMSTableWriteES::Executing, ms_write_es).unwrap();

    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &es.query.selection,
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
          self.handle_eval_error_table_es(statuses, &query_id, eval_error);
          return;
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Compute the Write Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(es.query.assignment.iter().map(|(col, _)| col.clone()));

    // Compute the Write Region
    let col_region = Vec::from_iter(col_region.into_iter());
    let write_region = TableRegion { col_region, row_region: row_region.clone() };

    // Verify that we have WriteRegion Isolation with Subsequent Reads. We abort
    // if we don't, and we amend this MSQuery's VerifyingReadWriteRegions if we do.
    let timestamp = es.timestamp.clone();
    if !self.check_write_region_isolation(&write_region, &timestamp) {
      // Send an abortion.
      let sender_path = es.sender_path.clone();
      self.send_to_path(
        sender_path.clone(),
        CommonQuery::QueryAborted(msg::QueryAborted {
          return_path: sender_path.query_id.clone(),
          query_id: query_id.clone(),
          payload: msg::AbortedData::QueryError(
            msg::QueryError::WriteRegionConflictWithSubsequentRead,
          ),
        }),
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
      es.state = MSWriteExecutionS::Pending(MSWritePending {
        read_region: read_region.clone(),
        query_id: protect_query_id.clone(),
      });

      // Add a ReadRegion to the m_waiting_read_protected.
      let orig_p = OrigP { status_path: es.query_id.clone() };
      let protect_request = (orig_p, protect_query_id, read_region);
      let verifying = self.verifying_writes.get_mut(&es.timestamp).unwrap();
      verifying.m_waiting_read_protected.insert(protect_request);
    }
  }

  /// This is used to simply delete a TableReadES or MSTable*ES due to an expression
  /// evaluation error that occured inside, and respond to the client. Note that this
  /// function doesn't do any other kind of cleanup.
  fn handle_eval_error_table_es(
    &mut self,
    statuses: &mut Statuses,
    query_id: &QueryId,
    eval_error: EvalError,
  ) {
    let tablet_status = statuses.get(&query_id).unwrap();
    let sender_path = match tablet_status {
      TabletStatus::GRQueryES(_) => panic!(),
      TabletStatus::FullTableReadES(read_es) => {
        let es = cast!(FullTableReadES::Executing, read_es).unwrap();
        es.sender_path.clone()
      }
      TabletStatus::TMStatus(_) => panic!(),
      TabletStatus::MSQueryES(_) => panic!(),
      TabletStatus::FullMSTableReadES(_) => panic!(),
      TabletStatus::FullMSTableWriteES(ms_write_es) => {
        let es = cast!(FullMSTableWriteES::Executing, ms_write_es).unwrap();
        es.sender_path.clone()
      }
    };

    // If an error occurs here, we simply abort this whole query and respond
    // to the sender with an Abort.
    let aborted = msg::QueryAborted {
      return_path: sender_path.query_id.clone(),
      query_id: query_id.clone(),
      payload: msg::AbortedData::QueryError(msg::QueryError::TypeError {
        msg: format!("{:?}", eval_error),
      }),
    };

    self.send_to_path(sender_path, CommonQuery::QueryAborted(aborted));
    self.exit_and_clean_up(statuses, query_id.clone());
  }

  /// Here, the subquery GRQueryES at `subquery_id` must already be cleaned up. This
  /// function informs the ES at OrigP and updates its state accordingly.
  fn handle_internal_columns_dne(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    rem_cols: Vec<ColName>,
  ) {
    let query_id = orig_p.status_path;
    if let Some(tablet_status) = statuses.get_mut(&query_id) {
      match tablet_status {
        TabletStatus::GRQueryES(_) => panic!(),
        TabletStatus::FullTableReadES(read_es) => {
          let es = cast!(FullTableReadES::Executing, read_es).unwrap();
          let executing = cast!(ExecutionS::Executing, &mut es.state).unwrap();

          // Insert a requested_locked_columns for the missing columns.
          let locked_cols_qid = self.add_requested_locked_columns(
            OrigP { status_path: query_id.clone() },
            es.timestamp,
            rem_cols.clone(),
          );

          // We replace `subquery_id` with a new one to guarantee it never gets mangled
          // when we create a new GRQueryES. We also update `executing` accordingly.
          let new_subquery_id = mk_qid(&mut self.rand);
          let pos = executing.subquery_pos.iter().position(|id| &subquery_id == id).unwrap();
          executing.subquery_pos.insert(pos, new_subquery_id.clone());

          let old_subquery = executing.subquery_status.subqueries.remove(&subquery_id).unwrap();
          let old_pending = cast!(SingleSubqueryStatus::Pending, old_subquery).unwrap();
          let old_context_schema = &old_pending.context.context_schema;
          executing.subquery_status.subqueries.insert(
            new_subquery_id.clone(),
            SingleSubqueryStatus::LockingSchemas(SubqueryLockingSchemas {
              old_columns: old_context_schema.column_context_schema.clone(),
              trans_table_names: old_context_schema.trans_table_names(),
              new_cols: rem_cols,
              query_id: locked_cols_qid,
            }),
          );
        }
        TabletStatus::TMStatus(_) => panic!(),
        TabletStatus::MSQueryES(_) => panic!(),
        TabletStatus::FullMSTableReadES(_) => panic!(),
        TabletStatus::FullMSTableWriteES(_) => panic!(),
      }
    }
  }

  /// This computes GRQueryESs corresponding to every element in `subqueries`.
  fn compute_subqueries(
    &mut self,
    query_id: &QueryId,
    root_query_path: &QueryPath,
    tier_map: &TierMap,
    selection: &proc::ValExpr,
    subqueries: &Vec<proc::GRQuery>,
    timestamp: &Timestamp,
    context: &Context,
    query_plan: &QueryPlan,
  ) -> Result<Vec<GRQueryES>, EvalError> {
    // Iterate over every GRQuery. Compute the external_cols (by taking
    // just taking the union of all external_cols in the nodes in the corresponding
    // element in `children` in the query plan). This is the ColumnContextSchema.
    // Then compute TransTableSchema.

    // Split the ColumnContextSchema that over `safe_present_cols`, and `external_cols`
    // at the top-level. Start building the Context by iterating over the Main
    // Context.  We can take the `external_cols` split to take those cols. Then,
    // we can take the `safe_present_cols` split to compute a TableView.
    // I guess we can iterate over the primary key, see if it's in the split,
    // and then when computing the context, only take a range query over that.

    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &selection,
      &self.table_schema,
      &timestamp,
      &context.context_schema,
    );

    // We first compute all GRQueryESs before adding them to `tablet_status`, in case
    // an error occurs here
    let mut gr_query_statuses = Vec::<GRQueryES>::new();
    for subquery_index in 0..subqueries.len() {
      let subquery = subqueries.get(subquery_index).unwrap();
      let child = query_plan.col_usage_node.children.get(subquery_index).unwrap();

      // This computes a ContextSchema for the subquery, as well as expose a conversion
      // utility to compute ContextRows.
      let conv = ContextConverter::create_from_query_plan(
        &context.context_schema,
        &query_plan.col_usage_node,
        subquery_index,
      );

      // Construct the `ContextRow`s. To do this, we iterate over main Query's
      // `ContextRow`s and then the corresponding `ContextRow`s for the subquery.
      // We hold the child `ContextRow`s in Vec, and we use a HashSet to avoid duplicates.
      let mut new_context_rows = Vec::<ContextRow>::new();
      let mut new_row_set = HashSet::<ContextRow>::new();
      for context_row in &context.context_rows {
        // Next, we compute the tightest KeyBound for this `context_row`, compute the
        // corresponding subtable using `safe_present_split`, and then extend it by this
        // `context_row.column_context_row`. We also add the `TransTableContextRow`. This
        // results in a set of `ContextRows` that we add to the childs context.
        let key_bounds = keybound_computer.compute_keybounds(&context_row)?;
        let (_, mut subtable) =
          compute_subtable(&conv.safe_present_split, &key_bounds, &self.storage);
        for mut row in subtable {
          let new_context_row = conv.compute_child_context_row(context_row, row);
          if !new_row_set.contains(&new_context_row) {
            new_row_set.insert(new_context_row.clone());
            new_context_rows.push(new_context_row);
          }
        }
      }

      // Finally, compute the context.
      let context =
        Rc::new(Context { context_schema: conv.context_schema, context_rows: new_context_rows });

      // Construct the GRQueryES
      let gr_query_id = mk_qid(&mut self.rand);
      let gr_query_es = GRQueryES {
        root_query_path: root_query_path.clone(),
        tier_map: tier_map.clone(),
        timestamp: timestamp.clone(),
        context,
        new_trans_table_context: vec![],
        query_id: gr_query_id.clone(),
        query: subquery.clone(),
        query_plan: GRQueryPlan {
          gossip_gen: query_plan.gossip_gen.clone(),
          trans_table_schemas: query_plan.trans_table_schemas.clone(),
          col_usage_nodes: child.clone(),
        },
        new_rms: Default::default(),
        trans_table_view: vec![],
        state: GRExecutionS::Start,
        orig_p: OrigP { status_path: query_id.clone() },
      };
      gr_query_statuses.push(gr_query_es)
    }

    Ok(gr_query_statuses)
  }

  /// This recomputes GRQueryESs that corresponds to `protect_query_id`.
  fn recompute_subquery(
    &mut self,
    executing: &mut Executing,
    protect_query_id: &QueryId,
    query_id: &QueryId,
    root_query_path: &QueryPath,
    tier_map: &TierMap,
    selection: &proc::ValExpr,
    subqueries: &Vec<proc::GRQuery>,
    timestamp: &Timestamp,
    context: &Context,
    query_plan: &QueryPlan,
  ) -> Result<(QueryId, GRQueryES), EvalError> {
    // Find the subquery that this `protect_query_id` is referring to. There should
    // always be such a Subquery.
    let (subquery_id, protect_status) = (|| {
      for (subquery_id, state) in &executing.subquery_status.subqueries {
        match state {
          SingleSubqueryStatus::PendingReadRegion(protect_status) => {
            if &protect_status.query_id == protect_query_id {
              return Some((subquery_id, protect_status));
            }
          }
          _ => {}
        }
      }
      return None;
    })()
    .unwrap();

    // Setup ability to compute a tight Keybound for every ContextRow.
    let keybound_computer = ContextKeyboundComputer::new(
      &selection,
      &self.table_schema,
      &timestamp,
      &context.context_schema,
    );

    // Find the GRQuery that corresponds to this subquery
    let subquery_index = executing.subquery_pos.iter().position(|id| id == subquery_id).unwrap();
    let subquery = subqueries.get(subquery_index).unwrap();

    // This computes a ContextSchema for the subquery, as well as expose a conversion
    // utility to compute ContextRows. Notice we avoid using the child QueryPlan,
    // since it's out-of-date by this point.
    let conv = ContextConverter::general_create(
      &context.context_schema,
      protect_status.new_columns.clone(),
      protect_status.trans_table_names.clone(),
      &timestamp,
      &self.table_schema,
    );

    // Construct the `ContextRow`s. To do this, we iterate over main Query's
    // `ContextRow`s and then the corresponding `ContextRow`s for the subquery.
    // We hold the child `ContextRow`s in Vec, and we use a HashSet to avoid duplicates.
    let mut new_context_rows = Vec::<ContextRow>::new();
    let mut new_row_set = HashSet::<ContextRow>::new();
    for context_row in &context.context_rows {
      // Next, we compute the tightest KeyBound for this `context_row`, compute the
      // corresponding subtable using `safe_present_split`, and then extend it by this
      // `context_row.column_context_row`. We also add the `TransTableContextRow`. This
      // results in a set of `ContextRows` that we add to the childs context.
      let key_bounds = keybound_computer.compute_keybounds(&context_row)?;
      let (_, mut subtable) =
        compute_subtable(&conv.safe_present_split, &key_bounds, &self.storage);
      for mut row in subtable {
        let new_context_row = conv.compute_child_context_row(context_row, row);
        if !new_row_set.contains(&new_context_row) {
          new_row_set.insert(new_context_row.clone());
          new_context_rows.push(new_context_row);
        }
      }
    }

    // Finally, compute the context.
    let context =
      Rc::new(Context { context_schema: conv.context_schema, context_rows: new_context_rows });

    // Construct the GRQueryES
    let child = query_plan.col_usage_node.children.get(subquery_index).unwrap();
    let gr_query_es = GRQueryES {
      root_query_path: root_query_path.clone(),
      tier_map: tier_map.clone(),
      timestamp: timestamp.clone(),
      context: context.clone(),
      new_trans_table_context: vec![],
      query_id: subquery_id.clone(),
      query: subquery.clone(),
      query_plan: GRQueryPlan {
        gossip_gen: query_plan.gossip_gen.clone(),
        trans_table_schemas: query_plan.trans_table_schemas.clone(),
        col_usage_nodes: child.clone(),
      },
      new_rms: Default::default(),
      trans_table_view: vec![],
      state: GRExecutionS::Start,
      orig_p: OrigP { status_path: query_id.clone() },
    };

    // Advance the SingleSubqueryStatus, add in the subquery to `table_statuses`,
    // and start evaluating the GRQueryES
    let subquery_id = subquery_id.clone();
    let single_status = executing.subquery_status.subqueries.get_mut(&subquery_id).unwrap();
    *single_status = SingleSubqueryStatus::Pending(SubqueryPending { context });

    Ok((subquery_id.clone(), gr_query_es))
  }

  /// We get this if a ReadProtection was granted by the Main Loop. This includes standard
  /// read_protected, or m_read_protected.
  fn read_protected_for_query(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    protect_query_id: QueryId,
  ) {
    let query_id = orig_p.status_path;
    if let Some(tablet_status) = statuses.get_mut(&query_id) {
      match tablet_status {
        TabletStatus::GRQueryES(_) => panic!(),
        TabletStatus::FullTableReadES(read_es) => {
          let es = cast!(FullTableReadES::Executing, read_es).unwrap();
          match &mut es.state {
            ExecutionS::Start => panic!(),
            ExecutionS::Pending(pending) => {
              let gr_query_statuses = match self.compute_subqueries(
                &es.query_id,
                &es.root_query_path,
                &es.tier_map,
                &es.query.selection,
                &collect_select_subqueries(&es.query),
                &es.timestamp,
                &es.context,
                &es.query_plan,
              ) {
                Ok(gr_query_statuses) => gr_query_statuses,
                Err(eval_error) => {
                  self.handle_eval_error_table_es(statuses, &query_id, eval_error);
                  return;
                }
              };

              // Here, we have computed all GRQueryESs, and we can now add them to `read_statuses`
              // and move the TableReadESs state to `Executing`.
              let mut gr_query_ids = Vec::<QueryId>::new();
              let mut subquery_status = SubqueryStatus { subqueries: Default::default() };
              for gr_query_es in &gr_query_statuses {
                gr_query_ids.push(gr_query_es.query_id.clone());
                subquery_status.subqueries.insert(
                  gr_query_es.query_id.clone(),
                  SingleSubqueryStatus::Pending(SubqueryPending {
                    context: gr_query_es.context.clone(),
                  }),
                );
              }

              // Move the ES to the Executing state.
              es.state = ExecutionS::Executing(Executing {
                completed: 0,
                subquery_pos: gr_query_ids.clone(),
                subquery_status,
                row_region: pending.read_region.row_region.clone(),
              });

              // We do this after modifying `es.state` to avoid invalidating the `es` reference.
              for gr_query_es in gr_query_statuses {
                let query_id = gr_query_es.query_id.clone();
                statuses.insert(query_id, TabletStatus::GRQueryES(gr_query_es));
              }

              // Drive GRQueries
              for query_id in gr_query_ids {
                self.advance_gr_query(statuses, query_id);
              }
            }
            ExecutionS::Executing(executing) => {
              let (subquery_id, gr_query_es) = match self.recompute_subquery(
                executing,
                &protect_query_id,
                &es.query_id,
                &es.root_query_path,
                &es.tier_map,
                &es.query.selection,
                &collect_select_subqueries(&es.query),
                &es.timestamp,
                &es.context,
                &es.query_plan,
              ) {
                Ok(gr_query_statuses) => gr_query_statuses,
                Err(eval_error) => {
                  self.handle_eval_error_table_es(statuses, &query_id, eval_error);
                  return;
                }
              };

              statuses.insert(subquery_id.clone(), TabletStatus::GRQueryES(gr_query_es));
              self.advance_gr_query(statuses, subquery_id);
            }
          }
        }
        TabletStatus::TMStatus(_) => panic!(),
        TabletStatus::MSQueryES(_) => panic!(),
        TabletStatus::FullMSTableReadES(_) => panic!(),
        TabletStatus::FullMSTableWriteES(read_es) => {
          let es = cast!(FullMSTableWriteES::Executing, read_es).unwrap();
          match &mut es.state {
            MSWriteExecutionS::Start => panic!(),
            MSWriteExecutionS::Pending(pending) => {
              let gr_query_statuses = match self.compute_subqueries(
                &es.query_id,
                &es.root_query_path,
                &es.tier_map,
                &es.query.selection,
                &collect_update_subqueries(&es.query),
                &es.timestamp,
                &es.context,
                &es.query_plan,
              ) {
                Ok(gr_query_statuses) => gr_query_statuses,
                Err(eval_error) => {
                  self.handle_eval_error_table_es(statuses, &query_id, eval_error);
                  return;
                }
              };

              // Here, we have computed all GRQueryESs, and we can now add them to `read_statuses`
              // and move the TableReadESs state to `Executing`.
              let mut gr_query_ids = Vec::<QueryId>::new();
              let mut subquery_status = SubqueryStatus { subqueries: Default::default() };
              for gr_query_es in &gr_query_statuses {
                gr_query_ids.push(gr_query_es.query_id.clone());
                subquery_status.subqueries.insert(
                  gr_query_es.query_id.clone(),
                  SingleSubqueryStatus::Pending(SubqueryPending {
                    context: gr_query_es.context.clone(),
                  }),
                );
              }

              // Move the ES to the Executing state.
              es.state = MSWriteExecutionS::Executing(Executing {
                completed: 0,
                subquery_pos: gr_query_ids.clone(),
                subquery_status,
                row_region: pending.read_region.row_region.clone(),
              });

              // We do this after modifying `es.state` to avoid invalidating the `es` reference.
              for gr_query_es in gr_query_statuses {
                let query_id = gr_query_es.query_id.clone();
                statuses.insert(query_id, TabletStatus::GRQueryES(gr_query_es));
              }

              // Drive GRQueries
              for query_id in gr_query_ids {
                self.advance_gr_query(statuses, query_id);
              }
            }
            MSWriteExecutionS::Executing(executing) => {
              let (subquery_id, gr_query_es) = match self.recompute_subquery(
                executing,
                &protect_query_id,
                &es.query_id,
                &es.root_query_path,
                &es.tier_map,
                &es.query.selection,
                &collect_update_subqueries(&es.query),
                &es.timestamp,
                &es.context,
                &es.query_plan,
              ) {
                Ok(gr_query_statuses) => gr_query_statuses,
                Err(eval_error) => {
                  self.handle_eval_error_table_es(statuses, &query_id, eval_error);
                  return;
                }
              };

              statuses.insert(subquery_id.clone(), TabletStatus::GRQueryES(gr_query_es));
              self.advance_gr_query(statuses, subquery_id);
            }
          }
        }
      }
    }
  }

  fn advance_gr_query(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    let tablet_status = statuses.get_mut(&query_id).unwrap();
    let es = cast!(TabletStatus::GRQueryES, tablet_status).unwrap();

    // Compute the next stage
    let next_stage_idx = match &es.state {
      GRExecutionS::Start => 0,
      GRExecutionS::ReadStage(read_stage) => read_stage.stage_idx + 1,
      _ => panic!(),
    };

    if next_stage_idx < es.query_plan.col_usage_nodes.len() {
      // This means that we have still have stages to evaluate, so we move on.
      self.process_gr_query_stage(statuses, query_id, next_stage_idx);
    } else {
      // This means the GRQueryES is done, so we send the desired result
      // back to the originator.
      let return_trans_table_pos = lookup_pos(&es.trans_table_view, &es.query.returning).unwrap();
      let (_, (schema, table_views)) = es.trans_table_view.get(return_trans_table_pos).unwrap();

      // To compute the result, recall that we need to associate the Context to each TableView.
      let mut result = Vec::<TableView>::new();
      for extended_trans_tables_row in &es.new_trans_table_context {
        let idx = extended_trans_tables_row.get(return_trans_table_pos).unwrap();
        result.push(table_views.get(*idx).unwrap().clone());
      }

      // Finally, we Exit and Clean Up the GRQueryES and send the results back to the
      // originator.
      let orig_p = es.orig_p.clone();
      let schema = schema.clone();
      let new_rms = es.new_rms.clone();
      self.exit_and_clean_up(statuses, query_id.clone());
      self.handle_gr_query_done(statuses, orig_p, query_id, new_rms, (schema, result));
    }
  }

  /// This function moves the GRQueryES to the Stage indicated by `stage_idx`.
  /// This index must be valid (an actual stage).
  fn process_gr_query_stage(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    stage_idx: usize,
  ) {
    let tablet_status = statuses.get_mut(&query_id).unwrap();
    let es = cast!(TabletStatus::GRQueryES, tablet_status).unwrap();
    assert!(stage_idx < es.query_plan.col_usage_nodes.len());
    let (_, (_, child)) = es.query_plan.col_usage_nodes.get(stage_idx).unwrap();

    // Compute the `ColName`s and `TransTableNames` that we want in the child Query, and
    // process that.
    let context_cols = child.external_cols.clone();
    let context_trans_tables = node_external_trans_tables(child);
    self.process_gr_query_stage_simple(
      statuses,
      query_id,
      stage_idx,
      &context_cols,
      context_trans_tables,
    );
  }

  /// This is a common function used for sending processing a GRQueryES stage that
  /// doesn't use the child QueryPlan to process the child ContextSchema, but rather
  /// uses the `context_cols` and `context_trans_table`.
  fn process_gr_query_stage_simple(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    stage_idx: usize,
    context_cols: &Vec<ColName>,
    context_trans_tables: Vec<TransTableName>,
  ) {
    let tablet_status = statuses.get_mut(&query_id).unwrap();
    let es = cast!(TabletStatus::GRQueryES, tablet_status).unwrap();
    assert!(stage_idx < es.query_plan.col_usage_nodes.len());

    // We first compute the Context
    let mut context_schema = ContextSchema::default();

    // Point all `context_cols` columns to their index in main Query's ContextSchema.
    let mut context_col_index = HashMap::<ColName, usize>::new();
    let col_schema = &es.context.context_schema.column_context_schema;
    for (index, col) in col_schema.iter().enumerate() {
      if context_cols.contains(col) {
        context_col_index.insert(col.clone(), index);
      }
    }

    // Compute the ColumnContextSchema
    context_schema.column_context_schema.extend(context_cols.clone());

    // Split the `context_trans_tables` according to which of them are defined
    // in this GRQueryES, and which come from the outside.
    let mut local_trans_table_split = Vec::<TransTableName>::new();
    let mut external_trans_table_split = Vec::<TransTableName>::new();
    let completed_local_trans_tables: HashSet<TransTableName> =
      es.trans_table_view.iter().map(|(name, _)| name).cloned().collect();
    for trans_table_name in context_trans_tables {
      if completed_local_trans_tables.contains(&trans_table_name) {
        local_trans_table_split.push(trans_table_name);
      } else {
        assert!(es.query_plan.trans_table_schemas.contains_key(&trans_table_name));
        external_trans_table_split.push(trans_table_name);
      }
    }

    // Point all `external_trans_table_split` to their index in main Query's ContextSchema.
    let trans_table_context_schema = &es.context.context_schema.trans_table_context_schema;
    let mut context_external_trans_table_index = HashMap::<TransTableName, usize>::new();
    for (index, prefix) in trans_table_context_schema.iter().enumerate() {
      if external_trans_table_split.contains(&prefix.trans_table_name) {
        context_external_trans_table_index.insert(prefix.trans_table_name.clone(), index);
      }
    }

    // Compute the TransTableContextSchema
    for trans_table_name in &local_trans_table_split {
      context_schema.trans_table_context_schema.push(TransTableLocationPrefix {
        source: NodeGroupId::Tablet(self.this_tablet_group_id.clone()),
        query_id: query_id.clone(),
        trans_table_name: trans_table_name.clone(),
      });
    }
    for trans_table_name in &external_trans_table_split {
      let index = context_external_trans_table_index.get(trans_table_name).unwrap();
      let trans_table_prefix = trans_table_context_schema.get(*index).unwrap().clone();
      context_schema.trans_table_context_schema.push(trans_table_prefix);
    }

    // Construct the `ContextRow`s. To do this, we iterate over main Query's
    // `ContextRow`s and then the corresponding `ContextRow`s for the subquery.
    // We hold the child `ContextRow`s in Vec, and we use a HashSet to avoid duplicates.
    let mut new_context_rows = Vec::<ContextRow>::new();
    let mut new_row_map = HashMap::<ContextRow, usize>::new();
    // We also map the indices of the GRQueryES Context to that of the SubqueryStatus.
    let mut parent_context_map = Vec::<usize>::new();
    for (row_idx, context_row) in es.context.context_rows.iter().enumerate() {
      let mut new_context_row = ContextRow::default();

      // Compute the `ColumnContextRow`
      for col in context_cols {
        let index = context_col_index.get(col).unwrap();
        let col_val = context_row.column_context_row.get(*index).unwrap().clone();
        new_context_row.column_context_row.push(col_val);
      }

      // Compute the `TransTableContextRow`
      for local_trans_table_idx in 0..local_trans_table_split.len() {
        let trans_table_context_row = es.new_trans_table_context.get(row_idx).unwrap();
        let row_elem = trans_table_context_row.get(local_trans_table_idx).unwrap();
        new_context_row.trans_table_context_row.push(*row_elem as u32);
      }
      for trans_table_name in &external_trans_table_split {
        let index = context_external_trans_table_index.get(trans_table_name).unwrap();
        let trans_index = context_row.trans_table_context_row.get(*index).unwrap();
        new_context_row.trans_table_context_row.push(*trans_index);
      }

      if !new_row_map.contains_key(&new_context_row) {
        new_row_map.insert(new_context_row.clone(), new_context_rows.len());
        new_context_rows.push(new_context_row.clone());
      }

      parent_context_map.push(new_row_map.get(&new_context_row).unwrap().clone());
    }

    // Finally, compute the context.
    let context = Rc::new(Context { context_schema, context_rows: new_context_rows });

    // Next, we compute the QueryPlan we should send out.

    // Compute the TransTable schemas. We use the true TransTables for all prior
    // locally defined TransTables.
    let trans_table_schemas = &es.query_plan.trans_table_schemas;
    let mut child_trans_table_schemas = HashMap::<TransTableName, Vec<ColName>>::new();
    for trans_table_name in &local_trans_table_split {
      let (_, (schema, _)) =
        es.trans_table_view.iter().find(|(name, _)| trans_table_name == name).unwrap();
      child_trans_table_schemas.insert(trans_table_name.clone(), schema.clone());
    }
    for trans_table_name in &external_trans_table_split {
      let schema = trans_table_schemas.get(trans_table_name).unwrap();
      child_trans_table_schemas.insert(trans_table_name.clone(), schema.clone());
    }

    let (_, (_, child)) = es.query_plan.col_usage_nodes.get(stage_idx).unwrap();
    let query_plan = QueryPlan {
      gossip_gen: es.query_plan.gossip_gen.clone(),
      trans_table_schemas: child_trans_table_schemas,
      col_usage_node: child.clone(),
    };

    // Construct the TMStatus
    let tm_query_id = mk_qid(&mut self.rand);
    let mut tm_status = TMStatus {
      node_group_ids: Default::default(),
      new_rms: Default::default(),
      responded_count: 0,
      tm_state: Default::default(),
      orig_p: OrigP { status_path: es.query_id.clone() },
    };

    let sender_path = QueryPath {
      slave_group_id: self.this_slave_group_id.clone(),
      maybe_tablet_group_id: Some(self.this_tablet_group_id.clone()),
      query_id: tm_query_id.clone(),
    };

    // Send out the PerformQuery and populate TMStatus accordingly.
    let (_, stage) = es.query.trans_tables.get(stage_idx).unwrap();
    let child_sql_query = cast!(proc::GRQueryStage::SuperSimpleSelect, stage).unwrap();
    match &child_sql_query.from {
      TableRef::TablePath(table_path) => {
        // Here, we must do a SuperSimpleTableSelectQuery.
        let child_query = msg::SuperSimpleTableSelectQuery {
          timestamp: es.timestamp.clone(),
          context: context.deref().clone(),
          sql_query: child_sql_query.clone(),
          query_plan,
        };
        let general_query = msg::GeneralQuery::SuperSimpleTableSelectQuery(child_query);

        // Compute the TabletGroups involved.
        for tablet_group_id in get_min_tablets(
          &self.sharding_config,
          &self.gossip,
          table_path,
          &child_sql_query.selection,
        ) {
          let child_query_id = mk_qid(&mut self.rand);
          let sid = self.tablet_address_config.get(&tablet_group_id).unwrap();
          let eid = self.slave_address_config.get(&sid).unwrap();

          // Send out PerformQuery.
          self.network_output.send(
            eid,
            msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
              tablet_group_id.clone(),
              msg::TabletMessage::PerformQuery(msg::PerformQuery {
                root_query_path: es.root_query_path.clone(),
                sender_path: sender_path.clone(),
                query_id: child_query_id.clone(),
                tier_map: es.tier_map.clone(),
                query: general_query.clone(),
              }),
            )),
          );

          // Add the TabletGroup into the TMStatus.
          let node_group_id = NodeGroupId::Tablet(tablet_group_id);
          tm_status.node_group_ids.insert(node_group_id, child_query_id.clone());
          tm_status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
        }
      }
      TableRef::TransTableName(trans_table_name) => {
        // Here, we must do a SuperSimpleTransTableSelectQuery. Recall there is only one RM.
        let location_prefix = context
          .context_schema
          .trans_table_context_schema
          .iter()
          .find(|prefix| &prefix.trans_table_name == trans_table_name)
          .unwrap()
          .clone();
        let child_query = msg::SuperSimpleTransTableSelectQuery {
          location_prefix: location_prefix.clone(),
          context: context.deref().clone(),
          sql_query: child_sql_query.clone(),
          query_plan,
        };

        // Construct PerformQuery
        let general_query = msg::GeneralQuery::SuperSimpleTransTableSelectQuery(child_query);
        let child_query_id = mk_qid(&mut self.rand);
        let perform_query = msg::PerformQuery {
          root_query_path: es.root_query_path.clone(),
          sender_path: sender_path.clone(),
          query_id: child_query_id.clone(),
          tier_map: es.tier_map.clone(),
          query: general_query.clone(),
        };

        // Send out PerformQuery, wrapping it according to if the
        // TransTable is on a Slave or a Tablet
        let node_group_id = location_prefix.source.clone();
        self.send_to_node(node_group_id.clone(), CommonQuery::PerformQuery(perform_query));

        // Add the TabletGroup into the TMStatus.
        tm_status.node_group_ids.insert(node_group_id, child_query_id.clone());
        tm_status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
      }
    };

    // Create a SubqueryStatus and move the GRQueryES to the next Stage.
    es.state = GRExecutionS::ReadStage(ReadStage {
      stage_idx,
      parent_context_map,
      subquery_id: tm_query_id.clone(),
      single_subquery_status: SingleSubqueryStatus::Pending(SubqueryPending {
        context: context.clone(),
      }),
    });

    // Add the TMStatus into read_statuses
    statuses.insert(tm_query_id, TabletStatus::TMStatus(tm_status));
  }

  fn handle_query_success(&mut self, statuses: &mut Statuses, query_success: msg::QuerySuccess) {
    let tm_query_id = &query_success.query_id;
    if let Some(tablet_status) = statuses.get_mut(tm_query_id) {
      // Since the `tablet_status` must be a TMStatus, we cast it.
      let tm_status = cast!(TabletStatus::TMStatus, tablet_status).unwrap();
      // We just add the result of the `query_success` here.
      let tm_wait_value = tm_status.tm_state.get_mut(&query_success.query_id).unwrap();
      *tm_wait_value = TMWaitValue::Result(query_success.result.clone());
      tm_status.new_rms.extend(query_success.new_rms);
      tm_status.responded_count += 1;
      if tm_status.responded_count == tm_status.tm_state.len() {
        // Remove the `TMStatus` and take ownership
        let tablet_status = statuses.remove(&query_success.query_id).unwrap();
        let tm_status = cast!(TabletStatus::TMStatus, tablet_status).unwrap();
        // Merge there TableViews together
        let mut results = Vec::<(Vec<ColName>, Vec<TableView>)>::new();
        for (_, tm_wait_value) in tm_status.tm_state {
          results.push(cast!(TMWaitValue::Result, tm_wait_value).unwrap());
        }
        let merged_result = merge_table_views(results);
        let gr_query_id = tm_status.orig_p.status_path;
        self.handle_tm_done(statuses, gr_query_id, tm_query_id.clone(), merged_result);
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
    (_, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    let query_id = orig_p.status_path;
    if let Some(tablet_status) = statuses.get_mut(&query_id) {
      match tablet_status {
        TabletStatus::GRQueryES(_) => panic!(),
        TabletStatus::FullTableReadES(read_es) => {
          let es = cast!(FullTableReadES::Executing, read_es).unwrap();

          // Add the subquery results into the TableReadES.
          es.new_rms.extend(subquery_new_rms);
          let executing_state = cast!(ExecutionS::Executing, &mut es.state).unwrap();
          let subquery_status = &mut executing_state.subquery_status;
          let single_status = subquery_status.subqueries.get_mut(&subquery_id).unwrap();
          let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
          *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
            context: context.clone(),
            result: table_views,
          });
          executing_state.completed += 1;

          // If all subqueries have been evaluated, finish the TableReadES
          // and respond to the client.
          if executing_state.completed == subquery_status.subqueries.len() {
            /*
             We are trying to compute the final Vec<TableView> that we send off to the
             request sender. We iterator ContextRow by ContextRow. For each, we read
             every relevent key. Recall that keybounds are computed like:

             match compute_key_region(
                &es.query.selection,
                &self.table_schema.key_cols,
                col_context)

            Which only uses the main ContextRow. Then, we iterate every key in this Key
            Bounds. For each, we use that + main Context Row to compute the Context Row
            for every child query. We hold HashMap<ContextRow, usize> for each subquery,
            ultimately allowing us to lookup SingleSubqueryContext's Vec<TableView>.
            (Technically, we don't even need the ContextRow in Finished anymore.) Thus,
            for every main ContextRow + in-bound TableRow, we can replace appearances of
            ValExpr::Subquery with actual values. (Note we also throw runtime errors if
            the subquery TableViews aren't single values.)

            Note that the child ContextRows we compute here will generally have different
            column ordering the ones computed originally. Fortunately, this doesn't matter,
            since we only rely on the distinctness of a child ContextRow from the prior
            ones in order to update the HashMap<ContextRow, usize>.
            */

            let num_subqueries = executing_state.subquery_pos.len();

            // Construct the ContextConverters for all subqueries
            let mut converters = Vec::<ContextConverter>::new();
            for subquery_id in &executing_state.subquery_pos {
              let single_status = subquery_status.subqueries.get(subquery_id).unwrap();
              let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
              let context_schema = &result.context.context_schema;
              converters.push(ContextConverter::general_create(
                &es.context.context_schema,
                context_schema.column_context_schema.clone(),
                context_schema.trans_table_names(),
                &es.timestamp,
                &self.table_schema,
              ));
            }

            // Setup the child_context_row_maps that will be populated over time.
            let mut child_context_row_maps = Vec::<HashMap<ContextRow, usize>>::new();
            for _ in 0..num_subqueries {
              child_context_row_maps.push(HashMap::new());
            }

            // Compute the Schema that should belong to every TableView that's returned
            // from this TableReadES.
            let mut res_col_names = Vec::<(ColName, ColType)>::new();
            for col_name in &es.query.projection.clone() {
              let col_type = if let Some(pos) = lookup_pos(&self.table_schema.key_cols, col_name) {
                let (_, col_type) = self.table_schema.key_cols.get(pos).unwrap();
                col_type.clone()
              } else {
                self.table_schema.val_cols.strong_static_read(col_name, es.timestamp).unwrap()
              };
              res_col_names.push((col_name.clone(), col_type));
            }

            // Compute the set of columns to read from the table. This should should include all
            // top-level ColumnRefs that are in expressions but not subqueries (these are included
            // in `query_plan.col_usage_node.safe_present_cols`), the project columns, and all
            // Internal columns used in subqueries (which are best derived from the subquery
            // Contexts, since they are generally a superset of the Query Plan).
            let mut cols_to_read_set = HashSet::<ColName>::new();
            cols_to_read_set.extend(es.query_plan.col_usage_node.safe_present_cols.clone());
            cols_to_read_set.extend(es.query.projection.clone());
            for conv in &converters {
              cols_to_read_set.extend(conv.safe_present_split.clone());
            }
            let cols_to_read = Vec::from_iter(cols_to_read_set.iter().cloned());

            // Setup ability to compute a tight Keybound for every ContextRow. This will be
            // the exact same as that computed before sending out the subqueries.
            let keybound_computer = ContextKeyboundComputer::new(
              &es.query.selection,
              &self.table_schema,
              &es.timestamp,
              &es.context.context_schema,
            );

            let mut res_table_views = Vec::<TableView>::new();
            for context_row in &es.context.context_rows {
              // Since we recomputed this exact `key_bounds` before going to the Pending
              // state, we can unwrap it; there should be no errors.
              let key_bounds = keybound_computer.compute_keybounds(context_row).unwrap();

              // We iterate over the rows of the Subtable computed for this ContextRow,
              // computing the TableView we desire for the ContextRow.
              let (subtable_schema, subtable) =
                compute_subtable(&cols_to_read, &key_bounds, &self.storage);

              // Next, we initialize the TableView that we are trying to construct
              // for this `context_row`.
              let mut res_table_view =
                TableView { col_names: res_col_names.clone(), rows: Default::default() };

              for subtable_row in subtable {
                // Now, we compute the subquery result for all subqueries for this
                // `context_row` + `subtable_row`.
                let mut subquery_vals = Vec::<TableView>::new();
                for index in 0..num_subqueries {
                  // Compute the child ContextRow for this subquery, and populate
                  // `child_context_row_map` accordingly.
                  let conv = converters.get(index).unwrap();
                  let child_context_row_map = child_context_row_maps.get_mut(index).unwrap();
                  let row = conv.extract_child_relevent_cols(&subtable_schema, &subtable_row);
                  let new_context_row = conv.compute_child_context_row(context_row, row);
                  if !child_context_row_map.contains_key(&new_context_row) {
                    let idx = child_context_row_map.len();
                    child_context_row_map.insert(new_context_row.clone(), idx);
                  }

                  // Get the child_context_idx to get the relevent TableView from the subquery
                  // results, and populate `subquery_vals`.
                  let child_context_idx = child_context_row_map.get(&new_context_row).unwrap();
                  let subquery_id = executing_state.subquery_pos.get(index).unwrap();
                  let single_status = subquery_status.subqueries.get(subquery_id).unwrap();
                  let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
                  subquery_vals.push(result.result.get(*child_context_idx).unwrap().clone());
                }

                /// Now, we evaluate all expressions in the SQL query and amend the
                /// result to this TableView (if the WHERE clause evaluates to true).
                match is_true(&evaluate_expr(
                  &es.query.selection,
                  &es.context.context_schema,
                  &context_row,
                  &subtable_schema,
                  &subtable_row,
                  &subquery_vals,
                )) {
                  Ok(bool_val) => {
                    if bool_val {
                      // This means that the current row should be selected for the result.
                      // First, we take the projected columns.
                      let mut res_row = Vec::<ColValN>::new();
                      for (res_col_name, _) in &res_col_names {
                        let idx = subtable_schema.iter().position(|k| res_col_name == k).unwrap();
                        res_row.push(subtable_row.get(idx).unwrap().clone());
                      }

                      // Then, we add the `res_row` into the TableView.
                      if let Some(count) = res_table_view.rows.get_mut(&res_row) {
                        count.add(1);
                      } else {
                        res_table_view.rows.insert(res_row, 1);
                      }
                    }
                  }
                  Err(eval_error) => {
                    self.handle_eval_error_table_es(statuses, &query_id, eval_error);
                    return;
                  }
                }
              }

              // Finally, accumulate the resulting TableView.
              res_table_views.push(res_table_view);
            }

            // Build the success message and respond.
            let success_msg = msg::QuerySuccess {
              return_path: es.sender_path.query_id.clone(),
              query_id: query_id.clone(),
              result: (es.query.projection.clone(), res_table_views),
              new_rms: es.new_rms.iter().cloned().collect(),
            };
            let sender_path = es.sender_path.clone();
            self.send_to_path(sender_path, CommonQuery::QuerySuccess(success_msg));

            // Remove the TableReadES.
            statuses.remove(&query_id);
          }
        }
        TabletStatus::TMStatus(_) => panic!(),
        TabletStatus::MSQueryES(_) => panic!(),
        TabletStatus::FullMSTableReadES(_) => panic!(),
        TabletStatus::FullMSTableWriteES(ms_write_es) => {
          // TODO: Recall that Updates can only update ValCols. The MSCoordES should verify this
          // as an optimization, but even if it doesn't, the Tablet should detect it at the latest
          // possible moment (like how we handle Type Errors), which is when we compute the
          // GenericTable, or even later (if we interpret the key ColName as a val column with the
          // same name), when we try applying it to `storage` (where we will find the GenericTable
          // has a non-conforming schema).
        }
      }
    }
  }

  fn handle_tm_done(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    tm_query_id: QueryId,
    (schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    let tablet_status = statuses.get_mut(&query_id).unwrap();
    let es = cast!(TabletStatus::GRQueryES, tablet_status).unwrap();
    let read_stage = cast!(GRExecutionS::ReadStage, &mut es.state).unwrap();
    let child_query_id = &read_stage.subquery_id;
    let single_status = &read_stage.single_subquery_status;

    // Extract the `context` from the SingleSubqueryStatus and verify
    // that it corresponds to `table_views`.
    let subquery_context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context;
    assert_eq!(child_query_id, &tm_query_id);
    assert_eq!(table_views.len(), subquery_context.context_rows.len());

    // For now, just assert assert that the schema that we get corresponds
    // to that in the QueryPlan.
    let (trans_table_name, (cur_schema, _)) =
      es.query_plan.col_usage_nodes.get(read_stage.stage_idx).unwrap();
    assert_eq!(&schema, cur_schema);

    // Amend the `new_trans_table_context`
    for i in 0..es.context.context_rows.len() {
      let idx = read_stage.parent_context_map.get(i).unwrap();
      es.new_trans_table_context.get_mut(i).unwrap().push(*idx);
    }

    // Add the `table_views` to the GRQueryES and advance it.
    es.trans_table_view.push((trans_table_name.clone(), (schema, table_views)));
    self.advance_gr_query(statuses, query_id);
  }

  fn handle_query_aborted(&mut self, statuses: &mut Statuses, query_aborted: msg::QueryAborted) {
    if let Some(tablet_status) = statuses.remove(&query_aborted.return_path) {
      let tm_status = cast!(TabletStatus::TMStatus, tablet_status).unwrap();
      // We Exit and Clean up this TMStatus (sending CancelQuery to all
      // remaining participants) and send the QueryAborted back to the orig_path
      for (node_group_id, child_query_id) in tm_status.node_group_ids {
        if tm_status.tm_state.get(&child_query_id).unwrap() == &TMWaitValue::Nothing
          && child_query_id != query_aborted.query_id
        {
          // If the child Query hasn't responded yet, and isn't also the Query that
          // just aborted, then we send it a CancelQuery
          self.send_to_node(
            node_group_id,
            CommonQuery::CancelQuery(msg::CancelQuery { query_id: child_query_id }),
          );
        }
      }

      // Finally, we propagate up the AbortData to the GRQueryES that owns this TMStatus
      let gr_query_id = tm_status.orig_p.status_path;
      self.handle_abort_gr_query_es(statuses, gr_query_id, query_aborted.payload);
    }
  }

  /// Propagate up an abort from a TMStatus up the owning GRQueryES.
  /// Note that the GRQueryES must exist.
  fn handle_abort_gr_query_es(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    aborted_data: AbortedData,
  ) {
    let tablet_status = statuses.get_mut(&query_id).unwrap();
    let es = cast!(TabletStatus::GRQueryES, tablet_status).unwrap();
    let read_stage = cast!(GRExecutionS::ReadStage, &es.state).unwrap();
    let pending = cast!(SingleSubqueryStatus::Pending, &read_stage.single_subquery_status).unwrap();
    match aborted_data {
      AbortedData::ColumnsDNE { missing_cols } => {
        // First, assert that the missing columns are indeed not already a part of the Context.
        for col in &missing_cols {
          assert!(!pending.context.context_schema.column_context_schema.contains(col));
        }

        // Next, we compute the subset of `missing_cols` that are not in the Context here.
        let mut rem_cols = Vec::<ColName>::new();
        for col in &missing_cols {
          if !es.context.context_schema.column_context_schema.contains(col) {
            rem_cols.push(col.clone());
          }
        }

        if !rem_cols.is_empty() {
          // If the Context has the missing columns, then we Exit and Clean Up this
          // GRQueryES and propagate the error upward.
          let orig_p = es.orig_p.clone();
          self.exit_and_clean_up(statuses, query_id.clone());
          self.handle_internal_columns_dne(statuses, orig_p, query_id, rem_cols);
        } else {
          // If the GRQueryES Context is sufficient, we simply amend the new columns
          // to the Context and reprocess the ReadStage.
          let context_schema = &pending.context.context_schema;
          let mut context_cols = context_schema.column_context_schema.clone();
          context_cols.extend(missing_cols);
          let context_trans_tables = context_schema.trans_table_names();
          let stage_idx = read_stage.stage_idx.clone();
          self.process_gr_query_stage_simple(
            statuses,
            query_id,
            stage_idx,
            &context_cols,
            context_trans_tables,
          );
        }
      }
      AbortedData::QueryError(query_error) => {
        // Here, we Exit and Clean Up the GRQueryES, and we propagate up the QueryError
        // Note that the only Subquery has already just been cleaned up.
        let orig_p = es.orig_p.clone();
        self.exit_and_clean_up(statuses, query_id.clone());
        self.propagate_query_error(statuses, orig_p, query_error);
      }
    }
  }

  /// This routes the QueryError to the appropriate top-level ES.
  fn propagate_query_error(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    query_error: msg::QueryError,
  ) {
    let query_id = orig_p.status_path;
    if let Some(tablet_status) = statuses.get_mut(&query_id) {
      match tablet_status {
        TabletStatus::GRQueryES(_) => panic!(),
        TabletStatus::FullTableReadES(read_es) => {
          let es = cast!(FullTableReadES::Executing, read_es).unwrap();

          // Build the error message and respond.
          let abort_query = msg::QueryAborted {
            return_path: es.sender_path.query_id.clone(),
            query_id: query_id.clone(),
            payload: msg::AbortedData::QueryError(query_error),
          };
          let sender_path = es.sender_path.clone();
          self.send_to_path(sender_path, CommonQuery::QueryAborted(abort_query));

          // Finally, Exit and Clean Up this TableReadES.
          self.exit_and_clean_up(statuses, query_id);
        }
        TabletStatus::TMStatus(_) => {}
        TabletStatus::MSQueryES(_) => {}
        TabletStatus::FullMSTableReadES(_) => {}
        TabletStatus::FullMSTableWriteES(_) => {}
      }
    }
  }

  /// This function is used to cancel an ES both for the case of CancelQuery, but also
  /// to generally Exit and Clean Up an ES from whatever state it's in. Thus, we don't
  /// require the ES to be in their valid state; an ES might reference another ES
  /// that no longer exists (because the calling function did some pre-cleanup).
  fn exit_and_clean_up(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    if let Some(tablet_status) = statuses.remove(&query_id) {
      match tablet_status {
        TabletStatus::GRQueryES(es) => {
          match es.state {
            GRExecutionS::Start => {}
            GRExecutionS::ReadStage(read_stage) => match read_stage.single_subquery_status {
              SingleSubqueryStatus::LockingSchemas(_) => panic!(),
              SingleSubqueryStatus::PendingReadRegion(_) => panic!(),
              SingleSubqueryStatus::Pending(_) => {
                self.exit_and_clean_up(statuses, read_stage.subquery_id);
              }
              SingleSubqueryStatus::Finished(_) => {}
            },
            GRExecutionS::MasterQueryReplanning(planning) => {
              // Remove if present
              if self.master_query_map.remove(&planning.master_query_id).is_some() {
                // If the removal was successful, we should also send a Cancellation
                // message to the Master.
                self.network_output.send(
                  &self.master_eid,
                  msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
                    msg::CancelMasterFrozenColUsage { query_id: planning.master_query_id },
                  )),
                );
              }
            }
          }
        }
        TabletStatus::FullTableReadES(read_es) => match read_es {
          FullTableReadES::QueryReplanning(es) => match es.status.state {
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
          },
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
              for (query_id, single_query) in executing.subquery_status.subqueries {
                match single_query {
                  SingleSubqueryStatus::LockingSchemas(locking_status) => {
                    self.remove_col_locking_request(locking_status.query_id);
                  }
                  SingleSubqueryStatus::PendingReadRegion(protect_status) => {
                    let protect_query_id = protect_status.query_id;
                    self.remove_read_protected_request(es.timestamp.clone(), protect_query_id);
                  }
                  SingleSubqueryStatus::Pending(_) => {
                    self.exit_and_clean_up(statuses, query_id);
                  }
                  SingleSubqueryStatus::Finished(_) => {}
                }
              }
            }
          },
        },
        TabletStatus::TMStatus(tm_status) => {
          // We Exit and Clean up this TMStatus (sending CancelQuery to all remaining participants)
          for (node_group_id, child_query_id) in tm_status.node_group_ids {
            if tm_status.tm_state.get(&child_query_id).unwrap() == &TMWaitValue::Nothing {
              // If the child Query hasn't responded, then sent it a CancelQuery
              self.send_to_node(
                node_group_id,
                CommonQuery::CancelQuery(msg::CancelQuery { query_id: child_query_id }),
              );
            }
          }
        }
        TabletStatus::MSQueryES(_) => panic!(),
        TabletStatus::FullMSTableReadES(_) => panic!(),
        TabletStatus::FullMSTableWriteES(_) => {
          // TODO: do this
        }
      }
    }
  }

  /// We call this when a DeadlockSafetyReadAbort happens
  fn deadlock_safety_read_abort(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    read_region: TableRegion,
  ) {
  }

  /// We call this when a DeadlockSafetyWriteAbort happens
  fn deadlock_safety_write_abort(&mut self, statuses: &mut Statuses, orig_p: OrigP) {}
}

/// This evaluates a `ValExpr` completely into a `ColVal`. When a ColumnRef is encountered
/// in the `expr`, we first search `subtable_row`, and if that's not present, we search the
/// `parent_context_row`.
fn evaluate_expr(
  expr: &proc::ValExpr,
  parent_context_schema: &ContextSchema,
  parent_context_row: &ContextRow,
  subtable_schema: &Vec<ColName>,
  subtable_row: &Vec<ColValN>,
  subquery_vals: &Vec<TableView>,
) -> ColValN {
  let mut subtable_row_map = HashMap::<ColName, ColValN>::new();
  for i in 0..subtable_schema.len() {
    subtable_row_map
      .insert(subtable_schema.get(i).unwrap().clone(), subtable_row.get(i).unwrap().clone());
  }
  unimplemented!()
}

/// This function simply deduces if the given `ColValN` sould be interpreted as true
/// during query evaluation (e.g. when used in the WHERE clause). An error is returned
/// if `val` isn't a Bool type.
fn is_true(val: &ColValN) -> Result<bool, EvalError> {
  match val {
    Some(ColVal::Bool(bool_val)) => Ok(bool_val.clone()),
    _ => Ok(false),
  }
}

/// This is used to compute the Keybound of a selection expression for
/// a given ContextRow containing external columns. Recall that the
/// selection expression might have ColumnRefs that aren't part of
/// the TableSchema (i.e. part of the external Context), and we can use
/// that to compute a tigher Keybound.
struct ContextKeyboundComputer {
  context_col_index: HashMap<ColName, usize>,
  selection: proc::ValExpr,
  key_cols: Vec<(ColName, ColType)>,
}

impl ContextKeyboundComputer {
  /// Here, `selection` is the the filtering expression. `parent_context_schema` is the set
  /// of External columns whose values we can fill in (during `compute_keybounds` calls)
  /// to help reduce the KeyBound. However, we need `table_schema` and the `timestamp` of
  /// the Query to determine which of these External columns are shadowed so we can avoid
  /// using them.
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

  /// Compute the tightest keybound for the given `parent_context_row` (whose schema
  /// must correspond to `parent_context_schema` passed in teh constructor.
  fn compute_keybounds(&self, parent_context_row: &ContextRow) -> Result<Vec<KeyBound>, EvalError> {
    // First, map all External Columns names to the corresponding values
    // in this ContextRow
    let mut col_context = HashMap::<ColName, ColValN>::new();
    for (col, index) in &self.context_col_index {
      let col_val = parent_context_row.column_context_row.get(*index).unwrap().clone();
      col_context.insert(col.clone(), col_val);
    }

    // Then, compute the keybound
    compute_key_region(&self.selection, &self.key_cols, col_context)
  }
}

/// This container computes and remembers how to construct a child ContextRow
/// from a parent ContexRow + Table row. We have to initially pass in the
/// the parent's ContextRowSchema, the parent's ColUsageNode, as well as `i`,
/// which points to the Subquery we want to compute child ContextRows for.
///
/// Importantly, order within `safe_present_split`, `external_split`, and
/// `trans_table_split` aren't guaranteed.
#[derive(Default)]
struct ContextConverter {
  // This is the child query's ContextSchema, and it's computed in the constructor.
  context_schema: ContextSchema,

  // These fields are the constituents of the `context_schema` above.
  safe_present_split: Vec<ColName>,
  external_split: Vec<ColName>,
  trans_table_split: Vec<TransTableName>,

  /// This maps the `ColName`s in `external_split` to their positions in the
  /// parent ColumnContextSchema.
  context_col_index: HashMap<ColName, usize>,
  /// This maps the `TransTableName`s in `trans_table_split` to their positions in the
  /// parent TransTableContextSchema.
  context_trans_table_index: HashMap<TransTableName, usize>,
}

impl ContextConverter {
  /// Compute all of the data members from a QueryPlan.
  fn create_from_query_plan(
    parent_context_schema: &ContextSchema,
    parent_node: &FrozenColUsageNode,
    subquery_index: usize,
  ) -> ContextConverter {
    let child = parent_node.children.get(subquery_index).unwrap();

    let mut safe_present_split = Vec::<ColName>::new();
    let mut external_split = Vec::<ColName>::new();
    let trans_table_split = nodes_external_trans_tables(child);

    // Construct the ContextSchema of the GRQueryES.
    let mut subquery_external_cols = HashSet::<ColName>::new();
    for (_, (_, node)) in child {
      subquery_external_cols.extend(node.external_cols.clone());
    }

    // Split the `subquery_external_cols` by which of those cols are in this Table,
    // and which aren't.
    for col in &subquery_external_cols {
      if parent_node.safe_present_cols.contains(col) {
        safe_present_split.push(col.clone());
      } else {
        external_split.push(col.clone());
      }
    }

    ContextConverter::finish_creation(
      parent_context_schema,
      safe_present_split,
      external_split,
      trans_table_split,
    )
  }

  /// Here, all ColNames in `child_columns` must be locked in the `table_schema`.
  /// In addition they must either appear present in the `table_schema`, or in the
  /// `parent_context_schema`. Finally, the `child_trans_table_names` must be contained
  /// in the `parent_context_schema` as well.
  fn general_create(
    parent_context_schema: &ContextSchema,
    child_columns: Vec<ColName>,
    child_trans_table_names: Vec<TransTableName>,
    timestamp: &Timestamp,
    table_schema: &TableSchema,
  ) -> ContextConverter {
    let mut safe_present_split = Vec::<ColName>::new();
    let mut external_split = Vec::<ColName>::new();

    // Whichever `ColName`s in `child_columns` that are also present in `table_schema` should
    // be added to `safe_present_cols`. Otherwise, they should be added to `external_split`.
    for col in child_columns {
      if contains_col(table_schema, &col, timestamp) {
        safe_present_split.push(col);
      } else {
        external_split.push(col);
      }
    }

    ContextConverter::finish_creation(
      parent_context_schema,
      safe_present_split,
      external_split,
      child_trans_table_names,
    )
  }

  /// Moves the `*_split` variables into a ContextConverter and compute the various
  /// convenience data.
  fn finish_creation(
    parent_context_schema: &ContextSchema,
    safe_present_split: Vec<ColName>,
    external_split: Vec<ColName>,
    trans_table_split: Vec<TransTableName>,
  ) -> ContextConverter {
    let mut conv = ContextConverter::default();
    conv.safe_present_split = safe_present_split;
    conv.external_split = external_split;
    conv.trans_table_split = trans_table_split;

    // Point all `conv.external_split` columns to their index in main Query's ContextSchema.
    let col_schema = &parent_context_schema.column_context_schema;
    for (index, col) in col_schema.iter().enumerate() {
      if conv.external_split.contains(col) {
        conv.context_col_index.insert(col.clone(), index);
      }
    }

    // Compute the ColumnContextSchema so that `safe_present_split` is first, and
    // `external_split` is second. This is relevent when we compute ContextRows.
    conv.context_schema.column_context_schema.extend(conv.safe_present_split.clone());
    conv.context_schema.column_context_schema.extend(conv.external_split.clone());

    // Point all `trans_table_split` to their index in main Query's ContextSchema.
    let trans_table_context_schema = &parent_context_schema.trans_table_context_schema;
    for (index, prefix) in trans_table_context_schema.iter().enumerate() {
      if conv.trans_table_split.contains(&prefix.trans_table_name) {
        conv.context_trans_table_index.insert(prefix.trans_table_name.clone(), index);
      }
    }

    // Compute the TransTableContextSchema
    for trans_table_name in &conv.trans_table_split {
      let index = conv.context_trans_table_index.get(trans_table_name).unwrap();
      let trans_table_prefix = trans_table_context_schema.get(*index).unwrap().clone();
      conv.context_schema.trans_table_context_schema.push(trans_table_prefix);
    }

    conv
  }

  /// Computes a child ContextRow. Note that the schema of `row` should be
  /// `self.safe_present_cols`. (Here, `parent_context_row` must have the schema
  /// of the `parent_context_schema` that was passed into the constructors).
  fn compute_child_context_row(
    &self,
    parent_context_row: &ContextRow,
    mut row: Vec<ColValN>,
  ) -> ContextRow {
    for col in &self.external_split {
      let index = self.context_col_index.get(col).unwrap();
      row.push(parent_context_row.column_context_row.get(*index).unwrap().clone());
    }
    let mut new_context_row = ContextRow::default();
    new_context_row.column_context_row = row;
    // Compute the `TransTableContextRow`
    for trans_table_name in &self.trans_table_split {
      let index = self.context_trans_table_index.get(trans_table_name).unwrap();
      let trans_index = parent_context_row.trans_table_context_row.get(*index).unwrap();
      new_context_row.trans_table_context_row.push(*trans_index);
    }
    new_context_row
  }

  /// Extracts the subset of `subtable_row` whose ColNames correspond to
  /// `self.safe_present_cols` and returns it in the order of `self.safe_present_cols`.
  fn extract_child_relevent_cols(
    &self,
    subtable_schema: &Vec<ColName>,
    subtable_row: &Vec<ColValN>,
  ) -> Vec<ColValN> {
    let mut row = Vec::<ColValN>::new();
    for col in &self.safe_present_split {
      let pos = subtable_schema.iter().position(|sub_col| col == sub_col).unwrap();
      row.push(subtable_row.get(pos).unwrap().clone());
    }
    row
  }

  /// Extracts the column values for the ColNames in `external_split`. Here,
  /// `parent_context_row` must have the schema of `parent_context_schema` that was passed
  /// into the construct, as usual.
  fn compute_col_context(&self, parent_context_row: &ContextRow) -> HashMap<ColName, ColValN> {
    // First, map all columns in `external_split` to their values in this ContextRow.
    let mut col_context = HashMap::<ColName, ColValN>::new();
    for (col, index) in &self.context_col_index {
      col_context
        .insert(col.clone(), parent_context_row.column_context_row.get(*index).unwrap().clone());
    }
    col_context
  }
}

/// Computes whether `col` is in `table_schema` at `timestamp`. Note that it must be
/// ensured that `col` is a locked column at this Timestamp.
fn contains_col(table_schema: &TableSchema, col: &ColName, timestamp: &Timestamp) -> bool {
  if lookup_pos(&table_schema.key_cols, col).is_some() {
    return true;
  }
  // If the `col` wasn't part of the PrimaryKey, then we need to
  // check the `val_cols` to check for presence
  if table_schema.val_cols.strong_static_read(col, *timestamp).is_some() {
    return true;
  }
  return false;
}

/// This function computes a minimum set of `TabletGroupId`s whose `TabletKeyRange`
/// has a non-empty intersect with the KeyRegion we compute from the given `selection`.
fn get_min_tablets(
  sharding_config: &HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  gossip: &GossipData,
  table_path: &TablePath,
  selection: &proc::ValExpr,
) -> Vec<TabletGroupId> {
  // Next, we try to reduce the number of TabletGroups we must contact by computing
  // the key_region of TablePath that we're going to be reading.
  let tablet_groups = sharding_config.get(table_path).unwrap();
  let key_cols = &gossip.gossiped_db_schema.get(table_path).unwrap().key_cols;
  match &compute_key_region(selection, key_cols, HashMap::new()) {
    Ok(_) => {
      // We use a trivial implementation for now, where we just return all TabletGroupIds
      tablet_groups.iter().map(|(_, tablet_group_id)| tablet_group_id.clone()).collect()
    }
    Err(_) => panic!(),
  }
}

/// Computes `KeyBound`s that have a corresponding shape to `key_cols`, such that
/// any key outside of this evalautes `expr` to false, given `col_context`.
/// TODO: is this some kind of joke? This doesn't do anything. `key_bounds` starts and stays empty.
fn compute_key_region(
  expr: &proc::ValExpr,
  key_cols: &Vec<(ColName, ColType)>,
  col_context: HashMap<ColName, ColValN>,
) -> Result<Vec<KeyBound>, EvalError> {
  let mut key_bounds = Vec::<KeyBound>::new();
  for (col_name, col_type) in key_cols {
    // Then, compute the ColBound and extend key_bounds.
    let col_bounds = compute_bound(col_name, col_type, expr, &col_context)?;
    let mut new_key_bounds = Vec::<KeyBound>::new();
    for key_bound in key_bounds {
      for col_bound in col_bounds.clone() {
        let mut new_key_bound = key_bound.clone();
        new_key_bound.key_col_bounds.push(col_bound.clone());
        new_key_bounds.push(new_key_bound);
      }
    }
    key_bounds = compress_row_region(new_key_bounds);
  }
  Ok(key_bounds)
}

/// This computes a sub-table from `storage` over the given `cols` and `key_bounds`.
/// This also returns the `ColNames` that correspond ot the subtable returned.
fn compute_subtable(
  cols: &Vec<ColName>,
  key_bounds: &Vec<KeyBound>,
  storage: &GenericMVTable,
) -> (Vec<ColName>, Vec<Vec<ColValN>>) {
  unimplemented!()
}

/// This function removes redundancy in the `row_region`. Redundancy may easily
/// arise from different ColumnContexts. In the future, we can be smarter and
/// sacrifice granularity for a simpler Key Region.
fn compress_row_region(row_region: Vec<KeyBound>) -> Vec<KeyBound> {
  row_region
}

/// Computes if `region` and intersects with any TableRegion in `regions`.
fn does_intersect(region: &TableRegion, regions: &BTreeSet<TableRegion>) -> bool {
  unimplemented!()
}

impl<R: QueryReplanningSqlView> CommonQueryReplanningES<R> {
  fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    matches!(self.state, CommonQueryReplanningS::Start);
    // Add column locking
    let locked_cols_qid = ctx.add_requested_locked_columns(
      self.orig_p.clone(),
      self.timestamp,
      self.sql_view.projected_cols(&ctx.table_schema),
    );
    self.state =
      CommonQueryReplanningS::ProjectedColumnLocking { locked_columns_query_id: locked_cols_qid };
  }

  fn column_locked<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>, query_id: QueryId) {
    match &self.state {
      CommonQueryReplanningS::ProjectedColumnLocking { .. } => {
        // We need to verify that the projected columns are present.
        for col in &self.sql_view.projected_cols(&ctx.table_schema) {
          if !contains_col(&ctx.table_schema, col, &self.timestamp) {
            // This means a projected column doesn't exist. Thus, we Exit and Clean Up.
            let sender_path = self.sender_path.clone();
            let aborted_msg = msg::QueryAborted {
              return_path: sender_path.query_id.clone(),
              query_id: query_id.clone(),
              payload: msg::AbortedData::QueryError(msg::QueryError::ProjectedColumnsDNE {
                msg: String::new(),
              }),
            };
            ctx.send_to_path(sender_path, CommonQuery::QueryAborted(aborted_msg));
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
          let locked_cols_qid =
            ctx.add_requested_locked_columns(self.orig_p.clone(), self.timestamp.clone(), all_cols);

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
            self.orig_p.clone(),
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

        let mut does_query_plan_align = (|| {
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
          assert!(self.query_plan.gossip_gen >= ctx.gossip.gossip_gen);
          let mut planner = ColUsagePlanner {
            gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
            timestamp: self.timestamp,
          };

          let all_cols = planner
            .get_all_cols(&mut self.query_plan.trans_table_schemas.clone(), &self.sql_view.exprs());

          // Add column locking
          let locked_cols_qid = ctx.add_requested_locked_columns(
            self.orig_p.clone(),
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
            self.orig_p.clone(),
            self.timestamp.clone(),
            all_cols.clone(),
          );
          // Repeat this state
          self.state = CommonQueryReplanningS::RecomputeQueryPlan {
            locked_columns_query_id: locked_cols_qid,
            locking_cols: all_cols,
          };
        } else {
          // Here, we can can finish the query plan and then move to Executing.

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
            &self.sql_view.projected_cols(&ctx.table_schema),
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
              let master_qid = mk_qid(&mut ctx.rand);

              ctx.network_output.send(
                &ctx.master_eid,
                msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
                  msg::PerformMasterFrozenColUsage {
                    query_id: master_qid.clone(),
                    timestamp: self.timestamp,
                    trans_table_schemas: self.query_plan.trans_table_schemas.clone(),
                    col_usage_tree: msg::ColUsageTree::MSQueryStage(self.sql_view.ms_query_stage()),
                  },
                )),
              );
              ctx.master_query_map.insert(master_qid.clone(), self.orig_p.clone());

              // Advance Read Status
              self.state =
                CommonQueryReplanningS::MasterQueryReplanning { master_query_id: master_qid };
              return;
            }
          }

          self.state = CommonQueryReplanningS::Done(true);
        }
      }
      _ => panic!(),
    }
  }
}
