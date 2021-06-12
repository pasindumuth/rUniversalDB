use crate::col_usage::{
  collect_select_subqueries, node_external_trans_tables, nodes_external_trans_tables,
  ColUsagePlanner, FrozenColUsageNode,
};
use crate::common::{
  lookup_pos, merge_table_views, mk_qid, GossipData, IOTypes, KeyBound, NetworkOut, OrigP,
  QueryPlan, TMStatus, TMWaitValue, TableRegion, TableSchema,
};
use crate::expression::{compute_bound, EvalError};
use crate::model::common::proc::{TableRef, ValExpr};
use crate::model::common::{
  iast, proc, ColType, ColValN, Context, ContextRow, ContextSchema, Gen, NodeGroupId, TableView,
  TierMap, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{
  ColName, ColVal, EndpointId, PrimaryKey, QueryId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::model::message::TabletMessage;
use crate::multiversion_map::MVM;
use rand::RngCore;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::read;
use std::iter::FromIterator;
use std::ops::{Add, Bound, Deref};
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
  pub sender_path: msg::SenderPath,
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
  query_id: QueryId,
}

#[derive(Debug)]
struct SubqueryPendingReadRegion {
  read_region: TableRegion,
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
}

#[derive(Debug)]
struct Executing {
  completed: usize,
  /// This fields is used to associate a subquery's QueryId with the position
  /// that it appears in the SQL query.
  subquery_pos: Vec<QueryId>,
  subquery_status: SubqueryStatus,
}

#[derive(Debug)]
enum ExecutionS {
  Start,
  Pending(Pending),
  Executing(Executing),
}

#[derive(Debug)]
struct TableReadES {
  root_query_id: QueryId,
  tier_map: TierMap,
  timestamp: Timestamp,
  context: Rc<Context>,

  // Fields needed for responding.
  sender_path: msg::SenderPath,
  query_id: QueryId,

  // Query-related fields.
  query: proc::SuperSimpleSelect,
  query_plan: QueryPlan,

  // Dynamically evolving fields.
  new_rms: HashSet<TabletGroupId>,
  state: ExecutionS,
}

#[derive(Debug)]
struct QueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to TableReadES.
  pub root_query_id: QueryId,
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
  subquery_status: SubqueryStatus,
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
  root_query_id: QueryId,
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
  new_rms: HashSet<TabletGroupId>,
  trans_table_view: Vec<(TransTableName, (Vec<ColName>, Vec<TableView>))>,
  state: GRExecutionS,

  /// This holds the path to the parent ES.
  orig_p: OrigP,
}

// -----------------------------------------------------------------------------------------------
//  MSQueryES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
struct MSTableWriteES {}

#[derive(Debug)]
struct MSTableReadES {}

#[derive(Debug)]
struct MSWriteQueryReplanningES {
  /// The below fields are from PerformQuery and are passed through to TableReadES.
  pub root_query_id: QueryId,
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

#[derive(Debug)]
struct MSQueryES {
  timestamp: Timestamp,
  ms_write_statuses: BTreeMap<u32, FullMSTableWriteES>,
  ms_read_statuses: HashMap<QueryId, FullMSTableReadES>,
}

// -----------------------------------------------------------------------------------------------
//  ReadStatus
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum ReadStatus {
  GRQueryES(GRQueryES),
  FullTableReadES(FullTableReadES),
  TMStatus(TMStatus),
}

// -----------------------------------------------------------------------------------------------
//  Region Isolation Algorithm
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct VerifyingReadWriteRegion {
  orig_p: OrigP,
  m_waiting_read_protected: BTreeSet<(OrigP, TableRegion)>,
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

enum ReadProtectionGrant {
  Read { timestamp: Timestamp, protect_request: (OrigP, TableRegion) },
  MRead { timestamp: Timestamp, protect_request: (OrigP, TableRegion) },
  DeadlockSafetyReadAbort { timestamp: Timestamp, protect_request: (OrigP, TableRegion) },
  DeadlockSafetyWriteAbort { timestamp: Timestamp, orig_p: OrigP },
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

#[derive(Debug)]
pub struct TabletState<T: IOTypes> {
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

  waiting_read_protected: BTreeMap<Timestamp, BTreeSet<(OrigP, TableRegion)>>,
  read_protected: BTreeMap<Timestamp, BTreeSet<TableRegion>>,

  // Schema Change and Locking
  prepared_schema_change: BTreeMap<Timestamp, HashMap<ColName, Option<ColType>>>,
  request_index: BTreeMap<Timestamp, BTreeSet<QueryId>>, // Used to help iterate requests in order.
  requested_locked_columns: HashMap<QueryId, (OrigP, Timestamp, Vec<ColName>)>,

  /// Child Queries
  ms_statuses: HashMap<QueryId, MSQueryES>,
  read_statuses: HashMap<QueryId, ReadStatus>,
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
      ms_statuses: Default::default(),
      read_statuses: Default::default(),
      master_query_map: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::TabletMessage) {
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
                orig_p: OrigP::ReadPath(perform_query.query_id.clone()),
                state: CommonQueryReplanningS::Start,
              };
              comm_plan_es.start::<T>(
                &mut self.rand,
                &self.table_schema,
                &mut self.request_index,
                &mut self.requested_locked_columns,
              );
              // Add ReadStatus
              self.read_statuses.insert(
                perform_query.query_id.clone(),
                ReadStatus::FullTableReadES(FullTableReadES::QueryReplanning(QueryReplanningES {
                  root_query_id: perform_query.root_query_id,
                  tier_map: perform_query.tier_map,
                  query_id: perform_query.query_id.clone(),
                  status: comm_plan_es,
                })),
              );
            }
          }
          msg::GeneralQuery::UpdateQuery(query) => {
            let tier = *perform_query.tier_map.map.get(&query.sql_query.table).unwrap();

            // Lookup the MSQueryES
            if !self.ms_statuses.contains_key(&perform_query.root_query_id) {
              // If it doesn't exist, we try adding one. Of course, we must
              // check if the Timestamp is available or not.
              let timestamp = query.timestamp;
              if self.verifying_writes.contains_key(&timestamp)
                || self.prepared_writes.contains_key(&timestamp)
              {
                // This means the Timestamp is already in use, so we have to Abort.
                let sender_path = &perform_query.sender_path;
                let eid = self.slave_address_config.get(&sender_path.slave_group_id).unwrap();
                self.network_output.send(
                  &eid,
                  msg::NetworkMessage::Slave(msg::SlaveMessage::QueryAborted(msg::QueryAborted {
                    sender_state_path: sender_path.state_path.clone(),
                    tablet_group_id: self.this_tablet_group_id.clone(),
                    query_id: perform_query.query_id.clone(),
                    payload: msg::AbortedData::ProjectedColumnsDNE { msg: String::new() },
                  })),
                );
                return;
              } else {
                // This means that we can add an MSQueryES at the Timestamp
                self.ms_statuses.insert(
                  perform_query.root_query_id.clone(),
                  MSQueryES {
                    timestamp: query.timestamp,
                    ms_write_statuses: Default::default(),
                    ms_read_statuses: Default::default(),
                  },
                );

                // Also add a VerifyingReadWriteRegion
                assert!(!self.verifying_writes.contains_key(&timestamp));
                self.verifying_writes.insert(
                  timestamp,
                  VerifyingReadWriteRegion {
                    orig_p: OrigP::MSQueryWritePath(perform_query.root_query_id.clone(), tier),
                    m_waiting_read_protected: BTreeSet::new(),
                    m_read_protected: BTreeSet::new(),
                    m_write_protected: BTreeSet::new(),
                  },
                );
              }
            }
            let ms_query_es = self.ms_statuses.get_mut(&perform_query.root_query_id).unwrap();

            // Create an MSWriteTableES in the QueryReplanning state, and add it to
            // the MSQueryES.
            let mut comm_plan_es = CommonQueryReplanningES {
              timestamp: query.timestamp,
              context: Rc::new(query.context),
              sql_view: query.sql_query.clone(),
              query_plan: query.query_plan,
              sender_path: perform_query.sender_path,
              orig_p: OrigP::ReadPath(perform_query.query_id.clone()),
              state: CommonQueryReplanningS::Start,
            };
            comm_plan_es.start::<T>(
              &mut self.rand,
              &self.table_schema,
              &mut self.request_index,
              &mut self.requested_locked_columns,
            );
            assert!(!ms_query_es.ms_write_statuses.contains_key(&tier));
            ms_query_es.ms_write_statuses.insert(
              tier,
              FullMSTableWriteES::QueryReplanning(MSWriteQueryReplanningES {
                root_query_id: perform_query.root_query_id,
                tier_map: perform_query.tier_map,
                query_id: perform_query.query_id,
                status: comm_plan_es,
              }),
            );
          }
        }
      }
      msg::TabletMessage::CancelQuery(_) => unimplemented!(),
      msg::TabletMessage::QueryAborted(query_aborted) => {
        // fuck
      }
      msg::TabletMessage::QuerySuccess(query_success) => {
        self.handle_query_success(query_success);
      }
      msg::TabletMessage::Query2PCPrepare(_) => unimplemented!(),
      msg::TabletMessage::Query2PCAbort(_) => unimplemented!(),
      msg::TabletMessage::Query2PCCommit(_) => unimplemented!(),
      msg::TabletMessage::MasterFrozenColUsageAborted(_) => unimplemented!(),
      msg::TabletMessage::MasterFrozenColUsageSuccess(_) => unimplemented!(),
    }
  }

  fn run_main_loop(&mut self) {
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
      let maybe_first_done = (|| {
        for (_, query_set) in &self.request_index {
          for query_id in query_set {
            let (orig_p, timestamp, cols) = self.requested_locked_columns.get(query_id).unwrap();
            let not_preparing = (|| {
              for col in cols {
                // Check if the `col` is being Prepared.
                if let Some(prep_timestamp) = col_prepare_timestamps.get(col) {
                  if timestamp >= prep_timestamp {
                    return false;
                  }
                }
              }
              return true;
            })();
            if not_preparing {
              // This means that this task is done. We still need to enforce the lats
              // to be high enough.
              for col in cols {
                let maybe_key_col =
                  self.table_schema.key_cols.iter().find(|(col_name, _)| col == col_name);
                if maybe_key_col.is_none() {
                  // We need to update the lat in val_cols.
                  self.table_schema.val_cols.read(col, timestamp.clone());
                }
              }
              return Some((timestamp.clone(), query_id.clone(), orig_p.clone()));
            }
          }
        }
        return None;
      })();
      if let Some((timestamp, query_id, orig_p)) = maybe_first_done {
        // Maintain the request_index.
        self.request_index.get_mut(&timestamp).unwrap().remove(&query_id);
        if self.request_index.get_mut(&timestamp).unwrap().is_empty() {
          self.request_index.remove(&timestamp);
        }

        // Maintain the requested_locked_columns.
        self.requested_locked_columns.remove(&query_id);

        // Process
        self.columns_locked_for_query(orig_p);
        change_occurred = true;
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
              let (_, read_region) = protect_request;
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
                let (_, read_region) = protect_request;
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
              let (_, read_region) = protect_request;
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
              let (orig_p, read_region) = protect_request;
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
              let (_, read_region) = protect_request;
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
            let mut set = self.waiting_read_protected.get_mut(&timestamp).unwrap();
            set.remove(&protect_request);
            if set.is_empty() {
              self.waiting_read_protected.remove(&timestamp);
            }
            let (orig_p, read_region) = protect_request;

            // Add the ReadRegion to `read_protected`.
            if let Some(set) = self.read_protected.get_mut(&timestamp) {
              set.insert(read_region.clone());
            } else {
              let read_regions = vec![read_region.clone()];
              self.read_protected.insert(timestamp, read_regions.into_iter().collect());
            }

            // Inform the originator.
            self.read_protected_for_query(orig_p, read_region);
          }
          ReadProtectionGrant::MRead { timestamp, protect_request } => {
            // Remove the protect_request, adding the ReadRegion to `m_read_protected`.
            let mut verifying_write = self.verifying_writes.get_mut(&timestamp).unwrap();
            verifying_write.m_waiting_read_protected.remove(&protect_request);

            let (orig_p, read_region) = protect_request;
            verifying_write.m_read_protected.insert(read_region.clone());

            // Inform the originator.
            self.read_protected_for_query(orig_p, read_region);
          }
          ReadProtectionGrant::DeadlockSafetyReadAbort { timestamp, protect_request } => {
            // Remove the protect_request, doing any extra cleanups too.
            let mut set = self.waiting_read_protected.get_mut(&timestamp).unwrap();
            set.remove(&protect_request);
            if set.is_empty() {
              self.waiting_read_protected.remove(&timestamp);
            }
            let (orig_p, read_region) = protect_request;

            // Inform the originator
            self.deadlock_safety_read_abort(orig_p, read_region);
          }
          ReadProtectionGrant::DeadlockSafetyWriteAbort { orig_p, timestamp } => {
            // Remove the `verifying_write` at `timestamp`.
            self.verifying_writes.remove(&timestamp);

            // Inform the originator.
            self.deadlock_safety_write_abort(orig_p);
          }
        }
        change_occurred = true;
      }
    }
  }

  fn columns_locked_for_query(&mut self, orig_p: OrigP) {
    match orig_p.clone() {
      OrigP::MSCoordPath(_) => {}
      OrigP::MSQueryWritePath(root_query_id, tier) => {
        if let Some(ms_query_es) = self.ms_statuses.get_mut(&root_query_id) {
          if let Some(ms_write_es) = ms_query_es.ms_write_statuses.get_mut(&tier) {
            // Advance the QueryReplanning now that the desired columns have been locked.
            let plan_es = cast!(FullMSTableWriteES::QueryReplanning, ms_write_es).unwrap();
            let query_id = &plan_es.query_id;
            let comm_plan_es = &mut plan_es.status;
            comm_plan_es.column_locked::<T>(
              query_id.clone(),
              &mut self.rand,
              &mut self.network_output,
              &self.this_tablet_group_id,
              &self.master_eid,
              &self.gossip,
              &self.table_schema,
              &self.slave_address_config,
              &mut self.request_index,
              &mut self.requested_locked_columns,
              &mut self.master_query_map,
            );
            // We check if the QueryReplanning is done.
            match comm_plan_es.state {
              CommonQueryReplanningS::Done(success) => {
                if success {
                  *ms_write_es = FullMSTableWriteES::Executing(MSTableWriteES {});
                } else {
                  // TODO: terminate the MSTableWriteES properly
                  self.read_statuses.remove(query_id);
                }
              }
              _ => {}
            }
          }
        }
      }
      OrigP::MSQueryReadPath(_, _) => {}
      OrigP::ReadPath(query_id) => {
        if let Some(read_status) = self.read_statuses.get_mut(&query_id) {
          // Advance the QueryReplanning now that the desired columns have been locked.
          let read_es = cast!(ReadStatus::FullTableReadES, read_status).unwrap();
          let plan_es = cast!(FullTableReadES::QueryReplanning, read_es).unwrap();
          let comm_plan_es = &mut plan_es.status;
          comm_plan_es.column_locked::<T>(
            query_id.clone(),
            &mut self.rand,
            &mut self.network_output,
            &self.this_tablet_group_id,
            &self.master_eid,
            &self.gossip,
            &self.table_schema,
            &self.slave_address_config,
            &mut self.request_index,
            &mut self.requested_locked_columns,
            &mut self.master_query_map,
          );
          // We check if the QueryReplanning is done.
          match comm_plan_es.state {
            CommonQueryReplanningS::Done(success) => {
              if success {
                // If the QueryReplanning was successful, we move the FullTableReadES
                // to Executing in the Start state, and immediately start executing it.
                *read_es = FullTableReadES::Executing(TableReadES {
                  root_query_id: plan_es.root_query_id.clone(),
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
                self.start_table_read_es(&query_id);
              } else {
                // Recall that if QueryReplanning had ended in a failure (i.e.
                // having missing columns), then `CommonQueryReplanningES` will
                // have send back the necessary responses. Thus, we only need to
                // Exist the ES here.
                self.read_statuses.remove(&query_id);
              }
            }
            _ => {}
          }
        }
      }
    }
  }

  /// Processes the Start state of TableReadES.
  fn start_table_read_es(&mut self, query_id: &QueryId) {
    let read_status = self.read_statuses.get_mut(&query_id).unwrap();
    let read_es = cast!(ReadStatus::FullTableReadES, read_status).unwrap();
    let es = cast!(FullTableReadES::Executing, read_es).unwrap();

    // Compute the Column Region.
    let mut col_region = HashSet::<ColName>::new();
    col_region.extend(es.query.projection.clone());
    col_region.extend(es.query_plan.col_usage_node.safe_present_cols.clone());

    // Here, `context_col_index` has every External Column used by
    // the query as a key.
    let mut context_col_index = HashMap::<ColName, usize>::new();
    let col_schema = es.context.context_schema.column_context_schema.clone();
    for (index, col) in col_schema.iter().enumerate() {
      if es.query_plan.col_usage_node.external_cols.contains(col) {
        context_col_index.insert(col.clone(), index);
      }
    }

    // Compute the Row Region by taking the union across all ContextRows
    let mut row_region = Vec::<KeyBound>::new();
    for context_row in &es.context.context_rows {
      // First, map all External Columns names to the corresponding values
      // in this ContextRow
      let mut col_context = HashMap::<ColName, ColValN>::new();
      for (col, index) in &context_col_index {
        col_context
          .insert(col.clone(), context_row.column_context_row.get(*index).unwrap().clone());
      }

      match compute_key_region(&es.query.selection, &self.table_schema.key_cols, col_context) {
        Ok(key_bounds) => {
          for key_bound in key_bounds {
            row_region.push(key_bound);
          }
        }
        Err(eval_error) => {
          self.handle_eval_error_table_es(&query_id, eval_error);
          return;
        }
      }
    }
    row_region = compress_row_region(row_region);

    // Move the TableReadES to the Pending state with the given ReadRegion.
    let col_region: Vec<ColName> = col_region.into_iter().collect();
    let read_region = TableRegion { col_region, row_region };
    es.state = ExecutionS::Pending(Pending { read_region: read_region.clone() });

    // Add read protection
    if let Some(waiting) = self.waiting_read_protected.get_mut(&es.timestamp) {
      waiting.insert((OrigP::ReadPath(es.query_id.clone()), read_region));
    } else {
      self.waiting_read_protected.insert(
        es.timestamp,
        vec![(OrigP::ReadPath(es.query_id.clone()), read_region)].into_iter().collect(),
      );
    }
  }

  /// This is used to simply delete a TableReadES due to an expression evaluation
  /// error that occured inside, and respond to the client. Note that this function doesn't
  /// do any other kind of cleanup.
  fn handle_eval_error_table_es(&mut self, query_id: &QueryId, eval_error: EvalError) {
    let read_status = self.read_statuses.get_mut(&query_id).unwrap();
    let read_es = cast!(ReadStatus::FullTableReadES, read_status).unwrap();
    let es = cast!(FullTableReadES::Executing, read_es).unwrap();

    // If an error occurs here, we simply abort this whole query and respond
    // to the sender with an Abort.
    let aborted = msg::QueryAborted {
      sender_state_path: es.sender_path.state_path.clone(),
      tablet_group_id: self.this_tablet_group_id.clone(),
      query_id: query_id.clone(),
      payload: msg::AbortedData::TypeError { msg: format!("{:?}", eval_error) },
    };

    let sid = &es.sender_path.slave_group_id;
    let eid = self.slave_address_config.get(sid).unwrap();
    self.network_output.send(
      &eid,
      msg::NetworkMessage::Slave(
        if let Some(tablet_group_id) = &es.sender_path.maybe_tablet_group_id {
          msg::SlaveMessage::TabletMessage(
            tablet_group_id.clone(),
            msg::TabletMessage::QueryAborted(aborted),
          )
        } else {
          msg::SlaveMessage::QueryAborted(aborted)
        },
      ),
    );
    self.read_statuses.remove(&query_id);
  }

  /// We get this if a ReadProtection was granted by the Main Loop. This includes standard
  /// read_protected, or m_read_protected.
  fn read_protected_for_query(&mut self, orig_p: OrigP, _: TableRegion) {
    match orig_p {
      OrigP::MSCoordPath(_) => panic!(),
      OrigP::MSQueryWritePath(_, _) => {}
      OrigP::MSQueryReadPath(_, _) => {}
      OrigP::ReadPath(query_id) => {
        if let Some(read_status) = self.read_statuses.get_mut(&query_id) {
          let read_es = cast!(ReadStatus::FullTableReadES, read_status).unwrap();
          let es = cast!(FullTableReadES::Executing, read_es).unwrap();
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

          // We first compute all GRQueryESs before adding them to `read_status`, in case
          // an error occurs here
          let mut gr_query_statuses = Vec::<GRQueryES>::new();
          let subqueries = collect_select_subqueries(&es.query);
          for subquery_index in 0..subqueries.len() {
            let subquery = subqueries.get(subquery_index).unwrap();
            let child = es.query_plan.col_usage_node.children.get(subquery_index).unwrap();

            // This computes a ContextSchema for the subquery, as well as expose a conversion
            // utility to compute ContextRows.
            let conv = ContextConverter::new(
              &es.context.context_schema,
              &es.query_plan.col_usage_node,
              subquery_index,
            );

            // Construct the `ContextRow`s. To do this, we iterate over main Query's
            // `ContextRow`s and then the corresponding `ContextRow`s for the subquery.
            // We hold the child `ContextRow`s in Vec, and we use a HashSet to avoid duplicates.
            let mut new_context_rows = Vec::<ContextRow>::new();
            let mut new_row_set = HashSet::<ContextRow>::new();
            for context_row in &es.context.context_rows {
              // Next, we compute the tightest KeyBound for this `context_row`, compute the
              // corresponding subtable using `safe_present_split`, and then extend it by this
              // `context_row.column_context_row`. We also add the `TransTableContextRow`. This
              // results in a set of `ContextRows` that we add to the childs context.
              match compute_key_region(
                &es.query.selection,
                &self.table_schema.key_cols,
                conv.compute_col_context(&context_row),
              ) {
                Ok(key_bounds) => {
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
                Err(eval_error) => {
                  // If an error occurs here, we simply abort this whole query and respond
                  // to the sender with an Abort.
                  let aborted = msg::QueryAborted {
                    sender_state_path: es.sender_path.state_path.clone(),
                    tablet_group_id: self.this_tablet_group_id.clone(),
                    query_id: query_id.clone(),
                    payload: msg::AbortedData::TypeError { msg: format!("{:?}", eval_error) },
                  };

                  let sid = &es.sender_path.slave_group_id;
                  let eid = self.slave_address_config.get(sid).unwrap();
                  self.network_output.send(
                    &eid,
                    msg::NetworkMessage::Slave(
                      if let Some(tablet_group_id) = &es.sender_path.maybe_tablet_group_id {
                        msg::SlaveMessage::TabletMessage(
                          tablet_group_id.clone(),
                          msg::TabletMessage::QueryAborted(aborted),
                        )
                      } else {
                        msg::SlaveMessage::QueryAborted(aborted)
                      },
                    ),
                  );
                  self.read_statuses.remove(&query_id);
                  return;
                }
              }
            }

            // Finally, compute the context.
            let context = Rc::new(Context {
              context_schema: conv.context_schema,
              context_rows: new_context_rows,
            });

            // Construct the GRQueryES
            let gr_query_id = mk_qid(&mut self.rand);
            let gr_query_es = GRQueryES {
              root_query_id: es.root_query_id.clone(),
              tier_map: es.tier_map.clone(),
              timestamp: es.timestamp.clone(),
              context,
              new_trans_table_context: vec![],
              query_id: gr_query_id.clone(),
              query: subquery.clone(),
              query_plan: GRQueryPlan {
                gossip_gen: es.query_plan.gossip_gen.clone(),
                trans_table_schemas: es.query_plan.trans_table_schemas.clone(),
                col_usage_nodes: child.clone(),
              },
              new_rms: Default::default(),
              trans_table_view: vec![],
              state: GRExecutionS::Start,
              orig_p: OrigP::ReadPath(query_id.clone()),
            };
            gr_query_statuses.push(gr_query_es)
          }

          // Here, we have computed all GRQueryESs, and we can now add them to `read_statuses`
          // and move the TableReadESs state to `Executing`.
          let mut subquery_status = SubqueryStatus { subqueries: Default::default() };
          for gr_query_es in &gr_query_statuses {
            subquery_status.subqueries.insert(
              gr_query_es.query_id.clone(),
              SingleSubqueryStatus::Pending(SubqueryPending {
                context: gr_query_es.context.clone(),
              }),
            );
          }

          let mut gr_query_ids = Vec::<QueryId>::new();
          for gr_query_es in &gr_query_statuses {
            gr_query_ids.push(gr_query_es.query_id.clone());
          }

          es.state = ExecutionS::Executing(Executing {
            completed: 0,
            subquery_pos: gr_query_ids.clone(),
            subquery_status,
          });

          for gr_query_es in gr_query_statuses {
            let query_id = gr_query_es.query_id.clone();
            self.read_statuses.insert(query_id, ReadStatus::GRQueryES(gr_query_es));
          }

          // Drive GRQueries
          for query_id in gr_query_ids {
            self.advance_gr_query(query_id);
          }
        }
      }
    }
  }

  fn advance_gr_query(&mut self, query_id: QueryId) {
    let read_status = self.read_statuses.get_mut(&query_id).unwrap();
    let es = cast!(ReadStatus::GRQueryES, read_status).unwrap();

    // Compute the next stage
    let next_stage_idx = match &es.state {
      GRExecutionS::Start => 0,
      GRExecutionS::ReadStage(read_stage) => read_stage.stage_idx + 1,
      _ => panic!(),
    };

    if next_stage_idx < es.query_plan.col_usage_nodes.len() {
      // This means that we have still have stages to evaluate, so we move on.
      self.process_gr_query_stage(query_id, next_stage_idx);
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

      let orig_p = es.orig_p.clone();
      let schema = schema.clone();
      let new_rms = es.new_rms.clone();
      self.handle_gr_query_done(orig_p, query_id, new_rms, (schema, result));
    }
  }

  /// This function moves the GRQueryES to the Stage indicated by `stage_idx`.
  /// This index must be valid (an actual stage).
  fn process_gr_query_stage(&mut self, query_id: QueryId, stage_idx: usize) {
    let read_status = self.read_statuses.get_mut(&query_id).unwrap();
    let es = cast!(ReadStatus::GRQueryES, read_status).unwrap();
    assert!(stage_idx < es.query_plan.col_usage_nodes.len());
    let (_, (_, child)) = es.query_plan.col_usage_nodes.get(stage_idx).unwrap();

    // We first compute the Context
    let mut context_schema = ContextSchema::default();

    // Compute the `external_split`.
    let external_split = child.external_cols.clone();

    // Point all `external_split` columns to their index in main Query's ContextSchema.
    let mut context_col_index = HashMap::<ColName, usize>::new();
    let col_schema = &es.context.context_schema.column_context_schema;
    for (index, col) in col_schema.iter().enumerate() {
      if external_split.contains(col) {
        context_col_index.insert(col.clone(), index);
      }
    }

    // Compute the ColumnContextSchema
    context_schema.column_context_schema.extend(external_split.clone());

    // Compute the TransTables that the child Context will use.
    let child_query_trans_tables = node_external_trans_tables(child);

    // Split the `child_query_trans_tables` according to which of them are defined
    // in this GRQueryES, and which come from the outside.
    let mut local_trans_table_split = Vec::<TransTableName>::new();
    let mut external_trans_table_split = Vec::<TransTableName>::new();
    let completed_local_trans_tables: HashSet<TransTableName> =
      es.trans_table_view.iter().map(|(name, _)| name).cloned().collect();
    for trans_table_name in child_query_trans_tables {
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
      for col in &external_split {
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

    // Compute the TransTable schemas
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

    let query_plan = QueryPlan {
      gossip_gen: es.query_plan.gossip_gen.clone(),
      trans_table_schemas: child_trans_table_schemas,
      col_usage_node: child.clone(),
    };

    // Construct the TMStatus
    let tm_query_id = mk_qid(&mut self.rand);
    let mut tm_status = TMStatus {
      root_query_id: es.root_query_id.clone(),
      new_rms: Default::default(),
      responded_count: 0,
      tm_state: Default::default(),
      orig_p: OrigP::ReadPath(es.query_id.clone()),
    };

    let sender_path = msg::SenderPath {
      slave_group_id: self.this_slave_group_id.clone(),
      maybe_tablet_group_id: Some(self.this_tablet_group_id.clone()),
      state_path: msg::SenderStatePath::TMStatusQueryId(tm_query_id.clone()),
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
                root_query_id: es.root_query_id.clone(),
                sender_path: sender_path.clone(),
                query_id: child_query_id.clone(),
                tier_map: es.tier_map.clone(),
                query: general_query.clone(),
              }),
            )),
          );

          // Add the TabletGroup into the TMStatus.
          let node_group_id = NodeGroupId::Tablet(tablet_group_id);
          tm_status.tm_state.insert(node_group_id, TMWaitValue::QueryId(child_query_id));
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
          root_query_id: es.root_query_id.clone(),
          sender_path: sender_path.clone(),
          query_id: child_query_id.clone(),
          tier_map: es.tier_map.clone(),
          query: general_query.clone(),
        };

        // Send out PerformQuery, wrapping it according to if the
        // TransTable is on a Slave or a Tablet
        let node_group_id = match &location_prefix.source {
          NodeGroupId::Tablet(tablet_group_id) => {
            let sid = self.tablet_address_config.get(tablet_group_id).unwrap();
            let eid = self.slave_address_config.get(sid).unwrap();
            let network_msg = msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
              tablet_group_id.clone(),
              msg::TabletMessage::PerformQuery(perform_query),
            ));
            self.network_output.send(eid, network_msg);
            NodeGroupId::Tablet(tablet_group_id.clone())
          }
          NodeGroupId::Slave(sid) => {
            let eid = self.slave_address_config.get(sid).unwrap();
            let network_msg =
              msg::NetworkMessage::Slave(msg::SlaveMessage::PerformQuery(perform_query));
            self.network_output.send(eid, network_msg);
            NodeGroupId::Slave(sid.clone())
          }
        };

        // Add the TabletGroup into the TMStatus.
        tm_status.tm_state.insert(node_group_id, TMWaitValue::QueryId(child_query_id.clone()));
      }
    };

    // Create a SubqueryStatus and move the GRQueryES to the next Stage.
    let mut subquery_status = SubqueryStatus { subqueries: Default::default() };
    subquery_status.subqueries.insert(
      tm_query_id.clone(),
      SingleSubqueryStatus::Pending(SubqueryPending { context: context.clone() }),
    );
    es.state =
      GRExecutionS::ReadStage(ReadStage { stage_idx, parent_context_map, subquery_status });

    // Add the TMStatus into read_statuses
    self.read_statuses.insert(tm_query_id, ReadStatus::TMStatus(tm_status));
  }

  fn handle_query_success(&mut self, query_success: msg::QuerySuccess) {
    let tm_query_id = &query_success.query_id;
    if let Some(read_status) = self.read_statuses.get_mut(tm_query_id) {
      // Since the `read_status` must be a TMStatus, we cast it.
      let tm_status = cast!(ReadStatus::TMStatus, read_status).unwrap();
      // We just add the result of the `query_success` here.
      let tm_wait_value = tm_status.tm_state.get_mut(&query_success.node_group_id).unwrap();
      *tm_wait_value = TMWaitValue::Result(query_success.result.clone());
      tm_status.new_rms.extend(query_success.new_rms);
      tm_status.responded_count += 1;
      if tm_status.responded_count == tm_status.tm_state.len() {
        // Remove the `TMStatus` and take ownershup
        let read_status = self.read_statuses.remove(&query_success.query_id).unwrap();
        let tm_status = cast!(ReadStatus::TMStatus, read_status).unwrap();
        // Merge there TableViews together
        let mut results = Vec::<(Vec<ColName>, Vec<TableView>)>::new();
        for (_, tm_wait_value) in tm_status.tm_state {
          results.push(cast!(TMWaitValue::Result, tm_wait_value).unwrap());
        }
        let merged_result = merge_table_views(results);
        let gr_query_id = cast!(OrigP::ReadPath, tm_status.orig_p).unwrap();
        self.handle_tm_done(gr_query_id, tm_query_id.clone(), merged_result);
      }
    }
  }

  /// This function processes the result of a GRQueryES.
  fn handle_gr_query_done(
    &mut self,
    orig_p: OrigP,
    subquery_id: QueryId,
    subquery_new_rms: HashSet<TabletGroupId>,
    (_, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    match orig_p {
      OrigP::MSCoordPath(_) => panic!(),
      OrigP::MSQueryWritePath(_, _) => {}
      OrigP::MSQueryReadPath(_, _) => {}
      OrigP::ReadPath(query_id) => {
        if let Some(read_status) = self.read_statuses.get_mut(&query_id) {
          let read_es = cast!(ReadStatus::FullTableReadES, read_status).unwrap();
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
            ultimately allowing us to lookup SingleSubqueryContext's Vec<TableView>. (We
            don't even need the ContextRow in Finished anymore.) Thus, for every
            main ContextRow + in-bound TableRow, we can replace appearances of
            ValExpr::Subquery with actual values. (Note we also throw runtime errors if
            the subquery TableViews aren't single values.)
            */

            let subqueries = collect_select_subqueries(&es.query);

            // Construct the ContextConverters for all subqueries
            let mut converters = Vec::<ContextConverter>::new();
            for subquery_index in 0..subqueries.len() {
              converters.push(ContextConverter::new(
                &es.context.context_schema,
                &es.query_plan.col_usage_node,
                subquery_index,
              ));
            }

            // Setup the child_context_row_maps that will be populated over time.
            let mut child_context_row_maps = Vec::<HashMap<ContextRow, usize>>::new();
            for _ in 0..subqueries.len() {
              child_context_row_maps.push(HashMap::new());
            }

            // Compute the Schema that should belong to every TableView that's returned
            // from this TableReadES.
            let mut res_col_names = Vec::<(ColName, ColType)>::new();
            for col_name in &es.query_plan.col_usage_node.requested_cols {
              let col_type = if let Some(pos) = lookup_pos(&self.table_schema.key_cols, col_name) {
                let (_, col_type) = self.table_schema.key_cols.get(pos).unwrap();
                col_type.clone()
              } else {
                self.table_schema.val_cols.strong_static_read(col_name, es.timestamp).unwrap()
              };
              res_col_names.push((col_name.clone(), col_type));
            }

            // Now, we are recomputing the KeyRegion the same way as we had to when
            // we went to Pending to wait for the ReadRegion to lock.

            // Here, `context_col_index` has every External Column used by
            // the query as a key.
            let mut context_col_index = HashMap::<ColName, usize>::new();
            let col_schema = es.context.context_schema.column_context_schema.clone();
            for (index, col) in col_schema.iter().enumerate() {
              if es.query_plan.col_usage_node.external_cols.contains(col) {
                context_col_index.insert(col.clone(), index);
              }
            }

            let mut res_table_views = Vec::<TableView>::new();
            for context_row in &es.context.context_rows {
              // First, map all External Columns names to the corresponding values
              // in this ContextRow
              let mut col_context = HashMap::<ColName, ColValN>::new();
              for (col, index) in &context_col_index {
                col_context
                  .insert(col.clone(), context_row.column_context_row.get(*index).unwrap().clone());
              }

              // Since we recomputed this exact `key_bounds` before going to the Pending
              // state, we can unwrap it; there should be no errors.
              let key_bounds =
                compute_key_region(&es.query.selection, &self.table_schema.key_cols, col_context)
                  .unwrap();

              // We iterate over the rows of the Subtable computed for this ContextRow,
              // computing the TableView we desire for the ContextRow.
              let (subtable_schema, subtable) = compute_subtable(
                &es.query_plan.col_usage_node.safe_present_cols,
                &key_bounds,
                &self.storage,
              );

              // Next, we initialize the TableView that we are trying to construct for
              // this `context_row`.

              let mut res_table_view =
                TableView { col_names: res_col_names.clone(), rows: Default::default() };

              for subtable_row in subtable {
                // Now, we compute the subquery result for all subqueries for this
                // `context_row` + `subtable_row`.
                let mut subquery_vals = Vec::<TableView>::new();
                for index in 0..subqueries.len() {
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
                let mut subtable_row_map = HashMap::<ColName, ColValN>::new();
                for i in 0..subtable_schema.len() {
                  subtable_row_map.insert(
                    subtable_schema.get(i).unwrap().clone(),
                    subtable_row.get(i).unwrap().clone(),
                  );
                }

                match is_true(&evaluate_expr(
                  &es.query.selection,
                  &subtable_row_map,
                  &subquery_vals,
                )) {
                  Ok(bool_val) => {
                    if bool_val {
                      // This means that the current row should be selected for the result.
                      // First, we take the projected columns.
                      let mut res_row = Vec::<ColValN>::new();
                      for (res_col_name, _) in &res_col_names {
                        // Now, what? We take this
                        // We need to compute the row for the select cols. Thus, for every
                        // select col, we need to find it's position. Then, we need to read
                        // the row
                        let idx = subtable_schema.iter().position(|k| res_col_name == k).unwrap();
                        res_row.push(subtable_row.get(idx).unwrap().clone());
                      }

                      // And the `res_row` into the TableView.
                      if let Some(count) = res_table_view.rows.get_mut(&res_row) {
                        count.add(1);
                      } else {
                        res_table_view.rows.insert(res_row, 1);
                      }
                    }
                  }
                  Err(eval_error) => {
                    self.handle_eval_error_table_es(&query_id, eval_error);
                    return;
                  }
                }
              }

              // Finally, accumulate the resulting TableView.
              res_table_views.push(res_table_view);
            }

            // Build the success message and respond.
            let success_msg = msg::QuerySuccess {
              sender_state_path: es.sender_path.state_path.clone(),
              node_group_id: NodeGroupId::Tablet(self.this_tablet_group_id.clone()),
              query_id: query_id.clone(),
              result: (es.query_plan.col_usage_node.requested_cols.clone(), res_table_views),
              new_rms: es.new_rms.iter().cloned().collect(),
            };

            let sid = &es.sender_path.slave_group_id;
            let eid = self.slave_address_config.get(sid).unwrap();
            self.network_output.send(
              &eid,
              msg::NetworkMessage::Slave(
                if let Some(tablet_group_id) = &es.sender_path.maybe_tablet_group_id {
                  msg::SlaveMessage::TabletMessage(
                    tablet_group_id.clone(),
                    msg::TabletMessage::QuerySuccess(success_msg),
                  )
                } else {
                  msg::SlaveMessage::QuerySuccess(success_msg)
                },
              ),
            );

            // Remove the TableReadES.
            self.read_statuses.remove(&query_id);
          }
        }
      }
    }
  }

  fn handle_tm_done(
    &mut self,
    query_id: QueryId,
    tm_query_id: QueryId,
    (schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    let read_status = self.read_statuses.get_mut(&query_id).unwrap();
    let es = cast!(ReadStatus::GRQueryES, read_status).unwrap();
    let read_stage = cast!(GRExecutionS::ReadStage, &mut es.state).unwrap();
    let subqueries = &read_stage.subquery_status.subqueries;

    // Extract the `context` from the SingleSubqueryStatus and verify
    // that it corresponds to `table_views`.
    assert_eq!(subqueries.len(), 1);
    let (child_query_id, subquery_status) = subqueries.iter().next().unwrap();
    let subquery_context = &cast!(SingleSubqueryStatus::Pending, subquery_status).unwrap().context;
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
    self.advance_gr_query(query_id);
  }

  /// We call this when a DeadlockSafetyReadAbort happens
  fn deadlock_safety_read_abort(&mut self, orig_p: OrigP, read_region: TableRegion) {}

  /// We call this when a DeadlockSafetyWriteAbort happens
  fn deadlock_safety_write_abort(&mut self, orig_p: OrigP) {}
}

/// This evaluates a `ValExpr` completely into a `ColVal` by using the column values
/// and the subquery values.
fn evaluate_expr(
  expr: &proc::ValExpr,
  subtable_row_map: &HashMap<ColName, ColValN>,
  subquery_vals: &Vec<TableView>,
) -> ColValN {
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

/// This container computes and remembers how to construct a child ContextRow
/// from a parent ContexRow + Table row. We have to initially pass in the
/// the parent's ContextRowSchema, the parent's ColUsageNode, as well as `i`,
/// which points to the Subquery we want to compute child ContextRows for.
#[derive(Default)]
struct ContextConverter {
  // This is the child query's ContextSchema, and it's computed in the constructor.
  context_schema: ContextSchema,

  // These fields are the constituents of the `context_schema above.
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
  /// Compute all of the data members.
  fn new(
    parent_context_schema: &ContextSchema,
    parent_node: &FrozenColUsageNode,
    subquery_index: usize,
  ) -> ContextConverter {
    let mut conv = ContextConverter::default();
    let child = parent_node.children.get(subquery_index).unwrap();

    // Construct the ContextSchema of the GRQueryES.
    let mut context_schema = ContextSchema::default();
    let mut subquery_external_cols = HashSet::<ColName>::new();
    for (_, (_, node)) in child {
      subquery_external_cols.extend(node.external_cols.clone());
    }

    // Split the `subquery_external_cols` by which of those cols are in this Table,
    // and which aren't.
    for col in &subquery_external_cols {
      if parent_node.safe_present_cols.contains(col) {
        conv.safe_present_split.push(col.clone());
      } else {
        conv.external_split.push(col.clone());
      }
    }

    // Point all `conv.external_split` columns to their index in main Query's ContextSchema.
    let col_schema = &parent_context_schema.column_context_schema;
    for (index, col) in col_schema.iter().enumerate() {
      if conv.external_split.contains(col) {
        conv.context_col_index.insert(col.clone(), index);
      }
    }

    // Compute the ColumnContextSchema so that `afe_present_split` is first, and
    // `external_split` is second. This is relevent when we compute ContextRows.
    context_schema.column_context_schema.extend(conv.safe_present_split.clone());
    context_schema.column_context_schema.extend(conv.external_split.clone());

    // Compute the TransTables that the child Context will use.
    let trans_table_split = nodes_external_trans_tables(child);

    // Point all `trans_table_split` to their index in main Query's ContextSchema.
    let trans_table_context_schema = &parent_context_schema.trans_table_context_schema;
    for (index, prefix) in trans_table_context_schema.iter().enumerate() {
      if trans_table_split.contains(&prefix.trans_table_name) {
        conv.context_trans_table_index.insert(prefix.trans_table_name.clone(), index);
      }
    }

    // Compute the TransTableContextSchema
    for trans_table_name in &trans_table_split {
      let index = conv.context_trans_table_index.get(trans_table_name).unwrap();
      let trans_table_prefix = trans_table_context_schema.get(*index).unwrap().clone();
      context_schema.trans_table_context_schema.push(trans_table_prefix);
    }

    conv
  }

  /// Use the given `parent_context_row` to create a child ContextRow,
  /// consuming the given child ColumnContextRow in the process. (Here,
  /// this child ColumnContextRow is computed from the outside before
  /// being passed in.)
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

  /// This function takes a Table row, uses the `safe_present_cols` of this
  /// subquery + `subtable_schema` to compute the child ColumnContextRow. It
  /// can be fed into `compute_child_context_row` later on.
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

  /// Computes the ColumnContext for the given ContextRow of the parent.
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

/// Add the following triple into `requested_locked_columns`, making sure to update
/// `request_index` as well.
fn add_requested_locked_columns<T: IOTypes>(
  orig_p: OrigP,
  timestamp: Timestamp,
  cols: Vec<ColName>,
  // Tablet members
  rand: &mut T::RngCoreT,
  request_index: &mut BTreeMap<Timestamp, BTreeSet<QueryId>>,
  requested_locked_columns: &mut HashMap<QueryId, (OrigP, Timestamp, Vec<ColName>)>,
) -> QueryId {
  let locked_cols_qid = mk_qid(rand);
  // Add column locking
  requested_locked_columns.insert(locked_cols_qid.clone(), (orig_p, timestamp, cols));
  // Update index
  if let Some(set) = request_index.get_mut(&timestamp) {
    set.insert(locked_cols_qid.clone());
  } else {
    let mut set = BTreeSet::<QueryId>::new();
    set.insert(locked_cols_qid.clone());
    request_index.insert(timestamp, set);
  };
  locked_cols_qid
}

impl<R: QueryReplanningSqlView> CommonQueryReplanningES<R> {
  fn start<T: IOTypes>(
    &mut self,
    // Tablet members
    rand: &mut T::RngCoreT,
    table_schema: &TableSchema,
    request_index: &mut BTreeMap<Timestamp, BTreeSet<QueryId>>,
    requested_locked_columns: &mut HashMap<QueryId, (OrigP, Timestamp, Vec<ColName>)>,
  ) {
    matches!(self.state, CommonQueryReplanningS::Start);
    // Add column locking
    let locked_cols_qid = add_requested_locked_columns::<T>(
      self.orig_p.clone(),
      self.timestamp,
      self.sql_view.projected_cols(table_schema),
      rand,
      request_index,
      requested_locked_columns,
    );
    self.state =
      CommonQueryReplanningS::ProjectedColumnLocking { locked_columns_query_id: locked_cols_qid };
  }

  fn column_locked<T: IOTypes>(
    &mut self,
    query_id: QueryId,
    // Tablet members
    rand: &mut T::RngCoreT,
    network_output: &mut T::NetworkOutT,
    this_tablet_group_id: &TabletGroupId,
    master_eid: &EndpointId,
    gossip: &Arc<GossipData>,
    table_schema: &TableSchema,
    slave_address_config: &HashMap<SlaveGroupId, EndpointId>,
    request_index: &mut BTreeMap<Timestamp, BTreeSet<QueryId>>,
    requested_locked_columns: &mut HashMap<QueryId, (OrigP, Timestamp, Vec<ColName>)>,
    master_query_map: &mut HashMap<QueryId, OrigP>,
  ) {
    match &self.state {
      CommonQueryReplanningS::ProjectedColumnLocking { .. } => {
        // We need to verify that the projected columns are present.
        for col in &self.sql_view.projected_cols(table_schema) {
          let maybe_key_col = table_schema.key_cols.iter().find(|(col_name, _)| col == col_name);
          if maybe_key_col.is_none() {
            // If the `col` wasn't part of the PrimaryKey, then we need to
            // check the `val_cols` to check for presence
            if table_schema.val_cols.strong_static_read(col, self.timestamp.clone()).is_none() {
              // This means a projected column doesn't exist. Thus, we Exit and Clean Up.
              let sender_path = &self.sender_path;
              let eid = slave_address_config.get(&sender_path.slave_group_id).unwrap();
              network_output.send(
                &eid,
                msg::NetworkMessage::Slave(msg::SlaveMessage::QueryAborted(msg::QueryAborted {
                  sender_state_path: sender_path.state_path.clone(),
                  tablet_group_id: this_tablet_group_id.clone(),
                  query_id: query_id.clone(),
                  payload: msg::AbortedData::ProjectedColumnsDNE { msg: String::new() },
                })),
              );
              self.state = CommonQueryReplanningS::Done(false);
              return;
            }
          }
        }

        // Next, we check if the `gen` in the query plan is more recent than the
        // current Tablet's `gen`, and we act accordingly.
        if gossip.gossip_gen <= self.query_plan.gossip_gen {
          // Lock all of the `external_cols` and `safe_present` columns in this query plan.
          let node = &self.query_plan.col_usage_node;
          let mut all_cols = node.external_cols.clone();
          all_cols.extend(node.safe_present_cols.iter().cloned());

          // Add column locking
          let locked_cols_qid = add_requested_locked_columns::<T>(
            self.orig_p.clone(),
            self.timestamp.clone(),
            all_cols,
            rand,
            request_index,
            requested_locked_columns,
          );

          // Advance Read Status
          self.state =
            CommonQueryReplanningS::ColumnLocking { locked_columns_query_id: locked_cols_qid };
        } else {
          // Replan the query. First, we need to gather the set of `ColName`s touched by the
          // query, and then we need to lock them.
          let mut planner = ColUsagePlanner {
            gossiped_db_schema: &gossip.gossiped_db_schema,
            timestamp: self.timestamp,
          };

          let all_cols = planner
            .get_all_cols(&mut self.query_plan.trans_table_schemas.clone(), &self.sql_view.exprs());

          // Add column locking
          let locked_cols_qid = add_requested_locked_columns::<T>(
            self.orig_p.clone(),
            self.timestamp.clone(),
            all_cols.clone(),
            rand,
            request_index,
            requested_locked_columns,
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
            let maybe_key_col = table_schema.key_cols.iter().find(|(col_name, _)| col == col_name);
            // Since the key_cols are static, no query plan should have a
            // key_col as an external col. Thus, we assert.
            assert!(maybe_key_col.is_none());
            if table_schema.val_cols.strong_static_read(&col, self.timestamp).is_some() {
              return false;
            }
          }
          // Next, check that `safe_present_cols` are present.
          for col in &self.query_plan.col_usage_node.safe_present_cols {
            let maybe_key_col = table_schema.key_cols.iter().find(|(col_name, _)| col == col_name);
            if maybe_key_col.is_none() {
              if table_schema.val_cols.strong_static_read(&col, self.timestamp).is_none() {
                return false;
              }
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
          assert!(self.query_plan.gossip_gen >= gossip.gossip_gen);
          let mut planner = ColUsagePlanner {
            gossiped_db_schema: &gossip.gossiped_db_schema,
            timestamp: self.timestamp,
          };

          let all_cols = planner
            .get_all_cols(&mut self.query_plan.trans_table_schemas.clone(), &self.sql_view.exprs());

          // Add column locking
          let locked_cols_qid = add_requested_locked_columns::<T>(
            self.orig_p.clone(),
            self.timestamp.clone(),
            all_cols.clone(),
            rand,
            request_index,
            requested_locked_columns,
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
          gossiped_db_schema: &gossip.gossiped_db_schema,
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
          let locked_cols_qid = add_requested_locked_columns::<T>(
            self.orig_p.clone(),
            self.timestamp.clone(),
            all_cols.clone(),
            rand,
            request_index,
            requested_locked_columns,
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
            let maybe_key_col = table_schema.key_cols.iter().find(|(col_name, _)| col == col_name);
            if maybe_key_col.is_some() {
              schema_cols.push(col.clone());
            } else {
              // If `col` isn't a `key_col`, then it might be a `val_col`.
              if table_schema.val_cols.strong_static_read(col, self.timestamp).is_some() {
                schema_cols.push(col.clone());
              }
            }
          }

          // Construct a new QueryPlan using the above `schema_cols`.
          let mut planner = ColUsagePlanner {
            gossiped_db_schema: &gossip.gossiped_db_schema,
            timestamp: self.timestamp,
          };
          let (_, col_usage_node) = planner.plan_stage_query_with_schema(
            &mut self.query_plan.trans_table_schemas.clone(),
            &self.sql_view.projected_cols(&table_schema),
            &proc::TableRef::TablePath(self.sql_view.table().clone()),
            schema_cols,
            &self.sql_view.exprs(),
          );

          let external_cols = col_usage_node.external_cols.clone();
          self.query_plan.gossip_gen = gossip.gossip_gen;
          self.query_plan.col_usage_node = col_usage_node;

          // Next, we check to see if all ColNames in `external_cols` is contaiend
          // in the context. If not, we have to consult the Master.
          for col in external_cols {
            if !self.context.context_schema.column_context_schema.contains(&col) {
              // This means we need to consult the Master.
              let master_qid = mk_qid(rand);

              network_output.send(
                master_eid,
                msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
                  msg::PerformMasterFrozenColUsage {
                    query_id: master_qid.clone(),
                    timestamp: self.timestamp,
                    trans_table_schemas: self.query_plan.trans_table_schemas.clone(),
                    col_usage_tree: msg::ColUsageTree::MSQueryStage(self.sql_view.ms_query_stage()),
                  },
                )),
              );
              master_query_map.insert(master_qid.clone(), self.orig_p.clone());

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
