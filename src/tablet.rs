use crate::col_usage::ColUsagePlanner;
use crate::common::{
  mk_qid, GossipData, IOTypes, NetworkOut, OrigP, QueryPlan, TMStatus, TableSchema,
};
use crate::model::common::{proc, ColType, Context, TierMap, TransTableName};
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
use std::ops::Bound::{Included, Unbounded};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  CommonQueryReplanningES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
enum QueryReplanningSqlView {
  SuperSimpleSelect(proc::SuperSimpleSelect),
  Update(proc::Update),
}

impl QueryReplanningSqlView {
  fn projected_cols(&self, table_schema: &TableSchema) -> Vec<ColName> {
    match &self {
      Self::SuperSimpleSelect(select) => {
        return select.projection.clone();
      }
      Self::Update(update) => {
        let mut projected_cols =
          Vec::from_iter(update.assignment.iter().map(|(col, _)| col.clone()));
        projected_cols.extend(table_schema.key_cols.iter().map(|(col, _)| col.clone()));
        return projected_cols;
      }
    }
  }

  fn table(&self) -> &TablePath {
    match &self {
      Self::SuperSimpleSelect(select) => cast!(proc::TableRef::TablePath, &select.from).unwrap(),
      Self::Update(update) => &update.table,
    }
  }

  fn exprs(&self) -> Vec<proc::ValExpr> {
    match &self {
      Self::SuperSimpleSelect(select) => vec![select.selection.clone()],
      Self::Update(update) => {
        let mut exprs = Vec::new();
        exprs.push(update.selection.clone());
        for (_, expr) in &update.assignment {
          exprs.push(expr.clone());
        }
        return exprs;
      }
    }
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
struct CommonQueryReplanningES {
  // These members are parallel to the messages in `msg::GeneralQuery`.
  pub timestamp: Timestamp,
  pub context: Rc<Context>,
  pub sql_view: QueryReplanningSqlView,
  pub query_plan: QueryPlan,

  /// Path of the original sender (needed for responding with errors).
  pub sender_path: msg::SenderPath,
  /// The OrigP of the Task holding this CommonQueryReplanningES
  pub orig_p: OrigP,
  /// The state of the CommonQueryReplanningES
  pub state: CommonQueryReplanningS,
}

// -----------------------------------------------------------------------------------------------
//  MSQueryCoordinationES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
enum ExecutionS {
  Pending(ReadRegion),
  Executing,
}

#[derive(Debug)]
struct TableReadES {
  tier_map: TierMap,
  timestamp: Timestamp,
  context: Rc<Context>,

  // Metadata copied from outside.
  sender_path: msg::SenderPath,
  query_id: QueryId,
  query: proc::SuperSimpleSelect,
  // Results of the query planning.
  query_plan: QueryPlan,

  // The dynamically evolving fields.
  all_rms: HashSet<TabletGroupId>,
  state: ExecutionS,
}

#[derive(Debug)]
struct QueryReplanningES {
  pub tier_map: TierMap,
  pub sql_query: proc::SuperSimpleSelect,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES,
}

#[derive(Debug)]
enum FullTableReadES {
  QueryReplanning(QueryReplanningES),
  Executing(TableReadES),
}

#[derive(Debug)]
enum ReadStatus {
  FullTableReadES(FullTableReadES),
  TMStatus(TMStatus),
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
  pub tier_map: TierMap,
  pub sql_query: proc::Update,
  pub query_id: QueryId,
  /// Used for updating the query plan
  pub status: CommonQueryReplanningES,
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
  pub status: CommonQueryReplanningES,
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
//  Region Isolation Algorithm
// -----------------------------------------------------------------------------------------------

type ReadRegion = (Vec<ColName>, Vec<(PrimaryKey, PrimaryKey)>);
type WriteRegion = (Vec<ColName>, Vec<(PrimaryKey, PrimaryKey)>);

#[derive(Debug)]
struct VerifyingReadWriteRegion {
  orig_p: OrigP,
  m_waiting_read_protected: Vec<(OrigP, ReadRegion)>,
  m_read_protected: Vec<ReadRegion>,
  m_write_protected: Vec<WriteRegion>,
}

#[derive(Debug)]
struct ReadWriteRegion {
  m_read_protected: Vec<ReadRegion>,
  m_write_protected: Vec<WriteRegion>,
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
// We also can't do the "shared auxiliary function" part, because the code we pull out
// will call functions that use `self`.
//
// Alternatively, we can stop trying to short-circuit forward and just always move onto
// the next state. Design is such a way that that's possible. Use the Main Loop.

// Naming: ES for structs, S for enums, names consistent with type (potentially shorter),
// perform_query, query, sql_query.
// `query_id` for main one in question.

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

  waiting_read_protected: BTreeMap<Timestamp, Vec<(OrigP, ReadRegion)>>,
  read_protected: BTreeMap<Timestamp, Vec<ReadRegion>>,

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
                sql_view: QueryReplanningSqlView::SuperSimpleSelect(query.sql_query.clone()),
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
                perform_query.query_id,
                ReadStatus::FullTableReadES(FullTableReadES::QueryReplanning(QueryReplanningES {
                  tier_map: perform_query.tier_map,
                  sql_query: query.sql_query,
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
                    m_waiting_read_protected: vec![],
                    m_read_protected: vec![],
                    m_write_protected: vec![],
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
              sql_view: QueryReplanningSqlView::Update(query.sql_query.clone()),
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
                tier_map: perform_query.tier_map,
                sql_query: query.sql_query,
                query_id: perform_query.query_id,
                status: comm_plan_es,
              }),
            );
          }
        }
      }
      msg::TabletMessage::CancelQuery(_) => unimplemented!(),
      msg::TabletMessage::QueryAborted(_) => unimplemented!(),
      msg::TabletMessage::QuerySuccess(_) => unimplemented!(),
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

      change_occurred = false;
    }
  }

  fn columns_locked_for_query(&mut self, orig_p: OrigP) {
    match orig_p {
      OrigP::MSCoordPath(_) => {}
      OrigP::MSQueryWritePath(root_query_id, tier) => {
        if let Some(ms_query_es) = self.ms_statuses.get_mut(&root_query_id) {
          if let Some(ms_write_es) = ms_query_es.ms_write_statuses.get_mut(&tier) {
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
          match comm_plan_es.state {
            CommonQueryReplanningS::Done(success) => {
              if success {
                *read_es = FullTableReadES::Executing(TableReadES {
                  tier_map: plan_es.tier_map.clone(),
                  timestamp: comm_plan_es.timestamp,
                  context: comm_plan_es.context.clone(),
                  sender_path: comm_plan_es.sender_path.clone(),
                  query_id,
                  query: plan_es.sql_query.clone(),
                  query_plan: comm_plan_es.query_plan.clone(),
                  all_rms: Default::default(),
                  state: ExecutionS::Executing, // TODO: start this properly.
                });
              } else {
                self.read_statuses.remove(&query_id);
              }
            }
            _ => {}
          }
        }
      }
    }
  }
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

impl CommonQueryReplanningES {
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
                    col_usage_tree: msg::ColUsageTree::MSQueryStage(match &self.sql_view {
                      QueryReplanningSqlView::SuperSimpleSelect(select) => {
                        proc::MSQueryStage::SuperSimpleSelect(select.clone())
                      }
                      QueryReplanningSqlView::Update(update) => {
                        proc::MSQueryStage::Update(update.clone())
                      }
                    }),
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
