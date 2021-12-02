use crate::common::{lookup_pos, BasicIOCtx, GossipData, TableSchema};
use crate::expression::{compute_key_region, construct_cexpr, evaluate_c_expr, EvalError};
use crate::model::common::{
  proc, CNodePath, CSubNodePath, CTNodePath, CTQueryPath, CTSubNodePath, ColName, ColVal, ColValN,
  ContextRow, ContextSchema, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait,
  QueryId, SlaveGroupId, TNodePath, TSubNodePath, TablePath, TableView, TabletGroupId, Timestamp,
  TransTableName,
};
use crate::model::message as msg;
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  Server Context Base
// -----------------------------------------------------------------------------------------------

/// This is used to present a consistent view of all servers in the system, include
/// the Tablet, Slave, and Master.
pub trait ServerContextBase {
  // Getters

  fn leader_map(&self) -> &BTreeMap<PaxosGroupId, LeadershipId>;
  fn this_gid(&self) -> PaxosGroupId;
  fn this_eid(&self) -> &EndpointId;
  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage);

  // Send utilities

  fn send_to_slave_leadership(
    &mut self,
    payload: msg::SlaveRemotePayload,
    to_sid: SlaveGroupId,
    to_lid: LeadershipId,
  ) {
    if self.is_leader() {
      // Only send out messages if this node is the Leader. This ensures that
      // followers do not leak out Leadership information of this PaxosGroup.

      let to_gid = to_sid.to_gid();

      let this_gid = self.this_gid();
      let this_lid = self.leader_map().get(&this_gid).unwrap();

      let remote_message = msg::RemoteMessage {
        payload,
        from_lid: this_lid.clone(),
        from_gid: this_gid,
        to_lid: to_lid.clone(),
        to_gid,
      };

      self.send(
        &to_lid.eid,
        msg::NetworkMessage::Slave(msg::SlaveMessage::RemoteMessage(remote_message)),
      );
    }
  }

  fn send_to_slave_common(&mut self, to_sid: SlaveGroupId, payload: msg::SlaveRemotePayload) {
    let to_lid = self.leader_map().get(&to_sid.to_gid()).unwrap().clone();
    self.send_to_slave_leadership(payload, to_sid, to_lid);
  }

  // send_to_c

  fn send_to_c_lid(
    &mut self,
    node_path: CNodePath,
    query: msg::CoordMessage,
    to_lid: LeadershipId,
  ) {
    let CSubNodePath::Coord(cid) = node_path.sub;
    self.send_to_slave_leadership(
      msg::SlaveRemotePayload::CoordMessage(cid, query),
      node_path.sid.clone(),
      to_lid,
    );
  }

  fn send_to_c(&mut self, node_path: CNodePath, query: msg::CoordMessage) {
    let CSubNodePath::Coord(cid) = node_path.sub;
    self.send_to_slave_common(
      node_path.sid.clone(),
      msg::SlaveRemotePayload::CoordMessage(cid, query),
    );
  }

  // send_to_t

  fn send_to_t_lid(
    &mut self,
    node_path: TNodePath,
    query: msg::TabletMessage,
    to_lid: LeadershipId,
  ) {
    let TSubNodePath::Tablet(tid) = node_path.sub;
    self.send_to_slave_leadership(
      msg::SlaveRemotePayload::TabletMessage(tid, query),
      node_path.sid.clone(),
      to_lid,
    );
  }

  fn send_to_t(&mut self, node_path: TNodePath, query: msg::TabletMessage) {
    let TSubNodePath::Tablet(tid) = node_path.sub;
    self.send_to_slave_common(
      node_path.sid.clone(),
      msg::SlaveRemotePayload::TabletMessage(tid, query),
    );
  }

  // send_to_ct

  fn send_to_ct_lid(&mut self, node_path: CTNodePath, query: CommonQuery, to_lid: LeadershipId) {
    self.send_to_slave_leadership(
      query.into_remote_payload(node_path.sub),
      node_path.sid.clone(),
      to_lid,
    );
  }

  fn send_to_ct(&mut self, node_path: CTNodePath, query: CommonQuery) {
    self.send_to_slave_common(node_path.sid.clone(), query.into_remote_payload(node_path.sub));
  }

  // send_to_master

  fn send_to_master(&mut self, payload: msg::MasterRemotePayload) {
    if self.is_leader() {
      // Only send out messages if this node is the Leader. This ensures that
      // followers do not leak out Leadership information of this PaxosGroup.

      let master_gid = PaxosGroupId::Master;
      let master_lid = self.leader_map().get(&master_gid).unwrap().clone();

      let this_gid = self.this_gid();
      let this_lid = self.leader_map().get(&this_gid).unwrap();

      let remote_message = msg::RemoteMessage {
        payload,
        from_lid: this_lid.clone(),
        from_gid: this_gid,
        to_lid: master_lid.clone(),
        to_gid: master_gid,
      };

      self.send(
        &master_lid.eid,
        msg::NetworkMessage::Master(msg::MasterMessage::RemoteMessage(remote_message)),
      );
    }
  }

  /// Returns true iff this is the Leader.
  fn is_leader(&self) -> bool {
    let lid = self.leader_map().get(&self.this_gid()).unwrap();
    &lid.eid == self.this_eid()
  }
}

// -----------------------------------------------------------------------------------------------
//  Slave Server Context
// -----------------------------------------------------------------------------------------------

/// This is used to present a consistent view of Tablets and Slave to shared ESs so
/// that they can execute agnotisticly.
pub struct SlaveServerContext<'a, IO: BasicIOCtx> {
  /// IO
  pub io_ctx: &'a mut IO,

  /// Metadata
  pub this_sid: &'a SlaveGroupId,
  pub this_eid: &'a EndpointId,
  pub sub_node_path: &'a CTSubNodePath,

  /// Paxos
  pub leader_map: &'a BTreeMap<PaxosGroupId, LeadershipId>,

  /// Gossip
  pub gossip: &'a Arc<GossipData>,
}

impl<'a, IO: BasicIOCtx> SlaveServerContext<'a, IO> {
  /// Construct a `NodePath` from a `NodeGroupId`.
  /// NOTE: the `tid` must exist in the `gossip` at this point.
  pub fn mk_node_path_from_tablet(&self, tid: TabletGroupId) -> TNodePath {
    let sid = self.gossip.tablet_address_config.get(&tid).unwrap();
    TNodePath { sid: sid.clone(), sub: TSubNodePath::Tablet(tid.clone()) }
  }

  /// Make a `CTQueryPath` of an ES at this node with `query_id`.
  pub fn mk_this_query_path(&self, query_id: QueryId) -> CTQueryPath {
    CTQueryPath {
      node_path: CTNodePath { sid: self.this_sid.clone(), sub: self.sub_node_path.clone() },
      query_id,
    }
  }

  /// This responds to the given `sender_path` with a QueryAborted containing the given
  /// `abort_data`. Here, the `query_id` is that of the ES that's responding.
  pub fn send_abort_data(
    &mut self,
    sender_path: CTQueryPath,
    query_id: QueryId,
    abort_data: msg::AbortedData,
  ) {
    let aborted = msg::QueryAborted {
      return_qid: sender_path.query_id.clone(),
      responder_path: self.mk_this_query_path(query_id),
      payload: abort_data,
    };
    self.send_to_ct(sender_path.node_path, CommonQuery::QueryAborted(aborted));
  }

  /// This responds to the given `sender_path` with a QueryAborted containing the given
  /// `query_error`. Here, the `query_id` is that of the ES that's responding.
  pub fn send_query_error(
    &mut self,
    sender_path: CTQueryPath,
    query_id: QueryId,
    query_error: msg::QueryError,
  ) {
    self.send_abort_data(sender_path, query_id, msg::AbortedData::QueryError(query_error));
  }

  /// This function computes a minimum set of `TabletGroupId`s whose `TabletKeyRange`
  /// has a non-empty intersect with the KeyRegion we compute from the given `selection`.
  ///
  /// This function returns at least 1 TabletGroupId. This is because if even the `selection`
  /// would be false for all keys, the GRQueryES or MSCoordES needs to know the schema of
  /// the subtables. That can't be determined without contacting a relevant Tablet (who
  /// will perform Column Locking as well to ensure idempotence).
  pub fn get_min_tablets(
    &self,
    table_path: &TablePath,
    table_ref: &proc::GeneralSource,
    gen: &Gen,
    selection: &proc::ValExpr,
  ) -> Vec<TabletGroupId> {
    // Compute the Row Region that this selection is accessing.
    let table_path_gen = (table_path.clone(), gen.clone());
    let key_cols = &self.gossip.db_schema.get(&table_path_gen).unwrap().key_cols;

    // TODO: We use a trivial implementation for now. Do a proper implementation later.
    let _ = compute_key_region(selection, BTreeMap::new(), table_ref, key_cols);
    self.get_all_tablets(table_path, gen)
  }

  /// Simply returns all `TabletGroupId`s for a `TablePath` and `Gen`
  pub fn get_all_tablets(&self, table_path: &TablePath, gen: &Gen) -> Vec<TabletGroupId> {
    let table_path_gen = (table_path.clone(), gen.clone());
    let tablet_groups = self.gossip.sharding_config.get(&table_path_gen).unwrap();
    tablet_groups.iter().map(|(_, tablet_group_id)| tablet_group_id.clone()).collect()
  }
}

impl<'a, IO: BasicIOCtx> ServerContextBase for SlaveServerContext<'a, IO> {
  fn leader_map(&self) -> &BTreeMap<PaxosGroupId, LeadershipId> {
    self.leader_map
  }

  /// Gets the `PaxosGroupId` of the Slave.
  fn this_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.this_sid.clone())
  }

  fn this_eid(&self) -> &EndpointId {
    self.this_eid
  }

  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage) {
    self.io_ctx.send(eid, msg);
  }
}

// -----------------------------------------------------------------------------------------------
//  Main Slave Server Context
// -----------------------------------------------------------------------------------------------

/// This is used to easily use the `ServerContextBase` methods in the Slave thread.
pub struct MainSlaveServerContext<'a, IO: BasicIOCtx> {
  /// IO
  pub io_ctx: &'a mut IO,

  /// Metadata
  pub this_sid: &'a SlaveGroupId,
  pub this_eid: &'a EndpointId,

  /// Paxos
  pub leader_map: &'a BTreeMap<PaxosGroupId, LeadershipId>,
}

impl<'a, IO: BasicIOCtx> ServerContextBase for MainSlaveServerContext<'a, IO> {
  fn leader_map(&self) -> &BTreeMap<PaxosGroupId, LeadershipId> {
    self.leader_map
  }

  /// Gets the `PaxosGroupId` of the Slave.
  fn this_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.this_sid.clone())
  }

  fn this_eid(&self) -> &EndpointId {
    self.this_eid
  }

  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage) {
    self.io_ctx.send(eid, msg);
  }
}

// -----------------------------------------------------------------------------------------------
//  Master Server Context
// -----------------------------------------------------------------------------------------------

/// This is used to present a consistent view of Master to shared ESs so that they can
/// execute agnotisticly.
pub struct MasterServerContext<'a, IO> {
  /// IO
  pub io_ctx: &'a mut IO,

  /// Metadata
  pub this_eid: &'a EndpointId,

  /// Paxos
  pub leader_map: &'a BTreeMap<PaxosGroupId, LeadershipId>,
}

impl<'a, IO: BasicIOCtx> ServerContextBase for MasterServerContext<'a, IO> {
  fn leader_map(&self) -> &BTreeMap<PaxosGroupId, LeadershipId> {
    self.leader_map
  }

  /// Gets the `PaxosGroupId` of the Master.
  fn this_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Master
  }

  fn this_eid(&self) -> &EndpointId {
    self.this_eid
  }

  fn send(&mut self, eid: &EndpointId, msg: msg::NetworkMessage) {
    self.io_ctx.send(eid, msg);
  }
}

// -----------------------------------------------------------------------------------------------
//  Common Query
// -----------------------------------------------------------------------------------------------
// These enums are used for communication between algorithms.

/// This is a convenience enum to streamline the sending of PCSA messages to `Tablet`s and `Coord`s.
pub enum CommonQuery {
  PerformQuery(msg::PerformQuery),
  CancelQuery(msg::CancelQuery),
  QueryAborted(msg::QueryAborted),
  QuerySuccess(msg::QuerySuccess),
}

impl CommonQuery {
  pub fn into_tablet_msg(self) -> msg::TabletMessage {
    match self {
      CommonQuery::PerformQuery(query) => msg::TabletMessage::PerformQuery(query),
      CommonQuery::CancelQuery(query) => msg::TabletMessage::CancelQuery(query),
      CommonQuery::QueryAborted(query) => msg::TabletMessage::QueryAborted(query),
      CommonQuery::QuerySuccess(query) => msg::TabletMessage::QuerySuccess(query),
    }
  }

  pub fn into_coord_msg(self) -> msg::CoordMessage {
    match self {
      CommonQuery::PerformQuery(query) => msg::CoordMessage::PerformQuery(query),
      CommonQuery::CancelQuery(query) => msg::CoordMessage::CancelQuery(query),
      CommonQuery::QueryAborted(query) => msg::CoordMessage::QueryAborted(query),
      CommonQuery::QuerySuccess(query) => msg::CoordMessage::QuerySuccess(query),
    }
  }

  pub fn into_remote_payload(self, sub_node_path: CTSubNodePath) -> msg::SlaveRemotePayload {
    match sub_node_path {
      CTSubNodePath::Tablet(tid) => {
        msg::SlaveRemotePayload::TabletMessage(tid.clone(), self.into_tablet_msg())
      }
      CTSubNodePath::Coord(cid) => {
        msg::SlaveRemotePayload::CoordMessage(cid.clone(), self.into_coord_msg())
      }
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Query Evaluations
// -----------------------------------------------------------------------------------------------

/// Maps all `ColName`s to `ColValN`s by first using that in the subtable, and then the context.
pub fn mk_col_map(
  column_context_schema: &Vec<ColName>,
  column_context_row: &Vec<Option<ColVal>>,
  subtable_schema: &Vec<ColName>,
  subtable_row: &Vec<Option<ColVal>>,
) -> BTreeMap<ColName, ColValN> {
  let mut col_map = BTreeMap::<ColName, ColValN>::new();
  assert_eq!(subtable_schema.len(), subtable_row.len());
  for i in 0..subtable_schema.len() {
    let col_name = subtable_schema.get(i).unwrap().clone();
    let col_val = subtable_row.get(i).unwrap().clone();
    col_map.insert(col_name, col_val);
  }

  assert_eq!(column_context_schema.len(), column_context_row.len());
  for i in 0..column_context_schema.len() {
    let col_name = column_context_schema.get(i).unwrap().clone();
    let col_val = column_context_row.get(i).unwrap().clone();
    if !col_map.contains_key(&col_name) {
      // If the ColName was already in the subtable, we don't take the ColValN here.
      col_map.insert(col_name, col_val);
    }
  }
  return col_map;
}

/// Verifies that each `TableView` only contains one cell, and then extract that cell value.
pub fn extract_subquery_vals(
  raw_subquery_vals: &Vec<TableView>,
) -> Result<Vec<ColValN>, EvalError> {
  // Next, we reduce the subquery values to single values.
  let mut subquery_vals = Vec::<ColValN>::new();
  for raw_val in raw_subquery_vals {
    if raw_val.rows.len() != 1 {
      return Err(EvalError::InvalidSubqueryResult);
    }

    // Check that there is one row, and that row has only one column value.
    let (row, count) = raw_val.rows.iter().next().unwrap();
    if row.len() != 1 || count != &1 {
      return Err(EvalError::InvalidSubqueryResult);
    }

    subquery_vals.push(row.get(0).unwrap().clone());
  }
  Ok(subquery_vals)
}

#[derive(Debug, Default)]
pub struct EvaluatedSuperSimpleSelect {
  /// This is the evaluated `expr` in every `proc::SelectItem`, both for aggregate and not.
  pub projection: Vec<ColValN>,
  pub selection: ColValN,
}

/// This evaluates a SuperSimpleSelect completely. When a ColumnRef is encountered in the `expr`,
/// it searches `col_names` and `col_vals` to get the value. In addition, `subquery_vals` should
/// have a length equal to that of how many GRQuerys there are in the `expr`.
pub fn evaluate_super_simple_select(
  select: &proc::SuperSimpleSelect,
  col_names: &Vec<proc::ColumnRef>,
  col_vals: &Vec<ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<EvaluatedSuperSimpleSelect, EvalError> {
  // We map all ColNames to their ColValNs using the Context and subtable.
  let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
  for i in 0..col_names.len() {
    col_map.insert(col_names.get(i).unwrap().clone(), col_vals.get(i).unwrap().clone());
  }

  // Next, we reduce the subquery values to single values.
  let subquery_vals = extract_subquery_vals(raw_subquery_vals)?;

  // Construct the Evaluated Select
  let mut evaluated_select = EvaluatedSuperSimpleSelect::default();
  let mut next_subquery_idx = 0;
  for (select_item, _) in &select.projection {
    let expr = match select_item {
      proc::SelectItem::ValExpr(expr) => expr,
      proc::SelectItem::UnaryAggregate(unary_agg) => &unary_agg.expr,
    };
    let c_expr = construct_cexpr(expr, &col_map, &subquery_vals, &mut next_subquery_idx)?;
    evaluated_select.projection.push(evaluate_c_expr(&c_expr)?);
  }
  evaluated_select.selection = evaluate_c_expr(&construct_cexpr(
    &select.selection,
    &col_map,
    &subquery_vals,
    &mut next_subquery_idx,
  )?)?;

  Ok(evaluated_select)
}

#[derive(Debug, Default)]
pub struct EvaluatedUpdate {
  pub assignment: Vec<(ColName, ColValN)>,
  pub selection: ColValN,
}

/// This evaluates a Update completely. When a ColumnRef is encountered in the `expr`,
/// it searches `col_names` and `col_vals` to get the value. In addition, `subquery_vals` should
/// have a length equal to that of how many GRQuerys there are in the `expr`.
pub fn evaluate_update(
  update: &proc::Update,
  col_names: &Vec<proc::ColumnRef>,
  col_vals: &Vec<ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<EvaluatedUpdate, EvalError> {
  // We map all ColNames to their ColValNs.
  let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
  for i in 0..col_names.len() {
    col_map.insert(col_names.get(i).unwrap().clone(), col_vals.get(i).unwrap().clone());
  }

  // Next, we reduce the subquery values to single values.
  let subquery_vals = extract_subquery_vals(raw_subquery_vals)?;

  // Construct the Evaluated Update
  let mut evaluated_update = EvaluatedUpdate::default();
  let mut next_subquery_idx = 0;
  for (col_name, expr) in &update.assignment {
    let c_expr = construct_cexpr(expr, &col_map, &subquery_vals, &mut next_subquery_idx)?;
    evaluated_update.assignment.push((col_name.clone(), evaluate_c_expr(&c_expr)?));
  }
  evaluated_update.selection = evaluate_c_expr(&construct_cexpr(
    &update.selection,
    &col_map,
    &subquery_vals,
    &mut next_subquery_idx,
  )?)?;

  Ok(evaluated_update)
}

pub fn mk_eval_error(eval_error: EvalError) -> msg::QueryError {
  msg::QueryError::TypeError { msg: format!("{:?}", eval_error) }
}

// -----------------------------------------------------------------------------------------------
//  Tablet Utilities
// -----------------------------------------------------------------------------------------------

/// Computes whether `col` is in `table_schema` at `timestamp`. Note that it must be
/// ensured that `col` is a locked column at this Timestamp.
pub fn contains_col(table_schema: &TableSchema, col: &ColName, timestamp: &Timestamp) -> bool {
  lookup_pos(&table_schema.key_cols, col).is_some()
    || table_schema.val_cols.strong_static_read(col, *timestamp).is_some()
}

/// Computes whether `col` is in `table_schema` at `timestamp`. Here, the `col` need not be
/// locked at `timestamp`; we just use a `static_read`, which doesn't guarantee idempotence
/// of any sort.
pub fn weak_contains_col(table_schema: &TableSchema, col: &ColName, timestamp: &Timestamp) -> bool {
  lookup_pos(&table_schema.key_cols, col).is_some()
    || table_schema.val_cols.static_read(col, *timestamp).is_some()
}

/// Computes whether `col` is in `table_schema` at the latest time which `col` had been modified
/// in the schema. Obviously, this function is not idempotent.
pub fn weak_contains_col_latest(table_schema: &TableSchema, col: &ColName) -> bool {
  lookup_pos(&table_schema.key_cols, col).is_some()
    || table_schema.val_cols.get_last_version(col).is_some()
}

/// Returns true iff the `col` is either a KeyCol in `table_schema`, or a ValCol
/// with high enough `lat`.
pub fn is_col_locked(table_schema: &TableSchema, col: &ColName, timestamp: &Timestamp) -> bool {
  lookup_pos(&table_schema.key_cols, col).is_some()
    || table_schema.val_cols.get_lat(col) >= *timestamp
}

/// Returns true iff one of the columns in `cols` passes `is_col_locked`.
pub fn are_cols_locked(
  table_schema: &TableSchema,
  cols: &Vec<ColName>,
  timestamp: &Timestamp,
) -> bool {
  for col in cols {
    if !is_col_locked(table_schema, col, timestamp) {
      return false;
    }
  }
  return true;
}

// -----------------------------------------------------------------------------------------------
//  Context Computation
// -----------------------------------------------------------------------------------------------

/// This container computes and remembers how to construct a child ContextRow
/// from a parent ContexRow + Table row.
///
/// Importantly, order within `safe_present_split`, `external_split`, and
/// `trans_table_split` aren't guaranteed.
#[derive(Default)]
struct ContextConverter {
  /// This is the child query's ContextSchema, and it is computed in the constructor.
  context_schema: ContextSchema,

  // These fields partition `context_schema` above. However, we split the
  // ColumnContextSchema according to which are in the Table and which are not.
  safe_present_split: Vec<ColName>,
  external_split: Vec<proc::ColumnRef>,
  trans_table_split: Vec<TransTableName>,

  /// This maps the `ColName`s in `external_split` to their positions in the
  /// parent ColumnContextSchema.
  context_col_index: BTreeMap<proc::ColumnRef, usize>,
  /// This maps the `TransTableName`s in `trans_table_split` to their positions in the
  /// parent TransTableContextSchema.
  context_trans_table_index: BTreeMap<TransTableName, usize>,
}

impl ContextConverter {
  /// Sets up and creates a `ContextConverter`. Here, `child_columns` and `child_trans_table_names`
  /// are the column and TransTable that are to be used in the `Context` that is constructed.
  /// Importantly, `ContextSchema` of this new `Context` does not preserve the order of
  /// `child_cols`. Here, `local_table` is only used to infer if a column in `child_columns`
  /// needs to be read from the parent `Context` or not.
  fn create<LocalTableT: LocalTable>(
    parent_context_schema: &ContextSchema,
    local_table: &LocalTableT,
    child_columns: Vec<proc::ColumnRef>,
    child_trans_table_names: Vec<TransTableName>,
  ) -> ContextConverter {
    let mut safe_present_split = Vec::<ColName>::new();
    let mut external_split = Vec::<proc::ColumnRef>::new();
    // Same as `safe_present_split` but we keep the table alias.
    let mut aliased_safe_present_split = Vec::<proc::ColumnRef>::new();

    // Whichever `ColName`s in `child_columns` that are also present in `local_table` should
    // be added to `safe_present_cols`. Otherwise, they should be added to `external_split`.
    for col in child_columns {
      if local_table.contains_col_ref(&col) {
        safe_present_split.push(col.col_name.clone());
        aliased_safe_present_split.push(col);
      } else {
        external_split.push(col);
      }
    }

    let mut conv = ContextConverter::default();
    conv.safe_present_split = safe_present_split;
    conv.external_split = external_split;
    conv.trans_table_split = child_trans_table_names;

    // Point all `conv.external_split` columns to their index in main Query's ContextSchema.
    let col_schema = &parent_context_schema.column_context_schema;
    for (index, col) in col_schema.iter().enumerate() {
      if conv.external_split.contains(col) {
        conv.context_col_index.insert(col.clone(), index);
      }
    }

    // Compute the ColumnContextSchema so that `safe_present_split` is first, and
    // `external_split` is second. This is relevent when we compute ContextRows.
    conv.context_schema.column_context_schema.extend(aliased_safe_present_split);
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
  /// of the `parent_context_schema` that was passed into the constructors). The `ContextRow`
  /// that is constructed has a Schema of `context_schema`.
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
}

// -----------------------------------------------------------------------------------------------
//  Context Construction
// -----------------------------------------------------------------------------------------------

pub trait LocalTable {
  /// Checks if the given `col` is in the schema of the LocalTable.
  fn contains_col(&self, col: &ColName) -> bool;

  /// The `GeneralSource` used in the query that this LocalTable is being used as a Data
  /// Source for. This is needed to interpret `ColumnRef`s as refering to the Data Source or not.
  fn source(&self) -> &proc::GeneralSource;

  /// Checks whether this `ColumnRef` refers to a column in this `LocalTable`, taking the alias
  /// in the `source` into account.
  fn contains_col_ref(&self, col: &proc::ColumnRef) -> bool {
    if let Some(table_name) = &col.table_name {
      if self.source().name() == table_name {
        debug_assert!(self.contains_col(&col.col_name));
        true
      } else {
        false
      }
    } else {
      self.contains_col(&col.col_name)
    }
  }

  /// Here, every `col_names` must be in `contains_col`, and every `ColumnRef` in
  /// `parent_context_schema` must have `contains_col_ref` evaluate to false. This function
  /// get all rows in the local table that's associated with the given parent ContextRow.
  /// Here, we return the value of every row (`Vec<ColValN>`), as well as the count of how
  /// many times that row occurred (this is more performant than returning the same row over
  /// and over again).
  fn get_rows(
    &self,
    parent_context_schema: &ContextSchema,
    parent_context_row: &ContextRow,
    col_names: &Vec<ColName>,
  ) -> Vec<(Vec<ColValN>, u64)>;
}

/// This is used to construct Child Contexts and iterate over them, where it can
/// run a custom callback.
pub struct ContextConstructor<LocalTableT: LocalTable> {
  parent_context_schema: ContextSchema,
  local_table: LocalTableT,

  /// Here, there is on converter for each element in `children` passed into the constructor.
  converters: Vec<ContextConverter>,
}

impl<LocalTableT: LocalTable> ContextConstructor<LocalTableT> {
  /// Here, the `Vec<ColName>` in each child has to either exist in the LocalTable, or in the
  /// `parent_context_schema`. In addition the `Vec<TransTableName>` must exist in the
  /// `parent_context_schema`.
  pub fn new(
    parent_context_schema: ContextSchema,
    local_table: LocalTableT,
    children: Vec<(Vec<proc::ColumnRef>, Vec<TransTableName>)>,
  ) -> ContextConstructor<LocalTableT> {
    let mut converters = Vec::<ContextConverter>::new();
    for (col_names, trans_table_names) in &children {
      converters.push(ContextConverter::create(
        &parent_context_schema,
        &local_table,
        col_names.clone(),
        trans_table_names.clone(),
      ));
    }
    ContextConstructor { parent_context_schema, local_table, converters }
  }

  /// This gets the schemas for every element in `children` passed in the constructor.
  pub fn get_schemas(&self) -> Vec<ContextSchema> {
    self.converters.iter().map(|conv| conv.context_schema.clone()).collect()
  }

  /// Here, the rows in the `parent_context_rows` must correspond to `parent_context_schema`.
  /// The elements in `extra_cols` must be in either `parent_context_schema` or the LocalTable.
  ///
  /// Consider the `callback`. The first `usize` is the parent ContextRow currently being
  /// used. The `Vec<ColValN>` correspond to `extra_cols` passed in here. The `u64` is the
  /// count. (This is an optimization; the alternatively would be for callback can to be called
  /// consecutively multiple times.)
  pub fn run<
    CbT: FnMut(usize, Vec<ColValN>, Vec<(ContextRow, usize)>, u64) -> Result<(), EvalError>,
  >(
    &self,
    parent_context_rows: &Vec<ContextRow>,
    extra_cols: Vec<proc::ColumnRef>,
    callback: &mut CbT,
  ) -> Result<(), EvalError> {
    // Compute the set of all columns that we have to read from the LocalTable. First,
    // figure out which of the ColNames in `extra_cols` are in the LocalTable, and then,
    // figure the which of the `ColName`s in each child are in the LocalTable.
    let mut local_schema_set = BTreeSet::<ColName>::new();
    for col in &extra_cols {
      if self.local_table.contains_col_ref(col) {
        local_schema_set.insert(col.col_name.clone());
      }
    }
    for conv in &self.converters {
      local_schema_set.extend(conv.safe_present_split.clone());
    }
    let local_schema = Vec::from_iter(local_schema_set.into_iter());

    // Iterate through all parent ContextRows, compute the local rows, then iterate through those,
    // construct child ContextRows, populate `child_context_row_maps`, then run the callback.
    let mut child_context_row_maps = Vec::<BTreeMap<ContextRow, usize>>::new();
    for _ in 0..self.converters.len() {
      child_context_row_maps.push(BTreeMap::new());
    }

    for parent_context_row_idx in 0..parent_context_rows.len() {
      let parent_context_row = parent_context_rows.get(parent_context_row_idx).unwrap();
      for (mut local_row, count) in
        self.local_table.get_rows(&self.parent_context_schema, parent_context_row, &local_schema)
      {
        // First, construct the child ContextRows.
        let mut child_context_rows = Vec::<(ContextRow, usize)>::new();
        for index in 0..self.converters.len() {
          // Compute the child ContextRow for this subquery, and populate
          // `child_context_row_map` accordingly.
          let conv = self.converters.get(index).unwrap();
          let child_context_row_map = child_context_row_maps.get_mut(index).unwrap();
          let row = conv.extract_child_relevent_cols(&local_schema, &local_row);
          let child_context_row = conv.compute_child_context_row(parent_context_row, row);
          if !child_context_row_map.contains_key(&child_context_row) {
            let idx = child_context_row_map.len();
            child_context_row_map.insert(child_context_row.clone(), idx);
          }

          // Get the child_context_idx to get the relevent TableView from the subquery
          // results, and populate `subquery_vals`.
          let child_context_idx = child_context_row_map.get(&child_context_row).unwrap();
          child_context_rows.push((child_context_row, *child_context_idx));
        }

        // Then, extract values for the `extra_cols` by first searching the `local_schema`,
        // and then the parent Context.
        let mut extra_col_vals = Vec::<ColValN>::new();
        let mut local_row_it = local_row.into_iter();
        for col in &extra_cols {
          if self.local_table.contains_col_ref(col) {
            extra_col_vals.push(local_row_it.next().unwrap());
          } else {
            let pos = self
              .parent_context_schema
              .column_context_schema
              .iter()
              .position(|parent_col| col == parent_col)
              .unwrap();
            extra_col_vals.push(parent_context_row.column_context_row.get(pos).unwrap().clone());
          }
        }

        // Finally, run the callback with the given inputs.
        callback(parent_context_row_idx, extra_col_vals, child_context_rows, count)?;
      }
    }
    Ok(())
  }
}
