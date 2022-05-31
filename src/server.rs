use crate::common::{
  lookup_pos, BasicIOCtx, CoreIOCtx, FullGen, GossipData, LeaderMap, TableSchema, Timestamp,
};
use crate::common::{
  CNodePath, CSubNodePath, CTNodePath, CTQueryPath, CTSubNodePath, ColName, ColVal, ColValN,
  ContextRow, ContextSchema, EndpointId, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait,
  QueryId, SlaveGroupId, TNodePath, TSubNodePath, TablePath, TableView, TabletGroupId,
  TransTableName,
};
use crate::expression::{compute_key_region, construct_cexpr, evaluate_c_expr, EvalError};
use crate::message as msg;
use crate::sql_ast::proc;
use crate::sql_ast::proc::SelectClause;
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

  fn leader_map(&self) -> &LeaderMap;
  fn this_gid(&self) -> &PaxosGroupId;
  fn this_eid(&self) -> &EndpointId;

  // Send utilities

  fn send_to_slave_leadership<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    payload: msg::SlaveRemotePayload,
    to_sid: SlaveGroupId,
    to_lid: LeadershipId,
  ) {
    if self.is_leader() {
      // Only send out messages if this node is the Leader. This ensures that
      // followers do not leak out Leadership information of this PaxosGroup.

      let to_gid = to_sid.to_gid();

      let this_gid = self.this_gid().clone();
      let this_lid = self.leader_map().get(&this_gid).unwrap();

      let remote_message = msg::RemoteMessage {
        payload,
        from_lid: this_lid.clone(),
        from_gid: this_gid,
        to_lid: to_lid.clone(),
        to_gid,
      };

      io_ctx.send(
        &to_lid.eid,
        msg::NetworkMessage::Slave(msg::SlaveMessage::RemoteMessage(remote_message)),
      );
    }
  }

  fn send_to_slave_common<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    to_sid: SlaveGroupId,
    payload: msg::SlaveRemotePayload,
  ) {
    let to_lid = self.leader_map().get(&to_sid.to_gid()).unwrap().clone();
    self.send_to_slave_leadership(io_ctx, payload, to_sid, to_lid);
  }

  // send_to_c

  fn send_to_c_lid<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    node_path: CNodePath,
    query: msg::CoordMessage,
    to_lid: LeadershipId,
  ) {
    let CSubNodePath::Coord(cid) = node_path.sub;
    self.send_to_slave_leadership(
      io_ctx,
      msg::SlaveRemotePayload::CoordMessage(cid, query),
      node_path.sid.clone(),
      to_lid,
    );
  }

  fn send_to_c<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    node_path: CNodePath,
    query: msg::CoordMessage,
  ) {
    let CSubNodePath::Coord(cid) = node_path.sub;
    self.send_to_slave_common(
      io_ctx,
      node_path.sid.clone(),
      msg::SlaveRemotePayload::CoordMessage(cid, query),
    );
  }

  // send_to_t

  fn send_to_t_lid<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    node_path: TNodePath,
    query: msg::TabletMessage,
    to_lid: LeadershipId,
  ) {
    let TSubNodePath::Tablet(tid) = node_path.sub;
    self.send_to_slave_leadership(
      io_ctx,
      msg::SlaveRemotePayload::TabletMessage(tid, query),
      node_path.sid.clone(),
      to_lid,
    );
  }

  fn send_to_t<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    node_path: TNodePath,
    query: msg::TabletMessage,
  ) {
    let TSubNodePath::Tablet(tid) = node_path.sub;
    self.send_to_slave_common(
      io_ctx,
      node_path.sid.clone(),
      msg::SlaveRemotePayload::TabletMessage(tid, query),
    );
  }

  // send_to_ct

  fn send_to_ct_lid<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    node_path: CTNodePath,
    query: CommonQuery,
    to_lid: LeadershipId,
  ) {
    self.send_to_slave_leadership(
      io_ctx,
      query.into_remote_payload(node_path.sub),
      node_path.sid.clone(),
      to_lid,
    );
  }

  fn send_to_ct<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    node_path: CTNodePath,
    query: CommonQuery,
  ) {
    self.send_to_slave_common(
      io_ctx,
      node_path.sid.clone(),
      query.into_remote_payload(node_path.sub),
    );
  }

  // send_to_master

  fn send_to_master<IOCtx: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    payload: msg::MasterRemotePayload,
  ) {
    if self.is_leader() {
      // Only send out messages if this node is the Leader. This ensures that
      // followers do not leak out Leadership information of this PaxosGroup.

      let master_gid = PaxosGroupId::Master;
      let master_lid = self.leader_map().get(&master_gid).unwrap().clone();

      let this_gid = self.this_gid().clone();
      let this_lid = self.leader_map().get(&this_gid).unwrap();

      let remote_message = msg::RemoteMessage {
        payload,
        from_lid: this_lid.clone(),
        from_gid: this_gid,
        to_lid: master_lid.clone(),
        to_gid: master_gid,
      };

      io_ctx.send(
        &master_lid.eid,
        msg::NetworkMessage::Master(msg::MasterMessage::RemoteMessage(remote_message)),
      );
    }
  }

  /// Returns true iff this is the Leader.
  fn is_leader(&self) -> bool {
    let lid = self.leader_map().get(self.this_gid()).unwrap();
    &lid.eid == self.this_eid()
  }
}

// -----------------------------------------------------------------------------------------------
//  CT Server Context
// -----------------------------------------------------------------------------------------------

/// This is used to present a consistent view of Tablets and Slave to shared ESs so
/// that they can execute agnotisticly.
pub trait CTServerContext: ServerContextBase {
  fn this_sid(&self) -> &SlaveGroupId;
  fn sub_node_path(&self) -> &CTSubNodePath;
  fn gossip(&self) -> &Arc<GossipData>;

  /// Construct a `NodePath` for a `TabletGroupId`.
  /// NOTE: the `tid` must exist in the `gossip` at this point.
  fn mk_tablet_node_path(&self, tid: TabletGroupId) -> TNodePath {
    let sid = self.gossip().get().tablet_address_config.get(&tid).unwrap();
    TNodePath { sid: sid.clone(), sub: TSubNodePath::Tablet(tid.clone()) }
  }

  /// Make a `CTQueryPath` of an ES at this node with `query_id`.
  fn mk_this_query_path(&self, query_id: QueryId) -> CTQueryPath {
    CTQueryPath {
      node_path: CTNodePath { sid: self.this_sid().clone(), sub: self.sub_node_path().clone() },
      query_id,
    }
  }

  /// This responds to the given `sender_path` with a QueryAborted containing the given
  /// `abort_data`. Here, the `query_id` is that of the ES that's responding.
  fn send_abort_data<IOCtx: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    sender_path: CTQueryPath,
    query_id: QueryId,
    abort_data: msg::AbortedData,
  ) {
    let aborted = msg::QueryAborted {
      return_qid: sender_path.query_id.clone(),
      responder_path: self.mk_this_query_path(query_id),
      payload: abort_data,
    };
    self.send_to_ct(io_ctx, sender_path.node_path, CommonQuery::QueryAborted(aborted));
  }

  /// This responds to the given `sender_path` with a QueryAborted containing the given
  /// `query_error`. Here, the `query_id` is that of the ES that's responding.
  fn send_query_error<IOCtx: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IOCtx,
    sender_path: CTQueryPath,
    query_id: QueryId,
    query_error: msg::QueryError,
  ) {
    self.send_abort_data(io_ctx, sender_path, query_id, msg::AbortedData::QueryError(query_error));
  }

  /// This function computes a minimum set of `TabletGroupId`s whose `TabletKeyRange`
  /// has a non-empty intersect with the KeyRegion we compute from the given `selection`.
  ///
  /// This function returns at least 1 TabletGroupId. This is because if even the `selection`
  /// would be false for all keys, the GRQueryES or MSCoordES needs to know the schema of
  /// the subtables. That can't be determined without contacting a relevant Tablet (who
  /// will perform Column Locking as well to ensure idempotence).
  fn get_min_tablets(
    &self,
    table_path: &TablePath,
    full_gen: &FullGen,
    table_ref: &proc::GeneralSource,
    selection: &proc::ValExpr,
  ) -> Vec<TabletGroupId> {
    // Compute the Row Region that this selection is accessing.
    let (gen, _) = full_gen;
    let table_path_gen = (table_path.clone(), gen.clone());
    let key_cols = &self.gossip().get().db_schema.get(&table_path_gen).unwrap().key_cols;

    // TODO: We use a trivial implementation for now. Do a proper implementation later.
    let _ = compute_key_region(selection, BTreeMap::new(), table_ref, key_cols);
    self.get_all_tablets(table_path, full_gen)
  }

  /// Simply returns all `TabletGroupId`s for a `TablePath` and `Gen`
  fn get_all_tablets(&self, table_path: &TablePath, full_gen: &FullGen) -> Vec<TabletGroupId> {
    let table_path_gen = (table_path.clone(), full_gen.clone());
    let tablet_groups = self.gossip().get().sharding_config.get(&table_path_gen).unwrap();
    tablet_groups.iter().map(|(_, tablet_group_id)| tablet_group_id.clone()).collect()
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

/// This evaluates a SuperSimpleSelect completely. The given `col_refs` and `col_vals` have the
/// same length and correspond. When a ColumnRef is encountered in the `expr`, we search the
/// corresponding `ColValN` in `col_vals` and substitute if for evaluation. In addition,
/// `subquery_vals` should have a length equal to that of how many GRQuerys there are
/// in the `expr`. When a GRQuery is encoutered, we take the corresponding value in
/// `subquery_vals` and substite it in for evaluation.
///
/// Note that `schema` is the Schema of the `LocalTable` that this SELECT is reading.
pub fn evaluate_super_simple_select(
  select: &proc::SuperSimpleSelect,
  schema: &Vec<Option<ColName>>,
  col_refs: &Vec<ExtraColumnRef>,
  col_vals: &Vec<ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<EvaluatedSuperSimpleSelect, EvalError> {
  // We break apart `col_refs` based on if `ExtraColumnRef` is named or not. (Only expressions
  // used named columns. The unnamed columns, referred to by indices, is only used for SELECT *.)
  let mut named_col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
  let mut index_col_map = BTreeMap::<usize, ColValN>::new();
  for (i, col_ref) in col_refs.iter().enumerate() {
    let col_val = col_vals.get(i).unwrap().clone();
    match col_ref {
      ExtraColumnRef::Named(col_name) => {
        named_col_map.insert(col_name.clone(), col_val);
      }
      ExtraColumnRef::Unnamed(index) => {
        index_col_map.insert(*index, col_val);
      }
    }
  }

  // Next, we reduce the subquery values to single values.
  let subquery_vals = extract_subquery_vals(raw_subquery_vals)?;

  // Construct the Evaluated Select
  let mut evaluated_select = EvaluatedSuperSimpleSelect::default();
  let mut next_subquery_idx = 0;
  match &select.projection {
    SelectClause::SelectList(select_list) => {
      for (select_item, _) in select_list {
        let expr = match select_item {
          proc::SelectItem::ValExpr(expr) => expr,
          proc::SelectItem::UnaryAggregate(unary_agg) => &unary_agg.expr,
        };
        let c_expr = construct_cexpr(expr, &named_col_map, &subquery_vals, &mut next_subquery_idx)?;
        evaluated_select.projection.push(evaluate_c_expr(&c_expr)?);
      }
    }
    SelectClause::Wildcard => {
      for (i, maybe_col_name) in schema.iter().enumerate() {
        let col_val = if let Some(col_name) = maybe_col_name {
          named_col_map
            .get(&proc::ColumnRef { table_name: None, col_name: col_name.clone() })
            .unwrap()
            .clone()
        } else {
          index_col_map.get(&i).unwrap().clone()
        };
        evaluated_select.projection.push(col_val);
      }
    }
  }

  evaluated_select.selection = evaluate_c_expr(&construct_cexpr(
    &select.selection,
    &named_col_map,
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

#[derive(Debug, Default)]
pub struct EvaluatedDelete {
  pub selection: ColValN,
}

/// This evaluates a Delete completely. When a ColumnRef is encountered in the `expr`,
/// it searches `col_names` and `col_vals` to get the value. In addition, `subquery_vals` should
/// have a length equal to that of how many GRQuerys there are in the `expr`.
pub fn evaluate_delete(
  delete: &proc::Delete,
  col_names: &Vec<proc::ColumnRef>,
  col_vals: &Vec<ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<EvaluatedDelete, EvalError> {
  // We map all ColNames to their ColValNs using the Context and subtable.
  let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
  for i in 0..col_names.len() {
    col_map.insert(col_names.get(i).unwrap().clone(), col_vals.get(i).unwrap().clone());
  }

  // Next, we reduce the subquery values to single values.
  let subquery_vals = extract_subquery_vals(raw_subquery_vals)?;

  // Construct the Evaluated Select
  let mut evaluated_select = EvaluatedDelete::default();
  let mut next_subquery_idx = 0;
  evaluated_select.selection = evaluate_c_expr(&construct_cexpr(
    &delete.selection,
    &col_map,
    &subquery_vals,
    &mut next_subquery_idx,
  )?)?;

  Ok(evaluated_select)
}

pub fn mk_eval_error(eval_error: EvalError) -> msg::QueryError {
  msg::QueryError::TypeError { msg: format!("{:?}", eval_error) }
}

// -----------------------------------------------------------------------------------------------
//  Tablet Utilities
// -----------------------------------------------------------------------------------------------

/// Computes whether `col` is in `table_schema` at `timestamp`. Note that it must be
/// ensured that `col` is a locked column at this Timestamp.
pub fn strong_contains_col(
  table_schema: &TableSchema,
  col: &ColName,
  timestamp: &Timestamp,
) -> bool {
  lookup_pos(&table_schema.key_cols, col).is_some()
    || table_schema.val_cols.strong_static_read(col, timestamp).is_some()
}

/// Computes whether `col` is in `table_schema` at `timestamp`. Here, the `col` need not be
/// locked at `timestamp`; we just use a `static_read`, which doesn't guarantee idempotence
/// of any sort.
pub fn contains_col(table_schema: &TableSchema, col: &ColName, timestamp: &Timestamp) -> bool {
  lookup_pos(&table_schema.key_cols, col).is_some()
    || contains_val_col(table_schema, col, timestamp)
}

/// Computes whether `col` is in `table_schema.val_cols` at `timestamp`. Here, the `col` need not
/// be locked at `timestamp`; we just use a `static_read`, which doesn't guarantee idempotence
/// of any sort.
pub fn contains_val_col(table_schema: &TableSchema, col: &ColName, timestamp: &Timestamp) -> bool {
  table_schema.val_cols.static_read(col, timestamp).is_some()
}

/// Computes whether `col` is in `table_schema` at the latest time which `col` had been modified
/// in the schema. Obviously, this function is not idempotent.
pub fn contains_col_latest(table_schema: &TableSchema, col: &ColName) -> bool {
  lookup_pos(&table_schema.key_cols, col).is_some()
    || table_schema.val_cols.get_last_version(col).is_some()
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
  /// Here, `local_table` is only used to infer if a column in `child_columns` needs to be read
  /// from the parent `Context`, or from the `local_table`. Importantly, `ContextSchema` of this
  /// new `Context` does not preserve the order of `child_columns`; in particular, the columns
  /// in `child_columns` that are a part of the `local_table` are pulled to the front.
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

  /// This function computes a child `ContextRow` (with a schema of `context_schema`) by simply
  /// extracting the relevant parts of `parent_context_row`.
  ///
  /// Here, `safe_present_col_vals` must have the schema of the `self.safe_present_cols`,
  /// and `parent_context_row` must have the schema of the `parent_context_schema` that
  /// was passed into the constructor.
  fn compute_child_context_row(
    &self,
    parent_context_row: &ContextRow,
    safe_present_col_vals: Vec<ColValN>,
  ) -> ContextRow {
    let mut new_context_row = ContextRow::default();
    new_context_row.column_context_row = safe_present_col_vals;
    for col in &self.external_split {
      let index = self.context_col_index.get(col).unwrap();
      new_context_row
        .column_context_row
        .push(parent_context_row.column_context_row.get(*index).unwrap().clone());
    }

    // Compute the `TransTableContextRow`
    for trans_table_name in &self.trans_table_split {
      let index = self.context_trans_table_index.get(trans_table_name).unwrap();
      let trans_index = parent_context_row.trans_table_context_row.get(*index).unwrap();
      new_context_row.trans_table_context_row.push(*trans_index);
    }
    new_context_row
  }

  /// Extracts the subset of `subtable_row` whose `ColName`s correspond to
  /// `self.safe_present_cols`, and returns it in the order of `self.safe_present_cols`.
  fn extract_safe_present_col_vals(
    &self,
    subtable_schema: &Vec<LocalColumnRef>,
    subtable_row: &Vec<ColValN>,
  ) -> Vec<ColValN> {
    let mut row = Vec::<ColValN>::new();
    for col in &self.safe_present_split {
      let pos = subtable_schema
        .iter()
        .position(|sub_col_ref| match sub_col_ref {
          LocalColumnRef::Named(sub_col) => col == sub_col,
          LocalColumnRef::Unnamed(_) => false,
        })
        .unwrap();
      row.push(subtable_row.get(pos).unwrap().clone());
    }
    row
  }
}

// -----------------------------------------------------------------------------------------------
//  Context Construction
// -----------------------------------------------------------------------------------------------

/// This is an extension to `ColumnRef` that allows the user of `ContextConstructor` to
/// reference columns in the `LocalTable` that do not have name using its position in the schema.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExtraColumnRef {
  Named(proc::ColumnRef),
  Unnamed(usize),
}

impl ExtraColumnRef {
  pub fn into_local(self) -> LocalColumnRef {
    match self {
      ExtraColumnRef::Named(col_name) => LocalColumnRef::Named(col_name.col_name),
      ExtraColumnRef::Unnamed(index) => LocalColumnRef::Unnamed(index),
    }
  }
}

/// This allows the users to optionally read columns in the `LocalTable` using an
/// index to its position in the schema instead.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LocalColumnRef {
  Named(ColName),
  Unnamed(usize),
}

pub trait LocalTable {
  /// The `GeneralSource` used in the query that this `LocalTable` is being used as a Data
  /// Source for. This is needed to interpret whether a `ColumnRef`s is referring to the
  /// Data Source or not.
  fn source(&self) -> &proc::GeneralSource;

  /// Gets the schema of the `LocalTable`.
  fn schema(&self) -> &Vec<Option<ColName>>;

  /// Checks if the given `col` is in the schema of the LocalTable.
  fn contains_col(&self, col: &ColName) -> bool {
    for schema_col in self.schema() {
      if schema_col.as_ref() == Some(col) {
        return true;
      }
    }
    false
  }

  /// Checks if the given `local_col_ref` is in the schema of the LocalTable.
  fn contains_local_col_ref(&self, local_col_ref: &LocalColumnRef) -> bool {
    match local_col_ref {
      LocalColumnRef::Named(col_name) => self.contains_col(col_name),
      LocalColumnRef::Unnamed(index) => index < &self.schema().len(),
    }
  }

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

  /// Checks whether this `ExtraColumnRef` refers to a column in this `LocalTable`.
  fn contains_extra_col_ref(&self, col: &ExtraColumnRef) -> bool {
    match col {
      ExtraColumnRef::Named(col_ref) => self.contains_col_ref(col_ref),
      ExtraColumnRef::Unnamed(index) => index < &self.schema().len(),
    }
  }

  /// Here, every `local_col_refs` must be in `contains_local_col_ref`, and every `ColumnRef`
  /// in `parent_context_schema` must have `contains_col_ref` evaluate to false. This function
  /// gets all rows in the `LocalTable` that is associated with the given parent ContextRow.
  /// Here, the returned rows `Vec<ColValN>` have a schema of `local_col_refs`, and the `u64`
  /// associated with it is the number of times that each row occurred.
  fn get_rows(
    &self,
    parent_context_schema: &ContextSchema,
    parent_context_row: &ContextRow,
    local_col_refs: &Vec<LocalColumnRef>,
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
  /// Here, the `Vec<proc::ColumnRef>` in each child has to either exist in the LocalTable, or
  /// in the `parent_context_schema`. In addition the `Vec<TransTableName>` must exist in the
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

  /// An initial pass that is used to construct the child `Context`s in a deterministic order.
  /// Recall that when `run` executes similar code, the presence of `extra_cols` might change
  /// the order in which child `ContextRow`s are constructed. This is a problem if we want the
  /// child `ContextRow` index (`usize`) to be consisted before sending subqueries, and after
  /// results are received.
  fn create_child_context_row_maps(
    &self,
    parent_context_rows: &Vec<ContextRow>,
  ) -> Vec<BTreeMap<ContextRow, usize>> {
    // Compute the set of all columns that we have to read from the LocalTable, only for
    // context construction.
    let mut local_schema_set = BTreeSet::<LocalColumnRef>::new();
    for conv in &self.converters {
      local_schema_set
        .extend(conv.safe_present_split.iter().map(|c| LocalColumnRef::Named(c.clone())));
    }
    let local_schema = Vec::from_iter(local_schema_set.into_iter());

    // Iterate through all parent ContextRows, compute the local rows, then iterate through those,
    // construct child ContextRows, populate `child_context_row_maps`, then run the callback.
    let mut child_context_row_maps = Vec::<BTreeMap<ContextRow, usize>>::new();
    for _ in 0..self.converters.len() {
      child_context_row_maps.push(BTreeMap::new());
    }

    for parent_context_row in parent_context_rows {
      for (mut local_row, _) in
        self.local_table.get_rows(&self.parent_context_schema, parent_context_row, &local_schema)
      {
        // First, construct the child ContextRows.
        for (index, conv) in self.converters.iter().enumerate() {
          // Compute the child ContextRow for this subquery, and populate
          // `child_context_row_map` accordingly.
          let child_context_row = conv.compute_child_context_row(
            parent_context_row,
            conv.extract_safe_present_col_vals(&local_schema, &local_row),
          );

          let child_context_row_map = child_context_row_maps.get_mut(index).unwrap();
          if !child_context_row_map.contains_key(&child_context_row) {
            let idx = child_context_row_map.len();
            child_context_row_map.insert(child_context_row.clone(), idx);
          }
        }
      }
    }
    child_context_row_maps
  }

  /// Here, the rows in the `parent_context_rows` must correspond to `parent_context_schema`.
  /// The elements in `extra_cols` must be in either `parent_context_schema` or the `LocalTable`.
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
    extra_cols: Vec<ExtraColumnRef>,
    callback: &mut CbT,
  ) -> Result<(), EvalError> {
    // Compute the set of all columns that we have to read from the LocalTable. First,
    // figure out which of the ColNames in `extra_cols` are in the LocalTable, and then,
    // figure the which of the `ColName`s in each child are in the LocalTable.
    let mut local_schema_set = BTreeSet::<LocalColumnRef>::new();
    for col in &extra_cols {
      if self.local_table.contains_extra_col_ref(col) {
        local_schema_set.insert(col.clone().into_local());
      }
    }
    for conv in &self.converters {
      local_schema_set
        .extend(conv.safe_present_split.iter().map(|c| LocalColumnRef::Named(c.clone())));
    }
    let local_schema = Vec::from_iter(local_schema_set.into_iter());

    let child_context_row_maps = self.create_child_context_row_maps(parent_context_rows);
    for (parent_context_row_idx, parent_context_row) in parent_context_rows.iter().enumerate() {
      for (mut local_row, count) in
        self.local_table.get_rows(&self.parent_context_schema, parent_context_row, &local_schema)
      {
        // First, construct the child ContextRows.
        let mut child_context_rows = Vec::<(ContextRow, usize)>::new();
        for (index, conv) in self.converters.iter().enumerate() {
          // Compute the child ContextRow for this subquery, and populate
          // `child_context_rows` accordingly.
          let child_context_row = conv.compute_child_context_row(
            parent_context_row,
            conv.extract_safe_present_col_vals(&local_schema, &local_row),
          );

          // Get the child_context_idx to get the relevent TableView from the subquery
          // results, and populate `subquery_vals`.
          let child_context_row_map = child_context_row_maps.get(index).unwrap();
          let child_context_idx = child_context_row_map.get(&child_context_row).unwrap();
          child_context_rows.push((child_context_row, *child_context_idx));
        }

        // Then, extract values for the `extra_cols` by first searching the `local_schema`,
        // and then the parent Context.
        let mut extra_col_vals = Vec::<ColValN>::new();
        for col in &extra_cols {
          if self.local_table.contains_extra_col_ref(col) {
            let target_local_ref = col.clone().into_local();
            let pos =
              local_schema.iter().position(|local_ref| local_ref == &target_local_ref).unwrap();
            extra_col_vals.push(local_row.get(pos).unwrap().clone());
          } else {
            let pos = self
              .parent_context_schema
              .column_context_schema
              .iter()
              .position(|parent_col| cast!(ExtraColumnRef::Named, col).unwrap() == parent_col)
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
