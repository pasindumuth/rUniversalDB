use crate::col_usage::{collect_top_level_cols, nodes_external_trans_tables, FrozenColUsageNode};
use crate::common::{lookup_pos, GossipData, IOTypes, KeyBound, NetworkOut, OrigP, TableSchema};
use crate::expression::{compute_key_region, construct_cexpr, evaluate_c_expr, EvalError};
use crate::model::common::{
  proc, ColName, ColType, ColVal, ColValN, ContextRow, ContextSchema, EndpointId, NodeGroupId,
  QueryId, QueryPath, SlaveGroupId, TablePath, TableView, TabletGroupId, TabletKeyRange, Timestamp,
  TransTableName,
};
use crate::model::message as msg;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  Common ServerContext
// -----------------------------------------------------------------------------------------------
// This is used to present a consistent view of Tablets and Slave to shared ESs so
// that they can execute agnotisticly.

pub struct ServerContext<'a, T: IOTypes> {
  /// IO Objects.
  pub rand: &'a mut T::RngCoreT,
  pub clock: &'a mut T::ClockT,
  pub network_output: &'a mut T::NetworkOutT,

  /// Metadata
  pub this_slave_group_id: &'a SlaveGroupId,
  pub maybe_this_tablet_group_id: Option<&'a TabletGroupId>,
  pub master_eid: &'a EndpointId,

  /// Gossip
  pub gossip: &'a mut Arc<GossipData>,

  /// Distribution
  pub sharding_config: &'a mut HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  pub tablet_address_config: &'a mut HashMap<TabletGroupId, SlaveGroupId>,
  pub slave_address_config: &'a mut HashMap<SlaveGroupId, EndpointId>,

  /// Child Queries
  pub master_query_map: &'a mut HashMap<QueryId, OrigP>,
}

impl<'a, T: IOTypes> ServerContext<'a, T> {
  /// This function infers weather the CommonQuery is destined for a Slave or a Tablet
  /// by using the `sender_path`, and then acts accordingly.
  pub fn send_to_path(&mut self, sender_path: QueryPath, common_query: CommonQuery) {
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

  /// This responds to the given `sender_path` with a QueryAborted containing the given
  /// `abort_data`. Here, the `query_id` is that of the ES that's responding.
  pub fn send_abort_data(
    &mut self,
    sender_path: QueryPath,
    query_id: QueryId,
    abort_data: msg::AbortedData,
  ) {
    let aborted = msg::QueryAborted {
      return_path: sender_path.query_id.clone(),
      query_id: query_id.clone(),
      payload: abort_data,
    };
    self.send_to_path(sender_path, CommonQuery::QueryAborted(aborted));
  }

  /// This responds to the given `sender_path` with a QueryAborted containing the given
  /// `query_error`. Here, the `query_id` is that of the ES that's responding.
  pub fn send_query_error(
    &mut self,
    sender_path: QueryPath,
    query_id: QueryId,
    query_error: msg::QueryError,
  ) {
    self.send_abort_data(sender_path, query_id, msg::AbortedData::QueryError(query_error));
  }

  /// This is similar to the above, except uses a `node_group_id`.
  pub fn send_to_node(&mut self, node_group_id: NodeGroupId, common_query: CommonQuery) {
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

  /// This function computes a minimum set of `TabletGroupId`s whose `TabletKeyRange`
  /// has a non-empty intersect with the KeyRegion we compute from the given `selection`.
  pub fn get_min_tablets(
    &self,
    table_path: &TablePath,
    selection: &proc::ValExpr,
  ) -> Vec<TabletGroupId> {
    // Next, we try to reduce the number of TabletGroups we must contact by computing
    // the key_region of TablePath that we're going to be reading.
    let tablet_groups = self.sharding_config.get(table_path).unwrap();
    let key_cols = &self.gossip.gossiped_db_schema.get(table_path).unwrap().key_cols;
    match &compute_key_region(selection, HashMap::new(), key_cols) {
      Ok(_) => {
        // We use a trivial implementation for now, where we just return all TabletGroupIds
        tablet_groups.iter().map(|(_, tablet_group_id)| tablet_group_id.clone()).collect()
      }
      Err(_) => panic!(),
    }
  }

  /// Get the NodeGroupId of this Server.
  pub fn node_group_id(&self) -> NodeGroupId {
    if let Some(tablet_group_id) = self.maybe_this_tablet_group_id {
      NodeGroupId::Tablet(tablet_group_id.clone())
    } else {
      NodeGroupId::Slave(self.this_slave_group_id.clone())
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Common Query
// -----------------------------------------------------------------------------------------------
// These enums are used for communication between algorithms.

/// This is a convenience enum to strealine the sending of PCSA messages to Tablets and Slaves.
pub enum CommonQuery {
  PerformQuery(msg::PerformQuery),
  CancelQuery(msg::CancelQuery),
  QueryAborted(msg::QueryAborted),
  QuerySuccess(msg::QuerySuccess),
}

impl CommonQuery {
  pub fn tablet_msg(self) -> msg::TabletMessage {
    match self {
      CommonQuery::PerformQuery(query) => msg::TabletMessage::PerformQuery(query),
      CommonQuery::CancelQuery(query) => msg::TabletMessage::CancelQuery(query),
      CommonQuery::QueryAborted(query) => msg::TabletMessage::QueryAborted(query),
      CommonQuery::QuerySuccess(query) => msg::TabletMessage::QuerySuccess(query),
    }
  }

  pub fn slave_msg(self) -> msg::SlaveMessage {
    match self {
      CommonQuery::PerformQuery(query) => msg::SlaveMessage::PerformQuery(query),
      CommonQuery::CancelQuery(query) => msg::SlaveMessage::CancelQuery(query),
      CommonQuery::QueryAborted(query) => msg::SlaveMessage::QueryAborted(query),
      CommonQuery::QuerySuccess(query) => msg::SlaveMessage::QuerySuccess(query),
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Query Evaluations
// -----------------------------------------------------------------------------------------------
// These enums are used for communication between algorithms.

/// Maps all `ColName`s to `ColValN`s by first using that in the subtable, and then the context.
pub fn mk_col_map(
  column_context_schema: &Vec<ColName>,
  column_context_row: &Vec<Option<ColVal>>,
  subtable_schema: &Vec<ColName>,
  subtable_row: &Vec<Option<ColVal>>,
) -> HashMap<ColName, ColValN> {
  let mut col_map = HashMap::<ColName, ColValN>::new();
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
  pub selection: ColValN,
}

/// This evaluates a SuperSimpleSelect completely. When a ColumnRef is encountered in the `expr`,
/// it searches `col_names` and `col_vals` to get the value. In addition, `subquery_vals` should
/// have a length equal to that of how many GRQuerys there are in the `expr`.
pub fn evaluate_super_simple_select_2(
  select: &proc::SuperSimpleSelect,
  col_names: &Vec<ColName>,
  col_vals: &Vec<ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<EvaluatedSuperSimpleSelect, EvalError> {
  // We map all ColNames to their ColValNs using the Context and subtable.
  let mut col_map = HashMap::<ColName, ColValN>::new();
  for i in 0..col_names.len() {
    col_map.insert(col_names.get(i).unwrap().clone(), col_vals.get(i).unwrap().clone());
  }

  // Next, we reduce the subquery values to single values.
  let subquery_vals = extract_subquery_vals(raw_subquery_vals)?;

  // Construct the Evaluated Select
  let mut next_subquery_idx = 0;
  Ok(EvaluatedSuperSimpleSelect {
    selection: evaluate_c_expr(&construct_cexpr(
      &select.selection,
      &col_map,
      &subquery_vals,
      &mut next_subquery_idx,
    )?)?,
  })
}

/// This evaluates a SuperSimpleSelect completely. When a ColumnRef is encountered
/// in the `expr`, we first search `subtable_row`, and if that's not present, we search the
/// `column_context_row`. In addition, `subquery_vals` should have a length equal to that of
/// how many GRQuerys there are in the `expr`.
pub fn evaluate_super_simple_select(
  select: &proc::SuperSimpleSelect,
  column_context_schema: &Vec<ColName>,
  column_context_row: &Vec<ColValN>,
  subtable_schema: &Vec<ColName>,
  subtable_row: &Vec<ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<EvaluatedSuperSimpleSelect, EvalError> {
  // We map all ColNames to their ColValNs using the Context and subtable.
  let col_map =
    mk_col_map(column_context_schema, column_context_row, subtable_schema, subtable_row);

  // Next, we reduce the subquery values to single values.
  let subquery_vals = extract_subquery_vals(raw_subquery_vals)?;

  // Construct the Evaluated Select
  let mut next_subquery_idx = 0;
  Ok(EvaluatedSuperSimpleSelect {
    selection: evaluate_c_expr(&construct_cexpr(
      &select.selection,
      &col_map,
      &subquery_vals,
      &mut next_subquery_idx,
    )?)?,
  })
}

#[derive(Debug, Default)]
pub struct EvaluatedUpdate {
  pub assignment: Vec<(ColName, ColValN)>,
  pub selection: ColValN,
}

/// This evaluates a Update completely. When a ColumnRef is encountered in the `expr`,
/// it searches `col_names` and `col_vals` to get the value. In addition, `subquery_vals` should
/// have a length equal to that of how many GRQuerys there are in the `expr`.
pub fn evaluate_update_2(
  update: &proc::Update,
  col_names: &Vec<ColName>,
  col_vals: &Vec<ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<EvaluatedUpdate, EvalError> {
  // We map all ColNames to their ColValNs using the Context and subtable.
  let mut col_map = HashMap::<ColName, ColValN>::new();
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

  return Ok(evaluated_update);
}

pub fn mk_eval_error(eval_error: EvalError) -> msg::QueryError {
  msg::QueryError::TypeError { msg: format!("{:?}", eval_error) }
}

// -----------------------------------------------------------------------------------------------
//  Tablet Utilities
// -----------------------------------------------------------------------------------------------
// These are here solely so other shared code can compile (normally, Tablet related thins
// shouldn't be shared between Tablet and Slave).

/// Computes whether `col` is in `table_schema` at `timestamp`. Note that it must be
/// ensured that `col` is a locked column at this Timestamp.
pub fn contains_col(table_schema: &TableSchema, col: &ColName, timestamp: &Timestamp) -> bool {
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

// -----------------------------------------------------------------------------------------------
//  Context Computation
// -----------------------------------------------------------------------------------------------
// These enums are used for communication between algorithms.

/// This container computes and remembers how to construct a child ContextRow
/// from a parent ContexRow + Table row.
///
/// Importantly, order within `safe_present_split`, `external_split`, and
/// `trans_table_split` aren't guaranteed.
#[derive(Default)]
pub struct ContextConverter {
  // This is the child query's ContextSchema, and it's computed in the constructor.
  pub context_schema: ContextSchema,

  // These fields partition `context_schema` above. However, we split the
  // ColumnContextSchema according to which are in the Table and which are not.
  pub safe_present_split: Vec<ColName>,
  pub external_split: Vec<ColName>,
  pub trans_table_split: Vec<TransTableName>,

  /// This maps the `ColName`s in `external_split` to their positions in the
  /// parent ColumnContextSchema.
  context_col_index: HashMap<ColName, usize>,
  /// This maps the `TransTableName`s in `trans_table_split` to their positions in the
  /// parent TransTableContextSchema.
  context_trans_table_index: HashMap<TransTableName, usize>,
}

impl ContextConverter {
  /// This function is best understood as being a way to compute all data members of
  /// `ContextConverter` for the subquery at `subquery_index` inside the `parent_node`.
  pub fn create_from_query_plan(
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
  pub fn general_create(
    parent_context_schema: &ContextSchema,
    timestamp: &Timestamp,
    table_schema: &TableSchema,
    child_columns: Vec<ColName>,
    child_trans_table_names: Vec<TransTableName>,
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

  /// Here, all ColNames in `child_columns` either appear present in the `trans_table_schema`,
  /// or in the `parent_context_schema`. In addition, the `child_trans_table_names` must be
  /// contained in the `parent_context_schema` as well.
  pub fn trans_general_create(
    parent_context_schema: &ContextSchema,
    trans_table_schema: &Vec<ColName>,
    child_columns: Vec<ColName>,
    child_trans_table_names: Vec<TransTableName>,
  ) -> ContextConverter {
    let mut safe_present_split = Vec::<ColName>::new();
    let mut external_split = Vec::<ColName>::new();

    // Whichever `ColName`s in `child_columns` that are also present in `trans_table_schema`
    // should be added to `safe_present_cols`. Otherwise, they should be added to `external_split`.
    for col in child_columns {
      if trans_table_schema.contains(&col) {
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

  /// This is a unified approach to `general_create` and `trans_general_create` that uses a
  /// `LocalTable` to infer table schema instead.
  pub fn local_table_create<LocalTableT: LocalTable>(
    parent_context_schema: &ContextSchema,
    local_table: &LocalTableT,
    child_columns: Vec<ColName>,
    child_trans_table_names: Vec<TransTableName>,
  ) -> ContextConverter {
    let mut safe_present_split = Vec::<ColName>::new();
    let mut external_split = Vec::<ColName>::new();

    // Whichever `ColName`s in `child_columns` that are also present in `local_table` should
    // be added to `safe_present_cols`. Otherwise, they should be added to `external_split`.
    for col in child_columns {
      if local_table.contains_col(&col) {
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
  pub fn finish_creation(
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
  pub fn compute_child_context_row(
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
  pub fn extract_child_relevent_cols(
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
  pub fn compute_col_context(&self, parent_context_row: &ContextRow) -> HashMap<ColName, ColValN> {
    // First, map all columns in `external_split` to their values in this ContextRow.
    let mut col_context = HashMap::<ColName, ColValN>::new();
    for (col, index) in &self.context_col_index {
      col_context
        .insert(col.clone(), parent_context_row.column_context_row.get(*index).unwrap().clone());
    }
    col_context
  }
}

// -----------------------------------------------------------------------------------------------
//  Context Construction
// -----------------------------------------------------------------------------------------------
// These enums are used for communication between algorithms.

pub trait LocalTable {
  /// Checks if the given `col` is in the schema of the LocalTable.
  fn contains_col(&self, col: &ColName) -> bool;

  /// Gets all rows in the local table that's associated with the given parent ContextRow.
  fn get_rows(
    &self,
    parent_context_schema: &ContextSchema,
    parent_context_row: &ContextRow,
    col_names: &Vec<ColName>,
  ) -> Result<Vec<Vec<ColValN>>, EvalError>;
}

pub struct ContextConstructor<LocalTableT: LocalTable> {
  parent_context_schema: ContextSchema,
  local_table: LocalTableT,
  children: Vec<(Vec<ColName>, Vec<TransTableName>)>,

  /// Here, there is on converter for each element in `children` above.
  converters: Vec<ContextConverter>,
}

impl<LocalTableT: LocalTable> ContextConstructor<LocalTableT> {
  /// Here, the `Vec<ColName>` in each child has to either exist in the LocalTable, or in the
  /// `parent_context_schema`. In addition the `Vec<TransTableName>` must exist in the
  /// `parent_context_schema`.
  pub fn new(
    parent_context_schema: ContextSchema,
    local_table: LocalTableT,
    children: Vec<(Vec<ColName>, Vec<TransTableName>)>,
  ) -> ContextConstructor<LocalTableT> {
    let mut converters = Vec::<ContextConverter>::new();
    for (col_names, trans_table_names) in &children {
      converters.push(ContextConverter::local_table_create(
        &parent_context_schema,
        &local_table,
        col_names.clone(),
        trans_table_names.clone(),
      ));
    }
    ContextConstructor { parent_context_schema, local_table, children, converters }
  }

  /// This gets the schemas for every element in `children` passed in the constructor.
  pub fn get_schemas(&self) -> Vec<ContextSchema> {
    self.converters.iter().map(|conv| conv.context_schema.clone()).collect()
  }

  /// Here, the rows in the `parent_context_rows` must correspond to `parent_context_schema`.
  /// The first `usize` in the `callback` is the parent ContextRow currently being used.
  pub fn run<CbT: FnMut(usize, Vec<ColValN>, Vec<(ContextRow, usize)>) -> Result<(), EvalError>>(
    &self,
    parent_context_rows: &Vec<ContextRow>,
    extra_cols: Vec<ColName>,
    callback: &mut CbT,
  ) -> Result<(), EvalError> {
    // Compute the set of all columns that we have to read from the LocalTable. First,
    // figure out which of the ColNames in `extra_cols` are in the LocalTable, and then,
    // figure the which of the `ColName`s in each child are in the LocalTable.
    let mut local_schema_set = HashSet::<ColName>::new();
    for col in &extra_cols {
      if self.local_table.contains_col(col) {
        local_schema_set.insert(col.clone());
      }
    }
    for conv in &self.converters {
      local_schema_set.extend(conv.safe_present_split.clone());
    }
    let local_schema = Vec::from_iter(local_schema_set.into_iter());

    // Iterate through all parent ContextRows, compute the local rows, then iterate through those,
    // construct child ContextRows, populate `child_context_row_maps`, then run the callback.
    let mut child_context_row_maps = Vec::<HashMap<ContextRow, usize>>::new();
    for _ in 0..self.converters.len() {
      child_context_row_maps.push(HashMap::new());
    }

    for parent_context_row_idx in 0..parent_context_rows.len() {
      let parent_context_row = parent_context_rows.get(parent_context_row_idx).unwrap();
      for local_row in
        self.local_table.get_rows(&self.parent_context_schema, parent_context_row, &local_schema)?
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
        for col in &extra_cols {
          if let Some(pos) = local_schema.iter().position(|local_col| col == local_col) {
            extra_col_vals.push(local_row.get(pos).unwrap().clone());
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
        callback(parent_context_row_idx, extra_col_vals, child_context_rows)?;
      }
    }
    Ok(())
  }
}
