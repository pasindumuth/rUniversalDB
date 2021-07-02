use crate::common::{GossipData, IOTypes, NetworkOut, OrigP};
use crate::expression::{construct_cexpr, evaluate_c_expr, EvalError};
use crate::model::common::{
  proc, ColName, ColVal, ColValN, EndpointId, QueryId, QueryPath, SlaveGroupId, TablePath,
  TableView, TabletGroupId, TabletKeyRange,
};
use crate::model::message as msg;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ServerContext<'a, T: IOTypes> {
  /// IO Objects.
  pub rand: &'a mut T::RngCoreT,
  pub clock: &'a mut T::ClockT,
  pub network_output: &'a mut T::NetworkOutT,

  /// Metadata
  pub this_slave_group_id: &'a mut SlaveGroupId,
  pub master_eid: &'a mut EndpointId,

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

/// This evaluates a Update completely. When a ColumnRef is encountered
/// in the `expr`, we first search `subtable_row`, and if that's not present, we search the
/// `column_context_row`. In addition, `subquery_vals` should have a length equal to that of
/// how many GRQuerys there are in the `expr`.
pub fn evaluate_update(
  update: &proc::Update,
  column_context_schema: &Vec<ColName>,
  column_context_row: &Vec<ColValN>,
  subtable_schema: &Vec<ColName>,
  subtable_row: &Vec<ColValN>,
  raw_subquery_vals: &Vec<TableView>,
) -> Result<EvaluatedUpdate, EvalError> {
  // We map all ColNames to their ColValNs using the Context and subtable.
  let col_map =
    mk_col_map(column_context_schema, column_context_row, subtable_schema, subtable_row);

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
