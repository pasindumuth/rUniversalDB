use crate::model::common::iast::SelectClause;
use crate::model::common::proc::SimpleSource;
use crate::model::common::{iast, proc, ColName, TablePath, TransTableName};
use crate::model::message as msg;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;

#[path = "test/query_converter_test.rs"]
pub mod query_converter_test;

pub fn convert_to_msquery(query: iast::Query) -> Result<proc::MSQuery, msg::ExternalAbortedData> {
  // First, we rename all TransTable definitions and references so that they're
  // unique. To do this, we just prepend `tt\n\`, where `n` is integers that we get
  // from a counter. This guarantees uniqueness, since backslashes aren't allowed in
  // Table names.
  let mut ctx =
    RenameContext { trans_table_map: BTreeMap::new(), counter: 0, table_names: BTreeSet::new() };
  let mut renamed_query = query.clone();
  rename_trans_tables_query_r(&mut ctx, &mut renamed_query);

  // Next, we flatten the `renamed_query` to produce an MSQuery. Since we renamed
  // all TransTable references, this won't change the semantics.
  flatten_top_level_query(&renamed_query, &mut ctx.counter)
}

// -----------------------------------------------------------------------------------------------
//  Utilities
// -----------------------------------------------------------------------------------------------

/// Make a unique name for the TransTable
fn unique_name(counter: &mut u32, trans_table_name: &String) -> String {
  *counter += 1;
  format!("tt\\{}\\{}", *counter - 1, trans_table_name)
}

/// Check if a `table_name` is a TransTable, assuming that it would already have been made unique.
fn to_source_ref(table_name: &String) -> proc::GeneralSourceRef {
  if table_name.len() >= 3 && &table_name[..3] == "tt\\" {
    proc::GeneralSourceRef::TransTableName(TransTableName(table_name.clone()))
  } else {
    proc::GeneralSourceRef::TablePath(TablePath(table_name.clone()))
  }
}

// -----------------------------------------------------------------------------------------------
//  TransTable Renaming
// -----------------------------------------------------------------------------------------------

struct RenameContext {
  /// This stays unmutated across a function call.
  trans_table_map: BTreeMap<String, Vec<String>>,
  /// This is incremented.
  counter: u32,
  /// The set of regular table names we detect.
  table_names: BTreeSet<String>,
}

/// This functions renames the TransTables in `query` by prepending 'tt\\n\\',
/// where 'n' is a counter the increments by 1 for every TransTable. It updates updates
/// TransTable usages in `SuperSimpleSelects`, since writes are not allows to use TransTables
/// (which we validate during Flattening later).
///
/// Note: this function leaves the `ctx.trans_table_map` that is passed in unmodified.
fn rename_trans_tables_query_r(ctx: &mut RenameContext, query: &mut iast::Query) {
  let mut trans_table_map = BTreeMap::<String, String>::new(); // Maps new name to old name.
  for (trans_table_name, cte_query) in &mut query.ctes {
    rename_trans_tables_query_r(ctx, cte_query); // Recurse

    // Add the TransTable name and its new name to the context.
    let renamed_trans_table_name = unique_name(&mut ctx.counter, trans_table_name);
    if let Some(rename_stack) = ctx.trans_table_map.get_mut(trans_table_name) {
      rename_stack.push(renamed_trans_table_name.clone());
    } else {
      ctx.trans_table_map.insert(trans_table_name.clone(), vec![renamed_trans_table_name.clone()]);
    }
    trans_table_map.insert(renamed_trans_table_name.clone(), trans_table_name.clone());
    *trans_table_name = renamed_trans_table_name; // Rename the TransTable
  }

  // Then, rename the top-level QueryBody.
  match &mut query.body {
    iast::QueryBody::Query(child_query) => {
      rename_trans_tables_query_r(ctx, child_query);
    }
    iast::QueryBody::SuperSimpleSelect(select) => {
      // Rename the Table to select if it's a TransTable.
      if let Some(rename_stack) = ctx.trans_table_map.get_mut(&select.from.source_ref) {
        if select.from.alias.is_none() {
          // To avoid having to rename ColumnRefs that have a `table_ref` present that
          // refers to this Data Source, we add a table alias with the original name.
          select.from.alias = Some(select.from.source_ref.clone());
        }
        select.from.source_ref = rename_stack.last().unwrap().clone();
      }

      // Rename the Projection Clause
      match &mut select.projection {
        SelectClause::SelectList(select_list) => {
          for (select_item, _) in select_list {
            match select_item {
              iast::SelectItem::ValExpr(val_expr) => {
                rename_trans_tables_val_expr_r(ctx, val_expr);
              }
              iast::SelectItem::UnaryAggregate(unary_agg) => {
                rename_trans_tables_val_expr_r(ctx, &mut unary_agg.expr);
              }
            }
          }
        }
        SelectClause::Wildcard => {}
      }

      // Rename the Where Clause
      rename_trans_tables_val_expr_r(ctx, &mut select.selection);
    }
    iast::QueryBody::Update(update) => {
      // Rename the Set Clause
      for (_, val_expr) in &mut update.assignments {
        rename_trans_tables_val_expr_r(ctx, val_expr);
      }

      // Rename the Where Clause
      rename_trans_tables_val_expr_r(ctx, &mut update.selection);
    }
    iast::QueryBody::Insert(_) => {}
    iast::QueryBody::Delete(delete) => {
      // Rename the Where Clause
      rename_trans_tables_val_expr_r(ctx, &mut delete.selection);
    }
  }

  // Remove the TransTables defined by this Query from the ctx.
  for (trans_table_name, _) in &query.ctes {
    let orig_name = trans_table_map.get(trans_table_name).unwrap();
    let rename_stack = ctx.trans_table_map.get_mut(orig_name).unwrap();
    rename_stack.pop();
    if rename_stack.is_empty() {
      ctx.trans_table_map.remove(trans_table_name);
    }
  }
}

fn rename_trans_tables_val_expr_r(ctx: &mut RenameContext, val_expr: &mut iast::ValExpr) {
  match val_expr {
    iast::ValExpr::ColumnRef { .. } => {} // Nothing to rename
    iast::ValExpr::UnaryExpr { expr, .. } => {
      rename_trans_tables_val_expr_r(ctx, expr);
    }
    iast::ValExpr::BinaryExpr { left, right, .. } => {
      rename_trans_tables_val_expr_r(ctx, left);
      rename_trans_tables_val_expr_r(ctx, right);
    }
    iast::ValExpr::Value { .. } => {} // Nothing to rename
    iast::ValExpr::Subquery { query } => {
      rename_trans_tables_query_r(ctx, query);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Query to MSQuery
// -----------------------------------------------------------------------------------------------

/// Flattens the `query` into a into a `MSQuery`.
fn flatten_top_level_query(
  query: &iast::Query,
  counter: &mut u32,
) -> Result<proc::MSQuery, msg::ExternalAbortedData> {
  let aux_table_name = unique_name(counter, &"".to_string());
  let mut ms_query = proc::MSQuery {
    trans_tables: Vec::default(),
    returning: TransTableName(aux_table_name.clone()),
  };
  flatten_top_level_query_r(&aux_table_name, query, counter, &mut ms_query.trans_tables)?;
  Ok(ms_query)
}

/// Flattens the `query` into a into a `trans_table_map`. For the TableView
/// produced by the query itself, create an auxiliary TransTable with the
/// name of `assignment_name` and add it into the map as well.
/// Note: we need `counter` because we need to create auxiliary TransTables
/// for the query bodies.
fn flatten_top_level_query_r(
  assignment_name: &String,
  query: &iast::Query,
  counter: &mut u32,
  trans_table_map: &mut Vec<(TransTableName, proc::MSQueryStage)>,
) -> Result<(), msg::ExternalAbortedData> {
  // First, have the CTEs flatten their Querys and add their TransTables to the map.
  for (trans_table_name, cte_query) in &query.ctes {
    flatten_top_level_query_r(trans_table_name, cte_query, counter, trans_table_map)?;
  }

  // Then, add this QueryBody as a TransTable
  match &query.body {
    iast::QueryBody::Query(child_query) => {
      flatten_top_level_query_r(assignment_name, child_query, counter, trans_table_map)
    }
    iast::QueryBody::SuperSimpleSelect(select) => {
      trans_table_map.push((
        TransTableName(assignment_name.clone()),
        proc::MSQueryStage::SuperSimpleSelect(flatten_select(select, counter)?),
      ));
      Ok(())
    }
    iast::QueryBody::Update(update) => {
      let mut ms_update = proc::Update {
        table: SimpleSource {
          source_ref: TablePath(update.table.source_ref.clone()),
          alias: update.table.alias.clone(),
        },
        assignment: Vec::new(),
        selection: flatten_val_expr_r(&update.selection, counter)?,
      };
      for (col_name, val_expr) in &update.assignments {
        ms_update
          .assignment
          .push((ColName(col_name.clone()), flatten_val_expr_r(val_expr, counter)?))
      }
      trans_table_map
        .push((TransTableName(assignment_name.clone()), proc::MSQueryStage::Update(ms_update)));
      Ok(())
    }
    iast::QueryBody::Insert(insert) => {
      let mut ms_insert = proc::Insert {
        table: SimpleSource {
          source_ref: TablePath(insert.table.source_ref.clone()),
          alias: insert.table.alias.clone(),
        },
        columns: insert.columns.iter().map(|x| ColName(x.clone())).collect(),
        values: Vec::new(),
      };
      for row in &insert.values {
        let mut p_row = Vec::<proc::ValExpr>::new();
        for val_expr in row {
          p_row.push(flatten_val_expr_r(val_expr, counter)?);
        }
        ms_insert.values.push(p_row);
      }

      trans_table_map
        .push((TransTableName(assignment_name.clone()), proc::MSQueryStage::Insert(ms_insert)));
      Ok(())
    }
    iast::QueryBody::Delete(delete) => {
      let ms_delete = proc::Delete {
        table: SimpleSource {
          source_ref: TablePath(delete.table.source_ref.clone()),
          alias: delete.table.alias.clone(),
        },
        selection: flatten_val_expr_r(&delete.selection, counter)?,
      };
      trans_table_map
        .push((TransTableName(assignment_name.clone()), proc::MSQueryStage::Delete(ms_delete)));
      Ok(())
    }
  }
}

pub fn flatten_val_expr_r(
  val_expr: &iast::ValExpr,
  counter: &mut u32,
) -> Result<proc::ValExpr, msg::ExternalAbortedData> {
  match val_expr {
    iast::ValExpr::ColumnRef { table_name, col_name } => {
      Ok(proc::ValExpr::ColumnRef(proc::ColumnRef {
        table_name: table_name.clone(),
        col_name: ColName(col_name.clone()),
      }))
    }
    iast::ValExpr::UnaryExpr { op, expr } => Ok(proc::ValExpr::UnaryExpr {
      op: op.clone(),
      expr: Box::new(flatten_val_expr_r(expr, counter)?),
    }),
    iast::ValExpr::BinaryExpr { op, left, right } => Ok(proc::ValExpr::BinaryExpr {
      op: op.clone(),
      left: Box::new(flatten_val_expr_r(left, counter)?),
      right: Box::new(flatten_val_expr_r(right, counter)?),
    }),
    iast::ValExpr::Value { val } => Ok(proc::ValExpr::Value { val: val.clone() }),
    iast::ValExpr::Subquery { query } => {
      // Notice that we don't actually need anything after the backslash in the
      // new TransTable name. We only keep it for the original TransTables for
      // debugging purposes.
      let aux_table_name = unique_name(counter, &"".to_string());
      let mut gr_query = proc::GRQuery {
        trans_tables: Vec::default(),
        returning: TransTableName(aux_table_name.clone()),
      };
      flatten_sub_query_r(&aux_table_name, &query, counter, &mut gr_query.trans_tables)?;
      Ok(proc::ValExpr::Subquery { query: Box::from(gr_query) })
    }
  }
}

fn flatten_sub_query_r(
  assignment_name: &String,
  query: &iast::Query,
  counter: &mut u32,
  trans_table_map: &mut Vec<(TransTableName, proc::GRQueryStage)>,
) -> Result<(), msg::ExternalAbortedData> {
  // First, have the CTEs flatten their Querys and add their TransTables to the map.
  for (trans_table_name, cte_query) in &query.ctes {
    flatten_sub_query_r(trans_table_name, cte_query, counter, trans_table_map)?;
  }

  // Then, add this QueryBody as a TransTable
  match &query.body {
    iast::QueryBody::Query(child_query) => {
      flatten_sub_query_r(assignment_name, child_query, counter, trans_table_map)
    }
    iast::QueryBody::SuperSimpleSelect(select) => {
      trans_table_map.push((
        TransTableName(assignment_name.clone()),
        proc::GRQueryStage::SuperSimpleSelect(flatten_select(select, counter)?),
      ));
      Ok(())
    }
    iast::QueryBody::Update(_) => {
      Err(msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidUpdate))
    }
    iast::QueryBody::Insert(_) => {
      Err(msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidInsert))
    }
    iast::QueryBody::Delete(_) => {
      Err(msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidDelete))
    }
  }
}

fn flatten_select(
  select: &iast::SuperSimpleSelect,
  counter: &mut u32,
) -> Result<proc::SuperSimpleSelect, msg::ExternalAbortedData> {
  let p_projection = match &select.projection {
    iast::SelectClause::SelectList(select_list) => {
      let mut p_select_list = Vec::<(proc::SelectItem, Option<ColName>)>::new();
      for (item, alias) in select_list {
        let select_item = match item {
          iast::SelectItem::ValExpr(val_expr) => {
            proc::SelectItem::ValExpr(flatten_val_expr_r(val_expr, counter)?)
          }
          iast::SelectItem::UnaryAggregate(unary_agg) => {
            proc::SelectItem::UnaryAggregate(proc::UnaryAggregate {
              distinct: unary_agg.distinct,
              op: unary_agg.op.clone(),
              expr: flatten_val_expr_r(&unary_agg.expr, counter)?,
            })
          }
        };
        p_select_list.push((select_item, alias.clone().map(|x| ColName(x))))
      }
      proc::SelectClause::SelectList(p_select_list)
    }
    iast::SelectClause::Wildcard => proc::SelectClause::Wildcard,
  };

  Ok(proc::SuperSimpleSelect {
    distinct: select.distinct,
    projection: p_projection,
    from: proc::GeneralSource {
      source_ref: to_source_ref(&select.from.source_ref),
      alias: select.from.alias.clone(),
    },
    selection: flatten_val_expr_r(&select.selection, counter)?,
  })
}
