use crate::common::rand::RandGen;
use crate::model::common::{
  ColumnName, ColumnValue, PrimaryKey, SelectQueryId, SelectView, Timestamp, TransactionId,
  WriteDiff,
};
use crate::model::evalast::{
  EvalBinaryOp, EvalLiteral, Holder, PostEvalExpr, PreEvalExpr, SelectKeyTask, SelectQueryTask,
  UpdateKeyEvalValTask, UpdateKeyNoneTask, UpdateKeyTask, WriteQueryTask,
};
use crate::model::message::{FromSelectTask, FromWriteTask};
use crate::model::sqlast::{BinaryOp, Literal, SelectStmt, UpdateStmt, ValExpr};
use crate::storage::relational_tablet::RelationalTablet;
use rand::Rng;
use std::collections::BTreeMap;

/// Errors that can occur when evaluating columns while transforming
/// an AST into an EvalAST.
#[derive(Debug)]
pub enum EvalErr {
  ColumnDNE,
  TypeError,
  TableConstraintBroken,
}

/// We have Tasks, and then we have Task Enums. Tasks are structs, and Task
/// Enums are Enums where each Variant is a single Tuple Struct that wraps
/// a Task Type. Every Task will appear as a variant in some Task Enum.
/// Every Task Enum will have their own `eval` function, which
/// returns the Task Type (perhaps the same as the input) that we should
/// transition to, and potentially some subqueries that needs to be sent
/// out.
///
/// A Task will generally have some sub-Tasks and maybe a `pending_queries`
/// list of it's own. These sub-Tasks will be defined in the Task Type via
/// through Task Enums, and it will be held at some relative path
/// away from the Task (like in `UpdateTask`, where we have one
/// `UpdateKeyTasks` located at every `PrimaryKey`). A task is
/// finished when it's sub-Tasks are finished and all expressions
/// have been evaluated.
///
/// We construct WriteQueryTasks from the bottom up.

/// This function looks at the `UpdateKeyTask`'s own context and tries
/// to evaluate it as far as possible.
pub fn eval_update_key_task(
  rand_gen: &mut RandGen,
  update_key_task: &mut Holder<UpdateKeyTask>,
  sub_sid: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, SelectStmt>, EvalErr> {
  match &mut update_key_task.val {
    UpdateKeyTask::Start(task) => {
      panic!("hi")
    }
    UpdateKeyTask::EvalWhere(task) => {
      if let Some(_) = task.pending_subqueries.remove(sub_sid) {
        // The subquery is indeed relevent to this task.
        task
          .complete_subqueries
          .insert(sub_sid.clone(), subquery_ret.clone());
        if task.pending_subqueries.is_empty() {
          // This means we are finished waiting for subqueries to arrive;
          // we may start evaluating the expressions.
          match eval_expr(&task.complete_subqueries, &task.where_clause)? {
            EvalLiteral::Bool(where_val) => {
              if where_val {
                let (post_expr, subqueries) = replace_sub(rand_gen, &task.set_val);
                update_key_task.val = UpdateKeyTask::EvalVal(UpdateKeyEvalValTask {
                  set_col: task.set_col.clone(),
                  set_val: post_expr,
                  table_constraints: task.table_constraints.clone(),
                  pending_subqueries: subqueries.clone(),
                  complete_subqueries: Default::default(),
                });
                return Ok(subqueries);
              } else {
                update_key_task.val = UpdateKeyTask::None(UpdateKeyNoneTask {});
                return Ok(BTreeMap::new());
              }
            }
            _ => return Err(EvalErr::TypeError),
          }
        }
      }
      return Ok(BTreeMap::new());
    }
    UpdateKeyTask::EvalVal(_) => {
      // Do nothing.
      Ok(BTreeMap::new())
    }
    UpdateKeyTask::EvalConstraints(_) => {
      // Do nothing.
      Ok(BTreeMap::new())
    }
    UpdateKeyTask::None(_) => {
      // Do nothing.
      Ok(BTreeMap::new())
    }
    UpdateKeyTask::Done(_) => {
      // Do nothing.
      Ok(BTreeMap::new())
    }
  }
}

fn eval_expr(
  subquery_vals: &BTreeMap<SelectQueryId, SelectView>,
  expr: &PostEvalExpr,
) -> Result<EvalLiteral, EvalErr> {
  panic!("TODO: implement");
}

fn replace_sub(
  rand_gen: &mut RandGen,
  pre_expr: &PreEvalExpr,
) -> (PostEvalExpr, BTreeMap<SelectQueryId, SelectStmt>) {
  panic!("Implement");
}

/// This function adds the adds the `subquery` at the location of
/// `path`, and then executes things as far as they will go. If
/// we realize the `WriteQueryTask` cannot be finished (due to
/// type errors, runtime errors (dividing by zero) or broken
/// table constriants), then we return an Err instead.
pub fn eval_write_graph(
  rand_gen: &mut RandGen,
  write_query_task: &mut Holder<WriteQueryTask>,
  path: &FromWriteTask,
  sub_sid: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, (FromWriteTask, SelectStmt)>, EvalErr> {
  match (&path, &write_query_task.val) {
    (FromWriteTask::UpdateTask { key }, WriteQueryTask::UpdateTask(task)) => {
      panic!(
        "TODO: impelment. Fairly easy; just forward it to the eval_update_key_task,\
      and if all eval_update_key_tasks are done, then move `write_query_task` to done."
      )
    }
    (FromWriteTask::InsertTask { key }, WriteQueryTask::InsertTask(task)) => {
      panic!("TODO: implement")
    }
    (FromWriteTask::InsertSelectTask, WriteQueryTask::InsertSelectTask(task)) => {
      panic!("TODO: implement")
    }
    _ => {
      // Ignore all other combination of `path` and `write_query_task`;
      // those shouldn't result in any transitions.
      Ok(BTreeMap::new())
    }
  }
}

/// I hypothesize this will be the master interface for
/// evaluating EvalSelect.
pub fn eval_select_graph(
  rand_gen: &mut RandGen,
  select_task: &mut Holder<SelectQueryTask>,
  path: &FromSelectTask,
  sub_sid: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, (FromSelectTask, SelectStmt)>, EvalErr> {
  match &select_task.val {
    SelectQueryTask::SelectTask(task) => panic!("TODO: implement"),
    SelectQueryTask::SelectDoneTask(task) => panic!("TODO: implement"),
  };
  Err(EvalErr::ColumnDNE)
}

fn lookup<K: PartialEq + Eq, V: Clone>(vec: &Vec<(K, V)>, key: &K) -> Option<V> {
  for (k, v) in vec {
    if k == key {
      return Some(v.clone());
    }
  }
  return None;
}

pub fn table_insert(
  rel_tab: &mut RelationalTablet,
  key: &PrimaryKey,
  col_name: &ColumnName,
  val: EvalLiteral,
  timestamp: &Timestamp,
) {
  panic!("TODO: implement")
}

pub fn table_insert_diff(rel_tab: &mut RelationalTablet, diff: &WriteDiff, timestamp: &Timestamp) {
  panic!("TODO: implement")
}

/// This function takes all ColumnNames in the `update_stmt`, replaces
/// them with their values from `rel_tab` (from the row with key `key`
/// at `timestamp`). We do this deeply, including subqueries. Then
/// we construct EvalUpdateStmt with all subqueries in tact, i.e. they
/// aren't turned into SubqueryIds yet.
pub fn start_eval_update_key_task(
  rand_gen: &mut RandGen,
  update_stmt: UpdateStmt,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<(UpdateKeyTask, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  panic!(
    "TODO: implement. This will likely use eval_update_key_task to help\
  move the UpdateKeyTask as far forward as possible. Also, we should return the\
  FromTask paths for each subquery."
  );
}

/// This function takes all ColumnNames in the `update_stmt`, replaces
/// them with their values from `rel_tab` (from the row with key `key`
/// at `timestamp`). We do this deeply, including subqueries. Then
/// we construct EvalUpdateStmt with all subqueries in tact, i.e. they
/// aren't turned into SubqueryIds yet.
pub fn start_eval_select_key_task(
  rand_gen: &mut RandGen,
  select_stmt: SelectStmt,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<(SelectKeyTask, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  panic!(
    "TODO: implement. This will likely use eval_update_key_task to help\
  move the UpdateKeyTask as far forward as possible."
  );
}

/// This function evaluates the `EvalExpr`. This function replaces any `Subquery`
/// variants with a `SubqueryId` by sending out the actual subquery for execution.
/// It returns either another `EvalExpr` if we still need to wait for Subqueries
/// to execute, or a `Literal` if the valuation of `expr` is complete.
///
/// This adds whatever subqueries are necessary to evaluate the subqueries.
/// This should be errors. type errors.
fn evaluate_expr(
  context: &Vec<(SelectQueryId, EvalLiteral)>,
  expr: PostEvalExpr,
) -> Result<EvalLiteral, EvalErr> {
  match expr {
    PostEvalExpr::BinaryExpr { op, lhs, rhs } => {
      let elhs = evaluate_expr(context, *lhs)?;
      let erhs = evaluate_expr(context, *rhs)?;
      match op {
        EvalBinaryOp::AND => panic!("TODO: implement."),
        EvalBinaryOp::OR => panic!("TODO: implement."),
        EvalBinaryOp::LT => panic!("TODO: implement."),
        EvalBinaryOp::LTE => panic!("TODO: implement."),
        EvalBinaryOp::E => panic!("TODO: implement."),
        EvalBinaryOp::GT => panic!("TODO: implement."),
        EvalBinaryOp::GTE => panic!("TODO: implement."),
        EvalBinaryOp::PLUS => panic!("TODO: implement."),
        EvalBinaryOp::TIMES => panic!("TODO: implement."),
        EvalBinaryOp::MINUS => panic!("TODO: implement."),
        EvalBinaryOp::DIV => panic!("TODO: implement."),
      }
    }
    PostEvalExpr::Literal(literal) => Ok(literal),
    PostEvalExpr::SubqueryId(subquery_id) => {
      // The subquery_id here must certainly be in the `context`,
      // since this function can only be called when all subqueries
      // have been answered. Reach into and it and return it.
      if let Some(val) = lookup(context, &subquery_id) {
        Ok(val.clone())
      } else {
        panic!("subquery_id {:?} must exist in {:?}", subquery_id, context)
      }
    }
  }
}

/// This Transforms the `expr` into a EvalExpr. It replaces all Subqueries with
/// Box<SelectStmt>, where if a column in the main query appears in the subquery,
/// we replace it with the evaluated value of the column. We also replace a column
/// name with the valuated value of the column in the main query as well.
///
/// (Don't worry if replacing the column names is wrong. It might be, if SQL binds
/// column names tighter to inner selects. Or SQL can throw an error. But I don't
/// need that level of foresight here. Maybe we can have a super high-level pass
/// that does verification later.)
fn evaluate_columns(
  rand_gen: &mut RandGen,
  expr: ValExpr,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<(PostEvalExpr, Vec<(SelectQueryId, SelectStmt)>), EvalErr> {
  /// Recursive function here.
  fn evaluate_columns_r(
    rand_gen: &mut RandGen,
    subqueries: &mut Vec<(SelectQueryId, SelectStmt)>,
    expr: ValExpr,
    rel_tab: &RelationalTablet,
    key: &PrimaryKey,
    timestamp: &Timestamp,
  ) -> Result<PostEvalExpr, EvalErr> {
    match expr {
      ValExpr::Literal(literal) => Ok(PostEvalExpr::Literal(convert_literal(&literal))),
      ValExpr::BinaryExpr { op, lhs, rhs } => Ok(PostEvalExpr::BinaryExpr {
        op: convert_op(&op),
        lhs: Box::new(evaluate_columns_r(
          rand_gen, subqueries, *lhs, rel_tab, key, timestamp,
        )?),
        rhs: Box::new(evaluate_columns_r(
          rand_gen, subqueries, *rhs, rel_tab, key, timestamp,
        )?),
      }),
      ValExpr::Column(col) => {
        let col_name = ColumnName(col.clone());
        if let Some(_) = rel_tab.col_name_exists(&col_name) {
          // If the column name actually exists in the current tablet's
          // schema, we replace the column name with the column value.
          Ok(PostEvalExpr::Literal(
            match rel_tab.get_partial_val(key, &col_name, timestamp) {
              Some(ColumnValue::Int(int)) => EvalLiteral::Int(int),
              Some(ColumnValue::String(string)) => EvalLiteral::String(string.to_string()),
              Some(ColumnValue::Bool(boolean)) => EvalLiteral::Bool(boolean),
              Some(ColumnValue::Unit) => {
                panic!("The Unit ColumnValue should never appear for a cell in the table.")
              }
              None => EvalLiteral::Null,
            },
          ))
        } else {
          // If the column name doesn't exist in the schema, then this query
          // is not valid and we propagate up an error message.
          Err(EvalErr::ColumnDNE)
        }
      }
      ValExpr::Subquery(subquery) => {
        // Create a random SubqueryId
        let mut bytes: [u8; 8] = [0; 8];
        rand_gen.rng.fill(&mut bytes);
        let subquery_id = SelectQueryId(TransactionId(bytes));
        // Add the Subquery to the `subqueries` vector.
        subqueries.push((
          subquery_id.clone(),
          SelectStmt {
            col_names: subquery.col_names.clone(),
            table_name: subquery.table_name.clone(),
            where_clause: replace_column(key, timestamp, rel_tab, &subquery.where_clause),
          },
        ));
        // The PostEvalExpr thats should replace the ValExpr::Subquery is simply
        // the subqueryId that was sent out to the clients.
        Ok(PostEvalExpr::SubqueryId(subquery_id))
      }
    }
  }

  let mut subqueries = Vec::new();
  let eval_expr = evaluate_columns_r(rand_gen, &mut subqueries, expr, rel_tab, key, timestamp)?;
  return Ok((eval_expr, subqueries));
}

/// Suppose `col_name` is a column name in the schema of `rel_tab`. This
/// function finds all instances of `col_name` in `expr` and replaces it
/// with the value of that column in `rel_tab` for the row keyed by `key`.
fn replace_column(
  key: &PrimaryKey,
  timestamp: &Timestamp,
  rel_tab: &RelationalTablet,
  expr: &ValExpr,
) -> ValExpr {
  match expr {
    ValExpr::BinaryExpr { op, lhs, rhs } => ValExpr::BinaryExpr {
      op: op.clone(),
      lhs: Box::new(replace_column(key, timestamp, rel_tab, lhs)),
      rhs: Box::new(replace_column(key, timestamp, rel_tab, rhs)),
    },
    ValExpr::Literal(literal) => ValExpr::Literal(literal.clone()),
    ValExpr::Column(col) => {
      let col_name = ColumnName(col.clone());
      if let Some(_) = rel_tab.col_name_exists(&col_name) {
        // If the column name exists in the current tablet's schema,
        // we replace the column name with the column value.
        ValExpr::Literal(match rel_tab.get_partial_val(key, &col_name, timestamp) {
          Some(ColumnValue::Int(int)) => Literal::Int(int.to_string()),
          Some(ColumnValue::String(string)) => Literal::String(string.to_string()),
          Some(ColumnValue::Bool(boolean)) => Literal::Bool(boolean),
          Some(ColumnValue::Unit) => {
            panic!("The Unit ColumnValue should never appear for a cell in the table.")
          }
          None => Literal::Null,
        })
      } else {
        // If the column name doesn't refer to a column in the
        // main query, then leave it alone.
        ValExpr::Column(col.clone())
      }
    }
    ValExpr::Subquery(subquery) => ValExpr::Subquery(Box::from(SelectStmt {
      col_names: subquery.col_names.clone(),
      table_name: subquery.table_name.clone(),
      where_clause: replace_column(key, timestamp, rel_tab, &subquery.where_clause),
    })),
  }
}

fn convert_literal(literal: &Literal) -> EvalLiteral {
  match &literal {
    Literal::String(string) => EvalLiteral::String(string.clone()),
    Literal::Int(str) => EvalLiteral::Int(str.parse::<i32>().unwrap()),
    Literal::Bool(boolean) => EvalLiteral::Bool(boolean.clone()),
    Literal::Null => EvalLiteral::Null,
  }
}

fn convert_op(op: &BinaryOp) -> EvalBinaryOp {
  match op {
    BinaryOp::AND => EvalBinaryOp::AND,
    BinaryOp::OR => EvalBinaryOp::OR,
    BinaryOp::LT => EvalBinaryOp::LT,
    BinaryOp::LTE => EvalBinaryOp::LTE,
    BinaryOp::E => EvalBinaryOp::E,
    BinaryOp::GT => EvalBinaryOp::GT,
    BinaryOp::GTE => EvalBinaryOp::GTE,
    BinaryOp::PLUS => EvalBinaryOp::PLUS,
    BinaryOp::TIMES => EvalBinaryOp::TIMES,
    BinaryOp::MINUS => EvalBinaryOp::MINUS,
    BinaryOp::DIV => EvalBinaryOp::DIV,
  }
}
