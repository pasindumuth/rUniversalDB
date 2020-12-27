use crate::common::rand::RandGen;
use crate::common::utils::mk_tid;
use crate::model::common::{
  ColumnName, ColumnValue, PrimaryKey, SelectQueryId, SelectView, Timestamp, WriteDiff,
};
use crate::model::evalast::{
  EvalBinaryOp, EvalLiteral, Holder, InsertRowDoneTask, InsertRowEvalConstraintsTask,
  InsertRowEvalValTask, InsertRowStartTask, InsertRowTask, PostEvalExpr, PreEvalExpr,
  SelectKeyDoneTask, SelectKeyEvalWhereTask, SelectKeyNoneTask, SelectKeyStartTask, SelectKeyTask,
  SelectQueryTask, UpdateKeyDoneTask, UpdateKeyEvalConstraintsTask, UpdateKeyEvalValTask,
  UpdateKeyEvalWhereTask, UpdateKeyNoneTask, UpdateKeyStartTask, UpdateKeyTask, WriteQueryTask,
};
use crate::model::message::{FromSelectTask, FromWriteTask};
use crate::model::sqlast::{BinaryOp, InsertStmt, Literal, SelectStmt, UpdateStmt, ValExpr};
use crate::storage::relational_tablet::RelationalTablet;
use std::collections::{BTreeMap, HashMap};

/// Errors that can occur when evaluating columns while transforming
/// an AST into an EvalAST.
#[derive(Debug)]
pub enum EvalErr {
  /// When a user tries to use a column that doesn't exist.
  ColumnDNE,
  /// For when types in an expression don't match.
  TypeError,
  /// Used if we try inserting NULL into a PrimaryKey column
  NullError,
  /// For errors lik divide-by-zero, etc.
  ArithmeticError,
  /// If an insert is attempted but it violates table constraints.
  ConstraintViolation,
  MalformedSQL,
  /// This could have been due to a malformed key, a column that doesn't
  /// exist, or the value not matching the columns type.
  InsertionFailure,
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
///
/// It might be possible to rename "Tasks" to State somehow, since
/// that actually makes sense.

// -------------------------------------------------------------------------------------------------
//  Update Task
// -------------------------------------------------------------------------------------------------

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
    UpdateKeyTask::Start(_) => {
      // No subqueries should ever really come here.. if it does, it
      // should indicate a bug in our implementation. However, logically
      // we should just ignore such messages, so that is what we do.
    }
    UpdateKeyTask::EvalWhere(task) => {
      if let Some(_) = task.pending_subqueries.remove(sub_sid) {
        // The subquery is indeed relevent to this task.
        task
          .complete_subqueries
          .insert(sub_sid.clone(), subquery_ret.clone());
        return continue_update_key_task(rand_gen, update_key_task, rel_tab);
      }
    }
    UpdateKeyTask::EvalVal(task) => {
      if let Some(_) = task.pending_subqueries.remove(sub_sid) {
        // The subquery is indeed relevent to this task.
        task
          .complete_subqueries
          .insert(sub_sid.clone(), subquery_ret.clone());
        return continue_update_key_task(rand_gen, update_key_task, rel_tab);
      }
    }
    UpdateKeyTask::EvalConstraints(task) => {
      if let Some(_) = task.pending_subqueries.remove(sub_sid) {
        // The subquery is indeed relevent to this task.
        task
          .complete_subqueries
          .insert(sub_sid.clone(), subquery_ret.clone());
        return continue_update_key_task(rand_gen, update_key_task, rel_tab);
      }
    }
    UpdateKeyTask::None(_) => {
      // Do nothing.
    }
    UpdateKeyTask::Done(_) => {
      // Do nothing.
    }
  }
  Ok(BTreeMap::new())
}

/// This takes in a UpdateKeyTask and continues to evaluate it. For each
/// variant, it looks to see if there are any pending queries left and
/// moves the `update_key_task` to the next Task.
fn continue_update_key_task(
  rand_gen: &mut RandGen,
  update_key_task: &mut Holder<UpdateKeyTask>,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, SelectStmt>, EvalErr> {
  match &mut update_key_task.val {
    UpdateKeyTask::Start(task) => {
      let (post_expr, subqueries) = pre_to_post_expr(rand_gen, &task.where_clause);
      update_key_task.val = UpdateKeyTask::EvalWhere(UpdateKeyEvalWhereTask {
        set_col: task.set_col.clone(),
        set_val: task.set_val.clone(),
        where_clause: post_expr,
        table_constraints: task.table_constraints.clone(),
        pending_subqueries: subqueries.clone(),
        complete_subqueries: Default::default(),
      });
      if subqueries.is_empty() {
        // If there are no subqueries, we may continue.
        return continue_update_key_task(rand_gen, update_key_task, rel_tab);
      } else {
        return Ok(subqueries);
      }
    }
    UpdateKeyTask::EvalWhere(task) => {
      if task.pending_subqueries.is_empty() {
        // This means we are finished waiting for subqueries to arrive;
        // we may start evaluating the expressions.
        match evaluate_expr(&task.complete_subqueries, &task.where_clause)? {
          EvalLiteral::Bool(where_val) => {
            if where_val {
              let (post_expr, subqueries) = pre_to_post_expr(rand_gen, &task.set_val);
              update_key_task.val = UpdateKeyTask::EvalVal(UpdateKeyEvalValTask {
                set_col: task.set_col.clone(),
                set_val: post_expr,
                table_constraints: task.table_constraints.clone(),
                pending_subqueries: subqueries.clone(),
                complete_subqueries: Default::default(),
              });
              if subqueries.is_empty() {
                // If there are no subqueries, we may continue.
                return continue_update_key_task(rand_gen, update_key_task, rel_tab);
              } else {
                return Ok(subqueries);
              }
            } else {
              update_key_task.val = UpdateKeyTask::None(UpdateKeyNoneTask {});
            }
          }
          _ => return Err(EvalErr::TypeError),
        };
      }
    }
    UpdateKeyTask::EvalVal(task) => {
      if task.pending_subqueries.is_empty() {
        // This means we are finished waiting for subqueries to arrive;
        // we may start evaluating the expressions.
        let (post_exprs, subqueries) = pre_to_post_exprs(rand_gen, &task.table_constraints);
        update_key_task.val = UpdateKeyTask::EvalConstraints(UpdateKeyEvalConstraintsTask {
          set_col: task.set_col.clone(),
          set_val: evaluate_expr(&task.complete_subqueries, &task.set_val)?,
          table_constraints: post_exprs,
          pending_subqueries: subqueries.clone(),
          complete_subqueries: Default::default(),
        });
        if subqueries.is_empty() {
          // If there are no subqueries, we may continue.
          return continue_update_key_task(rand_gen, update_key_task, rel_tab);
        } else {
          return Ok(subqueries);
        }
      }
    }
    UpdateKeyTask::EvalConstraints(task) => {
      if task.pending_subqueries.is_empty() {
        // This means we are finished waiting for subqueries to arrive;
        // we may start evaluating the expressions.
        let constraint_vals = evaluate_exprs(&task.complete_subqueries, &task.table_constraints)?;
        for constraint_val in constraint_vals {
          match constraint_val {
            EvalLiteral::Bool(val) => {
              if !val {
                // A Table constraint was violated, so we return an EvalErr.
                return Err(EvalErr::ConstraintViolation);
              }
            }
            _ => return Err(EvalErr::TypeError),
          }
        }
        update_key_task.val = UpdateKeyTask::Done(UpdateKeyDoneTask {
          set_col: task.set_col.clone(),
          set_val: task.set_val.clone(),
        });
      }
    }
    UpdateKeyTask::None(_) => {
      // Do nothing.
    }
    UpdateKeyTask::Done(_) => {
      // Do nothing.
    }
  }
  Ok(BTreeMap::new())
}

/// This function takes all ColumnNames in the `update_stmt`, replaces
/// them with their values from `rel_tab` (from the row with key `key`
/// at `timestamp`). We do this deeply, including subqueries. Then
/// we construct EvalUpdateStmt with all subqueries in tact, i.e. they
/// aren't turned into SubqueryIds yet.
pub fn start_eval_update_key_task(
  rand_gen: &mut RandGen,
  update_stmt: &UpdateStmt,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<(UpdateKeyTask, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  let mut task = Holder::from(UpdateKeyTask::Start(UpdateKeyStartTask {
    set_col: verify_col(&update_stmt.set_col, rel_tab)?,
    set_val: val_to_pre_expr(&update_stmt.set_val, rel_tab, key, timestamp)?,
    where_clause: val_to_pre_expr(&update_stmt.where_clause, rel_tab, key, timestamp)?,
    table_constraints: vec![],
  }));
  let context = continue_update_key_task(rand_gen, &mut task, rel_tab)?;
  return Ok((task.val, context));
}

// -------------------------------------------------------------------------------------------------
//  Insert Task
// -------------------------------------------------------------------------------------------------

/// This function looks at the `InsertRowTask`'s own context and tries
/// to evaluate it as far as possible.
pub fn eval_insert_row_task(
  rand_gen: &mut RandGen,
  insert_row_task: &mut Holder<InsertRowTask>,
  sub_sid: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, SelectStmt>, EvalErr> {
  match &mut insert_row_task.val {
    InsertRowTask::Start(_) => {
      // No subqueries should ever really come here.. if it does, it
      // should indicate a bug in our implementation. However, logically
      // we should just ignore such messages, so that is what we do.
    }
    InsertRowTask::EvalVal(task) => {
      if let Some(_) = task.pending_subqueries.remove(sub_sid) {
        // The subquery is indeed relevent to this task.
        task
          .complete_subqueries
          .insert(sub_sid.clone(), subquery_ret.clone());
        return continue_insert_row_task(rand_gen, insert_row_task, rel_tab);
      }
    }
    InsertRowTask::EvalConstraints(task) => {
      if let Some(_) = task.pending_subqueries.remove(sub_sid) {
        // The subquery is indeed relevent to this task.
        task
          .complete_subqueries
          .insert(sub_sid.clone(), subquery_ret.clone());
        return continue_insert_row_task(rand_gen, insert_row_task, rel_tab);
      }
    }
    InsertRowTask::Done(_) => {
      // Do nothing.
    }
  }
  Ok(BTreeMap::new())
}

/// This takes in a InsertRowTask and continues to evaluate it. For each
/// variant, it looks to see if there are any pending queries left and
/// moves the `insert_row_task` to the next Task.
pub fn continue_insert_row_task(
  rand_gen: &mut RandGen,
  insert_row_task: &mut Holder<InsertRowTask>,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, SelectStmt>, EvalErr> {
  match &mut insert_row_task.val {
    InsertRowTask::Start(task) => {
      let (post_exprs, subqueries) = pre_to_post_exprs(rand_gen, &task.insert_vals);
      insert_row_task.val = InsertRowTask::EvalVal(InsertRowEvalValTask {
        insert_cols: task.insert_cols.clone(),
        insert_vals: post_exprs,
        table_constraints: task.table_constraints.clone(),
        pending_subqueries: subqueries.clone(),
        complete_subqueries: Default::default(),
      });
      if subqueries.is_empty() {
        // If there are no subqueries, we may continue.
        return continue_insert_row_task(rand_gen, insert_row_task, rel_tab);
      } else {
        return Ok(subqueries);
      }
    }
    InsertRowTask::EvalVal(task) => {
      if task.pending_subqueries.is_empty() {
        // This means we are finished waiting for subqueries to arrive;
        // we may start evaluating the expressions.
        let (post_exprs, subqueries) = pre_to_post_exprs(rand_gen, &task.table_constraints);
        insert_row_task.val = InsertRowTask::EvalConstraints(InsertRowEvalConstraintsTask {
          insert_cols: task.insert_cols.clone(),
          insert_vals: evaluate_exprs(&task.complete_subqueries, &task.insert_vals)?,
          table_constraints: post_exprs,
          pending_subqueries: subqueries.clone(),
          complete_subqueries: Default::default(),
        });
        if subqueries.is_empty() {
          // If there are no subqueries, we may continue.
          return continue_insert_row_task(rand_gen, insert_row_task, rel_tab);
        } else {
          return Ok(subqueries);
        }
      }
    }
    InsertRowTask::EvalConstraints(task) => {
      if task.pending_subqueries.is_empty() {
        // This means we are finished waiting for subqueries to arrive;
        // we may start evaluating the expressions.
        let constraint_vals = evaluate_exprs(&task.complete_subqueries, &task.table_constraints)?;
        for constraint_val in constraint_vals {
          match constraint_val {
            EvalLiteral::Bool(val) => {
              if !val {
                // A Table constraint was violated, so we return an EvalErr.
                return Err(EvalErr::ConstraintViolation);
              }
            }
            _ => return Err(EvalErr::TypeError),
          }
        }
        insert_row_task.val = InsertRowTask::Done(InsertRowDoneTask {
          insert_cols: task.insert_cols.clone(),
          insert_vals: task.insert_vals.clone(),
        });
      }
    }
    InsertRowTask::Done(_) => {
      // Do nothing.
    }
  }
  Ok(BTreeMap::new())
}

/// This function verifies that all `col_names` are actual columns in the
/// `rel_tab`'s schema, and then procudes an InsertRowTask for the `index`th
/// value in the VALUES list in the `update_stmt`.
pub fn start_eval_insert_row_task(
  rand_gen: &mut RandGen,
  insert_stmt: &InsertStmt,
  rel_tab: &RelationalTablet,
  index: usize,
) -> Result<(InsertRowTask, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  let insert_row = insert_stmt.insert_vals.get(index).unwrap();
  let mut task = Holder::from(InsertRowTask::Start(InsertRowStartTask {
    insert_cols: verify_cols(&insert_stmt.col_names, rel_tab)?,
    insert_vals: val_to_pre_exprs_no_col(&insert_row)?,
    table_constraints: vec![],
  }));
  let context = continue_insert_row_task(rand_gen, &mut task, rel_tab)?;
  return Ok((task.val, context));
}

// -------------------------------------------------------------------------------------------------
//  Write Query Task
// -------------------------------------------------------------------------------------------------

/// This function adds the adds the `subquery` at the location of
/// `path`, and then executes things as far as they will go. If
/// we realize the `WriteQueryTask` cannot be finished (due to
/// type errors, runtime errors (dividing by zero) or broken
/// table constriants), then we return an Err instead.
pub fn eval_write_graph(
  rand_gen: &mut RandGen,
  write_task: &mut Holder<WriteQueryTask>,
  path: &FromWriteTask,
  sub_sid: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, (FromWriteTask, SelectStmt)>, EvalErr> {
  match (&path, &mut write_task.val) {
    (FromWriteTask::UpdateTask { key }, WriteQueryTask::UpdateTask(task)) => {
      if let Some(update_key_task) = task.key_tasks.get_mut(key) {
        let subqueries =
          eval_update_key_task(rand_gen, update_key_task, sub_sid, subquery_ret, rel_tab)?;
        match &mut update_key_task.val {
          UpdateKeyTask::Done(done_task) => {
            // The Key Task is done, so we can just add its value into the
            // WriteDiff we are constructing.
            task.key_vals.push((
              key.clone(),
              Some(vec![(
                done_task.set_col.clone(),
                from_lit(done_task.set_val.clone()),
              )]),
            ));
            // Remove the key task because it is done.
            task.key_tasks.remove(key).unwrap();
            if task.key_vals.is_empty() {
              // This means that the whole Write Task is finished as well.
              write_task.val = WriteQueryTask::WriteDoneTask(task.key_vals.clone());
            }
          }
          _ => {
            let mut context = BTreeMap::<SelectQueryId, (FromWriteTask, SelectStmt)>::new();
            for (sid, select_stmt) in subqueries {
              context.insert(sid, (path.clone(), select_stmt));
            }
            return Ok(context);
          }
        }
      }
    }
    (FromWriteTask::InsertTask { index }, WriteQueryTask::InsertTask(task)) => {
      if let Some(insert_row_task) = task.tasks.get_mut(index) {
        let subqueries =
          eval_insert_row_task(rand_gen, insert_row_task, sub_sid, subquery_ret, rel_tab)?;
        match &mut insert_row_task.val {
          InsertRowTask::Done(done_task) => {
            // This means the InsertRowTask for the row completed
            // without the need for Subqueries, and that an
            // insert should take place.
            col_vals_into_diff(
              &mut task.key_vals,
              rel_tab,
              &done_task.insert_cols,
              done_task.insert_vals.clone(),
            )?;
            // Remove the row task because it is done.
            task.tasks.remove(index).unwrap();
            if task.key_vals.is_empty() {
              // This means that the whole Write Task is finished as well.
              write_task.val = WriteQueryTask::WriteDoneTask(task.key_vals.clone());
            }
          }
          _ => {
            let mut context = BTreeMap::<SelectQueryId, (FromWriteTask, SelectStmt)>::new();
            for (sid, select_stmt) in subqueries {
              context.insert(sid, (path.clone(), select_stmt));
            }
            return Ok(context);
          }
        }
      }
    }
    (FromWriteTask::InsertSelectTask, WriteQueryTask::InsertSelectTask(_)) => {
      panic!("TODO: implement")
    }
    _ => {
      // Ignore all other combination of `path` and `write_task`;
      // those shouldn't result in any transitions.
    }
  }
  Ok(BTreeMap::new())
}

// -------------------------------------------------------------------------------------------------
//  Select Task
// -------------------------------------------------------------------------------------------------

pub fn eval_select_key_task(
  rand_gen: &mut RandGen,
  select_key_task: &mut Holder<SelectKeyTask>,
  sub_sid: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, SelectStmt>, EvalErr> {
  match &mut select_key_task.val {
    SelectKeyTask::Start(_) => {
      // No subqueries should ever really come here.. if it does, it
      // should indicate a bug in our implementation. However, logically
      // we should just ignore such messages, so that is what we do.
    }
    SelectKeyTask::EvalWhere(task) => {
      if let Some(_) = task.pending_subqueries.remove(sub_sid) {
        // The subquery is indeed relevent to this task.
        task
          .complete_subqueries
          .insert(sub_sid.clone(), subquery_ret.clone());
        return continue_select_key_task(rand_gen, select_key_task, rel_tab);
      }
    }
    SelectKeyTask::None(_) => {
      // Do nothing.
    }
    SelectKeyTask::Done(_) => {
      // Do nothing.
    }
  }
  Ok(BTreeMap::new())
}

/// This takes in a UpdateKeyTask and continues to evaluate it. For each
/// variant, it looks to see if there are any pending queries left and
/// moves the `update_key_task` to the next Task.
pub fn continue_select_key_task(
  rand_gen: &mut RandGen,
  select_key_task: &mut Holder<SelectKeyTask>,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, SelectStmt>, EvalErr> {
  match &mut select_key_task.val {
    SelectKeyTask::Start(task) => {
      let (post_expr, subqueries) = pre_to_post_expr(rand_gen, &task.where_clause);
      select_key_task.val = SelectKeyTask::EvalWhere(SelectKeyEvalWhereTask {
        sel_cols: task.sel_cols.clone(),
        where_clause: post_expr,
        pending_subqueries: subqueries.clone(),
        complete_subqueries: Default::default(),
      });
      if subqueries.is_empty() {
        // If there are no subqueries, we may continue.
        return continue_select_key_task(rand_gen, select_key_task, rel_tab);
      } else {
        return Ok(subqueries);
      }
    }
    SelectKeyTask::EvalWhere(task) => {
      if task.pending_subqueries.is_empty() {
        // This means we are finished waiting for subqueries to arrive;
        // we may start evaluating the expressions.
        match evaluate_expr(&task.complete_subqueries, &task.where_clause)? {
          EvalLiteral::Bool(where_val) => {
            if where_val {
              select_key_task.val = SelectKeyTask::Done(SelectKeyDoneTask {
                sel_cols: task.sel_cols.clone(),
              });
            } else {
              select_key_task.val = SelectKeyTask::None(SelectKeyNoneTask {});
            }
          }
          _ => return Err(EvalErr::TypeError),
        };
      }
    }
    SelectKeyTask::None(_) => {
      // Do nothing.
    }
    SelectKeyTask::Done(_) => {
      // Do nothing.
    }
  }
  Ok(BTreeMap::new())
}

/// This function takes all ColumnNames in the `update_stmt`, replaces
/// them with their values from `rel_tab` (from the row with key `key`
/// at `timestamp`). We do this deeply, including subqueries. Then
/// we construct a SelectKeyTask.
pub fn start_eval_select_key_task(
  rand_gen: &mut RandGen,
  select_stmt: SelectStmt,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<(SelectKeyTask, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  let mut task = Holder::from(SelectKeyTask::Start(SelectKeyStartTask {
    sel_cols: verify_cols(&select_stmt.col_names, rel_tab)?,
    where_clause: val_to_pre_expr(&select_stmt.where_clause, rel_tab, key, timestamp)?,
  }));
  let context = continue_select_key_task(rand_gen, &mut task, rel_tab)?;
  return Ok((task.val, context));
}

// -------------------------------------------------------------------------------------------------
//  Select Query Task
// -------------------------------------------------------------------------------------------------

/// This function routes the given subquery `sub_sid` down to the specific
/// sub task indicated by `path` and executes the given `select_task` as far
/// as it will go.
pub fn eval_select_graph(
  rand_gen: &mut RandGen,
  select_task: &mut Holder<SelectQueryTask>,
  path: &FromSelectTask,
  sub_sid: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, (FromSelectTask, SelectStmt)>, EvalErr> {
  match (&path, &mut select_task.val) {
    (FromSelectTask::SelectTask { key }, SelectQueryTask::SelectTask(task)) => {
      if let Some(update_key_task) = task.key_tasks.get_mut(key) {
        let subqueries =
          eval_select_key_task(rand_gen, update_key_task, sub_sid, subquery_ret, rel_tab)?;
        match &mut update_key_task.val {
          SelectKeyTask::Done(done_task) => {
            // This means we should add the row into the SelectView. Easy.
            let mut view_row = Vec::new();
            for sel_col in &done_task.sel_cols {
              view_row.push((
                sel_col.clone(),
                rel_tab.get_partial_val(&key, &sel_col, &task.timestamp),
              ));
            }
            task.select_view.insert(key.clone(), view_row);
            // Remove the key task because it is done.
            task.key_tasks.remove(key).unwrap();
            if task.key_tasks.is_empty() {
              // This means that the whole Select Task is finished as well.
              select_task.val = SelectQueryTask::SelectDoneTask(task.select_view.clone());
            }
          }
          _ => {
            let mut context = BTreeMap::<SelectQueryId, (FromSelectTask, SelectStmt)>::new();
            for (sid, select_stmt) in subqueries {
              context.insert(sid, (path.clone(), select_stmt));
            }
            return Ok(context);
          }
        }
      }
    }
    _ => {
      // Ignore all other combination of `path` and `select_query_task`;
      // those shouldn't result in any transitions.
    }
  };
  Ok(BTreeMap::new())
}

// -------------------------------------------------------------------------------------------------
//  Expression Transformations
// -------------------------------------------------------------------------------------------------

/// Function transforms a `ValExpr` into a `PreEvalExpr`, where the
/// `ValExpr` doesn't contain any `Column` nodes (unless it's under a Subquery).
/// These sorts of expressions are common place in INSERT queries where there
/// is no existing table row that's in the picture. If we find a `Column`
/// (that's not underneath a Subquery), then we return an EvalErr.
fn val_to_pre_expr_no_col(expr: &ValExpr) -> Result<PreEvalExpr, EvalErr> {
  match expr {
    ValExpr::BinaryExpr { op, lhs, rhs } => Ok(PreEvalExpr::BinaryExpr {
      op: convert_op(&op),
      lhs: Box::new(val_to_pre_expr_no_col(lhs)?),
      rhs: Box::new(val_to_pre_expr_no_col(rhs)?),
    }),
    ValExpr::Literal(literal) => Ok(PreEvalExpr::Literal(convert_literal(&literal)?)),
    ValExpr::Column(_) => Err(EvalErr::ColumnDNE),
    ValExpr::Subquery(subquery) => Ok(PreEvalExpr::Subquery(subquery.clone())),
  }
}

fn val_to_pre_exprs_no_col(exprs: &Vec<ValExpr>) -> Result<Vec<PreEvalExpr>, EvalErr> {
  trans_res(exprs, |expr| val_to_pre_expr_no_col(expr))
}

/// This function first transforms the `ValExpr` into another `ValExpr` by
/// replacing all `Column` nodes that are a part of the schema of `rel_tab`
/// with the actual column value at the given `key` and `timestamp`. Then,
/// we trivially this `ValExpr` to a `PreEvalExpr`. We might not be able to
/// do this step because there might remain `Column` nodes that weren't
/// replaced and that are not under some Subquery. This means the user sent
/// a Query where they used an undefined column name in some expression. Thus
/// we return a `ColumnDNE` error.
fn val_to_pre_expr(
  expr: &ValExpr,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<PreEvalExpr, EvalErr> {
  // First replace the columns, and then convert to PreEvalExpr.
  return val_to_pre_expr_no_col(&replace_columns(expr, rel_tab, key, timestamp));
}

fn val_to_pre_exprs(
  exprs: &Vec<ValExpr>,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<Vec<PreEvalExpr>, EvalErr> {
  trans_res(exprs, |expr| val_to_pre_expr(expr, rel_tab, key, timestamp))
}

/// This function replaces all instances of `Subquery`s in `pre_expr`
/// with randomly generated `SubqueryId`s, and then returns the
/// constructed `post_expr` along with the subqueries and their
/// `SubqueryId`s.
fn pre_to_post_expr(
  rand_gen: &mut RandGen,
  pre_expr: &PreEvalExpr,
) -> (PostEvalExpr, BTreeMap<SelectQueryId, SelectStmt>) {
  /// We use recursion to do this.
  fn pre_to_post_expr_r(
    rand_gen: &mut RandGen,
    subqueries: &mut BTreeMap<SelectQueryId, SelectStmt>,
    expr: PreEvalExpr,
  ) -> PostEvalExpr {
    match expr {
      PreEvalExpr::Literal(literal) => PostEvalExpr::Literal(literal),
      PreEvalExpr::BinaryExpr { op, lhs, rhs } => PostEvalExpr::BinaryExpr {
        op,
        lhs: Box::new(pre_to_post_expr_r(rand_gen, subqueries, *lhs)),
        rhs: Box::new(pre_to_post_expr_r(rand_gen, subqueries, *rhs)),
      },
      PreEvalExpr::Subquery(subquery) => {
        // Create a random SubqueryId
        let subquery_id = SelectQueryId(mk_tid(&mut rand_gen.rng));
        // Add the Subquery to the `subqueries` vector.
        subqueries.insert(subquery_id.clone(), *subquery);
        // Replace the subquery with the new subquery_id.
        PostEvalExpr::SubqueryId(subquery_id)
      }
    }
  }
  // Call the recursive function.
  let mut subqueries = BTreeMap::new();
  let post_expr = pre_to_post_expr_r(rand_gen, &mut subqueries, pre_expr.clone());
  (post_expr, subqueries)
}

/// This function calls `pre_to_post_expr` for each element in `pre_exprs`,
/// merging the subqueries produced by each before returning it.
fn pre_to_post_exprs(
  rand_gen: &mut RandGen,
  pre_exprs: &Vec<PreEvalExpr>,
) -> (Vec<PostEvalExpr>, BTreeMap<SelectQueryId, SelectStmt>) {
  let mut all_subqueries = BTreeMap::new();
  let mut post_exprs = Vec::new();
  for pre_expr in pre_exprs {
    let (post_expr, subqueries) = pre_to_post_expr(rand_gen, pre_expr);
    post_exprs.push(post_expr);
    for (sub_sid, subquery) in subqueries {
      all_subqueries.insert(sub_sid, subquery);
    }
  }
  return (post_exprs, all_subqueries);
}

/// This function evaluates the given `expr`. The `subquery_vals`
/// should be sufficient to finish `expr` (we throw a fatal error if
/// it isn't). We throw an EvalErr for things like Type mismatches,
/// diving by zero, etc.
///
/// The BinaryOp evaluation is very simple; it doesn't even implement
/// the SQL standard properly yet (NULL's are treated right).
fn evaluate_expr(
  sub_vals: &BTreeMap<SelectQueryId, SelectView>,
  expr: &PostEvalExpr,
) -> Result<EvalLiteral, EvalErr> {
  match expr {
    PostEvalExpr::BinaryExpr { op, lhs, rhs } => {
      let elhs = evaluate_expr(sub_vals, &lhs)?;
      let erhs = evaluate_expr(sub_vals, &rhs)?;
      match op {
        EvalBinaryOp::AND => match (elhs, erhs) {
          (EvalLiteral::Bool(lbool), EvalLiteral::Bool(rbool)) => {
            Ok(EvalLiteral::Bool(lbool && rbool))
          }
          _ => Err(EvalErr::TypeError),
        },
        EvalBinaryOp::OR => match (elhs, erhs) {
          (EvalLiteral::Bool(lbool), EvalLiteral::Bool(rbool)) => {
            Ok(EvalLiteral::Bool(lbool || rbool))
          }
          _ => Err(EvalErr::TypeError),
        },
        EvalBinaryOp::LT => match (elhs, erhs) {
          (EvalLiteral::Int(lint), EvalLiteral::Int(rint)) => Ok(EvalLiteral::Bool(lint < rint)),
          _ => Err(EvalErr::TypeError),
        },
        EvalBinaryOp::LTE => match (elhs, erhs) {
          (EvalLiteral::Int(lint), EvalLiteral::Int(rint)) => Ok(EvalLiteral::Bool(lint <= rint)),
          _ => Err(EvalErr::TypeError),
        },
        EvalBinaryOp::E => Ok(EvalLiteral::Bool(elhs == erhs)),
        EvalBinaryOp::GT => match (elhs, erhs) {
          (EvalLiteral::Int(lint), EvalLiteral::Int(rint)) => Ok(EvalLiteral::Bool(lint > rint)),
          _ => Err(EvalErr::TypeError),
        },
        EvalBinaryOp::GTE => match (elhs, erhs) {
          (EvalLiteral::Int(lint), EvalLiteral::Int(rint)) => Ok(EvalLiteral::Bool(lint >= rint)),
          _ => Err(EvalErr::TypeError),
        },
        EvalBinaryOp::PLUS => match (elhs, erhs) {
          (EvalLiteral::Int(lint), EvalLiteral::Int(rint)) => Ok(EvalLiteral::Int(lint + rint)),
          _ => Err(EvalErr::TypeError),
        },
        EvalBinaryOp::TIMES => match (elhs, erhs) {
          (EvalLiteral::Int(lint), EvalLiteral::Int(rint)) => Ok(EvalLiteral::Int(lint * rint)),
          _ => Err(EvalErr::TypeError),
        },
        EvalBinaryOp::MINUS => match (elhs, erhs) {
          (EvalLiteral::Int(lint), EvalLiteral::Int(rint)) => Ok(EvalLiteral::Int(lint - rint)),
          _ => Err(EvalErr::TypeError),
        },
      }
    }
    PostEvalExpr::Literal(literal) => Ok(literal.clone()),
    PostEvalExpr::SubqueryId(sub_sid) => {
      // The sub_sid should be in the sub_vals passed in.
      let sub_val = sub_vals.get(sub_sid).unwrap().clone();
      // There should only be one row returned from the subquery.
      if sub_val.len() != 1 {
        return Err(EvalErr::ArithmeticError);
      }
      let (_, col_vals) = sub_val.iter().next().unwrap();
      // There should only be one column value in the row returned.
      if col_vals.len() != 1 {
        return Err(EvalErr::ArithmeticError);
      }
      Ok(to_lit(col_vals.first().unwrap().1.clone()))
    }
  }
}

fn evaluate_exprs(
  subquery_vals: &BTreeMap<SelectQueryId, SelectView>,
  exprs: &Vec<PostEvalExpr>,
) -> Result<Vec<EvalLiteral>, EvalErr> {
  trans_res(exprs, |expr| evaluate_expr(subquery_vals, expr))
}

/// This function verifies that the `col_name` is in the
/// schema of the `rel_tab`.
fn verify_col(col_name: &String, rel_tab: &RelationalTablet) -> Result<ColumnName, EvalErr> {
  let col_name = ColumnName(col_name.clone());
  if rel_tab.col_name_exists(&col_name) {
    Ok(col_name)
  } else {
    return Err(EvalErr::ColumnDNE);
  }
}

fn verify_cols(
  col_names: &Vec<String>,
  rel_tab: &RelationalTablet,
) -> Result<Vec<ColumnName>, EvalErr> {
  trans_res(col_names, |col_name| verify_col(col_name, rel_tab))
}

/// Suppose `col_name` is a column name in the schema of `rel_tab`. This
/// function finds all instances of `col_name` in `expr` and replaces it
/// with the value of that column in `rel_tab` for the row keyed by `key`
/// at the given `timestamp`.
fn replace_columns(
  expr: &ValExpr,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> ValExpr {
  match expr {
    ValExpr::BinaryExpr { op, lhs, rhs } => ValExpr::BinaryExpr {
      op: op.clone(),
      lhs: Box::new(replace_columns(lhs, rel_tab, key, timestamp)),
      rhs: Box::new(replace_columns(rhs, rel_tab, key, timestamp)),
    },
    ValExpr::Literal(literal) => ValExpr::Literal(literal.clone()),
    ValExpr::Column(col) => {
      let col_name = ColumnName(col.clone());
      if rel_tab.col_name_exists(&col_name) {
        // If the column name exists in the current tablet's schema,
        // we replace the column name with the column value.
        ValExpr::Literal(match rel_tab.get_partial_val(key, &col_name, timestamp) {
          Some(ColumnValue::Int(int)) => Literal::Int(int.to_string()),
          Some(ColumnValue::String(string)) => Literal::String(string.to_string()),
          Some(ColumnValue::Bool(boolean)) => Literal::Bool(boolean),
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
      where_clause: replace_columns(&subquery.where_clause, rel_tab, key, timestamp),
    })),
  }
}

// -------------------------------------------------------------------------------------------------
//  Relational Tablet Modifers
// -------------------------------------------------------------------------------------------------

/// The following two functions, `table_insert_call` and `table_insert_row`
/// are used during UPDATE and INSERT key/row task processing during the
/// initial construction of the Update/Insert Task so that if the key/row
/// task succeeds, it's inserted directly into the `rel_tab`. However, if
/// we don't complete the Update/Insert Task, the remaining key/row Task
/// results are put into a `WriteDiff` where it is merged into `rel_tab`
/// at some future point via a call to `tablet_insert_diff`.

pub fn table_insert_cell(
  rel_tab: &mut RelationalTablet,
  key: &PrimaryKey,
  col_name: &ColumnName,
  val: EvalLiteral,
  timestamp: &Timestamp,
) -> Result<(), EvalErr> {
  if rel_tab
    .insert_partial_val(key.clone(), col_name.clone(), from_lit(val), timestamp)
    .is_ok()
  {
    Ok(())
  } else {
    Err(EvalErr::InsertionFailure)
  }
}

/// This function takes the `col_names` and `col_vals`, constructs a PrimaryKey
/// and value according to the schema of `rel_tab`, and inserts this key-value
/// pair into `rel_tab`. Importantly, we assume that by this point, `verify_insert`
/// was already called, so we assume that we have the gaurantees provided there.
///
/// Note that an error might be thrown when we try to construct the PrimaryKey
/// and we realize one of the elements in there is NULL. It might alse thrown
/// if the col_vals type doesn't match that of the `ColumnType` in the schema.
pub fn table_insert_row(
  rel_tab: &mut RelationalTablet,
  col_names: &Vec<ColumnName>,
  col_vals: Vec<EvalLiteral>,
  timestamp: &Timestamp,
) -> Result<(), EvalErr> {
  assert_eq!(col_names.len(), col_vals.len());
  let mut col_name_val_map: HashMap<ColumnName, Option<ColumnValue>> = HashMap::new();
  for (col_name, col_val) in col_names.iter().zip(col_vals.iter()) {
    col_name_val_map.insert(col_name.clone(), from_lit(col_val.clone()));
  }
  let mut key = PrimaryKey { cols: vec![] };
  for (_, key_col_name) in &rel_tab.schema.key_cols {
    // Due to `verify_insert`, we may use `unwrap` without concern.
    if let Some(key_col_val) = col_name_val_map.remove(key_col_name).unwrap() {
      key.cols.push(key_col_val);
    } else {
      // This cannot evaluate to NULL because all column values in a
      // Primary Key must be present.
      return Err(EvalErr::NullError);
    }
  }
  let mut partial_val = vec![];
  for (col_name, col_val) in col_name_val_map {
    partial_val.push((col_name, col_val));
  }
  if rel_tab
    .insert_partial_vals(key, partial_val, timestamp)
    .is_ok()
  {
    Ok(())
  } else {
    // We will get here if the `col_vals` doesn't match the
    // `ColumnType` in the schema.
    Err(EvalErr::InsertionFailure)
  }
}

/// This functions inserts the key-value pairs in `diff` into the
/// `rel_tab` and the given `timestamp`.
///
/// Here, the WriteDiff must always conform to the schema.
pub fn table_insert_diff(
  rel_tab: &mut RelationalTablet,
  diff: &WriteDiff,
  timestamp: &Timestamp,
) -> Result<(), EvalErr> {
  for (key, partial_vals) in diff {
    if !rel_tab.verify_row_key(&key) {
      // We check to see if the key conforms preemptively.
      return Err(EvalErr::InsertionFailure);
    }
    if !rel_tab.is_in_key_range(&key) {
      // It's actually okay if we find a key that isn't in
      // the tablet's range. This happens if we are performing
      // an INSERT.
      continue;
    }

    if rel_tab
      .insert_row_diff(key.clone(), partial_vals.clone(), timestamp)
      .is_err()
    {
      // This will most often be because an Update didn't check that the
      // the col_name exists, or that the EvalLiteral that Update or Insert
      // evaluated to didn't line up with the schema.
      return Err(EvalErr::InsertionFailure);
    }
  }
  Ok(())
}

/// This function takes the `col_names` and `col_vals`, constructs a PrimaryKey
/// and value according to the schema of `rel_tab`, and inserts this key-value
/// pair into `diff`. Importantly, we assume that by this point, `verify_insert`
/// was already called, so we assume that we have the gaurantees provided there.
/// Note that an error might be thrown when we try to construct the PrimaryKey
/// and we realize one of the elements in there is NULL.
fn col_vals_into_diff(
  diff: &mut WriteDiff,
  rel_tab: &RelationalTablet,
  col_names: &Vec<ColumnName>,
  col_vals: Vec<EvalLiteral>,
) -> Result<(), EvalErr> {
  assert_eq!(col_names.len(), col_vals.len());
  let mut col_name_val_map: HashMap<ColumnName, Option<ColumnValue>> = HashMap::new();
  for (col_name, col_val) in col_names.iter().zip(col_vals.iter()) {
    col_name_val_map.insert(col_name.clone(), from_lit(col_val.clone()));
  }
  let mut key = PrimaryKey { cols: vec![] };
  for (_, key_col_name) in &rel_tab.schema.key_cols {
    // Due to `verify_insert`, we may use `unwrap` without concern.
    if let Some(key_col_val) = col_name_val_map.remove(key_col_name).unwrap() {
      key.cols.push(key_col_val);
    } else {
      // This cannot evaluate to NULL because all column values in a
      // Primary Key must be present.
      return Err(EvalErr::NullError);
    }
  }
  let mut partial_val = vec![];
  for (col_name, col_val) in col_name_val_map {
    partial_val.push((col_name, col_val));
  }
  diff.push((key, Some(partial_val)));
  Ok(())
}

// -------------------------------------------------------------------------------------------------
//  Type Conversion function
// -------------------------------------------------------------------------------------------------

/// Trivially converts an `EvalLiteral` to `Option<ColumnValue>`.
fn from_lit(eval_lit: EvalLiteral) -> Option<ColumnValue> {
  match eval_lit {
    EvalLiteral::Int(i32) => Some(ColumnValue::Int(i32)),
    EvalLiteral::Bool(bool) => Some(ColumnValue::Bool(bool)),
    EvalLiteral::String(string) => Some(ColumnValue::String(string)),
    EvalLiteral::Null => None,
  }
}

/// Trivially converts an `Option<ColumnValue>` to `EvalLiteral`.
fn to_lit(col_val: Option<ColumnValue>) -> EvalLiteral {
  match col_val {
    Some(ColumnValue::Int(i32)) => EvalLiteral::Int(i32),
    Some(ColumnValue::Bool(bool)) => EvalLiteral::Bool(bool),
    Some(ColumnValue::String(string)) => EvalLiteral::String(string),
    None => EvalLiteral::Null,
  }
}

/// Converts a SQL `Literal` into `EvalLiteral`. Since `Literal` is
/// verbatim what the user provided us, their numerical values might be
/// malformed (i.e. too big). Thus, this function can return an error too.
fn convert_literal(literal: &Literal) -> Result<EvalLiteral, EvalErr> {
  match &literal {
    Literal::String(string) => Ok(EvalLiteral::String(string.clone())),
    Literal::Int(str) => {
      if let Ok(int) = str.parse::<i32>() {
        Ok(EvalLiteral::Int(int))
      } else {
        Err(EvalErr::ArithmeticError)
      }
    }
    Literal::Bool(boolean) => Ok(EvalLiteral::Bool(boolean.clone())),
    Literal::Null => Ok(EvalLiteral::Null),
  }
}

/// Trivially converts `BinaryOp` to `EvalBinaryOp`.  
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
  }
}

// -------------------------------------------------------------------------------------------------
//  SQL Verification functions
// -------------------------------------------------------------------------------------------------

/// This function verifies 3 things: the `insert_stmt`'s `col_names` are part of the
/// schema of `rel_tab`, that `col_names` spans the PrimaryKey columns in the schema,
/// and that every tuple in the VALUES clause has the same length as `col_names`.
/// Notably, this function doesn't do type checking of the expression; that
/// can only be at runtime.
pub fn verify_insert(rel_tab: &RelationalTablet, insert_stmt: &InsertStmt) -> Result<(), EvalErr> {
  let col_names: Vec<ColumnName> = insert_stmt
    .col_names
    .iter()
    .map(|col_name| ColumnName(col_name.clone()))
    .collect();
  // We first verify that the columns provided are part of the schema.
  if !rel_tab.col_names_exists(&col_names) {
    return Err(EvalErr::MalformedSQL);
  }
  // Next, we verify that the given set of columns span the PrimaryKey columns.
  for (_, key_col_name) in &rel_tab.schema.key_cols {
    if !col_names.contains(key_col_name) {
      return Err(EvalErr::MalformedSQL);
    }
  }
  // Finally, we verify that all tuples to be inserted have the same number
  // of elements as `col_names`; the columns to be inserted to.
  for insert_val in &insert_stmt.insert_vals {
    if insert_val.len() != col_names.len() {
      return Err(EvalErr::MalformedSQL);
    }
  }
  Ok(())
}

// -------------------------------------------------------------------------------------------------
//  Container Utilities
// -------------------------------------------------------------------------------------------------

fn trans_res<T, V, F: Fn(&T) -> Result<V, EvalErr>>(
  vals: &Vec<T>,
  f: F,
) -> Result<Vec<V>, EvalErr> {
  let mut trans_vals = Vec::new();
  for val in vals {
    trans_vals.push(f(val)?);
  }
  return Ok(trans_vals);
}
