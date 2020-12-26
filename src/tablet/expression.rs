use crate::common::rand::RandGen;
use crate::model::common::{
  ColumnName, ColumnValue, PrimaryKey, SelectQueryId, SelectView, Timestamp, TransactionId,
  WriteDiff,
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
use rand::Rng;
use std::collections::BTreeMap;

/// Errors that can occur when evaluating columns while transforming
/// an AST into an EvalAST.
#[derive(Debug)]
pub enum EvalErr {
  ColumnDNE,
  TypeError,
  ConstraintViolation,
  MalformedSQL,
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
pub fn continue_update_key_task(
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
  if insert_stmt.col_names.len() != insert_row.len() {
    return Err(EvalErr::MalformedSQL);
  } else {
    let mut task = Holder::from(InsertRowTask::Start(InsertRowStartTask {
      insert_cols: verify_cols(&insert_stmt.col_names, rel_tab)?,
      insert_vals: val_to_pre_exprs_no_col(&insert_row)?,
      table_constraints: vec![],
    }));
    let context = continue_insert_row_task(rand_gen, &mut task, rel_tab)?;
    return Ok((task.val, context));
  }
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
                convert(done_task.set_val.clone()),
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
              &done_task.insert_cols,
              done_task.insert_vals.clone(),
            );
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
    (FromWriteTask::InsertSelectTask, WriteQueryTask::InsertSelectTask(task)) => {
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
  Err(EvalErr::ColumnDNE)
}

// -------------------------------------------------------------------------------------------------
//  Expression Transformations
// -------------------------------------------------------------------------------------------------

/// Function transforms a `ValExpr` into a `PreEvalExpr`, where the
/// `ValExpr` doesn't refer to any column values. These sorts of expressions
/// are common place in INSERT queries where there is no existing table row
/// that's in the picture. If we find a `Column` (that's not underneath a
/// Subquery), then we return an EvalErr.
fn val_to_pre_expr_no_col(expr: &ValExpr) -> Result<PreEvalExpr, EvalErr> {
  panic!("TODO: implement")
}

fn val_to_pre_exprs_no_col(exprs: &Vec<ValExpr>) -> Result<Vec<PreEvalExpr>, EvalErr> {
  trans_res(exprs, |expr| val_to_pre_expr_no_col(expr))
}

/// This function performs a traveral of the expression tree in `expr`,
/// deeply substituting `Column` nodes with the actual value of the
/// column using the given `rel_tab` for the given `key` and `timestamp`.
/// If the `Column` node is in a Subquery, then the replaced value is a
/// sqlast type.
fn val_to_pre_expr(
  expr: &ValExpr,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<PreEvalExpr, EvalErr> {
  panic!("TODO: implement")
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
/// with randomly generated `SubqueryId`s, and then returns
/// those subqueries along with their `SubqueryId`s.
fn pre_to_post_expr(
  rand_gen: &mut RandGen,
  pre_expr: &PreEvalExpr,
) -> (PostEvalExpr, BTreeMap<SelectQueryId, SelectStmt>) {
  panic!("Implement");
}

fn pre_to_post_exprs(
  rand_gen: &mut RandGen,
  pre_exprs: &Vec<PreEvalExpr>,
) -> (Vec<PostEvalExpr>, BTreeMap<SelectQueryId, SelectStmt>) {
  panic!("Implement");
}

/// This function evaluates the given `expr`. The `subquery_vals`
/// that are provided must be sufficient to finish `expr`, otherwise
/// we throw an error.
fn evaluate_expr(
  subquery_vals: &BTreeMap<SelectQueryId, SelectView>,
  expr: &PostEvalExpr,
) -> Result<EvalLiteral, EvalErr> {
  panic!("TODO: implement");
}

fn evaluate_exprs(
  subquery_vals: &BTreeMap<SelectQueryId, SelectView>,
  exprs: &Vec<PostEvalExpr>,
) -> Result<Vec<EvalLiteral>, EvalErr> {
  trans_res(exprs, |expr| evaluate_expr(subquery_vals, expr))
}

/// This function verfies that the `col_name` is in the
/// schema of the `rel_tab`.
fn verify_col(col_name: &String, rel_tab: &RelationalTablet) -> Result<ColumnName, EvalErr> {
  panic!("TODO: implement")
}

fn verify_cols(
  col_names: &Vec<String>,
  rel_tab: &RelationalTablet,
) -> Result<Vec<ColumnName>, EvalErr> {
  trans_res(col_names, |col_name| verify_col(col_name, rel_tab))
}
/// Trivially converts an `EvalLiteral` to `Option<ColumnValue>`.
fn convert(eval_lit: EvalLiteral) -> Option<ColumnValue> {
  match eval_lit {
    EvalLiteral::Int(i32) => Some(ColumnValue::Int(i32)),
    EvalLiteral::Bool(bool) => Some(ColumnValue::Bool(bool)),
    EvalLiteral::String(string) => Some(ColumnValue::String(string)),
    EvalLiteral::Null => None,
  }
}

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
) {
  panic!("TODO: implement")
}

pub fn table_insert_row(
  rel_tab: &mut RelationalTablet,
  col_names: &Vec<ColumnName>,
  col_vals: Vec<EvalLiteral>,
  timestamp: &Timestamp,
) {
  panic!("TODO: implement")
}

pub fn table_insert_diff(rel_tab: &mut RelationalTablet, diff: &WriteDiff, timestamp: &Timestamp) {
  panic!("TODO: implement")
}

fn col_vals_into_diff(
  diff: &mut WriteDiff,
  col_names: &Vec<ColumnName>,
  col_vals: Vec<EvalLiteral>,
) {
  panic!("TODO: implement")
}

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

// -------------------------------------------------------------------------------------------------
//  Miscellaneous
// -------------------------------------------------------------------------------------------------

fn lookup<K: PartialEq + Eq, V: Clone>(vec: &Vec<(K, V)>, key: &K) -> Option<V> {
  for (k, v) in vec {
    if k == key {
      return Some(v.clone());
    }
  }
  return None;
}

/// This function evaluates the `EvalExpr`. This function replaces any `Subquery`
/// variants with a `SubqueryId` by sending out the actual subquery for execution.
/// It returns either another `EvalExpr` if we still need to wait for Subqueries
/// to execute, or a `Literal` if the valuation of `expr` is complete.
///
/// This adds whatever subqueries are necessary to evaluate the subqueries.
/// This should be errors. type errors.
fn evaluate_expr2(
  context: &Vec<(SelectQueryId, EvalLiteral)>,
  expr: PostEvalExpr,
) -> Result<EvalLiteral, EvalErr> {
  match expr {
    PostEvalExpr::BinaryExpr { op, lhs, rhs } => {
      let elhs = evaluate_expr2(context, *lhs)?;
      let erhs = evaluate_expr2(context, *rhs)?;
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
  let evaluate_expr2 =
    evaluate_columns_r(rand_gen, &mut subqueries, expr, rel_tab, key, timestamp)?;
  return Ok((evaluate_expr2, subqueries));
}

/// Suppose `col_name` is a column name in the schema of `rel_tab`. This
/// function finds all instances of `col_name` in `expr` and replaces it
/// with the value of that column in `rel_tab` for the row keyed by `key`
/// at the given `timestamp`.
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
