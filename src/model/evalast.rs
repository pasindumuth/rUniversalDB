use crate::model::common::{
  ColumnName, ColumnValue, PrimaryKey, Row, SelectQueryId, SelectView, Timestamp, WriteDiff,
};
use crate::model::sqlast::SelectStmt;
use serde::export::fmt::Debug;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// This module holds a similar datastructure to sqlast, except this
/// is used in the Transaction Processor to Evaluate a SQL Transaction,
/// rather than parse it.

/// EvalStmt will actually just be a bunch of states in a state
/// machine, where there are many starts points, end points, and
/// paths. There are 2 types of expressions in this system. There
/// is PreEvalExpr, which hold Subquery(Box<SelectStmt>), and there
/// are SubqueryId(SelectQueryId). We will have an EvalTask that
/// holds one state at a time and all necessary SelectQueryIds,
/// and their returned values so that we can do a State Transition.
/// The set of `pending_subqueries` in `EvalTask` initially
/// encompasses all SubqueryIds in all PostEvalExpr of the current
/// state.

// -------------------------------------------------------------------------------------------------
// Top Level Tasks
// -------------------------------------------------------------------------------------------------

/// A variable of type WriteQueryTask takes on one of several different Task
/// Types at any point. When executing the WriteQueryTask, the Task Type that
/// the variables holds is called the "current Task". It will evolve, switching
/// between Task Types until it converges onto the WriteDoneTask variant.
/// (We can interpet this to be a trivial "Task" that just contains the
/// rows to be written. We call such tasks Trivial Tasks).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum WriteQueryTask {
  UpdateTask(UpdateTask),
  InsertTask(InsertTask),
  InsertSelectTask(InsertSelectTask),
  WriteDoneTask(WriteDiff),
}

/// This is similar to `WriteQueryTask` above, except it's for read
/// operations. When executing a SelectQueryTask, the variable holding
/// it will converge onto SelectDoneTask.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SelectQueryTask {
  SelectTask(SelectTask),
  SelectDoneTask(SelectView),
}

// -------------------------------------------------------------------------------------------------
// Select Evaluation State Machines
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SelectKeyTask {
  Start(SelectKeyStartTask),
  EvalWhere(SelectKeyEvalWhereTask),
  None(SelectKeyNoneTask),
  Done(SelectKeyDoneTask),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SelectKeyStartTask {
  pub sel_cols: Vec<ColumnName>,
  pub where_clause: PreEvalExpr,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SelectKeyEvalWhereTask {
  pub sel_cols: Vec<ColumnName>,
  pub where_clause: PostEvalExpr,
  pub pending_subqueries: BTreeMap<SelectQueryId, SelectStmt>,
  pub complete_subqueries: BTreeMap<SelectQueryId, SelectView>,
}

/// We get here if the WHERE clause evaluated to false.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SelectKeyNoneTask {}

/// We get here if the WHERE clause evaluated to false.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SelectKeyDoneTask {
  pub sel_cols: Vec<ColumnName>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SelectTask {
  pub key_tasks: BTreeMap<PrimaryKey, Holder<SelectKeyTask>>,
  pub select_view: SelectView,
  pub timestamp: Timestamp,
}

// -------------------------------------------------------------------------------------------------
// Update Evaluation State Machines
// -------------------------------------------------------------------------------------------------
// The Types in this section are used for evaluating UPDATE statements.

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum UpdateKeyTask {
  Start(UpdateKeyStartTask),
  EvalWhere(UpdateKeyEvalWhereTask),
  EvalVal(UpdateKeyEvalValTask),
  EvalConstraints(UpdateKeyEvalConstraintsTask),
  None(UpdateKeyNoneTask),
  Done(UpdateKeyDoneTask),
}

/// We start an UPDATE Query with this task.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateKeyStartTask {
  pub set_col: ColumnName,
  pub set_val: PreEvalExpr,
  pub where_clause: PreEvalExpr,
  pub table_constraints: Vec<PreEvalExpr>,
}

/// We always perform this task to evaluate the where clause.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateKeyEvalWhereTask {
  pub set_col: ColumnName,
  pub set_val: PreEvalExpr,
  pub where_clause: PostEvalExpr,
  pub table_constraints: Vec<PreEvalExpr>,
  pub pending_subqueries: BTreeMap<SelectQueryId, SelectStmt>,
  pub complete_subqueries: BTreeMap<SelectQueryId, SelectView>,
}

/// If the WHERE clause evaluates to true, we do this.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateKeyEvalValTask {
  pub set_col: ColumnName,
  pub set_val: PostEvalExpr,
  pub table_constraints: Vec<PreEvalExpr>,
  pub pending_subqueries: BTreeMap<SelectQueryId, SelectStmt>,
  pub complete_subqueries: BTreeMap<SelectQueryId, SelectView>,
}

/// If the `set_val` evaluates successful, we check to see that
/// the table constraints hold.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateKeyEvalConstraintsTask {
  pub set_col: ColumnName,
  pub set_val: EvalLiteral,
  pub table_constraints: Vec<PostEvalExpr>,
  pub pending_subqueries: BTreeMap<SelectQueryId, SelectStmt>,
  pub complete_subqueries: BTreeMap<SelectQueryId, SelectView>,
}

/// We get here if the WHERE clause evaluated to false.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateKeyNoneTask {}

/// If the table constraints all evaluate to true, then we get here.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateKeyDoneTask {
  pub set_col: ColumnName,
  pub set_val: EvalLiteral,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateTask {
  pub key_tasks: BTreeMap<PrimaryKey, Holder<UpdateKeyTask>>,
  pub key_vals: WriteDiff,
}

// -------------------------------------------------------------------------------------------------
// Insert Values Evaluation State Machines
// -------------------------------------------------------------------------------------------------
// The Types in this section are used for evaluating INSERT INTO ... VALUES statements.

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum InsertKeyTask {
  Start(InsertKeyStartTask),
  None(InsertKeyNoneTask),
  Done(InsertKeyDoneTask),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InsertKeyStartTask {
  pub insert_cols: Vec<ColumnName>,
  pub insert_vals: Vec<Vec<PreEvalExpr>>,
  pub table_constraints: Vec<PreEvalExpr>,
  pub pending_subqueries: BTreeMap<SelectQueryId, SelectStmt>,
  pub complete_subqueries: BTreeMap<SelectQueryId, SelectView>,
}

/// This states indicates that the WHERE clause evaluated
/// to false and nothing should be updated.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InsertKeyNoneTask {}

/// This state indicates that the WHERE clause evaluated
/// true, and that columns `set_col` should be set to value
/// `set_val`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InsertKeyDoneTask {
  pub insert_cols: Vec<ColumnName>,
  pub insert_vals: Vec<Vec<EvalLiteral>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InsertTask {
  pub tasks: BTreeMap<PrimaryKey, Holder<InsertKeyTask>>,
}

// -------------------------------------------------------------------------------------------------
// Insert Select Evaluation State Machines
// -------------------------------------------------------------------------------------------------
// The Types in this section are used for evaluating INSERT INTO ... Select statements.

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InsertSelectTask {
  pub insert_cols: Vec<ColumnName>,
  pub subquery: PreEvalExpr,
  pub table_constraints: Vec<PreEvalExpr>,
  pub pending_subqueries: BTreeMap<SelectQueryId, SelectStmt>,
  pub complete_subqueries: BTreeMap<SelectQueryId, SelectView>,
}

// -------------------------------------------------------------------------------------------------
// General Evaluation Types
// -------------------------------------------------------------------------------------------------
// These types are used to help define the evaluation states above.

/// This represnts common binary operations that can appear
/// between ValExprs.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EvalBinaryOp {
  AND,
  OR,
  LT,
  LTE,
  E,
  GT,
  GTE,
  PLUS,
  TIMES,
  MINUS,
  DIV,
}

/// This AST type represents a literal, like a quoted string,
/// int, decimal number, or NULL. Notice that column-values are no
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EvalLiteral {
  Int(i32),
  Bool(bool),
  String(String),
  Null,
}

/// Used before Subqueries have been sent out.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PreEvalExpr {
  BinaryExpr {
    op: EvalBinaryOp,
    lhs: Box<PreEvalExpr>,
    rhs: Box<PreEvalExpr>,
  },
  Literal(EvalLiteral),
  /// Holds a Subquery from the SQL AST. This, and the the SubqueryId
  /// Variant, are the key differentiators of EvalStmt structure
  /// and the SqlStmt.
  Subquery(Box<SelectStmt>),
}

/// Used after Subqueries have been sent out.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PostEvalExpr {
  BinaryExpr {
    op: EvalBinaryOp,
    lhs: Box<PostEvalExpr>,
    rhs: Box<PostEvalExpr>,
  },
  Literal(EvalLiteral),
  /// The SelectQueryId for when the Subquery is in the process of being
  /// processed.
  SubqueryId(SelectQueryId),
}

// -------------------------------------------------------------------------------------------------
// Misc
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Holder<T> {
  pub val: T,
}

impl<T> Holder<T> {
  pub fn from(val: T) -> Holder<T> {
    Holder { val }
  }
}
