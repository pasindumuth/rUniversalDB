use crate::model::common::{ColumnName, SelectQueryId};
use crate::model::sqlast::{SelectStmt, UpdateStmt};
use serde::{Deserialize, Serialize};

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

/// Creating an EvalSelectStmt1 is done during the column
/// reading phase, which is distinct from all these phases,
/// which are only about waiting for Subquery results.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EvalSelect {
  Select1(EvalSelectStmt1),
  Select2(EvalSelectStmt2),
  Select3(EvalSelectStmt3),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EvalUpdate {
  Update1(EvalUpdateStmt1),
  Update2(EvalUpdateStmt2),
  Update3(EvalUpdateStmt3),
  Update4(EvalUpdateStmt4),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalSelectStmt1 {
  pub col_names: Vec<ColumnName>,
  pub where_clause: PreEvalExpr,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalSelectStmt2 {
  pub col_names: Vec<ColumnName>,
  pub where_clause: PostEvalExpr,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalSelectStmt3 {
  pub col_names: Vec<ColumnName>,
  pub where_clause: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalUpdateStmt1 {
  pub set_col: ColumnName,
  pub set_val: PreEvalExpr,
  pub where_clause: PreEvalExpr,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalUpdateStmt2 {
  pub set_col: ColumnName,
  pub set_val: PreEvalExpr,
  pub where_clause: PostEvalExpr,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalUpdateStmt3 {
  pub set_col: ColumnName,
  pub set_val: PostEvalExpr,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalUpdateStmt4 {
  pub set_col: ColumnName,
  pub set_val: EvalLiteral,
}

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
