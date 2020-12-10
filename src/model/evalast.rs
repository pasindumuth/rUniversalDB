use crate::model::common::SelectQueryId;
use crate::model::sqlast::SelectStmt;
use serde::{Deserialize, Serialize};

/// This module holds a similar datastructure to sqlast, except this
/// is used in the Transaction Processor to Evaluate a SQL Transaction,
/// rather than parse it.

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EvalSqlStmt {
  Select(EvalSelectStmt),
  Update(EvalUpdateStmt),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalSelectStmt {
  pub col_names: Vec<String>,
  pub table_name: String,
  pub where_clause: EvalExpr,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EvalUpdateStmt {
  pub table_name: String,
  /// For now, we only support one column with one value (which
  /// can generally be an expression).
  pub set_col: String,
  pub set_val: EvalExpr,
  pub where_clause: EvalExpr,
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
  String(String),
  Bool(bool),
  Null,
}

/// This AST type represents expressions that evaluate to
/// a value. This includes binary operations between literal types.
/// column values, and even subqueries (which, at runtime, must evalute
/// to a single value).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EvalExpr {
  BinaryExpr {
    op: EvalBinaryOp,
    lhs: Box<EvalExpr>,
    rhs: Box<EvalExpr>,
  },
  Literal(EvalLiteral),
  /// For now, we don't assume it's possible to define symbol aliases
  /// using `AS`. No colum names are qualified with a `.` before it.
  Column(String),
  /// Holds a Subquery from the SQL AST. This, and the the SubqueryId
  /// Variant, are the key differentiators of EvalSqlStmt structure
  /// and the SqlStmt.
  Subquery(Box<SelectStmt>),
  /// The SelectQueryId for when the Subquery is in the process of being
  /// processed.
  SubqueryId(SelectQueryId),
}
