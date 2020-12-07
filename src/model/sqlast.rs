use serde::{Deserialize, Serialize};

// -------------------------------------------------------------------------------------------------
// Root AST Node
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Root {
  SqlStmt(SqlStmt),
  Test(Test),
}
// -------------------------------------------------------------------------------------------------
// Test AST Node
// -------------------------------------------------------------------------------------------------

/// An AST Node that can contain as it's children other AST
/// Nodes that we want to be able to easily construct for testing.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Test {
  ValExpr(ValExpr),
}

// -------------------------------------------------------------------------------------------------
// Sql AST Node
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SqlStmt {
  Select(SelectStmt),
  // Update(UpdateStmt),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SelectStmt {
  pub col_names: Vec<String>,
  pub table_name: String,
  pub where_clause: ValExpr,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct UpdateStmt {
  pub table_name: String,
  /// For now, we only support one column with one value (which
  /// can generally be an expression).
  pub set_col: String,
  pub set_val: ValExpr,
  pub where_clause: ValExpr,
}

/// This represnts common binary operations that can appear
/// between ValExprs.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp {
  And,
  Or,
  LT,
  LTE,
  E,
  GT,
  GTE,
}

/// This AST type represents a literal, like a quoted string,
/// int, decimal number, or NULL.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Literal {
  String(String),
  Int(String),
  Bool(bool),
  Null,
}

/// This AST type represents expressions that evaluate to
/// a value. This includes binary operations between literal types.
/// column values, and even subqueries (which, at runtime, must evalute
/// to a single value).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ValExpr {
  BinaryExpr {
    op: BinaryOp,
    lhs: Box<ValExpr>,
    rhs: Box<ValExpr>,
  },
  Literal(Literal),
  // For now, we don't assume it's possible to define symbol aliases
  // using `AS`. No colum names are qualified with a `.` before it.
  Column(String),
  Subquery(Box<SelectStmt>),
}
