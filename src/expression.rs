use crate::model::common::iast::BinaryOp;
use crate::model::common::{iast, proc, ColBound, ColName, ColType, ColVal, ColValN, SingleBound};
use std::collections::HashMap;
use std::ops::Deref;

/*
Optimizations:
- For each key_col, return Vec<ColBound>. To compute KeyBound, we take the Cartesian product.
- We can pre-evaluate constant expressions before evaluating them with the `col_context`.
*/

enum EvalError {
  /// An invalid unary operation was attempted.
  InvalidUnaryOp,
  /// An invalid binary operation was attempted.
  InvalidBinaryOp,
  /// Placeholder error
  GenericError,
}

fn full_bound() -> ColBound {
  ColBound::new(SingleBound::Unbounded, SingleBound::Unbounded)
}

fn empty_bound(col_type: &ColType) -> ColBound {
  let single_val = match col_type {
    ColType::Int => ColVal::Int(0),
    ColType::Bool => ColVal::Bool(true),
    ColType::String => ColVal::String("".to_string()),
  };
  ColBound::new(SingleBound::Excluded(single_val.clone()), SingleBound::Excluded(single_val))
}

/// Evaluates the given `expr` only given some (potentially insufficient) `col_context`.
/// If the `col_context` is insufficient, we return None. We return an error if there
/// was a type error, or other fatal evaluation error.
fn eval_expr(
  expr: &proc::ValExpr,
  col_context: &HashMap<ColName, ColValN>,
) -> Result<Option<ColValN>, EvalError> {
  match expr {
    proc::ValExpr::ColumnRef { col_ref } => {
      if let Some(col_val) = col_context.get(col_ref) {
        Ok(Some(col_val.clone()))
      } else {
        Ok(None)
      }
    }
    proc::ValExpr::UnaryExpr { op, expr } => {
      if let Some(expr_val) = eval_expr(expr, col_context)? {
        match (op, expr_val) {
          (iast::UnaryOp::Plus, Some(ColVal::Int(val))) => Ok(Some(Some(ColVal::Int(val)))),
          (iast::UnaryOp::Minus, Some(ColVal::Int(val))) => Ok(Some(Some(ColVal::Int(-val)))),
          (iast::UnaryOp::Not, Some(ColVal::Bool(val))) => Ok(Some(Some(ColVal::Bool(!val)))),
          (iast::UnaryOp::IsNull, None) => Ok(Some(Some(ColVal::Bool(true)))),
          (iast::UnaryOp::IsNotNull, None) => Ok(Some(Some(ColVal::Bool(false)))),
          (iast::UnaryOp::IsNotNull, _) => Ok(Some(Some(ColVal::Bool(true)))),
          _ => Err(EvalError::InvalidUnaryOp),
        }
      } else {
        Ok(None)
      }
    }
    proc::ValExpr::BinaryExpr { op, left, right } => Err(EvalError::GenericError),
    proc::ValExpr::Value { val } => Err(EvalError::GenericError),
    proc::ValExpr::Subquery { query } => Err(EvalError::GenericError),
  }
}

/// The post-condition is that for all `ColVal`s that fall outside of the `ColBound`,
/// it should be possible to evaluate `expr` (given `col_context`) and it should
/// evaluate to `ColVal::Bool(false)`. Note that the bounds we compute need not
/// be the best ones; we will sacrifice quality for simplicity.
fn compute_singe_bound(
  col: &ColName,
  col_type: &ColType,
  expr: &proc::ValExpr,
  col_context: HashMap<ColName, ColValN>,
) -> Result<ColBound, EvalError> {
  match expr {
    proc::ValExpr::ColumnRef { col_ref } => {
      // If the `col_ref` is a part of `col_context` and it maps to `false`,
      // then all values of `col` will make `expr` `false`.
      if let Some(col_val) = col_context.get(col_ref) {
        if let Some(ColVal::Bool(false)) = col_val {
          return Ok(empty_bound(col_type));
        }
      }
      // Otherwise, return the Unbounded bound
      return Ok(full_bound());
    }
    proc::ValExpr::UnaryExpr { op, expr } => match (op, expr.deref()) {
      (iast::UnaryOp::IsNotNull, proc::ValExpr::Value { val: iast::Value::Null }) => {
        Ok(empty_bound(col_type))
      }
      _ => Ok(full_bound()),
    },
    proc::ValExpr::BinaryExpr { op, left, right } => Err(EvalError::GenericError),
    proc::ValExpr::Value { val } => Err(EvalError::GenericError),
    proc::ValExpr::Subquery { query } => Err(EvalError::GenericError),
  }
}

/// Similar to the above, but more granular since we return a vector of.
fn compute_bound(
  col: &ColName,
  col_type: &ColType,
  expr: &proc::ValExpr,
  col_context: HashMap<ColName, ColValN>,
) -> Result<Vec<ColBound>, EvalError> {
  // TODO: Complete
  Err(EvalError::GenericError)
}
