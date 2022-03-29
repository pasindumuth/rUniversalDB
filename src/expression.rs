use crate::common::{
  lookup, BoundType, ColBound, KeyBound, PolyColBound, ReadRegion, SingleBound, WriteRegion,
  WriteRegionType,
};
use crate::model::common::proc::ValExpr;
use crate::model::common::{iast, proc, ColName, ColType, ColVal, ColValN};
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;
use std::ops::Deref;

#[cfg(test)]
#[path = "./expression_test.rs"]
mod expression_test;

// -----------------------------------------------------------------------------------------------
//  Expression Evaluation
// -----------------------------------------------------------------------------------------------

/**
 * Ad hoc `NULL` Handling:
 *
 * A shortcoming of this expression evaluation system is that `NULL` might be handled in
 * a Non-Standard way. Nevertheless, study and take inspiration from Postgres and choose a
 * sensible scheme that is simple to implement and useful for simulation testing.
 */

/// These primarily exist for testing the expression evaluation code. It's not used
/// by the system for decision making.
#[derive(Debug, PartialEq, Eq)]
pub enum EvalError {
  /// An invalid unary operation was attempted.
  InvalidUnaryOp,
  /// An invalid binary operation was attempted.
  InvalidBinaryOp,
  /// Placeholder error
  GenericError,
  /// Invalid subquery result.
  InvalidSubqueryResult,
  /// Invalid subquery result.
  NumberParseError,
  /// Invalid Boolean Expression leaf
  InvalidBoolExpr,
  /// Invalid Boolean Expression leaf
  TypeError,
  /// The `ValExpr` is not a Simple ValExpr.
  InvalidSimpleExpr,
}

/// This is the expression type we use to Compute a value (hence why it is called CExpr).
/// Note that there are no more `ColumnRef`s
#[derive(Debug)]
pub enum CExpr {
  UnaryExpr { op: iast::UnaryOp, expr: Box<CExpr> },
  BinaryExpr { op: iast::BinaryOp, left: Box<CExpr>, right: Box<CExpr> },
  Value { val: ColValN },
}

/// This is the expression type we use to evaluate KeyBounds. Recall that we generally only have
/// a subset of `ColumnRefs` with a known value; the remaining columns and subquery results are
/// all unknown. We replace the known ColumnRefs with `Value`, the ColumnRefs that are Key Cols
/// with a `KeyColumnRef`, and the unknown things with `Unknown`.
#[derive(Debug)]
pub enum KBExpr {
  KeyColumnRef { col_name: ColName },
  UnaryExpr { op: iast::UnaryOp, expr: Box<KBExpr> },
  BinaryExpr { op: iast::BinaryOp, left: Box<KBExpr>, right: Box<KBExpr> },
  Value { val: ColValN },
  UnknownValue,
}

/// This parses an `iast::Value` into a valid `ColValN`. Issues might arise if the string that's
/// representing an integer is too big or has non-digit characters.
pub fn construct_colvaln(val: iast::Value) -> Result<ColValN, EvalError> {
  let col_val = match val {
    iast::Value::Number(num_string) => {
      if let Ok(parsed_num) = num_string.parse::<i32>() {
        Some(ColVal::Int(parsed_num))
      } else {
        return Err(EvalError::GenericError);
      }
    }
    iast::Value::QuotedString(string_val) => Some(ColVal::String(string_val)),
    iast::Value::Boolean(bool_val) => Some(ColVal::Bool(bool_val)),
    iast::Value::Null => None,
  };
  Ok(col_val)
}

/// Construct a `CExpr` recursively from the given `sql_expr`. The `ColumnRefs` should be replaced
/// by the value in `col_map` (such a value should exist), and the `Subquery`s should be replaced
/// by the values in `subquery_vals` starting from `next_subquery_idx`. The `next_subquery_idx`
/// should be increment to point passed the final subquery_val that was used.
pub fn construct_cexpr(
  sql_expr: &proc::ValExpr,
  col_map: &BTreeMap<proc::ColumnRef, ColValN>,
  subquery_vals: &Vec<ColValN>,
  next_subquery_idx: &mut usize,
) -> Result<CExpr, EvalError> {
  let c_expr = match sql_expr {
    ValExpr::ColumnRef(col) => CExpr::Value { val: col_map.get(col).unwrap().clone() },
    ValExpr::UnaryExpr { op, expr } => CExpr::UnaryExpr {
      op: op.clone(),
      expr: Box::new(construct_cexpr(expr.deref(), col_map, subquery_vals, next_subquery_idx)?),
    },
    ValExpr::BinaryExpr { op, left, right } => CExpr::BinaryExpr {
      op: op.clone(),
      left: Box::new(construct_cexpr(left.deref(), col_map, subquery_vals, next_subquery_idx)?),
      right: Box::new(construct_cexpr(right.deref(), col_map, subquery_vals, next_subquery_idx)?),
    },
    ValExpr::Value { val } => CExpr::Value { val: construct_colvaln(val.clone())? },
    ValExpr::Subquery { .. } => {
      // Here, we simply take the next subquery and increment `next_subquery_idx`.
      let subquery_val = subquery_vals.get(*next_subquery_idx).unwrap().clone();
      *next_subquery_idx += 1;
      CExpr::Value { val: subquery_val }
    }
  };
  Ok(c_expr)
}

/// Construct a `CExpr` for Simple `ValExpr`s, which are `ValExpr`s where there
/// are no `ColumnRef`s or `Subquery`s.
pub fn construct_simple_cexpr(sql_expr: &proc::ValExpr) -> Result<CExpr, EvalError> {
  match sql_expr {
    ValExpr::ColumnRef(_) => Err(EvalError::InvalidSimpleExpr),
    ValExpr::UnaryExpr { op, expr } => {
      Ok(CExpr::UnaryExpr { op: op.clone(), expr: Box::new(construct_simple_cexpr(expr.deref())?) })
    }
    ValExpr::BinaryExpr { op, left, right } => Ok(CExpr::BinaryExpr {
      op: op.clone(),
      left: Box::new(construct_simple_cexpr(left.deref())?),
      right: Box::new(construct_simple_cexpr(right.deref())?),
    }),
    ValExpr::Value { val } => Ok(CExpr::Value { val: construct_colvaln(val.clone())? }),
    ValExpr::Subquery { .. } => Err(EvalError::InvalidSimpleExpr),
  }
}

/// Common function for evaluating a unary expression with fully-evaluated insides.
///
/// The `NULL` handling was observed with Postgres, where we applied the unary operations
/// to a column name (not the NULL keyword directly).
fn evaluate_unary_op(op: &iast::UnaryOp, expr: ColValN) -> Result<ColValN, EvalError> {
  match (op, expr) {
    // Plus
    (iast::UnaryOp::Plus, Some(ColVal::Int(val))) => Ok(Some(ColVal::Int(val))),
    (iast::UnaryOp::Plus, None) => Ok(None),
    // Minus
    (iast::UnaryOp::Minus, Some(ColVal::Int(val))) => Ok(Some(ColVal::Int(-val))),
    (iast::UnaryOp::Minus, None) => Ok(None),
    // Not
    (iast::UnaryOp::Not, Some(ColVal::Bool(val))) => Ok(Some(ColVal::Bool(!val))),
    (iast::UnaryOp::Not, None) => Ok(None), // Note that Postgres does not evaluate this to `false`
    // IsNull
    (iast::UnaryOp::IsNull, Some(_)) => Ok(Some(ColVal::Bool(false))),
    (iast::UnaryOp::IsNull, None) => Ok(Some(ColVal::Bool(true))),
    // IsNotNull
    (iast::UnaryOp::IsNotNull, Some(_)) => Ok(Some(ColVal::Bool(true))),
    (iast::UnaryOp::IsNotNull, None) => Ok(Some(ColVal::Bool(false))),
    // Invalid
    _ => Err(EvalError::InvalidUnaryOp),
  }
}

/// Common function for evaluating a binary expression with fully-evaluate left and right sides.
///
/// The `NULL` handling was observed with Postgres, where we applied the unary operations
/// to a column name (not the NULL keyword directly). Observe that for most operators, if either
/// side is `NULL`, we bubble that up.
fn evaluate_binary_op(
  op: &iast::BinaryOp,
  left: ColValN,
  right: ColValN,
) -> Result<ColValN, EvalError> {
  match (op, left, right) {
    // Plus
    (iast::BinaryOp::Plus, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Int(left_val + right_val)))
    }
    (iast::BinaryOp::Plus, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::Plus, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::Plus, None, None) => Ok(None),
    // Minus
    (iast::BinaryOp::Minus, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Int(left_val - right_val)))
    }
    (iast::BinaryOp::Minus, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::Minus, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::Minus, None, None) => Ok(None),
    // Multiply
    (iast::BinaryOp::Multiply, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Int(left_val * right_val)))
    }
    (iast::BinaryOp::Multiply, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::Multiply, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::Multiply, None, None) => Ok(None),
    // Divide
    (iast::BinaryOp::Divide, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      if right_val != 0 {
        Ok(Some(ColVal::Int(left_val / right_val)))
      } else {
        Err(EvalError::InvalidBinaryOp)
      }
    }
    (iast::BinaryOp::Divide, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::Divide, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::Divide, None, None) => Ok(None),
    // Modulus
    (iast::BinaryOp::Modulus, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      if right_val != 0 {
        Ok(Some(ColVal::Int(left_val % right_val)))
      } else {
        Err(EvalError::InvalidBinaryOp)
      }
    }
    (iast::BinaryOp::Modulus, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::Modulus, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::Modulus, None, None) => Ok(None),
    // StringConcat
    (
      iast::BinaryOp::StringConcat,
      Some(ColVal::String(left_val)),
      Some(ColVal::String(right_val)),
    ) => {
      let mut result = left_val.clone();
      result.extend(right_val.chars());
      Ok(Some(ColVal::String(result)))
    }
    (iast::BinaryOp::StringConcat, Some(ColVal::String(_)), None) => Ok(None),
    (iast::BinaryOp::StringConcat, None, Some(ColVal::String(_))) => Ok(None),
    (iast::BinaryOp::StringConcat, None, None) => Ok(None),
    // Gt
    (iast::BinaryOp::Gt, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val > right_val)))
    }
    (iast::BinaryOp::Gt, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::Gt, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::Gt, None, None) => Ok(None),
    // Lt
    (iast::BinaryOp::Lt, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val < right_val)))
    }
    (iast::BinaryOp::Lt, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::Lt, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::Lt, None, None) => Ok(None),
    // GtEq
    (iast::BinaryOp::GtEq, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val >= right_val)))
    }
    (iast::BinaryOp::GtEq, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::GtEq, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::GtEq, None, None) => Ok(None),
    // LtEq
    (iast::BinaryOp::LtEq, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val <= right_val)))
    }
    // TODO: define this for strings and bool.
    (iast::BinaryOp::LtEq, Some(ColVal::Int(_)), None) => Ok(None),
    (iast::BinaryOp::LtEq, None, Some(ColVal::Int(_))) => Ok(None),
    (iast::BinaryOp::LtEq, None, None) => Ok(None),
    // Spaceship
    (iast::BinaryOp::Spaceship, left_val, right_val) => {
      // Recall that unlike '=', this operator is NULL-safe.
      Ok(Some(ColVal::Bool(left_val == right_val)))
    }
    // Eq
    (iast::BinaryOp::Eq, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val == right_val)))
    }
    (iast::BinaryOp::Eq, Some(ColVal::Bool(left_val)), Some(ColVal::Bool(right_val))) => {
      Ok(Some(ColVal::Bool(left_val == right_val)))
    }
    (iast::BinaryOp::Eq, Some(ColVal::String(left_val)), Some(ColVal::String(right_val))) => {
      Ok(Some(ColVal::Bool(left_val == right_val)))
    }
    (iast::BinaryOp::Eq, Some(_), None) => Ok(None),
    (iast::BinaryOp::Eq, None, Some(_)) => Ok(None),
    (iast::BinaryOp::Eq, None, None) => Ok(None),
    // NotEq
    (iast::BinaryOp::NotEq, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val != right_val)))
    }
    (iast::BinaryOp::NotEq, Some(ColVal::Bool(left_val)), Some(ColVal::Bool(right_val))) => {
      Ok(Some(ColVal::Bool(left_val != right_val)))
    }
    (iast::BinaryOp::NotEq, Some(ColVal::String(left_val)), Some(ColVal::String(right_val))) => {
      Ok(Some(ColVal::Bool(left_val != right_val)))
    }
    (iast::BinaryOp::NotEq, Some(_), None) => Ok(None),
    (iast::BinaryOp::NotEq, None, Some(_)) => Ok(None),
    (iast::BinaryOp::NotEq, None, None) => Ok(None),
    // And
    (iast::BinaryOp::And, Some(ColVal::Bool(left_val)), Some(ColVal::Bool(right_val))) => {
      Ok(Some(ColVal::Bool(left_val && right_val)))
    }
    (iast::BinaryOp::And, Some(ColVal::Bool(_)), None) => Ok(None),
    (iast::BinaryOp::And, None, Some(ColVal::Bool(_))) => Ok(None),
    (iast::BinaryOp::And, None, None) => Ok(None),
    // Or
    // Here, if one side is `true`, the whole thing is `true`. Otherwise, if one side is
    // `NULL`, the whole thing is `NULL`. Otherwise, the whole thing is `false`.
    (iast::BinaryOp::Or, Some(ColVal::Bool(left_val)), Some(ColVal::Bool(right_val))) => {
      Ok(Some(ColVal::Bool(left_val || right_val)))
    }
    (iast::BinaryOp::Or, Some(ColVal::Bool(val)), None)
    | (iast::BinaryOp::Or, None, Some(ColVal::Bool(val))) => {
      if !val {
        Ok(None)
      } else {
        Ok(Some(ColVal::Bool(true)))
      }
    }
    (iast::BinaryOp::Or, None, None) => Ok(None),
    // Invalid
    _ => Err(EvalError::InvalidBinaryOp),
  }
}

/// This is a general expression evaluator.
pub fn evaluate_c_expr(c_expr: &CExpr) -> Result<ColValN, EvalError> {
  match c_expr {
    CExpr::UnaryExpr { op, expr } => evaluate_unary_op(op, evaluate_c_expr(expr.deref())?),
    CExpr::BinaryExpr { op, left, right } => {
      evaluate_binary_op(op, evaluate_c_expr(left.deref())?, evaluate_c_expr(right.deref())?)
    }
    CExpr::Value { val } => Ok(val.clone()),
  }
}

/// Given a Table with `key_cols` KeyCols and a SQL Query (e.g. a SELECT) with `source`
/// Data Source Name (i.e. FROM clause), this function checks whether the `ColumnRef` is
/// referring to a KeyCol in this Table.
fn contains_key_col_ref(
  source: &proc::GeneralSource,
  key_cols: &Vec<(ColName, ColType)>,
  col: &proc::ColumnRef,
) -> bool {
  if let Some(table_name) = &col.table_name {
    if table_name == source.name() {
      if lookup(key_cols, &col.col_name).is_some() {
        return true;
      }
    }
  } else {
    if lookup(key_cols, &col.col_name).is_some() {
      return true;
    }
  }
  false
}

/// Construct a `KBExpr` for evaluating KeyBounds. The `col_map` contains values for
/// columns which are known (i.e. the ColNames from the parent context that we should
/// use), and `key_cols` are the Key Columns of the Table.
///
/// Note that the `ColumnRef`s in `col_map` must evaluate `contains_key_col_ref` to false.
fn construct_kb_expr(
  expr: proc::ValExpr,
  col_map: &BTreeMap<proc::ColumnRef, ColValN>,
  source: &proc::GeneralSource,
  key_cols: &Vec<(ColName, ColType)>,
) -> Result<KBExpr, EvalError> {
  let kb_expr = match expr {
    ValExpr::ColumnRef(col) => {
      if let Some(val) = col_map.get(&col) {
        KBExpr::Value { val: val.clone() }
      } else {
        if contains_key_col_ref(source, key_cols, &col) {
          KBExpr::KeyColumnRef { col_name: col.col_name }
        } else {
          KBExpr::UnknownValue
        }
      }
    }
    ValExpr::UnaryExpr { op, expr } => {
      KBExpr::UnaryExpr { op, expr: Box::new(construct_kb_expr(*expr, col_map, source, key_cols)?) }
    }
    ValExpr::BinaryExpr { op, left, right } => KBExpr::BinaryExpr {
      op,
      left: Box::new(construct_kb_expr(*left, col_map, source, key_cols)?),
      right: Box::new(construct_kb_expr(*right, col_map, source, key_cols)?),
    },
    ValExpr::Value { val } => KBExpr::Value { val: construct_colvaln(val.clone())? },
    ValExpr::Subquery { .. } => KBExpr::UnknownValue,
  };
  Ok(kb_expr)
}

/// This evaluates a `KBExpr`s if all of its nodes are a `UnaryExpr`, `BinaryExpr`, or `Value`.
/// The evaluation would be identical to that of `CExpr`. If the `KBExpr` has other types of
/// nodes, then we return `None`. As usual, if there is certainly an invalid expression, we
/// return an error.
fn evaluate_kb_expr(kb_expr: &KBExpr) -> Result<Option<ColValN>, EvalError> {
  let val = match kb_expr {
    KBExpr::KeyColumnRef { .. } => None,
    KBExpr::UnaryExpr { op, expr } => {
      if let Some(val) = evaluate_kb_expr(expr.deref())? {
        Some(evaluate_unary_op(op, val)?)
      } else {
        None
      }
    }
    KBExpr::BinaryExpr { op, left, right } => {
      if let (Some(left_val), Some(right_val)) =
        (evaluate_kb_expr(left.deref())?, evaluate_kb_expr(right.deref())?)
      {
        Some(evaluate_binary_op(op, left_val, right_val)?)
      } else {
        None
      }
    }
    KBExpr::Value { val } => Some(val.clone()),
    KBExpr::UnknownValue => None,
  };
  Ok(val)
}

// -----------------------------------------------------------------------------------------------
//  Expression Evaluation Utilities
// -----------------------------------------------------------------------------------------------

/// This function simply deduces if the given `ColValN` should be interpreted as true
/// during query evaluation (e.g. when used in the WHERE clause). Recall that `NULL` should
/// be interpreted as false. Otherwise, `val` should be a bool type, and if it is not, we
/// return an error.
pub fn is_true(val: &ColValN) -> Result<bool, EvalError> {
  match val {
    Some(ColVal::Bool(bool_val)) => Ok(bool_val.clone()),
    None => Ok(false),
    _ => Err(EvalError::TypeError),
  }
}

// -----------------------------------------------------------------------------------------------
//  Keybound Computation
// -----------------------------------------------------------------------------------------------

/// This function returns `ColBound`s such that `ColVal`s of type `T` outside of these
/// bounds would surely evaluate `kb_expr` to `false` or `NULL`.
///
/// Importantly, `kb_expr` might not even be evaluatable for `ColVal` within the `ColBound`s
/// (i.e. there might be runtime errors or type errors). But that is not something we check here.
///
/// This is used to help determine a ColBound for <, <=, =, <=>, >=, > operators,
/// whose bounds are all determined very similarly. Here, `left_f` indicates what
/// `ColBound` should be returned if `col_name` is on the left, and the RHS evaluates
/// to a non-NULL value of compatible Type `T` (`right_f` is analogous).
///
/// Note that every possible `BoundType` (i.e. the Types that implement it) have the
/// comparison operators (including inequality) defined in Postgres. This includes
/// `String` and `bool`.
///
/// Generally, we use Postgres conventions.
///   1. If one of the sides is NULL, the `kb_expr` is always `NULL`.
fn boolean_leaf_constraint<T: BoundType + Clone>(
  kb_expr: &KBExpr,
  col_name: &ColName,
  left: &Box<KBExpr>,
  right: &Box<KBExpr>,
  left_f: fn(T) -> ColBound<T>,
  right_f: fn(T) -> ColBound<T>,
) -> Vec<ColBound<T>> {
  if let KBExpr::KeyColumnRef { col_name: key_col_name } = left.deref() {
    // First, we see if the left side is a ColumnRef to `col_name`.
    if key_col_name == col_name {
      // In this case, we see if we can evaluate the `right` expression.
      if let Ok(Some(right_val)) = evaluate_kb_expr(right.deref()) {
        if None == right_val {
          // If `right_val` is NULL, then `kb_expr` is always false.
          return vec![];
        } else if let Some(val) = T::col_valn_cast(right_val) {
          // If `right_val` has the correct Type, then we can compute a keybounds.
          return vec![left_f(val)];
        }
      }
    }
  } else if let KBExpr::KeyColumnRef { col_name: key_col_name } = right.deref() {
    // Otherwise, we see if the right side is a ColumnRef to `col_name`.
    if key_col_name == col_name {
      // In this case, we see if we can evaluate the `left` expression.
      if let Ok(Some(left_val)) = evaluate_kb_expr(left.deref()) {
        if None == left_val {
          // If `left_val` is NULL, then `kb_expr` is always false.
          return vec![];
        } else if let Some(val) = T::col_valn_cast(left_val) {
          // If `left_val` has the correct Type, then we can compute a keybounds.
          return vec![right_f(val)];
        }
      }
    }
  } else if let Ok(Some(val)) = evaluate_kb_expr(kb_expr) {
    if val == Some(ColVal::Bool(false)) || val == None {
      // Finally, we see if the KBExpr can be successfully evaluated, and it evaluates
      // to false (or NULL, which SQL interprets as false for boolean expressions), then
      // we can just return an empty bounds.
      return vec![];
    }
  }

  // Otherwise, we give up.
  return vec![full_bound::<T>()];
}

/// Computes `Vec<ColBound>` such that any `ColVal` not in any of these bounds with
/// type `T` substituted in the `kb_expr` wherever `col_name` shows up as a `KeyColumnRef`
/// would surely result the expression evaluating to `false` or `NULL`.
///
/// Importantly, `kb_expr` might not even be evaluatable for `ColVal` within the `ColBound`s
/// (i.e. there might be runtime errors or type errors). But that is not something we check here.
///
/// The essence of this function is AND and OR, particularly AND. An important observation in
/// Postgres was that for AND, if one side is `NULL` or `false`, then the whole expression is
/// interpreted as `Null` or `false` without evaluating the other side. Note that the other
/// side does not even have to evaluate successfully.
///
/// Read the Design Docs "Expression Evaluation" section for more information.
fn compute_col_bounds<T: Ord + BoundType + Clone>(
  kb_expr: &KBExpr,
  col_name: &ColName,
) -> Vec<ColBound<T>> {
  match kb_expr {
    KBExpr::KeyColumnRef { .. } => vec![full_bound::<T>()],
    KBExpr::UnaryExpr { .. } => vec![full_bound::<T>()],
    KBExpr::BinaryExpr { op, left, right } => match op {
      iast::BinaryOp::Plus => vec![full_bound::<T>()],
      iast::BinaryOp::Minus => vec![full_bound::<T>()],
      iast::BinaryOp::Multiply => vec![full_bound::<T>()],
      iast::BinaryOp::Divide => vec![full_bound::<T>()],
      iast::BinaryOp::Modulus => vec![full_bound::<T>()],
      iast::BinaryOp::StringConcat => vec![full_bound::<T>()],
      iast::BinaryOp::Gt => boolean_leaf_constraint(
        kb_expr,
        col_name,
        left,
        right,
        |val| ColBound::<T>::new(SingleBound::Excluded(val), SingleBound::Unbounded),
        |val| ColBound::<T>::new(SingleBound::Unbounded, SingleBound::Excluded(val)),
      ),
      iast::BinaryOp::Lt => boolean_leaf_constraint(
        kb_expr,
        col_name,
        left,
        right,
        |val| ColBound::<T>::new(SingleBound::Unbounded, SingleBound::Excluded(val)),
        |val| ColBound::<T>::new(SingleBound::Excluded(val), SingleBound::Unbounded),
      ),
      iast::BinaryOp::GtEq => boolean_leaf_constraint(
        kb_expr,
        col_name,
        left,
        right,
        |val| ColBound::<T>::new(SingleBound::Included(val), SingleBound::Unbounded),
        |val| ColBound::<T>::new(SingleBound::Unbounded, SingleBound::Included(val)),
      ),
      iast::BinaryOp::LtEq => boolean_leaf_constraint(
        kb_expr,
        col_name,
        left,
        right,
        |val| ColBound::<T>::new(SingleBound::Unbounded, SingleBound::Included(val)),
        |val| ColBound::<T>::new(SingleBound::Included(val), SingleBound::Unbounded),
      ),
      iast::BinaryOp::Spaceship | iast::BinaryOp::Eq => boolean_leaf_constraint(
        kb_expr,
        col_name,
        left,
        right,
        |val: T| ColBound::<T>::new(SingleBound::Included(val.clone()), SingleBound::Included(val)),
        |val: T| ColBound::<T>::new(SingleBound::Included(val.clone()), SingleBound::Included(val)),
      ),
      iast::BinaryOp::NotEq => vec![full_bound::<T>()],
      iast::BinaryOp::And => {
        let left_bounds = compute_col_bounds(left.deref(), col_name);
        let right_bounds = compute_col_bounds(right.deref(), col_name);
        merged_col_bounds(col_bounds_intersect(left_bounds, right_bounds))
      }
      iast::BinaryOp::Or => {
        let mut left_bounds = compute_col_bounds(left.deref(), col_name);
        let right_bounds = compute_col_bounds(right.deref(), col_name);
        left_bounds.extend(right_bounds);
        merged_col_bounds(left_bounds)
      }
    },
    KBExpr::Value { val } => match val {
      None => vec![],
      Some(ColVal::Bool(bool_val)) => {
        if *bool_val {
          vec![full_bound::<T>()]
        } else {
          vec![]
        }
      }
      Some(_) => vec![full_bound::<T>()],
    },
    KBExpr::UnknownValue => vec![full_bound::<T>()],
  }
}

/// Same as above, but uses the `col_type` and wraps each `ColBound` into a `PolyColBound`.
fn compute_poly_col_bounds(
  kb_expr: &KBExpr,
  col_name: &ColName,
  col_type: &ColType,
) -> Vec<PolyColBound> {
  match col_type {
    ColType::Int => compute_col_bounds::<i32>(&kb_expr, col_name)
      .into_iter()
      .map(|bound| PolyColBound::Int(bound))
      .collect(),
    ColType::Bool => compute_col_bounds::<bool>(&kb_expr, col_name)
      .into_iter()
      .map(|bound| PolyColBound::Bool(bound))
      .collect(),
    ColType::String => compute_col_bounds::<String>(&kb_expr, col_name)
      .into_iter()
      .map(|bound| PolyColBound::String(bound))
      .collect(),
  }
}

/// Computes a single, all full `PolyColBound` for the given `col_type`.
fn full_poly_col_bounds(col_type: &ColType) -> Vec<PolyColBound> {
  match col_type {
    ColType::Int => {
      let bound = ColBound { start: SingleBound::Unbounded, end: SingleBound::Unbounded };
      vec![(PolyColBound::Int(bound))]
    }
    ColType::Bool => {
      let bound = ColBound { start: SingleBound::Unbounded, end: SingleBound::Unbounded };
      vec![(PolyColBound::Bool(bound))]
    }
    ColType::String => {
      let bound = ColBound { start: SingleBound::Unbounded, end: SingleBound::Unbounded };
      vec![(PolyColBound::String(bound))]
    }
  }
}

/// Eliminates intersection between the `ColBound`s
fn merged_col_bounds<T: Ord + BoundType + Clone>(col_bounds: Vec<ColBound<T>>) -> Vec<ColBound<T>> {
  // TODO: Do this properly. This is a non-critical optimization.
  col_bounds
}

/// This function removes redundancy in the `row_region`. Redundancy may easily
/// arise from different ContextRows. In the future, we can be smarter and
/// sacrifice granularity for a simpler Key Region.
pub fn compress_row_region(row_region: Vec<KeyBound>) -> Vec<KeyBound> {
  row_region
}

/// For a given Table, for a given WHERE clause and FROM clause, this function computes
/// `Vec<KeyBound>` such that any PrimaryKey outside of it will certainly evaluate
/// the WHERE clause to `false` or `NULL`. The `key_cols` are the KeyCols of the Table,
/// the `expr` is the WHERE clause, and `source` is the Data Source Name in the FROM
/// clause, and `col_map` are some values that we can substitute some `ColumnRef`s in
/// the `expr` with. Importantly, none of these `ColumnRef`s should refer to a KeyCol (i.e.
/// `contains_key_col_ref` should evaluate to `false`).
///
/// This returns the compressed regions.
///
/// Importantly, `expr` might not even be evaluatable for some keys within the  `KeyBound` (i.e.
/// there might be runtime errors or type errors). But that is not something we check here.
pub fn compute_key_region(
  expr: &proc::ValExpr,
  col_map: BTreeMap<proc::ColumnRef, ColValN>,
  source: &proc::GeneralSource,
  key_cols: &Vec<(ColName, ColType)>,
) -> Vec<KeyBound> {
  // Enforce the precondition.
  debug_assert!((|| {
    for (col, _) in &col_map {
      if contains_key_col_ref(source, key_cols, &col) {
        return false;
      }
    }
    true
  })());

  let kb_expr_res = construct_kb_expr(expr.clone(), &col_map, source, key_cols);
  let mut key_bounds = Vec::<KeyBound>::new();

  // The strategy here is to start with an all-encompassing KeyRegion, then reduce the
  // KeyRegion for each key column independently.
  key_bounds.push(KeyBound { col_bounds: vec![] });
  for (col_name, col_type) in key_cols {
    // Then, compute the ColBound and extend key_bounds.
    let col_bounds = if let Ok(kb_expr) = &kb_expr_res {
      compute_poly_col_bounds(kb_expr, col_name, col_type)
    } else {
      full_poly_col_bounds(col_type)
    };
    let mut new_key_bounds = Vec::<KeyBound>::new();
    for key_bound in key_bounds {
      for col_bound in col_bounds.clone() {
        let mut new_key_bound = key_bound.clone();
        new_key_bound.col_bounds.push(col_bound);
        new_key_bounds.push(new_key_bound);
      }
    }
    key_bounds = compress_row_region(new_key_bounds);
  }

  key_bounds
}

// -----------------------------------------------------------------------------------------------
//  Region Intersection Utilities
// -----------------------------------------------------------------------------------------------

/// Essentially computes the intersection of 2 `ColBound`s, returning a pair
/// of `SingleBound`. Note that the first element can be greater than the second.
///
/// This function can easily be proven. Suppose C is the true intersect, and C' is the
/// interval that is returned. Observe C is in C', since the start of C' is the start of either
/// `left` or `right`, and same with the end. To prove that C' is in C, the only tricky case is
/// when `b1 > b2` or `b2 > b1` branches are taken to construct C'. See below for a brief
/// explanation.
/// TODO: prove this function formally.
fn col_bound_intersect_interval<'a, T: Ord>(
  left: &'a ColBound<T>,
  right: &'a ColBound<T>,
) -> (&'a SingleBound<T>, &'a SingleBound<T>) {
  let lower = match (&left.start, &right.start) {
    (SingleBound::Unbounded, _) => &right.start,
    (_, SingleBound::Unbounded) => &left.start,
    (SingleBound::Included(b1), SingleBound::Included(b2))
    | (SingleBound::Excluded(b1), SingleBound::Excluded(b2))
    | (SingleBound::Included(b1), SingleBound::Excluded(b2)) => {
      if b1 > b2 {
        // A one-sided interval, [b1, inf) must be a subset of (b2, inf) since if c is in
        // the former, then c >= b1 > b2, meaning it is in (b2, inf).
        &left.start
      } else {
        &right.start
      }
    }
    (SingleBound::Excluded(b1), SingleBound::Included(b2)) => {
      if b2 > b1 {
        // Similar reasoning as above
        &right.start
      } else {
        &left.start
      }
    }
  };
  let upper = match (&left.end, &right.end) {
    (SingleBound::Unbounded, _) => &right.end,
    (_, SingleBound::Unbounded) => &left.end,
    (SingleBound::Included(b1), SingleBound::Included(b2))
    | (SingleBound::Excluded(b1), SingleBound::Excluded(b2))
    | (SingleBound::Included(b1), SingleBound::Excluded(b2)) => {
      if b2 > b1 {
        // Similar reasoning as above
        &left.end
      } else {
        &right.end
      }
    }
    (SingleBound::Excluded(b1), SingleBound::Included(b2)) => {
      if b1 > b2 {
        // Similar reasoning as above
        &right.end
      } else {
        &left.end
      }
    }
  };
  (lower, upper)
}

/// Returns true if the `interval` is surely empty. If it returns false, then its very likely
/// that the set is non-empty, but it might be.
///
/// To see how we can have a false negative, consider the case that T is `bool`, and consider the
/// bound `(SingleBound::Excluded(false), SingleBound::Excluded(true))`. Although this set
/// is empty, the below function will return false (since the last branch is taken, and since
/// `false >= true` is a false statement).
fn is_surely_interval_empty<T: Ord>(interval: (&SingleBound<T>, &SingleBound<T>)) -> bool {
  match interval {
    (SingleBound::Unbounded, _) => false,
    (_, SingleBound::Unbounded) => false,
    (SingleBound::Included(b1), SingleBound::Included(b2)) => b1 > b2,
    (SingleBound::Excluded(b1), SingleBound::Excluded(b2))
    | (SingleBound::Included(b1), SingleBound::Excluded(b2))
    | (SingleBound::Excluded(b1), SingleBound::Included(b2)) => b1 >= b2,
  }
}

/// This computes the intersection of 2 sets of `ColBound`s
fn col_bounds_intersect<T: Ord + Clone>(
  left_bounds: Vec<ColBound<T>>,
  right_bounds: Vec<ColBound<T>>,
) -> Vec<ColBound<T>> {
  let mut intersection = Vec::<ColBound<T>>::new();
  for left_bound in &left_bounds {
    for right_bound in &right_bounds {
      let (lower, upper) = col_bound_intersect_interval(left_bound, right_bound);
      if !is_surely_interval_empty((lower, upper)) {
        intersection.push(ColBound { start: lower.clone(), end: upper.clone() })
      }
    }
  }
  intersection
}

/// Take the complement of a `ColBound`.
fn invert_col_bound<T: Ord>(bound: ColBound<T>) -> Vec<ColBound<T>> {
  let intervals = match (bound.start, bound.end) {
    (SingleBound::Unbounded, SingleBound::Unbounded) => vec![],
    (SingleBound::Unbounded, SingleBound::Included(b)) => {
      vec![(SingleBound::Excluded(b), SingleBound::Unbounded)]
    }
    (SingleBound::Unbounded, SingleBound::Excluded(b)) => {
      vec![(SingleBound::Included(b), SingleBound::Unbounded)]
    }
    (SingleBound::Included(b), SingleBound::Unbounded) => {
      vec![(SingleBound::Unbounded, SingleBound::Excluded(b))]
    }
    (SingleBound::Included(b1), SingleBound::Included(b2)) => {
      if b1 > b2 {
        vec![(SingleBound::Unbounded, SingleBound::Unbounded)]
      } else {
        vec![
          (SingleBound::Unbounded, SingleBound::Excluded(b1)),
          (SingleBound::Excluded(b2), SingleBound::Unbounded),
        ]
      }
    }
    (SingleBound::Included(b1), SingleBound::Excluded(b2)) => {
      if b1 >= b2 {
        vec![(SingleBound::Unbounded, SingleBound::Unbounded)]
      } else {
        vec![
          (SingleBound::Unbounded, SingleBound::Excluded(b1)),
          (SingleBound::Included(b2), SingleBound::Unbounded),
        ]
      }
    }
    (SingleBound::Excluded(b), SingleBound::Unbounded) => {
      vec![(SingleBound::Unbounded, SingleBound::Included(b))]
    }
    (SingleBound::Excluded(b1), SingleBound::Included(b2)) => {
      if b1 >= b2 {
        vec![(SingleBound::Unbounded, SingleBound::Unbounded)]
      } else {
        vec![
          (SingleBound::Unbounded, SingleBound::Included(b1)),
          (SingleBound::Excluded(b2), SingleBound::Unbounded),
        ]
      }
    }
    (SingleBound::Excluded(b1), SingleBound::Excluded(b2)) => {
      if b1 >= b2 {
        vec![(SingleBound::Unbounded, SingleBound::Unbounded)]
      } else {
        vec![
          (SingleBound::Unbounded, SingleBound::Included(b1)),
          (SingleBound::Included(b2), SingleBound::Unbounded),
        ]
      }
    }
  };
  intervals.into_iter().map(|(start, end)| ColBound { start, end }).collect()
}

/// This function computes the complement of this `bounds`.
fn invert_col_bounds<T: Ord + Clone>(bounds: Vec<ColBound<T>>) -> Vec<ColBound<T>> {
  // Recall that the complement of a union is the intersect of the complements.
  let mut cum = vec![full_bound::<T>()];
  for bound in bounds {
    cum = col_bounds_intersect(cum, invert_col_bound(bound));
  }
  cum
}

/// A convenience function for concisely computing a full ColBound.
fn full_bound<T>() -> ColBound<T> {
  ColBound::<T>::new(SingleBound::Unbounded, SingleBound::Unbounded)
}

// -----------------------------------------------------------------------------------------------
//  Region Isolation Property Utilities
// -----------------------------------------------------------------------------------------------

/// Computes whether the `Vec<ColName>`s have a common value.
fn does_col_regions_intersect(col_region1: &Vec<ColName>, col_region2: &Vec<ColName>) -> bool {
  for col in col_region1 {
    if col_region2.contains(col) {
      return true;
    }
  }
  false
}

/// Returns true if the `ColBound`s surely intersect.
fn might_col_intersect<T: Ord>(bound1: &ColBound<T>, bound2: &ColBound<T>) -> bool {
  let interval = col_bound_intersect_interval(bound1, bound2);
  !is_surely_interval_empty(interval)
}

/// Returns true if the `KeyBound`s surely intersect.
fn might_key_bound_intersect(key_bound1: &KeyBound, key_bound2: &KeyBound) -> bool {
  assert_eq!(key_bound1.col_bounds.len(), key_bound2.col_bounds.len());
  for (pc1, pc2) in key_bound1.col_bounds.iter().zip(key_bound2.col_bounds.iter()) {
    let might_col_bound_intersect = match (pc1, pc2) {
      (PolyColBound::Int(c1), PolyColBound::Int(c2)) => might_col_intersect(c1, c2),
      (PolyColBound::Bool(c1), PolyColBound::Bool(c2)) => might_col_intersect(c1, c2),
      (PolyColBound::String(c1), PolyColBound::String(c2)) => might_col_intersect(c1, c2),
      _ => panic!(),
    };
    if !might_col_bound_intersect {
      return false;
    }
  }
  true
}

/// Computes whether the given row regions surely intersect. The schemas of the `KeyBounds` should
/// match, i.e. they should all be the same length and every corresponding element must have
/// the same type. Returning false might be a false negative under rare circumstances.
fn might_row_region_intersect(row_region1: &Vec<KeyBound>, row_region2: &Vec<KeyBound>) -> bool {
  for key_bound1 in row_region1 {
    for key_bound2 in row_region2 {
      if might_key_bound_intersect(key_bound1, key_bound2) {
        return true;
      }
    }
  }
  false
}

/// Returns true if this `WriteRegion` surely has the Region Isolation Property with the
/// `ReadRegion`. Returning false might be a false negative under rare circumstances.
fn is_surely_isolated(write_region: &WriteRegion, read_region: &ReadRegion) -> bool {
  if might_row_region_intersect(&read_region.row_region, &write_region.row_region) {
    if write_region.presence {
      false
    } else {
      !does_col_regions_intersect(&read_region.val_col_region, &write_region.val_col_region)
    }
  } else {
    true
  }
}

/// Returns true if this `WriteRegion` surely has the Region Isolation Property with these
/// `ReadRegion`s. Returning false might be a false negative under rare circumstances.
pub fn is_surely_isolated_multiread(
  write_region: &WriteRegion,
  read_regions: &BTreeSet<ReadRegion>,
) -> bool {
  for read_region in read_regions {
    if !is_surely_isolated(&write_region, &read_region) {
      return false;
    }
  }
  true
}

/// Returns true if these `WriteRegion`s surely has the Region Isolation Property with the
/// `ReadRegion`. Returning false might be a false negative under rare circumstances.
pub fn is_surely_isolated_multiwrite(
  write_regions: &BTreeSet<WriteRegion>,
  read_region: &ReadRegion,
) -> bool {
  for write_region in write_regions {
    if !is_surely_isolated(&write_region, &read_region) {
      return false;
    }
  }
  true
}
