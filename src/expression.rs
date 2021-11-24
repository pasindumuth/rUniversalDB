use crate::common::{
  ColBound, KeyBound, PolyColBound, ReadRegion, SingleBound, WriteRegion, WriteRegionType,
};
use crate::model::common::proc::ValExpr;
use crate::model::common::{iast, proc, ColName, ColType, ColVal, ColValN};
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;
use std::ops::Deref;

// -----------------------------------------------------------------------------------------------
//  Expression Evaluation
// -----------------------------------------------------------------------------------------------

/// These primarily exist for testing the expression evaluation code. It's not used
/// by the system for decision making.
#[derive(Debug)]
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
}

/// This is the expression type we use to Compute a value (hence why it's called CExpr).
#[derive(Debug)]
pub enum CExpr {
  UnaryExpr { op: iast::UnaryOp, expr: Box<CExpr> },
  BinaryExpr { op: iast::BinaryOp, left: Box<CExpr>, right: Box<CExpr> },
  Value { val: ColValN },
}

/// This is the expression type we use to evaluate KeyBounds. Recall that we generally only have
/// a subset of ColumnRefs with a known value; the remaining columns and subquery results are
/// all unknown. We replace the known ColumnRefs with `Value`, the Key Columns with a `ColumnRef`,
/// and the unknown things with `Unknown`.
#[derive(Debug)]
pub enum KBExpr {
  ColumnRef { col_ref: ColName },
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
  col_map: &BTreeMap<ColName, ColValN>,
  subquery_vals: &Vec<ColValN>,
  next_subquery_idx: &mut usize,
) -> Result<CExpr, EvalError> {
  let c_expr = match sql_expr {
    ValExpr::ColumnRef { col_ref } => CExpr::Value { val: col_map.get(col_ref).unwrap().clone() },
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

/// Common function for evaluating a unary expression with fully-evaluate insides.
fn evaluate_unary_op(op: &iast::UnaryOp, expr: ColValN) -> Result<ColValN, EvalError> {
  match (op, expr) {
    (iast::UnaryOp::Plus, Some(ColVal::Int(val))) => Ok(Some(ColVal::Int(val))),
    (iast::UnaryOp::Minus, Some(ColVal::Int(val))) => Ok(Some(ColVal::Int(-val))),
    (iast::UnaryOp::Not, Some(ColVal::Bool(val))) => Ok(Some(ColVal::Bool(!val))),
    (iast::UnaryOp::IsNull, None) => Ok(Some(ColVal::Bool(true))),
    (iast::UnaryOp::IsNull, Some(_)) => Ok(Some(ColVal::Bool(false))),
    (iast::UnaryOp::IsNotNull, None) => Ok(Some(ColVal::Bool(false))),
    (iast::UnaryOp::IsNotNull, Some(_)) => Ok(Some(ColVal::Bool(true))),
    _ => Err(EvalError::InvalidUnaryOp),
  }
}

/// Common function for evaluating a binary expression with fully-evaluate left and right sides.
fn evaluate_binary_op(
  op: &iast::BinaryOp,
  left: ColValN,
  right: ColValN,
) -> Result<ColValN, EvalError> {
  match (op, left, right) {
    (iast::BinaryOp::Plus, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Int(left_val + right_val)))
    }
    (iast::BinaryOp::Minus, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Int(left_val - right_val)))
    }
    (iast::BinaryOp::Multiply, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Int(left_val * right_val)))
    }
    (iast::BinaryOp::Divide, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      if right_val != 0 && left_val % right_val == 0 {
        Ok(Some(ColVal::Int(left_val / right_val)))
      } else {
        Err(EvalError::InvalidBinaryOp)
      }
    }
    (iast::BinaryOp::Modulus, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Int(left_val % right_val)))
    }
    (
      iast::BinaryOp::StringConcat,
      Some(ColVal::String(left_val)),
      Some(ColVal::String(right_val)),
    ) => {
      let mut result = left_val.clone();
      result.extend(right_val.chars());
      Ok(Some(ColVal::String(result)))
    }
    (iast::BinaryOp::Gt, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val > right_val)))
    }
    (iast::BinaryOp::Lt, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val < right_val)))
    }
    (iast::BinaryOp::GtEq, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val >= right_val)))
    }
    (iast::BinaryOp::LtEq, Some(ColVal::Int(left_val)), Some(ColVal::Int(right_val))) => {
      Ok(Some(ColVal::Bool(left_val <= right_val)))
    }
    (iast::BinaryOp::Spaceship, left_val, right_val) => {
      // Recall that unlike '=', this operator is NULL-safe.
      Ok(Some(ColVal::Bool(left_val == right_val)))
    }
    (iast::BinaryOp::Eq, left_val, right_val) => {
      if left_val.is_none() || right_val.is_none() {
        // Recall that the '=' operator returns NULL if one of the sides is NULL.
        Ok(None)
      } else {
        Ok(Some(ColVal::Bool(left_val == right_val)))
      }
    }
    (iast::BinaryOp::NotEq, left_val, right_val) => Ok(Some(ColVal::Bool(left_val != right_val))),
    (iast::BinaryOp::And, Some(ColVal::Bool(left_val)), Some(ColVal::Bool(right_val))) => {
      Ok(Some(ColVal::Bool(left_val && right_val)))
    }
    (iast::BinaryOp::Or, Some(ColVal::Bool(left_val)), Some(ColVal::Bool(right_val))) => {
      Ok(Some(ColVal::Bool(left_val || right_val)))
    }
    _ => Err(EvalError::InvalidBinaryOp),
  }
}

// This is a general expression evaluator.
pub fn evaluate_c_expr(c_expr: &CExpr) -> Result<ColValN, EvalError> {
  match c_expr {
    CExpr::UnaryExpr { op, expr } => evaluate_unary_op(op, evaluate_c_expr(expr.deref())?),
    CExpr::BinaryExpr { op, left, right } => {
      evaluate_binary_op(op, evaluate_c_expr(left.deref())?, evaluate_c_expr(right.deref())?)
    }
    CExpr::Value { val } => Ok(val.clone()),
  }
}

/// Construct a KBExpr for evaluating KeyBounds. The `col_map` contains values for
/// columns which are known (i.e. the ColNames from the parent context that we should
/// use), and `key_cols` are the Key Columns of the Table.  
pub fn construct_kb_expr(
  expr: proc::ValExpr,
  col_map: &BTreeMap<ColName, ColValN>,
  key_cols: &Vec<ColName>,
) -> Result<KBExpr, EvalError> {
  let kb_expr = match expr {
    ValExpr::ColumnRef { col_ref } => {
      if let Some(val) = col_map.get(&col_ref) {
        KBExpr::Value { val: val.clone() }
      } else if key_cols.contains(&col_ref) {
        KBExpr::ColumnRef { col_ref }
      } else {
        KBExpr::UnknownValue
      }
    }
    ValExpr::UnaryExpr { op, expr } => {
      KBExpr::UnaryExpr { op, expr: Box::new(construct_kb_expr(*expr, col_map, key_cols)?) }
    }
    ValExpr::BinaryExpr { op, left, right } => KBExpr::BinaryExpr {
      op,
      left: Box::new(construct_kb_expr(*left, col_map, key_cols)?),
      right: Box::new(construct_kb_expr(*right, col_map, key_cols)?),
    },
    ValExpr::Value { val } => KBExpr::Value { val: construct_colvaln(val.clone())? },
    ValExpr::Subquery { .. } => KBExpr::UnknownValue,
  };
  Ok(kb_expr)
}

// This evaluates KBExprs, which is the same as CExpr, except if any sub-expr is an
// unknown value, then this returns an empty optional.
pub fn evaluate_kb_expr(kb_expr: &KBExpr) -> Result<Option<ColValN>, EvalError> {
  let val = match kb_expr {
    KBExpr::ColumnRef { .. } => None,
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

/// This function simply deduces if the given `ColValN` sould be interpreted as true
/// during query evaluation (e.g. when used in the WHERE clause). An error is returned
/// if `val` isn't a Bool type.
pub fn is_true(val: &ColValN) -> Result<bool, EvalError> {
  match val {
    Some(ColVal::Bool(bool_val)) => Ok(bool_val.clone()),
    _ => Ok(false),
  }
}

// -----------------------------------------------------------------------------------------------
//  Keybound Computation
// -----------------------------------------------------------------------------------------------

/// This trait is used to cast the `ColValN` values down to their underlying
/// types. This is necessary for expression evaluation.
pub trait BoundType: Sized {
  // We need `: Sized` here, otherwise for the `Result`, we get: "the size for values of type
  // `Self` cannot be known at compilation time". (Note that if we just returned `Self`, we
  // wouldn't get this error.)
  fn col_val_cast(col_val: ColValN) -> Option<Self>;
}

impl BoundType for i32 {
  fn col_val_cast(col_val: ColValN) -> Option<Self> {
    if let Some(ColVal::Int(val)) = col_val {
      Some(val)
    } else {
      None
    }
  }
}

impl BoundType for bool {
  fn col_val_cast(col_val: ColValN) -> Option<Self> {
    if let Some(ColVal::Bool(val)) = col_val {
      Some(val)
    } else {
      None
    }
  }
}
impl BoundType for String {
  fn col_val_cast(col_val: ColValN) -> Option<Self> {
    if let Some(ColVal::String(val)) = col_val {
      Some(val)
    } else {
      None
    }
  }
}

/// This is used to help determine a ColBound for <, <=, =, <=>, >=, > operators,
/// whose bounds are all determined very similarly. Here, `left_f` indicates what
/// `ColBound` should be returned if `col_name` is on the left, and the RHS evaluates
/// to a non-NULL value of compatible Type `T` (`right_f` is analogous).
///
/// Generally, we use Postgres conventions.
///   1. If one of the sides is NULL, the `kb_expr` is always false.
///   2. If there is a Type mismatch, then this is a fatal runtime error.
pub fn boolean_leaf_constraint<T: BoundType + Clone>(
  kb_expr: &KBExpr,
  col_name: &ColName,
  left: &Box<KBExpr>,
  right: &Box<KBExpr>,
  left_f: fn(T) -> ColBound<T>,
  right_f: fn(T) -> ColBound<T>,
) -> Result<Vec<ColBound<T>>, EvalError> {
  if let KBExpr::ColumnRef { col_ref } = left.deref() {
    // First, we see if the left side is a ColumnRef to `col_name`.
    if col_ref == col_name {
      // In this case, we see if we can evaluate the `right` expression.
      if let Some(right_val) = evaluate_kb_expr(right.deref())? {
        return if None == right_val {
          // If `right_val` is NULL, then `kb_expr` is always false.
          Ok(vec![])
        } else if let Some(val) = T::col_val_cast(right_val) {
          // If `right_val` has the correct Type, then we can compute a keybounds.
          Ok(vec![left_f(val)])
        } else {
          // Otherwise, we detected a TypeError.
          Err(EvalError::TypeError)
        };
      }
    }
  } else if let KBExpr::ColumnRef { col_ref } = right.deref() {
    // Otherwise, we see if the right side is a ColumnRef to `col_name`.
    if col_ref == col_name {
      // In this case, we see if we can evaluate the `left` expression.
      if let Some(left_val) = evaluate_kb_expr(left.deref())? {
        return if None == left_val {
          // If `left_val` is NULL, then `kb_expr` is always false.
          Ok(vec![])
        } else if let Some(val) = T::col_val_cast(left_val) {
          // If `left_val` has the correct Type, then we can compute a keybounds.
          Ok(vec![right_f(val)])
        } else {
          // Otherwise, we detected a TypeError.
          Err(EvalError::TypeError)
        };
      }
    }
  } else if let Some(val) = evaluate_kb_expr(kb_expr)? {
    if val == Some(ColVal::Bool(false)) || val == None {
      // Finally, we see if the KBExpr can be successfully evaluated, and it evaluates
      // to false (or NULL, which SQL interprets as false for boolean expressions), then
      // we can just return an empty bounds.
      return Ok(vec![]);
    }
  }

  // Otherwise, we give up.
  return Ok(vec![full_bound::<T>()]);
}

/// This function expects the  `kb_expr` to be a boolean expression (with potentially
/// `UnknownValue`s). It then computes a `Vec<ColBound>` such that any `ColValN`
/// outside of these bounds for the `col_name` cannot possibly result in `kb_expr`
/// evaluating to true, no matter what the `UnknownValue`s and other `ColumnRef`s take on.
fn compute_col_bounds<T: Ord + BoundType + Clone>(
  kb_expr: &KBExpr,
  col_name: &ColName,
) -> Result<Vec<ColBound<T>>, EvalError> {
  match kb_expr {
    KBExpr::ColumnRef { .. } => Err(EvalError::InvalidBoolExpr),
    KBExpr::UnaryExpr { op, expr } => {
      let inner_bounds = compute_col_bounds::<T>(expr.deref(), col_name)?;
      match op {
        iast::UnaryOp::Plus => Err(EvalError::InvalidBoolExpr),
        iast::UnaryOp::Minus => Err(EvalError::InvalidBoolExpr),
        iast::UnaryOp::Not => Ok(invert_col_bounds(inner_bounds)),
        iast::UnaryOp::IsNull => {
          if let Some(Some(_)) = evaluate_kb_expr(expr.deref())? {
            // If the `expr` definitely evaluates to some non-NULL value, then this `kb_expr`
            // definitely evaluates to false for every value of `col_name`. Thus, we return
            // an empty bounds.
            Ok(vec![])
          } else {
            Ok(vec![full_bound::<T>()])
          }
        }
        iast::UnaryOp::IsNotNull => {
          if let Some(None) = evaluate_kb_expr(expr.deref())? {
            // If the `expr` definitely evaluates to NULL, then this `kb_expr` definitely
            // evaluates to false for every value of `col_name`. Thus, we return an empty bounds.
            Ok(vec![])
          } else {
            Ok(vec![full_bound::<T>()])
          }
        }
      }
    }
    KBExpr::BinaryExpr { op, left, right } => {
      match op {
        iast::BinaryOp::Plus => Err(EvalError::InvalidBoolExpr),
        iast::BinaryOp::Minus => Err(EvalError::InvalidBoolExpr),
        iast::BinaryOp::Multiply => Err(EvalError::InvalidBoolExpr),
        iast::BinaryOp::Divide => Err(EvalError::InvalidBoolExpr),
        iast::BinaryOp::Modulus => Err(EvalError::InvalidBoolExpr),
        iast::BinaryOp::StringConcat => Err(EvalError::InvalidBoolExpr),
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
          |val: T| {
            ColBound::<T>::new(SingleBound::Included(val.clone()), SingleBound::Included(val))
          },
          |val: T| {
            ColBound::<T>::new(SingleBound::Included(val.clone()), SingleBound::Included(val))
          },
        ),
        iast::BinaryOp::NotEq => {
          // For simplicity, we don't try dig into the sides of binary operator. Note that this
          // this renders `NotEq` fairly useless for decreasing ColBound.
          if Some(Some(ColVal::Bool(false))) == evaluate_kb_expr(kb_expr)? {
            // If the KBExpr can be evaluated succesfully, and it evalutes to false, then we
            // can just return an empty bounds.
            Ok(vec![])
          } else {
            // Otherwise, we give up.
            Ok(vec![full_bound::<T>()])
          }
        }
        iast::BinaryOp::And => {
          let left_bounds = compute_col_bounds(left.deref(), col_name)?;
          let right_bounds = compute_col_bounds(right.deref(), col_name)?;
          Ok(col_bounds_intersect(left_bounds, right_bounds))
        }
        iast::BinaryOp::Or => {
          let mut left_bounds = compute_col_bounds(left.deref(), col_name)?;
          let right_bounds = compute_col_bounds(right.deref(), col_name)?;
          left_bounds.extend(right_bounds);
          Ok(left_bounds)
        }
      }
    }
    KBExpr::Value { val } => {
      match val {
        None => Ok(vec![]),
        Some(ColVal::Bool(bool_val)) => {
          if *bool_val {
            Ok(vec![full_bound::<T>()])
          } else {
            Ok(vec![])
          }
        }
        Some(_) => {
          // Recall that only `ColVal::Bool` and NULL are permissing for boolean expressions.
          Err(EvalError::InvalidBoolExpr)
        }
      }
    }
    KBExpr::UnknownValue => {
      // Here, we merely don't know what the value is, so we don't make any constraints.
      Ok(vec![full_bound::<T>()])
    }
  }
}

/// Same as above, but uses the `col_type` and wraps each `ColBound` into a `PolyColBound`.
pub fn compute_poly_col_bounds(
  kb_expr: &KBExpr,
  col_name: &ColName,
  col_type: &ColType,
) -> Result<Vec<PolyColBound>, EvalError> {
  Ok(match col_type {
    ColType::Int => compute_col_bounds::<i32>(&kb_expr, col_name)?
      .into_iter()
      .map(|bound| PolyColBound::Int(bound))
      .collect(),
    ColType::Bool => compute_col_bounds::<bool>(&kb_expr, col_name)?
      .into_iter()
      .map(|bound| PolyColBound::Bool(bound))
      .collect(),
    ColType::String => compute_col_bounds::<String>(&kb_expr, col_name)?
      .into_iter()
      .map(|bound| PolyColBound::String(bound))
      .collect(),
  })
}

/// This function removes redundancy in the `row_region`. Redundancy may easily
/// arise from different ContextRows. In the future, we can be smarter and
/// sacrifice granularity for a simpler Key Region.
pub fn compress_row_region(row_region: Vec<KeyBound>) -> Vec<KeyBound> {
  row_region
}

/// Computes `KeyBound`s that have a corresponding shape to `key_cols` such that any key
/// outside of this is guaranteed to evaluate `expr` to false. We have `col_map` as concrete
/// values that we substitute into `expr` first. This returns the compressed regions.
pub fn compute_key_region(
  expr: &proc::ValExpr,
  col_map: BTreeMap<ColName, ColValN>,
  key_cols: &Vec<(ColName, ColType)>,
) -> Result<Vec<KeyBound>, EvalError> {
  let key_col_names = Vec::from_iter(key_cols.iter().map(|(name, _)| name.clone()));
  let kb_expr = construct_kb_expr(expr.clone(), &col_map, &key_col_names)?;
  let mut key_bounds = Vec::<KeyBound>::new();

  // The strategy here is to start with an all-encompassing KeyRegion, then reduce the
  // KeyRegion for each key column independently.
  key_bounds.push(KeyBound { col_bounds: vec![] });
  for (col_name, col_type) in key_cols {
    // Then, compute the ColBound and extend key_bounds.
    let col_bounds = compute_poly_col_bounds(&kb_expr, col_name, col_type)?;
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

  Ok(key_bounds)
}

// -----------------------------------------------------------------------------------------------
//  Region Intersection Utilities
// -----------------------------------------------------------------------------------------------

/// Essentially computes the intersection of 2 `ColBound`s, returning a pair
/// of `SingleBound`. Note that the first element can be greater than the second.
pub fn col_bound_intersect_interval<'a, T: Ord>(
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
        &left.start
      } else {
        &right.start
      }
    }
    (SingleBound::Excluded(b1), SingleBound::Included(b2)) => {
      if b1 >= b2 {
        &left.start
      } else {
        &right.start
      }
    }
  };
  let upper = match (&left.end, &right.end) {
    (SingleBound::Unbounded, _) => &right.end,
    (_, SingleBound::Unbounded) => &left.end,
    (SingleBound::Included(b1), SingleBound::Included(b2))
    | (SingleBound::Excluded(b1), SingleBound::Excluded(b2))
    | (SingleBound::Included(b1), SingleBound::Excluded(b2)) => {
      if b1 >= b2 {
        &right.end
      } else {
        &left.end
      }
    }
    (SingleBound::Excluded(b1), SingleBound::Included(b2)) => {
      if b1 > b2 {
        &right.end
      } else {
        &left.end
      }
    }
  };
  (lower, upper)
}

/// Checks if the given interval (where the first element can generally be greater
/// than the second) is non-empty.
pub fn is_interval_empty<T: Ord>(interval: (&SingleBound<T>, &SingleBound<T>)) -> bool {
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
pub fn col_bounds_intersect<T: Ord + Clone>(
  left_bounds: Vec<ColBound<T>>,
  right_bounds: Vec<ColBound<T>>,
) -> Vec<ColBound<T>> {
  let mut intersection = Vec::<ColBound<T>>::new();
  for left_bound in &left_bounds {
    for right_bound in &right_bounds {
      let (lower, upper) = col_bound_intersect_interval(left_bound, right_bound);
      if !is_interval_empty((lower, upper)) {
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
pub fn invert_col_bounds<T: Ord + Clone>(bounds: Vec<ColBound<T>>) -> Vec<ColBound<T>> {
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

/// Computes whether the given row regions intersect. The schemas of the KeyBounds should match,
/// i.e. they should all be the same length and every element must have corresponding types.
fn does_row_region_intersect(row_region1: &Vec<KeyBound>, row_region2: &Vec<KeyBound>) -> bool {
  fn does_col_intersect<T: Ord>(bound1: &ColBound<T>, bound2: &ColBound<T>) -> bool {
    let interval = col_bound_intersect_interval(bound1, bound2);
    is_interval_empty(interval)
  }

  fn does_key_bound_intersect(key_bound1: &KeyBound, key_bound2: &KeyBound) -> bool {
    assert_eq!(key_bound1.col_bounds.len(), key_bound2.col_bounds.len());
    for (pc1, pc2) in key_bound1.col_bounds.iter().zip(key_bound2.col_bounds.iter()) {
      let does_col_bound_intersect = match (pc1, pc2) {
        (PolyColBound::Int(c1), PolyColBound::Int(c2)) => does_col_intersect(c1, c2),
        (PolyColBound::Bool(c1), PolyColBound::Bool(c2)) => does_col_intersect(c1, c2),
        (PolyColBound::String(c1), PolyColBound::String(c2)) => does_col_intersect(c1, c2),
        _ => panic!(),
      };
      if !does_col_bound_intersect {
        return false;
      }
    }
    true
  }

  for key_bound1 in row_region1 {
    for key_bound2 in row_region2 {
      if does_key_bound_intersect(key_bound1, key_bound2) {
        return true;
      }
    }
  }
  false
}

/// Returns true if this `WriteRegion` has the Region Isolation Property with the `ReadRegion`.
pub fn is_isolated(write_region: &WriteRegion, read_region: &ReadRegion) -> bool {
  if does_row_region_intersect(&read_region.row_region, &write_region.row_region) {
    match &write_region.write_type {
      WriteRegionType::FixedRowsVarCols { val_col_region } => {
        !does_col_regions_intersect(&read_region.val_col_region, &val_col_region)
      }
      WriteRegionType::VarRows => false,
    }
  } else {
    true
  }
}

/// Returns true if this `WriteRegion` has the Region Isolation Property with these `ReadRegion`s.
pub fn is_isolated_multiread(
  write_region: &WriteRegion,
  read_regions: &BTreeSet<ReadRegion>,
) -> bool {
  for read_region in read_regions {
    if !is_isolated(&write_region, &read_region) {
      return false;
    }
  }
  true
}

/// Returns true if these `WriteRegion`s has the Region Isolation Property with the `ReadRegion`.
pub fn is_isolated_multiwrite(
  write_regions: &BTreeSet<WriteRegion>,
  read_region: &ReadRegion,
) -> bool {
  for write_region in write_regions {
    if !is_isolated(&write_region, &read_region) {
      return false;
    }
  }
  true
}
