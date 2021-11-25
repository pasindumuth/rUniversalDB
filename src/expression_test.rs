use crate::expression::{
  construct_cexpr, construct_colvaln, does_col_regions_intersect, evaluate_binary_op,
  evaluate_c_expr, CExpr, EvalError,
};
use crate::model::common::{iast, proc, ColVal};
use crate::query_converter::flatten_val_expr_r;
use crate::sql_parser::convert_expr;
use crate::test_utils::cn;
use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Expression Evaluation
// -----------------------------------------------------------------------------------------------

/// Utility for converting a raw SQL expression, not containing `Subquery`s or `ColumnRef`s.
fn parse_expr(expr_str: &str) -> CExpr {
  let dialect = GenericDialect {};
  let mut tokenizer = Tokenizer::new(&dialect, expr_str);
  let tokens = tokenizer.tokenize().unwrap();
  let mut parser = Parser::new(tokens, &dialect);
  let sql_expr = parser.parse_expr().unwrap();
  let internal_expr = convert_expr(sql_expr).unwrap();
  let val_expr = flatten_val_expr_r(&internal_expr, &mut 0).unwrap();
  construct_cexpr(&val_expr, &mut BTreeMap::new(), &mut Vec::new(), &mut 0).unwrap()
}

#[test]
fn construct_colvaln_test() {
  // Number
  assert_eq!(construct_colvaln(iast::Value::Number("42".to_string())), Ok(Some(ColVal::Int(42))));
  assert_eq!(construct_colvaln(iast::Value::Number("".to_string())), Err(EvalError::GenericError));
  assert_eq!(
    construct_colvaln(iast::Value::Number("999999999999".to_string())),
    Err(EvalError::GenericError)
  );
  assert_eq!(
    construct_colvaln(iast::Value::Number("1234hello".to_string())),
    Err(EvalError::GenericError)
  );

  // String, Boolean, Null
  assert_eq!(
    construct_colvaln(iast::Value::QuotedString("hello".to_string())),
    Ok(Some(ColVal::String("hello".to_string())))
  );
  assert_eq!(construct_colvaln(iast::Value::Boolean(true)), Ok(Some(ColVal::Bool(true))));
  assert_eq!(construct_colvaln(iast::Value::Null), Ok(None));
}

#[test]
fn evaluate_unary_op_test() {
  // Plus
  assert_eq!(evaluate_c_expr(&parse_expr("+10")), Ok(Some(ColVal::Int(10))));
  assert_eq!(evaluate_c_expr(&parse_expr("-10")), Ok(Some(ColVal::Int(-10))));
  // Not
  assert_eq!(evaluate_c_expr(&parse_expr("NOT true")), Ok(Some(ColVal::Bool(false))));
  assert_eq!(evaluate_c_expr(&parse_expr("NOT (NULL)")), Ok(None));
}

#[test]
fn evaluate_binary_op_test() {
  // Divide
  assert_eq!(evaluate_c_expr(&parse_expr("20/10")), Ok(Some(ColVal::Int(2))));
  assert_eq!(evaluate_c_expr(&parse_expr("20/15")), Ok(Some(ColVal::Int(1))));
  assert_eq!(evaluate_c_expr(&parse_expr("20/25")), Ok(Some(ColVal::Int(0))));
  assert_eq!(evaluate_c_expr(&parse_expr("-30/20")), Ok(Some(ColVal::Int(-1))));
  assert_eq!(evaluate_c_expr(&parse_expr("10/0")), Err(EvalError::InvalidBinaryOp));
  // OR
  assert_eq!(evaluate_c_expr(&parse_expr("true OR NULL")), Ok(Some(ColVal::Bool(true))));
  assert_eq!(evaluate_c_expr(&parse_expr("NULL OR true")), Ok(Some(ColVal::Bool(true))));
  assert_eq!(evaluate_c_expr(&parse_expr("false OR NULL")), Ok(None));
  assert_eq!(evaluate_c_expr(&parse_expr("NULL OR false")), Ok(None));
  assert_eq!(evaluate_c_expr(&parse_expr("NULL OR NULL")), Ok(None));
  assert_eq!(evaluate_c_expr(&parse_expr("false OR false")), Ok(Some(ColVal::Bool(false))));
  assert_eq!(evaluate_c_expr(&parse_expr("false OR 3")), Err(EvalError::InvalidBinaryOp));
}

// -----------------------------------------------------------------------------------------------
//  Region Isolation Property Utilities
// -----------------------------------------------------------------------------------------------

#[test]
fn does_col_regions_intersect_test() {
  let cols1 = vec![cn("c1"), cn("c2")];
  let cols2 = vec![cn("c2"), cn("c3")];
  let cols3 = vec![cn("c4")];
  let cols4 = vec![];
  assert!(does_col_regions_intersect(&cols1, &cols2));
  assert!(does_col_regions_intersect(&cols2, &cols1));
  assert!(!does_col_regions_intersect(&cols1, &cols3));
  assert!(!does_col_regions_intersect(&cols3, &cols1));
  assert!(!does_col_regions_intersect(&cols3, &cols4));
  assert!(!does_col_regions_intersect(&cols4, &cols3));
}
