use crate::model::sqlast::{Root, SqlStmt, Test};
use lrlex::lrlex_mod;
use lrpar::lrpar_mod;

lrlex_mod!("sql/sql.l");
lrpar_mod!("sql/sql.y");

/// Parses a string containing SQL code into an AST.
fn parse_root(l: &String) -> Option<Root> {
  let lexerdef = sql_l::lexerdef();
  let lexer = lexerdef.lexer(l);
  // Pass the lexer to the parser and lex and parse the input.
  let (res, errs) = sql_y::parse(&lexer);
  // TODO: Figure out how to turn off this error correction....
  // Also, test Select Subquery.
  for e in errs {
    println!("{}", e.pp(&lexer, &sql_y::token_epp));
  }

  return res;
}

fn parse_test(l: &String) -> Option<Test> {
  match parse_root(l) {
    Some(Root::Test(ast)) => Some(ast),
    _ => None,
  }
}

pub fn parse_sql(l: &String) -> Option<SqlStmt> {
  match parse_root(l) {
    Some(Root::SqlStmt(ast)) => Some(ast),
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use crate::model::sqlast::{BinaryOp, Literal, Root, Test, ValExpr};
  use crate::sql::parser::{parse_sql, parse_test};

  /// This test makes sure that all literals in a expression are
  /// getting parsed properly into a singular expression containing
  /// just that literal.
  #[test]
  fn literal_test() {
    assert_eq!(
      parse_test(&"\"hi\"".to_string()),
      Some(Test::ValExpr(ValExpr::Literal(Literal::String(
        "hi".to_string()
      ))))
    );
    assert_eq!(
      parse_test(&"123".to_string()),
      Some(Test::ValExpr(ValExpr::Literal(Literal::Int(
        "123".to_string()
      ))))
    );
    assert_eq!(
      parse_test(&"TRUE".to_string()),
      Some(Test::ValExpr(ValExpr::Literal(Literal::Bool(true))))
    );
    assert_eq!(
      parse_test(&"NULL".to_string()),
      Some(Test::ValExpr(ValExpr::Literal(Literal::Null)))
    );
  }

  /// This test makes sure that all binary operations are getting parsed
  /// properly.
  #[test]
  fn binary_expr_basic_test() {
    assert_eq!(
      parse_test(&"TRUE AND FALSE".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::And,
        lhs: Box::new(ValExpr::Literal(Literal::Bool(true))),
        rhs: Box::new(ValExpr::Literal(Literal::Bool(false))),
      }))
    );
    assert_eq!(
      parse_test(&"TRUE OR FALSE".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::Or,
        lhs: Box::new(ValExpr::Literal(Literal::Bool(true))),
        rhs: Box::new(ValExpr::Literal(Literal::Bool(false))),
      }))
    );
    assert_eq!(
      parse_test(&"1 <= 2".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::LTE,
        lhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
      }))
    );
    assert_eq!(
      parse_test(&"1 < 2".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::LT,
        lhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
      }))
    );
    assert_eq!(
      parse_test(&"1 >= 2".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::GTE,
        lhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
      }))
    );
    assert_eq!(
      parse_test(&"1 > 2".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::GT,
        lhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
      }))
    );
    assert_eq!(
      parse_test(&"1 = 2".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::E,
        lhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
      }))
    );
  }

  #[test]
  fn binary_op_assoc_test() {
    // This assert checks to make sure that AND is bound tighter than OR.
    assert_eq!(
      parse_test(&"TRUE OR TRUE AND FALSE".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::Or,
        lhs: Box::new(ValExpr::Literal(Literal::Bool(true))),
        rhs: Box::new(ValExpr::BinaryExpr {
          op: BinaryOp::And,
          lhs: Box::new(ValExpr::Literal(Literal::Bool(true))),
          rhs: Box::new(ValExpr::Literal(Literal::Bool(false))),
        }),
      }))
    );
  }

  /// This test makes sure that all expressions are getting parsed
  /// properly.
  #[test]
  fn val_expr_test() {
    assert_eq!(
      parse_test(&"key".to_string()),
      Some(Test::ValExpr(ValExpr::Column("key".to_string())))
    );
  }
}
