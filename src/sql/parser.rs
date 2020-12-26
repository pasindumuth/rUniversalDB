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
  if errs.len() > 0 {
    None
  } else {
    res
  }
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
  use crate::model::sqlast::{
    BinaryOp, InsertStmt, Literal, SelectStmt, SqlStmt, Test, UpdateStmt, ValExpr,
  };
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
        op: BinaryOp::AND,
        lhs: Box::new(ValExpr::Literal(Literal::Bool(true))),
        rhs: Box::new(ValExpr::Literal(Literal::Bool(false))),
      }))
    );
    assert_eq!(
      parse_test(&"TRUE OR FALSE".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::OR,
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
    assert_eq!(
      parse_test(&"1 + 2".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::PLUS,
        lhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
      }))
    );
    assert_eq!(
      parse_test(&"1 * 2".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::TIMES,
        lhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
      }))
    );
    assert_eq!(
      parse_test(&"1 - 2".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::MINUS,
        lhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
      }))
    );
  }

  /// This tests makes sure that expressions get parsed according to
  /// operator precedence and associativity rules.
  #[test]
  fn binary_op_assoc_test() {
    // This assert checks to make sure that AND is bound tighter than OR,
    // despite coming as the second binary operator.
    assert_eq!(
      parse_test(&"TRUE OR TRUE AND FALSE".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::OR,
        lhs: Box::new(ValExpr::Literal(Literal::Bool(true))),
        rhs: Box::new(ValExpr::BinaryExpr {
          op: BinaryOp::AND,
          lhs: Box::new(ValExpr::Literal(Literal::Bool(true))),
          rhs: Box::new(ValExpr::Literal(Literal::Bool(false))),
        }),
      }))
    );
    assert_eq!(
      parse_test(&"FALSE < 2 AND 3 = \"hello\"".to_string()),
      Some(Test::ValExpr(ValExpr::BinaryExpr {
        op: BinaryOp::AND,
        lhs: Box::new(ValExpr::BinaryExpr {
          op: BinaryOp::LT,
          lhs: Box::new(ValExpr::Literal(Literal::Bool(false))),
          rhs: Box::new(ValExpr::Literal(Literal::Int("2".to_string()))),
        }),
        rhs: Box::new(ValExpr::BinaryExpr {
          op: BinaryOp::E,
          lhs: Box::new(ValExpr::Literal(Literal::Int("3".to_string()))),
          rhs: Box::new(ValExpr::Literal(Literal::String("hello".to_string()))),
        }),
      }))
    );
    assert_eq!(parse_test(&"1 < 1 = 1".to_string()), None);
    assert_eq!(parse_test(&"1 <= 1 = 1".to_string()), None);
    assert_eq!(parse_test(&"1 = 1 = 1".to_string()), None);
    assert_eq!(parse_test(&"1 > 1 = 1".to_string()), None);
    assert_eq!(parse_test(&"1 >= 1 = 1".to_string()), None);
  }

  /// This test checks to see if Select Statements are being parse properly.
  #[test]
  fn select_parse_test() {
    assert_eq!(
      parse_sql(&"SELECT key, val FROM table WHERE key = 123".to_string()),
      Some(SqlStmt::Select(SelectStmt {
        col_names: vec!["key".to_string(), "val".to_string()],
        table_name: "table".to_string(),
        where_clause: ValExpr::BinaryExpr {
          op: BinaryOp::E,
          lhs: Box::new(ValExpr::Column("key".to_string())),
          rhs: Box::new(ValExpr::Literal(Literal::Int("123".to_string()))),
        }
      }))
    );
    let sql = "SELECT key, val
               FROM table
               WHERE key = 123";
    assert_eq!(
      parse_sql(&sql.to_string()),
      Some(SqlStmt::Select(SelectStmt {
        col_names: vec!["key".to_string(), "val".to_string()],
        table_name: "table".to_string(),
        where_clause: ValExpr::BinaryExpr {
          op: BinaryOp::E,
          lhs: Box::new(ValExpr::Column("key".to_string())),
          rhs: Box::new(ValExpr::Literal(Literal::Int("123".to_string()))),
        }
      }))
    );
    // This Select Query contains a Subquery
    let sql = "SELECT key1, val
               FROM table1
               WHERE key1 =
                (SELECT key2
                 FROM table2
                 WHERE key2 = \"Bob\")";
    assert_eq!(
      parse_sql(&sql.to_string()),
      Some(SqlStmt::Select(SelectStmt {
        col_names: vec!["key1".to_string(), "val".to_string()],
        table_name: "table1".to_string(),
        where_clause: ValExpr::BinaryExpr {
          op: BinaryOp::E,
          lhs: Box::new(ValExpr::Column("key1".to_string())),
          rhs: Box::new(ValExpr::Subquery(Box::from(SelectStmt {
            col_names: vec!["key2".to_string()],
            table_name: "table2".to_string(),
            where_clause: ValExpr::BinaryExpr {
              op: BinaryOp::E,
              lhs: Box::new(ValExpr::Column("key2".to_string())),
              rhs: Box::new(ValExpr::Literal(Literal::String("Bob".to_string()))),
            }
          }))),
        }
      }))
    );
  }

  /// This test checks to see if Update Statements are being parse properly.
  #[test]
  fn update_parse_test() {
    let sql = "UPDATE table
               SET val = val + 1
               WHERE key < 3";
    assert_eq!(
      parse_sql(&sql.to_string()),
      Some(SqlStmt::Update(UpdateStmt {
        table_name: "table".to_string(),
        set_col: "val".to_string(),
        set_val: ValExpr::BinaryExpr {
          op: BinaryOp::PLUS,
          lhs: Box::new(ValExpr::Column("val".to_string())),
          rhs: Box::new(ValExpr::Literal(Literal::Int("1".to_string()))),
        },
        where_clause: ValExpr::BinaryExpr {
          op: BinaryOp::LT,
          lhs: Box::new(ValExpr::Column("key".to_string())),
          rhs: Box::new(ValExpr::Literal(Literal::Int("3".to_string()))),
        }
      }))
    );
    // Advanced Update Statement with Subqueries
    let sql = "UPDATE table
               SET val = val + (SELECT key2
                                FROM table2
                                WHERE key2 = \"Bob\")
               WHERE key =
                (SELECT key3
                 FROM table3
                 WHERE key3 AND val3)";
    assert_eq!(
      parse_sql(&sql.to_string()),
      Some(SqlStmt::Update(UpdateStmt {
        table_name: "table".to_string(),
        set_col: "val".to_string(),
        set_val: ValExpr::BinaryExpr {
          op: BinaryOp::PLUS,
          lhs: Box::new(ValExpr::Column("val".to_string())),
          rhs: Box::new(ValExpr::Subquery(Box::from(SelectStmt {
            col_names: vec!["key2".to_string()],
            table_name: "table2".to_string(),
            where_clause: ValExpr::BinaryExpr {
              op: BinaryOp::E,
              lhs: Box::new(ValExpr::Column("key2".to_string())),
              rhs: Box::new(ValExpr::Literal(Literal::String("Bob".to_string()))),
            }
          }))),
        },
        where_clause: ValExpr::BinaryExpr {
          op: BinaryOp::E,
          lhs: Box::new(ValExpr::Column("key".to_string())),
          rhs: Box::new(ValExpr::Subquery(Box::from(SelectStmt {
            col_names: vec!["key3".to_string()],
            table_name: "table3".to_string(),
            where_clause: ValExpr::BinaryExpr {
              op: BinaryOp::AND,
              lhs: Box::new(ValExpr::Column("key3".to_string())),
              rhs: Box::new(ValExpr::Column("val3".to_string())),
            }
          }))),
        }
      }))
    );
  }

  #[test]
  fn insert_parse_test() {
    // Advanced Update Statement with Subqueries
    let sql = "INSERT INTO table (col1, col2)
               VALUES
                 (1, \"hello\"),
                 ((SELECT key3
                   FROM table3
                   WHERE key3 AND val3), TRUE)";
    assert_eq!(
      parse_sql(&sql.to_string()),
      Some(SqlStmt::Insert(InsertStmt {
        table_name: "table".to_string(),
        col_names: vec!["col1".to_string(), "col2".to_string()],
        insert_vals: vec![
          vec![
            ValExpr::Literal(Literal::Int("1".to_string())),
            ValExpr::Literal(Literal::String("hello".to_string()))
          ],
          vec![
            ValExpr::Subquery(Box::from(SelectStmt {
              col_names: vec!["key3".to_string()],
              table_name: "table3".to_string(),
              where_clause: ValExpr::BinaryExpr {
                op: BinaryOp::AND,
                lhs: Box::new(ValExpr::Column("key3".to_string())),
                rhs: Box::new(ValExpr::Column("val3".to_string())),
              }
            })),
            ValExpr::Literal(Literal::Bool(true))
          ]
        ],
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
