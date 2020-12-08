%start root

%left      OR
%left      AND
%nonassoc LTE LT GTE GT E
%left      '+' '-'
%left      '*' '/'

%%

root -> Root
  : sql_stmt        { Root::SqlStmt($1) }
  | expr            { Root::Test(Test::ValExpr($1)) }
  ;

sql_stmt -> SqlStmt
  : select_stmt     { SqlStmt::Select($1) }
  | update_stmt     { SqlStmt::Update($1) }
  ;

select_stmt -> SelectStmt
  : 'SELECT' iden_list 'FROM' iden 'WHERE' expr
    {
      SelectStmt {
        col_names: $2,
        table_name: $4,
        where_clause: $6,
      }
    }
  ;

update_stmt -> UpdateStmt
  : 'UPDATE' iden 'SET' iden 'E' expr 'WHERE' expr
    {
      UpdateStmt {
        table_name: $2,
        set_col: $4,
        set_val: $6,
        where_clause: $8,
      }
    }
  ;

iden_list -> Vec<String>
  : iden
    {
      vec![$1]
    }
  | iden_list ',' iden
    {
      $1.push($3);
      $1
    }
  ;

expr -> ValExpr
  : expr 'AND' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::AND,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr 'OR' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::OR,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr 'LTE' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::LTE,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr 'LT' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::LT,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr 'GTE' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::GTE,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr 'GT' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::GT,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr 'E' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::E,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr '+' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::PLUS,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr '-' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::MINUS,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr '*' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::TIMES,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | expr '/' expr
    {
      ValExpr::BinaryExpr {
        op: BinaryOp::DIV,
        lhs: Box::new($1),
        rhs: Box::new($3),
      }
    }
  | literal                       { ValExpr::Literal($1) }
  | iden                          { ValExpr::Column($1) }
  | '(' select_stmt ')'           { ValExpr::Subquery(Box::new($2)) }
  ;

literal -> Literal
  : quoted_string  { Literal::String($1) }
  | int            { Literal::Int($1) }
  | bool           { Literal::Bool($1) }
  | null           { Literal::Null }
  ;

iden -> String
  : "identifier"
    {
      $lexer.span_str($1.as_ref().unwrap().span()).to_string()
    }
  ;

quoted_string -> String
  : "quoted_string"
    {
      let s = $lexer.span_str($1.as_ref().unwrap().span());
      s[1..s.len() - 1].to_string()
    }
  ;

int -> String
  : 'INT'
    {
      $lexer.span_str($1.as_ref().unwrap().span()).to_string()
    }
  ;

bool -> bool
  : 'TRUE'  { true }
  | 'FALSE' { false }
  ;


null -> ()
  : 'NULL' { () }
  ;

%%

use crate::model::sqlast::*;
