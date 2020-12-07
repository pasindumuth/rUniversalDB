%start sql_statement
%%
sql_statement -> SqlStmt
  : 'SELECT' iden_list 'FROM' iden
    {
      SqlStmt::Select(SelectStmt {col_names: $2, table_name: $4})
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

iden -> String
  : "identifier"
    {
      $lexer.span_str($1.as_ref().unwrap().span()).to_string()
    }
  ;

%%

use runiversal::model::sqlast::{SqlStmt, SelectStmt};
