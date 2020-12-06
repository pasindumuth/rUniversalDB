%start sql_statement
%%
sql_statement -> Statement
  : 'SELECT' iden_list 'FROM' iden
    {
      Statement::Select($2, $4)
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

use runiversal::model::sqlast::Statement;
