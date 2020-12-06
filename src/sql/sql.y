%start Expr
%%
Expr -> Result<u64, ()>
    : Expr 'PLUS' Term { Ok($1? + $3?) }
    | Term { $1 }
    | Expr 'DOUBLE_PLUS' Term { Ok($1? + $3? + $3?) }
    ;

Term -> Result<u64, ()>
    : Term 'MUL' Factor { Ok($1? * $3?) }
    | Factor { $1 }
    ;

Factor -> Result<u64, ()>
    : 'LBRACK' Expr 'RBRACK' { $2 }
    | 'INT'
      {
          let v = $1.map_err(|_| ())?;
          parse_int($lexer.span_str(v.span()))
      }
    ;
%%

use runiversal::sql::parser::parse_int;
