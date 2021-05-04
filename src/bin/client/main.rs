use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() {
  println!("hello world");

  use sqlparser::dialect::GenericDialect;
  use sqlparser::parser::Parser;

  let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

  let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

  let ast = Parser::parse_sql(&dialect, sql).unwrap();

  println!("AST: {:#?}", ast);
}
