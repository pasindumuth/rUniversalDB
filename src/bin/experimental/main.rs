use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;

fn main() {
  let mut s = BTreeMap::new();
  s.insert("hello", 4);
  println!("{:#?}", s);
}

fn sql() {
  //
  // let sql = "SELECT a, b, 123, myfunc(b) \
  //          FROM table_1 \
  //          WHERE a > b AND b < 100 \
  //          ORDER BY a DESC, b";

  let sql = "
    INSERT INTO inventory (product_id, email, count)
    VALUES (2, 'my_email_2', 25);
  
    SELECT SUM(DISTINCT count)
    FROM inventory;
  ";

  let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

  let ast = Parser::parse_sql(&dialect, sql);

  println!("AST: {:#?}", ast);
}
