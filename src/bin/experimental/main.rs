// use rmp_serde;
// use runiversal::model::message::Message::Basic;
// use runiversal::net::network::send;
// use std::collections::LinkedList;
// use std::env;
// use std::io::stdin;
// use std::net::TcpStream;
//
// fn main() {
//   let mut args: LinkedList<String> = env::args().collect();
//   args.pop_front(); // Remove the program name arg.
//   let ip = args.pop_front().unwrap();
//   let port = args.pop_front().unwrap().parse::<i32>().unwrap();
//
//   // Connect to the server
//   let stream = TcpStream::connect(format!("{}:{}", ip, port)).unwrap();
//
//   loop {
//     // Read in the message to send
//     let mut val = String::new();
//     stdin().read_line(&mut val).unwrap();
//     let msg = Basic(val);
//
//     // Send the message
//     let buf = rmp_serde::to_vec(&msg).unwrap();
//     send(buf.as_slice(), &stream);
//   }
// }

fn main() {
  sql()
}

fn sql() {
  use sqlparser::dialect::GenericDialect;
  use sqlparser::parser::Parser;
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
