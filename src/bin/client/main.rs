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

#[derive(Debug)]
struct A {
  p1: u32,
  p2: u32,
}

impl A {
  fn get_ref(&mut self) -> Ref {
    return Ref { q: &mut self.p1 };
  }
}

#[derive(Debug)]
struct Ref<'a> {
  q: &'a mut u32,
}

fn main() {
  let mut a = A { p1: 1, p2: 2 };
  let r = a.get_ref();
  *r.q += 1;
  println!("{:?}", a);
}

//
// fn sql() {
//   use sqlparser::dialect::GenericDialect;
//   use sqlparser::parser::Parser;
//
//   let sql = "SELECT a, b, 123, myfunc(b) \
//            FROM table_1 \
//            WHERE a > b AND b < 100 \
//            ORDER BY a DESC, b";
//
//   let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
//
//   let ast = Parser::parse_sql(&dialect, sql).unwrap();
//
//   println!("AST: {:#?}", ast);
// }
