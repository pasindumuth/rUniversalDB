use std::io::{self, BufRead, Write};

use lrlex::lrlex_mod;
use lrpar::lrpar_mod;

// Using `lrlex_mod!` brings the lexer for `sql.l` into scope. By default the
// module name will be `sql_l` (i.e. the file name, minus any extensions,
// with a suffix of `_l`).
lrlex_mod!("sql/sql.l");
// Using `lrpar_mod!` brings the parser for `sql.y` into scope. By default the
// module name will be `sql_y` (i.e. the file name, minus any extensions,
// with a suffix of `_y`).
lrpar_mod!("sql/sql.y");

fn main() {
  // Get the `LexerDef` for the `calc` language.
  let lexerdef = sql_l::lexerdef();
  let stdin = io::stdin();
  loop {
    print!(">>> ");
    io::stdout().flush().ok();
    match stdin.lock().lines().next() {
      Some(Ok(ref l)) => {
        if l.trim().is_empty() {
          continue;
        }
        // Now we create a lexer with the `lexer` method with which
        // we can lex an input.
        let lexer = lexerdef.lexer(l);
        // Pass the lexer to the parser and lex and parse the input.
        let (res, errs) = sql_y::parse(&lexer);
        for e in errs {
          println!("{}", e.pp(&lexer, &sql_y::token_epp));
        }
        match res {
          Some(r) => println!("Result: {:?}", r),
          _ => eprintln!("Unable to evaluate expression."),
        }
      }
      _ => break,
    }
  }
}
