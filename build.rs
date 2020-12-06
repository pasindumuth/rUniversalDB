use cfgrammar::yacc::YaccKind;
use lrlex::LexerBuilder;
use lrpar::CTParserBuilder;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  // This is for generating a Parser for our SQL language.
  let lex_rule_ids_map = CTParserBuilder::new()
    .yacckind(YaccKind::Grmtools)
    .process_file_in_src("sql/sql.y")?;
  LexerBuilder::new()
    .rule_ids_map(lex_rule_ids_map)
    .process_file_in_src("sql/sql.l")?;
  Ok(())
}
