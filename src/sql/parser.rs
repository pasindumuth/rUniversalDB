pub fn parse_int(s: &str) -> Result<u64, ()> {
  match s.parse::<u64>() {
    Ok(val) => Ok(val),
    Err(_) => {
      eprintln!("{} cannot be represented as a u64", s);
      Err(())
    }
  }
}
