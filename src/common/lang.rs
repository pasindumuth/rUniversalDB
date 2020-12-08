/// These are very low-level utilities where I consider
/// it a shortcoming of the language that there isn't something
/// I can already use.

pub fn rvec(i: i32, j: i32) -> Vec<i32> {
  (i..j).collect()
}

/// The first argument is a single-element Tuple Struct Variant
/// whose inside's we want to extract to. The second argument is the
/// value we to extract from. If the value is a reference, the return
/// value here is a reference. Otherwise, the value is moved.
macro_rules! cast {
  ($enum:path, $expr:expr) => {{
    if let $enum(item) = $expr {
      Ok(item)
    } else {
      Err("Could not cast the value to the desired Variant.")
    }
  }};
}

#[cfg(test)]
mod tests {
  enum Enum {
    V1(i32),
    V2(String),
  }

  #[test]
  fn cast_test() {
    let e = Enum::V2("value".to_string());
    let inner_incorrect = cast!(Enum::V1, &e);
    assert!(inner_incorrect.is_err());
    let inner_correct = cast!(Enum::V2, &e);
    assert_eq!(inner_correct, Ok(&"value".to_string()));
  }
}
