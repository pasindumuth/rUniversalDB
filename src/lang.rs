macro_rules! collection {
    // map-like
    ($($k:expr => $v:expr),* $(,)?) => {
        std::iter::Iterator::collect(std::array::IntoIter::new([$(($k, $v),)*]))
    };
    // set-like
    ($($v:expr),* $(,)?) => {
        std::iter::Iterator::collect(std::array::IntoIter::new([$($v,)*]))
    };
}

/// The first argument is a single-element Tuple Struct Variant
/// whose inside's we want to extract to. The second argument is the
/// value we to extract from. If the value is a reference, the return
/// value here is a reference. Otherwise, the value is moved.
#[macro_export]
macro_rules! cast {
  ($enum:path, $expr:expr) => {{
    if let $enum(item) = $expr {
      Some(item)
    } else {
      debug_assert!(false);
      None
    }
  }};
}

/// Same as the above, but the expected branch might be the `None`
/// branch, so we do not debug assert.
#[macro_export]
macro_rules! cast_safe {
  ($enum:path, $expr:expr) => {{
    if let $enum(item) = $expr {
      Some(item)
    } else {
      None
    }
  }};
}

/// A macro that makes it easy to check that an expression is true,
/// and then exit the current function if it is false (in production,
/// but assert in development).
#[macro_export]
macro_rules! check {
  ($expr:expr) => {{
    if $expr {
      Some(())
    } else {
      debug_assert!(false);
      None
    }? // We place the `?` here, since it is easy to forget
       // when using this macro (since it does not return anything).
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
