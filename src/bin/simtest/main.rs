#![feature(map_first_last)]

use crate::parallel_test::test_all_parallel;
use crate::serial_test::test_all_serial;

#[macro_export]
macro_rules! cast {
  ($enum:path, $expr:expr) => {{
    if let $enum(item) = $expr {
      Ok(item)
    } else {
      Err("Could not cast the value to the desired Variant.")
    }
  }};
}

mod parallel_test;
mod serial_test;
mod simulation;

fn main() {
  test_all_serial();
  test_all_parallel();
}
