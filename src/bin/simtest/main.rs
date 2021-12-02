#![feature(map_first_last)]

use crate::advanced_serial_test::test_all_advanced_serial;
use crate::basic_serial_test::test_all_basic_serial;
use crate::parallel_test::test_all_parallel;

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

mod advanced_serial_test;
mod basic_serial_test;
mod parallel_test;
mod serial_test_utils;
mod simulation;

fn main() {
  println!("Basic Serial Tests:");
  test_all_basic_serial();
  println!("\n");
  println!("Advanced Serial Tests:");
  test_all_advanced_serial();
  println!("\n");
  println!("Parallel Tests:");
  test_all_parallel();
}
