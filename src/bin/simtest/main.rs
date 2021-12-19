#![feature(map_first_last)]

use crate::advanced_parallel_test::test_all_advanced_parallel;
use crate::advanced_serial_test::test_all_advanced_serial;
use crate::basic_parallel_test::test_all_basic_parallel;
use crate::basic_serial_test::test_all_basic_serial;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;

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

mod advanced_parallel_test;
mod advanced_serial_test;
mod basic_parallel_test;
mod basic_serial_test;
mod serial_test_utils;
mod simulation;
mod stats;

/**
 * Debugging Tips:
 *  - We thread a global RNG through all test cases. However, in every test case, we try to
 *    use it for nothing more than creating a new RNG by creating a random seed. The reason
 *    for this is so that if a failure happens, we can just print the seed and then quickly
 *    reproduce by using that seed directly to run the test case.
 */

fn main() {
  let mut rand = XorShiftRng::from_seed([0; 16]);
  println!("Basic Serial Tests:");
  test_all_basic_serial(&mut rand);
  println!("\n");
  println!("Advanced Serial Tests:");
  test_all_advanced_serial(&mut rand);
  println!("\n");
  println!("Basic Parallel Tests:");
  test_all_basic_parallel(&mut rand);
  println!("\n");
  // TODO: this test grinds to a halt when we use the many-messages delivery scheme.
  // println!("Advanced Parallel Tests:");
  // test_all_advanced_parallel(&mut rand);
}
