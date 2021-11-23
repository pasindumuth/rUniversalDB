#![feature(map_first_last)]

use crate::serial_test::{simple_test, subquery_test};

mod serial_test;
mod simulation;

fn main() {
  simple_test();
  subquery_test();
}
