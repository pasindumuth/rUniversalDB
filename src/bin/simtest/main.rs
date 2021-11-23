#![feature(map_first_last)]

use crate::serial_test::tp_test;

mod serial_test;
mod simulation;

fn main() {
  tp_test()
}
