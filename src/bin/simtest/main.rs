#![feature(map_first_last)]

use crate::serial_test::{multi_stage_test, simple_test, subquery_test, trans_table_test};

mod serial_test;
mod simulation;

fn main() {
  simple_test();
  subquery_test();
  trans_table_test();
  multi_stage_test();
}
