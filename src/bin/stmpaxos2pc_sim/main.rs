#![feature(map_first_last)]

use crate::tests::test;

mod message;
mod simple_rm_es;
mod simple_tm_es;
mod simulation;
mod slave;
mod stm_simple_rm_es;
mod stm_simple_tm_es;
mod tests;

#[macro_use]
extern crate runiversal;

fn main() {
  test()
}
