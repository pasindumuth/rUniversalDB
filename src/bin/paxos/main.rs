#![feature(map_first_last)]

use crate::simulation::{SimpleBundle, Simulation};
use runiversal::model::message as msg;

mod simulation;

fn main() {
  test();
}

fn test() {
  let mut sim = Simulation::new([0; 16], 5);

  // The number of milliseconds to run the simulation.
  const SIMULATION_DURATION: u32 = 1000;
  sim.simulate_n_ms(SIMULATION_DURATION);

  // Results
  let mut max_index: usize = 0;
  loop {
    let mut maybe_val: Option<&msg::PLEntry<SimpleBundle>> = None;
    for (_, paxos_data) in &sim.paxos_data {
      if let Some(entry) = paxos_data.paxos_log.get(max_index) {
        if let Some(cur_entry) = maybe_val {
          assert_eq!(cur_entry, entry);
        } else {
          maybe_val = Some(entry);
        }
      }
    }
    if maybe_val.is_some() {
      max_index += 1;
    } else {
      break;
    }
  }
  for (_, paxos_data) in &sim.paxos_data {
    println!("Size: {:#?}", paxos_data.paxos_log.len());
  }
}
