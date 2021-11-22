#![feature(map_first_last)]

use crate::simulation::{SimpleBundle, Simulation};
use runiversal::model::common::{Gen, LeadershipId};
use runiversal::model::message as msg;

mod simulation;

fn main() {
  test();
}

fn test() {
  println!("test_basic");
  test_basic();

  println!("test_leader_partition");
  test_leader_partition();
}

fn print_stats(sim: &Simulation) {
  for (_, paxos_data) in &sim.paxos_data {
    println!("Size: {:#?}", sim.max_common_index + paxos_data.paxos_log.len());
  }
}

/// This is a basic test with random queues being paused temporarily randomly.
fn test_basic() {
  let mut sim = Simulation::new([0; 16], 5);
  sim.simulate_n_ms(1000);
  print_stats(&sim);
}

/// Okay, let's get serious. Run this thing for a bit, then find the latest leader,
/// partition it run it some more, and verify that more `PLEntry`s were added.
fn test_leader_partition() {
  let mut sim = Simulation::new([0; 16], 5);
  sim.simulate_n_ms(10000);
  print_stats(&sim);

  // Find the latest Leader
  let lid = LeadershipId { gen: Gen(0), eid: sim.address_config[0].clone() };
  let mut latest_leader_changed = msg::LeaderChanged { lid };
  for entry in sim.global_paxos_log.iter().rev() {
    if let msg::PLEntry::LeaderChanged(leader_changed) = entry {
      latest_leader_changed = leader_changed.clone();
      break;
    }
  }

  // Partition out this Leader
  let leader_eid = latest_leader_changed.lid.eid;
  let eids = sim.address_config.clone();
  for eid in eids {
    sim.block_queue_permanently(leader_eid.clone(), eid.clone());
    sim.block_queue_permanently(eid, leader_eid.clone());
  }

  sim.simulate_n_ms(10000);
}
