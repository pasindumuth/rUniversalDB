#![feature(map_first_last)]

use crate::simulation::{SimConfig, SimpleBundle, Simulation};
use rand::RngCore;
use rand_xorshift::XorShiftRng;
use runiversal::model::common::{Gen, LeadershipId};
use runiversal::model::message as msg;
use std::iter::FromIterator;

mod simulation;

fn main() {
  test();
}

fn test() {
  println!("test_basic");
  test_basic();

  println!("test_leader_partition");
  test_leader_partition();

  println!("test_general_partition");
  test_general_partition();
}

fn default_config() -> SimConfig {
  SimConfig { target_temp_blocked_frac: 0.5, max_pause_time_ms: 2000 }
}

fn print_stats(sim: &Simulation) {
  for (_, paxos_data) in &sim.paxos_data {
    println!("Size: {:#?}", sim.max_common_index + paxos_data.paxos_log.len());
  }
}

// -----------------------------------------------------------------------------------------------
//  test_basic
// -----------------------------------------------------------------------------------------------

/// This is a basic test with random queues being paused temporarily randomly.
fn test_basic() {
  let mut sim = Simulation::new([0; 16], 5, default_config());
  sim.simulate_n_ms(1000);
  print_stats(&sim);
}

// -----------------------------------------------------------------------------------------------
//  test_leader_partition
// -----------------------------------------------------------------------------------------------

/// Run the simulation for a bit, find the latest leader, partition it out, and then
/// run the simulation some more. Verify that more `PLEntry`s were added.
fn test_leader_partition() {
  let mut sim = Simulation::new([0; 16], 5, default_config());
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

  sim.simulate_n_ms(20000);
  print_stats(&sim);
}

// -----------------------------------------------------------------------------------------------
//  test_general_partition
// -----------------------------------------------------------------------------------------------

/// Generates a partition out of `indicies, where at least one partition has the
/// majority of nodes (as Paxos requires).
fn gen_partition(rand: &mut XorShiftRng, mut indices: Vec<usize>) -> Vec<Vec<usize>> {
  assert!(indices.len() > 0);

  fn add_partition(
    rand: &mut XorShiftRng,
    partition: &mut Vec<Vec<usize>>,
    rem_indices: &mut Vec<usize>,
    new_partition_len: usize,
  ) {
    assert!(new_partition_len <= rem_indices.len());
    let mut new_partition = Vec::<usize>::new();
    while new_partition.len() < new_partition_len {
      let r = rand.next_u32() as usize % rem_indices.len();
      new_partition.push(rem_indices.remove(r));
    }
    partition.push(new_partition);
  }

  let mut partition = Vec::<Vec<usize>>::new();
  // Construct the majority partition
  let majority_partition_len = indices.len() / 2 + 1;
  add_partition(rand, &mut partition, &mut indices, majority_partition_len);
  // Construct other partitions
  while indices.len() > 0 {
    let next_partition_len = (rand.next_u32() as usize % indices.len()) + 1;
    add_partition(rand, &mut partition, &mut indices, next_partition_len);
  }

  partition
}

/// Here, `partition` is a partition of the indices `sim.address_config`. This function
/// permamently blocks queues between these partitions.
fn block_partition(sim: &mut Simulation, partition: &Vec<Vec<usize>>) {
  let eids = sim.address_config.clone();
  for i in 0..partition.len() {
    for j in 0..partition.len() {
      if i != j {
        for idx_i in partition.get(i).unwrap() {
          for idx_j in partition.get(j).unwrap() {
            let eid_i = eids.get(*idx_i).unwrap().clone();
            let eid_j = eids.get(*idx_j).unwrap().clone();
            sim.block_queue_permanently(eid_i, eid_j);
          }
        }
      }
    }
  }
}

/// Here, `partition` is a partition of the indices `sim.address_config`. This function
/// permamently unblocks queues between these partitions.
fn unblock_partition(sim: &mut Simulation, partition: &Vec<Vec<usize>>) {
  let eids = sim.address_config.clone();
  for i in 0..partition.len() {
    for j in 0..partition.len() {
      if i != j {
        for idx_i in partition.get(i).unwrap() {
          for idx_j in partition.get(j).unwrap() {
            let eid_i = eids.get(*idx_i).unwrap().clone();
            let eid_j = eids.get(*idx_j).unwrap().clone();
            sim.unblock_queue_permanently(eid_i, eid_j);
          }
        }
      }
    }
  }
}

/// Loop around for some time, creating and changing network partition. Verify that
/// the algorithm is safe and that new `PLEntry`s constantly get added.
fn test_general_partition() {
  let sim_config = SimConfig { target_temp_blocked_frac: 0.0, max_pause_time_ms: 0 };
  let mut sim = Simulation::new([0; 16], 5, sim_config);
  let all_indices: Vec<usize> = (0..sim.address_config.len()).collect();

  let mut cur_time = 0;
  let mut cur_partition = gen_partition(&mut sim.rand, all_indices.clone());
  while cur_time < 200000 {
    let time_for_partition = sim.rand.next_u32() as usize % 15000;
    sim.simulate_n_ms(time_for_partition as u32);
    cur_time += time_for_partition;

    // Change the partition
    unblock_partition(&mut sim, &cur_partition);
    cur_partition = gen_partition(&mut sim.rand, all_indices.clone());
    block_partition(&mut sim, &cur_partition);

    println!("Stats at time {:?}", cur_time);
    print_stats(&sim);
  }

  for entry in sim.global_paxos_log.iter() {
    if let msg::PLEntry::LeaderChanged(leader_changed) = entry {
      println!("{:?}", leader_changed);
    }
  }
}
