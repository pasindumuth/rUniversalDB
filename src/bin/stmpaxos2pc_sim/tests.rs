use crate::message as msg;
use crate::simulation::{mk_client_eid, mk_slave_eid, Simulation};
use crate::slave::SlavePLm;
use rand::RngCore;
use runiversal::common::mk_qid;
use runiversal::model::common::{EndpointId, SlaveGroupId};
use runiversal::stmpaxos2pc_tm::RMPLm;
use std::collections::HashMap;

/// This checks for 2PC Consistency among the Global PLs in the simulation. Recall that
/// 2PC Consistency is where some RMs do not commit if any other RM aborts
fn check_consistency(sim: &Simulation) -> bool {
  // Check whether a Committed PLm is among the RM PLs. Same with Aborted PLms.
  let mut exists_committed = false;
  let mut exists_aborted = false;
  for (_, bundles) in &sim.global_pls {
    for bundle in bundles {
      for plm in &bundle.plms {
        if let SlavePLm::SimpleRM(RMPLm::Committed(_)) = plm {
          exists_committed = true;
        }
        if let SlavePLm::SimpleRM(RMPLm::Aborted(_)) = plm {
          exists_aborted = true;
        }
      }
    }
  }

  !(exists_committed && exists_aborted)
}

/// This checks for 2PC Completion. Recall that 2PC Completion is where every
/// RM either Commits or Aborts.
fn check_completion(sim: &Simulation) -> bool {
  'outer: for (_, bundles) in &sim.global_pls {
    for bundle in bundles {
      for plm in &bundle.plms {
        if let SlavePLm::SimpleRM(RMPLm::Committed(_)) = plm {
          continue 'outer;
        }
        if let SlavePLm::SimpleRM(RMPLm::Aborted(_)) = plm {
          continue 'outer;
        }
      }
    }
    return false;
  }
  return true;
}

pub fn test() {
  // Fundamental seed used for all random number generation,
  // providing determinism.
  let mut seed = [0; 16];
  for i in 0..16 {
    seed[i] = i as u8;
  }

  // Setup Simulation

  // Create 5 SlaveGroups, each with 3 nodes.
  let mut slave_address_config = HashMap::<SlaveGroupId, Vec<EndpointId>>::new();
  for i in 0..5 {
    let mut eids = Vec::<EndpointId>::new();
    for j in 0..3 {
      eids.push(mk_slave_eid(&(i * 5 + j)));
    }
    slave_address_config.insert(SlaveGroupId(format!("s{}", i)), eids);
  }

  let client_eid = mk_client_eid(&0);
  let mut sim = Simulation::new(seed, 1, slave_address_config.clone());

  // Run the simulation to warm it up. Activity here consists of Leadership changes,
  // Gossip, Paxos Insertions, etc.
  sim.simulate_n_ms(100);

  // Randomly construct a SimpleRequest and send it to a random Slave
  // to perform Simple STMPaxos2PC.

  // Randomly chosen TM
  let tm = SlaveGroupId(format!("s{}", sim.rand.next_u32() % 5));
  let tm_eid = sim.leader_map.get(&tm).unwrap().eid.clone();

  // Randomly chosen RMs
  let num_rms = (sim.rand.next_u32() % 5) + 1;
  let mut all_slaves: Vec<SlaveGroupId> = slave_address_config.keys().cloned().collect();
  let mut chosen_slaves = Vec::<SlaveGroupId>::new();
  for _ in 0..num_rms {
    let r = sim.rand.next_u32() % all_slaves.len() as u32;
    chosen_slaves.push(all_slaves.remove(r as usize));
  }

  let request = msg::SimpleRequest { query_id: mk_qid(&mut sim.rand), rms: chosen_slaves };
  sim.add_msg(
    msg::NetworkMessage::Slave(msg::SlaveMessage::ExternalMessage(
      msg::ExternalMessage::SimpleRequest(request),
    )),
    &client_eid,
    &tm_eid,
  );

  /// The number of iterations we simulate for, where we check 2PC
  /// consistency after each iteration.
  const NUM_CONSISTENCY_ITERATIONS: u32 = 5;
  /// Number of iterations per iteration.
  const MS_PER_ITERATION: u32 = 5;

  // Continue simulating, checking 2PC Consistency after each round
  for i in 0..NUM_CONSISTENCY_ITERATIONS {
    sim.simulate_n_ms(MS_PER_ITERATION);
    if !check_consistency(&mut sim) {
      let time_elapsed = i * MS_PER_ITERATION;
      print!("Test Failed: Consistency check after {}ms failed.", time_elapsed);
      return;
    }
  }

  // Finally, run the Simulation in Cooldown Mode and test for STMPaxos2PC
  // completion at end. "Cooldown Mode" is defined to be where no Leadership changes occur.
  sim.sim_params.pl_entry_delivery_prob = 70;
  sim.sim_params.global_pl_insertion_prob = 30;

  /// Here, "cooldown ms" are the number of milliseconds that we expect the STMPaxos2PC to finish,
  /// given that no leadership changes happen during this time. Although this can be calculated,
  /// we simply guess a sensible number for expedience.
  const EXPECTED_COOLDOWN_MS: u32 = 100;

  sim.simulate_n_ms(EXPECTED_COOLDOWN_MS);

  if !check_consistency(&mut sim) {
    print!("Test Failed: Consistency check after cooldown failed.");
    return;
  }

  if !check_completion(&mut sim) {
    print!("Test Failed: STMPaxos2PC did not complete after cooldown.");
    return;
  }

  print!("Test Successful!");
}
