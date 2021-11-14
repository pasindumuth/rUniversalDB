use crate::message as msg;
use crate::simple_tm_es::SimplePayloadTypes;
use crate::simulation::{mk_client_eid, mk_slave_eid, Simulation};
use crate::slave::SlavePLm;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::mk_qid;
use runiversal::model::common::{EndpointId, SlaveGroupId};
use runiversal::paxos2pc_tm::RMPLm;
use runiversal::test_utils::mk_sid;
use std::collections::BTreeMap;

enum CompletionResult {
  Invalid,
  SuccessfullyCommitted,
  SuccessfullyAborted,
  SuccessfullyTrivial,
}

/// This checks for 2PC Completion. Recall that 2PC Completion is where every
/// RM either Commits or Aborts.
fn check_completion(sim: &Simulation, rms: &Vec<SlaveGroupId>) -> CompletionResult {
  let mut rms_plms = BTreeMap::<SlaveGroupId, Vec<RMPLm<SimplePayloadTypes>>>::new();

  // Add RMPLms
  for rm in rms {
    rms_plms.insert(rm.clone(), vec![]);
    for pl_entry in sim.global_pls.get(rm).unwrap() {
      if let msg::PLEntry::Bundle(bundle) = pl_entry {
        for plm in &bundle.plms {
          if let SlavePLm::SimpleRM(rm_plm) = plm {
            rms_plms.get_mut(rm).unwrap().push(rm_plm.clone());
          }
        }
      }
    }
  }

  // Check if the execution was trivial
  let mut is_trivial = true;
  for (_, rm_plms) in &rms_plms {
    match rm_plms[..] {
      [] => continue,
      _ => {
        is_trivial = false;
        break;
      }
    }
  }
  if is_trivial {
    return CompletionResult::SuccessfullyTrivial;
  }

  // Check if the execution committed
  let mut did_commit = true;
  for (_, rm_plms) in &rms_plms {
    match rm_plms[..] {
      [RMPLm::Prepared(_), RMPLm::Committed(_)] => continue,
      _ => {
        did_commit = false;
        break;
      }
    }
  }
  if did_commit {
    return CompletionResult::SuccessfullyCommitted;
  }

  // Check if the execution aborted
  let mut did_abort = true;
  for (_, rm_plms) in &rms_plms {
    match rm_plms[..] {
      [] => continue,
      [RMPLm::Prepared(_), RMPLm::Aborted(_)] => continue,
      _ => {
        did_abort = false;
        break;
      }
    }
  }
  if did_abort {
    return CompletionResult::SuccessfullyAborted;
  }

  // Otherwise, this was an invalid execution.
  return CompletionResult::Invalid;
}

pub fn test_single(test_num: u32, seed: [u8; 16]) {
  // Setup Simulation

  // Create 5 SlaveGroups, each with 3 nodes.
  const NUM_PAXOS_GROUPS: u32 = 5;
  const NUM_PAXOS_NODES: u32 = 3;
  let mut slave_address_config = BTreeMap::<SlaveGroupId, Vec<EndpointId>>::new();
  for i in 0..NUM_PAXOS_GROUPS {
    let mut eids = Vec::<EndpointId>::new();
    for j in 0..NUM_PAXOS_NODES {
      eids.push(mk_slave_eid(&(i * NUM_PAXOS_NODES + j)));
    }
    slave_address_config.insert(SlaveGroupId(format!("s{}", i)), eids);
  }

  let client_eid = mk_client_eid(&0);
  let mut sim = Simulation::new(seed, 1, slave_address_config.clone());

  // Run the simulation to warm it up. Activity here consists of Leadership changes,
  // Gossip, Paxos Insertions, etc.
  sim.simulate_n_ms(100);

  // Randomly construct a SimpleRequest and send it to a random Slave
  // to perform Simple Paxos2PC.

  // Take s0 to be the TM.
  let tm = mk_sid("s0");
  let tm_eid = sim.leader_map.get(&tm).unwrap().eid.clone();

  // Randomly chose RMs, where none of them are the TM.
  // Recall that Paxos2PC requires at least one.
  let num_rms = (sim.rand.next_u32() % (NUM_PAXOS_GROUPS - 1)) + 1;
  let mut all_slaves: Vec<SlaveGroupId> = slave_address_config.keys().cloned().collect();
  all_slaves.remove(all_slaves.iter().position(|i| i == &tm).unwrap());
  let mut rms = Vec::<SlaveGroupId>::new();
  for _ in 0..num_rms {
    let r = sim.rand.next_u32() % all_slaves.len() as u32;
    rms.push(all_slaves.remove(r as usize));
  }

  let request = msg::SimpleRequest { query_id: mk_qid(&mut sim.rand), rms: rms.clone() };
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
  sim.simulate_n_ms(NUM_CONSISTENCY_ITERATIONS * MS_PER_ITERATION);

  // Finally, run the Simulation in Cooldown Mode and test for Paxos2PC
  // completion at end. "Cooldown Mode" is defined to be where no Leadership changes occur.
  sim.sim_params.pl_entry_delivery_prob = 70;
  sim.sim_params.global_pl_insertion_prob = 30;

  /// Here, "cooldown ms" are the number of milliseconds that we expect the Paxos2PC to finish,
  /// given that no leadership changes happen during this time. Although this can be calculated,
  /// we simply guess a sensible number for expedience.
  const EXPECTED_COOLDOWN_MS: u32 = 500;

  sim.simulate_n_ms(EXPECTED_COOLDOWN_MS);

  match check_completion(&mut sim, &rms) {
    CompletionResult::Invalid => {
      println!(
        "{:?}. Paxos2PC Test Failed: Invalid PLs after cooldown. Seed: {:?}",
        test_num, seed
      );
    }
    CompletionResult::SuccessfullyCommitted => {
      println!("{:?}.    Paxos2PC SuccessfullyCommitted!", test_num);
    }
    CompletionResult::SuccessfullyAborted => {
      println!("{:?}.    Paxos2PC SuccessfullyAborted!", test_num);
    }
    CompletionResult::SuccessfullyTrivial => {
      println!("{:?}.    Paxos2PC SuccessfullyTrivial!", test_num);
    }
  }
}