use crate::basic_serial_test::mk_general_sim;
use crate::serial_test_utils::{setup_with_seed, simulate_until_clean, TestContext};
use crate::simulation::Simulation;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::mk_rid;
use runiversal::model::common::{
  EndpointId, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, RequestId, SlaveGroupId, Timestamp,
};
use runiversal::model::message as msg;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::mk_slave_eid;
use runiversal::test_utils::{mk_eid, mk_seed, mk_sid};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Query Generation Utils
// -----------------------------------------------------------------------------------------------

fn mk_inventory_insert(r: &mut XorShiftRng) -> String {
  let mut values = Vec::<String>::new();
  let num_vals = r.next_u32() % 5;
  for _ in 0..num_vals {
    values.push(format!(
      "({}, 'my_email_{}', {})",
      r.next_u32() % 100,
      r.next_u32() % 100,
      r.next_u32() % 100
    ));
  }

  format!(
    " INSERT INTO inventory (product_id, email, count)
      VALUES {};
    ",
    values.join(", ")
  )
}

fn mk_inventory_update(r: &mut XorShiftRng) -> String {
  let query_type = r.next_u32() % 3;
  if query_type == 0 {
    format!(
      " UPDATE inventory
        SET count = count + {}
        WHERE product_id >= {};
      ",
      r.next_u32() % 5,
      r.next_u32() % 100
    )
  } else if query_type == 1 {
    format!(
      " UPDATE inventory
        SET count = count - {}
        WHERE product_id >= {};
      ",
      r.next_u32() % 5,
      r.next_u32() % 100
    )
  } else if query_type == 2 {
    format!(
      " UPDATE inventory
        SET email = 'my_email_{}'
        WHERE count >= {};
      ",
      r.next_u32() % 100,
      r.next_u32() % 100
    )
  } else {
    panic!()
  }
}

fn mk_inventory_select(r: &mut XorShiftRng) -> String {
  let query_type = r.next_u32() % 3;
  if query_type == 0 {
    format!(
      " SELECT email
        FROM inventory
        WHERE count >= {};
      ",
      r.next_u32() % 100
    )
  } else if query_type == 1 {
    format!(
      " SELECT product_id
        FROM inventory
        WHERE count >= {} AND count < {};
      ",
      r.next_u32() % 50,
      r.next_u32() % 50 + 50
    )
  } else if query_type == 2 {
    format!(
      " SELECT count
        FROM inventory
        WHERE product_id >= {} AND product_id < {};
      ",
      r.next_u32() % 50,
      r.next_u32() % 50 + 50
    )
  } else {
    panic!()
  }
}

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

/// Replay the requests that succeeded in timestamp order serially, and very that
/// the results are the same.
fn verify_req_res(
  rand: &mut XorShiftRng,
  req_res_map: BTreeMap<RequestId, (msg::PerformExternalQuery, msg::ExternalMessage)>,
) -> Option<(u32, u32, u32)> {
  let (mut sim, mut ctx) = setup_with_seed(mk_seed(rand));
  let mut sorted_success_res =
    BTreeMap::<Timestamp, (msg::PerformExternalQuery, msg::ExternalQuerySuccess)>::new();
  let total_queries = req_res_map.len() as u32;
  for (_, (req, res)) in req_res_map {
    if let msg::ExternalMessage::ExternalQuerySuccess(success) = res {
      if !sorted_success_res.insert(success.timestamp.clone(), (req, success)).is_none() {
        // Here, two responses had the same timestamp. We cannot replay this, so we
        // simply skip this test.
        return None;
      }
    }
  }

  {
    ctx.send_ddl_query(
      &mut sim,
      " CREATE TABLE inventory (
          product_id INT PRIMARY KEY,
          email      VARCHAR,
          count      INT
        );
      ",
      10000,
    );
  }

  let successful_queries = sorted_success_res.len() as u32;
  for (_, (req, res)) in sorted_success_res {
    ctx.execute_query(&mut sim, req.query.as_str(), 10000, res.result);
  }

  Some((*sim.true_timestamp() as u32, total_queries, successful_queries))
}

// -----------------------------------------------------------------------------------------------
//  test_all_paxos_parallel
// -----------------------------------------------------------------------------------------------

pub fn test_all_paxos_parallel(rand: &mut XorShiftRng) {
  for i in 0..50 {
    println!("Running round {:?}", i);
    paxos_parallel_test(rand);
  }
}

pub fn paxos_parallel_test(rand: &mut XorShiftRng) {
  let mut sim = mk_general_sim(mk_seed(rand), 3, 5, 5);
  let mut ctx = TestContext::new();

  // Setup Tables

  {
    ctx.send_ddl_query(
      &mut sim,
      " CREATE TABLE inventory (
          product_id INT PRIMARY KEY,
          email      VARCHAR,
          count      INT
        );
      ",
      10000,
    );
  }

  sim.remove_all_responses(); // Clear the DDL response.

  // Run the simulation
  let client_eids: Vec<_> = sim.get_all_responses().keys().cloned().collect();
  let sids: Vec<_> = sim.slave_address_config().keys().cloned().collect();
  let gids: Vec<_> = sim.leader_map.keys().cloned().collect();

  // These 2 are kept in sync, where the set of RequestIds in each map are always the same.
  let mut req_lid_map = BTreeMap::<RequestId, (PaxosGroupId, LeadershipId)>::new();
  let mut req_map = BTreeMap::<EndpointId, BTreeMap<RequestId, msg::PerformExternalQuery>>::new();
  for eid in &client_eids {
    req_map.insert(eid.clone(), BTreeMap::new());
  }

  // Elements from the above are moved here as responses arrive
  let mut req_res_map =
    BTreeMap::<RequestId, (msg::PerformExternalQuery, msg::ExternalMessage)>::new();

  const SIM_DURATION: u128 = 1000; // The duration that we run the simulation
  while sim.true_timestamp() < &SIM_DURATION {
    // Generate a random query
    let query_type = sim.rand.next_u32() % 3;
    let query = match query_type {
      0 => mk_inventory_insert(&mut sim.rand),
      1 => mk_inventory_update(&mut sim.rand),
      _ => mk_inventory_select(&mut sim.rand),
    };

    // Construct a request and populate `req_map`
    let request_id = mk_rid(&mut sim.rand);
    let client_idx = sim.rand.next_u32() as usize % client_eids.len();
    let client_eid = client_eids.get(client_idx).unwrap();

    let perform = msg::PerformExternalQuery {
      sender_eid: client_eid.clone(),
      request_id: request_id.clone(),
      query: query.to_string(),
    };
    req_map.get_mut(client_eid).unwrap().insert(request_id.clone(), perform.clone());

    // Choose a SlaveGroupId, record its leadership, and then send the `query` to it.
    let slave_idx = sim.rand.next_u32() as usize % sids.len();
    let sid = sids.get(slave_idx).unwrap();
    let gid = sid.to_gid();
    let cur_lid = sim.leader_map.get(&gid).unwrap().clone();
    req_lid_map.insert(request_id, (gid, cur_lid.clone()));
    sim.add_msg(
      msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
        msg::SlaveExternalReq::PerformExternalQuery(perform),
      )),
      client_eid,
      &cur_lid.eid,
    );

    // TODO: pass in seeds into the test cases instead of the whole rand to avoid
    //  the possibility of using it more than once (which makes the test not-reproducible
    //  with just a seed value).
    // TODO: count the number of Leadership changes and report it. (Just iterate through
    //  leader_map and take the sum of the gens). Also, report the average number of rows
    //  returned by SELECT and UPDATE queries (not inserts since those are trivial).
    // TODO: Also do DELETE queries data.

    // Potentially start a Leadership change in a node by randomly choosing a PaxosGroupId.
    if !sim.is_leadership_changing() {
      if sim.rand.next_u32() % 5 == 0 {
        let gid = gids.get(sim.rand.next_u32() as usize % gids.len()).unwrap();
        sim.start_leadership_change(gid.clone());
      }
    }

    // Simulate for at-most 50 ms at a time
    let sim_duration = sim.rand.next_u32() % 50;
    sim.simulate_n_ms(sim_duration);

    // Move any new responses to to `req_res_map`.
    for (eid, responses) in sim.remove_all_responses() {
      for res in responses {
        let external = cast!(msg::NetworkMessage::External, res).unwrap();
        let request_id = match &external {
          msg::ExternalMessage::ExternalQuerySuccess(success) => &success.request_id,
          msg::ExternalMessage::ExternalQueryAborted(aborted) => &aborted.request_id,
          _ => panic!(),
        };

        let req = req_map.get_mut(&eid).unwrap().remove(request_id).unwrap();
        req_lid_map.remove(request_id);
        req_res_map.insert(request_id.clone(), (req, external));
      }
    }
  }

  // Iterate for some time limit to receiving responses
  const RESPONSE_TIME_LIMIT: u128 = 10000;
  let start_time = *sim.true_timestamp();
  while *sim.true_timestamp() < start_time + RESPONSE_TIME_LIMIT {
    // Next, we see if all unresponded requests have an old Leadership or not.
    let mut all_old = true;
    for (_, (gid, lid)) in &req_lid_map {
      let cur_lid = sim.leader_map.get(&gid).unwrap();
      if cur_lid.gen >= lid.gen {
        all_old = false;
        break;
      }
    }

    // Break out if we are done.
    if all_old {
      break;
    } else {
      // Otherwise, simulate for 50ms.
      sim.simulate_n_ms(50);

      // Move any new responses to to `req_res_map`.
      for (eid, responses) in sim.remove_all_responses() {
        for res in responses {
          let external = cast!(msg::NetworkMessage::External, res).unwrap();
          let request_id = match &external {
            msg::ExternalMessage::ExternalQuerySuccess(success) => &success.request_id,
            msg::ExternalMessage::ExternalQueryAborted(aborted) => &aborted.request_id,
            _ => panic!(),
          };

          let req = req_map.get_mut(&eid).unwrap().remove(request_id).unwrap();
          req_lid_map.remove(request_id);
          req_res_map.insert(request_id.clone(), (req, external));
        }
      }
    }
  }

  // Simulate more for a cooldown time and verify that all resources get cleaned up.
  assert!(simulate_until_clean(&mut sim, 10000));

  // Verify the responses are correct
  if let Some((true_time, total_queries, successful_queries)) =
    verify_req_res(&mut sim.rand, req_res_map)
  {
    // Count the number of Leadership changes.
    let mut num_leadership_changes = 0;
    for (_, lid) in &sim.leader_map {
      num_leadership_changes += lid.gen.0;
    }

    println!(
      "Test 'test_all_paxos_parallel' Passed! Replay time taken: {:?}ms.
       Total Queries: {:?}, Succeeded: {:?}, Leadership Changes: {:?}",
      true_time, total_queries, successful_queries, num_leadership_changes
    );
  } else {
    println!("Skipped Test 'test_all_paxos_parallel' due to Timestamp Conflict");
  }
}
