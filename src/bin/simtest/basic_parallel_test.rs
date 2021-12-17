use crate::serial_test_utils::{setup, TestContext};
use crate::simulation::Simulation;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::mk_rid;
use runiversal::model::common::{EndpointId, RequestId, SlaveGroupId, Timestamp};
use runiversal::model::message as msg;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::mk_slave_eid;
use runiversal::test_utils::{mk_eid, mk_sid};
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
  req_res_map: BTreeMap<RequestId, (msg::PerformExternalQuery, msg::ExternalMessage)>,
) -> Option<(u32, u32, u32)> {
  let (mut sim, mut ctx) = setup();
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
      100,
    );
  }

  let successful_queries = sorted_success_res.len() as u32;
  for (_, (req, res)) in sorted_success_res {
    ctx.execute_query(&mut sim, req.query.as_str(), 10000, res.result);
  }

  Some((*sim.true_timestamp() as u32, total_queries, successful_queries))
}

// -----------------------------------------------------------------------------------------------
//  test_all_basic_parallel
// -----------------------------------------------------------------------------------------------

pub fn test_all_basic_parallel() {
  let mut orig_rand = XorShiftRng::from_seed([0; 16]);
  for i in 0..50 {
    let mut seed = [0; 16];
    orig_rand.fill_bytes(&mut seed);
    println!("Running round {:?}", i);
    basic_parallel_test(seed);
  }
}

pub fn basic_parallel_test(seed: [u8; 16]) {
  let master_address_config: Vec<EndpointId> = vec![mk_eid("me0")];
  let slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>> = vec![
    (mk_sid("s0"), vec![mk_slave_eid(0)]),
    (mk_sid("s1"), vec![mk_slave_eid(1)]),
    (mk_sid("s2"), vec![mk_slave_eid(2)]),
    (mk_sid("s3"), vec![mk_slave_eid(3)]),
    (mk_sid("s4"), vec![mk_slave_eid(4)]),
  ]
  .into_iter()
  .collect();

  // We create 3 clients.
  let mut sim =
    Simulation::new(seed, 3, slave_address_config, master_address_config, PaxosConfig::prod());
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
      100,
    );
  }

  sim.remove_all_responses(); // Clear the DDL response.

  // Run the simulation
  let client_eids: Vec<_> = sim.get_all_responses().keys().cloned().collect();
  let mut req_map = BTreeMap::<EndpointId, BTreeMap<RequestId, msg::PerformExternalQuery>>::new();
  for eid in &client_eids {
    req_map.insert(eid.clone(), BTreeMap::new());
  }

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
    let client_idx = (sim.rand.next_u32() % client_eids.len() as u32) as usize;
    let client_eid = client_eids.get(client_idx).unwrap();

    let perform = msg::PerformExternalQuery {
      sender_eid: client_eid.clone(),
      request_id: request_id.clone(),
      query: query.to_string(),
    };
    req_map.get_mut(client_eid).unwrap().insert(request_id, perform.clone());

    // Send the request and simulate
    let slave_idx = sim.rand.next_u32() % client_eids.len() as u32;
    let slave_eid = mk_slave_eid(slave_idx);
    sim.add_msg(
      msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
        msg::SlaveExternalReq::PerformExternalQuery(perform),
      )),
      client_eid,
      &slave_eid,
    );

    let sim_duration = sim.rand.next_u32() % 50; // simulation only 50 ms at a time
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
        req_res_map.insert(request_id.clone(), (req, external));
      }
    }
  }

  // Verify the responses are correct
  if let Some((true_time, total_queries, successful_queries)) = verify_req_res(req_res_map) {
    println!(
      "Test 'test_all_basic_parallel' Passed! Replay time taken: {:?}ms.
       Total Queries: {:?}, Succeeded: {:?}",
      true_time, total_queries, successful_queries
    );
  } else {
    println!("Skipped Test 'test_all_basic_parallel' due to Timestamp Conflict");
  }
}
