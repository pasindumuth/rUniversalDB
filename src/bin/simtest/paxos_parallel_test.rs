use crate::basic_serial_test::mk_general_sim;
use crate::serial_test_utils::{setup, simulate_until_clean, TestContext};
use crate::simulation::Simulation;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{mk_rid, mk_t, Timestamp};
use runiversal::model::common::iast;
use runiversal::model::common::{
  EndpointId, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, RequestId, SlaveGroupId,
};
use runiversal::model::message as msg;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::mk_slave_eid;
use runiversal::sql_parser::convert_ast;
use runiversal::test_utils::{mk_eid, mk_seed, mk_sid};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Query Generation Utils
// -----------------------------------------------------------------------------------------------

fn mk_uint(r: &mut XorShiftRng, abs: u32) -> u32 {
  r.next_u32() % abs
}

fn mk_int(r: &mut XorShiftRng, abs: u32) -> i32 {
  let val = (r.next_u32() % abs) as i32;
  if r.next_u32() % 2 == 0 {
    -val
  } else {
    val
  }
}

fn mk_inventory_insert(r: &mut XorShiftRng) -> String {
  let mut values = Vec::<String>::new();
  let num_vals = r.next_u32() % 5;
  for _ in 0..num_vals {
    values.push(format!(
      "({}, 'my_email_{}', {})",
      mk_int(r, 100),
      mk_uint(r, 100),
      mk_int(r, 100)
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
      mk_uint(r, 5),
      mk_int(r, 100)
    )
  } else if query_type == 1 {
    format!(
      " UPDATE inventory
        SET count = count - {}
        WHERE product_id >= {};
      ",
      mk_uint(r, 5),
      mk_int(r, 100)
    )
  } else if query_type == 2 {
    format!(
      " UPDATE inventory
        SET email = 'my_email_{}'
        WHERE count >= {};
      ",
      mk_uint(r, 100),
      mk_int(r, 100)
    )
  } else {
    panic!()
  }
}

fn mk_inventory_delete(r: &mut XorShiftRng) -> String {
  let query_type = r.next_u32() % 3;
  if query_type == 0 {
    format!(
      " DELETE
        FROM inventory
        WHERE count >= {};
      ",
      mk_int(r, 100)
    )
  } else if query_type == 1 {
    format!(
      " DELETE
        FROM inventory
        WHERE count >= {} AND count < {};
      ",
      mk_int(r, 50),
      mk_int(r, 50) + 100
    )
  } else if query_type == 2 {
    format!(
      " DELETE
        FROM inventory
        WHERE product_id >= {} AND product_id < {};
      ",
      mk_int(r, 50),
      mk_int(r, 50) + 100
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
      mk_int(r, 100)
    )
  } else if query_type == 1 {
    format!(
      " SELECT product_id
        FROM inventory
        WHERE count >= {} AND count < {};
      ",
      mk_int(r, 50),
      mk_int(r, 50) + 100
    )
  } else if query_type == 2 {
    format!(
      " SELECT count
        FROM inventory
        WHERE product_id >= {} AND product_id < {};
      ",
      mk_int(r, 50),
      mk_int(r, 50) + 100
    )
  } else {
    panic!()
  }
}

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

/// Results of `verify_req_res`, which contains extra statistics useful for checking
/// non-triviality of the test.
struct VerifyResult {
  replay_duration: Timestamp,
  total_queries: u32,
  successful_queries: u32,
  num_selects: u32,
  average_select_rows: f32,
}

/// Replay the requests that succeeded in timestamp order serially, and very that
/// the results are the same.
fn verify_req_res(
  rand: &mut XorShiftRng,
  req_res_map: BTreeMap<RequestId, (msg::PerformExternalQuery, msg::ExternalMessage)>,
) -> Option<VerifyResult> {
  // Sort the request-responses and filter out failures.
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

  // Compute various statistics
  let mut num_selects = 0;
  let mut row_sum = 0;
  for (_, (req, res)) in &sorted_success_res {
    let parsed_ast = Parser::parse_sql(&GenericDialect {}, &req.query).unwrap();
    let ast = convert_ast(parsed_ast).unwrap();
    match ast.body {
      iast::QueryBody::SuperSimpleSelect(_) => {
        num_selects += 1;
        row_sum += res.result.rows.len();
      }
      _ => {}
    }
  }
  let average_select_rows = row_sum as f32 / num_selects as f32;

  // Run the Replay
  let (mut sim, mut ctx) = setup(mk_seed(rand));
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

  Some(VerifyResult {
    replay_duration: sim.true_timestamp().clone(),
    total_queries,
    successful_queries,
    num_selects,
    average_select_rows,
  })
}

// -----------------------------------------------------------------------------------------------
//  test_all_paxos_parallel
// -----------------------------------------------------------------------------------------------

pub fn test_all_basic_parallel(rand: &mut XorShiftRng) {
  for i in 0..50 {
    println!("Running round {:?}", i);
    parallel_test(mk_seed(rand), 1);
  }
}

pub fn test_all_paxos_parallel(rand: &mut XorShiftRng) {
  for i in 0..50 {
    println!("Running round {:?}", i);
    parallel_test(mk_seed(rand), 5);
  }
}

pub fn parallel_test(seed: [u8; 16], num_paxos_nodes: u32) {
  let mut sim = mk_general_sim(seed, 3, 5, num_paxos_nodes);
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
  let sim_duration = mk_t(SIM_DURATION);
  for iteration in 0.. {
    let timestamp = sim.true_timestamp();
    if timestamp >= &sim_duration {
      break;
    }

    // Generate a Query
    let query = if iteration < 3 {
      // For the first few iterations, we always choose Insert.
      mk_inventory_insert(&mut sim.rand)
    } else {
      // Otherwise, we randomly generate any type of query chosen using a hard-coded distribution.
      let i = sim.rand.next_u32() % 100;
      if i < 18 {
        mk_inventory_insert(&mut sim.rand)
      } else if i < 53 {
        mk_inventory_update(&mut sim.rand)
      } else if i < 88 {
        mk_inventory_select(&mut sim.rand)
      } else {
        mk_inventory_delete(&mut sim.rand)
      }
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

    // Potentially start a Leadership change in a node by randomly choosing a PaxosGroupId.
    // Note: that we only do this if the PaxosGroups have more than 1 element.
    if num_paxos_nodes > 1 {
      if !sim.is_leadership_changing() {
        if sim.rand.next_u32() % 5 == 0 {
          let gid = gids.get(sim.rand.next_u32() as usize % gids.len()).unwrap();
          sim.start_leadership_change(gid.clone());
        }
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
  let end_time = sim.true_timestamp().add(mk_t(RESPONSE_TIME_LIMIT));
  while sim.true_timestamp() < &end_time {
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
  if let Some(res) = verify_req_res(&mut sim.rand, req_res_map) {
    // Count the number of Leadership changes.
    let mut num_leadership_changes = 0;
    for (_, lid) in &sim.leader_map {
      num_leadership_changes += lid.gen.0;
    }

    println!(
      "Test 'test_all_paxos_parallel' Passed! Replay time taken: {:?}ms.
       Total Queries: {:?}, Succeeded: {:?}, Leadership Changes: {:?}, 
       # Selects: {:?}, Avg. Selected Rows: {:?}",
      res.replay_duration.time_ms,
      res.total_queries,
      res.successful_queries,
      num_leadership_changes,
      res.num_selects,
      res.average_select_rows
    );
  } else {
    println!("Skipped Test 'test_all_paxos_parallel' due to Timestamp Conflict");
  }
}
