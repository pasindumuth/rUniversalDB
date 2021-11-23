#![feature(map_first_last)]

mod simulation;

use crate::simulation::Simulation;
use runiversal::common::TableSchema;
use runiversal::model::common::{
  ColName, ColType, EndpointId, Gen, PrimaryKey, RequestId, SlaveGroupId, TablePath, TableView,
  TabletGroupId, TabletKeyRange,
};
use runiversal::model::message as msg;
use runiversal::simulation_utils::{mk_client_eid, mk_slave_eid};
use runiversal::test_utils::{cn, cvi, cvs, mk_eid, mk_sid, mk_tab, mk_tid};
use std::collections::BTreeMap;

fn main() {
  tp_test()
}

/// Simulations `sim` until an External response is collected at `eid`, or until
/// `time_limit` milliseconds have passed.
fn simulate_until_response(sim: &mut Simulation, eid: &EndpointId, time_limit: u32) -> bool {
  let mut duration = 0;
  let prev_num_responses = sim.get_responses(eid).len();
  while duration < time_limit {
    sim.simulate1ms();
    duration += 1;
    if sim.get_responses(eid).len() > prev_num_responses {
      return true;
    }
  }
  false
}

/// This is a test that solely tests Transaction Processing. We take all PaxosGroups to just
/// have one node. We only check for SQL semantics compatibility.
fn tp_test() {
  let master_address_config: Vec<EndpointId> = vec![mk_eid("me0")];
  let slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>> = vec![
    (mk_sid("s0"), vec![mk_slave_eid(&0)]),
    (mk_sid("s1"), vec![mk_slave_eid(&1)]),
    (mk_sid("s2"), vec![mk_slave_eid(&2)]),
    (mk_sid("s3"), vec![mk_slave_eid(&3)]),
    (mk_sid("s4"), vec![mk_slave_eid(&4)]),
  ]
  .into_iter()
  .collect();

  let mut sim = Simulation::new([0; 16], 1, slave_address_config, master_address_config);
  let sender_eid = mk_client_eid(&0);

  {
    // Create a table
    let query = "
      CREATE TABLE inventory (
        product_id INT PRIMARY KEY,
        email      VARCHAR
      );
    ";

    let request_id = RequestId("rid0".to_string());
    sim.add_msg(
      msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
        msg::MasterExternalReq::PerformExternalDDLQuery(msg::PerformExternalDDLQuery {
          sender_eid: sender_eid.clone(),
          request_id: request_id.clone(),
          query: query.to_string(),
        }),
      )),
      &sender_eid,
      &mk_eid("me0"),
    );

    assert!(simulate_until_response(&mut sim, &sender_eid, 100));
    match sim.get_responses(&sender_eid).iter().last().unwrap() {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(payload)) => {
        assert_eq!(payload.request_id, request_id)
      }
      _ => panic!(),
    }
  }

  {
    // Insert data into the table
    let query = "
      INSERT INTO inventory (product_id, email)
      VALUES (0, 'my_email_0'),
             (1, 'my_email_1');
    ";

    let request_id = RequestId("rid1".to_string());
    sim.add_msg(
      msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
        msg::SlaveExternalReq::PerformExternalQuery(msg::PerformExternalQuery {
          sender_eid: sender_eid.clone(),
          request_id: request_id.clone(),
          query: query.to_string(),
        }),
      )),
      &sender_eid,
      &mk_eid("se0"),
    );

    assert!(simulate_until_response(&mut sim, &sender_eid, 100));
    match sim.get_responses(&sender_eid).iter().last().unwrap() {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
        let mut exp_result = TableView::new(vec![cn("product_id"), cn("email")]);
        exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
        exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1"))]);
        assert_eq!(payload.request_id, request_id);
        assert_eq!(payload.result, exp_result);
      }
      _ => panic!(),
    }
  }

  {
    // Read data the table
    let query = "
      SELECT product_id, email
      FROM inventory
      WHERE true;
    ";

    let request_id = RequestId("rid2".to_string());
    sim.add_msg(
      msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
        msg::SlaveExternalReq::PerformExternalQuery(msg::PerformExternalQuery {
          sender_eid: sender_eid.clone(),
          request_id: request_id.clone(),
          query: query.to_string(),
        }),
      )),
      &sender_eid,
      &mk_eid("se0"),
    );

    assert!(simulate_until_response(&mut sim, &sender_eid, 100));
    match sim.get_responses(&sender_eid).iter().last().unwrap() {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
        let mut exp_result = TableView::new(vec![cn("product_id"), cn("email")]);
        exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
        exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1"))]);
        assert_eq!(payload.request_id, request_id);
        assert_eq!(payload.result, exp_result);
      }
      _ => panic!(),
    }
  }

  println!("Responses: {:#?}", sim.get_all_responses());
}
