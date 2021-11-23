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

/**
 * This suite of tests consists of simple serial Transaction Processing.
 * Only one query executes at a time.
 */

struct TestContext {
  next_request_idx: u32,
  /// The client that we always use.
  sender_eid: EndpointId,
  /// The master node that we always use.
  master_eid: EndpointId,
  /// The slave node that we always use.
  slave_eid: EndpointId,
}

impl TestContext {
  fn new() -> TestContext {
    TestContext {
      next_request_idx: 0,
      sender_eid: mk_eid("ce0"),
      master_eid: mk_eid("me0"),
      slave_eid: mk_eid("se0"),
    }
  }

  /// Executes the DDL `query` using `sim` with a time limit of `time_limit`. If the query
  /// finishes, we check that it succeeded.
  fn send_ddl_query(&mut self, sim: &mut Simulation, query: &str, time_limit: u32) {
    let request_id = RequestId(format!("rid{:?}", self.next_request_idx));
    self.next_request_idx += 1;
    sim.add_msg(
      msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
        msg::MasterExternalReq::PerformExternalDDLQuery(msg::PerformExternalDDLQuery {
          sender_eid: self.sender_eid.clone(),
          request_id: request_id.clone(),
          query: query.to_string(),
        }),
      )),
      &self.sender_eid,
      &self.master_eid,
    );

    assert!(simulate_until_response(sim, &self.sender_eid, time_limit));
    match sim.get_responses(&self.sender_eid).iter().last().unwrap() {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(payload)) => {
        assert_eq!(payload.request_id, request_id)
      }
      _ => panic!(),
    }
  }

  /// Executes the `query` using `sim` with a time limit of `time_limit`. If the query
  /// finishes, we check that it succeeded and that the resulting `TableView` is the same
  /// as `expr_result`.
  fn send_query(
    &mut self,
    sim: &mut Simulation,
    query: &str,
    time_limit: u32,
    exp_result: TableView,
  ) {
    let request_id = RequestId(format!("rid{:?}", self.next_request_idx));
    self.next_request_idx += 1;
    sim.add_msg(
      msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
        msg::SlaveExternalReq::PerformExternalQuery(msg::PerformExternalQuery {
          sender_eid: self.sender_eid.clone(),
          request_id: request_id.clone(),
          query: query.to_string(),
        }),
      )),
      &self.sender_eid,
      &self.slave_eid,
    );

    assert!(simulate_until_response(sim, &self.sender_eid, time_limit));
    match sim.get_responses(&self.sender_eid).iter().last().unwrap() {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
        assert_eq!(payload.request_id, request_id);
        assert_eq!(payload.result, exp_result);
      }
      _ => panic!(),
    }
  }
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
pub fn tp_test() {
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
  let mut context = TestContext::new();

  {
    // Create a table
    context.send_ddl_query(
      &mut sim,
      " CREATE TABLE inventory (
          product_id INT PRIMARY KEY,
          email      VARCHAR
        );
      ",
      100,
    );
  }

  {
    // Insert data into the table
    let mut exp_result = TableView::new(vec![cn("product_id"), cn("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1"))]);
    context.send_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email)
        VALUES (0, 'my_email_0'),
               (1, 'my_email_1');
      ",
      100,
      exp_result,
    );
  }

  {
    // Read data the table
    let mut exp_result = TableView::new(vec![cn("product_id"), cn("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1"))]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email
        FROM inventory
        WHERE true;
      ",
      100,
      exp_result,
    );
  }

  {
    // Update the table
    let mut exp_result = TableView::new(vec![cn("product_id"), cn("email")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_3"))]);
    context.send_query(
      &mut sim,
      " UPDATE inventory
        SET email = 'my_email_3'
        WHERE product_id = 1;
      ",
      100,
      exp_result,
    );
  }

  {
    // Read data the table
    let mut exp_result = TableView::new(vec![cn("product_id"), cn("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_3"))]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email
        FROM inventory
        WHERE true;
      ",
      100,
      exp_result,
    );
  }

  println!("Responses: {:#?}", sim.get_all_responses());
}
