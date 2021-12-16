use crate::simulation::Simulation;
use runiversal::model::common::{EndpointId, RequestId, SlaveGroupId, TableView};
use runiversal::model::message as msg;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::mk_slave_eid;
use runiversal::test_utils::{cno, cvi, cvs, mk_eid, mk_sid};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

pub struct TestContext {
  next_request_idx: u32,
  /// The client that we always use.
  pub sender_eid: EndpointId,
  /// The master node that we always use.
  master_eid: EndpointId,
  /// The slave node that we always use.
  slave_eid: EndpointId,
}

impl TestContext {
  pub fn new() -> TestContext {
    TestContext {
      next_request_idx: 0,
      sender_eid: mk_eid("ce0"),
      master_eid: mk_eid("me0"),
      slave_eid: mk_eid("se0"),
    }
  }

  /// Executes the DDL `query` using `sim` with a time limit of `time_limit`. If the query
  /// finishes, we check that it succeeded.
  pub fn send_ddl_query(&mut self, sim: &mut Simulation, query: &str, time_limit: u32) {
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
    let response = sim.get_responses(&self.sender_eid).iter().last().unwrap();
    match response {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(payload)) => {
        assert_eq!(payload.request_id, request_id)
      }
      _ => panic!("Incorrect Response: {:#?}", response),
    }
  }

  /// Enque `query` into `sim` and return the `RequestId` that was used for it.
  pub fn send_query(&mut self, sim: &mut Simulation, query: &str) -> RequestId {
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
    request_id
  }

  /// Send a Cancellation request for the given `RequestId`.
  pub fn send_cancellation(&mut self, sim: &mut Simulation, request_id: RequestId) {
    sim.add_msg(
      msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
        msg::SlaveExternalReq::CancelExternalQuery(msg::CancelExternalQuery {
          sender_eid: self.sender_eid.clone(),
          request_id,
        }),
      )),
      &self.sender_eid,
      &self.slave_eid,
    );
  }

  /// Executes the `query` using `sim` with a time limit of `time_limit`. If the query
  /// finishes, we check that it succeeded and that the resulting `TableView` is the same
  /// as `expr_result`.
  pub fn execute_query(
    &mut self,
    sim: &mut Simulation,
    query: &str,
    time_limit: u32,
    exp_result: TableView,
  ) {
    let request_id = self.send_query(sim, query);
    assert!(simulate_until_response(sim, &self.sender_eid, time_limit));
    let response = sim.get_responses(&self.sender_eid).iter().last().unwrap();
    match response {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
        assert_eq!(payload.request_id, request_id);
        assert_eq!(payload.result, exp_result);
      }
      _ => panic!("Incorrect Response: {:#?}", response),
    }
  }
}

/// Simulations `sim` until an External response is collected at `eid`, or until
/// `time_limit` milliseconds have passed.
pub fn simulate_until_response(sim: &mut Simulation, eid: &EndpointId, time_limit: u32) -> bool {
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

pub fn setup() -> (Simulation, TestContext) {
  setup_with_seed([0; 16])
}

pub fn setup_with_seed(seed: [u8; 16]) -> (Simulation, TestContext) {
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

  let sim =
    Simulation::new(seed, 1, slave_address_config, master_address_config, PaxosConfig::prod());
  let context = TestContext::new();
  (sim, context)
}

pub fn setup_inventory_table(sim: &mut Simulation, context: &mut TestContext) {
  {
    context.send_ddl_query(
      sim,
      " CREATE TABLE inventory (
          product_id INT PRIMARY KEY,
          email      VARCHAR,
          count      INT
        );
      ",
      10000,
    );
  }
}

pub fn populate_inventory_table_basic(sim: &mut Simulation, context: &mut TestContext) {
  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    context.execute_query(
      sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (0, 'my_email_0', 15),
               (1, 'my_email_1', 25);
      ",
      10000,
      exp_result,
    );
  }
}

pub fn setup_user_table(sim: &mut Simulation, context: &mut TestContext) {
  {
    context.send_ddl_query(
      sim,
      " CREATE TABLE user (
          email      VARCHAR PRIMARY KEY,
          balance    INT,
        );
      ",
      10000,
    );
  }
}

pub fn populate_setup_user_table_basic(sim: &mut Simulation, context: &mut TestContext) {
  {
    let mut exp_result = TableView::new(vec![cno("email"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(60))]);
    exp_result.add_row(vec![Some(cvs("my_email_2")), Some(cvi(70))]);
    context.execute_query(
      sim,
      " INSERT INTO user (email, balance)
        VALUES ('my_email_0', 50),
               ('my_email_1', 60),
               ('my_email_2', 70);
      ",
      10000,
      exp_result,
    );
  }
}
