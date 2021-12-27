use crate::simulation::Simulation;
use runiversal::model::common::{EndpointId, RequestId, SlaveGroupId, TableView};
use runiversal::model::message as msg;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::{mk_master_eid, mk_slave_eid};
use runiversal::test_utils::{cno, cvi, cvs, mk_eid, mk_sid};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Serial Utils
// -----------------------------------------------------------------------------------------------

pub struct TestContext {
  next_request_idx: u32,
  /// The client that we always use.
  pub sender_eid: EndpointId,
  /// The master node that we always use.
  master_eid: EndpointId,
  /// The slave node that we always use.
  slave_eid: EndpointId,

  /// The index that the next response sent back to `sender_eid` should take.
  next_response_idx: usize,
}

impl TestContext {
  pub fn new() -> TestContext {
    TestContext {
      next_request_idx: 0,
      sender_eid: mk_eid("ce0"),
      master_eid: mk_eid("me0"),
      slave_eid: mk_eid("se0"),
      next_response_idx: 0,
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

    assert!(self.simulate_until_response(sim, time_limit));
    let response = self.next_response(sim);
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
    assert!(self.simulate_until_response(sim, time_limit));
    let response = self.next_response(sim);
    match response {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
        assert_eq!(payload.request_id, request_id);
        assert_eq!(payload.result, exp_result);
      }
      _ => panic!("Incorrect Response: {:#?}", response),
    }
  }

  /// Same as above, except we do not check the returned resulting `TableView`.
  pub fn execute_query_simple(&mut self, sim: &mut Simulation, query: &str, time_limit: u32) {
    let request_id = self.send_query(sim, query);
    assert!(self.simulate_until_response(sim, time_limit));
    let response = self.next_response(sim);
    match response {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
        assert_eq!(payload.request_id, request_id);
      }
      _ => panic!("Incorrect Response: {:#?}", response),
    }
  }

  /// Executes the `query` using `sim` with a time limit of `time_limit`. Here,
  /// we expect it to fail.
  pub fn execute_query_failure<PredT: Fn(&msg::ExternalAbortedData) -> bool>(
    &mut self,
    sim: &mut Simulation,
    query: &str,
    time_limit: u32,
    abort_check: PredT,
  ) {
    let request_id = self.send_query(sim, query);
    assert!(self.simulate_until_response(sim, time_limit));
    let response = self.next_response(sim);
    match response {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(payload)) => {
        assert_eq!(payload.request_id, request_id);
        assert!(abort_check(&payload.payload));
      }
      _ => panic!("Incorrect Response: {:#?}", response),
    }
  }

  /// Simulates `sim` until an External response is collected at `eid`, or until
  /// `time_limit` milliseconds have passed. This returns true exactly when there
  /// is a new message that can be read with `next_response`.
  pub fn simulate_until_response(&mut self, sim: &mut Simulation, time_limit: u32) -> bool {
    let prev_num_responses = sim.get_responses(&self.sender_eid).len();
    if self.next_response_idx < prev_num_responses {
      // This means that the next response is already here (e.g. due to the last
      // millisecond returned 2 responses).
      true
    } else if self.next_response_idx == prev_num_responses {
      let mut duration = 0;
      while duration < time_limit {
        sim.simulate1ms();
        duration += 1;
        if sim.get_responses(&self.sender_eid).len() > self.next_response_idx {
          return true;
        }
      }
      false
    } else {
      panic!()
    }
  }

  /// Gets the response pointed to by `next_response_idx`, panicing if such a response
  /// does not exist. Make sure that a call to `simulate_until_response` returns true
  /// to ensure this does not happen.
  ///
  /// Note: that this is not idempotent.
  pub fn next_response<'a>(&mut self, sim: &'a mut Simulation) -> &'a msg::NetworkMessage {
    let response = sim.get_responses(&self.sender_eid).get(self.next_response_idx).unwrap();
    self.next_response_idx += 1;
    response
  }
}

/// Simulates `sim` until all of them are in their steady state, or until
/// `time_limit` milliseconds have passed.
///
/// NOTE: If this fails, to find out where, switch the argument in `check_resources_clean`
/// from `false` to `true`.
pub fn simulate_until_clean(sim: &mut Simulation, time_limit: u32) -> bool {
  let mut duration = 0;
  while duration < time_limit {
    sim.simulate1ms();
    duration += 1;
    if sim.check_resources_clean(false) {
      return true;
    }
  }
  false
}

pub fn setup(seed: [u8; 16]) -> (Simulation, TestContext) {
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

  let sim =
    Simulation::new(seed, 1, slave_address_config, master_address_config, PaxosConfig::test(), 1);
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

pub fn populate_user_table_basic(sim: &mut Simulation, context: &mut TestContext) {
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

// -----------------------------------------------------------------------------------------------
//  Parallel Utils
// -----------------------------------------------------------------------------------------------

pub fn mk_general_sim(
  seed: [u8; 16],
  num_clients: u32,
  num_paxos_groups: u32,
  num_paxos_nodes: u32,
  timestamp_suffix_divisor: u64,
) -> Simulation {
  // Create one Slave Paxos Group to test Leader change logic with.
  let mut master_address_config = Vec::<EndpointId>::new();
  for i in 0..num_paxos_nodes {
    master_address_config.push(mk_master_eid(i));
  }
  let mut slave_address_config = BTreeMap::<SlaveGroupId, Vec<EndpointId>>::new();
  for i in 0..num_paxos_groups {
    let mut eids = Vec::<EndpointId>::new();
    for j in 0..num_paxos_nodes {
      eids.push(mk_slave_eid(i * num_paxos_nodes + j));
    }
    slave_address_config.insert(SlaveGroupId(format!("s{}", i)), eids);
  }

  Simulation::new(
    seed,
    num_clients,
    slave_address_config.clone(),
    master_address_config.clone(),
    PaxosConfig::test(),
    timestamp_suffix_divisor,
  )
}
