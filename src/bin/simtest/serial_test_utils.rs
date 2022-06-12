use crate::simulation::Simulation;
use log::warn;
use runiversal::common::{mk_t, ColName, ColValN, QueryResult, RangeEnds};
use runiversal::common::{EndpointId, PaxosGroupId, RequestId, SlaveGroupId};
use runiversal::coord::CoordConfig;
use runiversal::free_node_manager::FreeNodeType;
use runiversal::master::MasterConfig;
use runiversal::message as msg;
use runiversal::node::NodeConfig;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::{mk_client_eid, mk_node_eid};
use runiversal::slave::SlaveConfig;
use runiversal::tablet::TabletConfig;
use runiversal::test_utils::{cno, cvi, cvs, mk_sid};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Serial Utils
// -----------------------------------------------------------------------------------------------

pub struct TestContext {
  next_request_idx: u32,
  /// The client that we always use.
  sender_eid: EndpointId,
  /// The master node that we always use.
  master_eid: EndpointId,
  /// The slave node that we always use.
  slave_eid: EndpointId,

  /// The index that the next response sent back to `sender_eid` should take.
  next_response_idx: usize,
}

impl TestContext {
  /// This takes in the simulation that will be used in the other methods in this class.
  /// Importantly, there should be a Master and at least one SlaveGroup already. There
  /// should also at least be client in `sim`.
  pub fn new(sim: &Simulation) -> TestContext {
    // Extract a `master_eid`
    let gossip = sim.full_db_schema();
    let master_eid = gossip.master_address_config.get(0).unwrap().clone();

    // Extract a `slave_eid`
    let mut it = gossip.slave_address_config.iter();
    let (_, slave_eids) = it.next().unwrap();
    let slave_eid = slave_eids.get(0).unwrap().clone();

    TestContext {
      next_request_idx: 0,
      sender_eid: mk_client_eid(0),
      master_eid,
      slave_eid,
      next_response_idx: 0,
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  Const Getters
  // -----------------------------------------------------------------------------------------------

  pub fn sender_eid(&self) -> &EndpointId {
    &self.sender_eid
  }

  pub fn slave_eid(&self) -> &EndpointId {
    &self.slave_eid
  }

  // -----------------------------------------------------------------------------------------------
  //  Execution Methods
  // -----------------------------------------------------------------------------------------------

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
  /// finishes, we check that it succeeded and that the resulting `ResultView` is the same
  /// as `expr_result`.
  pub fn execute_query(
    &mut self,
    sim: &mut Simulation,
    query: &str,
    time_limit: u32,
    exp_result: QueryResult,
  ) {
    let request_id = self.send_query(sim, query);
    assert!(self.simulate_until_response(sim, time_limit));
    let response = self.next_response(sim);
    match response {
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
        assert_eq!(payload.request_id, request_id);
        if payload.result != exp_result {
          println!("{:#?}", query);
          println!("{:#?}", request_id);
          assert_eq!(payload.result, exp_result);
        }
      }
      _ => panic!("Incorrect Response: {:#?}", response),
    }
  }

  /// Same as above, except we do not check the returned resulting `ResultView`.
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
        if !abort_check(&payload.payload) {
          panic!("Incorrect error payload: {:#?}", payload);
        }
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

/// Simple common setup with a PaxosGroup size of 1.
pub fn setup(seed: [u8; 16]) -> (Simulation, TestContext) {
  let sim = mk_general_sim(seed, 1, 5, 1, 1, 0);
  let context = TestContext::new(&sim);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("email"), cno("balance")]);
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

/// Build the `NodeConfig` we should use for testing
fn get_test_configs(num_paxos_groups: u32, timestamp_suffix_divisor: u64) -> NodeConfig {
  let paxos_config = PaxosConfig {
    heartbeat_threshold: 3,
    heartbeat_period_ms: mk_t(5),
    next_index_period_ms: mk_t(10),
    retry_defer_time_ms: mk_t(5),
    proposal_increment: 1000,
    remote_next_index_thresh: 5,
    max_failable: 1,
  };

  let free_node_heartbeat_timer_ms = 5;
  let remote_leader_changed_period_ms = 5;
  let failure_detector_period_ms = 5;
  let check_unconfirmed_eids_period_ms = 15;
  let master_config = MasterConfig {
    timestamp_suffix_divisor,
    slave_group_size: num_paxos_groups,
    remote_leader_changed_period_ms,
    failure_detector_period_ms,
    check_unconfirmed_eids_period_ms,
    gossip_data_period_ms: 5,
    num_coords: 3,
    free_node_heartbeat_timer_ms,
  };
  let slave_config = SlaveConfig {
    timestamp_suffix_divisor,
    remote_leader_changed_period_ms,
    failure_detector_period_ms,
    check_unconfirmed_eids_period_ms,
  };

  let coord_config = CoordConfig { timestamp_suffix_divisor };
  let tablet_config = TabletConfig { timestamp_suffix_divisor };

  // Combine the above
  NodeConfig {
    free_node_heartbeat_timer_ms,
    paxos_config,
    coord_config,
    master_config,
    slave_config,
    tablet_config,
  }
}

/// Constructs a `Simulation` an instantiates the rUniversalDB by creating a MasterGroup,
/// then `num_slave_groups` number of SlaveGroups, and `num_clients` number of clients.
pub fn mk_general_sim(
  seed: [u8; 16],
  num_clients: u32,
  num_slave_groups: u32,
  num_paxos_nodes: u32,
  timestamp_suffix_divisor: u64,
  num_reconfig_free_nodes: u32,
) -> Simulation {
  // Create the sim
  let num_count = (num_slave_groups + 1) * num_paxos_nodes + num_reconfig_free_nodes;
  let node_config = get_test_configs(num_paxos_nodes, timestamp_suffix_divisor);
  let mut sim = Simulation::new(seed, num_clients, num_count, node_config);

  // Construct the Master PaxosGroup to initiate the system.
  let master_eids: Vec<_> =
    RangeEnds::rvec(0, num_paxos_nodes).iter().map(|i| mk_node_eid(*i)).collect();

  // We take the first client as the admin client which starts the Master group.
  let admin_client = mk_client_eid(0);
  for eid in &master_eids {
    sim.add_msg(
      msg::NetworkMessage::FreeNode(msg::FreeNodeMessage::StartMaster(msg::StartMaster {
        master_eids: master_eids.clone(),
      })),
      &admin_client,
      eid,
    );
  }

  // Simulate until all Master nodes come into Existence.
  for _ in 0..10000 {
    sim.simulate1ms();
    if sim.do_nodes_exist(&master_eids) {
      break;
    }
  }

  // Assert that the above managed to create the Master.
  assert!(sim.do_nodes_exist(&master_eids));

  // Next, we need to register as many nodes as necessary as FreeNodes to the new Master
  // so that there requested number of SlaveGroups can be created.
  for i in 0..(num_paxos_nodes * num_slave_groups) {
    let eid = mk_node_eid(num_paxos_nodes + i);
    sim.register_free_node(&eid, FreeNodeType::NewSlaveFreeNode);
  }

  // Simulate to start all of these SlaveGroups.
  for _ in 0..10000 {
    sim.simulate1ms();
    if sim.full_db_schema().slave_address_config.len() as u32 == num_slave_groups {
      break;
    }
  }

  // Assert that the above managed to create the Slaves and have the Master know about it.
  assert_eq!(sim.full_db_schema().slave_address_config.len() as u32, num_slave_groups);

  // Next, we need to register as many nodes as necessary as FreeNodes to the new Master
  // so that there requested number of SlaveGroups can be created.
  for i in 0..num_reconfig_free_nodes {
    let eid = mk_node_eid(num_paxos_nodes * (1 + num_slave_groups) + i);
    sim.register_free_node(&eid, FreeNodeType::ReconfigFreeNode);
  }

  sim
}
