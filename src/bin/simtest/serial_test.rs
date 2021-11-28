use crate::simulation::Simulation;
use runiversal::common::TableSchema;
use runiversal::model::common::{
  ColName, ColType, EndpointId, Gen, PrimaryKey, RequestId, SlaveGroupId, TablePath, TableView,
  TabletGroupId, TabletKeyRange,
};
use runiversal::model::message as msg;
use runiversal::simulation_utils::{mk_client_eid, mk_slave_eid};
use runiversal::test_utils::{cno, cvi, cvs, mk_eid, mk_sid, mk_tab, mk_tid};
use std::collections::BTreeMap;

/**
 * This suite of tests consists of simple serial Transaction Processing.
 * Only one query executes at a time.
 */

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

pub struct TestContext {
  next_request_idx: u32,
  /// The client that we always use.
  sender_eid: EndpointId,
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

  /// Executes the `query` using `sim` with a time limit of `time_limit`. If the query
  /// finishes, we check that it succeeded and that the resulting `TableView` is the same
  /// as `expr_result`.
  pub fn send_query(
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

pub fn setup() -> (Simulation, TestContext) {
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

  let sim = Simulation::new([0; 16], 1, slave_address_config, master_address_config);
  let context = TestContext::new();
  (sim, context)
}

fn setup_inventory_table(sim: &mut Simulation, context: &mut TestContext) {
  {
    context.send_ddl_query(
      sim,
      " CREATE TABLE inventory (
          product_id INT PRIMARY KEY,
          email      VARCHAR,
          count      INT
        );
      ",
      100,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    context.send_query(
      sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (0, 'my_email_0', 15),
               (1, 'my_email_1', 25);
      ",
      100,
      exp_result,
    );
  }
}

fn setup_user_table(sim: &mut Simulation, context: &mut TestContext) {
  {
    context.send_ddl_query(
      sim,
      " CREATE TABLE user (
          email      VARCHAR PRIMARY KEY,
          balance    INT,
        );
      ",
      100,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("email"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(60))]);
    exp_result.add_row(vec![Some(cvs("my_email_2")), Some(cvi(70))]);
    context.send_query(
      sim,
      " INSERT INTO user (email, balance)
        VALUES ('my_email_0', 50),
               ('my_email_1', 60),
               ('my_email_2', 70);
      ",
      100,
      exp_result,
    );
  }
}

// -----------------------------------------------------------------------------------------------
//  test_all_serial
// -----------------------------------------------------------------------------------------------

pub fn test_all_serial() {
  simple_test();
  subquery_test();
  trans_table_test();
  select_projection_test();
  multi_stage_test();
}

// -----------------------------------------------------------------------------------------------
//  simple_test
// -----------------------------------------------------------------------------------------------

/// This is a test that solely tests Transaction Processing. We take all PaxosGroups to just
/// have one node. We only check for SQL semantics compatibility.
fn simple_test() {
  let (mut sim, mut context) = setup();

  // Test Basic Queries
  setup_inventory_table(&mut sim, &mut context);

  // Test Simple Update-Select

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1"))]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
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
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_3"))]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  // Test Simple Multi-Stage Transactions

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_5"))]);
    context.send_query(
      &mut sim,
      " UPDATE inventory
        SET email = 'my_email_4'
        WHERE product_id = 0;

        UPDATE inventory
        SET email = 'my_email_5'
        WHERE product_id = 1;
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_4"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_5"))]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  // Test NULL data

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(6)), Some(cvs("my_email_6"))]);
    context.send_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email)
        VALUES (6, 'my_email_6');
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(6)), Some(cvs("my_email_6")), None]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory
        WHERE product_id = 6
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_4")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_5")), Some(cvi(25))]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory
        WHERE count IS NOT NULL
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'simple_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  subquery_test
// -----------------------------------------------------------------------------------------------

fn subquery_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables

  {
    context.send_ddl_query(
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

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(25))]);
    context.send_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (0, 'my_email_0', 15),
               (2, 'my_email_2', 25);
      ",
      100,
      exp_result,
    );
  }

  {
    context.send_ddl_query(
      &mut sim,
      " CREATE TABLE user (
          email      VARCHAR PRIMARY KEY,
          balance    INT,
        );
      ",
      100,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("email"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(30))]);
    exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvs("my_email_2")), Some(cvi(30))]);
    context.send_query(
      &mut sim,
      " INSERT INTO user (email, balance)
        VALUES ('my_email_0', 30),
               ('my_email_1', 50),
               ('my_email_2', 30);
      ",
      100,
      exp_result,
    );
  }

  // Test Simple Subquery

  {
    let mut exp_result = TableView::new(vec![cno("balance")]);
    exp_result.add_row(vec![Some(cvi(30))]);
    context.send_query(
      &mut sim,
      " SELECT balance
        FROM user
        WHERE email = (
          SELECT email
          FROM inventory
          WHERE product_id = 0);
      ",
      100,
      exp_result,
    );
  }

  // Test Correlated Subquery

  {
    let mut exp_result = TableView::new(vec![cno("balance")]);
    exp_result.add_row(vec![Some(cvi(30))]);
    context.send_query(
      &mut sim,
      " SELECT balance
        FROM user
        WHERE email = (
          SELECT email
          FROM inventory
          WHERE count = balance / 2);
      ",
      100,
      exp_result,
    );
  }

  // Test Subquery with TransTable

  {
    let mut exp_result = TableView::new(vec![cno("balance")]);
    exp_result.add_row(vec![Some(cvi(30))]);
    context.send_query(
      &mut sim,
      " SELECT balance
        FROM user
        WHERE email = (
          WITH
            v1 AS (SELECT email, count
                   FROM inventory)
          SELECT email
          FROM v1
          WHERE count = balance / 2);
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'subquery_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  trans_table_test
// -----------------------------------------------------------------------------------------------

fn trans_table_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut context);
  setup_user_table(&mut sim, &mut context);

  // Test TransTable Reads

  {
    let mut exp_result = TableView::new(vec![cno("email")]);
    exp_result.add_row(vec![Some(cvs("my_email_1"))]);
    exp_result.add_row(vec![Some(cvs("my_email_2"))]);
    context.send_query(
      &mut sim,
      " WITH
          v1 AS (SELECT email, balance
                 FROM  user
                 WHERE balance >= 60)
        SELECT email
        FROM v1;
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'trans_table_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  select_projection_test
// -----------------------------------------------------------------------------------------------

fn select_projection_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut context);
  setup_user_table(&mut sim, &mut context);

  // Test advanced expression in the SELECT projection.

  {
    let mut exp_result = TableView::new(vec![cno("e"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(60))]);
    exp_result.add_row(vec![Some(cvs("my_email_2")), Some(cvi(70))]);
    context.send_query(
      &mut sim,
      " SELECT email AS e, balance
        FROM  user
        WHERE balance >= 60;
      ",
      100,
      exp_result,
    );

    let mut exp_result = TableView::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(120))]);
    exp_result.add_row(vec![Some(cvi(140))]);
    context.send_query(
      &mut sim,
      " SELECT balance * 2
        FROM  user
        WHERE balance >= 60;
      ",
      100,
      exp_result,
    );

    let mut exp_result = TableView::new(vec![cno("b")]);
    exp_result.add_row(vec![Some(cvi(120))]);
    exp_result.add_row(vec![Some(cvi(140))]);
    context.send_query(
      &mut sim,
      " WITH
          v1 AS (SELECT balance * 2 AS b
                 FROM  user
                 WHERE balance >= 60)
        SELECT b
        FROM v1;
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'select_projection_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  multi_stage_test
// -----------------------------------------------------------------------------------------------

fn multi_stage_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut context);
  setup_user_table(&mut sim, &mut context);

  // Multi-Stage Transactions with TransTables

  {
    let mut exp_result = TableView::new(vec![cno("email"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(80))]);
    context.send_query(
      &mut sim,
      " UPDATE user
        SET balance = balance + 20
        WHERE email = (
          SELECT email
          FROM inventory
          WHERE product_id = 1);
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(30))]);
    context.send_query(
      &mut sim,
      " UPDATE user
        SET balance = balance + 20
        WHERE email = (
          SELECT email
          FROM inventory
          WHERE product_id = 1);
  
        UPDATE inventory
        SET count = count + 5
        WHERE email = (
          SELECT email
          FROM user
          WHERE balance >= 80);
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'multi_stage_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}
