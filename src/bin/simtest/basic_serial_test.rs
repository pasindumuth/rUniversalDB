use crate::serial_test_utils::{
  populate_inventory_table_basic, populate_setup_user_table_basic, setup, setup_inventory_table,
  setup_user_table, setup_with_seed, simulate_until_response, TestContext,
};
use crate::simulation::Simulation;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::TableSchema;
use runiversal::model::common::{
  ColName, ColType, EndpointId, Gen, PaxosGroupIdTrait, PrimaryKey, RequestId, SlaveGroupId,
  TablePath, TableView, TabletGroupId, TabletKeyRange,
};
use runiversal::model::message as msg;
use runiversal::model::message::NetworkMessage;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::{mk_master_eid, mk_slave_eid};
use runiversal::test_utils::{cno, cvi, cvs, mk_eid, mk_sid, mk_tab, mk_tid};
use std::collections::BTreeMap;

/**
 * This suite of tests consists of simple serial Transaction Processing. Only one query
 * executes at a time. These are basic tests that primarly test for new SQL features.
 */

// -----------------------------------------------------------------------------------------------
//  test_all_basic_serial
// -----------------------------------------------------------------------------------------------

pub fn test_all_basic_serial() {
  simple_test();
  subquery_test();
  trans_table_test();
  select_projection_test();
  insert_test();
  multi_key_test();
  multi_stage_test();
  aggregation_test();
  aliased_column_resolution_test();
  basic_add_column();
  drop_column();
  basic_delete_test();
  insert_delete_insert_test();
  ghost_deleted_row_test();
  cancellation_test();
  paxos_leader_change_test();
}

// -----------------------------------------------------------------------------------------------
//  simple_test
// -----------------------------------------------------------------------------------------------

/// This is a test that solely tests Transaction Processing. We take all PaxosGroups to just
/// have one node. We only check for SQL semantics compatibility.
fn simple_test() {
  let (mut sim, mut ctx) = setup();

  // Test Basic Queries
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);

  // Test Simple Update-Select

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1"))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_3"))]);
    ctx.execute_query(
      &mut sim,
      " UPDATE inventory
        SET email = 'my_email_3'
        WHERE product_id = 1;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_3"))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Test Simple Multi-Stage Transactions

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_5"))]);
    ctx.execute_query(
      &mut sim,
      " UPDATE inventory
        SET email = 'my_email_4'
        WHERE product_id = 0;

        UPDATE inventory
        SET email = 'my_email_5'
        WHERE product_id = 1;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_4"))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_5"))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Test NULL data

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(6)), Some(cvs("my_email_6"))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email)
        VALUES (6, 'my_email_6');
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(6)), Some(cvs("my_email_6")), None]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory
        WHERE product_id = 6
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_4")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_5")), Some(cvi(25))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory
        WHERE count IS NOT NULL
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'simple_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  subquery_test
// -----------------------------------------------------------------------------------------------

fn subquery_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(25))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (0, 'my_email_0', 15),
               (2, 'my_email_2', 25);
      ",
      10000,
      exp_result,
    );
  }

  setup_user_table(&mut sim, &mut ctx);

  {
    let mut exp_result = TableView::new(vec![cno("email"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(30))]);
    exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvs("my_email_2")), Some(cvi(30))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO user (email, balance)
        VALUES ('my_email_0', 30),
               ('my_email_1', 50),
               ('my_email_2', 30);
      ",
      10000,
      exp_result,
    );
  }

  // Test Simple Subquery

  {
    let mut exp_result = TableView::new(vec![cno("balance")]);
    exp_result.add_row(vec![Some(cvi(30))]);
    ctx.execute_query(
      &mut sim,
      " SELECT balance
        FROM user
        WHERE email = (
          SELECT email
          FROM inventory
          WHERE product_id = 0);
      ",
      10000,
      exp_result,
    );
  }

  // Test Correlated Subquery

  {
    let mut exp_result = TableView::new(vec![cno("balance")]);
    exp_result.add_row(vec![Some(cvi(30))]);
    ctx.execute_query(
      &mut sim,
      " SELECT balance
        FROM user
        WHERE email = (
          SELECT email
          FROM inventory
          WHERE count = balance / 2);
      ",
      10000,
      exp_result,
    );
  }

  // Test Subquery with TransTable

  {
    let mut exp_result = TableView::new(vec![cno("balance")]);
    exp_result.add_row(vec![Some(cvi(30))]);
    ctx.execute_query(
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
      10000,
      exp_result,
    );
  }

  println!("Test 'subquery_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  trans_table_test
// -----------------------------------------------------------------------------------------------

fn trans_table_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_setup_user_table_basic(&mut sim, &mut ctx);

  // Test TransTable Reads

  {
    let mut exp_result = TableView::new(vec![cno("email")]);
    exp_result.add_row(vec![Some(cvs("my_email_1"))]);
    exp_result.add_row(vec![Some(cvs("my_email_2"))]);
    ctx.execute_query(
      &mut sim,
      " WITH
          v1 AS (SELECT email, balance
                 FROM  user
                 WHERE balance >= 60)
        SELECT email
        FROM v1;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'trans_table_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  select_projection_test
// -----------------------------------------------------------------------------------------------

fn select_projection_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_setup_user_table_basic(&mut sim, &mut ctx);

  // Test advanced expression in the SELECT projection.

  {
    let mut exp_result = TableView::new(vec![cno("e"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(60))]);
    exp_result.add_row(vec![Some(cvs("my_email_2")), Some(cvi(70))]);
    ctx.execute_query(
      &mut sim,
      " SELECT email AS e, balance
        FROM  user
        WHERE balance >= 60;
      ",
      10000,
      exp_result,
    );

    let mut exp_result = TableView::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(120))]);
    exp_result.add_row(vec![Some(cvi(140))]);
    ctx.execute_query(
      &mut sim,
      " SELECT balance * 2
        FROM  user
        WHERE balance >= 60;
      ",
      10000,
      exp_result,
    );

    let mut exp_result = TableView::new(vec![cno("b")]);
    exp_result.add_row(vec![Some(cvi(120))]);
    exp_result.add_row(vec![Some(cvi(140))]);
    ctx.execute_query(
      &mut sim,
      " WITH
          v1 AS (SELECT balance * 2 AS b
                 FROM  user
                 WHERE balance >= 60)
        SELECT b
        FROM v1;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'select_projection_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  insert_test
// -----------------------------------------------------------------------------------------------

fn insert_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);

  // Fully Insert with NULL

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), None]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (0, 'my_email_0', 15),
               (1, 'my_email_1', NULL);
      ",
      10000,
      exp_result,
    );
  }

  // Partial Insert with NULL

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2"))]);
    exp_result.add_row(vec![Some(cvi(3)), None]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email)
        VALUES (2, 'my_email_2'),
               (3, NULL);
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'insert_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  multi_key_test
// -----------------------------------------------------------------------------------------------

fn multi_key_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables

  {
    ctx.send_ddl_query(
      &mut sim,
      " CREATE TABLE table1 (
          k1 INT,
          k2 INT,
          v1 INT,
          v2 INT,
          PRIMARY KEY (k1, k2)
        );
      ",
      10000,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("k1"), cno("k2"), cno("v1"), cno("v2")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(0)), Some(cvi(0)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(1)), Some(cvi(0)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(2)), Some(cvi(0)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(3)), Some(cvi(0)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(0)), Some(cvi(0)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(1)), Some(cvi(0)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(2)), Some(cvi(0)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvi(0)), Some(cvi(0)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvi(1)), Some(cvi(0)), Some(cvi(0))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO table1 (k1, k2, v1, v2)
        VALUES (0, 0, 0, 0),
               (0, 1, 0, 0),
               (0, 2, 0, 0),
               (0, 3, 0, 0),
               (1, 0, 0, 0),
               (1, 1, 0, 0),
               (1, 2, 0, 0),
               (2, 0, 0, 0),
               (2, 1, 0, 0);
      ",
      10000,
      exp_result,
    );
  }

  // Range queries for multiple Key Columns

  {
    let mut exp_result = TableView::new(vec![cno("k1"), cno("k2"), cno("v1")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(1)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(2)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(1)), Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(2)), Some(cvi(0))]);
    ctx.execute_query(
      &mut sim,
      " SELECT k1, k2, v1
        FROM table1
        WHERE 0 <= k1 AND k1 <= 1 AND 1 <= k2 AND k2 <= 2;
      ",
      10000,
      exp_result,
    );
  }

  // Update using a complex WHERE clause

  {
    let mut exp_result = TableView::new(vec![cno("k1"), cno("k2"), cno("v1")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(1)), Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(2)), Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(1)), Some(cvi(2))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(2)), Some(cvi(2))]);
    ctx.execute_query(
      &mut sim,
      " UPDATE table1
        SET v1 = k1 + 1
        WHERE 0 <= k1 AND k1 <= 1 AND 1 <= k2 AND k2 <= 2;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("k1"), cno("k2"), cno("v1")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(1)), Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(2)), Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(1)), Some(cvi(2))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(2)), Some(cvi(2))]);
    ctx.execute_query(
      &mut sim,
      " SELECT k1, k2, v1
        FROM table1
        WHERE 0 <= k1 AND k1 <= 1 AND 1 <= k2 AND k2 <= 2;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'multi_key_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  multi_stage_test
// -----------------------------------------------------------------------------------------------

fn multi_stage_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_setup_user_table_basic(&mut sim, &mut ctx);

  // Multi-Stage Transactions with TransTables

  {
    let mut exp_result = TableView::new(vec![cno("email"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(80))]);
    ctx.execute_query(
      &mut sim,
      " UPDATE user
        SET balance = balance + 20
        WHERE email = (
          SELECT email
          FROM inventory
          WHERE product_id = 1);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(30))]);
    ctx.execute_query(
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
      10000,
      exp_result,
    );
  }

  println!("Test 'multi_stage_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  aggregation_test
// -----------------------------------------------------------------------------------------------

fn aggregation_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(3)), Some(cvs("my_email_3")), None]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (2, 'my_email_2', 25),
               (3, 'my_email_3', NULL);
      ",
      10000,
      exp_result,
    );
  }

  // Test basic Aggregates

  {
    let mut exp_result = TableView::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(65))]);
    ctx.execute_query(
      &mut sim,
      " SELECT SUM(count)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(3))]);
    ctx.execute_query(
      &mut sim,
      " SELECT COUNT(count)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Test inner DISTINCT

  {
    let mut exp_result = TableView::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(40))]);
    ctx.execute_query(
      &mut sim,
      " SELECT SUM(DISTINCT count)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Test outer DISTINCT

  {
    let mut exp_result = TableView::new(vec![cno("count")]);
    exp_result.add_row(vec![Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(25))]);
    exp_result.add_row(vec![None]);
    ctx.execute_query(
      &mut sim,
      " SELECT count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("count")]);
    exp_result.add_row(vec![Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(25))]);
    exp_result.add_row(vec![None]);
    ctx.execute_query(
      &mut sim,
      " SELECT DISTINCT count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'aggregation_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  aliased_column_resolution_test
// -----------------------------------------------------------------------------------------------

fn aliased_column_resolution_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(3)), Some(cvs("my_email_3")), None]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (0, 'my_email_0', 15),
               (1, 'my_email_1', 25),
               (2, 'my_email_2', 25),
               (3, 'my_email_3', NULL);
      ",
      10000,
      exp_result,
    );
  }

  // Basic column shadowing test

  {
    let mut exp_result = TableView::new(vec![cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(2))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id
        FROM inventory AS outer
        WHERE count = (
           SELECT count
           FROM inventory AS inner
           WHERE inner.email = outer.email);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(2))]);
    exp_result.add_row(vec![Some(cvi(3))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id
        FROM inventory AS outer
        WHERE email = (
           SELECT email
           FROM inventory AS inner
           WHERE inner.email = outer.email);
      ",
      10000,
      exp_result,
    );
  }

  // Qualified column with unqualified table
  {
    let mut exp_result = TableView::new(vec![cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(2))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id
        FROM inventory AS outer
        WHERE count = (
           SELECT count
           FROM inventory
           WHERE inventory.email = outer.email);
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'aliased_column_resolution_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  basic_add_column
// -----------------------------------------------------------------------------------------------

fn basic_add_column() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);

  // Add Column and Write to it

  {
    ctx.send_ddl_query(
      &mut sim,
      " ALTER TABLE inventory
        ADD COLUMN price INT;
      ",
      10000,
    );
  }

  {
    let mut exp_result =
      TableView::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15)), None]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25)), None]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count, price
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("price")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(100))]);
    ctx.execute_query(
      &mut sim,
      " UPDATE inventory
        SET price = 100
        WHERE product_id = 0;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result =
      TableView::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35)), Some(cvi(200))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count, price)
        VALUES (2, 'my_email_2', 35, 200);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result =
      TableView::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15)), Some(cvi(100))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25)), None]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35)), Some(cvi(200))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count, price
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'basic_add_column' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  drop_column
// -----------------------------------------------------------------------------------------------

fn drop_column() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);

  // See if deleting a column and adding it back results in SELECTs now reading null
  // for that column (rather than a non-null value that was previously there).

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (2, 'my_email_2', 35);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Drop the column and add it back
  {
    ctx.send_ddl_query(
      &mut sim,
      " ALTER TABLE inventory
        DROP COLUMN count;
      ",
      10000,
    );

    ctx.send_ddl_query(
      &mut sim,
      " ALTER TABLE inventory
        ADD COLUMN count INT;
      ",
      10000,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), None]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), None]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), None]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Re-populated "count"
  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(16))]);
    ctx.execute_query(
      &mut sim,
      " UPDATE inventory
        SET count = 16
        WHERE product_id = 0;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(16))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), None]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), None]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'drop_column' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  basic_delete_test
// -----------------------------------------------------------------------------------------------

/// Sees if a single Delete Query does indeed delete data.
fn basic_delete_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (2, 'my_email_2', 35);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Delete rows with a non-trivial expression and check that they all get deleted

  {
    let mut exp_result = TableView::new(vec![]);
    ctx.execute_query(
      &mut sim,
      " DELETE
        FROM inventory
        WHERE product_id % 2 = 0;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Delete it again and see if that succeeds

  println!("Test 'basic_delete_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  insert_delete_insert_test
// -----------------------------------------------------------------------------------------------

/// Sees if a Transaction with an Insert a row, then Deletes it, and then tries Inserting
/// it again, then it all works.
fn insert_delete_insert_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (2, 'my_email_2', 35),
               (3, 'my_email_3', 45),
               (4, 'my_email_4', 55);
  
        DELETE
        FROM inventory
        WHERE product_id % 2 = 0;
  
        INSERT INTO inventory (product_id, email, count)
        VALUES (2, 'my_email_2', 35);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35))]);
    exp_result.add_row(vec![Some(cvi(3)), Some(cvs("my_email_3")), Some(cvi(45))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'insert_delete_insert_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  ghost_deleted_row_test
// -----------------------------------------------------------------------------------------------

/// Sees if a deleted row is re-inserted with some columns unspecified, they start off as
/// NULL, instead of their prior value due to the delete.
fn ghost_deleted_row_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![]);
    ctx.execute_query(
      &mut sim,
      " DELETE
        FROM inventory
        WHERE product_id = 0;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0"))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email)
        VALUES (0, 'my_email_0');
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), None]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'ghost_deleted_row_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  cancellation_test
// -----------------------------------------------------------------------------------------------

fn cancellation_test() {
  let mut rand = XorShiftRng::from_seed([0; 16]);
  let mut test_time_taken = 0;

  // We repeat this loop to get a good balance between successful
  // and unsuccessful cancellations.
  let mut total_count = 0;
  let mut cancelled_count = 0;
  while total_count < 10 && cancelled_count < 4 && total_count - cancelled_count < 4 {
    let mut seed = [0; 16];
    rand.fill_bytes(&mut seed);
    let (mut sim, mut ctx) = setup_with_seed(seed);

    // Setup Tables
    setup_inventory_table(&mut sim, &mut ctx);
    populate_inventory_table_basic(&mut sim, &mut ctx);
    setup_user_table(&mut sim, &mut ctx);
    populate_setup_user_table_basic(&mut sim, &mut ctx);

    // Send the query and simulate
    let query = "
      INSERT INTO inventory (product_id, email, count)
      VALUES (2, 'my_email_2', 35);

      UPDATE user
      SET balance = balance + 2 * (
        SELECT sum(count)
        FROM inventory AS inv
        WHERE inv.email = user.email)
      WHERE email = 'my_email_1' OR email = 'my_email_2';

      DELETE
      FROM inventory
      WHERE product_id = 2;
    ";

    // This is called when `payload` is expected to be the response to the above query.
    fn check_success(
      sim: &mut Simulation,
      ctx: &mut TestContext,
      payload: &msg::ExternalQuerySuccess,
      request_id: RequestId,
    ) {
      assert_eq!(payload.request_id, request_id);

      // Check that the TableView in the response is what we expect.
      let mut exp_result = TableView::new(vec![]);
      assert_eq!(payload.result, exp_result);

      // Check that final data in the system is what we expect.
      {
        let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
        exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
        exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
        ctx.execute_query(
          sim,
          " SELECT product_id, email, count
            FROM inventory;
          ",
          10000,
          exp_result,
        );
      }

      {
        let mut exp_result = TableView::new(vec![cno("email"), cno("balance")]);
        exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(50))]);
        exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(110))]);
        exp_result.add_row(vec![Some(cvs("my_email_2")), Some(cvi(140))]);
        ctx.execute_query(
          sim,
          " SELECT email, balance
            FROM user;
          ",
          10000,
          exp_result,
        );
      }
    }

    let request_id = ctx.send_query(&mut sim, query);

    // Simulate. If we respond early, then check that the output is what we expect.
    let mut cancel_succeeded = false;
    if simulate_until_response(&mut sim, &ctx.sender_eid, 15) {
      let response = sim.get_responses(&ctx.sender_eid).iter().last().unwrap();
      match response.clone() {
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
          check_success(&mut sim, &mut ctx, &payload, request_id.clone());
        }
        _ => panic!(),
      }
    } else {
      // Otherwise, send a cancellation and simulate until the end.
      ctx.send_cancellation(&mut sim, request_id.clone());
      assert!(simulate_until_response(&mut sim, &ctx.sender_eid, 10000));
      let response = sim.get_responses(&ctx.sender_eid).iter().last().unwrap();
      match response.clone() {
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
          // Here, the original query responded before the cancellation could.
          check_success(&mut sim, &mut ctx, &payload, request_id.clone());
        }
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(payload)) => {
          assert_eq!(payload.request_id, request_id.clone());
          if payload.payload == msg::ExternalAbortedData::CancelDenied {
            // In case the cancellation failed, finish the original query, expecting success.
            assert!(simulate_until_response(&mut sim, &ctx.sender_eid, 10000));
            let response = sim.get_responses(&ctx.sender_eid).iter().last().unwrap();
            match response.clone() {
              msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(
                payload,
              )) => {
                check_success(&mut sim, &mut ctx, &payload, request_id.clone());
              }
              _ => panic!(),
            }
          } else {
            // Otherwise, mark the cancellation as successful.
            assert_eq!(payload.payload, msg::ExternalAbortedData::CancelConfirmed);
            cancel_succeeded = true;
          }
        }
        _ => panic!("Incorrect Response: {:#?}", response),
      }
    }

    // Simulate more for a cooldown time and verify that all resources get cleaned up.
    const TP_COOLDOWN_MS: u32 = 500;
    let mut duration = 0;
    while duration < TP_COOLDOWN_MS {
      sim.simulate1ms();
      duration += 1;
    }
    sim.check_resources_clean();

    // If the Query had been successfull cancelled, we verify that the data in the system
    // is what we exact. We do this after cooldown to know that this is the stead state.
    if cancel_succeeded {
      {
        let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
        exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
        exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
        ctx.execute_query(
          &mut sim,
          " SELECT product_id, email, count
            FROM inventory;
          ",
          10000,
          exp_result,
        );
      }

      {
        let mut exp_result = TableView::new(vec![cno("email"), cno("balance")]);
        exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(50))]);
        exp_result.add_row(vec![Some(cvs("my_email_1")), Some(cvi(60))]);
        exp_result.add_row(vec![Some(cvs("my_email_2")), Some(cvi(70))]);
        ctx.execute_query(
          &mut sim,
          " SELECT email, balance
            FROM user;
          ",
          10000,
          exp_result,
        );
      }

      cancelled_count += 1;
    }

    // Track the test execution time.
    test_time_taken += sim.true_timestamp();
    total_count += 1;
  }

  // Check that we encoutered healthy balance of cancelled and non-cancelled. (If not, we
  // should tune the above such that we do).
  if cancelled_count < 4 && total_count - cancelled_count < 4 {
    panic!();
  }

  println!("Test 'cancellation_test' Passed! Time taken: {:?}ms", test_time_taken)
}

// -----------------------------------------------------------------------------------------------
//  paxos_leader_change_test
// -----------------------------------------------------------------------------------------------

fn paxos_leader_change_test() {
  // Create one Slave Paxos Group to test Leader change logic with.
  const NUM_PAXOS_GROUPS: u32 = 1;
  const NUM_PAXOS_NODES: u32 = 5;
  let mut master_address_config = Vec::<EndpointId>::new();
  for i in 0..NUM_PAXOS_NODES {
    master_address_config.push(mk_master_eid(i));
  }
  let mut slave_address_config = BTreeMap::<SlaveGroupId, Vec<EndpointId>>::new();
  for i in 0..NUM_PAXOS_GROUPS {
    let mut eids = Vec::<EndpointId>::new();
    for j in 0..NUM_PAXOS_NODES {
      eids.push(mk_slave_eid(i * NUM_PAXOS_NODES + j));
    }
    slave_address_config.insert(SlaveGroupId(format!("s{}", i)), eids);
  }

  let mut sim = Simulation::new(
    [0; 16],
    1,
    slave_address_config.clone(),
    master_address_config.clone(),
    PaxosConfig::test(),
  );

  // Warmup the simulation
  sim.simulate_n_ms(100);

  // Block the current leader of the SlaveGroup
  let (sid, _) = slave_address_config.first_key_value().unwrap();
  let lid = sim.leader_map.get(&sid.to_gid()).unwrap().clone();
  sim.blocked_leadership = Some(lid.clone());

  // Simulate until the Leadership changes
  let mut leader_did_change = false;
  for _ in 0..1000 {
    sim.simulate1ms();
    let cur_lid = sim.leader_map.get(&sid.to_gid()).unwrap().clone();
    if cur_lid.gen > lid.gen {
      leader_did_change = true;
      sim.blocked_leadership = None;
      break;
    }
  }

  if leader_did_change {
    println!("Test 'paxos_leader_change_test' Passed! Time taken: {:?}ms", sim.true_timestamp());
  } else {
    panic!();
  }
}
