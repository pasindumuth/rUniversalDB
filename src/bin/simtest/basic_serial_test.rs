use crate::serial_test_utils::{
  populate_inventory_table_basic, populate_setup_user_table_basic, setup, setup_inventory_table,
  setup_user_table,
};
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
  alter_table();
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
  populate_inventory_table_basic(&mut sim, &mut context);

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
  setup_inventory_table(&mut sim, &mut context);

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

  setup_user_table(&mut sim, &mut context);

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
  populate_inventory_table_basic(&mut sim, &mut context);
  setup_user_table(&mut sim, &mut context);
  populate_setup_user_table_basic(&mut sim, &mut context);

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
  populate_inventory_table_basic(&mut sim, &mut context);
  setup_user_table(&mut sim, &mut context);
  populate_setup_user_table_basic(&mut sim, &mut context);

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
//  insert_test
// -----------------------------------------------------------------------------------------------

fn insert_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut context);

  // Fully Insert with NULL

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), None]);
    context.send_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (0, 'my_email_0', 15),
               (1, 'my_email_1', NULL);
      ",
      100,
      exp_result,
    );
  }

  // Partial Insert with NULL

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2"))]);
    exp_result.add_row(vec![Some(cvi(3)), None]);
    context.send_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email)
        VALUES (2, 'my_email_2'),
               (3, NULL);
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'insert_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  multi_key_test
// -----------------------------------------------------------------------------------------------

fn multi_key_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables

  {
    context.send_ddl_query(
      &mut sim,
      " CREATE TABLE table1 (
          k1 INT PRIMARY KEY,
          k2 INT PRIMARY KEY,
          v1 INT,
          v2 INT
        );
      ",
      100,
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
    context.send_query(
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
      100,
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
    context.send_query(
      &mut sim,
      " SELECT k1, k2, v1
        FROM table1
        WHERE 0 <= k1 AND k1 <= 1 AND 1 <= k2 AND k2 <= 2;
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'multi_key_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  multi_stage_test
// -----------------------------------------------------------------------------------------------

fn multi_stage_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut context);
  populate_inventory_table_basic(&mut sim, &mut context);
  setup_user_table(&mut sim, &mut context);
  populate_setup_user_table_basic(&mut sim, &mut context);

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

// -----------------------------------------------------------------------------------------------
//  aggregation_test
// -----------------------------------------------------------------------------------------------

fn aggregation_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut context);
  populate_inventory_table_basic(&mut sim, &mut context);

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(3)), Some(cvs("my_email_3")), None]);
    context.send_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (2, 'my_email_2', 25),
               (3, 'my_email_3', NULL);
      ",
      100,
      exp_result,
    );
  }

  // Test basic Aggregates

  {
    let mut exp_result = TableView::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(65))]);
    context.send_query(
      &mut sim,
      " SELECT SUM(count)
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(3))]);
    context.send_query(
      &mut sim,
      " SELECT COUNT(count)
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  // Test inner DISTINCT

  {
    let mut exp_result = TableView::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(40))]);
    context.send_query(
      &mut sim,
      " SELECT SUM(DISTINCT count)
        FROM inventory;
      ",
      100,
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
    context.send_query(
      &mut sim,
      " SELECT count
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("count")]);
    exp_result.add_row(vec![Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(25))]);
    exp_result.add_row(vec![None]);
    context.send_query(
      &mut sim,
      " SELECT DISTINCT count
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'aggregation_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  aliased_column_resolution_test
// -----------------------------------------------------------------------------------------------

fn aliased_column_resolution_test() {
  let (mut sim, mut context) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut context);

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(25))]);
    exp_result.add_row(vec![Some(cvi(3)), Some(cvs("my_email_3")), None]);
    context.send_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (0, 'my_email_0', 15),
               (1, 'my_email_1', 25),
               (2, 'my_email_2', 25),
               (3, 'my_email_3', NULL);
      ",
      100,
      exp_result,
    );
  }

  // Basic column shadowing test

  {
    let mut exp_result = TableView::new(vec![cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(2))]);
    context.send_query(
      &mut sim,
      " SELECT product_id
        FROM inventory AS outer
        WHERE count = (
           SELECT count
           FROM inventory AS inner
           WHERE inner.email = outer.email);
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(2))]);
    exp_result.add_row(vec![Some(cvi(3))]);
    context.send_query(
      &mut sim,
      " SELECT product_id
        FROM inventory AS outer
        WHERE email = (
           SELECT email
           FROM inventory AS inner
           WHERE inner.email = outer.email);
      ",
      100,
      exp_result,
    );
  }

  // Qualified column with unqualified table
  {
    let mut exp_result = TableView::new(vec![cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(2))]);
    context.send_query(
      &mut sim,
      " SELECT product_id
        FROM inventory AS outer
        WHERE count = (
           SELECT count
           FROM inventory
           WHERE inventory.email = outer.email);
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'aliased_column_resolution_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}

// -----------------------------------------------------------------------------------------------
//  alter_table
// -----------------------------------------------------------------------------------------------

fn alter_table() {
  let (mut sim, mut context) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut context);
  populate_inventory_table_basic(&mut sim, &mut context);

  {
    context.send_ddl_query(
      &mut sim,
      " ALTER TABLE inventory
        ADD COLUMN price INT;
      ",
      100,
    );
  }

  // Add Column and Write to it

  {
    let mut exp_result =
      TableView::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15)), None]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25)), None]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email, count, price
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("price")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(100))]);
    context.send_query(
      &mut sim,
      " UPDATE inventory
        SET price = 100
        WHERE product_id = 0;
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result =
      TableView::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35)), Some(cvi(200))]);
    context.send_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count, price)
        VALUES (2, 'my_email_2', 35, 200);
      ",
      100,
      exp_result,
    );
  }

  {
    let mut exp_result =
      TableView::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvs("my_email_0")), Some(cvi(15)), Some(cvi(100))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), Some(cvi(25)), None]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(35)), Some(cvi(200))]);
    context.send_query(
      &mut sim,
      " SELECT product_id, email, count, price
        FROM inventory;
      ",
      100,
      exp_result,
    );
  }

  println!("Test 'alter_table' Passed! Time taken: {:?}ms", sim.true_timestamp())
}
