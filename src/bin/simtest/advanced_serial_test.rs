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
use runiversal::test_utils::{cno, cvi, cvs, mk_eid, mk_sid, mk_tab, mk_tid};
use std::collections::BTreeMap;

/**
 * These are more advanced serial tests. Recall that the `basic_serial_test` incrementally
 * introduces new SQL features and tests them. That inheritantly requires earlier tests to be
 * more restricted than later tests. These tests have no such restriction.
 */

// -----------------------------------------------------------------------------------------------
//  test_all_advanced_serial
// -----------------------------------------------------------------------------------------------

pub fn test_all_advanced_serial() {
  subquery_test();
  trans_table_test();
}

// -----------------------------------------------------------------------------------------------
//  subquery_test
// -----------------------------------------------------------------------------------------------

fn subquery_test() {
  let (mut sim, mut ctx) = setup();

  // Setup Tables
  setup_inventory_table(&mut sim, &mut ctx);
  populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_setup_user_table_basic(&mut sim, &mut ctx);

  // Test Multiple Subqueries

  {
    let mut exp_result = TableView::new(vec![cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(0))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id
        FROM inventory AS inv
        WHERE (
          SELECT balance * 2
          FROM user
          WHERE email = inv.email) = 50 + (
          SELECT balance
          FROM user
          WHERE email = inv.email) 
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
    ctx.execute_query(
      &mut sim,
      " WITH
          v1 AS (SELECT email, balance
                 FROM  user
                 WHERE balance >= 60),
          v2 AS (SELECT product_id, email
                 FROM  inventory
                 WHERE 0 < (
                   SELECT COUNT(email)
                   FROM v1
                   WHERE email = inventory.email))
        SELECT email
        FROM v2;
      ",
      10000,
      exp_result,
    );
  }

  // Test TransTable Reads with a Write

  {
    let mut exp_result = TableView::new(vec![cno("product_id"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(30))]);
    ctx.execute_query(
      &mut sim,
      " UPDATE user
        SET balance = balance + 20
        WHERE email = (
          WITH
            v1 AS (SELECT email, balance
                   FROM user
                   WHERE balance >= 60),
            v2 AS (SELECT product_id, email
                   FROM  inventory
                   WHERE 0 < (
                     SELECT COUNT(email)
                     FROM v1
                     WHERE email = inventory.email))
          SELECT email
          FROM v2);
  
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

  println!("Test 'trans_table_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}
