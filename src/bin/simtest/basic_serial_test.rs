use crate::serial_test_utils::{
  deprecated_populate_inventory_table_basic, deprecated_setup_inventory_table, mk_general_sim,
  populate_product_stock_table_basic, populate_user_table_basic, setup, setup_product_stock_table,
  setup_user_table, simulate_until_clean, TestContext,
};
use crate::simulation::Simulation;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{mk_t, remove_item, TableSchema, Timestamp};
use runiversal::common::{
  ColName, ColType, EndpointId, Gen, LeadershipId, PaxosGroupIdTrait, PrimaryKey, QueryResult,
  RequestId, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange,
};
use runiversal::message as msg;
use runiversal::message::ExternalQueryError;
use runiversal::paxos::PaxosConfig;
use runiversal::test_utils::{cno, cvi, cvs, mk_seed, mk_sid, mk_tab, mk_tid};
use std::collections::BTreeMap;

/**
 * This suite of tests consists of simple serial Transaction Processing. Only one query
 * executes at a time. These are basic tests that primarly test for new SQL features.
 */

// -----------------------------------------------------------------------------------------------
//  test_all_basic_serial
// -----------------------------------------------------------------------------------------------

pub fn test_all_basic_serial(rand: &mut XorShiftRng) {
  simple_test(mk_seed(rand));
  subquery_test(mk_seed(rand));
  advanced_subquery_test(mk_seed(rand));
  trans_table_test(mk_seed(rand));
  advanced_trans_table_test(mk_seed(rand));
  select_projection_test(mk_seed(rand));
  insert_test(mk_seed(rand));
  update_test(mk_seed(rand));
  multi_key_test(mk_seed(rand));
  multi_stage_test(mk_seed(rand));
  aggregation_test(mk_seed(rand));
  avg_aggregation_test(mk_seed(rand));
  aliased_column_resolution_test(mk_seed(rand));
  basic_add_column(mk_seed(rand));
  drop_column(mk_seed(rand));
  basic_delete_test(mk_seed(rand));
  insert_delete_insert_test(mk_seed(rand));
  ghost_deleted_row_test(mk_seed(rand));
  drop_table_test(mk_seed(rand));
  simple_join_test(mk_seed(rand));
  advanced_join_test(mk_seed(rand));
  join_errors_test(mk_seed(rand));
  cancellation_test(mk_seed(rand));
  paxos_leader_change_test(mk_seed(rand));
  paxos_basic_serial_test(mk_seed(rand));
}

// -----------------------------------------------------------------------------------------------
//  simple_test
// -----------------------------------------------------------------------------------------------

/// This is a test that solely tests Transaction Processing. We take all PaxosGroups to just
/// have one node. We only check for SQL semantics compatibility.
fn simple_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Test Basic Queries
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

  // Test Simple Update-Select

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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

  println!("Test 'simple_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  subquery_test
// -----------------------------------------------------------------------------------------------

fn subquery_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("email"), cno("balance")]);
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
    let mut exp_result = QueryResult::new(vec![cno("balance")]);
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
    let mut exp_result = QueryResult::new(vec![cno("balance")]);
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
    let mut exp_result = QueryResult::new(vec![cno("balance")]);
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

  println!("Test 'subquery_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

fn advanced_subquery_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);

  // Test Multiple Subqueries

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id")]);
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

  println!("Test 'advanced_subquery_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  trans_table_test
// -----------------------------------------------------------------------------------------------

fn trans_table_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);

  // Test TransTable Reads

  {
    let mut exp_result = QueryResult::new(vec![cno("email")]);
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

  println!("Test 'trans_table_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

fn advanced_trans_table_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);

  // Test TransTable Reads

  {
    let mut exp_result = QueryResult::new(vec![cno("email")]);
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

  // Test TransTable Reads with nested CTEs. Of course, we can use CTEs introduced
  // prior in the outer query from with the CTEs in an inner query.

  {
    let mut exp_result = QueryResult::new(vec![cno("email")]);
    exp_result.add_row(vec![Some(cvs("my_email_1"))]);
    ctx.execute_query(
      &mut sim,
      " WITH
          v1 AS (SELECT email, balance
                 FROM  user
                 WHERE balance >= 60),
          v2 AS (WITH
                   v2 AS (SELECT product_id, email
                          FROM  inventory
                          WHERE 0 < (
                            SELECT COUNT(email)
                            FROM v1
                            WHERE email = inventory.email))
                 SELECT email
                 FROM v2)
        SELECT email
        FROM v2;
      ",
      10000,
      exp_result,
    );
  }

  // Test TransTable Reads with a Write

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count")]);
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

  println!(
    "Test 'advanced_trans_table_test' Passed! Time taken: {:?}ms",
    sim.true_timestamp().time_ms
  )
}

// -----------------------------------------------------------------------------------------------
//  select_projection_test
// -----------------------------------------------------------------------------------------------

fn select_projection_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);

  // Test advanced expression in the SELECT projection.

  {
    let mut exp_result = QueryResult::new(vec![cno("e"), cno("balance")]);
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

    let mut exp_result = QueryResult::new(vec![None]);
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

    let mut exp_result = QueryResult::new(vec![cno("b")]);
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

    // SELECT * tests

    let mut exp_result = QueryResult::new(vec![cno("email"), cno("balance")]);
    exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(50))]);
    ctx.execute_query(
      &mut sim,
      " SELECT *
        FROM user
        WHERE email = 'my_email_0';
      ",
      10000,
      exp_result,
    );

    let mut exp_result =
      QueryResult::new(vec![cno("email"), cno("balance"), None, cno("email"), cno("balance")]);
    exp_result.add_row(vec![
      Some(cvs("my_email_0")),
      Some(cvi(50)),
      Some(cvi(80)),
      Some(cvs("my_email_0")),
      Some(cvi(50)),
    ]);
    ctx.execute_query(
      &mut sim,
      " SELECT *, (SELECT SUM(count) FROM inventory) * 2, *
        FROM user
        WHERE email = 'my_email_0';
      ",
      10000,
      exp_result,
    );

    // Tests that SELECT * will read anonymous columns from a CTE properly.
    let mut exp_result = QueryResult::new(vec![cno("e"), None]);
    exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(100))]);
    ctx.execute_query(
      &mut sim,
      " WITH
          v1 AS (SELECT email AS e, balance * 2
                 FROM  user
                 WHERE email = 'my_email_0')
        SELECT *
        FROM v1;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'select_projection_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  insert_test
// -----------------------------------------------------------------------------------------------

fn insert_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);

  // Fully Insert with NULL

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
    exp_result.add_row(vec![Some(cvi(-1)), Some(cvs("my_email_2"))]);
    exp_result.add_row(vec![Some(cvi(3)), None]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email)
        VALUES (-1, 'my_email_2'),
               (3, NULL);
      ",
      10000,
      exp_result,
    );
  }

  // Failures cases

  {
    // Duplicate column failure
    ctx.execute_query_failure(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count, count)
        VALUES (2, 'my_email_2', 35);
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidInsert) => {
          true
        }
        _ => false,
      },
    );
  }

  {
    // Incomplete Keycols
    ctx.execute_query_failure(
      &mut sim,
      " INSERT INTO inventory (email, count)
        VALUES ('my_email_2', 35);
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidInsert) => {
          true
        }
        _ => false,
      },
    );
  }

  println!("Test 'insert_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  update_test
// -----------------------------------------------------------------------------------------------

fn update_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);

  // Failures cases

  {
    // Duplicate column failure
    ctx.execute_query_failure(
      &mut sim,
      " UPDATE inventory
        SET email = 'my_email_2', email = 'my_email_3';
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidUpdate) => {
          true
        }
        _ => false,
      },
    );
  }

  {
    // Writing to KeyCol
    ctx.execute_query_failure(
      &mut sim,
      " UPDATE inventory
        SET product_id = 1;
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidUpdate) => {
          true
        }
        _ => false,
      },
    );
  }

  println!("Test 'update_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  multi_key_test
// -----------------------------------------------------------------------------------------------

fn multi_key_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

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
    let mut exp_result = QueryResult::new(vec![cno("k1"), cno("k2"), cno("v1"), cno("v2")]);
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
    let mut exp_result = QueryResult::new(vec![cno("k1"), cno("k2"), cno("v1")]);
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
    let mut exp_result = QueryResult::new(vec![cno("k1"), cno("k2"), cno("v1")]);
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
    let mut exp_result = QueryResult::new(vec![cno("k1"), cno("k2"), cno("v1")]);
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

  println!("Test 'multi_key_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  multi_stage_test
// -----------------------------------------------------------------------------------------------

fn multi_stage_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);

  // Multi-Stage Transactions with TransTables

  {
    let mut exp_result = QueryResult::new(vec![cno("email"), cno("balance")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count")]);
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

  println!("Test 'multi_stage_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  aggregation_test
// -----------------------------------------------------------------------------------------------

fn aggregation_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![None]);
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
    let mut exp_result = QueryResult::new(vec![None]);
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
    let mut exp_result = QueryResult::new(vec![None]);
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
    let mut exp_result = QueryResult::new(vec![cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("count")]);
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

  // Test all NULL column SUM

  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![None]);
    ctx.execute_query(
      &mut sim,
      " SELECT SUM(count)
        FROM inventory
        WHERE product_id = 3;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'aggregation_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

fn avg_aggregation_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);

  // Test empty table
  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![None]);
    ctx.execute_query(
      &mut sim,
      " SELECT AVG(count)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Test all NULL column
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_1")), None]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (1, 'my_email_1', NULL);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![None]);
    ctx.execute_query(
      &mut sim,
      " SELECT AVG(count)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  // Test nominal column
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
    exp_result.add_row(vec![Some(cvi(2)), Some(cvs("my_email_2")), Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(3)), Some(cvs("my_email_3")), Some(cvi(6))]);
    ctx.execute_query(
      &mut sim,
      " INSERT INTO inventory (product_id, email, count)
        VALUES (2, 'my_email_2', 1),
               (3, 'my_email_3', 6);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(3))]);
    ctx.execute_query(
      &mut sim,
      " SELECT AVG(count)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'avg_aggregation_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  aliased_column_resolution_test
// -----------------------------------------------------------------------------------------------

fn aliased_column_resolution_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id")]);
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

  println!(
    "Test 'aliased_column_resolution_test' Passed! Time taken: {:?}ms",
    sim.true_timestamp().time_ms
  )
}

// -----------------------------------------------------------------------------------------------
//  basic_add_column
// -----------------------------------------------------------------------------------------------

fn basic_add_column(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

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
      QueryResult::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("price")]);
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
      QueryResult::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
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
      QueryResult::new(vec![cno("product_id"), cno("email"), cno("count"), cno("price")]);
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

  println!("Test 'basic_add_column' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  drop_column
// -----------------------------------------------------------------------------------------------

fn drop_column(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

  // See if deleting a column and adding it back results in SELECTs now reading null
  // for that column (rather than a non-null value that was previously there).

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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

  // Drop the column
  {
    ctx.send_ddl_query(
      &mut sim,
      " ALTER TABLE inventory
        DROP COLUMN count;
      ",
      10000,
    );
  }

  // Ensure we get an error if we try using it
  {
    ctx.execute_query_failure(
      &mut sim,
      " SELECT product_id, email, count
        FROM inventory;
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::NonExistentColumn(_),
        ) => true,
        _ => false,
      },
    );
  }

  // Add it back
  {
    ctx.send_ddl_query(
      &mut sim,
      " ALTER TABLE inventory
        ADD COLUMN count INT;
      ",
      10000,
    );
  }

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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

  println!("Test 'drop_column' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  basic_delete_test
// -----------------------------------------------------------------------------------------------

/// Sees if a single Delete Query does indeed delete data.
fn basic_delete_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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

  println!("Test 'basic_delete_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  insert_delete_insert_test
// -----------------------------------------------------------------------------------------------

/// Sees if a Transaction with an Insert a row, then Deletes it, and then tries Inserting
/// it again, then it all works.
fn insert_delete_insert_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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

  println!(
    "Test 'insert_delete_insert_test' Passed! Time taken: {:?}ms",
    sim.true_timestamp().time_ms
  )
}

// -----------------------------------------------------------------------------------------------
//  ghost_deleted_row_test
// -----------------------------------------------------------------------------------------------

/// Sees if a deleted row is re-inserted with some columns unspecified, they start off as
/// NULL, instead of their prior value due to the delete.
fn ghost_deleted_row_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
    let mut exp_result = QueryResult::new(vec![]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
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
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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

  println!("Test 'ghost_deleted_row_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  drop_table
// -----------------------------------------------------------------------------------------------

fn drop_table_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Create a Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(2))]);
    ctx.execute_query(
      &mut sim,
      " SELECT count(product_id)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(3))]);
    ctx.execute_query(
      &mut sim,
      " SELECT count(email)
        FROM user;
      ",
      10000,
      exp_result,
    );
  }

  // Drop 'inventory'

  {
    ctx.send_ddl_query(
      &mut sim,
      " DROP TABLE inventory;
      ",
      10000,
    );
  }

  {
    ctx.execute_query_failure(
      &mut sim,
      " SELECT count(product_id)
        FROM inventory;
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::TablesDNE(_)) => true,
        _ => false,
      },
    );
  }

  // Create 'inventory' again and verify it is empty
  deprecated_setup_inventory_table(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(0))]);
    ctx.execute_query(
      &mut sim,
      " SELECT count(product_id)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(3))]);
    ctx.execute_query(
      &mut sim,
      " SELECT count(email)
        FROM user;
      ",
      10000,
      exp_result,
    );
  }

  // Add data to 'inventory'

  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

  {
    let mut exp_result = QueryResult::new(vec![None]);
    exp_result.add_row(vec![Some(cvi(2))]);
    ctx.execute_query(
      &mut sim,
      " SELECT count(product_id)
        FROM inventory;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'drop_table_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  Joins
// -----------------------------------------------------------------------------------------------

fn simple_join_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);

  // Select one column from one side of a JOIN.
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(0))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(1))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id
        FROM inventory JOIN user ON true
      ",
      10000,
      exp_result,
    );
  }

  // Select with aliases and a simple ON clause. Recall that INNER JOIN is the same as JOIN.
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("balance")]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(70))]);
    ctx.execute_query(
      &mut sim,
      " SELECT I.product_id, balance
        FROM inventory AS I INNER JOIN user AS U ON U.balance = 70 AND I.product_id = 1;
      ",
      10000,
      exp_result,
    );
  }

  // Select with a dependency between the two sides of a Join.
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count"), cno("balance")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(15)), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(25)), Some(cvi(60))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, count, balance
        FROM inventory AS I JOIN user AS U ON I.email = U.email;
      ",
      10000,
      exp_result,
    );
  }

  // Select with a LEFT JOIN
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count"), cno("balance")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(15)), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(15)), Some(cvi(60))]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(15)), Some(cvi(70))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(25)), None]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, count, balance
        FROM inventory AS I LEFT JOIN user AS U ON I.email = 'my_email_0';
      ",
      10000,
      exp_result,
    );
  }

  // Select with a RIGHT JOIN
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count"), cno("balance")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(15)), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(25)), Some(cvi(50))]);
    exp_result.add_row(vec![None, None, Some(cvi(60))]);
    exp_result.add_row(vec![None, None, Some(cvi(70))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, count, balance
        FROM inventory AS I RIGHT JOIN user AS U ON U.email = 'my_email_0';
      ",
      10000,
      exp_result,
    );
  }

  // Select with a RIGHT JOIN and WHERE clause. We make sure that the WHERE clause
  // is clearly executed after row re-addition.
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count"), cno("balance")]);
    exp_result.add_row(vec![None, None, Some(cvi(60))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, count, balance
        FROM inventory AS I RIGHT JOIN user AS U ON U.email = 'my_email_0'
        WHERE U.email = 'my_email_1';
      ",
      10000,
      exp_result,
    );
  }

  // Select with a OUTER JOIN
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count"), cno("balance")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(15)), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(25)), None]);
    exp_result.add_row(vec![None, None, Some(cvi(60))]);
    exp_result.add_row(vec![None, None, Some(cvi(70))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, count, balance
        FROM inventory AS I FULL OUTER JOIN user AS U
          ON U.email = 'my_email_0' AND I.email = 'my_email_0';
      ",
      10000,
      exp_result,
    );
  }

  // Select with a RIGHT JOIN with an ON clause that would otherwise
  // produce a right-to-left dependency.
  {
    let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("count"), cno("balance")]);
    exp_result.add_row(vec![Some(cvi(0)), Some(cvi(15)), Some(cvi(50))]);
    exp_result.add_row(vec![Some(cvi(1)), Some(cvi(25)), Some(cvi(60))]);
    exp_result.add_row(vec![None, None, Some(cvi(70))]);
    ctx.execute_query(
      &mut sim,
      " SELECT product_id, count, balance
        FROM inventory AS I RIGHT JOIN user AS U ON I.email = U.email;
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'simple_join_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

fn advanced_join_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);
  setup_product_stock_table(&mut sim, &mut ctx);
  populate_product_stock_table_basic(&mut sim, &mut ctx);

  // Select with a derived table in the FROM clause.
  {
    let mut exp_result = QueryResult::new(vec![cno("double_count")]);
    exp_result.add_row(vec![Some(cvi(30))]);
    ctx.execute_query(
      &mut sim,
      " SELECT D.double_count
        FROM (SELECT I.count * 2 AS double_count
              FROM inventory AS I
              WHERE email = 'my_email_0') as D;
      ",
      10000,
      exp_result,
    );
  }

  // Select with a derived table on one side of a JOIN.
  {
    let mut exp_result = QueryResult::new(vec![cno("balance"), cno("double_count")]);
    exp_result.add_row(vec![Some(cvi(50)), Some(cvi(30))]);
    exp_result.add_row(vec![Some(cvi(60)), Some(cvi(30))]);
    exp_result.add_row(vec![Some(cvi(70)), Some(cvi(30))]);
    ctx.execute_query(
      &mut sim,
      " SELECT balance, double_count
        FROM user AS U JOIN (SELECT I.count * 2 AS double_count
                             FROM inventory AS I
                             WHERE email = 'my_email_0') as D;
      ",
      10000,
      exp_result,
    );
  }

  // Select with a LATERAL JOIN.
  {
    let mut exp_result = QueryResult::new(vec![cno("balance"), cno("double_count")]);
    exp_result.add_row(vec![Some(cvi(50)), Some(cvi(30))]);
    exp_result.add_row(vec![Some(cvi(60)), Some(cvi(50))]);
    ctx.execute_query(
      &mut sim,
      " SELECT balance, D.*
        FROM user AS U JOIN LATERAL (SELECT I.count * 2 AS double_count
                                     FROM inventory AS I
                                     WHERE I.email = U.email) AS D;
      ",
      10000,
      exp_result,
    );
  }

  // Select with simple triple Join.
  {
    let mut exp_result = QueryResult::new(vec![cno("count")]);
    exp_result.add_row(vec![Some(cvi(15))]);
    exp_result.add_row(vec![Some(cvi(25))]);
    ctx.execute_query(
      &mut sim,
      " SELECT I3.count
        FROM (inventory AS I1 JOIN
              inventory AS I2 ON I1.product_id = 0) JOIN
              inventory AS I3 ON I2.product_id = 1
      ",
      10000,
      exp_result,
    );
  }

  // Select with triple join with chain of dependencies (U depends in I, and I
  // depends on PS). We also have subqueries as well.
  {
    let mut exp_result = QueryResult::new(vec![cno("user_balance")]);
    exp_result.add_row(vec![Some(cvi(50))]);
    ctx.execute_query(
      &mut sim,
      " SELECT user_balance
        FROM ((SELECT email, balance as user_balance FROM user) AS U JOIN
              (SELECT * FROM inventory) AS I ON I.email = U.email) JOIN
              (SELECT * FROM product_stock) AS PS ON PS.product_id = I.product_id AND PS.id = 0;
      ",
      10000,
      exp_result,
    );
  }

  // Select with subquery in ON clause, were it remains as a weak conjunction.
  {
    let mut exp_result = QueryResult::new(vec![cno("balance"), cno("product_id")]);
    exp_result.add_row(vec![Some(cvi(60)), Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvi(70)), None]);
    ctx.execute_query(
      &mut sim,
      " SELECT balance, product_id
        FROM user AS U LEFT JOIN inventory AS I
          ON ((SELECT count(id) 
               FROM product_stock
               WHERE product_id = I.product_id) = 2)
          AND U.balance <= 60
        WHERE balance > 50;
      ",
      10000,
      exp_result,
    );
  }

  // Select with subquery in ON clause, where the subquery gets pushed down
  // as a strong conjunction.
  {
    let mut exp_result = QueryResult::new(vec![cno("email"), cno("balance"), cno("product_id")]);
    exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(50)), Some(cvi(1))]);
    exp_result.add_row(vec![Some(cvs("my_email_0")), Some(cvi(60)), Some(cvi(1))]);
    ctx.execute_query(
      &mut sim,
      " SELECT U2.email, U1.balance, product_id
        FROM user AS U2 JOIN (user AS U1 LEFT JOIN inventory AS I)
          ON ((SELECT count(id) 
               FROM product_stock
               WHERE product_id = I.product_id) = 2)
          AND U1.balance <= 60
        WHERE U2.email = 'my_email_0';
      ",
      10000,
      exp_result,
    );
  }

  println!("Test 'advanced_join_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

fn join_errors_test(seed: [u8; 16]) {
  let (mut sim, mut ctx) = setup(seed);

  // Setup Tables
  deprecated_setup_inventory_table(&mut sim, &mut ctx);
  deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
  setup_user_table(&mut sim, &mut ctx);
  populate_user_table_basic(&mut sim, &mut ctx);

  // Select with a RIGHT LATERAL JOIN
  {
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U1.balance
        FROM user AS U1 RIGHT JOIN LATERAL (SELECT * FROM user);
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::InvalidLateralJoin,
        ) => true,
        _ => false,
      },
    );
  }

  // Select with a Derived Table that does not have an alias
  {
    // Without LATERAL
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U1.balance
        FROM user AS U1 JOIN LATERAL (SELECT * FROM user);
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::NonAliasedDerivedTable,
        ) => true,
        _ => false,
      },
    );

    // With LATERAL
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U1.balance
        FROM user AS U1 JOIN (SELECT * FROM user);
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::NonAliasedDerivedTable,
        ) => true,
        _ => false,
      },
    );
  }

  // Select with two JOIN Leafs with the same alias.
  {
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U1.balance
        FROM user AS U1 JOIN user AS U1;
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::NonUniqueJoinLeafName,
        ) => true,
        _ => false,
      },
    );
  }

  // Select where the wrong name was used for the alias in a `ColumnRef`.
  {
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U1.balance
        FROM user AS U1 JOIN inventory AS I ON user.email = 'my_email_0';
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::NonExistentTableQualification,
        ) => true,
        _ => false,
      },
    );
  }

  // Select where the wildcard contains a qualification that does not exist.
  {
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U2.*
        FROM user AS U1;
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::InvalidWildcardQualification,
        ) => true,
        _ => false,
      },
    );
  }

  // Select containing a `ColumnRef` that does not exist.
  {
    // Without qualification.
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U1.balance
        FROM user AS U1 JOIN inventory AS I ON bad_column = 2;
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::NonExistentColumn(_),
        ) => true,
        _ => false,
      },
    );

    // With qualification.
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U1.balance
        FROM user AS U1 JOIN inventory AS I ON U1.bad_column = 2;
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::NonExistentColumn(_),
        ) => true,
        _ => false,
      },
    );
  }

  // Select with ambiguous column reference in ON clause
  {
    ctx.execute_query_failure(
      &mut sim,
      " SELECT U1.balance
        FROM user AS U1 JOIN inventory AS I ON email = 'my_email_0';
      ",
      10000,
      |abort_data| match abort_data {
        msg::ExternalAbortedData::QueryPlanningError(
          msg::QueryPlanningError::AmbiguousColumnRef,
        ) => true,
        _ => false,
      },
    );
  }

  println!("Test 'join_errors_test' Passed! Time taken: {:?}ms", sim.true_timestamp().time_ms)
}

// -----------------------------------------------------------------------------------------------
//  cancellation_test
// -----------------------------------------------------------------------------------------------

fn cancellation_test(seed: [u8; 16]) {
  let mut test_time_taken = mk_t(0);

  // We repeat this loop to get a good balance between successful
  // and unsuccessful cancellations.
  let mut total_count = 0;
  let mut cancelled_count = 0;
  while total_count < 10 && cancelled_count < 4 && total_count - cancelled_count < 4 {
    let (mut sim, mut ctx) = setup(seed);

    // Setup Tables
    deprecated_setup_inventory_table(&mut sim, &mut ctx);
    deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);
    setup_user_table(&mut sim, &mut ctx);
    populate_user_table_basic(&mut sim, &mut ctx);

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

      // Check that the ResultView in the response is what we expect.
      let mut exp_result = QueryResult::new(vec![]);
      assert_eq!(payload.result, exp_result);

      // Check that final data in the system is what we expect.
      {
        let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
        let mut exp_result = QueryResult::new(vec![cno("email"), cno("balance")]);
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
    if ctx.simulate_until_response(&mut sim, 15) {
      let response = ctx.next_response(&mut sim);
      match response.clone() {
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
          // Here, the original query responded before we can even send a cancellation.
          check_success(&mut sim, &mut ctx, &payload, request_id.clone());
        }
        _ => panic!(),
      }
    } else {
      // Otherwise, send a cancellation and simulate until the end.
      ctx.send_cancellation(&mut sim, request_id.clone());
      assert!(ctx.simulate_until_response(&mut sim, 10000));
      let response = ctx.next_response(&mut sim);
      match response.clone() {
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
          // Here, despite the cancellation request, the original request succeeded.
          check_success(&mut sim, &mut ctx, &payload, request_id.clone());
        }
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(payload)) => {
          // Here, the cancellation request successfullly cancelled the original request.
          assert_eq!(payload.payload, msg::ExternalAbortedData::CancelConfirmed);
          cancel_succeeded = true;
        }
        _ => panic!("Incorrect Response: {:#?}", response),
      }
    }

    // Simulate more for a cooldown time and verify that all resources get cleaned up.
    assert!(simulate_until_clean(&mut sim, 10000));

    // If the Query had been successfull cancelled, we verify that the data in the system
    // is what we exact. We do this after cooldown to know that this is the stead state.
    if cancel_succeeded {
      {
        let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email"), cno("count")]);
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
        let mut exp_result = QueryResult::new(vec![cno("email"), cno("balance")]);
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
    test_time_taken = test_time_taken.add(sim.true_timestamp().clone());
    total_count += 1;
  }

  // Check that we encoutered healthy balance of cancelled and non-cancelled. (If not, we
  // should tune the above such that we do).
  if cancelled_count < 4 && total_count - cancelled_count < 4 {
    panic!();
  }

  println!("Test 'cancellation_test' Passed! Time taken: {:?}ms", test_time_taken.time_ms)
}

// -----------------------------------------------------------------------------------------------
//  paxos_leader_change_test
// -----------------------------------------------------------------------------------------------

fn paxos_leader_change_test(seed: [u8; 16]) {
  // Create one Slave Paxos Group to test Leader change logic with.
  let mut sim = mk_general_sim(seed, 1, 1, 5, 1, 0);

  // Warmup the simulation
  sim.simulate_n_ms(100);

  // Block the current leader of the SlaveGroup
  let sid = sim.full_db_schema().slave_address_config.first_key_value().unwrap().0.clone();
  let lid = sim.leader_map.get(&sid.to_gid()).unwrap().clone();
  sim.start_leadership_change(sid.to_gid());

  // Simulate until the Leadership changes
  let mut leader_did_change = false;
  for _ in 0..1000 {
    sim.simulate1ms();
    let cur_lid = sim.leader_map.get(&sid.to_gid()).unwrap().clone();
    if cur_lid.gen > lid.gen {
      leader_did_change = true;
      break;
    }
  }

  if leader_did_change {
    println!(
      "Test 'paxos_leader_change_test' Passed! Time taken: {:?}ms",
      sim.true_timestamp().time_ms
    );
  } else {
    panic!();
  }
}

fn paxos_basic_serial_test(seed: [u8; 16]) {
  let mut test_time_taken = mk_t(0);

  const EXPECTED_TOTAL_TIME: u32 = 1000;
  const NUM_ITERATIONS: u32 = 10;
  const EXPECTED_TIME_PER_ITERATION: u32 = EXPECTED_TOTAL_TIME / NUM_ITERATIONS;

  // Simulate 10 iterations, where each iteration uses a new initial seed and also
  // changes the Leadership of some random node a little bit later than the last.
  let mut successful = 0;
  let mut failed = 0;
  'outer: for i in 0..NUM_ITERATIONS {
    println!("    iteration {:?}", i);
    let mut sim = mk_general_sim(seed, 1, 5, 5, 1, 0);
    let mut ctx = TestContext::new(&sim);

    // Test Simple Update-Select
    deprecated_setup_inventory_table(&mut sim, &mut ctx);
    deprecated_populate_inventory_table_basic(&mut sim, &mut ctx);

    // Send the query and simulate
    let query = "
      UPDATE inventory
      SET email = 'my_email_3'
      WHERE product_id = 1;
    ";
    let request_id = ctx.send_query(&mut sim, query);

    // Here, we try to distribute `target_change_timestamp` uniformly across a single
    // execution, incrementing a little every iteration. We divide by 2 to bias the
    // leadership change closer to the front.
    let target_change_timestamp =
      mk_t((i * (EXPECTED_TIME_PER_ITERATION / NUM_ITERATIONS / 2)) as u128);

    // Choose a random Slave to be the target of the Leadership change
    let mut sids: Vec<_> = sim.full_db_schema().slave_address_config.keys().cloned().collect();

    // Remove the `sid` contains the Slave EndpointId that queries are sent to in order to
    // avoid changing its Leadership.
    // TODO: avoid needing to this (e.g. use the sim.leader_map to send queries)
    for (sid, eids) in sim.full_db_schema().slave_address_config {
      if eids.contains(&ctx.slave_eid()) {
        remove_item(&mut sids, sid);
        break;
      }
    }

    let target_change_sid = sids.get(sim.rand.next_u32() as usize % sids.len()).unwrap().clone();

    let mut change_state = Some((target_change_sid, target_change_timestamp));

    // Simulate for at-most 10000ms, giving up if we do not get a response in time.
    for _ in 0..10000 {
      // Progress the LeaderChangeState
      if let Some((sid, timestamp)) = &change_state {
        if sim.true_timestamp() >= &timestamp {
          // Start changing the Leadership of `sid`
          sim.start_leadership_change(sid.to_gid());
          change_state = None;
        }
      }

      // Simulate 1ms
      let response_count = sim.get_responses(&ctx.sender_eid()).len();
      sim.simulate1ms();

      // If we get a response, act accordingly
      if response_count < sim.get_responses(&ctx.sender_eid()).len() {
        // Ensure we are not blocking any queues by this point so SELECT below will succeed.
        sim.stop_leadership_change();

        // Cooldown and check for cleanup
        assert!(simulate_until_clean(&mut sim, 10000));

        // TODO: After removing NodeDied, see if this test holds.
        // Get the response and validate it
        let response = ctx.next_response(&mut sim);
        match response.clone() {
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(payload)) => {
            assert_eq!(payload.request_id, request_id.clone());
            // Verify the result is what we expect
            let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
            exp_result.add_row(vec![Some(cvi(1)), Some(cvs("my_email_3"))]);
            assert_eq!(payload.result, exp_result);

            // Do a Select query and verify it matches what we expect the final data to be.
            let mut exp_result = QueryResult::new(vec![cno("product_id"), cno("email")]);
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

            successful += 1;
          }
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(abort)) => {
            assert_eq!(abort.request_id, request_id.clone());
            failed += 1;
          }
          _ => panic!(),
        };

        test_time_taken = test_time_taken.add(sim.true_timestamp().clone());
        continue 'outer;
      }
    }

    panic!();
  }

  // Ensure the test occurred in a sensible amount of time.
  assert!(test_time_taken < mk_t(1500));
  println!("Test 'paxos_basic_serial_test' Passed! Time taken: {:?}ms", test_time_taken.time_ms);
}
