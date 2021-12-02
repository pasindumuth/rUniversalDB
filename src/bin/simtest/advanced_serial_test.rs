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
 * These are more advanced serial tests. Recall that the `basic_serial_test` incrementally
 * introduces new SQL features and tests them. That inheritantly requires earlier tests to be
 * more restricted than later tests. These tests have no such restriction.
 */

// -----------------------------------------------------------------------------------------------
//  test_all_advanced_serial
// -----------------------------------------------------------------------------------------------

pub fn test_all_advanced_serial() {
  trans_table_test();
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
    context.send_query(
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
      100,
      exp_result,
    );
  }

  println!("Test 'trans_table_test' Passed! Time taken: {:?}ms", sim.true_timestamp())
}
