#![feature(stmt_expr_attributes)]
use crate::simulation::{slave_id, Simulation};
use crate::test_utils::{cn, cvi, cvs, exec_seq_session, make_view};
use runiversal::common::test_config::table_shape;
use runiversal::model::common::{
  ColumnName, ColumnType, EndpointId, Schema, TabletShape, Timestamp,
};
use std::collections::{BTreeMap, HashMap};

mod simulation;
mod test_utils;

// -------------------------------------------------------------------------------------------------
//  Test Utilities
// -------------------------------------------------------------------------------------------------

/// A pre-defined map of what tablets that each slave should be managing.
/// For the simulation, this map specifies all initial tables in the system,
/// initial number of slaves, which slave holds which tabet, etc.
fn tablet_config() -> HashMap<EndpointId, Vec<TabletShape>> {
  let mut tablet_config = HashMap::new();
  tablet_config.insert(slave_id(&0), vec![table_shape("table1", None, None)]);
  tablet_config.insert(slave_id(&1), vec![table_shape("table2", None, Some("j"))]);
  tablet_config.insert(
    slave_id(&2),
    vec![
      table_shape("table2", Some("j"), None),
      table_shape("table3", None, Some("d")),
      table_shape("table4", None, Some("k")),
    ],
  );
  tablet_config.insert(
    slave_id(&3),
    vec![table_shape("table3", Some("d"), Some("p"))],
  );
  tablet_config.insert(
    slave_id(&4),
    vec![
      table_shape("table3", Some("p"), None),
      table_shape("table4", Some("k"), None),
    ],
  );
  return tablet_config;
}

/// Schema that the above map was constructed assuming.
fn schema() -> Schema {
  Schema {
    key_cols: vec![(ColumnType::String, ColumnName(String::from("key")))],
    val_cols: vec![(ColumnType::Int, ColumnName(String::from("value")))],
  }
}

// -------------------------------------------------------------------------------------------------
//  Tests
// -------------------------------------------------------------------------------------------------

/// Test this does a simple INSERT and subsequent SELECT for a
/// single table that's just held in one node (i.e. one tablet).
fn basic_insert_select(sim: &mut Simulation) -> Result<(), String> {
  exec_seq_session(
    sim,
    &EndpointId::from("c0"),
    &EndpointId::from("s0"),
    vec![
      (
        r#"
          INSERT INTO table1 (key, value)
          VALUES ("hello", 1)
        "#,
        Timestamp(2),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          SELECT key, value
          FROM table1
          WHERE TRUE
        "#,
        Timestamp(2),
        #[rustfmt::skip] Ok(make_view(
          (vec![cn("key")], vec![(cn("value"))]),
          vec![
            vec![cvs("hello"), cvi(1)],
          ],
        )),
      ),
    ],
  )
}

/// This test checks to see if multiple values can be inserted
/// into a table that's just in one tablet.
fn insert_select_multi_tablet(sim: &mut Simulation) -> Result<(), String> {
  exec_seq_session(
    sim,
    &EndpointId::from("c0"),
    &EndpointId::from("s0"),
    vec![
      (
        r#"
          INSERT INTO table2 (key, value)
          VALUES ("hello", 1),
                 ("kello", 2)
        "#,
        Timestamp(2),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          SELECT key, value
          FROM table2
          WHERE TRUE
        "#,
        Timestamp(2),
        #[rustfmt::skip] Ok(make_view(
          (vec![cn("key")], vec![(cn("value"))]),
          vec![
            vec![cvs("hello"), cvi(1)],
            vec![cvs("kello"), cvi(2)]
          ],
        )),
      ),
    ],
  )
}

/// Test a simple Update by inserting a single value and then updating it.
fn basic_insert_update_select(sim: &mut Simulation) -> Result<(), String> {
  exec_seq_session(
    sim,
    &EndpointId::from("c0"),
    &EndpointId::from("s0"),
    vec![
      (
        r#"
          INSERT INTO table1 (key, value)
          VALUES ("hello", 1)
        "#,
        Timestamp(2),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          UPDATE table1
          SET value = 2
          WHERE key = "hello"
        "#,
        Timestamp(3),
        Ok(BTreeMap::new()),
      ),
      (
        &r#"
          SELECT key, value
          FROM table1
          WHERE TRUE
        "#,
        Timestamp(3),
        #[rustfmt::skip] Ok(make_view(
          (vec![cn("key")], vec![(cn("value"))]),
          vec![
            vec![cvs("hello"), cvi(2)]
          ],
        )),
      ),
    ],
  )
}

/// This test checks to see if multiple values can be inserted and then
/// updated through multiple UPDATE queries.
fn insert_update_select_multi_tablet(sim: &mut Simulation) -> Result<(), String> {
  exec_seq_session(
    sim,
    &EndpointId::from("c0"),
    &EndpointId::from("s0"),
    vec![
      (
        r#"
          INSERT INTO table3 (key, value)
          VALUES ("aello", 1),
                 ("hello", 2),
                 ("kello", 3),
                 ("rello", 4)
        "#,
        Timestamp(2),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          UPDATE table3
          SET value = 3
          WHERE key = "hello"
        "#,
        Timestamp(3),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          UPDATE table3
          SET value = 4
          WHERE key = "kello"
        "#,
        // Remember that the timestamp of all udates
        // that touch the same tablet must be distinct, even
        // if they touch different keys.
        Timestamp(4),
        Ok(BTreeMap::new()),
      ),
      (
        &r#"
          SELECT key, value
          FROM table3
          WHERE TRUE
        "#,
        Timestamp(4),
        #[rustfmt::skip] Ok(make_view(
          (vec![cn("key")], vec![(cn("value"))]),
          vec![
            vec![cvs("aello"), cvi(1)],
            vec![cvs("hello"), cvi(3)],
            vec![cvs("kello"), cvi(4)],
            vec![cvs("rello"), cvi(4)],
          ],
        )),
      ),
    ],
  )
}

/// This tests an UPDATE with a non-trivial WHERE clause, where only
/// a subset of keys should be touched in different tablets.
fn update_complex_where(sim: &mut Simulation) -> Result<(), String> {
  exec_seq_session(
    sim,
    &EndpointId::from("c0"),
    &EndpointId::from("s0"),
    vec![
      (
        r#"
          INSERT INTO table3 (key, value)
          VALUES ("aello", 1),
                 ("hello", 2),
                 ("kello", 3),
                 ("rello", 4)
        "#,
        Timestamp(2),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          UPDATE table3
          SET value = 5
          WHERE key = "hello" OR key = "rello"
        "#,
        Timestamp(3),
        Ok(BTreeMap::new()),
      ),
      (
        &r#"
          SELECT key, value
          FROM table3
          WHERE TRUE
        "#,
        Timestamp(4),
        #[rustfmt::skip] Ok(make_view(
          (vec![cn("key")], vec![(cn("value"))]),
          vec![
            vec![cvs("aello"), cvi(1)],
            vec![cvs("hello"), cvi(5)],
            vec![cvs("kello"), cvi(3)],
            vec![cvs("rello"), cvi(5)],
          ],
        )),
      ),
    ],
  )
}

/// This tests an UPDATE with a non-trivial WHERE clause, where only
/// a subset of keys should be touched in different tablets.
fn basic_subquery(sim: &mut Simulation) -> Result<(), String> {
  exec_seq_session(
    sim,
    &EndpointId::from("c0"),
    &EndpointId::from("s0"),
    vec![
      (
        r#"
          INSERT INTO table1 (key, value)
          VALUES ("hi", 2)
        "#,
        Timestamp(1),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          INSERT INTO table2 (key, value)
          VALUES ("hello",
            (
              SELECT value
              FROM table1
              WHERE key = "hi"
            ) + 1
          )
        "#,
        Timestamp(2),
        Ok(BTreeMap::new()),
      ),
      (
        &r#"
          SELECT key, value
          FROM table2
          WHERE TRUE
        "#,
        Timestamp(4),
        #[rustfmt::skip] Ok(make_view(
          (vec![cn("key")], vec![(cn("value"))]),
          vec![vec![cvs("hello"), cvi(3)]],
        )),
      ),
    ],
  )
}

/// This test does multiple subqueries over different tablets in a single
/// INSERT statement, just being a little less trivial.
fn multi_tablet_subquery(sim: &mut Simulation) -> Result<(), String> {
  exec_seq_session(
    sim,
    &EndpointId::from("c0"),
    &EndpointId::from("s0"),
    vec![
      (
        r#"
          INSERT INTO table3 (key, value)
          VALUES ("aello", 1),
                 ("hello", 2),
                 ("kello", 3),
                 ("rello", 4)
        "#,
        Timestamp(1),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          INSERT INTO table2 (key, value)
          VALUES ("hi", 2),
                 ("ri", 5)
        "#,
        Timestamp(1),
        Ok(BTreeMap::new()),
      ),
      (
        r#"
          INSERT INTO table1 (key, value)
          VALUES ("hello",
            (
              SELECT value
              FROM table3
              WHERE key = "rello"
            ) + (
              SELECT value
              FROM table2
              WHERE key = "hi"
            )
          )
        "#,
        Timestamp(2),
        Ok(BTreeMap::new()),
      ),
      (
        &r#"
          SELECT key, value
          FROM table1
          WHERE TRUE
        "#,
        Timestamp(4),
        #[rustfmt::skip] Ok(make_view(
          (vec![cn("key")], vec![(cn("value"))]),
          vec![vec![cvs("hello"), cvi(6)]],
        )),
      ),
    ],
  )
}

// -------------------------------------------------------------------------------------------------
//  Test Driver
// -------------------------------------------------------------------------------------------------

fn drive_test(test_num: u32, test_name: &str, test: fn(&mut Simulation) -> Result<(), String>) {
  // Fundamental seed used for all random number generation,
  // providing determinism.
  let mut seed = [0; 16];
  for i in 0..16 {
    seed[i] = (16 * test_num + i as u32) as u8;
  }

  match test(&mut Simulation::new(seed, schema(), tablet_config(), 5)) {
    Ok(_) => println!("Test {}, {}, Passed!", test_num, test_name),
    Err(err) => println!(
      "Test {}, {}, Failed with Error: {}",
      test_num, test_name, err
    ),
  }
}

fn test_driver() {
  drive_test(1, "basic_insert_select", basic_insert_select);
  drive_test(2, "insert_select_multi_tablet", insert_select_multi_tablet);
  drive_test(3, "basic_insert_update_select", basic_insert_update_select);
  drive_test(
    4,
    "insert_update_select_multi_tablet",
    insert_update_select_multi_tablet,
  );
  drive_test(5, "update_complex_where", update_complex_where);
  drive_test(6, "basic_subquery", basic_subquery);
  drive_test(7, "multi_tablet_subquery", multi_tablet_subquery);
}

fn main() {
  test_driver();
}
