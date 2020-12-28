use crate::simulation::{slave_id, Simulation};
use runiversal::common::test_config::table_shape;
use runiversal::common::utils::mk_tid;
use runiversal::model::common::{
  ColumnName as CN, ColumnType as CT, ColumnValue as CV, EndpointId, PrimaryKey, RequestId, Row,
  Schema, TabletPath, TabletShape, Timestamp,
};
use runiversal::model::message::{
  AdminMessage, AdminRequest, AdminResponse, NetworkMessage, SlaveMessage,
};
use runiversal::sql::parser::parse_sql;
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;

mod simulation;

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
    key_cols: vec![(CT::String, CN(String::from("key")))],
    val_cols: vec![(CT::Int, CN(String::from("value")))],
  }
}

/// Convenience function for creating a key that follows
/// the schema above.
fn mk_key(key: &str) -> PrimaryKey {
  PrimaryKey {
    cols: vec![CV::String(key.to_string())],
  }
}

fn mk_val(val: Option<i32>) -> Vec<Option<CV>> {
  vec![val.map(|val| CV::Int(val))]
}

fn add_req_res(
  sim: &mut Simulation,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
  req: AdminRequest,
  expected_res: AdminResponse,
  expected_res_map: &mut HashMap<RequestId, NetworkMessage>,
  rid: RequestId,
) {
  sim.add_msg(
    NetworkMessage::Slave(SlaveMessage::AdminRequest { req }),
    &from_eid,
    &to_eid,
  );
  expected_res_map.insert(
    rid,
    NetworkMessage::Admin(AdminMessage::AdminResponse { res: expected_res }),
  );
}

/// Checks to see if the messages in expected_res_map were
/// actually sent out by the Simulation.
fn check_expected_res(
  sim: &Simulation,
  expected_res_map: &HashMap<RequestId, NetworkMessage>,
) -> Result<(), String> {
  let mut res_map = HashMap::new();
  for (_, msgs) in sim.get_responses() {
    for msg in msgs {
      match msg {
        NetworkMessage::Admin(AdminMessage::AdminResponse { res }) => {
          let rid = match res {
            AdminResponse::Insert { rid, .. } => rid,
            AdminResponse::Read { rid, .. } => rid,
            AdminResponse::SqlQuery { rid, .. } => rid,
          };
          res_map.insert(rid.clone(), msg.clone());
        }
        _ => {}
      }
    }
  }
  for (request_id, expected_res) in expected_res_map.iter() {
    if let Some(res) = res_map.get(request_id) {
      if res != expected_res {
        return Err(format!(
          "True Response {:#?} does not match expected Response {:#?}",
          res, expected_res
        ));
      }
    } else {
      return Err(format!(
        "Response for RequestId {:#?} doesn't exist",
        request_id
      ));
    }
  }
  Ok(())
}

// -------------------------------------------------------------------------------------------------
//  Tests
// -------------------------------------------------------------------------------------------------

fn basic_read_write(sim: &mut Simulation) -> Result<(), String> {
  let path = TabletPath::from("table1");
  let key = mk_key("k");
  let value = mk_val(Some(2));
  let timestamp = Timestamp(2);
  let from_eid = EndpointId::from("c0");
  let to_eid = EndpointId::from("s0");
  let rid = sim.mk_request_id();
  sim.add_msg(
    NetworkMessage::Slave(SlaveMessage::AdminRequest {
      req: AdminRequest::Insert {
        rid,
        path: path.clone(),
        key: key.clone(),
        value: value.clone(),
        timestamp: timestamp.clone(),
      },
    }),
    &from_eid,
    &to_eid,
  );
  sim.simulate_all();

  let mut expected_res_map = HashMap::new();

  let rid = sim.mk_request_id();
  add_req_res(
    sim,
    &from_eid,
    &to_eid,
    AdminRequest::Read {
      rid: rid.clone(),
      path: path.clone(),
      key: key.clone(),
      timestamp: timestamp.clone(),
    },
    AdminResponse::Read {
      rid: rid.clone(),
      result: Ok(Some(Row {
        key: key.clone(),
        val: value.clone(),
      })),
    },
    &mut expected_res_map,
    rid,
  );
  sim.simulate_all();

  check_expected_res(sim, &expected_res_map)
}

/// Test this does a simple INSERT and subsequent SELECT for a
/// single table that's just held in one node (i.e. one tablet).
fn basic_insert_select(sim: &mut Simulation) -> Result<(), String> {
  let from_eid = EndpointId::from("c0");
  let to_eid = EndpointId::from("s0");

  let rid = sim.mk_request_id();
  let tid = mk_tid(&mut sim.rng);
  sim.add_msg(
    NetworkMessage::Slave(SlaveMessage::AdminRequest {
      req: AdminRequest::SqlQuery {
        rid,
        tid,
        sql: parse_sql(
          &r#"
            INSERT INTO table1 (key, value)
            VALUES ("hello", 1)
          "#
          .to_string(),
        )
        .unwrap(),
        timestamp: Timestamp(2),
      },
    }),
    &from_eid,
    &to_eid,
  );
  sim.simulate_all();

  let mut expected_res_map = HashMap::new();

  let rid = sim.mk_request_id();
  let tid = mk_tid(&mut sim.rng);
  add_req_res(
    sim,
    &from_eid,
    &to_eid,
    AdminRequest::SqlQuery {
      rid: rid.clone(),
      tid,
      sql: parse_sql(
        &r#"
            SELECT key, value
            FROM table1
            WHERE TRUE
          "#
        .to_string(),
      )
      .unwrap(),
      timestamp: Timestamp(2),
    },
    AdminResponse::SqlQuery {
      rid: rid.clone(),
      result: Ok(BTreeMap::from_iter(
        vec![(
          mk_key("hello"),
          vec![
            (CN("key".to_string()), Some(CV::String("hello".to_string()))),
            (CN("value".to_string()), Some(CV::Int(1))),
          ],
        )]
        .into_iter(),
      )),
    },
    &mut expected_res_map,
    rid,
  );
  sim.simulate_all();

  check_expected_res(sim, &expected_res_map)
}

/// This test checks to see if multiple values can be inserted
/// into a table that's just in one tablet.
fn insert_select_multi_tablet(sim: &mut Simulation) -> Result<(), String> {
  let from_eid = EndpointId::from("c0");
  let to_eid = EndpointId::from("s0");

  let rid = sim.mk_request_id();
  let tid = mk_tid(&mut sim.rng);
  sim.add_msg(
    NetworkMessage::Slave(SlaveMessage::AdminRequest {
      req: AdminRequest::SqlQuery {
        rid,
        tid,
        sql: parse_sql(
          &r#"
            INSERT INTO table2 (key, value)
            VALUES ("hello", 1),
                   ("kello", 2)
          "#
          .to_string(),
        )
        .unwrap(),
        timestamp: Timestamp(2),
      },
    }),
    &from_eid,
    &to_eid,
  );
  sim.simulate_all();

  let mut expected_res_map = HashMap::new();

  let rid = sim.mk_request_id();
  let tid = mk_tid(&mut sim.rng);
  add_req_res(
    sim,
    &from_eid,
    &to_eid,
    AdminRequest::SqlQuery {
      rid: rid.clone(),
      tid,
      sql: parse_sql(
        &r#"
            SELECT key, value
            FROM table2
            WHERE TRUE
          "#
        .to_string(),
      )
      .unwrap(),
      timestamp: Timestamp(2),
    },
    AdminResponse::SqlQuery {
      rid: rid.clone(),
      result: Ok(BTreeMap::from_iter(
        vec![
          (
            mk_key("hello"),
            vec![
              (CN("key".to_string()), Some(CV::String("hello".to_string()))),
              (CN("value".to_string()), Some(CV::Int(1))),
            ],
          ),
          (
            mk_key("kello"),
            vec![
              (CN("key".to_string()), Some(CV::String("kello".to_string()))),
              (CN("value".to_string()), Some(CV::Int(2))),
            ],
          ),
        ]
        .into_iter(),
      )),
    },
    &mut expected_res_map,
    rid,
  );
  sim.simulate_all();

  check_expected_res(sim, &expected_res_map)
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
  drive_test(1, "basic_read_write", basic_read_write);
  drive_test(2, "basic_insert_select", basic_insert_select);
  drive_test(3, "insert_select_multi_tablet", insert_select_multi_tablet);
}

fn main() {
  test_driver();
}
