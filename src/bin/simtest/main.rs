use crate::simulation::{slave_id, Simulation};
use runiversal::common::test_config::table_shape;
use runiversal::model::common::{
  ColumnName, ColumnType, ColumnValue, EndpointId, PrimaryKey, RequestId, Row, Schema, TabletPath,
  TabletShape, Timestamp,
};
use runiversal::model::message::{
  AdminMessage, AdminMeta, AdminPayload, AdminRequest, AdminResponse, SlaveMessage,
};
use std::collections::HashMap;

mod simulation;

// -------------------------------------------------------------------------------------------------
//  Test Utilities
// -------------------------------------------------------------------------------------------------

/// A pre-defined map of what tablets that each slave should be managing.
/// For the simulation, this map specifies all initial tables in the system,
/// initial number of slaves, which slave holds which tabet, etc.
fn key_space_config() -> HashMap<EndpointId, Vec<TabletShape>> {
  let mut key_space_config = HashMap::new();
  key_space_config.insert(slave_id(&0), vec![table_shape("table1", None, None)]);
  key_space_config.insert(slave_id(&1), vec![table_shape("table2", None, Some("j"))]);
  key_space_config.insert(
    slave_id(&2),
    vec![
      table_shape("table2", Some("j"), None),
      table_shape("table3", None, Some("d")),
      table_shape("table4", None, Some("k")),
    ],
  );
  key_space_config.insert(
    slave_id(&3),
    vec![table_shape("table3", Some("d"), Some("p"))],
  );
  key_space_config.insert(
    slave_id(&4),
    vec![
      table_shape("table3", Some("p"), None),
      table_shape("table4", Some("k"), None),
    ],
  );
  return key_space_config;
}

/// Schema that the above map was constructed assuming.
fn schema() -> Schema {
  Schema {
    key_cols: vec![(ColumnType::String, ColumnName(String::from("key")))],
    val_cols: vec![(ColumnType::Int, ColumnName(String::from("value")))],
  }
}

fn mk_key(key: &str) -> PrimaryKey {
  PrimaryKey {
    cols: vec![ColumnValue::String(key.to_string())],
  }
}

fn mk_val(val: Option<i32>) -> Vec<Option<ColumnValue>> {
  vec![val.map(|val| ColumnValue::Int(val))]
}

fn add_req_res(
  sim: &mut Simulation,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
  req: AdminRequest,
  expected_res: AdminResponse,
  expected_res_map: &mut HashMap<RequestId, SlaveMessage>,
) {
  let request_id = sim.add_admin_msg(from_eid, to_eid, AdminPayload::Request(req));
  expected_res_map.insert(
    request_id.clone(),
    SlaveMessage::Admin(AdminMessage {
      meta: AdminMeta { request_id },
      payload: AdminPayload::Response(expected_res),
    }),
  );
}

/// Checks to see if the messages in expected_res_map were
/// actually sent out by the Simulation.
fn check_expected_res(
  sim: &Simulation,
  expected_res_map: &HashMap<RequestId, SlaveMessage>,
) -> Result<(), String> {
  let mut res_map = HashMap::new();
  for (_, slave_msgs) in sim.get_responses() {
    for slave_msg in slave_msgs {
      match slave_msg {
        SlaveMessage::Admin(admin_msg) => {
          res_map.insert(admin_msg.meta.request_id.clone(), slave_msg.clone());
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

fn test1(sim: &mut Simulation) -> Result<(), String> {
  let path = TabletPath::from("table1");
  let key = mk_key("k");
  let value = mk_val(Some(2));
  let timestamp = Timestamp(2);
  let from_eid = EndpointId::from("c0");
  let to_eid = EndpointId::from("s0");
  sim.add_admin_msg(
    &from_eid,
    &to_eid,
    AdminPayload::Request(AdminRequest::Insert {
      path: path.clone(),
      key: key.clone(),
      value: value.clone(),
      timestamp: timestamp.clone(),
    }),
  );
  sim.simulate_all();

  let mut expected_res_map = HashMap::new();
  add_req_res(
    sim,
    &from_eid,
    &to_eid,
    AdminRequest::Read {
      path: path.clone(),
      key: key.clone(),
      timestamp: timestamp.clone(),
    },
    AdminResponse::Read {
      result: Ok(Some(Row {
        key: key.clone(),
        val: value.clone(),
      })),
    },
    &mut expected_res_map,
  );
  sim.simulate_all();

  check_expected_res(sim, &expected_res_map)
}

// -------------------------------------------------------------------------------------------------
//  Test Driver
// -------------------------------------------------------------------------------------------------

fn drive_test(test_num: u32, test: fn(&mut Simulation) -> Result<(), String>) {
  // Fundamental seed used for all random number generation,
  // providing determinism.
  let mut seed = [0; 16];
  for i in 0..16 {
    seed[i] = (16 * test_num + i as u32) as u8;
  }

  match test(&mut Simulation::new(seed, schema(), key_space_config(), 5)) {
    Ok(_) => println!("Test {} Passed!", test_num),
    Err(err) => println!("Test {} Failed with Error: {}", test_num, err),
  }
}

fn test_driver() {
  drive_test(1, test1);
}

fn main() {
  test_driver();
}
