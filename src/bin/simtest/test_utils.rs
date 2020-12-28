use crate::simulation::Simulation;
use runiversal::common::utils::mk_tid;
use runiversal::model::common::{
  ColumnName, ColumnValue, EndpointId, PrimaryKey, RequestId, SelectView, Timestamp,
};
use runiversal::model::message::{
  AdminMessage, AdminRequest, AdminResponse, NetworkMessage, SlaveMessage,
};
use runiversal::sql::parser::parse_sql;
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;

pub fn add_req_res(
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

pub fn add_sql_req_res(
  sim: &mut Simulation,
  expected_res_map: &mut HashMap<RequestId, NetworkMessage>,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
  query: &str,
  timestamp: Timestamp,
  expected_result: Result<SelectView, String>,
) {
  let rid = sim.mk_request_id();
  let tid = mk_tid(&mut sim.rng);
  add_req_res(
    sim,
    &from_eid,
    &to_eid,
    AdminRequest::SqlQuery {
      rid: rid.clone(),
      tid,
      sql: parse_sql(&query.to_string()).unwrap(),
      timestamp,
    },
    AdminResponse::SqlQuery {
      rid: rid.clone(),
      result: expected_result,
    },
    expected_res_map,
    rid,
  );
}

/// This function simulates a sequential request-response session
/// between a client `from_eid` to a singel server `to_eid`. Here,
/// `req_res` contains pairs of requests and responses. For every pair,
/// we send the request, simulate everything until we get a response,
/// and then verify the response is what we expected.
pub fn exec_seq_session(
  sim: &mut Simulation,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
  req_res: Vec<(&str, Timestamp, Result<SelectView, String>)>,
) -> Result<(), String> {
  for (query, timestamp, expected_result) in req_res {
    let mut expected_res_map = HashMap::new();
    add_sql_req_res(
      sim,
      &mut expected_res_map,
      &from_eid,
      &to_eid,
      query,
      timestamp,
      expected_result,
    );
    sim.simulate_all();
    check_expected_res(sim, &expected_res_map)?;
  }
  Ok(())
}

/// Checks to see if the messages in expected_res_map were
/// actually sent out by the Simulation.
pub fn check_expected_res(
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

/// Highly convenient function for creating ColumnValue::String values.
pub fn cvs(s: &str) -> ColumnValue {
  ColumnValue::String(s.to_string())
}

/// Highly convenient function for creating ColumnValue::Int values.
pub fn cvi(i: i32) -> ColumnValue {
  ColumnValue::Int(i)
}

/// Highly convenient function for creating ColumnNames.
pub fn cn(s: &str) -> ColumnName {
  ColumnName(s.to_string())
}

/// This function expands out our View making DSL into a `SelectView`,
/// which is what all Sql queries return.
pub fn make_view(
  col_names: (Vec<ColumnName>, Vec<ColumnName>),
  col_vals: Vec<Vec<ColumnValue>>,
) -> SelectView {
  let mut res = BTreeMap::<PrimaryKey, Vec<(ColumnName, Option<ColumnValue>)>>::new();
  for row in &col_vals {
    assert_eq!(col_names.0.len() + col_names.1.len(), row.len());
    let mut key = PrimaryKey { cols: vec![] };
    let mut col_name_vals = Vec::new();
    for i in 0..col_names.0.len() {
      key.cols.push(row[i].clone());
      col_name_vals.push((col_names.0[i].clone(), Some(row[i].clone())))
    }
    for i in 0..col_names.1.len() {
      col_name_vals.push((
        col_names.1[i].clone(),
        Some(row[col_names.0.len() + i].clone()),
      ))
    }
    res.insert(key, col_name_vals);
  }
  res
}

#[test]
fn test_make_view() {
  assert_eq!(
    BTreeMap::from_iter(
      vec![
        (
          PrimaryKey {
            cols: vec![ColumnValue::String("aello".to_string())],
          },
          vec![
            (ColumnName("key".to_string()), Some(cvs("aello"))),
            (ColumnName("value".to_string()), Some(cvi(1))),
          ],
        ),
        (
          PrimaryKey {
            cols: vec![ColumnValue::String("hello".to_string())],
          },
          vec![
            (ColumnName("key".to_string()), Some(cvs("hello"))),
            (ColumnName("value".to_string()), Some(cvi(5))),
          ],
        ),
        (
          PrimaryKey {
            cols: vec![ColumnValue::String("kello".to_string())],
          },
          vec![
            (ColumnName("key".to_string()), Some(cvs("kello"))),
            (ColumnName("value".to_string()), Some(cvi(3))),
          ],
        ),
        (
          PrimaryKey {
            cols: vec![ColumnValue::String("rello".to_string())],
          },
          vec![
            (ColumnName("key".to_string()), Some(cvs("rello"))),
            (ColumnName("value".to_string()), Some(cvi(5))),
          ],
        ),
      ]
      .into_iter(),
    ),
    make_view(
      (vec![cn("key")], vec![(cn("value"))]),
      vec![
        vec![cvs("aello"), cvi(1)],
        vec![cvs("hello"), cvi(5)],
        vec![cvs("kello"), cvi(3)],
        vec![cvs("rello"), cvi(5)]
      ]
    )
  );
}
