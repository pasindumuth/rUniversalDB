#![feature(map_first_last)]

mod simulation;

use crate::simulation::Simulation;
use runiversal::common::TableSchema;
use runiversal::model::common::{
  ColType, EndpointId, Gen, PrimaryKey, RequestId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange,
};
use runiversal::model::message as msg;
use runiversal::simulation_utils::{mk_client_eid, mk_slave_eid};
use runiversal::test_utils::{cn, cvi, cvs, mk_eid, mk_sid, mk_tab, mk_tid};
use std::collections::BTreeMap;

fn main() {
  tp_test()
}

/// This is a test that solely tests Transaction Processing. We take all PaxosGroups to just
/// have one node. We only check for SQL semantics compatibility.
fn tp_test() {
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

  let mut sim = Simulation::new([0; 16], 1, slave_address_config, master_address_config);

  // Create a Table
  let query = "
    CREATE TABLE inventory (
      product_id INT PRIMARY KEY,
      email      VARCHAR
    );
  ";

  sim.add_msg(
    msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
      msg::MasterExternalReq::PerformExternalDDLQuery(msg::PerformExternalDDLQuery {
        sender_eid: mk_client_eid(&0),
        request_id: RequestId("rid0".to_string()),
        query: query.to_string(),
      }),
    )),
    &mk_client_eid(&0),
    &mk_eid("me0"),
  );

  sim.simulate_n_ms(500);

  // let query = "\
  //   SELECT a, b, c, d \
  //   FROM table_1 AS hi(foo, boo, bar) \
  //   WHERE a > b AND b < -100 \
  //   ORDER BY a DESC, b";
  //
  // sim.add_msg(
  //   msg::NetworkMessage::Slave(msg::SlaveMessage::ExternalMessage(
  //     msg::SlaveExternalReq::PerformExternalQuery(msg::PerformExternalQuery {
  //       sender_eid: mk_client_eid(&0),
  //       request_id: RequestId("rid1".to_string()),
  //       query: query.to_string(),
  //     }),
  //   )),
  //   &mk_client_eid(&2),
  //   &mk_slave_eid(&2),
  // );

  // sim.simulate_all();
}
