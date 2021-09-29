mod simulation;

use crate::simulation::{client_eid, slave_eid, Simulation};
use runiversal::common::TableSchema;
use runiversal::model::common::{
  ColName, ColType, ColVal, EndpointId, Gen, PrimaryKey, RequestId, SlaveGroupId, TablePath,
  TabletGroupId, TabletKeyRange,
};
use runiversal::model::message as msg;
use runiversal::test_utils::{cn, cvi, cvs, mk_sid, mk_tab, mk_tid};
use std::collections::HashMap;

fn main() {
  // Fundamental seed used for all random number generation,
  // providing determinism.
  let mut seed = [0; 16];
  for i in 0..16 {
    seed[i] = i as u8;
  }

  let slave_address_config: HashMap<SlaveGroupId, EndpointId> = vec![
    (mk_sid("s0"), slave_eid(&0)),
    (mk_sid("s1"), slave_eid(&1)),
    (mk_sid("s2"), slave_eid(&2)),
    (mk_sid("s3"), slave_eid(&3)),
    (mk_sid("s4"), slave_eid(&4)),
  ]
  .into_iter()
  .collect();

  // We just have one Tablet per Slave for now.
  let tablet_address_config: HashMap<TabletGroupId, SlaveGroupId> = vec![
    (mk_tid("t0"), mk_sid("s0")),
    (mk_tid("t1"), mk_sid("s1")),
    (mk_tid("t2"), mk_sid("s2")),
    (mk_tid("t3"), mk_sid("s3")),
    (mk_tid("t4"), mk_sid("s4")),
  ]
  .into_iter()
  .collect();

  let schema: HashMap<TablePath, TableSchema> = vec![
    (
      mk_tab("tab0"),
      TableSchema::new(vec![(cn("id0"), ColType::String)], vec![(cn("c1"), ColType::Int)]),
    ),
    (
      mk_tab("tab1"),
      TableSchema::new(
        vec![(cn("id1"), ColType::String), (cn("id2"), ColType::String)],
        vec![(cn("c2"), ColType::Int)],
      ),
    ),
    (
      mk_tab("tab2"),
      TableSchema::new(
        vec![(cn("id3"), ColType::Int)],
        vec![(cn("c3"), ColType::String), (cn("c4"), ColType::Bool)],
      ),
    ),
  ]
  .into_iter()
  .collect();

  #[rustfmt::skip]
  let sharding_config: HashMap<(TablePath, Gen), Vec<(TabletKeyRange, TabletGroupId)>> = vec![
    (
      (mk_tab("tab0"), Gen(0)),
      vec![
        (
          TabletKeyRange {
            start: None,
            end: Some(PrimaryKey { cols: vec![cvs("d")] })
          },
          mk_tid("t0"),
        ),
        (
          TabletKeyRange {
            start: Some(PrimaryKey { cols: vec![cvs("d")] }),
            end: Some(PrimaryKey { cols: vec![cvs("q")] }),
          },
          mk_tid("t1"),
        ),
        (
          TabletKeyRange {
            start: Some(PrimaryKey { cols: vec![cvs("q")] }),
            end: None
          },
          mk_tid("t2"),
        ),
      ],
    ),
    (
      (mk_tab("tab1"), Gen(0)),
      vec![
        (
          TabletKeyRange {
            start: None,
            end: Some(PrimaryKey { cols: vec![cvs("f"), cvs("h")] })
          },
          mk_tid("t2"),
        ),
        (
          TabletKeyRange {
            start: Some(PrimaryKey { cols: vec![cvs("f"), cvs("h")] }),
            end: None
          },
          mk_tid("t3"),
        ),
      ],
    ),
    (
      (mk_tab("tab2"), Gen(0)),
      vec![
        (
          TabletKeyRange {
            start: None,
            end: Some(PrimaryKey { cols: vec![cvi(-100)] })
          },
          mk_tid("t2"),
        ),
        (
          TabletKeyRange {
            start: Some(PrimaryKey { cols: vec![cvi(-100)] }),
            end: Some(PrimaryKey { cols: vec![cvi(100)] }),
          },
          mk_tid("t3"),
        ),
        (
          TabletKeyRange {
            start: Some(PrimaryKey { cols: vec![cvi(100)] }),
            end: None
          },
          mk_tid("t4"),
        ),
      ],
    ),
  ]
  .into_iter()
  .collect();

  let mut sim =
    Simulation::new(seed, 5, schema, sharding_config, tablet_address_config, slave_address_config);

  // let query = "\
  //     SELECT a, b, 123, myfunc(b) \
  //     FROM table_1 \
  //     WHERE a > b AND b < 100 \
  //     ORDER BY a DESC, b;
  //   \
  //     SELECT a, b, 123, myfunc(b) \
  //     FROM table_1 \
  //     WHERE a > b AND b < 100 \
  //     ORDER BY a DESC, b";

  // let query = "\
  //   ALTER TABLE bank ADD COLUMN address STRING UNIQUE;";

  let query = "\
    SELECT a, b, c, d \
    FROM table_1 AS hi(foo, boo, bar) \
    WHERE a > b AND b < -100 \
    ORDER BY a DESC, b";

  sim.add_msg(
    msg::NetworkMessage::Slave(msg::SlaveMessage::ExternalMessage(
      msg::SlaveExternalReq::PerformExternalQuery(msg::PerformExternalQuery {
        sender_eid: client_eid(&0),
        request_id: RequestId("rid".to_string()),
        query: query.to_string(),
      }),
    )),
    &client_eid(&2),
    &slave_eid(&2),
  );

  sim.simulate_all();
}
