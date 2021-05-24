mod simulation;

use crate::simulation::{client_eid, slave_eid, Simulation};
use runiversal::model::common::{
  ColName, ColType, ColValue, EndpointId, PrimaryKey, RequestId, SlaveGroupId, TablePath,
  TableSchema, TabletGroupId, TabletKeyRange,
};
use runiversal::model::message::{ExternalMessage, NetworkMessage};
use runiversal::model::message::{PerformExternalQuery, SlaveMessage};
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
// Convenience functions
// -----------------------------------------------------------------------------------------------

pub fn cn(s: &str) -> ColName {
  ColName(s.to_string())
}

pub fn cvs(s: &str) -> ColValue {
  ColValue::String(s.to_string())
}

pub fn cvi(i: i32) -> ColValue {
  ColValue::Int(i)
}

pub fn mk_sid(id: &str) -> SlaveGroupId {
  SlaveGroupId(id.to_string())
}

pub fn mk_tid(id: &str) -> TabletGroupId {
  TabletGroupId(id.to_string())
}

pub fn mk_tab(table_path: &str) -> TablePath {
  TablePath(table_path.to_string())
}

fn main() {
  // Fundamental seed used for all random number generation,
  // providing determinism.
  let mut seed = [0; 16];
  for i in 0..16 {
    seed[i] = i as u8;
  }

  let slave_address_config: HashMap<SlaveGroupId, EndpointId> = [
    (mk_sid("s0"), slave_eid(&0)),
    (mk_sid("s1"), slave_eid(&1)),
    (mk_sid("s2"), slave_eid(&2)),
    (mk_sid("s3"), slave_eid(&3)),
    (mk_sid("s4"), slave_eid(&4)),
  ]
  .iter()
  .cloned()
  .collect();

  // We just have one Tablet per Slave for now.
  let tablet_address_config: HashMap<TabletGroupId, SlaveGroupId> = [
    (mk_tid("t0"), mk_sid("s0")),
    (mk_tid("t1"), mk_sid("s1")),
    (mk_tid("t2"), mk_sid("s2")),
    (mk_tid("t3"), mk_sid("s3")),
    (mk_tid("t4"), mk_sid("s4")),
  ]
  .iter()
  .cloned()
  .collect();

  let schema: HashMap<TablePath, TableSchema> = [
    (
      mk_tab("tab0"),
      TableSchema {
        key_cols: vec![(ColType::String, cn("id0"))],
        val_cols: vec![(ColType::Int, cn("c1"))],
      },
    ),
    (
      mk_tab("tab1"),
      TableSchema {
        key_cols: vec![(ColType::String, cn("id1")), (ColType::String, cn("id2"))],
        val_cols: vec![(ColType::Int, cn("c2"))],
      },
    ),
    (
      mk_tab("tab2"),
      TableSchema {
        key_cols: vec![(ColType::Int, cn("id3"))],
        val_cols: vec![(ColType::String, cn("c3")), (ColType::Bool, cn("c4"))],
      },
    ),
  ]
  .iter()
  .cloned()
  .collect();

  #[rustfmt::skip]
  let sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>> = [
    (
      mk_tab("tab0"),
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
      mk_tab("tab1"),
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
      mk_tab("tab2"),
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
  .iter()
  .cloned()
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
    NetworkMessage::Slave(SlaveMessage::PerformExternalQuery(PerformExternalQuery {
      request_id: RequestId("rid".to_string()),
      query: query.to_string(),
    })),
    &client_eid(&2),
    &slave_eid(&2),
  );

  sim.simulate_all();
}
