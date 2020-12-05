use crate::simulation::Simulation;
use runiversal::common::test_config::{endpoint, table_shape};
use std::collections::HashMap;

mod simulation;

fn main() {
  // Fundamental seed used for all random number generation,
  // providing determinism.
  let mut seed = [0u8; 16];
  for i in 0..16 {
    seed[i] = i as u8;
  }

  // A pre-defined map of what tablets that each slave should be managing.
  // For the simulation, this map specifies all initial tables in the system,
  // initial number of slaves, which slave holds which tabet, etc.
  let mut key_space_config = HashMap::new();
  key_space_config.insert(endpoint("s0"), vec![table_shape("table1", None, None)]);
  key_space_config.insert(endpoint("s1"), vec![table_shape("table2", None, Some("j"))]);
  key_space_config.insert(
    endpoint("s2"),
    vec![
      table_shape("table2", Some("j"), None),
      table_shape("table3", None, Some("d")),
      table_shape("table4", None, Some("k")),
    ],
  );
  key_space_config.insert(
    endpoint("s3"),
    vec![table_shape("table3", Some("d"), Some("p"))],
  );
  key_space_config.insert(
    endpoint("s4"),
    vec![
      table_shape("table3", Some("p"), None),
      table_shape("table4", Some("k"), None),
    ],
  );

  Simulation::new(seed, key_space_config, 5);
}
