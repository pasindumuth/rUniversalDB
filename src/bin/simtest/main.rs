mod simulation;

use crate::simulation::Simulation;
use std::collections::HashMap;

fn main() {
  // Fundamental seed used for all random number generation,
  // providing determinism.
  let mut seed = [0; 16];
  for i in 0..16 {
    seed[i] = i as u8;
  }

  Simulation::new(seed, HashMap::new(), 5);
}
