use crate::simulation::Simulation;

mod simulation;

fn main() {
    let mut seed = [0u8; 16];
    for i in 0..16 {
        seed[i] = i as u8;
    }

    Simulation::new(seed);
}
