use crate::tests_paxos2pc;
use crate::tests_stmpaxos2pc;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;

/// Run `test_single()` multiple times, each with a different seed.
pub fn test() {
  let mut orig_rand = XorShiftRng::from_seed([0; 16]);
  println!("Doing STMPaxos2PC Tests.");
  for i in 0..100 {
    let mut seed = [0; 16];
    orig_rand.fill_bytes(&mut seed);
    tests_stmpaxos2pc::test_single(i, seed);
  }
  println!("Doing Paxos2PC Tests.");
  for i in 0..100 {
    let mut seed = [0; 16];
    orig_rand.fill_bytes(&mut seed);
    tests_paxos2pc::test_single(i, seed);
  }
}
