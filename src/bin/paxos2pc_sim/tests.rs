use crate::tests_paxos2pc;
use crate::tests_stmpaxos2pc;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;

/// Run `test_single()` multiple times, each with a different seed.
pub fn test() {
  let mut orig_rand = XorShiftRng::from_seed([0; 16]);
  for i in 0..2000 {
    let mut seed = [0; 16];
    orig_rand.fill_bytes(&mut seed);
    if i % 2 == 0 {
      tests_stmpaxos2pc::test_single(i, seed);
    } else {
      tests_paxos2pc::test_single(i, seed);
    }
  }
}
