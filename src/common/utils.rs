use crate::model::common::TransactionId;
use rand::Rng;

pub fn mk_tid<R: Rng>(rng: &mut R) -> TransactionId {
  let mut bytes: [u8; 8] = [0; 8];
  rng.fill(&mut bytes);
  TransactionId(bytes)
}
