use crate::common::Timestamp;

#[test]
fn timestamp_test() {
  assert_eq!(Timestamp::new(1, 2).add(Timestamp::new(1, 1)), Timestamp::new(2, 3));
  assert_eq!(Timestamp::new(1, 2).add(Timestamp::new(1, u64::MAX)), Timestamp::new(3, 1));
}
