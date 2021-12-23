use crate::common::Timestamp;

#[test]
fn timestamp_test() {
  assert_eq!(Timestamp::add_random_fraction(vec![1, 2, 3], vec![1, 2, 3]), (0, vec![2, 4, 6]));
  assert_eq!(Timestamp::add_random_fraction(vec![128, 255], vec![127, 1]), (1, vec![0, 0]));
  assert_eq!(Timestamp::add_random_fraction(vec![255, 255], vec![1]), (1, vec![0, 255]));
  assert_eq!(Timestamp::add_random_fraction(vec![1], vec![255, 255]), (1, vec![0, 255]));

  assert_eq!(
    Timestamp::new(1, vec![1]).add(Timestamp::new(1, vec![255, 255])),
    Timestamp::new(3, vec![0, 255])
  );
}
