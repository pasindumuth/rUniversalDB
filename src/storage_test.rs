use super::add_version;
use crate::model::common::{ColVal, ColValN, Timestamp};

#[test]
fn add_version_test() {
  let mut versions = Vec::<(Timestamp, ColValN)>::new();
  add_version(&mut versions, 10, None);
  assert_eq!(versions, vec![(10, None)]);
  add_version(&mut versions, 20, None);
  assert_eq!(versions, vec![(10, None), (20, None)]);
  add_version(&mut versions, 5, None);
  assert_eq!(versions, vec![(5, None), (10, None), (20, None)]);
  add_version(&mut versions, 15, None);
  assert_eq!(versions, vec![(5, None), (10, None), (15, None), (20, None)]);
  add_version(&mut versions, 10, Some(ColVal::Int(10)));
  assert_eq!(versions, vec![(5, None), (10, Some(ColVal::Int(10))), (15, None), (20, None)]);
}
