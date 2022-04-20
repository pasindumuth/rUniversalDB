use super::add_version;
use crate::common::{mk_t, Timestamp};
use crate::model::common::{ColVal, ColValN};

#[test]
fn add_version_test() {
  let mut versions = Vec::<(Timestamp, ColValN)>::new();

  add_version(&mut versions, mk_t(10), None);
  assert_eq!(versions, vec![(mk_t(10), None)]);
  add_version(&mut versions, mk_t(20), None);
  assert_eq!(versions, vec![(mk_t(10), None), (mk_t(20), None)]);
  add_version(&mut versions, mk_t(5), None);
  assert_eq!(versions, vec![(mk_t(5), None), (mk_t(10), None), (mk_t(20), None)]);
  add_version(&mut versions, mk_t(15), None);
  assert_eq!(versions, vec![(mk_t(5), None), (mk_t(10), None), (mk_t(15), None), (mk_t(20), None)]);
  add_version(&mut versions, mk_t(10), Some(ColVal::Int(10)));
  assert_eq!(
    versions,
    vec![(mk_t(5), None), (mk_t(10), Some(ColVal::Int(10))), (mk_t(15), None), (mk_t(20), None)]
  );
}
