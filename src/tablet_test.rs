use crate::model::common::{PrimaryKey, TabletKeyRange};
use crate::tablet::check_range_inclusion;
use crate::test_utils::{cvb, cvi, cvs};

#[test]
fn test_key_comparison() {
  assert!(
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
      == PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
  );

  assert!(
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
      < PrimaryKey { cols: vec![cvi(3), cvs("a"), cvb(false)] }
  );

  assert!(
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
      < PrimaryKey { cols: vec![cvi(2), cvs("b"), cvb(false)] }
  );

  assert!(
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
      < PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(true)] }
  );
}
