use crate::expression::does_col_regions_intersect;
use crate::test_utils::cn;

#[test]
fn does_col_regions_intersect_test() {
  let cols1 = vec![cn("c1"), cn("c2")];
  let cols2 = vec![cn("c2"), cn("c3")];
  let cols3 = vec![cn("c4")];
  let cols4 = vec![];
  assert!(does_col_regions_intersect(&cols1, &cols2));
  assert!(does_col_regions_intersect(&cols2, &cols1));
  assert!(!does_col_regions_intersect(&cols1, &cols3));
  assert!(!does_col_regions_intersect(&cols3, &cols1));
  assert!(!does_col_regions_intersect(&cols3, &cols4));
  assert!(!does_col_regions_intersect(&cols4, &cols3));
}
