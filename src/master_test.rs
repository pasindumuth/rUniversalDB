use crate::master::MasterState;

// -----------------------------------------------------------------------------------------------
//  Consistency Testing
// -----------------------------------------------------------------------------------------------

pub fn assert_master_clean(master: &MasterState) {
  let statuses = &master.statuses;

  assert!(statuses.create_table_tm_ess.is_empty());
  assert!(statuses.alter_table_tm_ess.is_empty());
  assert!(statuses.drop_table_tm_ess.is_empty());
  assert!(statuses.planning_ess.is_empty());
}
