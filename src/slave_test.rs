use crate::slave::SlaveState;

// -----------------------------------------------------------------------------------------------
//  Consistency Testing
// -----------------------------------------------------------------------------------------------

pub fn assert_slave_clean(slave: &SlaveState) {
  let statuses = &slave.statuses;

  assert!(statuses.create_table_ess.is_empty());
}
