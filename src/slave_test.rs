use crate::slave::SlaveState;
use crate::test_utils::CheckCtx;

// -----------------------------------------------------------------------------------------------
//  Consistency Testing
// -----------------------------------------------------------------------------------------------

pub fn check_slave_clean(slave: &SlaveState, check_ctx: &mut CheckCtx) {
  let statuses = &slave.statuses;

  check_ctx.check(statuses.create_table_ess.is_empty());
}
