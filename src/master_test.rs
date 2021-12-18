use crate::master::MasterState;
use crate::test_utils::CheckCtx;

// -----------------------------------------------------------------------------------------------
//  Consistency Testing
// -----------------------------------------------------------------------------------------------

pub fn check_master_clean(master: &MasterState, check_ctx: &mut CheckCtx) {
  let statuses = &master.statuses;

  check_ctx.check(statuses.create_table_tm_ess.is_empty());
  check_ctx.check(statuses.alter_table_tm_ess.is_empty());
  check_ctx.check(statuses.drop_table_tm_ess.is_empty());
  check_ctx.check(statuses.planning_ess.is_empty());
}
