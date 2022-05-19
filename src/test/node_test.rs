use crate::master::master_test::check_master_clean;
use crate::node::{NodeState, State};
use crate::slave::slave_test::check_slave_clean;
use crate::test_utils::CheckCtx;

// -----------------------------------------------------------------------------------------------
//  Consistency Testing
// -----------------------------------------------------------------------------------------------

pub fn check_node_clean(node: &NodeState, check_ctx: &mut CheckCtx) {
  match &node.state {
    State::DNEState(_) => {}
    State::FreeNodeState(_, _) => {}
    State::NominalSlaveState(slave_state, _) => {
      check_slave_clean(&slave_state, check_ctx);
    }
    State::NominalMasterState(master_state, _) => {
      check_master_clean(&master_state, check_ctx);
    }
    State::PostExistence => {}
  }
}
