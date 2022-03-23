use crate::model::common::{
  ColName, ColVal, EndpointId, SlaveGroupId, TablePath, TabletGroupId, TransTableName,
};
use rand::RngCore;
use rand_xorshift::XorShiftRng;

pub fn cn(s: &str) -> ColName {
  ColName(s.to_string())
}

pub fn cno(s: &str) -> Option<ColName> {
  Some(ColName(s.to_string()))
}

pub fn cvs(s: &str) -> ColVal {
  ColVal::String(s.to_string())
}

pub fn cvi(i: i32) -> ColVal {
  ColVal::Int(i)
}

pub fn cvb(b: bool) -> ColVal {
  ColVal::Bool(b)
}

pub fn mk_sid(id: &str) -> SlaveGroupId {
  SlaveGroupId(id.to_string())
}

pub fn mk_eid(id: &str) -> EndpointId {
  EndpointId(id.to_string())
}

pub fn mk_tid(id: &str) -> TabletGroupId {
  TabletGroupId(id.to_string())
}

pub fn mk_tab(table_path: &str) -> TablePath {
  TablePath(table_path.to_string())
}

pub fn mk_ttab(table_path: &str) -> TransTableName {
  TransTableName(table_path.to_string())
}

// -----------------------------------------------------------------------------------------------
//  Random
// -----------------------------------------------------------------------------------------------

pub fn mk_seed(rand: &mut XorShiftRng) -> [u8; 16] {
  let mut seed = [0; 16];
  rand.fill_bytes(&mut seed);
  seed
}

// -----------------------------------------------------------------------------------------------
//  Check Context
// -----------------------------------------------------------------------------------------------

/// This is a utility for effectively accumulating the AND result of many boolean expressions.
/// If `check` is called even once with `false` after construction, we remember this fact
/// in `cum_bool`. We do not simply use a `&mut bool` because sometimes, we want to panic
/// if the AND expression would evaluate to false (and we want to do it early).
pub struct CheckCtx {
  pub should_assert: bool,
  cum_bool: bool,
}

impl CheckCtx {
  pub fn new(should_assert: bool) -> CheckCtx {
    CheckCtx { should_assert, cum_bool: true }
  }

  pub fn check(&mut self, boolean: bool) {
    if !boolean {
      if self.should_assert {
        panic!();
      } else {
        self.cum_bool = false;
      }
    }
  }

  pub fn get_result(&self) -> bool {
    self.cum_bool
  }
}
