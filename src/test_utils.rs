use crate::model::common::{
  ColName, ColValue, SlaveGroupId, TablePath, TabletGroupId, TransTableName,
};

pub fn cn(s: &str) -> ColName {
  ColName(s.to_string())
}

pub fn cvs(s: &str) -> ColValue {
  ColValue::String(s.to_string())
}

pub fn cvi(i: i32) -> ColValue {
  ColValue::Int(i)
}

pub fn mk_sid(id: &str) -> SlaveGroupId {
  SlaveGroupId(id.to_string())
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
