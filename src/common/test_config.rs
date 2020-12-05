use crate::model::common::{
  ColumnValue, EndpointId, PrimaryKey, TabletKeyRange, TabletPath, TabletShape,
};

pub fn endpoint(eid: &str) -> EndpointId {
  EndpointId(String::from(eid))
}

pub fn table_shape(path: &str, start: Option<&str>, end: Option<&str>) -> TabletShape {
  TabletShape {
    path: TabletPath {
      path: String::from(path),
    },
    range: TabletKeyRange {
      start: start.map(|start| PrimaryKey {
        cols: vec![ColumnValue::String(String::from(start))],
      }),
      end: end.map(|end| PrimaryKey {
        cols: vec![ColumnValue::String(String::from(end))],
      }),
    },
  }
}
