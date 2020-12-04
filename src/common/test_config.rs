use crate::model::common::{EndpointId, TabletKeyRange, TabletPath, TabletShape};
use std::collections::HashMap;

pub fn endpoint(eid: &str) -> EndpointId {
    EndpointId(String::from(eid))
}

pub fn table_shape(path: &str, start: Option<&str>, end: Option<&str>) -> TabletShape {
    TabletShape {
        path: TabletPath {
            path: String::from(path),
        },
        range: TabletKeyRange {
            start: start.map(|start| String::from(start)),
            end: end.map(|end| String::from(end)),
        },
    }
}
