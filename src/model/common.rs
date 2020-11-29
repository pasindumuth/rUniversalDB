use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EndpointId {
    pub e_id: String,
}

impl EndpointId {
    pub fn from(e_id: String) -> EndpointId {
        EndpointId { e_id: e_id }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TabletPath {
    pub path: String,
}

impl TabletPath {
    pub fn from(path: String) -> TabletPath {
        TabletPath { path: path }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TabletKeyRange {
    pub start: Option<String>,
    pub end: Option<String>,
}

impl TabletKeyRange {
    pub fn from(start: Option<String>, end: Option<String>) -> TabletKeyRange {
        TabletKeyRange { start, end }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TabletShape {
    pub path: TabletPath,
    pub range: TabletKeyRange,
}

impl TabletShape {
    pub fn from(path: TabletPath, range: TabletKeyRange) -> TabletShape {
        TabletShape { path, range }
    }
}
