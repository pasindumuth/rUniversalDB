use crate::model::common::{EndpointId, TabletShape};
use serde::{Deserialize, Serialize};

// Message that go into the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AdminMessage {
    Insert { key: String, value: String },
}

// Message that go into the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SlaveMessage {
    Client { msg: String },
    Admin { msg: String },
}

// Message that go into the Tablet's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TabletMessage {
    Input { eid: EndpointId, msg: String },
}

// Message that come out of the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SlaveActions {
    Forward {
        shape: TabletShape,
        msg: TabletMessage,
    },
    Send {
        eid: EndpointId,
        msg: SlaveMessage,
    },
}

// Message that come out of the Tablet's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TabletActions {
    Send { eid: EndpointId, msg: SlaveMessage },
}
