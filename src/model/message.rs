use crate::model::common::{EndpointId, TabletShape};
use serde::{Deserialize, Serialize};

// Message that go into the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SlaveMessage {
    Client(String),
    Admin(String),
}

// Message that go into the Tablet's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TabletMessage {
    Input(EndpointId, String),
}

// Message that come out of the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SlaveActions {
    Forward(TabletShape, TabletMessage),
    Send(EndpointId, SlaveMessage),
}

// Message that come out of the Tablet's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TabletActions {
    Send(EndpointId, SlaveMessage),
}
