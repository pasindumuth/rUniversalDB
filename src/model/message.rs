use crate::model::common::{ColumnValue, EndpointId, PrimaryKey, Row, TabletPath, TabletShape};
use serde::{Deserialize, Serialize};

/// These are PODs that are used for Threads to communicate with
/// each other. This includes communication over the network, as
/// well as across threads on the same machine.

// -------------------------------------------------------------------------------------------------
//  Admin messages
// -------------------------------------------------------------------------------------------------

/// Client Request
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientRequest {}

/// Client Response
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientResponse {}

/// Client Message
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientMessage {
  Request(AdminRequest),
  Response(AdminResponse),
}

// -------------------------------------------------------------------------------------------------
//  Admin messages
// -------------------------------------------------------------------------------------------------
/// These are message send by the Admin, for debugging,
/// development, and testing.

/// Admin Request
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AdminRequest {
  Insert {
    path: TabletPath,
    key: PrimaryKey,
    value: Vec<Option<ColumnValue>>,
  },
  Read {
    path: TabletPath,
    key: PrimaryKey,
  },
}

/// Admin Response
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AdminResponse {
  Insert { result: Result<(), String> },
  Read { result: Result<Option<Row>, String> },
}

/// Admin Message
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AdminMessage {
  Request(AdminRequest),
  Response(AdminResponse),
}

// -------------------------------------------------------------------------------------------------
//  Slave Message
// -------------------------------------------------------------------------------------------------

/// Message that go into the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SlaveMessage {
  Client(ClientMessage),
  Admin(AdminMessage),
}

// -------------------------------------------------------------------------------------------------
//  Miscellaneous
// -------------------------------------------------------------------------------------------------

/// Message that go into the Tablet's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TabletMessage {
  Input { eid: EndpointId, msg: String },
}

/// Message that come out of the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SlaveAction {
  Forward {
    shape: TabletShape,
    msg: TabletMessage,
  },
  Send {
    /// Endpoint to send the message to.
    eid: EndpointId,
    /// The message to send.
    msg: SlaveMessage,
  },
}

/// Message that come out of the Tablet's handler
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TabletAction {
  Send { eid: EndpointId, msg: SlaveMessage },
}
