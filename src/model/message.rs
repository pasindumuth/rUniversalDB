use crate::model::common::{
  ColumnValue, EndpointId, PrimaryKey, RequestId, Row, TabletPath, TabletShape, Timestamp,
};
use serde::{Deserialize, Serialize};

/// These are PODs that are used for Threads to communicate with
/// each other. This includes communication over the network, as
/// well as across threads on the same machine.
///
/// The model we use is that each Thread Type has its own Thread Message
/// ADT. We have several Thread Types, including Client, Admin, Slave,
/// and Tablet.

// -------------------------------------------------------------------------------------------------
// The Client Thread Message
// -------------------------------------------------------------------------------------------------

/// Client Message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {}

// -------------------------------------------------------------------------------------------------
//  Admin Thread Message
// -------------------------------------------------------------------------------------------------
/// The Admin Thread is used for system administrators to do back-door
/// operations on the rUniversal.

/// Admin Response
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AdminResponse {
  Insert {
    rid: RequestId,
    result: Result<(), String>,
  },
  Read {
    rid: RequestId,
    result: Result<Option<Row>, String>,
  },
}

/// Admin Message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AdminMessage {
  AdminResponse { res: AdminResponse },
}

// -------------------------------------------------------------------------------------------------
//  Slave Thread Message
// -------------------------------------------------------------------------------------------------

/// Admin Request
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum AdminRequest {
  Insert {
    rid: RequestId,
    path: TabletPath,
    key: PrimaryKey,
    value: Vec<Option<ColumnValue>>,
    timestamp: Timestamp,
  },
  Read {
    rid: RequestId,
    path: TabletPath,
    key: PrimaryKey,
    timestamp: Timestamp,
  },
}

/// Client Request
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {}

/// Message that go into the Slave's handler
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveMessage {
  AdminRequest { req: AdminRequest },
  ClientRequest { req: ClientRequest },
}

// -------------------------------------------------------------------------------------------------
//  Tablet Thread Message
// -------------------------------------------------------------------------------------------------

/// Message that go into the Tablet's handler.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TabletMessage {
  AdminRequest { eid: EndpointId, req: AdminRequest },
  ClientRequest { eid: EndpointId, req: ClientRequest },
}

// -------------------------------------------------------------------------------------------------
//  Actions
// -------------------------------------------------------------------------------------------------
/// These messages aren't like the others. These are the actions returned
/// by the thread's Handle Functions, which are pure functions. These actions
/// are effectively how the Handle Functions do side-effects without actually
/// doing it in the function.

/// Message that come out of the Slave's handler
#[derive(Debug)]
pub enum SlaveAction {
  Forward {
    shape: TabletShape,
    msg: TabletMessage,
  },
  Send {
    eid: EndpointId,
    msg: NetworkMessage,
  },
}

/// Message that come out of the Tablet's handler
#[derive(Debug)]
pub enum TabletAction {
  Send {
    eid: EndpointId,
    msg: NetworkMessage,
  },
}

// -------------------------------------------------------------------------------------------------
//  Miscellaneous
// -------------------------------------------------------------------------------------------------

/// A union of all messages that would need to be transferred over the
/// network. We don't send ClientMessages directly to the client because if we
/// introduce a bug where we accidentally send an Admin message to the client,
/// then that bug might not get caught; deserialization might work.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum NetworkMessage {
  Admin(AdminMessage),
  Client(ClientMessage),
  Slave(SlaveMessage),
}
