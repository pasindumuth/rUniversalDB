use crate::model::common::{
  EndpointId, PrimaryKey, RequestId, Row, SelectQueryId, SelectView, TabletShape, Timestamp,
  TransactionId, WriteQueryId,
};
use serde::{Deserialize, Serialize};

/// These are PODs that are used for Threads to communicate with
/// each other. This includes communication over the network, as
/// well as across threads on the same machine.
///
/// The model we use is that each Thread Type has its own Thread Message
/// ADT. We have several Thread Types, including Client, Admin, Slave,
/// and Tablet.
///
/// Sometimes, one Thread Type A wants to send another Thread Type B a
/// message of type C. However, it must do it through Thread Type D. To
/// do this, we create a message E, called a Forwarding Wrapper, which
/// just holds the Message C, plus some extra routing information for
/// Thread E to forward the message to Thread B. We suffix such messages
/// with FW.

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
  SqlQuery {
    rid: RequestId,
    result: Result<SelectView, String>,
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
  SqlQuery {
    rid: RequestId,
    tid: TransactionId,
    // sql: SqlStmt,
    timestamp: Timestamp,
  },
}

/// Client Request
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {}

/// Message that go into the Slave's handler
#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SlaveMessage {
  ClientRequest {
    req: ClientRequest,
  },
  AdminRequest {
    req: AdminRequest,
  },
  /// A Select Multi-Phase Commit (MPC) is done by having a Slave
  /// Thread be the Transaction Coordinator and Tablets (generally
  /// on different network nodes) be the Workers. The MPC is just
  /// a single Phase, where Prepare also Commits. There is no harm
  /// in one Tablet Committing while other's abort (unlike in the
  /// case of a Write MPC).
  SelectPrepared {
    /// Tablet Shape of the sending Tablet Thread
    tablet: TabletShape,
    /// Transaction ID
    sid: SelectQueryId,
    /// The View returned for the Select Query.
    /// `None` if the Partial Query failed.
    view_o: Option<SelectView>,
  },
  /// The Prepare Response for the Write Query 2PC algorithm, sent
  /// from Tablets Threads (RMs) to the Slave Thread (TM)
  WritePrepared {
    /// Tablet Shape of the sending Tablet Thread
    tablet: TabletShape,
    /// Transaction ID
    wid: WriteQueryId,
  },
  /// The Commit Response for the Write Query 2PC algorithm.
  WriteCommitted {
    /// Tablet Shape of the sending Tablet Thread
    tablet: TabletShape,
    /// Transaction ID of the Write Query.
    wid: WriteQueryId,
  },
  /// The Aborted message for the Write Query 2PC algorithm. This can be
  /// sent in place of a WritePrepared message, but is always sent as the
  /// response for when the TM send out an Abort message to the RMs.
  WriteAborted {
    /// Tablet Shape of the sending Tablet Thread
    tablet: TabletShape,
    /// Transaction ID of the Write Query.
    wid: WriteQueryId,
  },
  /// This is a request to perform a subquery. This is simply a Select
  /// Query, but it was sent by a Tablet.
  SubqueryRequest {
    /// Tablet Shape of the sending Tablet.
    tablet: TabletShape,
    sid: SelectQueryId,
    subquery_path: FromRoot,
    // select_stmt: SelectStmt,
    timestamp: Timestamp,
  },
  /// A Forwarding Wrapper for a Tablet's SelectPrepare.
  FW_SelectPrepare {
    tablet: TabletShape,
    msg: SelectPrepare,
  },
  /// A Forwarding Wrapper for a Tablet's WritePrepare.
  FW_WritePrepare {
    tablet: TabletShape,
    msg: WritePrepare,
  },
  /// A Forwarding Wrapper for a Tablet's WriteCommit.
  FW_WriteCommit {
    tablet: TabletShape,
    msg: WriteCommit,
  },
  /// A Forwarding Wrapper for a Tablet's WriteAbort.
  FW_WriteAbort {
    tablet: TabletShape,
    msg: WriteAbort,
  },
  /// A Forwarding Wrapper for a Tablet's SubqueryRequest.
  FW_SubqueryResponse {
    tablet: TabletShape,
    msg: SubqueryResponse,
  },
}

// -------------------------------------------------------------------------------------------------
//  Tablet Thread Message
// -------------------------------------------------------------------------------------------------

/// The Prepare message sent from a Slave (the TM) to a Tablet
/// (an RM) during the Select Query 1PC algorithm.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SelectPrepare {
  pub tm_eid: EndpointId,
  pub sid: SelectQueryId,
  // pub select_query: SelectStmt,
  pub timestamp: Timestamp,
}

/// Holds the ASTs for all Write-related queries, namely
/// Insert, Update, and Delete SQL statements.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum WriteQuery {
  // Update(UpdateStmt),
// Insert(InsertStmt),
}

/// The Prepare message sent from a Slave (the TM) to a Tablet
/// (an RM) during the Write Query 2PC algorithm.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WritePrepare {
  pub tm_eid: EndpointId,
  pub wid: WriteQueryId,
  pub write_query: WriteQuery,
  pub timestamp: Timestamp,
}

/// The Commit message sent from a Slave (the TM) to a Tablet
/// (an RM) during the Write Query 2PC algorithm.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WriteCommit {
  pub tm_eid: EndpointId,
  pub wid: WriteQueryId,
}

/// The Abort message sent from a Slave (the TM) to a Tablet
/// (an RM) during the Write Query 2PC algorithm.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct WriteAbort {
  pub tm_eid: EndpointId,
  pub wid: WriteQueryId,
}

/// A Subquery response sent from the Slave executing it to the
/// Tablet which requested it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SubqueryResponse {
  pub sid: SelectQueryId,
  pub subquery_path: FromRoot,
  pub result: Result<SelectView, String>,
}

/// Message that go into the Tablet's handler.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TabletMessage {
  SelectPrepare(SelectPrepare),
  WritePrepare(WritePrepare),
  WriteCommit(WriteCommit),
  WriteAbort(WriteAbort),
  SubqueryResponse(SubqueryResponse),
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
    tablet: TabletShape,
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

/// This describes the EvalTask the Subquery is from. This
/// is useful for routing the response back to the source.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FromRoot {
  Write {
    wid: WriteQueryId,
    from_prop: FromProp,
  },
  Select {
    sid: SelectQueryId,
    from_prop: FromProp,
  },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FromProp {
  pub combo: Vec<WriteQueryId>,
  pub from_combo: FromCombo,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FromCombo {
  Write {
    wid: WriteQueryId,
    from_task: FromWriteTask,
  },
  Select {
    sid: SelectQueryId,
    from_task: FromSelectTask,
  },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FromWriteTask {
  UpdateTask {
    key: PrimaryKey,
  },
  InsertTask {
    /// This index is the position of the expresion in the VALUES clause
    /// that the Subquery is destined to.
    index: usize,
  },
  InsertSelectTask,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FromSelectTask {
  SelectTask { key: PrimaryKey },
}
