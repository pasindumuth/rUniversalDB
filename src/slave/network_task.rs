use crate::model::common::{EndpointId, RequestId, Row, SelectQueryId, TabletShape, TransactionId};
use crate::model::message::FromRoot;
use std::collections::HashSet;

/// Network Tasks are generally used to maintain state
/// during an operations where nodes have to send messages
/// back and forth multiple times across the network. There
/// are 2 types of Network Tasks, SelectTasks for Select
/// Queries, and WriteTasks for Write Queries.

/// Holds onto the necessary metadata of a request so that
/// it can be responded at some later point.
#[derive(Debug, Clone)]
pub enum SelectRequestMeta {
  Admin {
    /// The EndpointId of the original sender.
    origin_eid: EndpointId,
    /// The RequestId of the original request.
    rid: RequestId,
  },
  Tablet {
    /// The EndpointId of the original sender.
    origin_eid: EndpointId,
    /// The TabletShape to forward to.
    tablet: TabletShape,
    /// The path which the SubqueryResponse must be routed back
    /// to in the receiving Tablet.
    subquery_path: FromRoot,
    /// The Transaction ID of the Select Query. Select Queries
    /// sent by Tablets create a `sid` so it can track things.
    sid: SelectQueryId,
  },
}

/// This is a 1PC task for managing a Select Query. It's only
/// a single phase commit algorithm.
#[derive(Debug, Clone)]
pub struct SelectTask {
  /// The set of Tablets in the 1PC
  pub tablets: Vec<(EndpointId, TabletShape)>,
  /// The set of Tablets that haven't responded yet
  pub pending_tablets: HashSet<(EndpointId, TabletShape)>,
  /// Currently constructed View from the Tablets that
  /// already responded.
  pub view: Vec<Row>,
  /// The endpoint which the SELECT Query originated from.
  pub req_meta: SelectRequestMeta,
}

/// Holds onto the necessary metadata of a request so that
/// it can be responded at some later point.
#[derive(Debug, Clone)]
pub struct WriteRequestMeta {
  /// The EndpointId of the original sender.
  pub origin_eid: EndpointId,
  /// The RequestId of the original request.
  pub rid: RequestId,
}

/// The phase for the Write 2PC algorithm.
#[derive(Debug, Clone)]
pub enum WritePhase {
  /// The Preparing phase, where the Slave waits for the Tablets
  /// to send back a Prepared (or Aborted) message.
  Preparing,
  /// The Committing Phase, where a the Slave is waiting for all
  /// Tablets to finish Committing.
  Committing,
  /// The Aborting Phase, where a the Slave is waiting for all
  /// Tablets to finish Aborting.
  Aborting,
}

/// This is a 2PC task for managing a Write Query.
#[derive(Debug, Clone)]
pub struct WriteTask {
  /// The set of Tablets in the 2PC
  pub tablets: Vec<(EndpointId, TabletShape)>,
  /// The set of Tablets that haven't responded to the current phase yet.
  pub pending_tablets: HashSet<(EndpointId, TabletShape)>,
  /// The tablets which successfully sent a Prepare message.
  pub prepared_tablets: HashSet<(EndpointId, TabletShape)>,
  /// The current phase of the MPC.
  pub phase: WritePhase,
  /// The endpoint which the SELECT Query originated from.
  pub req_meta: WriteRequestMeta,
}
