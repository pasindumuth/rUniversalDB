use crate::model::common::{EndpointId, RequestId, Row, TabletShape};
use std::collections::HashSet;

/// Network Tasks are generally used to maintain state
/// during an operations where nodes have to send messages
/// back and forth multiple times across the network.

/// Holds onto the necessary metadata of a request so that it can
/// be responded long after the request was deleted from memory.
#[derive(Debug, Clone)]
pub struct RequestMeta {
  /// The EndpointId of the original sender.
  pub origin_eid: EndpointId,
  /// The RequestId of the original request.
  pub rid: RequestId,
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
  pub req_meta: RequestMeta,
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
  pub req_meta: RequestMeta,
}

/// These are network tasks that are managed by the Slave
#[derive(Debug, Clone)]
pub enum NetworkTask {
  Select(SelectTask),
  Write(WriteTask),
}
