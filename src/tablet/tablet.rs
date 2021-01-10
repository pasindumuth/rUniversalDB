use crate::common::rand::RandGen;
use crate::model::common::{
  EndpointId, Schema, SelectQueryId, SelectView, TabletShape, Timestamp, WriteQueryId,
};
use crate::model::evalast::{Holder, WriteQueryTask};
use crate::model::message::{
  FromProp, FromRoot, NetworkMessage, SlaveMessage, TabletAction, TabletMessage, WriteQuery,
};
use crate::model::sqlast::SelectStmt;
use crate::storage::relational_tablet::RelationalTablet;

/// This struct contains a Vec of TabletActions, which should be
/// performed after a run of `handle_incoming_message`. Accumulating
/// actions to perform after-the-fact, rather than performing them
/// immediately, allows us to keep the `handle_incoming_message`
/// function pure.
#[derive(Debug)]
pub struct TabletSideEffects {
  pub actions: Vec<TabletAction>,
}

impl TabletSideEffects {
  pub fn new() -> TabletSideEffects {
    TabletSideEffects {
      actions: Vec::new(),
    }
  }

  /// Creates a `TabletSideEffects` with one element. This is a convenience
  /// function that's often used for returning errors from a function.
  pub fn from(action: TabletAction) -> TabletSideEffects {
    TabletSideEffects {
      actions: vec![action],
    }
  }

  /// Append the given action to the list of `actions`.
  pub fn add(&mut self, action: TabletAction) {
    self.actions.push(action);
  }

  /// Concatenates the actions in `other` with the current.
  pub fn append(&mut self, mut other: TabletSideEffects) {
    self.actions.append(&mut other.actions);
  }
}

/// This is a partial RelationalTablet used in intricate ways during
/// Transaction processing.
type View = RelationalTablet;

#[derive(Debug)]
struct CurrentWrite {
  wid: WriteQueryId,
  write_task: Holder<WriteQueryTask>,
}

// Usually, we use WriteQueryIds to refer to WriteQueries. This object
// is what a WriteQueryId refers to in the TabletState.
#[derive(Debug, Clone)]
struct WriteQueryMetadata {
  // The Timestamp of the write.
  timestamp: Timestamp,
  // EndpoingId of the TM for the write.
  tm_eid: EndpointId,
  // The actuay Write Query.
  write_query: WriteQuery,
}

// Usually, we use SelectQueryIds to refer to SelectQueries. This object
// is what a SelectQueryId refers to in the TabletState.
#[derive(Debug, Clone)]
struct SelectQueryMetadata {
  // The Timestamp of the select.
  timestamp: Timestamp,
  // EndpoingId of the TM for the select.
  tm_eid: EndpointId,
  // The actuay Select Query.
  select_stmt: SelectStmt,
}

#[derive(Debug)]
pub struct TabletState {}

impl TabletState {
  pub fn new(
    rand_gen: RandGen,
    this_tablet: TabletShape,
    this_slave_eid: EndpointId,
    schema: Schema,
  ) -> TabletState {
    TabletState {}
  }

  pub fn handle_incoming_message(
    &mut self,
    side_effects: &mut TabletSideEffects,
    msg: TabletMessage,
  ) {
    match &msg {
      TabletMessage::SelectPrepare(select_prepare) => {
        // side_effects.append(self.handle_select_prepare(select_prepare));
      }
      TabletMessage::WritePrepare(write_prepare) => {
        // side_effects.append(self.handle_write_prepare(write_prepare));
      }
      TabletMessage::WriteCommit(write_commit) => {
        // side_effects.append(self.handle_write_commit(write_commit));
      }
      TabletMessage::WriteAbort(write_abort) => {
        // side_effects.append(self.handle_write_abort(write_abort));
      }
      TabletMessage::SubqueryResponse(subquery_res) => {
        // side_effects.append(self.handle_subquery_response(subquery_res));
      }
    }
  }
}

fn write_aborted(
  tm_eid: &EndpointId,
  wid: &WriteQueryId,
  this_tablet: &TabletShape,
) -> TabletAction {
  TabletAction::Send {
    eid: tm_eid.clone(),
    msg: NetworkMessage::Slave(SlaveMessage::WriteAborted {
      tablet: this_tablet.clone(),
      wid: wid.clone(),
    }),
  }
}

fn write_prepared(
  tm_eid: &EndpointId,
  wid: &WriteQueryId,
  this_tablet: &TabletShape,
) -> TabletAction {
  TabletAction::Send {
    eid: tm_eid.clone(),
    msg: NetworkMessage::Slave(SlaveMessage::WritePrepared {
      tablet: this_tablet.clone(),
      wid: wid.clone(),
    }),
  }
}

fn write_committed(
  tm_eid: &EndpointId,
  wid: &WriteQueryId,
  this_tablet: &TabletShape,
) -> TabletAction {
  TabletAction::Send {
    eid: tm_eid.clone(),
    msg: NetworkMessage::Slave(SlaveMessage::WriteCommitted {
      tablet: this_tablet.clone(),
      wid: wid.clone(),
    }),
  }
}

fn select_aborted(
  tm_eid: &EndpointId,
  sid: &SelectQueryId,
  this_tablet: &TabletShape,
) -> TabletAction {
  TabletAction::Send {
    eid: tm_eid.clone(),
    msg: NetworkMessage::Slave(SlaveMessage::SelectPrepared {
      tablet: this_tablet.clone(),
      sid: sid.clone(),
      view_o: None,
    }),
  }
}

fn select_prepared(
  tm_eid: &EndpointId,
  sid: &SelectQueryId,
  this_tablet: &TabletShape,
  select_view: SelectView,
) -> TabletAction {
  TabletAction::Send {
    eid: tm_eid.clone(),
    msg: NetworkMessage::Slave(SlaveMessage::SelectPrepared {
      tablet: this_tablet.clone(),
      sid: sid.clone(),
      view_o: Some(select_view),
    }),
  }
}

enum VerificationFailure {
  /// This means that when trying to verify a CombinationStatus in a
  /// PropertyVerificationStatus, some verification failure occured,
  /// like a Table Constraint violation, or a divergent Select Query.
  VerificationFailure,
}

// Useful stuff for creating FromRoot, which is necessary stuff.
#[derive(Debug, Clone)]
enum StatusPath {
  SelectStatus { sid: SelectQueryId },
  WriteStatus { wid: WriteQueryId },
}

fn make_path(status_path: StatusPath, from_prop: FromProp) -> FromRoot {
  match status_path {
    StatusPath::SelectStatus { sid } => FromRoot::Select { sid, from_prop },
    StatusPath::WriteStatus { wid } => FromRoot::Write { wid, from_prop },
  }
}
