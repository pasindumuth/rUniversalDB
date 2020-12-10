use crate::common::rand::RandGen;
use crate::model::common::{
  PrimaryKey, Row, Schema, SelectQueryId, TabletShape, Timestamp, WriteQueryId,
};
use crate::model::evalast::EvalSqlStmt;
use crate::model::message::{
  AdminMessage, AdminRequest, AdminResponse, NetworkMessage, SelectPrepare, SlaveMessage,
  TabletAction, TabletMessage,
};
use crate::storage::relational_tablet::RelationalTablet;
use std::collections::{HashMap, HashSet};

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

  /// Append the given action to the list of `actions`.
  pub fn add(&mut self, action: TabletAction) {
    self.actions.push(action);
  }
}

/// This is a partial RelationalTablet used in intricate ways during
/// Transaction processing.
type View = RelationalTablet;

/// This is used for evaluating a SQL statement asynchronously,
/// holding the necessary metadata at all points.
#[derive(Debug)]
struct EvalSqlTask {
  eval_sql_stmt: EvalSqlStmt,
  pending_subqueries: HashSet<SelectQueryId>,
  complete_subqueries: HashMap<SelectQueryId, View>,
}

#[derive(Debug)]
struct CombinationStatus {
  combo: Vec<WriteQueryId>,
  /// Fields controlling verification of the Write Queries only.
  write_views: HashMap<WriteQueryId, View>,
  current_write: Option<(WriteQueryId, View)>,
  write_row_tasks: HashMap<WriteQueryId, HashMap<PrimaryKey, EvalSqlTask>>,

  /// Fields controlling verification of the Select Queries only.
  selects: HashMap<Timestamp, Vec<SelectQueryId>>,
  select_views: HashMap<SelectQueryId, Vec<View>>,
  select_row_tasks: HashMap<SelectQueryId, (View, HashMap<PrimaryKey, EvalSqlTask>)>,
}

/// This is used to control
#[derive(Debug)]
struct PropertyVerificationStatus {
  /// Maps a single combination to the CombinationStatus object
  /// managing the verification for it.
  combos: HashMap<Vec<WriteQueryId>, CombinationStatus>,
  selects: Vec<SelectQueryId>,
  select_views: HashMap<SelectQueryId, Vec<View>>,
  combos_verified: HashSet<Vec<WriteQueryId>>,
}

#[derive(Debug)]
pub struct TabletState {
  pub rand_gen: RandGen,
  pub this_shape: TabletShape,
  pub relational_tablet: RelationalTablet,

  /// These are the fields for doing Write Queries.
  non_reached_writes: HashMap<Timestamp, WriteQueryId>,
  already_reached_writes: HashMap<Timestamp, WriteQueryId>,
  committed_writes: HashMap<Timestamp, WriteQueryId>,
  writes_being_verified: HashMap<WriteQueryId, PropertyVerificationStatus>,

  /// These are the fields for doing Select Queries.
  non_reached_selects: HashMap<Timestamp, SelectQueryId>,
  already_reached_selects: HashMap<Timestamp, SelectQueryId>,
  selects_being_verified: HashMap<SelectQueryId, PropertyVerificationStatus>,
}

impl TabletState {
  pub fn new(rand_gen: RandGen, this_shape: TabletShape, schema: Schema) -> TabletState {
    TabletState {
      rand_gen,
      this_shape,
      relational_tablet: RelationalTablet::new(schema),
      non_reached_writes: Default::default(),
      already_reached_writes: Default::default(),
      committed_writes: Default::default(),
      writes_being_verified: Default::default(),
      non_reached_selects: Default::default(),
      already_reached_selects: Default::default(),
      selects_being_verified: Default::default(),
    }
  }

  pub fn handle_incoming_message(
    &mut self,
    side_effects: &mut TabletSideEffects,
    msg: TabletMessage,
  ) {
    match &msg {
      TabletMessage::ClientRequest { .. } => {
        panic!("Client messages not supported yet.");
      }
      TabletMessage::AdminRequest { eid, req } => match req {
        AdminRequest::Insert {
          rid,
          key,
          value,
          timestamp,
          ..
        } => {
          let row = Row {
            key: key.clone(),
            val: value.clone(),
          };
          let result = self.relational_tablet.insert_row(&row, *timestamp);
          side_effects.add(TabletAction::Send {
            eid: eid.clone(),
            msg: NetworkMessage::Admin(AdminMessage::AdminResponse {
              res: AdminResponse::Insert {
                rid: rid.clone(),
                result,
              },
            }),
          });
        }
        AdminRequest::Read {
          rid,
          key,
          timestamp,
          ..
        } => {
          let result = self.relational_tablet.read_row(&key, *timestamp);
          side_effects.add(TabletAction::Send {
            eid: eid.clone(),
            msg: NetworkMessage::Admin(AdminMessage::AdminResponse {
              res: AdminResponse::Read {
                rid: rid.clone(),
                result,
              },
            }),
          });
        }
        AdminRequest::SqlQuery { .. } => {
          panic!("Tablets should not get SQL statements {:?}.", msg);
        }
      },
      TabletMessage::SelectPrepare(_) => {
        panic!("SelectPrepare is not supported yet.");
      }
      TabletMessage::WritePrepare(_) => {
        panic!("WritePrepare is not supported yet.");
      }
      TabletMessage::WriteCommit(_) => {
        panic!("WriteCommit is not supported yet.");
      }
      TabletMessage::WriteAbort(_) => {
        panic!("WriteAbort is not supported yet.");
      }
      TabletMessage::SubqueryResponse(_) => {
        panic!("SubqueryResponse is not supported yet.");
      }
    }
  }
}
