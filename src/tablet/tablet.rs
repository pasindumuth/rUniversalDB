use crate::common::rand::RandGen;
use crate::model::common::{
  ColumnName, ColumnValue, EndpointId, PrimaryKey, Row, Schema, SelectQueryId, TabletPath,
  TabletShape, Timestamp, TransactionId, WriteQueryId,
};
use crate::model::evalast::{
  EvalBinaryOp, EvalLiteral, EvalSelect, EvalSelectStmt1, EvalSelectStmt2, EvalSelectStmt3,
  EvalUpdate, EvalUpdateStmt1, EvalUpdateStmt2, EvalUpdateStmt3, EvalUpdateStmt4, PostEvalExpr,
  PreEvalExpr,
};
use crate::model::message::{
  AdminMessage, AdminRequest, AdminResponse, NetworkMessage, SelectPrepare, SlaveMessage,
  SubqueryPathLeg1, SubqueryPathLeg2, SubqueryPathLeg3, SubqueryResponse, TabletAction,
  TabletMessage, WritePrepare, WriteQuery,
};
use crate::model::sqlast::ValExpr::{BinaryExpr, Subquery};
use crate::model::sqlast::{BinaryOp, Literal, SelectStmt, SqlStmt, UpdateStmt, ValExpr};
use crate::storage::relational_tablet::RelationalTablet;
use crate::tablet::tablet::EvalErr::ColumnDNE;
use rand::Rng;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::io::Write;
use std::iter::FromIterator;
use std::ops::Bound::Excluded;
use std::ops::Bound::Included;
use std::ops::Bound::Unbounded;
use std::ptr::replace;

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
type SelectView = BTreeMap<PrimaryKey, Vec<Option<ColumnValue>>>;

/// This is used for evaluating a SQL statement asynchronously,
/// holding the necessary metadata at all points.
#[derive(Debug)]
struct EvalUpdateTask {
  eval_expr: EvalUpdate,
  pending_subqueries: BTreeMap<SelectQueryId, SelectStmt>,
  complete_subqueries: BTreeMap<SelectQueryId, Vec<Row>>,
}

/// This is used for evaluating a SQL statement asynchronously,
/// holding the necessary metadata at all points.
#[derive(Debug)]
struct EvalSelectTask {
  eval_expr: EvalSelect,
  pending_subqueries: BTreeMap<SelectQueryId, SelectStmt>,
  complete_subqueries: BTreeMap<SelectQueryId, Vec<Row>>,
}

/// What's going on? Okay, so what does a subquery result look like?
/// Well, when we do selects for this tablet, we usually select a
/// subuset of cols. but if we consider it rather Map<PrimaryKey, Vec<Option<ColumnValue>>>,
/// then we can do a proper comparison for selects and actually see if theyre the same.

#[derive(Debug)]
struct CombinationStatus {
  combo: Vec<WriteQueryId>,
  /// Fields controlling verification of the Write Queries only.
  write_view: View,
  current_write: Option<WriteQueryId>,
  write_row_tasks: HashMap<PrimaryKey, EvalUpdateTask>,

  /// Fields controlling verification of the Select Queries only.
  select_row_tasks: HashMap<SelectQueryId, (SelectView, HashMap<PrimaryKey, EvalSelectTask>)>,
}

/// This is used to control
#[derive(Debug)]
struct PropertyVerificationStatus {
  /// Maps a single combination to the CombinationStatus object
  /// managing the verification for it.
  combo_status_map: HashMap<Vec<WriteQueryId>, CombinationStatus>,
  selects: BTreeMap<Timestamp, Vec<SelectQueryId>>,
  select_views: HashMap<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>,
  combos_verified: HashSet<Vec<WriteQueryId>>,
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
pub struct TabletState {
  pub rand_gen: RandGen,
  pub this_tablet: TabletShape,
  pub this_slave_eid: EndpointId,
  pub relational_tablet: RelationalTablet,

  /// This is the repository of Write Queries. All WriteQueryIds
  /// refer to a WriteQuery that's registered here. Conversely, all
  /// WriteQueryIds that appear here should appear in one of the
  /// other fields. We also store other metadata associated with the
  /// Write Query, name the timestamp and the TM eid is managing the
  /// transaction.
  write_query_map: HashMap<WriteQueryId, WriteQueryMetadata>,
  /// Similarly, this the repository of all Select Queries.
  select_query_map: HashMap<SelectQueryId, SelectQueryMetadata>,

  /// Below, we hold the core Transaction data. Note that we use
  /// a BTreeMap for maps that are keyed by timestamps, since we
  /// often need to do range queries on them.

  /// These are the fields for doing Write Queries.
  non_reached_writes: BTreeMap<Timestamp, WriteQueryId>,
  already_reached_writes: BTreeMap<Timestamp, WriteQueryId>,
  committed_writes: BTreeMap<Timestamp, WriteQueryId>,
  writes_being_verified: HashMap<WriteQueryId, PropertyVerificationStatus>,

  /// These are the fields for doing Select Queries.
  non_reached_selects: BTreeMap<Timestamp, Vec<SelectQueryId>>,
  already_reached_selects: BTreeMap<Timestamp, Vec<SelectQueryId>>,
  selects_being_verified: HashMap<SelectQueryId, PropertyVerificationStatus>,
}

impl TabletState {
  pub fn new(
    rand_gen: RandGen,
    this_tablet: TabletShape,
    this_slave_eid: EndpointId,
    schema: Schema,
  ) -> TabletState {
    TabletState {
      rand_gen,
      this_tablet,
      this_slave_eid,
      relational_tablet: RelationalTablet::new(schema),
      write_query_map: Default::default(),
      select_query_map: Default::default(),
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
      TabletMessage::SelectPrepare(SelectPrepare {
        tm_eid,
        sid,
        select_query,
        timestamp,
      }) => {
        panic!("SelectPrepare is not supported yet.");
        // now what? Well, I should probably start implementing....
        // this. SelectPrepare? Nah, implement writing first, idk.
        // it just feels like it makes more sense. Just do updates.
        // The timestamp should be distinct from all other writes.
      }
      TabletMessage::WritePrepare(write_prepare) => {
        side_effects.append(self.handle_write_prepare(write_prepare));
      }
      TabletMessage::WriteCommit(_) => {
        panic!("WriteCommit is not supported yet.");
      }
      TabletMessage::WriteAbort(_) => {
        panic!("WriteAbort is not supported yet.");
      }
      TabletMessage::SubqueryResponse(subquery_res) => {
        side_effects.append(self.handle_subquery_response(subquery_res));
      }
    }
  }

  /// This was taken out into it's own function so that we can handle the failure
  /// case easily (rather than mutating the main side-effects variable, we just
  /// return one instead).
  fn handle_write_prepare(&mut self, write_prepare: &WritePrepare) -> TabletSideEffects {
    let WritePrepare {
      tm_eid,
      wid,
      write_query,
      timestamp,
    } = write_prepare;
    let mut side_effects = TabletSideEffects::new();
    if self.non_reached_writes.contains_key(timestamp)
      || self.already_reached_writes.contains_key(timestamp)
      || self.committed_writes.contains_key(timestamp)
    {
      // For now, we disallow Write Queries with the same timestamp
      // from being committed in the same Tablet. This eliminates
      // the ambiguitiy of which Write Query to apply first, and
      // it would create complications when trying to do multi-stage
      // transactions later on anyways.
      side_effects.add(TabletAction::Send {
        eid: tm_eid.clone(),
        msg: NetworkMessage::Slave(SlaveMessage::WriteAborted {
          tablet: self.this_tablet.clone(),
          wid: wid.clone(),
        }),
      });
    }

    // NOTE: The pattern I'm using here, when creating a default version for a complex
    // state structure, is that I need to be able to build up these structure incrementally.
    // I can't be juggling like 40 variables and then assemble them in the end, hoping that
    // they have all the desired properties. It's better to start off with a default struct.
    // Every time this struct changes, we maintain some subset of properties. Then, we can
    // audit the code to make sure that by the end of this code, all properties for this
    // struct (as specified in our high-level design) is met.
    let mut status = PropertyVerificationStatus {
      combo_status_map: HashMap::<Vec<WriteQueryId>, CombinationStatus>::new(),
      selects: BTreeMap::<Timestamp, Vec<SelectQueryId>>::new(),
      select_views: HashMap::<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>::new(),
      combos_verified: HashSet::<Vec<WriteQueryId>>::new(),
    };
    status.selects = self.already_reached_selects.clone();
    self.write_query_map.insert(
      wid.clone(),
      WriteQueryMetadata {
        timestamp: timestamp.clone(),
        tm_eid: tm_eid.clone(),
        write_query: write_query.clone(),
      },
    );

    // Construct the `combos` and update `combo_status_map` accordingly
    for mut combo_as_map in create_combos(&self.committed_writes, &self.already_reached_writes) {
      // This code block constructions the CombinationStatus.
      combo_as_map.insert(timestamp.clone(), wid.clone());
      if let Ok(effects) = add_combo(
        &mut self.rand_gen,
        &mut status,
        StatusPath::WriteStatus { wid: wid.clone() },
        &combo_as_map.values().cloned().collect(),
        &self.relational_tablet,
        &self.write_query_map,
        &self.select_query_map,
        &self.this_slave_eid,
        &self.this_tablet,
      ) {
        side_effects.append(effects);
      } else {
        // The verification failed, so abort the write and clean up all
        // data structures where the write appears.
        side_effects.add(write_aborted(tm_eid, wid, &self.this_tablet));
        self.write_query_map.remove(wid);
      }
    }

    if status.combos_verified.len() == status.combo_status_map.len() {
      // We managed to verify all combos in the PropertyVerificationStatus.
      // This means we are finished the transaction. So, add to
      // already_reached_writes and update all associated
      // PropertyVerificationStatuses accordingly.
      self
        .already_reached_writes
        .insert(timestamp.clone(), wid.clone());
      // Send a response back to the TM.
      side_effects.add(write_prepared(tm_eid, wid, &self.this_tablet));
      side_effects.append(add_new_combos(
        &mut self.rand_gen,
        &mut self.non_reached_writes,
        &mut self.writes_being_verified,
        &mut self.write_query_map,
        &self.already_reached_writes,
        &self.committed_writes,
        &self.relational_tablet,
        &self.select_query_map,
        &self.this_slave_eid,
        &self.this_tablet,
      ));
    } else {
      // The PropertyVerificationStatus is not done. So add it
      // to non_reached_writes.
      self
        .non_reached_writes
        .insert(timestamp.clone(), wid.clone());
      self.writes_being_verified.insert(wid.clone(), status);
    }
    return side_effects;
  }

  /// This was taken out into it's own function so that we can handle the failure
  /// case easily (rather than mutating the main side-effects variable, we just
  /// return one instead).
  fn handle_subquery_response(&mut self, subquery_res: &SubqueryResponse) -> TabletSideEffects {
    let SubqueryResponse {
      sid,
      subquery_path,
      result,
    } = subquery_res;
    let mut side_effects = TabletSideEffects::new();
    (|| -> Option<()> {
      match subquery_path {
        SubqueryPathLeg1::Write {
          wid: orig_wid,
          leg2,
        } => {
          let status = self.writes_being_verified.get_mut(orig_wid)?;
          let mut combo_status = status.combo_status_map.get_mut(&leg2.combo)?;
          match &leg2.leg3 {
            SubqueryPathLeg3::Write { wid: cur_wid, key } => {
              if combo_status.current_write.as_ref() == Some(cur_wid) {
                let write_meta = self.write_query_map.get(cur_wid).unwrap().clone();
                let task = combo_status.write_row_tasks.get_mut(key)?;
                task.pending_subqueries.remove(sid)?;
                let subquery_ret = if let Ok(subquery_ret) = result.clone() {
                  subquery_ret
                } else {
                  // Here, we need to abort the whole transaction. Get rid of the
                  // the whole PropertyVerificationStatus.
                  self.non_reached_writes.remove(&write_meta.timestamp);
                  self.writes_being_verified.remove(orig_wid);
                  self.write_query_map.remove(orig_wid);
                  side_effects.add(write_aborted(
                    &write_meta.tm_eid,
                    orig_wid,
                    &self.this_tablet,
                  ));
                  return Some(());
                };
                task.complete_subqueries.insert(sid.clone(), subquery_ret);
                if task.pending_subqueries.is_empty() {
                  // Continue processing the EvalTask
                  let (eval_update, context) = if let Ok(eval_update) = eval_update_graph(
                    &mut self.rand_gen,
                    task.eval_expr.clone(),
                    task.complete_subqueries.clone(),
                  ) {
                    eval_update
                  } else {
                    // Here, we need to abort the whole transaction. Get rid of the
                    // the whole PropertyVerificationStatus.
                    self.non_reached_writes.remove(&write_meta.timestamp);
                    self.writes_being_verified.remove(orig_wid);
                    self.write_query_map.remove(orig_wid);
                    side_effects.add(write_aborted(
                      &write_meta.tm_eid,
                      orig_wid,
                      &self.this_tablet,
                    ));
                    return Some(());
                  };
                  let task_done = match eval_update {
                    EvalUpdate::Update4(EvalUpdateStmt4 { set_col, set_val }) => {
                      // This means the Update for the row completed
                      // without the need for subqueries.
                      table_insert(
                        &mut combo_status.write_view,
                        &key,
                        &set_col,
                        set_val,
                        &write_meta.timestamp,
                      );
                      true
                    }
                    _ => {
                      // This means we must perform async subqueries.
                      send(
                        &mut side_effects,
                        &self.this_slave_eid,
                        &self.this_tablet,
                        SubqueryPathLeg1::Write {
                          wid: orig_wid.clone(),
                          leg2: SubqueryPathLeg2 {
                            combo: combo_status.combo.clone(),
                            leg3: SubqueryPathLeg3::Write {
                              wid: cur_wid.clone(),
                              key: key.clone(),
                            },
                          },
                        },
                        &context,
                        &write_meta.timestamp,
                      );
                      task.eval_expr = eval_update;
                      task.pending_subqueries = context;
                      task.complete_subqueries = BTreeMap::new();
                      false
                    }
                  };
                  if task_done {
                    combo_status.write_row_tasks.remove(key).unwrap();
                    if combo_status.write_row_tasks.is_empty() {
                      // This means that that combo_status.current_write
                      // is done. Now, we need to move onto the next write.
                      let next_wid_index = combo_status
                        .combo
                        .iter()
                        .position(|x| x == cur_wid)
                        .unwrap()
                        + 1;
                      let effects = if let Ok(effects) = continue_combo(
                        &mut self.rand_gen,
                        &mut status.select_views,
                        &mut status.combos_verified,
                        &mut combo_status,
                        &status.selects,
                        next_wid_index as i32,
                        StatusPath::WriteStatus {
                          wid: orig_wid.clone(),
                        },
                        &self.write_query_map,
                        &self.select_query_map,
                        &self.this_slave_eid,
                        &self.this_tablet,
                      ) {
                        effects
                      } else {
                        // Here, we need to abort the whole transaction. Get rid of the
                        // the whole PropertyVerificationStatus.
                        self.non_reached_writes.remove(&write_meta.timestamp);
                        self.writes_being_verified.remove(orig_wid);
                        self.write_query_map.remove(orig_wid);
                        side_effects.add(write_aborted(
                          &write_meta.tm_eid,
                          orig_wid,
                          &self.this_tablet,
                        ));
                        return Some(());
                      };
                      side_effects.append(effects);
                      if status.combos_verified.len() == status.combo_status_map.len() {
                        // We managed to verify all combos in the PropertyVerificationStatus.
                        // This means we are finished the transaction. So, add to
                        // already_reached_writes and update all associated
                        // PropertyVerificationStatuses accordingly.
                        self.non_reached_writes.remove(&write_meta.timestamp);
                        self.writes_being_verified.remove(orig_wid);
                        self
                          .already_reached_writes
                          .insert(write_meta.timestamp.clone(), orig_wid.clone());
                        // Send a response back to the TM.
                        side_effects.add(write_prepared(
                          &write_meta.tm_eid,
                          orig_wid,
                          &self.this_tablet,
                        ));
                        side_effects.append(add_new_combos(
                          &mut self.rand_gen,
                          &mut self.non_reached_writes,
                          &mut self.writes_being_verified,
                          &mut self.write_query_map,
                          &self.already_reached_writes,
                          &self.committed_writes,
                          &self.relational_tablet,
                          &self.select_query_map,
                          &self.this_slave_eid,
                          &self.this_tablet,
                        ));
                      }
                    }
                  }
                };
              }
            }
            SubqueryPathLeg3::Select { sid, key } => {
              let (view, tasks) = combo_status.select_row_tasks.get_mut(sid)?;
              let task = tasks.get_mut(key)?;
              // Modifiy the task with the (sid, subquery_result)
            }
          };
        }
        SubqueryPathLeg1::Select { sid, leg2 } => panic!("TODO: implement"),
      }
      Some(())
    })();
    return side_effects;
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

enum VerificationFailure {
  /// This means that when trying to verify a CombinationStatus in a
  /// PropertyVerificationStatus, some verification failure occured,
  /// like a Table Constraint violation, or a divergent Select Query.
  VerificationFailure,
}

// Useful stuff for creating SubquerypathLeg1, which is necessary stuff.
#[derive(Debug, Clone)]
enum StatusPath {
  SelectStatus { sid: SelectQueryId },
  WriteStatus { wid: WriteQueryId },
}

fn make_path(status_path: StatusPath, leg2: SubqueryPathLeg2) -> SubqueryPathLeg1 {
  match status_path {
    StatusPath::SelectStatus { sid } => SubqueryPathLeg1::Select { sid, leg2 },
    StatusPath::WriteStatus { wid } => SubqueryPathLeg1::Write { wid, leg2 },
  }
}

fn add_new_combos(
  rand_gen: &mut RandGen,
  non_reached_writes: &mut BTreeMap<Timestamp, WriteQueryId>,
  writes_being_verified: &mut HashMap<WriteQueryId, PropertyVerificationStatus>,
  write_query_map: &mut HashMap<WriteQueryId, WriteQueryMetadata>,
  already_reached_writes: &BTreeMap<Timestamp, WriteQueryId>,
  committed_writes: &BTreeMap<Timestamp, WriteQueryId>,
  relational_tablet: &RelationalTablet,
  select_query_map: &HashMap<SelectQueryId, SelectQueryMetadata>,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> TabletSideEffects {
  let mut side_effects = TabletSideEffects::new();
  let mut writes_to_delete: Vec<(EndpointId, WriteQueryId, Timestamp)> = Vec::new();
  for (cur_wid, status) in writes_being_verified.iter_mut() {
    for mut combo_as_map in create_combos(committed_writes, already_reached_writes) {
      // This code block constructs the CombinationStatus.
      let cur_write_meta = write_query_map.get(&cur_wid).unwrap();
      combo_as_map.insert(cur_write_meta.timestamp.clone(), cur_wid.clone());
      if let Ok(effects) = add_combo(
        rand_gen,
        status,
        StatusPath::WriteStatus {
          wid: cur_wid.clone(),
        },
        &combo_as_map.values().cloned().collect(),
        relational_tablet,
        write_query_map,
        select_query_map,
        this_slave_eid,
        this_tablet,
      ) {
        side_effects.append(effects);
      } else {
        // The verification failed, so abort the write and clean up all
        // data structures where the write appears.
        writes_to_delete.push((
          cur_write_meta.tm_eid.clone(),
          cur_wid.clone(),
          cur_write_meta.timestamp.clone(),
        ));
      }
    }
  }
  // We delete the writes separately from the loop above to avoid
  // double-mutable borrows of `writes_to_delete`.
  for (cur_tm_eid, cur_wid, cur_timestamp) in &writes_to_delete {
    side_effects.add(write_aborted(cur_tm_eid, cur_wid, this_tablet));
    non_reached_writes.remove(cur_timestamp);
    writes_being_verified.remove(cur_wid);
    write_query_map.remove(cur_wid);
  }
  side_effects
}

fn add_combo(
  rand_gen: &mut RandGen,
  status: &mut PropertyVerificationStatus,
  status_path: StatusPath,
  combo: &Vec<WriteQueryId>,
  relational_tablet: &RelationalTablet,
  write_query_map: &HashMap<WriteQueryId, WriteQueryMetadata>,
  select_query_map: &HashMap<SelectQueryId, SelectQueryMetadata>,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> Result<TabletSideEffects, VerificationFailure> {
  let mut side_effects = TabletSideEffects::new();
  // This code block constructions the CombinationStatus.
  let mut combo_status = CombinationStatus {
    combo: Vec::<WriteQueryId>::new(),
    write_view: RelationalTablet::new(relational_tablet.schema().clone()),
    current_write: None,
    write_row_tasks: HashMap::<PrimaryKey, EvalUpdateTask>::new(),
    select_row_tasks:
      HashMap::<SelectQueryId, (SelectView, HashMap<PrimaryKey, EvalSelectTask>)>::new(),
  };
  combo_status.combo = combo.clone();
  side_effects.append(continue_combo(
    rand_gen,
    &mut status.select_views,
    &mut status.combos_verified,
    &mut combo_status,
    &status.selects,
    0,
    status_path,
    write_query_map,
    select_query_map,
    this_slave_eid,
    this_tablet,
  )?);
  status.combo_status_map.insert(combo.clone(), combo_status);
  Ok(side_effects)
}

fn continue_combo(
  rand_gen: &mut RandGen,
  select_views: &mut HashMap<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>,
  combos_verified: &mut HashSet<Vec<WriteQueryId>>,
  combo_status: &mut CombinationStatus,
  selects: &BTreeMap<Timestamp, Vec<SelectQueryId>>,
  wid_index: i32,
  status_path: StatusPath,
  write_query_map: &HashMap<WriteQueryId, WriteQueryMetadata>,
  select_query_map: &HashMap<SelectQueryId, SelectQueryMetadata>,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> Result<TabletSideEffects, VerificationFailure> {
  assert!(0 <= wid_index && wid_index <= combo_status.combo.len() as i32);
  let lower_bound = if wid_index == 0 {
    Unbounded
  } else {
    let wid = combo_status.combo.get((wid_index - 1) as usize).unwrap();
    Included(write_query_map.get(wid).unwrap().timestamp)
  };
  let mut side_effects = TabletSideEffects::new();
  let mut upper_bound = Unbounded;
  // Iterate over the Write Queries and update the `write_view` accordingly.
  for cur_wid in &combo_status.combo[wid_index as usize..] {
    combo_status.current_write = Some(cur_wid.clone());
    let write_meta = write_query_map.get(cur_wid).unwrap().clone();
    let mut key_write_row_tasks: HashMap<PrimaryKey, EvalUpdateTask> = HashMap::new();
    match &write_meta.write_query {
      WriteQuery::Update(update_stmt) => {
        assert_eq!(update_stmt.table_name, this_tablet.path.path);
        for key in combo_status.write_view.get_keys(&write_meta.timestamp) {
          if let Ok((eval_update, context)) = eval_update_graph(
            rand_gen,
            EvalUpdate::Update1(
              if let Ok(eval_update) = start_eval_update_stmt(
                update_stmt.clone(),
                &combo_status.write_view,
                &key,
                &write_meta.timestamp,
              ) {
                eval_update
              } else {
                return Err(VerificationFailure::VerificationFailure);
              },
            ),
            BTreeMap::new(),
          ) {
            match eval_update {
              EvalUpdate::Update4(EvalUpdateStmt4 { set_col, set_val }) => {
                // This means the Update for the row completed
                // without the need for subqueries.
                table_insert(
                  &mut combo_status.write_view,
                  &key,
                  &set_col,
                  set_val,
                  &write_meta.timestamp,
                );
              }
              _ => {
                // This means we must perform async subqueries.
                send(
                  &mut side_effects,
                  this_slave_eid,
                  this_tablet,
                  make_path(
                    status_path.clone(),
                    SubqueryPathLeg2 {
                      combo: combo_status.combo.clone(),
                      leg3: SubqueryPathLeg3::Write {
                        wid: cur_wid.clone(),
                        key: key.clone(),
                      },
                    },
                  ),
                  &context,
                  &write_meta.timestamp,
                );
                key_write_row_tasks.insert(
                  key.clone(),
                  EvalUpdateTask {
                    eval_expr: eval_update.clone(),
                    pending_subqueries: context,
                    complete_subqueries: Default::default(),
                  },
                );
              }
            }
          } else {
            return Err(VerificationFailure::VerificationFailure);
          }
        }
      }
    }
    if !key_write_row_tasks.is_empty() {
      // This means that we can't continue applying writes, since
      // we are now blocked by subqueries.
      combo_status.write_row_tasks = key_write_row_tasks;
      upper_bound = Excluded(write_meta.timestamp);
      break;
    } else {
      // This means we continue applying writes, moving on.
      combo_status.current_write = None;
    }
  }
  for (_, selects) in selects.range((lower_bound, upper_bound)) {
    for select_id in selects {
      // This is a new select_id that we must start handling.
      let select_query_meta = select_query_map.get(select_id).unwrap().clone();
      let mut view = SelectView::new();
      let mut key_read_row_tasks = HashMap::<PrimaryKey, EvalSelectTask>::new();
      for key in combo_status
        .write_view
        .get_keys(&select_query_meta.timestamp)
      {
        // The selects here must be valid so this should never
        // return an error. Thus, we may just unwrap.
        let (eval_select, context) = eval_select_graph(
          rand_gen,
          EvalSelect::Select1(
            if let Ok(eval_select) = start_eval_select_stmt(
              select_query_meta.select_stmt.clone(),
              &combo_status.write_view,
              &key,
              &select_query_meta.timestamp,
            ) {
              eval_select
            } else {
              return Err(VerificationFailure::VerificationFailure);
            },
          ),
          BTreeMap::new(),
        )
        .unwrap();
        match eval_select {
          EvalSelect::Select3(EvalSelectStmt3 {
            col_names,
            where_clause,
          }) => {
            if where_clause {
              // This means we should add the row into the SelectView. Easy.
              let mut view_row = Vec::new();
              for col_name in col_names {
                view_row.push(combo_status.write_view.get_partial_val(
                  &key,
                  &col_name,
                  &select_query_meta.timestamp,
                ));
              }
              view.insert(key, view_row);
            }
          }
          _ => {
            // This means we must perform async subqueries.
            send(
              &mut side_effects,
              this_slave_eid,
              this_tablet,
              make_path(
                status_path.clone(),
                SubqueryPathLeg2 {
                  combo: combo_status.combo.clone(),
                  leg3: SubqueryPathLeg3::Select {
                    sid: select_id.clone(),
                    key: key.clone(),
                  },
                },
              ),
              &context,
              &select_query_meta.timestamp,
            );
            key_read_row_tasks.insert(
              key.clone(),
              EvalSelectTask {
                eval_expr: eval_select.clone(),
                pending_subqueries: context,
                complete_subqueries: Default::default(),
              },
            );
          }
        }
      }
      if !key_read_row_tasks.is_empty() {
        combo_status
          .select_row_tasks
          .insert(select_id.clone(), (view, key_read_row_tasks));
      } else {
        if let Some((select_view, combo_ids)) = select_views.get_mut(&select_id) {
          if select_view != &view {
            //  End everything and return an error
            return Err(VerificationFailure::VerificationFailure);
          } else {
            combo_ids.insert(combo_status.combo.clone());
          }
        } else {
          select_views.insert(
            select_id.clone(),
            (
              view.clone(),
              HashSet::from_iter(vec![combo_status.combo.clone()].into_iter()),
            ),
          );
        }
      }
    }
  }
  if combo_status.select_row_tasks.is_empty() && combo_status.write_row_tasks.is_empty() {
    // This means that the combo status is actually finished.
    combos_verified.insert(combo_status.combo.clone());
  }
  Ok(side_effects)
}

fn send(
  side_effects: &mut TabletSideEffects,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
  subquery_path: SubqueryPathLeg1,
  subqueries: &BTreeMap<SelectQueryId, SelectStmt>,
  timestamp: &Timestamp,
) {
  // Send the subqueries to the Slave Thread that owns
  // this Tablet Thread so that it can execute the
  // subqueries and send it back here.
  for (subquery_id, select_stmt) in subqueries {
    side_effects.add(TabletAction::Send {
      eid: this_slave_eid.clone(),
      msg: NetworkMessage::Slave(SlaveMessage::SubqueryRequest {
        tablet: this_tablet.clone(),
        sid: subquery_id.clone(),
        subquery_path: subquery_path.clone(),
        select_stmt: select_stmt.clone(),
        timestamp: *timestamp,
      }),
    });
  }
}

/// I hypothesize this will be the master interface for
/// evaluating EvalSelect.
fn eval_select_graph(
  rand_gen: &mut RandGen,
  eval_select: EvalSelect,
  context: BTreeMap<SelectQueryId, Vec<Row>>,
) -> Result<(EvalSelect, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  match eval_select {
    EvalSelect::Select1(eval_select_1) => panic!("TODO: implement"),
    EvalSelect::Select2(eval_select_2) => panic!("TODO: implement"),
    EvalSelect::Select3(eval_select_3) => panic!("TODO: implement"),
  };
  Err(ColumnDNE)
}

/// I hypothesize this will be the master interface for
/// evaluating EvalUpdate.
fn eval_update_graph(
  rand_gen: &mut RandGen,
  eval_update: EvalUpdate,
  context: BTreeMap<SelectQueryId, Vec<Row>>,
) -> Result<(EvalUpdate, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  match eval_update {
    EvalUpdate::Update1(eval_update_1) => panic!("TODO: implement"),
    EvalUpdate::Update2(eval_update_2) => panic!("TODO: implement"),
    EvalUpdate::Update3(eval_update_3) => panic!("TODO: implement"),
    EvalUpdate::Update4(eval_update_4) => panic!("TODO: implement"),
  };
  Err(ColumnDNE)
}

fn lookup<K: PartialEq + Eq, V: Clone>(vec: &Vec<(K, V)>, key: &K) -> Option<V> {
  for (k, v) in vec {
    if k == key {
      return Some(v.clone());
    }
  }
  return None;
}

fn table_insert(
  rel_tab: &mut RelationalTablet,
  key: &PrimaryKey,
  col_name: &ColumnName,
  val: EvalLiteral,
  timestamp: &Timestamp,
) {
  panic!("TODO: implement")
}

/// Errors that can occur when evaluating columns while transforming
/// an AST into an EvalAST.
#[derive(Debug)]
enum EvalErr {
  ColumnDNE,
  TypeError,
}

/// This function takes all ColumnNames in the `update_stmt`, replaces
/// them with their values from `rel_tab` (from the row with key `key`
/// at `timestamp`). We do this deeply, including subqueries. Then
/// we construct EvalUpdateStmt with all subqueries in tact, i.e. they
/// aren't turned into SubqueryIds yet.
fn start_eval_update_stmt(
  update_stmt: UpdateStmt,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<EvalUpdateStmt1, EvalErr> {
  panic!("TODO: implement");
}

fn eval_update_stmt_1_to_2(
  update_stmt: EvalUpdateStmt1,
  rand_gen: &mut RandGen,
) -> Result<(EvalUpdateStmt2, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  panic!("TODO: implement");
}

fn eval_update_stmt_2_to_3(
  update_stmt: EvalUpdateStmt2,
  rand_gen: &mut RandGen,
  context: &Vec<(SelectQueryId, EvalLiteral)>,
) -> Result<(EvalUpdateStmt3, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  panic!("TODO: implement");
}

fn finish_eval_update_stmt(
  update_stmt: EvalUpdateStmt3,
  context: &Vec<(SelectQueryId, EvalLiteral)>,
) -> Result<EvalUpdateStmt4, EvalErr> {
  panic!("TODO: implement");
}

/// This function takes all ColumnNames in the `update_stmt`, replaces
/// them with their values from `rel_tab` (from the row with key `key`
/// at `timestamp`). We do this deeply, including subqueries. Then
/// we construct EvalUpdateStmt with all subqueries in tact, i.e. they
/// aren't turned into SubqueryIds yet.
fn start_eval_select_stmt(
  select_stmt: SelectStmt,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<EvalSelectStmt1, EvalErr> {
  panic!("TODO: implement");
}

fn eval_select_stmt_1_to_2(
  select_stmt: EvalSelectStmt1,
  rand_gen: &mut RandGen,
) -> Result<(EvalSelectStmt2, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  panic!("TODO: implement");
}

fn finish_eval_select_stmt(
  update_stmt: EvalSelectStmt2,
  context: &Vec<(SelectQueryId, EvalLiteral)>,
) -> Result<EvalSelectStmt3, EvalErr> {
  panic!("TODO: implement");
}

/// i think what we actually need is a pass to just EvalExpr
/// making the Subquery into SubqueryIds, sending those out.

/// This function evaluates the `EvalExpr`. This function replaces any `Subquery`
/// variants with a `SubqueryId` by sending out the actual subquery for execution.
/// It returns either another `EvalExpr` if we still need to wait for Subqueries
/// to execute, or a `Literal` if the valuation of `expr` is complete.
///
/// This adds whatever subqueries are necessary to evaluate the subqueries.
/// This should be errors. type errors.
fn evaluate_expr(
  context: &Vec<(SelectQueryId, EvalLiteral)>,
  expr: PostEvalExpr,
) -> Result<EvalLiteral, EvalErr> {
  match expr {
    PostEvalExpr::BinaryExpr { op, lhs, rhs } => {
      let elhs = evaluate_expr(context, *lhs)?;
      let erhs = evaluate_expr(context, *rhs)?;
      match op {
        EvalBinaryOp::AND => panic!("TODO: implement."),
        EvalBinaryOp::OR => panic!("TODO: implement."),
        EvalBinaryOp::LT => panic!("TODO: implement."),
        EvalBinaryOp::LTE => panic!("TODO: implement."),
        EvalBinaryOp::E => panic!("TODO: implement."),
        EvalBinaryOp::GT => panic!("TODO: implement."),
        EvalBinaryOp::GTE => panic!("TODO: implement."),
        EvalBinaryOp::PLUS => panic!("TODO: implement."),
        EvalBinaryOp::TIMES => panic!("TODO: implement."),
        EvalBinaryOp::MINUS => panic!("TODO: implement."),
        EvalBinaryOp::DIV => panic!("TODO: implement."),
      }
    }
    PostEvalExpr::Literal(literal) => Ok(literal),
    PostEvalExpr::SubqueryId(subquery_id) => {
      // The subquery_id here must certainly be in the `context`,
      // since this function can only be called when all subqueries
      // have been answered. Reach into and it and return it.
      if let Some(val) = lookup(context, &subquery_id) {
        Ok(val.clone())
      } else {
        panic!("subquery_id {:?} must exist in {:?}", subquery_id, context)
      }
    }
  }
}

/// This Transforms the `expr` into a EvalExpr. It replaces all Subqueries with
/// Box<SelectStmt>, where if a column in the main query appears in the subquery,
/// we replace it with the evaluated value of the column. We also replace a column
/// name with the valuated value of the column in the main query as well.
///
/// (Don't worry if replacing the column names is wrong. It might be, if SQL binds
/// column names tighter to inner selects. Or SQL can throw an error. But I don't
/// need that level of foresight here. Maybe we can have a super high-level pass
/// that does verification later.)
fn evaluate_columns(
  rand_gen: &mut RandGen,
  expr: ValExpr,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<(PostEvalExpr, Vec<(SelectQueryId, SelectStmt)>), EvalErr> {
  /// Recursive function here.
  fn evaluate_columns_r(
    rand_gen: &mut RandGen,
    subqueries: &mut Vec<(SelectQueryId, SelectStmt)>,
    expr: ValExpr,
    rel_tab: &RelationalTablet,
    key: &PrimaryKey,
    timestamp: &Timestamp,
  ) -> Result<PostEvalExpr, EvalErr> {
    match expr {
      ValExpr::Literal(literal) => Ok(PostEvalExpr::Literal(convert_literal(&literal))),
      ValExpr::BinaryExpr { op, lhs, rhs } => Ok(PostEvalExpr::BinaryExpr {
        op: convert_op(&op),
        lhs: Box::new(evaluate_columns_r(
          rand_gen, subqueries, *lhs, rel_tab, key, timestamp,
        )?),
        rhs: Box::new(evaluate_columns_r(
          rand_gen, subqueries, *rhs, rel_tab, key, timestamp,
        )?),
      }),
      ValExpr::Column(col) => {
        let col_name = ColumnName(col.clone());
        if let Some(_) = rel_tab.col_name_exists(&col_name) {
          // If the column name actually exists in the current tablet's
          // schema, we replace the column name with the column value.
          Ok(PostEvalExpr::Literal(
            match rel_tab.get_partial_val(key, &col_name, timestamp) {
              Some(ColumnValue::Int(int)) => EvalLiteral::Int(int),
              Some(ColumnValue::String(string)) => EvalLiteral::String(string.to_string()),
              Some(ColumnValue::Bool(boolean)) => EvalLiteral::Bool(boolean),
              Some(ColumnValue::Unit) => {
                panic!("The Unit ColumnValue should never appear for a cell in the table.")
              }
              None => EvalLiteral::Null,
            },
          ))
        } else {
          // If the column name doesn't exist in the schema, then this query
          // is not valid and we propagate up an error message.
          Err(ColumnDNE)
        }
      }
      ValExpr::Subquery(subquery) => {
        // Create a random SubqueryId
        let mut bytes: [u8; 8] = [0; 8];
        rand_gen.rng.fill(&mut bytes);
        let subquery_id = SelectQueryId(TransactionId(bytes));
        // Add the Subquery to the `subqueries` vector.
        subqueries.push((
          subquery_id.clone(),
          SelectStmt {
            col_names: subquery.col_names.clone(),
            table_name: subquery.table_name.clone(),
            where_clause: replace_column(key, timestamp, rel_tab, &subquery.where_clause),
          },
        ));
        // The PostEvalExpr thats should replace the ValExpr::Subquery is simply
        // the subqueryId that was sent out to the clients.
        Ok(PostEvalExpr::SubqueryId(subquery_id))
      }
    }
  }

  let mut subqueries = Vec::new();
  let eval_expr = evaluate_columns_r(rand_gen, &mut subqueries, expr, rel_tab, key, timestamp)?;
  return Ok((eval_expr, subqueries));
}

/// Suppose `col_name` is a column name in the schema of `rel_tab`. This
/// function finds all instances of `col_name` in `expr` and replaces it
/// with the value of that column in `rel_tab` for the row keyed by `key`.
fn replace_column(
  key: &PrimaryKey,
  timestamp: &Timestamp,
  rel_tab: &RelationalTablet,
  expr: &ValExpr,
) -> ValExpr {
  match expr {
    ValExpr::BinaryExpr { op, lhs, rhs } => ValExpr::BinaryExpr {
      op: op.clone(),
      lhs: Box::new(replace_column(key, timestamp, rel_tab, lhs)),
      rhs: Box::new(replace_column(key, timestamp, rel_tab, rhs)),
    },
    ValExpr::Literal(literal) => ValExpr::Literal(literal.clone()),
    ValExpr::Column(col) => {
      let col_name = ColumnName(col.clone());
      if let Some(_) = rel_tab.col_name_exists(&col_name) {
        // If the column name exists in the current tablet's schema,
        // we replace the column name with the column value.
        ValExpr::Literal(match rel_tab.get_partial_val(key, &col_name, timestamp) {
          Some(ColumnValue::Int(int)) => Literal::Int(int.to_string()),
          Some(ColumnValue::String(string)) => Literal::String(string.to_string()),
          Some(ColumnValue::Bool(boolean)) => Literal::Bool(boolean),
          Some(ColumnValue::Unit) => {
            panic!("The Unit ColumnValue should never appear for a cell in the table.")
          }
          None => Literal::Null,
        })
      } else {
        // If the colum name doesn't refer to a column in the
        // main query, then leave it alone.
        ValExpr::Column(col.clone())
      }
    }
    ValExpr::Subquery(subquery) => ValExpr::Subquery(Box::from(SelectStmt {
      col_names: subquery.col_names.clone(),
      table_name: subquery.table_name.clone(),
      where_clause: replace_column(key, timestamp, rel_tab, &subquery.where_clause),
    })),
  }
}

fn convert_literal(literal: &Literal) -> EvalLiteral {
  match &literal {
    Literal::String(string) => EvalLiteral::String(string.clone()),
    Literal::Int(str) => EvalLiteral::Int(str.parse::<i32>().unwrap()),
    Literal::Bool(boolean) => EvalLiteral::Bool(boolean.clone()),
    Literal::Null => EvalLiteral::Null,
  }
}

fn convert_op(op: &BinaryOp) -> EvalBinaryOp {
  match op {
    BinaryOp::AND => EvalBinaryOp::AND,
    BinaryOp::OR => EvalBinaryOp::OR,
    BinaryOp::LT => EvalBinaryOp::LT,
    BinaryOp::LTE => EvalBinaryOp::LTE,
    BinaryOp::E => EvalBinaryOp::E,
    BinaryOp::GT => EvalBinaryOp::GT,
    BinaryOp::GTE => EvalBinaryOp::GTE,
    BinaryOp::PLUS => EvalBinaryOp::PLUS,
    BinaryOp::TIMES => EvalBinaryOp::TIMES,
    BinaryOp::MINUS => EvalBinaryOp::MINUS,
    BinaryOp::DIV => EvalBinaryOp::DIV,
  }
}

/// For every subset of `variable_map`, this functions merges it with
/// `fixed_map` and adds it to `combos`.
fn create_combos<K: Ord + Clone, V: Clone>(
  fixed_map: &BTreeMap<K, V>,
  variable_map: &BTreeMap<K, V>,
) -> Vec<BTreeMap<K, V>> {
  // A recursive function that performs the algorithm.
  fn create_combos_R<K: Ord + Clone, V: Clone>(
    combos: &mut Vec<BTreeMap<K, V>>,
    fixed_map: &mut BTreeMap<K, V>,
    variable_map: &mut BTreeMap<K, V>,
  ) {
    if let Some((k, v)) = variable_map.pop_first() {
      create_combos_R(combos, fixed_map, variable_map);
      fixed_map.insert(k.clone(), v.clone());
      create_combos_R(combos, fixed_map, variable_map);
      // Restore inputs.
      fixed_map.remove(&k);
      variable_map.insert(k, v);
    } else {
      combos.push(fixed_map.clone());
    }
  }

  let mut combos = vec![];
  create_combos_R(
    &mut combos,
    &mut fixed_map.clone(),
    &mut variable_map.clone(),
  );
  return combos;
}

#[test]
fn create_combos_test() {
  let mut fixed_map = BTreeMap::from_iter(vec![(1, 10), (4, 11)].into_iter());
  let mut variable_map = BTreeMap::from_iter(vec![(2, 12), (3, 13)].into_iter());
  let combos = create_combos(&fixed_map, &variable_map);
  assert_eq!(combos.len(), 4);
  assert_eq!(
    &combos[0],
    &BTreeMap::from_iter(vec![(1, 10), (4, 11)].into_iter())
  );
  assert_eq!(
    &combos[1],
    &BTreeMap::from_iter(vec![(1, 10), (3, 13), (4, 11)].into_iter())
  );
  assert_eq!(
    &combos[2],
    &BTreeMap::from_iter(vec![(1, 10), (2, 12), (4, 11)].into_iter())
  );
  assert_eq!(
    &combos[3],
    &BTreeMap::from_iter(vec![(1, 10), (3, 13), (2, 12), (4, 11)].into_iter())
  );
}
