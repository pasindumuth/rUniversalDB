use crate::common::rand::RandGen;
use crate::model::common::{
  ColumnName, ColumnValue, EndpointId, PrimaryKey, Row, Schema, SelectQueryId, SelectView,
  TabletPath, TabletShape, Timestamp, TransactionId, WriteDiff, WriteQueryId,
};
use crate::model::evalast::{
  EvalBinaryOp, EvalLiteral, Holder, InsertKeyTask, PostEvalExpr, PreEvalExpr, SelectKeyDoneTask,
  SelectKeyTask, SelectQueryTask, SelectTask, UpdateKeyStartTask, UpdateKeyTask, UpdateTask,
  WriteQueryTask,
};
use crate::model::message::{
  AdminMessage, AdminRequest, AdminResponse, FromCombo, FromProp, FromRoot, FromSelectTask,
  FromWriteTask, NetworkMessage, SelectPrepare, SlaveMessage, SubqueryResponse, TabletAction,
  TabletMessage, WriteAbort, WriteCommit, WritePrepare, WriteQuery,
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

/// What's going on? Okay, so what does a subquery result look like?
/// Well, when we do selects for this tablet, we usually select a
/// subuset of cols. but if we consider it rather Map<PrimaryKey, Vec<Option<ColumnValue>>>,
/// then we can do a proper comparison for selects and actually see if theyre the same.

/// Idk, just cmomit stuff. When committing, just go through all ComboStatuses
/// everywhere and remove ones which dont include the newly committed thing. Same
/// with aborted.

#[derive(Debug)]
struct CurrentWrite {
  wid: WriteQueryId,
  write_task: Holder<WriteQueryTask>,
}

#[derive(Debug)]
struct CombinationStatus {
  combo: Vec<WriteQueryId>,
  /// Fields controlling verification of the Write Queries only.
  write_view: View,
  current_write: Option<CurrentWrite>,
  /// Fields controlling verification of the Select Queries only.
  select_tasks: HashMap<SelectQueryId, Holder<SelectQueryTask>>,
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
      TabletMessage::SelectPrepare(select_prepare) => {
        side_effects.append(self.handle_select_prepare(select_prepare));
      }
      TabletMessage::WritePrepare(write_prepare) => {
        side_effects.append(self.handle_write_prepare(write_prepare));
      }
      TabletMessage::WriteCommit(write_commit) => {
        side_effects.append(self.handle_write_commit(write_commit));
      }
      TabletMessage::WriteAbort(write_abort) => {
        side_effects.append(self.handle_write_abort(write_abort));
      }
      TabletMessage::SubqueryResponse(subquery_res) => {
        side_effects.append(self.handle_subquery_response(subquery_res));
      }
    }
  }

  fn handle_select_prepare(&mut self, select_prepare: &SelectPrepare) -> TabletSideEffects {
    let SelectPrepare {
      tm_eid,
      sid,
      select_query,
      timestamp,
    } = select_prepare;
    let mut side_effects = TabletSideEffects::new();
    // Create the PropertyVerificationStatus
    let mut prop_status = PropertyVerificationStatus {
      combo_status_map: HashMap::<Vec<WriteQueryId>, CombinationStatus>::new(),
      selects: BTreeMap::<Timestamp, Vec<SelectQueryId>>::new(),
      select_views: HashMap::<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>::new(),
      combos_verified: HashSet::<Vec<WriteQueryId>>::new(),
    };
    // There is only one Select Query whose invariance to different
    // combinations of Write that we need to verify, and that Select
    // Query is the one we are testing.
    prop_status
      .selects
      .insert(timestamp.clone(), vec![sid.clone()]);
    self.select_query_map.insert(
      sid.clone(),
      SelectQueryMetadata {
        timestamp: timestamp.clone(),
        tm_eid: tm_eid.clone(),
        select_stmt: select_query.clone(),
      },
    );

    // Get the already_reached_writes with timestamp <= `timestamp`
    let mut sub_already_reached_writes = BTreeMap::new();
    for (write_timestamp, wid) in self
      .already_reached_writes
      .range((Unbounded, Included(timestamp)))
    {
      sub_already_reached_writes.insert(write_timestamp.clone(), wid.clone());
    }

    // Get the comitted_writes with timestamp <= `timestamp`
    let mut sub_committed_writes = BTreeMap::new();
    for (write_timestamp, wid) in self
      .committed_writes
      .range((Unbounded, Included(timestamp)))
    {
      sub_committed_writes.insert(write_timestamp.clone(), wid.clone());
    }

    // Construct the `combos` and update `combo_status_map` accordingly
    for combo_as_map in create_combos(&sub_committed_writes, &sub_already_reached_writes) {
      // This code block constructions the CombinationStatus.
      if let Ok(effects) = add_combo(
        &mut self.rand_gen,
        &mut prop_status,
        StatusPath::SelectStatus { sid: sid.clone() },
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
        side_effects.add(select_aborted(tm_eid, sid, &self.this_tablet));
        self.select_query_map.remove(sid);
        return side_effects;
      }
    }

    if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
      // We managed to verify all combos in the PropertyVerificationStatus.
      // This means we are finished the transaction.
      let select_view = prop_status.select_views.get(&sid).unwrap().0.clone();
      side_effects.append(handle_select_query_completion(
        &mut self.rand_gen,
        &mut self.writes_being_verified,
        &mut self.non_reached_writes,
        &mut self.write_query_map,
        sid,
        timestamp,
        &self.select_query_map,
        &self.this_slave_eid,
        &self.this_tablet,
      ));
      add_select(&mut self.already_reached_selects, &timestamp, &sid);
      side_effects.add(select_prepared(
        &tm_eid,
        &sid,
        &self.this_tablet,
        select_view,
      ));
    } else {
      // The PropertyVerificationStatus is not done. So add it
      // to non_reached_selects.
      if let Some(select_ids) = self.non_reached_selects.get_mut(timestamp) {
        select_ids.push(sid.clone());
      } else {
        self
          .non_reached_selects
          .insert(timestamp.clone(), vec![sid.clone()]);
      }
      self.selects_being_verified.insert(sid.clone(), prop_status);
    }
    return side_effects;
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
    // Create the PropertyVerificationStatus
    let mut prop_status = PropertyVerificationStatus {
      combo_status_map: HashMap::<Vec<WriteQueryId>, CombinationStatus>::new(),
      selects: BTreeMap::<Timestamp, Vec<SelectQueryId>>::new(),
      select_views: HashMap::<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>::new(),
      combos_verified: HashSet::<Vec<WriteQueryId>>::new(),
    };
    prop_status.selects = self.already_reached_selects.clone();
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
        &mut prop_status,
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
        return side_effects;
      }
    }

    if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
      // We managed to verify all combos in the
      // PropertyVerificationStatus. This means we are finished
      // the transaction, so we need to add new combo_statuses to
      // writes_being_verified and selects_being_verified.
      side_effects.append(handle_write_query_completion(
        &mut self.rand_gen,
        &mut self.non_reached_writes,
        &mut self.writes_being_verified,
        &mut self.write_query_map,
        &mut self.non_reached_selects,
        &mut self.selects_being_verified,
        &mut self.select_query_map,
        &wid,
        &self.already_reached_writes,
        &self.committed_writes,
        &self.relational_tablet,
        &self.this_slave_eid,
        &self.this_tablet,
      ));
      self
        .already_reached_writes
        .insert(timestamp.clone(), wid.clone());
      // Send a response back to the TM.
      side_effects.add(write_prepared(tm_eid, wid, &self.this_tablet));
    } else {
      // The PropertyVerificationStatus is not done. So add it
      // to non_reached_writes.
      self
        .non_reached_writes
        .insert(timestamp.clone(), wid.clone());
      self.writes_being_verified.insert(wid.clone(), prop_status);
    }
    return side_effects;
  }

  fn handle_write_commit(&mut self, write_commit: &WriteCommit) -> TabletSideEffects {
    let WriteCommit {
      tm_eid,
      wid: orig_wid,
    } = write_commit;
    let mut side_effects = TabletSideEffects::new();
    // Get the associated write query.
    let orig_write_meta = if let Some(write_query) = self.write_query_map.get(orig_wid) {
      write_query.clone()
    } else {
      return side_effects;
    };

    self
      .already_reached_writes
      .remove(&orig_write_meta.timestamp)
      .unwrap();
    self
      .committed_writes
      .insert(orig_write_meta.timestamp.clone(), orig_wid.clone());
    // Removed completed write_query from the writes_being_verified.
    self.writes_being_verified.remove(&orig_wid);

    // Remove all combo_statuses in writes_being_verified that assume that this write
    // fails. Since we now know it doesn't, those combo_statuses are irrelevent. For every
    // prop_status, remove them from the combo_status_map, combos_verified, and select_views.
    let wids: Vec<WriteQueryId> = self.writes_being_verified.keys().cloned().collect();
    for wid in wids {
      if let Some(prop_status) = self.writes_being_verified.get_mut(&wid) {
        let combos: Vec<Vec<WriteQueryId>> = prop_status.combo_status_map.keys().cloned().collect();
        for combo in combos {
          if !combo.contains(&orig_wid) {
            prop_status.combo_status_map.remove(&combo).unwrap();
            prop_status.combos_verified.remove(&combo);
            for (_, (_, confirmed_combos)) in &mut prop_status.select_views {
              confirmed_combos.remove(&combo);
            }
          }
        }
        // By removing irrelevent combos, we may have just completed the prop_status.
        if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
          let write_meta = self.write_query_map.get(&wid).unwrap().clone();
          self.non_reached_writes.remove(&write_meta.timestamp);
          self.writes_being_verified.remove(&wid);
          side_effects.append(handle_write_query_completion(
            &mut self.rand_gen,
            &mut self.non_reached_writes,
            &mut self.writes_being_verified,
            &mut self.write_query_map,
            &mut self.non_reached_selects,
            &mut self.selects_being_verified,
            &mut self.select_query_map,
            &wid,
            &self.already_reached_writes,
            &self.committed_writes,
            &self.relational_tablet,
            &self.this_slave_eid,
            &self.this_tablet,
          ));
          self
            .already_reached_writes
            .insert(write_meta.timestamp.clone(), wid.clone());
          // Send a response back to the TM.
          side_effects.add(write_prepared(&write_meta.tm_eid, &wid, &self.this_tablet));
        }
      }
    }

    // Similar to the above, remove all such combo_statuses from selects_being_verified.
    let sids: Vec<SelectQueryId> = self.selects_being_verified.keys().cloned().collect();
    for sid in sids {
      if let Some(prop_status) = self.selects_being_verified.get_mut(&sid) {
        let sid = sid.clone();
        let combos: Vec<Vec<WriteQueryId>> = prop_status.combo_status_map.keys().cloned().collect();
        for combo in combos {
          if !combo.contains(&orig_wid) {
            prop_status.combo_status_map.remove(&combo).unwrap();
            prop_status.combos_verified.remove(&combo);
            for (_, (_, confirmed_combos)) in &mut prop_status.select_views {
              confirmed_combos.remove(&combo);
            }
          }
        }
        // By removing irrelevent combos, we may have just completed the prop_status.
        if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
          let select_view = prop_status.select_views.get(&sid).unwrap().0.clone();
          let select_meta = self.select_query_map.get(&sid).unwrap();
          remove_select(&mut self.non_reached_selects, &select_meta.timestamp, &sid);
          self.selects_being_verified.remove(&sid);
          side_effects.append(handle_select_query_completion(
            &mut self.rand_gen,
            &mut self.writes_being_verified,
            &mut self.non_reached_writes,
            &mut self.write_query_map,
            &sid,
            &select_meta.timestamp,
            &self.select_query_map,
            &self.this_slave_eid,
            &self.this_tablet,
          ));
          add_select(
            &mut self.already_reached_selects,
            &select_meta.timestamp,
            &sid,
          );
          side_effects.add(select_prepared(
            &select_meta.tm_eid,
            &sid,
            &self.this_tablet,
            select_view,
          ));
        }
      }
    }

    side_effects.add(write_committed(tm_eid, orig_wid, &self.this_tablet));
    return side_effects;
  }

  /// This is very similar to a write commit, except we remove combos_status
  /// that *contain* the aborted rwrite, instead of combo_statuses that don't.
  fn handle_write_abort(&mut self, write_abort: &WriteAbort) -> TabletSideEffects {
    let WriteAbort {
      tm_eid,
      wid: orig_wid,
    } = write_abort;
    let mut side_effects = TabletSideEffects::new();
    // Get and remove the associated write query.
    let orig_write_meta = if let Some(write_query) = self.write_query_map.remove(orig_wid) {
      write_query.clone()
    } else {
      return side_effects;
    };

    // Remove the write from already_reached_writes.
    self
      .already_reached_writes
      .remove(&orig_write_meta.timestamp)
      .unwrap();

    // Remove all combo_statuses in writes_being_verified that assume that this write
    // succeeds. Since we now know it doesn't, those combo_statuses are irrelevent. For every
    // prop_status, remove them from the combo_status_map, combos_verified, and select_views.
    let wids: Vec<WriteQueryId> = self.writes_being_verified.keys().cloned().collect();
    for wid in wids {
      if let Some(prop_status) = self.writes_being_verified.get_mut(&wid) {
        let combos: Vec<Vec<WriteQueryId>> = prop_status.combo_status_map.keys().cloned().collect();
        for combo in combos {
          if combo.contains(&orig_wid) {
            prop_status.combo_status_map.remove(&combo).unwrap();
            prop_status.combos_verified.remove(&combo);
            for (_, (_, confirmed_combos)) in &mut prop_status.select_views {
              confirmed_combos.remove(&combo);
            }
          }
        }
        // By removing irrelevent combos, we may have just completed the prop_status.
        if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
          let write_meta = self.write_query_map.get(&wid).unwrap().clone();
          self.non_reached_writes.remove(&write_meta.timestamp);
          self.writes_being_verified.remove(&wid);
          side_effects.append(handle_write_query_completion(
            &mut self.rand_gen,
            &mut self.non_reached_writes,
            &mut self.writes_being_verified,
            &mut self.write_query_map,
            &mut self.non_reached_selects,
            &mut self.selects_being_verified,
            &mut self.select_query_map,
            &wid,
            &self.already_reached_writes,
            &self.committed_writes,
            &self.relational_tablet,
            &self.this_slave_eid,
            &self.this_tablet,
          ));
          self
            .already_reached_writes
            .insert(write_meta.timestamp.clone(), wid.clone());
          // Send a response back to the TM.
          side_effects.add(write_prepared(&write_meta.tm_eid, &wid, &self.this_tablet));
        }
      }
    }

    // Similar to the above, remove all such combo_statuses from selects_being_verified.
    let sids: Vec<SelectQueryId> = self.selects_being_verified.keys().cloned().collect();
    for sid in sids {
      if let Some(prop_status) = self.selects_being_verified.get_mut(&sid) {
        let sid = sid.clone();
        let combos: Vec<Vec<WriteQueryId>> = prop_status.combo_status_map.keys().cloned().collect();
        for combo in combos {
          if !combo.contains(&orig_wid) {
            prop_status.combo_status_map.remove(&combo).unwrap();
            prop_status.combos_verified.remove(&combo);
            for (_, (_, confirmed_combos)) in &mut prop_status.select_views {
              confirmed_combos.remove(&combo);
            }
          }
        }
        // By removing irrelevent combos, we may have just completed the prop_status.
        if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
          let select_view = prop_status.select_views.get(&sid).unwrap().0.clone();
          let select_meta = self.select_query_map.get(&sid).unwrap();
          remove_select(&mut self.non_reached_selects, &select_meta.timestamp, &sid);
          self.selects_being_verified.remove(&sid);
          side_effects.append(handle_select_query_completion(
            &mut self.rand_gen,
            &mut self.writes_being_verified,
            &mut self.non_reached_writes,
            &mut self.write_query_map,
            &sid,
            &select_meta.timestamp,
            &self.select_query_map,
            &self.this_slave_eid,
            &self.this_tablet,
          ));
          add_select(
            &mut self.already_reached_selects,
            &select_meta.timestamp,
            &sid,
          );
          side_effects.add(select_prepared(
            &select_meta.tm_eid,
            &sid,
            &self.this_tablet,
            select_view,
          ));
        }
      }
    }

    side_effects.add(write_aborted(tm_eid, orig_wid, &self.this_tablet));
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
        FromRoot::Write {
          wid: orig_wid,
          from_prop,
        } => {
          // This Subquery is destined for a Write Query
          let orig_write_meta = self.write_query_map.get(orig_wid).unwrap().clone();
          let prop_status = self.writes_being_verified.get_mut(orig_wid)?;
          let combo_status = prop_status.combo_status_map.get_mut(&from_prop.combo)?;
          side_effects.append(
            if let Ok(effects) = handle_subquery_response_prop(
              &mut self.rand_gen,
              &mut prop_status.select_views,
              &mut prop_status.combos_verified,
              combo_status,
              &prop_status.selects,
              &sid,
              &StatusPath::WriteStatus {
                wid: orig_wid.clone(),
              },
              &from_prop,
              result.clone(),
              &self.write_query_map,
              &self.select_query_map,
              &self.this_slave_eid,
              &self.this_tablet,
            )? {
              effects
            } else {
              // Here, we need to abort the whole transaction. Get rid of the
              // the whole PropertyVerificationStatus.
              self.non_reached_writes.remove(&orig_write_meta.timestamp);
              self.writes_being_verified.remove(orig_wid);
              self.write_query_map.remove(orig_wid);
              side_effects.add(write_aborted(
                &orig_write_meta.tm_eid,
                orig_wid,
                &self.this_tablet,
              ));
              return Some(());
            },
          );
          // Check if we managed to verify all combos in the
          // PropertyVerificationStatus. This means we are finished the
          // transaction. So, add to already_reached_writes and update all
          // associated PropertyVerificationStatuses accordingly.
          if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
            self.non_reached_writes.remove(&orig_write_meta.timestamp);
            self.writes_being_verified.remove(orig_wid);
            side_effects.append(handle_write_query_completion(
              &mut self.rand_gen,
              &mut self.non_reached_writes,
              &mut self.writes_being_verified,
              &mut self.write_query_map,
              &mut self.non_reached_selects,
              &mut self.selects_being_verified,
              &mut self.select_query_map,
              orig_wid,
              &self.already_reached_writes,
              &self.committed_writes,
              &self.relational_tablet,
              &self.this_slave_eid,
              &self.this_tablet,
            ));
            self
              .already_reached_writes
              .insert(orig_write_meta.timestamp.clone(), orig_wid.clone());
            // Send a response back to the TM.
            side_effects.add(write_prepared(
              &orig_write_meta.tm_eid,
              orig_wid,
              &self.this_tablet,
            ));
          }
        }
        FromRoot::Select {
          sid: orig_sid,
          from_prop,
        } => {
          // This Subquery is destined for a Write Query
          let orig_select_meta = self.select_query_map.get(orig_sid).unwrap().clone();
          let prop_status = self.selects_being_verified.get_mut(orig_sid)?;
          let combo_status = prop_status.combo_status_map.get_mut(&from_prop.combo)?;
          side_effects.append(
            if let Ok(effects) = handle_subquery_response_prop(
              &mut self.rand_gen,
              &mut prop_status.select_views,
              &mut prop_status.combos_verified,
              combo_status,
              &prop_status.selects,
              &sid,
              &StatusPath::SelectStatus {
                sid: orig_sid.clone(),
              },
              &from_prop,
              result.clone(),
              &self.write_query_map,
              &self.select_query_map,
              &self.this_slave_eid,
              &self.this_tablet,
            )? {
              effects
            } else {
              // Here, we need to abort the whole transaction. Get rid of the
              // the whole PropertyVerificationStatus.
              self.non_reached_selects.remove(&orig_select_meta.timestamp);
              self.selects_being_verified.remove(orig_sid);
              self.select_query_map.remove(orig_sid);
              side_effects.add(select_aborted(
                &orig_select_meta.tm_eid,
                orig_sid,
                &self.this_tablet,
              ));
              return Some(());
            },
          );
          if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
            // We managed to verify all combos in the PropertyVerificationStatus.
            // This means we are finished the transaction. Here, we first remove
            // the Select Query from everywhere, and then update all Write Query
            // Prop Statuses.
            let select_view = prop_status.select_views.get(&sid).unwrap().0.clone();
            remove_select(
              &mut self.non_reached_selects,
              &orig_select_meta.timestamp,
              orig_sid,
            );
            self.selects_being_verified.remove(orig_sid);
            side_effects.append(handle_select_query_completion(
              &mut self.rand_gen,
              &mut self.writes_being_verified,
              &mut self.non_reached_writes,
              &mut self.write_query_map,
              orig_sid,
              &orig_select_meta.timestamp,
              &self.select_query_map,
              &self.this_slave_eid,
              &self.this_tablet,
            ));
            add_select(
              &mut self.already_reached_selects,
              &orig_select_meta.timestamp,
              &sid,
            );
            side_effects.add(select_prepared(
              &orig_select_meta.tm_eid,
              &sid,
              &self.this_tablet,
              select_view,
            ));
          }
        }
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

/// This function is called when a successful Write Query, `wid`, has been removed from
/// `writes_being_verified`, but before it is added into `already_reached_writes`.
/// This functions creates `combo_status`s for all existing `writes_being_verified`,
/// and all `selects_being_verified`, where each of the new combos have `wid` present
/// in there. Since this process might cause some Queries being verified to fail, this
/// function also deletes those Queries out of `non_reached_writes`, `writes_being_verified`,
/// and `write_query_map`, or from the `non_reached_selects`, `selects_being_verified`, and
/// `select_query_map`, and creates TM responses with the failures.
fn handle_write_query_completion(
  rand_gen: &mut RandGen,
  non_reached_writes: &mut BTreeMap<Timestamp, WriteQueryId>,
  writes_being_verified: &mut HashMap<WriteQueryId, PropertyVerificationStatus>,
  write_query_map: &mut HashMap<WriteQueryId, WriteQueryMetadata>,
  non_reached_selects: &mut BTreeMap<Timestamp, Vec<SelectQueryId>>,
  selects_being_verified: &mut HashMap<SelectQueryId, PropertyVerificationStatus>,
  select_query_map: &mut HashMap<SelectQueryId, SelectQueryMetadata>,
  wid: &WriteQueryId,
  already_reached_writes: &BTreeMap<Timestamp, WriteQueryId>,
  committed_writes: &BTreeMap<Timestamp, WriteQueryId>,
  relational_tablet: &RelationalTablet,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> TabletSideEffects {
  let write_meta = write_query_map.get(wid).unwrap().clone();
  let mut side_effects = TabletSideEffects::new();
  // Add combos to all writes_being_verified, deleting any if they
  // become invalidated.
  let mut writes_to_delete: Vec<(EndpointId, WriteQueryId, Timestamp)> = Vec::new();
  for (cur_wid, status) in writes_being_verified.iter_mut() {
    for mut combo_as_map in create_combos(committed_writes, already_reached_writes) {
      // This code block constructs the CombinationStatus.
      let cur_write_meta = write_query_map.get(&cur_wid).unwrap();
      combo_as_map.insert(cur_write_meta.timestamp.clone(), cur_wid.clone());
      combo_as_map.insert(write_meta.timestamp.clone(), wid.clone());
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
        // Move on to the next Write Query being verified.
        break;
      }
    }
  }
  // We delete the writes separately from the loop above to avoid
  // double-mutable borrows of `writes_being_verified`.
  for (cur_tm_eid, cur_wid, cur_timestamp) in &writes_to_delete {
    side_effects.add(write_aborted(cur_tm_eid, cur_wid, this_tablet));
    non_reached_writes.remove(cur_timestamp);
    writes_being_verified.remove(cur_wid);
    write_query_map.remove(cur_wid);
  }
  // Add combos to all selects_being_verified, deleting any if they
  // become invalidated.
  let mut selects_to_delete: Vec<(EndpointId, SelectQueryId, Timestamp)> = Vec::new();
  for (cur_sid, status) in selects_being_verified.iter_mut() {
    let cur_select_meta = select_query_map.get(&cur_sid).unwrap();
    // Only add new Combos to the Select Query PropStatus if the newly added
    // Write Query's timestamp is below that of the Select Query.
    if write_meta.timestamp <= cur_select_meta.timestamp {
      for mut combo_as_map in create_combos(committed_writes, already_reached_writes) {
        // This code block constructs the CombinationStatus.
        combo_as_map.insert(write_meta.timestamp.clone(), wid.clone());
        if let Ok(effects) = add_combo(
          rand_gen,
          status,
          StatusPath::SelectStatus {
            sid: cur_sid.clone(),
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
          // The verification failed, so abort the Select Query and clean up all
          // data structures where the Select appears.
          selects_to_delete.push((
            cur_select_meta.tm_eid.clone(),
            cur_sid.clone(),
            cur_select_meta.timestamp.clone(),
          ));
        }
      }
    }
  }
  // We delete the Select Query separately from the loop above to avoid
  // double-mutable borrows of `selects_to_delete`.
  for (cur_tm_eid, cur_sid, cur_timestamp) in &selects_to_delete {
    side_effects.add(select_aborted(cur_tm_eid, cur_sid, this_tablet));
    // Calling unwrap here should not be a problem, since we  know cur_sid
    // *must* be in `non_reached_selects` at `cur_timestamp`
    remove_select(non_reached_selects, cur_timestamp, cur_sid);
    selects_being_verified.remove(cur_sid);
    select_query_map.remove(cur_sid);
  }
  side_effects
}

/// This function updates all `writes_being_verified` in response to a Select Query
/// being added to `already_reached_selects`. In particular, this function adds this
/// SelectQuery to each combination of each PropStatus in `writes_being_verified` (we
/// don't need this for `selects_being_verified` since those should only have
/// one Select Query in each ComboStatus), and then evaluates it as far as it can.
/// If we encouter Write Queries that are now invalid (i.e where the Select Query
/// results don't match up between combos), then we remove them and send an abort
/// message to the Slave.
fn handle_select_query_completion(
  rand_gen: &mut RandGen,
  writes_being_verified: &mut HashMap<WriteQueryId, PropertyVerificationStatus>,
  non_reached_writes: &mut BTreeMap<Timestamp, WriteQueryId>,
  write_query_map: &mut HashMap<WriteQueryId, WriteQueryMetadata>,
  orig_sid: &SelectQueryId,
  timestamp: &Timestamp,
  select_query_map: &HashMap<SelectQueryId, SelectQueryMetadata>,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> TabletSideEffects {
  let mut side_effects = TabletSideEffects::new();
  let mut writes_to_delete: Vec<(EndpointId, WriteQueryId, Timestamp)> = Vec::new();
  for (wid, prop_status) in writes_being_verified.iter_mut() {
    // Add the new Select into `selects`
    let write_meta = write_query_map.get(wid).unwrap();
    if let Some(selects) = prop_status.selects.get_mut(timestamp) {
      selects.push(orig_sid.clone());
    } else {
      prop_status
        .selects
        .insert(timestamp.clone(), vec![orig_sid.clone()]);
    }
    prop_status.combos_verified.clear();
    // Start executing another Select for each combo.
    for (_, combo_status) in prop_status.combo_status_map.iter_mut() {
      // This is a new select_id that we must start handling.
      if let Ok(effects) = add_combo_select(
        rand_gen,
        &mut prop_status.select_views,
        combo_status,
        &StatusPath::WriteStatus { wid: wid.clone() },
        orig_sid,
        &select_query_map,
        &this_slave_eid,
        &this_tablet,
      ) {
        side_effects.append(effects)
      } else {
        // The verification failed, so abort the write and clean up all
        // data structures where the write appears.
        writes_to_delete.push((
          write_meta.tm_eid.clone(),
          wid.clone(),
          write_meta.timestamp.clone(),
        ));
        // Move on to the next Write Query being verified.
        break;
      };
      if combo_status.select_tasks.is_empty() && combo_status.current_write.is_none() {
        // This means that the combo status is finished (again).
        prop_status
          .combos_verified
          .insert(combo_status.combo.clone());
      }
    }
  }
  // We delete the writes separately from the loop above to avoid
  // double-mutable borrows of `writes_being_verified`.
  for (cur_tm_eid, cur_wid, cur_timestamp) in &writes_to_delete {
    side_effects.add(write_aborted(cur_tm_eid, cur_wid, &this_tablet));
    non_reached_writes.remove(cur_timestamp);
    writes_being_verified.remove(cur_wid);
    write_query_map.remove(cur_wid);
  }
  side_effects
}

/// Removes Select Query with ID `sid` and timestamp `timestamp`
/// from the given `non_reached_selects`.
fn remove_select(
  non_reached_selects: &mut BTreeMap<Timestamp, Vec<SelectQueryId>>,
  timestamp: &Timestamp,
  sid: &SelectQueryId,
) {
  let selects = non_reached_selects.get_mut(timestamp).unwrap();
  let select_index = selects.iter().position(|s| s == sid).unwrap();
  selects.remove(select_index);
  if selects.is_empty() {
    non_reached_selects.remove(timestamp);
  }
}

/// Adds the Select Query with ID `sid` and timestamp `timestamp`
/// from the given `selects` multi-value BTreeMap.
fn add_select(
  selects_map: &mut BTreeMap<Timestamp, Vec<SelectQueryId>>,
  timestamp: &Timestamp,
  sid: &SelectQueryId,
) {
  if let Some(selects) = selects_map.get_mut(timestamp) {
    selects.push(sid.clone());
  } else {
    selects_map.insert(timestamp.clone(), vec![sid.clone()]);
  }
}

/// This functions routes the given Subquery Response, whose ID was `sid`
/// and whose result was `result`, down to where it belongs in the
/// `combo_status`. It's possible that the Subquery is no longer relevent
/// for the `combo_status`; i.e. the task (WriteQueryTask or SelectQueryTask)
/// that issued the Subquery had disappeared for some reason. This is okay;
/// we just return a None when this happens. Otherwise, we return either
/// `TabletSideEffects` containing any new subqueries, or a `VerificationFailure`
/// indicating that we just discovered the Query that owns this `combo_status`
/// cannot be completed.
fn handle_subquery_response_prop(
  rand_gen: &mut RandGen,
  select_views: &mut HashMap<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>,
  combos_verified: &mut HashSet<Vec<WriteQueryId>>,
  combo_status: &mut CombinationStatus,
  selects: &BTreeMap<Timestamp, Vec<SelectQueryId>>,
  sid: &SelectQueryId,
  status_path: &StatusPath,
  from_prop: &FromProp,
  result: Result<SelectView, String>,
  write_query_map: &HashMap<WriteQueryId, WriteQueryMetadata>,
  select_query_map: &HashMap<SelectQueryId, SelectQueryMetadata>,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> Option<Result<TabletSideEffects, VerificationFailure>> {
  let mut side_effects = TabletSideEffects::new();
  let subquery_ret = if let Ok(subquery_ret) = result.clone() {
    subquery_ret
  } else {
    return Some(Err(VerificationFailure::VerificationFailure));
  };
  match &from_prop.from_combo {
    FromCombo::Write {
      wid: cur_wid,
      from_task,
    } => {
      // This Subquery is destined for a Write Query in the combo
      let current_write = combo_status.current_write.as_mut()?;
      if &current_write.wid == cur_wid {
        let context = if let Ok(context) = eval_write_graph(
          rand_gen,
          &mut current_write.write_task,
          &from_task,
          &sid,
          &subquery_ret,
          &combo_status.write_view,
        ) {
          context
        } else {
          return Some(Err(VerificationFailure::VerificationFailure));
        };
        let task_done = match &current_write.write_task.val {
          WriteQueryTask::WriteDoneTask(diff) => {
            // This means the Update for the row completed
            // without the need for any more subqueries.
            table_insert_diff(
              &mut combo_status.write_view,
              &diff,
              &write_query_map.get(cur_wid).unwrap().timestamp,
            );
            true
          }
          _ => {
            // This means that the UpdateKeyTask requires subqueries
            // to execute, so it is not complete.
            let mut subq = BTreeMap::<SelectQueryId, (FromRoot, SelectStmt)>::new();
            for (select_id, (from_task, select_stmt)) in context {
              subq.insert(
                select_id,
                (
                  make_path(
                    status_path.clone(),
                    FromProp {
                      combo: combo_status.combo.clone(),
                      from_combo: FromCombo::Write {
                        wid: cur_wid.clone(),
                        from_task,
                      },
                    },
                  ),
                  select_stmt,
                ),
              );
            }
            send(
              &mut side_effects,
              &this_slave_eid,
              &this_tablet,
              &subq,
              &write_query_map.get(cur_wid).unwrap().timestamp,
            );
            false
          }
        };
        if task_done {
          // This means that that combo_status.current_write
          // is done. Now, we need to move onto the next write.
          let next_wid_index = combo_status
            .combo
            .iter()
            .position(|x| x == cur_wid)
            .unwrap()
            + 1;
          let effects = if let Ok(effects) = continue_combo(
            rand_gen,
            select_views,
            combos_verified,
            combo_status,
            selects,
            next_wid_index as i32,
            status_path.clone(),
            &write_query_map,
            &select_query_map,
            &this_slave_eid,
            &this_tablet,
          ) {
            effects
          } else {
            return Some(Err(VerificationFailure::VerificationFailure));
          };
          side_effects.append(effects);
        };
      }
    }
    FromCombo::Select {
      sid: cur_sid,
      from_task,
    } => {
      // How do we implement this? Well, we need to dig into the right
      // SelectQueryTask, match the path. Simply forwarding should do.
      let mut select_task = combo_status.select_tasks.get_mut(cur_sid)?;
      let context = if let Ok(context) = eval_select_graph(
        rand_gen,
        &mut select_task,
        &from_task,
        &cur_sid,
        &subquery_ret,
        &combo_status.write_view,
      ) {
        context
      } else {
        return Some(Err(VerificationFailure::VerificationFailure));
      };
      match &select_task.val {
        SelectQueryTask::SelectDoneTask(view) => {
          // This means the Update for the row completed without the need
          // for any more subqueries. Thus, we finish up the Select Query,
          // checking to see if it matches what's in prop_status, and then
          // removing it from combo_status.select_tasks.
          if let Some((select_view, combo_ids)) = select_views.get_mut(&cur_sid) {
            if select_view != view {
              return Some(Err(VerificationFailure::VerificationFailure));
            } else {
              combo_ids.insert(combo_status.combo.clone());
            }
          } else {
            select_views.insert(
              cur_sid.clone(),
              (
                view.clone(),
                HashSet::from_iter(vec![combo_status.combo.clone()].into_iter()),
              ),
            );
          }
          combo_status.select_tasks.remove(cur_sid);
          if combo_status.select_tasks.is_empty() && combo_status.current_write.is_none() {
            // This means that the combo status is actually finished.
            combos_verified.insert(combo_status.combo.clone());
          }
        }
        _ => {
          // This means that the UpdateKeyTask requires subqueries
          // to execute, so it is not complete.
          let mut subq = BTreeMap::<SelectQueryId, (FromRoot, SelectStmt)>::new();
          for (select_id, (from_task, select_stmt)) in context {
            subq.insert(
              select_id,
              (
                make_path(
                  status_path.clone(),
                  FromProp {
                    combo: combo_status.combo.clone(),
                    from_combo: FromCombo::Select {
                      sid: cur_sid.clone(),
                      from_task,
                    },
                  },
                ),
                select_stmt,
              ),
            );
          }
          send(
            &mut side_effects,
            &this_slave_eid,
            &this_tablet,
            &subq,
            &select_query_map.get(cur_sid).unwrap().timestamp,
          );
        }
      };
    }
  };
  Some(Ok(side_effects))
}

/// This adds a `combo_status` to `status`, or returns a `VerificationFailure`
/// if trying to execute `combo_status` results in a `VerificationFailure`.
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
    select_tasks: HashMap::<SelectQueryId, Holder<SelectQueryTask>>::new(),
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

/// This assumes that all Write Queries in the `combo_status` whose index
/// is less than `wid_index` have been executed. At this point, `write_view`,
/// should be populated with the results of those Write Queries, and
/// `write_row_tasks` should be empty. Also, all Select Queries that don't
/// rely on the Write Query *prior* to `wid_index` or after it have been launched,
/// i.e. for each Select Query, there is either a job in `select_row_tasks`,
/// or the results of that Select Query are in `select_views`.
///
/// This function extends `wid_index` as far as possible, stopping only
/// if a Write Query launches Sub Queries. In this case, `write_row_tasks`
/// will get populated with Subqueries. If `wid_index` gets extended all
/// the way, it's possible that the whole `combo_task` is completed, in
/// which case we add it to `combos_verified`. These are on in the happy
/// path, though. Ulimate, the `combo_status` can fail, like if a table
/// constraint fails. In this clase, a `VerificationFailure` is returned,
/// and the input mut variables could have changed.
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
  combo_status.current_write = None; // Set this to None
  for cur_wid in &combo_status.combo[wid_index as usize..] {
    let cur_write_meta = write_query_map.get(cur_wid).unwrap().clone();
    match &cur_write_meta.write_query {
      // DELETEs will look very similar to updates, where we iterate through current
      // keys. However, INSERTs will be different. We will iterate over the new keys,
      // adding Tasks for keys that don't exist in `write_view.get_keys`. Remember,
      // MVMs have all keys defined, mapping to Options, where taking any subset of
      // keys for a given Write Query makes sense from it's point of view, as long
      // as each key's task ends with writing something (could be a None, or a Some(...))
      WriteQuery::Update(update_stmt) => {
        assert_eq!(update_stmt.table_name, this_tablet.path.path);
        let mut update_key_tasks = BTreeMap::<PrimaryKey, Holder<UpdateKeyTask>>::new();
        for key in combo_status.write_view.get_keys(&cur_write_meta.timestamp) {
          let (update_key_task, context) = if let Ok(update_task) = start_eval_update_key_task(
            rand_gen,
            update_stmt.clone(),
            &combo_status.write_view,
            &key,
            &cur_write_meta.timestamp,
          ) {
            update_task
          } else {
            return Err(VerificationFailure::VerificationFailure);
          };
          match update_key_task {
            UpdateKeyTask::Done(done_task) => {
              // This means the UpdateKeyTask for the row completed
              // without the need for Subqueries, and that an
              // update should take place.
              table_insert(
                &mut combo_status.write_view,
                &key,
                &done_task.set_col,
                done_task.set_val,
                &cur_write_meta.timestamp,
              );
            }
            UpdateKeyTask::None(_) => {
              // This means the UpdateKeyTask for the row completed
              // without the need for subqueries, and that there
              // shouldn't be any updates.
            }
            _ => {
              // This means that the UpdateKeyTask requires subqueries
              // to execute, so it is not complete.
              let mut subq = BTreeMap::<SelectQueryId, (FromRoot, SelectStmt)>::new();
              for (select_id, select_stmt) in context {
                subq.insert(
                  select_id,
                  (
                    make_path(
                      status_path.clone(),
                      FromProp {
                        combo: combo_status.combo.clone(),
                        from_combo: FromCombo::Write {
                          wid: cur_wid.clone(),
                          from_task: FromWriteTask::UpdateTask { key: key.clone() },
                        },
                      },
                    ),
                    select_stmt,
                  ),
                );
              }
              send(
                &mut side_effects,
                this_slave_eid,
                this_tablet,
                &subq,
                &cur_write_meta.timestamp,
              );
              update_key_tasks.insert(key.clone(), Holder::from(update_key_task));
            }
          }
        }
        if !update_key_tasks.is_empty() {
          // This means that we can't continue applying writes, since
          // we are now blocked by subqueries. We also break out if the
          // loop that iterates over Write Queries.
          upper_bound = Excluded(cur_write_meta.timestamp);
          combo_status.current_write = Some(CurrentWrite {
            wid: cur_wid.clone(),
            write_task: Holder::from(WriteQueryTask::UpdateTask(UpdateTask {
              key_tasks: update_key_tasks,
            })),
          });
          break;
        }
      }
    }
    // If we get here, it means that the WriteQuery processed above
    // was fully processed, and that we may continue applying writes.
  }
  for (_, selects) in selects.range((lower_bound, upper_bound)) {
    for select_id in selects {
      // This is a new select_id that we must start handling.
      side_effects.append(
        if let Ok(effects) = add_combo_select(
          rand_gen,
          select_views,
          combo_status,
          &status_path,
          select_id,
          select_query_map,
          this_slave_eid,
          this_tablet,
        ) {
          effects
        } else {
          return Err(VerificationFailure::VerificationFailure);
        },
      );
    }
  }
  if combo_status.select_tasks.is_empty() && combo_status.current_write.is_none() {
    // This means that the combo status is actually finished.
    combos_verified.insert(combo_status.combo.clone());
  }
  Ok(side_effects)
}

/// This function adds the given Select Query to the given
/// `combo_status`, executing it as far as it will go, potentially
/// adding it to `select_views` if it completes. Note that this function
/// doesn't check for completion of the `combo_status` in any way.
fn add_combo_select(
  rand_gen: &mut RandGen,
  select_views: &mut HashMap<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>,
  combo_status: &mut CombinationStatus,
  status_path: &StatusPath,
  select_id: &SelectQueryId,
  select_query_map: &HashMap<SelectQueryId, SelectQueryMetadata>,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> Result<TabletSideEffects, VerificationFailure> {
  let select_query_meta = select_query_map.get(select_id).unwrap().clone();
  // Get the Select Query
  let mut side_effects = TabletSideEffects::new();
  let mut view = SelectView::new();
  let mut select_tasks = BTreeMap::<PrimaryKey, Holder<SelectKeyTask>>::new();
  for key in combo_status
    .write_view
    .get_keys(&select_query_meta.timestamp)
  {
    // The selects here must be valid so this should never
    // return an error. Thus, we may just unwrap.
    let (select_key_task, context) = start_eval_select_key_task(
      rand_gen,
      select_query_meta.select_stmt.clone(),
      &combo_status.write_view,
      &key,
      &select_query_meta.timestamp,
    )
    .unwrap();
    match select_key_task {
      SelectKeyTask::Done(done_task) => {
        // This means we should add the row into the SelectView. Easy.
        let mut view_row = Vec::new();
        for sel_col in done_task.sel_cols {
          view_row.push((
            sel_col.clone(),
            combo_status
              .write_view
              .get_partial_val(&key, &sel_col, &select_query_meta.timestamp),
          ));
        }
        view.insert(key, view_row);
      }
      SelectKeyTask::None(_) => {
        // The WHERE clause evaluated to false, so do nothing.
      }
      _ => {
        // This means that the UpdateKeyTask requires subqueries
        // to execute, so it is not complete.
        let mut subq = BTreeMap::<SelectQueryId, (FromRoot, SelectStmt)>::new();
        for (select_id, select_stmt) in context {
          subq.insert(
            select_id.clone(),
            (
              make_path(
                status_path.clone(),
                FromProp {
                  combo: combo_status.combo.clone(),
                  from_combo: FromCombo::Select {
                    sid: select_id,
                    from_task: FromSelectTask::SelectTask { key: key.clone() },
                  },
                },
              ),
              select_stmt,
            ),
          );
        }
        send(
          &mut side_effects,
          this_slave_eid,
          this_tablet,
          &subq,
          &select_query_meta.timestamp,
        );
        select_tasks.insert(key.clone(), Holder::from(select_key_task));
      }
    }
  }
  if !select_tasks.is_empty() {
    // This means we must create a SelectTask and hold in
    // the combo_status.
    combo_status.select_tasks.insert(
      select_id.clone(),
      Holder::from(SelectQueryTask::SelectTask(SelectTask {
        tasks: select_tasks,
        select_view: view,
      })),
    );
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
  Ok(side_effects)
}

/// Send the subqueries to the Slave Thread that owns
/// this Tablet Thread so that it can execute the
/// Subqueries and send the results back here.
fn send(
  side_effects: &mut TabletSideEffects,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
  subqueries: &BTreeMap<SelectQueryId, (FromRoot, SelectStmt)>,
  timestamp: &Timestamp,
) {
  for (subquery_id, (subquery_path, select_stmt)) in subqueries {
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

/// We have Tasks, and then we have Task Enums. Tasks are structs, and Task
/// Enums are Enums where each Variant is a single Tuple Struct that wraps
/// a Task Type. Every Task will appear as a variant in some Task Enum.
/// Every Task Enum will have their own `eval` function, which
/// returns the Task Type (perhaps the same as the input) that we should
/// transition to, and potentially some subqueries that needs to be sent
/// out.
///
/// A Task will generally have some sub-Tasks and maybe a `pending_queries`
/// list of it's own. These sub-Tasks will be defined in the Task Type via
/// through Task Enums, and it will be held at some relative path
/// away from the Task (like in `UpdateTask`, where we have one
/// `UpdateKeyTasks` located at every `PrimaryKey`). A task is
/// finished when it's sub-Tasks are finished and all expressions
/// have been evaluated.
///
/// We construct WriteQueryTasks from the bottom up.

/// This function looks at the `UpdateKeyTask`'s own context and tries
/// to evaluate it as far as possible.
fn eval_update_key_task(
  rand_gen: &mut RandGen,
  update_key_task: &mut Holder<UpdateKeyTask>,
  subquery_id: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, SelectStmt>, EvalErr> {
  match &update_key_task.val {
    UpdateKeyTask::Start(task) => panic!("hi"),
    UpdateKeyTask::None(task) => panic!("hi"),
    UpdateKeyTask::Done(task) => panic!("hi"),
  }
}

/// This function adds the adds the `subquery` at the location of
/// `path`, and then executes things as far as they will go. If
/// we realize the `WriteQueryTask` cannot be finished (due to
/// type errors, runtime errors (dividing by zero) or broken
/// table constriants), then we return an Err instead.
fn eval_write_graph(
  rand_gen: &mut RandGen,
  write_query_task: &mut Holder<WriteQueryTask>,
  path: &FromWriteTask,
  subquery_id: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, (FromWriteTask, SelectStmt)>, EvalErr> {
  match (&path, &write_query_task.val) {
    (FromWriteTask::UpdateTask { key }, WriteQueryTask::UpdateTask(task)) => {
      panic!(
        "TODO: impelment. Fairly easy; just forward it to the eval_update_key_task,\
      and if all eval_update_key_tasks are done, then move `write_query_task` to done."
      )
    }
    (FromWriteTask::InsertTask { key }, WriteQueryTask::InsertTask(task)) => {
      panic!("TODO: implement")
    }
    (FromWriteTask::InsertSelectTask, WriteQueryTask::InsertSelectTask(task)) => {
      panic!("TODO: implement")
    }
    _ => {
      // Ignore all other combination of `path` and `write_query_task`;
      // those shouldn't result in any transitions.
      Ok(BTreeMap::new())
    }
  }
}

/// I hypothesize this will be the master interface for
/// evaluating EvalSelect.
fn eval_select_graph(
  rand_gen: &mut RandGen,
  select_task: &mut Holder<SelectQueryTask>,
  path: &FromSelectTask,
  subquery_id: &SelectQueryId,
  subquery_ret: &SelectView,
  rel_tab: &RelationalTablet,
) -> Result<BTreeMap<SelectQueryId, (FromSelectTask, SelectStmt)>, EvalErr> {
  match &select_task.val {
    SelectQueryTask::SelectTask(task) => panic!("TODO: implement"),
    SelectQueryTask::SelectDoneTask(task) => panic!("TODO: implement"),
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

fn table_insert_diff(rel_tab: &mut RelationalTablet, diff: &WriteDiff, timestamp: &Timestamp) {
  panic!("TODO: implement")
}

/// Errors that can occur when evaluating columns while transforming
/// an AST into an EvalAST.
#[derive(Debug)]
enum EvalErr {
  ColumnDNE,
  TypeError,
  TableConstraintBroken,
}

/// This function takes all ColumnNames in the `update_stmt`, replaces
/// them with their values from `rel_tab` (from the row with key `key`
/// at `timestamp`). We do this deeply, including subqueries. Then
/// we construct EvalUpdateStmt with all subqueries in tact, i.e. they
/// aren't turned into SubqueryIds yet.
fn start_eval_update_key_task(
  rand_gen: &mut RandGen,
  update_stmt: UpdateStmt,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<(UpdateKeyTask, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  panic!(
    "TODO: implement. This will likely use eval_update_key_task to help\
  move the UpdateKeyTask as far forward as possible."
  );
}
//
// fn eval_update_stmt_1_to_2(
//   update_stmt: EvalUpdateStmt1,
//   rand_gen: &mut RandGen,
// ) -> Result<(EvalUpdateStmt2, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
//   panic!("TODO: implement");
// }
//
// fn eval_update_stmt_2_to_3(
//   update_stmt: EvalUpdateStmt2,
//   rand_gen: &mut RandGen,
//   context: &Vec<(SelectQueryId, EvalLiteral)>,
// ) -> Result<(EvalUpdateStmt3, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
//   panic!("TODO: implement");
// }
//
// fn finish_eval_update_stmt(
//   update_stmt: EvalUpdateStmt3,
//   context: &Vec<(SelectQueryId, EvalLiteral)>,
// ) -> Result<EvalUpdateStmt4, EvalErr> {
//   panic!("TODO: implement");
// }

/// This function takes all ColumnNames in the `update_stmt`, replaces
/// them with their values from `rel_tab` (from the row with key `key`
/// at `timestamp`). We do this deeply, including subqueries. Then
/// we construct EvalUpdateStmt with all subqueries in tact, i.e. they
/// aren't turned into SubqueryIds yet.
fn start_eval_select_key_task(
  rand_gen: &mut RandGen,
  select_stmt: SelectStmt,
  rel_tab: &RelationalTablet,
  key: &PrimaryKey,
  timestamp: &Timestamp,
) -> Result<(SelectKeyTask, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
  panic!(
    "TODO: implement. This will likely use eval_update_key_task to help\
  move the UpdateKeyTask as far forward as possible."
  );
}

// fn eval_select_stmt_1_to_2(
//   select_stmt: EvalSelectStmt1,
//   rand_gen: &mut RandGen,
// ) -> Result<(EvalSelectStmt2, BTreeMap<SelectQueryId, SelectStmt>), EvalErr> {
//   panic!("TODO: implement");
// }
//
// fn finish_eval_select_stmt(
//   update_stmt: EvalSelectStmt2,
//   context: &Vec<(SelectQueryId, EvalLiteral)>,
// ) -> Result<EvalSelectStmt3, EvalErr> {
//   panic!("TODO: implement");
// }

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
