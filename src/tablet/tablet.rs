use crate::common::rand::RandGen;
use crate::model::common::{
  EndpointId, PrimaryKey, Row, Schema, SelectQueryId, SelectView, TabletShape, Timestamp,
  WriteQueryId,
};
use crate::model::evalast::{
  Holder, InsertRowTask, InsertTask, SelectKeyTask, SelectQueryTask, SelectTask, UpdateKeyTask,
  UpdateTask, WriteQueryTask,
};
use crate::model::message::{
  AdminMessage, AdminRequest, AdminResponse, FromCombo, FromProp, FromRoot, FromSelectTask,
  FromWriteTask, NetworkMessage, SelectPrepare, SlaveMessage, SubqueryResponse, TabletAction,
  TabletMessage, WriteAbort, WriteCommit, WritePrepare, WriteQuery,
};
use crate::model::sqlast::SelectStmt;
use crate::storage::relational_tablet::RelationalTablet;
use crate::tablet::expression::{
  eval_select_graph, eval_write_graph, start_eval_insert_row_task, start_eval_select_key_task,
  start_eval_update_key_task, table_insert_cell, table_insert_diff, table_insert_row,
  verify_insert,
};
use std::collections::{BTreeMap, HashMap, HashSet};
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
    let (new_sid, new_select_meta) = {
      let SelectPrepare {
        tm_eid,
        sid: new_sid,
        select_query,
        timestamp,
      } = select_prepare;
      (
        new_sid,
        SelectQueryMetadata {
          timestamp: timestamp.clone(),
          tm_eid: tm_eid.clone(),
          select_stmt: select_query.clone(),
        },
      )
    };
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
      .insert(new_select_meta.timestamp.clone(), vec![new_sid.clone()]);
    self
      .select_query_map
      .insert(new_sid.clone(), new_select_meta.clone());

    // Construct the `combos` and update `combo_status_map` accordingly
    for combo_as_map in create_partial_combos(
      &self.committed_writes,
      &self.already_reached_writes,
      &new_select_meta.timestamp,
    ) {
      // This code block constructions the CombinationStatus.
      if let Ok(effects) = add_combo(
        &mut self.rand_gen,
        &mut prop_status,
        StatusPath::SelectStatus {
          sid: new_sid.clone(),
        },
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
        side_effects.add(select_aborted(
          &new_select_meta.tm_eid,
          new_sid,
          &self.this_tablet,
        ));
        self.select_query_map.remove(new_sid);
        return side_effects;
      }
    }

    if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
      // We managed to verify all combos in the PropertyVerificationStatus.
      // This means we are finished the transaction.
      let select_view = prop_status.select_views.get(&new_sid).unwrap().0.clone();
      side_effects.append(handle_select_query_completion(
        &mut self.rand_gen,
        &mut self.writes_being_verified,
        &mut self.non_reached_writes,
        &mut self.write_query_map,
        new_sid,
        &new_select_meta.timestamp,
        &self.select_query_map,
        &self.this_slave_eid,
        &self.this_tablet,
      ));
      add_select(
        &mut self.already_reached_selects,
        &new_select_meta.timestamp,
        &new_sid,
      );
      side_effects.add(select_prepared(
        &new_select_meta.tm_eid,
        &new_sid,
        &self.this_tablet,
        select_view,
      ));
    } else {
      // The PropertyVerificationStatus is not done. So add it
      // to non_reached_selects.
      add_select(
        &mut self.non_reached_selects,
        &new_select_meta.timestamp,
        &new_sid,
      );
      self
        .selects_being_verified
        .insert(new_sid.clone(), prop_status);
    }
    return side_effects;
  }

  /// This was taken out into it's own function so that we can handle the failure
  /// case easily (rather than mutating the main side-effects variable, we just
  /// return one instead).
  fn handle_write_prepare(&mut self, write_prepare: &WritePrepare) -> TabletSideEffects {
    let mut side_effects = TabletSideEffects::new();
    let (new_wid, new_write_meta) = {
      let WritePrepare {
        tm_eid,
        wid: new_wid,
        write_query,
        timestamp,
      } = write_prepare;
      if self.non_reached_writes.contains_key(timestamp)
        || self.already_reached_writes.contains_key(timestamp)
        || self.committed_writes.contains_key(timestamp)
      {
        // For now, we disallow Write Queries with the same timestamp
        // from being committed in the same Tablet. This eliminates
        // the ambiguitiy of which Write Query to apply first, and
        // it would create complications when trying to do multi-stage
        // transactions later on anyways.
        side_effects.add(write_aborted(&tm_eid, &new_wid, &self.this_tablet));
        return side_effects;
      } else {
        (
          new_wid,
          WriteQueryMetadata {
            timestamp: timestamp.clone(),
            tm_eid: tm_eid.clone(),
            write_query: write_query.clone(),
          },
        )
      }
    };

    // Immediately insert the new_write_meta into the write_query_map, since
    // the helper function we use will rely on this. We just need to make sure to
    // clean this up if the Write Query fails before this function ends.
    self
      .write_query_map
      .insert(new_wid.clone(), new_write_meta.clone());

    // Create the PropertyVerificationStatus
    let mut prop_status = PropertyVerificationStatus {
      combo_status_map: HashMap::<Vec<WriteQueryId>, CombinationStatus>::new(),
      selects: BTreeMap::<Timestamp, Vec<SelectQueryId>>::new(),
      select_views: HashMap::<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>::new(),
      combos_verified: HashSet::<Vec<WriteQueryId>>::new(),
    };
    prop_status.selects = self.already_reached_selects.clone();

    // Construct the `combos` and update `combo_status_map` accordingly
    for mut combo_as_map in create_combos(&self.committed_writes, &self.already_reached_writes) {
      // This code block constructions the CombinationStatus.
      combo_as_map.insert(new_write_meta.timestamp.clone(), new_wid.clone());
      if let Ok(effects) = add_combo(
        &mut self.rand_gen,
        &mut prop_status,
        StatusPath::WriteStatus {
          wid: new_wid.clone(),
        },
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
        side_effects.add(write_aborted(
          &new_write_meta.tm_eid,
          new_wid,
          &self.this_tablet,
        ));
        self.write_query_map.remove(new_wid);
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
        &new_wid,
        &self.already_reached_writes,
        &self.committed_writes,
        &self.relational_tablet,
        &self.this_slave_eid,
        &self.this_tablet,
      ));
      self
        .already_reached_writes
        .insert(new_write_meta.timestamp.clone(), new_wid.clone());
      // Send a response back to the TM.
      side_effects.add(write_prepared(
        &new_write_meta.tm_eid,
        new_wid,
        &self.this_tablet,
      ));
    } else {
      // The PropertyVerificationStatus is not done. So add it
      // to `non_reached_writes` and `writes_being_verified`.
      self
        .non_reached_writes
        .insert(new_write_meta.timestamp.clone(), new_wid.clone());
      self
        .writes_being_verified
        .insert(new_wid.clone(), prop_status);
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
    let orig_write_meta = self.write_query_map.get(orig_wid).unwrap();
    self
      .already_reached_writes
      .remove(&orig_write_meta.timestamp)
      .unwrap();
    self
      .committed_writes
      .insert(orig_write_meta.timestamp.clone(), orig_wid.clone());

    // Remove all combo_statuses in writes_being_verified that assume that this orig_wid
    // fails. Since we now know it doesn't, those combo_statuses are irrelevent. For every
    // prop_status, remove them from the combo_status_map, combos_verified, and select_views.
    let wids: Vec<WriteQueryId> = self.writes_being_verified.keys().cloned().collect();
    for cur_wid in wids {
      if let Some(prop_status) = self.writes_being_verified.get_mut(&cur_wid) {
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
          let cur_write_meta = self.write_query_map.get(&cur_wid).unwrap().clone();
          self.non_reached_writes.remove(&cur_write_meta.timestamp);
          self.writes_being_verified.remove(&cur_wid);
          side_effects.append(handle_write_query_completion(
            &mut self.rand_gen,
            &mut self.non_reached_writes,
            &mut self.writes_being_verified,
            &mut self.write_query_map,
            &mut self.non_reached_selects,
            &mut self.selects_being_verified,
            &mut self.select_query_map,
            &cur_wid,
            &self.already_reached_writes,
            &self.committed_writes,
            &self.relational_tablet,
            &self.this_slave_eid,
            &self.this_tablet,
          ));
          self
            .already_reached_writes
            .insert(cur_write_meta.timestamp.clone(), cur_wid.clone());
          // Send a response back to the TM.
          side_effects.add(write_prepared(
            &cur_write_meta.tm_eid,
            &cur_wid,
            &self.this_tablet,
          ));
        }
      }
    }

    // Similar to the above, remove all such combo_statuses from selects_being_verified.
    let sids: Vec<SelectQueryId> = self.selects_being_verified.keys().cloned().collect();
    for cur_sid in sids {
      if let Some(prop_status) = self.selects_being_verified.get_mut(&cur_sid) {
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
          let select_view = prop_status.select_views.get(&cur_sid).unwrap().0.clone();
          let cur_select_meta = self.select_query_map.get(&cur_sid).unwrap();
          remove_select(
            &mut self.non_reached_selects,
            &cur_select_meta.timestamp,
            &cur_sid,
          );
          self.selects_being_verified.remove(&cur_sid);
          side_effects.append(handle_select_query_completion(
            &mut self.rand_gen,
            &mut self.writes_being_verified,
            &mut self.non_reached_writes,
            &mut self.write_query_map,
            &cur_sid,
            &cur_select_meta.timestamp,
            &self.select_query_map,
            &self.this_slave_eid,
            &self.this_tablet,
          ));
          add_select(
            &mut self.already_reached_selects,
            &cur_select_meta.timestamp,
            &cur_sid,
          );
          side_effects.add(select_prepared(
            &cur_select_meta.tm_eid,
            &cur_sid,
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
  /// that *contain* the aborted rewrite, instead of combo_statuses that don't.
  fn handle_write_abort(&mut self, write_abort: &WriteAbort) -> TabletSideEffects {
    let WriteAbort {
      tm_eid,
      wid: orig_wid,
    } = write_abort;
    let mut side_effects = TabletSideEffects::new();
    // Get and remove the associated write query.
    let orig_write_meta = self.write_query_map.remove(orig_wid).unwrap();
    self
      .already_reached_writes
      .remove(&orig_write_meta.timestamp)
      .unwrap();

    // Remove all combo_statuses in writes_being_verified that assume that this write
    // succeeds. Since we now know it doesn't, those combo_statuses are irrelevent. For every
    // prop_status, remove them from the combo_status_map, combos_verified, and select_views.
    let wids: Vec<WriteQueryId> = self.writes_being_verified.keys().cloned().collect();
    for cur_wid in wids {
      if let Some(prop_status) = self.writes_being_verified.get_mut(&cur_wid) {
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
          let cur_write_meta = self.write_query_map.get(&cur_wid).unwrap().clone();
          self.non_reached_writes.remove(&cur_write_meta.timestamp);
          self.writes_being_verified.remove(&cur_wid);
          side_effects.append(handle_write_query_completion(
            &mut self.rand_gen,
            &mut self.non_reached_writes,
            &mut self.writes_being_verified,
            &mut self.write_query_map,
            &mut self.non_reached_selects,
            &mut self.selects_being_verified,
            &mut self.select_query_map,
            &cur_wid,
            &self.already_reached_writes,
            &self.committed_writes,
            &self.relational_tablet,
            &self.this_slave_eid,
            &self.this_tablet,
          ));
          self
            .already_reached_writes
            .insert(cur_write_meta.timestamp.clone(), cur_wid.clone());
          // Send a response back to the TM.
          side_effects.add(write_prepared(
            &cur_write_meta.tm_eid,
            &cur_wid,
            &self.this_tablet,
          ));
        }
      }
    }

    // Similar to the above, remove all such combo_statuses from selects_being_verified.
    let sids: Vec<SelectQueryId> = self.selects_being_verified.keys().cloned().collect();
    for cur_sid in sids {
      if let Some(prop_status) = self.selects_being_verified.get_mut(&cur_sid) {
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
          let select_view = prop_status.select_views.get(&cur_sid).unwrap().0.clone();
          let cur_select_meta = self.select_query_map.get(&cur_sid).unwrap();
          remove_select(
            &mut self.non_reached_selects,
            &cur_select_meta.timestamp,
            &cur_sid,
          );
          self.selects_being_verified.remove(&cur_sid);
          side_effects.append(handle_select_query_completion(
            &mut self.rand_gen,
            &mut self.writes_being_verified,
            &mut self.non_reached_writes,
            &mut self.write_query_map,
            &cur_sid,
            &cur_select_meta.timestamp,
            &self.select_query_map,
            &self.this_slave_eid,
            &self.this_tablet,
          ));
          add_select(
            &mut self.already_reached_selects,
            &cur_select_meta.timestamp,
            &cur_sid,
          );
          side_effects.add(select_prepared(
            &cur_select_meta.tm_eid,
            &cur_sid,
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
      sid: sub_sid,
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
          let prop_status = self.writes_being_verified.get_mut(orig_wid)?;
          let orig_write_meta = self.write_query_map.get(orig_wid).unwrap().clone();
          let combo_status = prop_status.combo_status_map.get_mut(&from_prop.combo)?;
          if let Ok(effects) = handle_subquery_response_prop(
            &mut self.rand_gen,
            &mut prop_status.select_views,
            &mut prop_status.combos_verified,
            combo_status,
            &prop_status.selects,
            &sub_sid,
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
            side_effects.append(effects);
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
          };
          // Check if we managed to verify all combos in the
          // PropertyVerificationStatus. This means we are finished the
          // transaction. So, add to already_reached_writes and update all
          // PropertyVerificationStatuses accordingly.
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
          // This Subquery is destined for a Select Query
          let prop_status = self.selects_being_verified.get_mut(orig_sid)?;
          let orig_select_meta = self.select_query_map.get(orig_sid).unwrap().clone();
          let combo_status = prop_status.combo_status_map.get_mut(&from_prop.combo)?;
          if let Ok(effects) = handle_subquery_response_prop(
            &mut self.rand_gen,
            &mut prop_status.select_views,
            &mut prop_status.combos_verified,
            combo_status,
            &prop_status.selects,
            &sub_sid,
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
            side_effects.append(effects);
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
          };
          if prop_status.combos_verified.len() == prop_status.combo_status_map.len() {
            // We managed to verify all combos in the PropertyVerificationStatus.
            // This means we are finished the transaction. Here, we first remove
            // the Select Query from everywhere, and then update all
            // PropertyVerificationStatuses accordingly.
            let select_view = prop_status.select_views.get(&orig_sid).unwrap().0.clone();
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
              &orig_sid,
            );
            side_effects.add(select_prepared(
              &orig_select_meta.tm_eid,
              &orig_sid,
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
  orig_wid: &WriteQueryId,
  already_reached_writes: &BTreeMap<Timestamp, WriteQueryId>,
  committed_writes: &BTreeMap<Timestamp, WriteQueryId>,
  relational_tablet: &RelationalTablet,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> TabletSideEffects {
  let mut side_effects = TabletSideEffects::new();
  let orig_write_meta = write_query_map.get(orig_wid).unwrap().clone();
  // Add combos to all writes_being_verified, deleting any if they
  // become invalidated.
  let mut writes_to_delete: Vec<(EndpointId, WriteQueryId, Timestamp)> = Vec::new();
  for (cur_wid, prop_status) in writes_being_verified.iter_mut() {
    let cur_write_meta = write_query_map.get(&cur_wid).unwrap();
    for mut combo_as_map in create_combos(committed_writes, already_reached_writes) {
      // This code block constructs the CombinationStatus.
      combo_as_map.insert(cur_write_meta.timestamp.clone(), cur_wid.clone());
      combo_as_map.insert(orig_write_meta.timestamp.clone(), orig_wid.clone());
      if let Ok(effects) = add_combo(
        rand_gen,
        prop_status,
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
  for (cur_sid, prop_status) in selects_being_verified.iter_mut() {
    let cur_select_meta = select_query_map.get(&cur_sid).unwrap();
    // Only add new Combos to the Select Query prop_status if the newly added
    // Write Query's timestamp is <= that of the Select Query.
    if orig_write_meta.timestamp <= cur_select_meta.timestamp {
      for mut combo_as_map in create_partial_combos(
        committed_writes,
        already_reached_writes,
        &cur_select_meta.timestamp,
      ) {
        // This code block constructs the CombinationStatus.
        combo_as_map.insert(orig_write_meta.timestamp.clone(), orig_wid.clone());
        if let Ok(effects) = add_combo(
          rand_gen,
          prop_status,
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
  for (cur_wid, prop_status) in writes_being_verified.iter_mut() {
    // Add the new Select into `selects`
    let cur_write_meta = write_query_map.get(cur_wid).unwrap();
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
      if let Ok(effects) = add_combo_select(
        rand_gen,
        &mut prop_status.select_views,
        combo_status,
        &StatusPath::WriteStatus {
          wid: cur_wid.clone(),
        },
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
          cur_write_meta.tm_eid.clone(),
          cur_wid.clone(),
          cur_write_meta.timestamp.clone(),
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

// -------------------------------------------------------------------------------------------------
// Select PropertyVerificationStatus utils
// -------------------------------------------------------------------------------------------------

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

/// Create combinations of `already_reached_writes` and `committed_writes`,
/// for all Write Queries with timestamp <= `timestamp`.
fn create_partial_combos(
  committed_writes: &BTreeMap<Timestamp, WriteQueryId>,
  already_reached_writes: &BTreeMap<Timestamp, WriteQueryId>,
  timestamp: &Timestamp,
) -> Vec<BTreeMap<Timestamp, WriteQueryId>> {
  // Get the already_reached_writes with timestamp <= `timestamp`
  let mut sub_already_reached_writes = BTreeMap::new();
  for (write_timestamp, wid) in already_reached_writes.range((Unbounded, Included(timestamp))) {
    sub_already_reached_writes.insert(write_timestamp.clone(), wid.clone());
  }

  // Get the committed_writes with timestamp <= `timestamp`
  let mut sub_committed_writes = BTreeMap::new();
  for (write_timestamp, wid) in committed_writes.range((Unbounded, Included(timestamp))) {
    sub_committed_writes.insert(write_timestamp.clone(), wid.clone());
  }

  return create_combos(&sub_committed_writes, &sub_already_reached_writes);
}

/// This functions routes the given Subquery Response, whose ID was `sid`
/// and whose result was `result`, down to where it belongs in the
/// `combo_status`. It's possible that the Subquery is no longer relevent
/// for the `combo_status`; i.e. the task (WriteQueryTask or SelectQueryTask)
/// that issued the Subquery had disappeared for some reason. This is okay;
/// we just return a None when this happens. Otherwise, we return either
/// `TabletSideEffects` containing any new subqueries, or a `VerificationFailure`
/// indicating that we just discovered the Query that owns this `combo_status`
/// cannot be completed. Note that if this Subquery Response completes
/// the `combo_status`, then the calling function has to deal with this.
fn handle_subquery_response_prop(
  rand_gen: &mut RandGen,
  select_views: &mut HashMap<SelectQueryId, (SelectView, HashSet<Vec<WriteQueryId>>)>,
  combos_verified: &mut HashSet<Vec<WriteQueryId>>,
  combo_status: &mut CombinationStatus,
  selects: &BTreeMap<Timestamp, Vec<SelectQueryId>>,
  sub_sid: &SelectQueryId,
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
          &sub_sid,
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
            for (sid, (from_task, select_stmt)) in context {
              subq.insert(
                sid,
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
          if let Ok(effects) = continue_combo(
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
            side_effects.append(effects)
          } else {
            return Some(Err(VerificationFailure::VerificationFailure));
          };
        };
      }
    }
    FromCombo::Select {
      sid: cur_sid,
      from_task,
    } => {
      // This Subquery is destined for a Select Query in the combo.
      let mut select_task = combo_status.select_tasks.get_mut(cur_sid)?;
      let context = if let Ok(context) = eval_select_graph(
        rand_gen,
        &mut select_task,
        &from_task,
        &sub_sid,
        &subquery_ret,
        &combo_status.write_view,
      ) {
        context
      } else {
        return Some(Err(VerificationFailure::VerificationFailure));
      };
      match &select_task.val {
        SelectQueryTask::SelectDoneTask(view) => {
          // This means the SelectQueryTask completed without the need
          // for any more subqueries. Now, we finish up the Select Query,
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
          combo_status.select_tasks.remove(cur_sid).unwrap();
          if combo_status.select_tasks.is_empty() && combo_status.current_write.is_none() {
            // This means that the combo status is actually finished.
            combos_verified.insert(combo_status.combo.clone());
          }
        }
        _ => {
          // This means that the SelectQueryTask requires subqueries
          // to execute, so it is not complete.
          let mut subq = BTreeMap::<SelectQueryId, (FromRoot, SelectStmt)>::new();
          for (sid, (from_task, select_stmt)) in context {
            subq.insert(
              sid,
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

/// This adds a `combo_status` to `prop_status`, or returns a `VerificationFailure`
/// if trying to execute `combo_status` results in a `VerificationFailure`.
fn add_combo(
  rand_gen: &mut RandGen,
  prop_status: &mut PropertyVerificationStatus,
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
    write_view: RelationalTablet::new(relational_tablet.schema.clone()),
    current_write: None,
    select_tasks: HashMap::<SelectQueryId, Holder<SelectQueryTask>>::new(),
  };
  combo_status.combo = combo.clone();
  side_effects.append(continue_combo(
    rand_gen,
    &mut prop_status.select_views,
    &mut prop_status.combos_verified,
    &mut combo_status,
    &prop_status.selects,
    0,
    status_path,
    write_query_map,
    select_query_map,
    this_slave_eid,
    this_tablet,
  )?);
  prop_status
    .combo_status_map
    .insert(combo.clone(), combo_status);
  Ok(side_effects)
}

/// This assumes that all Write Queries in the `combo_status` whose index
/// is less than `wid_index` have been executed. At this point, `write_view`,
/// should be populated with the results of those Write Queries, and
/// `write_tasks` should be empty. Also, all Select Queries that don't rely
/// on the Write Query *prior* to `wid_index` or after it have been launched,
/// i.e. for each Select Query, there is either a job in `select_tasks`,
/// or the results of that Select Query are in `select_views`.
///
/// This function extends `wid_index` as far as possible, stopping only
/// if a Write Query launches Sub Queries. In this case, `write_task`
/// will get populated with Subqueries. If `wid_index` gets extended all
/// the way, it's possible that the whole `combo_task` is completed, in
/// which case we add it to `combos_verified`. These happen on in the happy
/// path, though. Generally, `combo_status` can fail, like if a table
/// constraint fails. In this clase, a `VerificationFailure` is returned,
/// but the input mut variables could have changed.
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
  let mut side_effects = TabletSideEffects::new();
  assert!(0 <= wid_index && wid_index <= combo_status.combo.len() as i32);
  let lower_bound = if wid_index == 0 {
    Unbounded
  } else {
    let wid = combo_status.combo.get((wid_index - 1) as usize).unwrap();
    Included(write_query_map.get(wid).unwrap().timestamp)
  };
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
            &update_stmt,
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
              // This means the InsertRowTask for the row completed
              // without the need for Subqueries. In this case, we insert
              // directly into the write_view and avoid creating a temporary
              // WriteDiff (since there is a chance we can avoid a WriteDiff
              // altogether).
              table_insert_cell(
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
              for (sid, select_stmt) in context {
                subq.insert(
                  sid,
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
          // we are now blocked by subqueries. Thus, we stop iterating
          // over Write Queries.
          upper_bound = Excluded(cur_write_meta.timestamp);
          combo_status.current_write = Some(CurrentWrite {
            wid: cur_wid.clone(),
            write_task: Holder::from(WriteQueryTask::UpdateTask(UpdateTask {
              key_tasks: update_key_tasks,
              key_vals: vec![],
            })),
          });
          break;
        }
      }
      WriteQuery::Insert(insert_stmt) => {
        assert_eq!(insert_stmt.table_name, this_tablet.path.path);
        // First do some validation of the insert_stmt.
        if verify_insert(&combo_status.write_view, &insert_stmt).is_err() {
          return Err(VerificationFailure::VerificationFailure);
        }
        // Next, construct the insert_row_tasks.
        let mut insert_row_tasks = BTreeMap::<usize, Holder<InsertRowTask>>::new();
        for index in 0..insert_stmt.insert_vals.len() {
          let (insert_row_task, context) = if let Ok(insert_task) =
            start_eval_insert_row_task(rand_gen, &insert_stmt, &combo_status.write_view, index)
          {
            insert_task
          } else {
            return Err(VerificationFailure::VerificationFailure);
          };
          match insert_row_task {
            InsertRowTask::Done(done_task) => {
              // This means the InsertRowTask for the row completed
              // without the need for Subqueries. In this case, we insert
              // directly into the write_view and avoid creating a temporary
              // WriteDiff (since there is a chance we can avoid a WriteDiff
              // altogether).
              if table_insert_row(
                &mut combo_status.write_view,
                &done_task.insert_cols,
                done_task.insert_vals,
                &cur_write_meta.timestamp,
              )
              .is_err()
              {
                return Err(VerificationFailure::VerificationFailure);
              };
            }
            _ => {
              // This means that the InsertRowTask requires subqueries
              // to execute, so it is not complete.
              let mut subq = BTreeMap::<SelectQueryId, (FromRoot, SelectStmt)>::new();
              for (sid, select_stmt) in context {
                subq.insert(
                  sid,
                  (
                    make_path(
                      status_path.clone(),
                      FromProp {
                        combo: combo_status.combo.clone(),
                        from_combo: FromCombo::Write {
                          wid: cur_wid.clone(),
                          from_task: FromWriteTask::InsertTask { index },
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
              insert_row_tasks.insert(index, Holder::from(insert_row_task));
            }
          }
        }
        if !insert_row_tasks.is_empty() {
          // This means that we can't continue applying writes, since
          // we are now blocked by subqueries. Thus, we stop iterating
          // over Write Queries.
          upper_bound = Excluded(cur_write_meta.timestamp);
          combo_status.current_write = Some(CurrentWrite {
            wid: cur_wid.clone(),
            write_task: Holder::from(WriteQueryTask::InsertTask(InsertTask {
              tasks: insert_row_tasks,
              key_vals: vec![],
            })),
          });
          break;
        }
      }
    }
    // If we get here, it means that the Write Query processed aboveaa
    // was fully processed, and that we may continue applying writes.
  }
  for (_, selects) in selects.range((lower_bound, upper_bound)) {
    for sid in selects {
      // This is a new sid that we must start handling.
      if let Ok(effects) = add_combo_select(
        rand_gen,
        select_views,
        combo_status,
        &status_path,
        sid,
        select_query_map,
        this_slave_eid,
        this_tablet,
      ) {
        side_effects.append(effects);
      } else {
        return Err(VerificationFailure::VerificationFailure);
      };
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
  orig_sid: &SelectQueryId,
  select_query_map: &HashMap<SelectQueryId, SelectQueryMetadata>,
  this_slave_eid: &EndpointId,
  this_tablet: &TabletShape,
) -> Result<TabletSideEffects, VerificationFailure> {
  let mut side_effects = TabletSideEffects::new();
  // Get the Select Query
  let orig_select_meta = select_query_map.get(orig_sid).unwrap().clone();
  let mut view = SelectView::new();
  let mut select_tasks = BTreeMap::<PrimaryKey, Holder<SelectKeyTask>>::new();
  for key in combo_status
    .write_view
    .get_keys(&orig_select_meta.timestamp)
  {
    // The selects here must be valid so this should never
    // return an error. Thus, we may just unwrap.
    let (select_key_task, context) = start_eval_select_key_task(
      rand_gen,
      orig_select_meta.select_stmt.clone(),
      &combo_status.write_view,
      &key,
      &orig_select_meta.timestamp,
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
              .get_partial_val(&key, &sel_col, &orig_select_meta.timestamp),
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
        for (orig_sid, select_stmt) in context {
          subq.insert(
            orig_sid.clone(),
            (
              make_path(
                status_path.clone(),
                FromProp {
                  combo: combo_status.combo.clone(),
                  from_combo: FromCombo::Select {
                    sid: orig_sid,
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
          &orig_select_meta.timestamp,
        );
        select_tasks.insert(key.clone(), Holder::from(select_key_task));
      }
    }
  }
  if !select_tasks.is_empty() {
    // This means we must create a SelectTask and hold in
    // the combo_status.
    combo_status.select_tasks.insert(
      orig_sid.clone(),
      Holder::from(SelectQueryTask::SelectTask(SelectTask {
        key_tasks: select_tasks,
        select_view: view,
        timestamp: orig_select_meta.timestamp.clone(),
      })),
    );
  } else {
    if let Some((select_view, combo_ids)) = select_views.get_mut(&orig_sid) {
      if select_view != &view {
        //  End everything and return an error
        return Err(VerificationFailure::VerificationFailure);
      } else {
        combo_ids.insert(combo_status.combo.clone());
      }
    } else {
      select_views.insert(
        orig_sid.clone(),
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

/// For every subset of `variable_map`, this functions merges it with
/// `fixed_map` and returns all such merged maps.
fn create_combos<K: Ord + Clone, V: Clone>(
  fixed_map: &BTreeMap<K, V>,
  variable_map: &BTreeMap<K, V>,
) -> Vec<BTreeMap<K, V>> {
  // A recursive function that performs the algorithm.
  fn create_combos_r<K: Ord + Clone, V: Clone>(
    combos: &mut Vec<BTreeMap<K, V>>,
    fixed_map: &mut BTreeMap<K, V>,
    variable_map: &mut BTreeMap<K, V>,
  ) {
    if let Some((k, v)) = variable_map.pop_first() {
      create_combos_r(combos, fixed_map, variable_map);
      fixed_map.insert(k.clone(), v.clone());
      create_combos_r(combos, fixed_map, variable_map);
      // Restore inputs.
      fixed_map.remove(&k);
      variable_map.insert(k, v);
    } else {
      combos.push(fixed_map.clone());
    }
  }

  let mut combos = vec![];
  create_combos_r(
    &mut combos,
    &mut fixed_map.clone(),
    &mut variable_map.clone(),
  );
  return combos;
}

#[test]
fn create_combos_test() {
  let fixed_map = BTreeMap::from_iter(vec![(1, 10), (4, 11)].into_iter());
  let variable_map = BTreeMap::from_iter(vec![(2, 12), (3, 13)].into_iter());
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
