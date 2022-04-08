use crate::col_usage::{iterate_stage_ms_query, GeneralStage};
use crate::common::{lookup, TableSchema, Timestamp};
use crate::master_query_planning_es::{
  DBSchemaView, ReqColPresenceError, ReqTablePresenceError, StaticValidationErrorTrait,
};
use crate::model::common::proc::MSQueryStage;
use crate::model::common::TablePath;
use crate::model::common::{proc, ColName, Gen, TierMap, TransTableName};
use crate::multiversion_map::MVM;
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};

/// Gather every reference to a `TablePath` found in the `query`.
pub fn collect_table_paths(query: &proc::MSQuery) -> BTreeSet<TablePath> {
  let mut table_paths = BTreeSet::<TablePath>::new();
  iterate_stage_ms_query(
    &mut |stage: GeneralStage| match stage {
      GeneralStage::SuperSimpleSelect(query) => {
        if let proc::GeneralSourceRef::TablePath(table_path) = &query.from.source_ref {
          table_paths.insert(table_path.clone());
        }
      }
      GeneralStage::Update(query) => {
        table_paths.insert(query.table.source_ref.clone());
      }
      GeneralStage::Insert(query) => {
        table_paths.insert(query.table.source_ref.clone());
      }
      GeneralStage::Delete(query) => {
        table_paths.insert(query.table.source_ref.clone());
      }
    },
    query,
  );
  table_paths
}

/// Compute the `TierMap` for every stage in the `MSQuery`. A `TablePath` should appear
/// in a `TierMap` iff it is written to by the `MSQuery`.
///
/// The `TierMap` for a stage contains the Tiers that should be used to read the `TablePath`s
/// inside. Note that if a stage is a write (e.g. an Update), the Tier of the written `TablePath`
/// in the `TierMap` is one behind (i.e. one more) the Tier that the write should commit at.
pub fn compute_all_tier_maps(ms_query: &proc::MSQuery) -> BTreeMap<TransTableName, TierMap> {
  let mut all_tier_maps = BTreeMap::<TransTableName, TierMap>::new();
  let mut cur_tier_map = BTreeMap::<TablePath, u32>::new();
  for (_, stage) in &ms_query.trans_tables {
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(_) => {}
      proc::MSQueryStage::Update(update) => {
        cur_tier_map.insert(update.table.source_ref.clone(), 0);
      }
      proc::MSQueryStage::Insert(insert) => {
        cur_tier_map.insert(insert.table.source_ref.clone(), 0);
      }
      proc::MSQueryStage::Delete(delete) => {
        cur_tier_map.insert(delete.table.source_ref.clone(), 0);
      }
    }
  }
  for (trans_table_name, stage) in ms_query.trans_tables.iter().rev() {
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(_) => {}
      proc::MSQueryStage::Update(update) => {
        *cur_tier_map.get_mut(&update.table.source_ref).unwrap() += 1;
      }
      proc::MSQueryStage::Insert(insert) => {
        *cur_tier_map.get_mut(&insert.table.source_ref).unwrap() += 1;
      }
      proc::MSQueryStage::Delete(delete) => {
        *cur_tier_map.get_mut(&delete.table.source_ref).unwrap() += 1;
      }
    }
    all_tier_maps.insert(trans_table_name.clone(), TierMap { map: cur_tier_map.clone() });
  }
  all_tier_maps
}

/// Computes `extra_req_cols`, which is a class of columns that must be present in the
/// Tablets according to the MSQuery. The presence of these columns need to be validated
/// before other algorithms can run, e.g. `ColUsagePlanner`. Note that KeyCols of a
/// Table can also be here.
pub fn compute_extra_req_cols(ms_query: &proc::MSQuery) -> BTreeMap<TablePath, Vec<ColName>> {
  let mut extra_req_cols = BTreeMap::<TablePath, Vec<ColName>>::new();

  // Helper to add extra columns to `extra_req_cols` which avoids duplicating
  // ColNames that are already present.
  fn add_cols(
    extra_req_cols: &mut BTreeMap<TablePath, Vec<ColName>>,
    table_path: &TablePath,
    col_names: Vec<ColName>,
  ) {
    // Recall there might already be required columns for this TablePath.
    if !extra_req_cols.contains_key(table_path) {
      extra_req_cols.insert(table_path.clone(), Vec::new());
    }
    let req_cols = extra_req_cols.get_mut(table_path).unwrap();
    for col_name in col_names {
      if !req_cols.contains(&col_name) {
        req_cols.push(col_name);
      }
    }
  }

  iterate_stage_ms_query(
    &mut |stage: GeneralStage| match stage {
      GeneralStage::SuperSimpleSelect(_) => {}
      GeneralStage::Update(query) => {
        add_cols(
          &mut extra_req_cols,
          &query.table.source_ref,
          query.assignment.iter().map(|(c, _)| c).cloned().collect(),
        );
      }
      GeneralStage::Insert(query) => {
        add_cols(&mut extra_req_cols, &query.table.source_ref, query.columns.clone());
      }
      GeneralStage::Delete(_) => {}
    },
    ms_query,
  );

  extra_req_cols
}

/// Computes a map that maps all `TablePath`s used in the MSQuery to the `Gen`
/// in the `table_generation` at `timestamp`.
///
/// Precondition:
///   1. All `TablePath`s in the MSQuery must have a non-None `Gen` in `table_generation`.
pub fn compute_table_location_map<ViewT: DBSchemaView>(
  view: &mut ViewT,
  table_paths: &BTreeSet<TablePath>,
) -> Result<BTreeMap<TablePath, Gen>, ViewT::ErrorT> {
  let mut table_location_map = BTreeMap::<TablePath, Gen>::new();
  for table_path in table_paths {
    table_location_map.insert(table_path.clone(), view.get_gen(table_path)?);
  }
  Ok(table_location_map)
}

pub enum StaticValidationError {
  InvalidUpdate,
  InvalidInsert,
}

/// This function performs validations of `ms_query` that requries nothing more than
/// calling `view.key_cols` for a given `TablePath`.
pub fn perform_static_validations<
  ErrorT: StaticValidationErrorTrait,
  ViewT: DBSchemaView<ErrorT = ErrorT>,
>(
  view: &mut ViewT,
  ms_query: &proc::MSQuery,
) -> Result<(), ErrorT> {
  for (_, stage) in &ms_query.trans_tables {
    match stage {
      proc::MSQueryStage::SuperSimpleSelect(_) => {}
      proc::MSQueryStage::Update(query) => {
        // Check that the `stage` is not trying to modify a KeyCol,
        // and all assigned columns are unique.
        let key_cols = view.key_cols(&query.table.source_ref)?;
        let mut all_cols = BTreeSet::<&ColName>::new();
        for (col_name, _) in &query.assignment {
          if !all_cols.insert(col_name) || lookup(key_cols, col_name).is_some() {
            return Err(ErrorT::mk_error(StaticValidationError::InvalidUpdate));
          }
        }
      }
      proc::MSQueryStage::Insert(query) => {
        // Check that the `stage` is inserting to all KeyCols.
        let key_cols = view.key_cols(&query.table.source_ref)?;
        for (col_name, _) in key_cols {
          if !query.columns.contains(col_name) {
            return Err(ErrorT::mk_error(StaticValidationError::InvalidInsert));
          }
        }

        // Check that all assigned columns are unique.
        let mut all_cols = BTreeSet::<&ColName>::new();
        for col_name in &query.columns {
          if !all_cols.insert(col_name) {
            return Err(ErrorT::mk_error(StaticValidationError::InvalidInsert));
          }
        }

        // Check that `values` has equal length to `columns`.
        for row in &query.values {
          if row.len() != query.columns.len() {
            return Err(ErrorT::mk_error(StaticValidationError::InvalidInsert));
          }
        }
      }
      proc::MSQueryStage::Delete(_) => {}
    }
  }

  Ok(())
}

/// Checks if all `ColName`s in the value of `extra_req_cols` are contained in
/// the `TablePath` in the key.
pub fn check_cols_present<ErrorT: ReqColPresenceError, ViewT: DBSchemaView<ErrorT = ErrorT>>(
  view: &mut ViewT,
  table_col_map: &BTreeMap<TablePath, Vec<ColName>>,
) -> Result<(), ErrorT> {
  for (table_path, col_names) in table_col_map {
    for col_name in col_names {
      if !view.contains_col(table_path, col_name)? {
        return Err(ErrorT::mk_error(col_name.clone()));
      }
    }
  }

  Ok(())
}
