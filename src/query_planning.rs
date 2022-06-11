use crate::col_usage::{iterate_stage_ms_query, GeneralStage};
use crate::common::{
  lookup, ColName, FullGen, Gen, TablePath, TableSchema, TierMap, Timestamp, TransTableName,
};
use crate::master_query_planning_es::{
  DBSchemaView, ReqColPresenceError, ReqTablePresenceError, StaticValidationErrorTrait,
};
use crate::multiversion_map::MVM;
use crate::sql_ast::proc;
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};

/// Gather every reference to a `TablePath` found in the `query`.
pub fn collect_table_paths(query: &proc::MSQuery) -> BTreeSet<TablePath> {
  let mut table_paths = BTreeSet::<TablePath>::new();
  iterate_stage_ms_query(
    &mut |stage: GeneralStage| match stage {
      GeneralStage::SuperSimpleSelect(query) => {
        // TODO: do properly.
        if let proc::GeneralSource::TablePath { table_path, .. } = &query.from {
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

/// Computes a map that maps all `TablePath`s used in the MSQuery to the `Gen`
/// in the `table_generation` at `timestamp`.
///
/// Precondition:
///   1. All `TablePath`s in the MSQuery must have a non-None `Gen` in `table_generation`.
pub fn compute_table_location_map<ViewT: DBSchemaView>(
  view: &mut ViewT,
  table_paths: &BTreeSet<TablePath>,
) -> Result<BTreeMap<TablePath, FullGen>, ViewT::ErrorT> {
  let mut table_location_map = BTreeMap::<TablePath, FullGen>::new();
  for table_path in table_paths {
    table_location_map.insert(table_path.clone(), view.get_gen(table_path)?);
  }
  Ok(table_location_map)
}

pub enum StaticValidationError {
  InvalidUpdate,
  InvalidInsert,
}

/// Validates the `MSQuery` in various ways. In particularly, this checks whether the
/// columnt that are written by an Insert or Update are valid and present in `view`.
pub fn perform_validations<
  ErrorT: StaticValidationErrorTrait + ReqColPresenceError,
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
        // all assigned columns are unique, and they are present.
        let table_path = &query.table.source_ref;
        let key_cols = view.key_cols(table_path)?.clone();
        let mut all_cols = BTreeSet::<&ColName>::new();
        for (col_name, _) in &query.assignment {
          if !all_cols.insert(col_name) || lookup(&key_cols, col_name).is_some() {
            return Err(StaticValidationErrorTrait::mk_error(StaticValidationError::InvalidUpdate));
          }
          if !view.contains_col(table_path, col_name)? {
            return Err(ReqColPresenceError::mk_error(col_name.clone()));
          }
        }
      }
      proc::MSQueryStage::Insert(query) => {
        // Check that the `stage` is inserting to all KeyCols.
        let table_path = &query.table.source_ref;
        let key_cols = view.key_cols(table_path)?;
        for (col_name, _) in key_cols {
          if !query.columns.contains(col_name) {
            return Err(StaticValidationErrorTrait::mk_error(StaticValidationError::InvalidInsert));
          }
        }

        // Check that every inserted column is present
        for col_name in &query.columns {
          if !view.contains_col(table_path, col_name)? {
            return Err(ReqColPresenceError::mk_error(col_name.clone()));
          }
        }

        // Check that all assigned columns are unique.
        let mut all_cols = BTreeSet::<&ColName>::new();
        for col_name in &query.columns {
          if !all_cols.insert(col_name) {
            return Err(StaticValidationErrorTrait::mk_error(StaticValidationError::InvalidInsert));
          }
        }

        // Check that `values` has equal length to `columns`.
        for row in &query.values {
          if row.len() != query.columns.len() {
            return Err(StaticValidationErrorTrait::mk_error(StaticValidationError::InvalidInsert));
          }
        }
      }
      proc::MSQueryStage::Delete(_) => {}
    }
  }

  Ok(())
}
