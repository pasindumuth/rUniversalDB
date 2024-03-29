use crate::col_usage::{QueryElement, QueryIterator};
use crate::common::{
  lookup, ColName, FullGen, Gen, TablePath, TableSchema, TierMap, Timestamp, TransTableName,
};
use crate::master_query_planning_es::{DBSchemaView, ErrorTrait};
use crate::message as msg;
use crate::multiversion_map::MVM;
use crate::sql_ast::proc;
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};

/// Gather every reference to a `TablePath` found in the `query`.
pub fn collect_table_paths(query: &proc::MSQuery) -> BTreeSet<TablePath> {
  let mut table_paths = BTreeSet::<TablePath>::new();
  QueryIterator::new().iterate_ms_query(
    &mut |stage: QueryElement| match stage {
      QueryElement::TableSelect(query) => {
        table_paths.insert(query.from.table_path.clone());
      }
      QueryElement::TransTableSelect(_) => {}
      QueryElement::JoinSelect(_) => {}
      QueryElement::JoinNode(_) => {}
      QueryElement::JoinLeaf(_) => {}
      QueryElement::TableSelect(query) => {
        table_paths.insert(query.from.table_path.clone());
      }
      QueryElement::Update(query) => {
        table_paths.insert(query.table.table_path.clone());
      }
      QueryElement::Insert(query) => {
        table_paths.insert(query.table.table_path.clone());
      }
      QueryElement::Delete(query) => {
        table_paths.insert(query.table.table_path.clone());
      }
      QueryElement::ValExpr(_) => {}
      QueryElement::MSQuery(_) => {}
      QueryElement::GRQuery(_) => {}
      QueryElement::GRQueryStage(_) => {}
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
      proc::MSQueryStage::TableSelect(_) => {}
      proc::MSQueryStage::TransTableSelect(_) => {}
      proc::MSQueryStage::JoinSelect(_) => {}
      proc::MSQueryStage::Update(update) => {
        cur_tier_map.insert(update.table.table_path.clone(), 0);
      }
      proc::MSQueryStage::Insert(insert) => {
        cur_tier_map.insert(insert.table.table_path.clone(), 0);
      }
      proc::MSQueryStage::Delete(delete) => {
        cur_tier_map.insert(delete.table.table_path.clone(), 0);
      }
    }
  }
  for (trans_table_name, stage) in ms_query.trans_tables.iter().rev() {
    match stage {
      proc::MSQueryStage::TableSelect(_) => {}
      proc::MSQueryStage::TransTableSelect(_) => {}
      proc::MSQueryStage::JoinSelect(_) => {}
      proc::MSQueryStage::Update(update) => {
        *cur_tier_map.get_mut(&update.table.table_path).unwrap() += 1;
      }
      proc::MSQueryStage::Insert(insert) => {
        *cur_tier_map.get_mut(&insert.table.table_path).unwrap() += 1;
      }
      proc::MSQueryStage::Delete(delete) => {
        *cur_tier_map.get_mut(&delete.table.table_path).unwrap() += 1;
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

/// Validates the `MSQuery` in various ways. In particularly, this checks whether the
/// columns that are written by an Insert or Update are valid and present in `view`.
pub fn perform_validations<ErrorT: ErrorTrait, ViewT: DBSchemaView<ErrorT = ErrorT>>(
  view: &mut ViewT,
  ms_query: &proc::MSQuery,
) -> Result<(), ErrorT> {
  for (_, stage) in &ms_query.trans_tables {
    match stage {
      proc::MSQueryStage::TableSelect(_) => {}
      proc::MSQueryStage::TransTableSelect(_) => {}
      proc::MSQueryStage::JoinSelect(_) => {}
      proc::MSQueryStage::Update(query) => {
        // Check that the `stage` is not trying to modify a KeyCol,
        // all assigned columns are unique, and they are present.
        let table_path = &query.table.table_path;
        let key_cols = view.key_cols(table_path)?.clone();
        let mut all_cols = BTreeSet::<&ColName>::new();
        for (col_name, _) in &query.assignment {
          if !all_cols.insert(col_name) || lookup(&key_cols, col_name).is_some() {
            return Err(ErrorTrait::mk_error(msg::QueryPlanningError::InvalidUpdate));
          }
          if !view.contains_col(table_path, col_name)? {
            return Err(ErrorTrait::mk_error(msg::QueryPlanningError::RequiredColumnDNE(
              col_name.clone(),
            )));
          }
        }
      }
      proc::MSQueryStage::Insert(query) => {
        // Check that the `stage` is inserting to all KeyCols.
        let table_path = &query.table.table_path;
        let key_cols = view.key_cols(table_path)?;
        for (col_name, _) in key_cols {
          if !query.columns.contains(col_name) {
            return Err(ErrorTrait::mk_error(msg::QueryPlanningError::InvalidInsert));
          }
        }

        // Check that every inserted column is present
        for col_name in &query.columns {
          if !view.contains_col(table_path, col_name)? {
            return Err(ErrorTrait::mk_error(msg::QueryPlanningError::RequiredColumnDNE(
              col_name.clone(),
            )));
          }
        }

        // Check that all assigned columns are unique.
        let mut all_cols = BTreeSet::<&ColName>::new();
        for col_name in &query.columns {
          if !all_cols.insert(col_name) {
            return Err(ErrorTrait::mk_error(msg::QueryPlanningError::InvalidInsert));
          }
        }

        // Check that `values` has equal length to `columns`.
        for row in &query.values {
          if row.len() != query.columns.len() {
            return Err(ErrorTrait::mk_error(msg::QueryPlanningError::InvalidInsert));
          }
        }
      }
      proc::MSQueryStage::Delete(_) => {}
    }
  }

  Ok(())
}
