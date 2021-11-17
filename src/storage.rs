use crate::common::{
  lookup, lookup_pos, ColBound, KeyBound, PolyColBound, SingleBound, TableSchema,
};
use crate::model::common::{ColName, ColType, ColVal, ColValN, PrimaryKey, TableView, Timestamp};
use std::collections::{BTreeMap, Bound};

// -----------------------------------------------------------------------------------------------
//  Storage Containers
// -----------------------------------------------------------------------------------------------

/// The multi-versioned container used to hold committed data. We refer to the tuple
/// `(PrimaryKey, Option<ColName>)` as the "Storage Key". We refer to the `Option<ColName>`
/// in the Storage Key as the "Column Indicator". We refer to a Storage Key-Value pair
/// as a Storage Row. We refer to a Storage Row where the Column Indicator is `None` as
/// a "Presence Row". The Storage Value in the Presence Row is `None` or `PRESENCE_VALN`,
/// indicate that the row is absent or present respectively.
pub type GenericMVTable = BTreeMap<(PrimaryKey, Option<ColName>), Vec<(Timestamp, ColValN)>>;
/// A single-versioned version of the above to hold views constructed by a write. We use
/// similar terminology to `GenericMVTable` to describe this.
pub type GenericTable = BTreeMap<(PrimaryKey, Option<ColName>), ColValN>;

/// A constant that indicates that a row is present.
pub const PRESENCE_VALN: ColValN = Some(ColVal::Int(0));

/// Finds the version at the given `timestamp`.
pub fn find_version<V>(versions: &Vec<(Timestamp, Option<V>)>, timestamp: Timestamp) -> &Option<V> {
  for (t, value) in versions.iter().rev() {
    if *t <= timestamp {
      return value;
    }
  }
  return &None;
}

/// Reads the version prior to the timestamp. This doesn't mutate
/// the lat if the read happens with a future timestamp.
pub fn static_read<'a>(
  mvt: &'a GenericMVTable,
  key: &(PrimaryKey, Option<ColName>),
  timestamp: Timestamp,
) -> &'a ColValN {
  if let Some(versions) = mvt.get(key) {
    find_version(versions, timestamp)
  } else {
    &None
  }
}

/// The RangeBound used to do range queries in Generic(MV)Table
type RangeQuery = (Bound<(PrimaryKey, Option<ColName>)>, Bound<(PrimaryKey, Option<ColName>)>);

// -----------------------------------------------------------------------------------------------
//  StorageView
// -----------------------------------------------------------------------------------------------

/// A trait for reading subtables from some kind of underlying table data view. The `key_region`
/// indicates the set of rows to read from the Table, and the `col_region` are the columns to read.
///
/// This function must a pure function; the order of the returned vectors matter.
pub trait StorageView {
  fn compute_subtable(
    &self,
    key_region: &Vec<KeyBound>,
    col_region: &Vec<ColName>,
    timestamp: &Timestamp,
  ) -> Vec<(Vec<ColValN>, u64)>;
}

// -----------------------------------------------------------------------------------------------
//  SimpleStorageView
// -----------------------------------------------------------------------------------------------

/// This is used to directly read data from persistent storage.
pub struct SimpleStorageView<'a> {
  storage: &'a GenericMVTable,
  table_schema: &'a TableSchema,
}

impl<'a> SimpleStorageView<'a> {
  pub fn new(storage: &'a GenericMVTable, table_schema: &'a TableSchema) -> SimpleStorageView<'a> {
    SimpleStorageView { storage, table_schema }
  }
}

impl<'a> StorageView for SimpleStorageView<'a> {
  /// Here, note that `key_region` doesn't need to be disjoint.
  fn compute_subtable(
    &self,
    key_region: &Vec<KeyBound>,
    column_region: &Vec<ColName>,
    timestamp: &Timestamp,
  ) -> Vec<(Vec<ColValN>, u64)> {
    // The general strategy that we use to extract a subtable for each `KeyBound` in `key_region`
    // is to take the lower bound, get an iterator to it in `storage`, and then iterate forward,
    // adding in every row that's within the `KeyBound` until we are beyond the upper bound of
    // `KeyBound`. We first construct a `GenericTable` containing rows for every KeyBound,
    // and then finally convert that to the return type.
    let mut snapshot_table = GenericTable::new();
    for key_bound in key_region {
      // Iterate over the the Storage Rows and insert them into `snapshot_table` if they
      // are within `key_bound` and aren't shadowed by an existing entry.
      let mut it = self.storage.range(compute_range_query(key_bound));
      let mut entry = it.next();
      while let Some(((pkey, ci), _)) = entry {
        assert!(ci.is_none()); // Recall that this should be the Presence Storage Row of `pkey`.
        match check_inclusion(key_bound, pkey) {
          KeyBoundInclusionResult::Included => {
            // This row is present within the `key_bound`, so we add it to `snapshot_table`.
            entry = it.next();
            while let Some(((_, Some(col_name)), versions)) = entry {
              let storage_key = (pkey.clone(), Some(col_name.clone()));
              if !snapshot_table.contains_key(&storage_key) {
                snapshot_table.insert(storage_key, find_version(versions, *timestamp).clone());
              }
              entry = it.next();
            }
          }
          KeyBoundInclusionResult::Done => break,
          KeyBoundInclusionResult::Excluded => {
            // Skip this row
            entry = it.next();
            while let Some(((_, Some(_)), _)) = entry {
              entry = it.next();
            }
          }
        }
      }
    }

    convert_to_table_view(snapshot_table, self.table_schema, column_region)
      .rows
      .into_iter()
      .collect()
  }
}

// -----------------------------------------------------------------------------------------------
//  MSStorageView
// -----------------------------------------------------------------------------------------------

/// This reads data by first replaying the Update Views on top of the persisted data.
pub struct MSStorageView<'a> {
  storage: &'a GenericMVTable,
  table_schema: &'a TableSchema,
  update_views: &'a BTreeMap<u32, GenericTable>,
  tier: u32,
}

impl<'a> MSStorageView<'a> {
  /// Here, in addition to the persistent storage, we pass in the `update_views` that
  /// we want to apply on top before reading data, and a `tier` to indicate up to which
  /// update should be applied.
  pub fn new(
    storage: &'a GenericMVTable,
    table_schema: &'a TableSchema,
    update_views: &'a BTreeMap<u32, GenericTable>,
    tier: u32,
  ) -> MSStorageView<'a> {
    MSStorageView { storage, table_schema, update_views, tier }
  }
}

impl<'a> StorageView for MSStorageView<'a> {
  /// Here, note that `key_region` doesn't need to be disjoint.
  fn compute_subtable(
    &self,
    key_region: &Vec<KeyBound>,
    column_region: &Vec<ColName>,
    timestamp: &Timestamp,
  ) -> Vec<(Vec<ColValN>, u64)> {
    // Here, we first compress all `update_views`, along with the `storage`,
    // to an auxiliary GenericTable only containing the relevent keys in `key_region`.
    // Then, we do something similar to `SimpleStorageView` to convert it to the return
    // type (via TableView).
    let mut snapshot_table = GenericTable::new();
    for key_bound in key_region {
      let bound = compute_range_query(key_bound);
      // First, apply the UpdateViews to `snapshot_table`.
      let tier_bound = (Bound::Included(self.tier), Bound::Unbounded);
      for (_, generic_table) in self.update_views.range(tier_bound) {
        // Iterate over the the UpdateView Rows and insert them into `snapshot_table` if they
        // are within `key_bound` and aren't shadowed by an existing entry.
        let mut it = generic_table.range(bound.clone());
        let mut entry = it.next();
        while let Some(((pkey, ci), _)) = entry {
          assert!(ci.is_none()); // Recall that this should be the Presence Storage Row of `pkey`.
          match check_inclusion(key_bound, pkey) {
            KeyBoundInclusionResult::Included => {
              // This row is present within the `key_bound`, so we add it to `snapshot_table`.
              entry = it.next();
              while let Some(((_, Some(col_name)), val)) = entry {
                let storage_key = (pkey.clone(), Some(col_name.clone()));
                if !snapshot_table.contains_key(&storage_key) {
                  snapshot_table.insert(storage_key, val.clone());
                }
                entry = it.next();
              }
            }
            KeyBoundInclusionResult::Done => break,
            KeyBoundInclusionResult::Excluded => {
              // Skip this row
              entry = it.next();
              while let Some(((_, Some(_)), _)) = entry {
                entry = it.next();
              }
            }
          }
        }
      }

      // Iterate over the the Storage Rows and insert them into `snapshot_table` if they
      // are within `key_bound` and aren't shadowed by an existing entry.
      let mut it = self.storage.range(bound.clone());
      let mut entry = it.next();
      while let Some(((pkey, ci), _)) = entry {
        assert!(ci.is_none()); // Recall that this should be the Presence Storage Row of `pkey`.
        match check_inclusion(key_bound, pkey) {
          KeyBoundInclusionResult::Included => {
            // This row is present within the `key_bound`, so we add it to `snapshot_table`.
            entry = it.next();
            while let Some(((_, Some(col_name)), versions)) = entry {
              let storage_key = (pkey.clone(), Some(col_name.clone()));
              if !snapshot_table.contains_key(&storage_key) {
                snapshot_table.insert(storage_key, find_version(versions, *timestamp).clone());
              }
              entry = it.next();
            }
          }
          KeyBoundInclusionResult::Done => break,
          KeyBoundInclusionResult::Excluded => {
            // Skip this row
            entry = it.next();
            while let Some(((_, Some(_)), _)) = entry {
              entry = it.next();
            }
          }
        }
      }
    }

    convert_to_table_view(snapshot_table, self.table_schema, column_region)
      .rows
      .into_iter()
      .collect()
  }
}

/// Compress the `update_views` by iterating from latest to earliest tier.
pub fn compress_updates_views(update_views: &BTreeMap<u32, GenericTable>) -> GenericTable {
  let mut snapshot_table = GenericTable::new();
  for (_, generic_table) in update_views.iter() {
    // Iterate over the the UpdateView Rows and insert them into `snapshot_table` if they
    // are within `key_bound` and aren't shadowed by an existing entry.
    let mut it = generic_table.iter();
    let mut entry = it.next();
    while let Some(((pkey, ci), _)) = entry {
      assert!(ci.is_none()); // Recall that this should be the Presence Storage Row of `pkey`.
      entry = it.next();
      while let Some(((_, Some(col_name)), val)) = entry {
        let storage_key = (pkey.clone(), Some(col_name.clone()));
        if !snapshot_table.contains_key(&storage_key) {
          snapshot_table.insert(storage_key, val.clone());
        }
        entry = it.next();
      }
    }
  }
  snapshot_table
}

/// Apply the `compressed_view` to `storage` and `timestamp`.
pub fn commit_to_storage(
  storage: &mut GenericMVTable,
  timestamp: &Timestamp,
  compressed_view: GenericTable,
) {
  for (key, value) in compressed_view {
    // Recall that since MSWriteES does Type Checking, the Compressed View can be applied
    // directly to `storage` without further checks.
    if let Some(versions) = storage.get_mut(&key) {
      // We do a sanity check that that the Region Isolation Algorithm did its job.
      let (last_timestamp, _) = versions.last().unwrap();
      assert!(timestamp > last_timestamp);
      versions.push((*timestamp, value));
    } else {
      storage.insert(key, vec![(*timestamp, value)]);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Subtable Utils
// -----------------------------------------------------------------------------------------------

/// The return value of `check_inclusion`.
enum KeyBoundInclusionResult {
  Included,
  Excluded,
  Done,
}

/// Computes whether `pkey` is in `key_bound`, returning the appropriate enum value.
/// Importantly, if not, we return `Done` only if we can definitively tell `pkey` is
/// beyond the upper bound of `key_bound`.
fn check_inclusion(key_bound: &KeyBound, pkey: &PrimaryKey) -> KeyBoundInclusionResult {
  // This boolean true only if all prior `pkey` ColVals that we iterated over are >= all of
  // possible ColVals that are within the corresponding ColBound in `key_bound`. When in doubt,
  // we should set this to false.
  let mut prefix_ge = true;

  // This is called when we determined the `pkey` is not in `key_bound`. We can use
  // `prefix_ge` to tell if the `pkey` is beyond `key_bound`.
  fn unincluded_res(prefix_ge: &bool) -> KeyBoundInclusionResult {
    if *prefix_ge {
      KeyBoundInclusionResult::Done
    } else {
      KeyBoundInclusionResult::Excluded
    }
  }

  // Here, we are checking inclusion for a column at index `i`. `val` is the ColVal in
  // the `pkey`, `col_bound` is the ColBound in `key_bound`, and `prefix_ge` takes on
  // the value it has accumulated so far. If we determine `pkey` isn't in `key_bound`, we
  // return the appropriate `KeyBoundInclusionResult`. If it's indeterminate, we return `None`.
  // We also update `prefix_ge` accordingly.
  fn check_col_inclusion<T: PartialOrd>(
    prefix_ge: &mut bool,
    val: &T,
    col_bound: &ColBound<T>,
  ) -> Option<KeyBoundInclusionResult> {
    match &col_bound.start {
      SingleBound::Included(start_val) if val < start_val => {
        *prefix_ge = false;
        return Some(unincluded_res(prefix_ge));
      }
      SingleBound::Excluded(start_val) if val <= start_val => {
        *prefix_ge = false;
        return Some(unincluded_res(prefix_ge));
      }
      _ => {}
    }
    match &col_bound.end {
      SingleBound::Included(end_val) => {
        if val > end_val {
          return Some(unincluded_res(prefix_ge));
        } else if val < end_val {
          *prefix_ge = false;
        }
      }
      SingleBound::Excluded(end_val) => {
        if val >= end_val {
          return Some(unincluded_res(prefix_ge));
        } else {
          // Here, although it's possible that `prefix_ge` can still remain true (like
          // if `end_val` is 10 but `val` is 9), for simplicity, we set it to `false`.
          *prefix_ge = false;
        }
      }
      SingleBound::Unbounded => *prefix_ge = false,
    }
    None
  }

  for (i, col_val) in pkey.cols.iter().enumerate() {
    let bound = key_bound.col_bounds.get(i).unwrap();
    match (col_val, bound) {
      (ColVal::Int(val), PolyColBound::Int(col_bound)) => {
        if let Some(res) = check_col_inclusion(&mut prefix_ge, val, col_bound) {
          return res;
        }
      }
      (ColVal::Bool(val), PolyColBound::Bool(col_bound)) => {
        if let Some(res) = check_col_inclusion(&mut prefix_ge, val, col_bound) {
          return res;
        }
      }
      (ColVal::String(val), PolyColBound::String(col_bound)) => {
        if let Some(res) = check_col_inclusion(&mut prefix_ge, val, col_bound) {
          return res;
        }
      }
      _ => panic!(),
    }
  }

  // If we get here, that means the `pkey` must be in `key_bound`.
  KeyBoundInclusionResult::Included
}

/// Computes a BTeeMap range query such that the upper bound is unbounded, and where the
/// lower bound is as high as possible yet still encompasses all of `keybound`.
fn compute_range_query(key_bound: &KeyBound) -> RangeQuery {
  // To get a Bound that's immediately below all Storage Keys of interest,
  // we simply use the prefix of the lower bound of `key_bound` where ColBound is not
  // Unbounded. Observe that the lexicographical ordering of vectors helps us here.
  let mut start_prefix = Vec::<ColVal>::new();
  for col_bound in &key_bound.col_bounds {
    match col_bound {
      PolyColBound::Int(col_bound) => match &col_bound.start {
        SingleBound::Included(v) | SingleBound::Excluded(v) => {
          start_prefix.push(ColVal::Int(v.clone()));
        }
        _ => break,
      },
      PolyColBound::String(col_bound) => match &col_bound.start {
        SingleBound::Included(v) | SingleBound::Excluded(v) => {
          start_prefix.push(ColVal::String(v.clone()));
        }
        _ => break,
      },
      PolyColBound::Bool(col_bound) => match &col_bound.start {
        SingleBound::Included(v) | SingleBound::Excluded(v) => {
          start_prefix.push(ColVal::Bool(v.clone()));
        }
        _ => break,
      },
    }
  }

  (Bound::Included((PrimaryKey::new(start_prefix), None)), Bound::Unbounded)
}

/// Select only the columns in `selection` from the other arguments.
fn select_row(
  pkey: &PrimaryKey,
  key_cols: &Vec<(ColName, ColType)>,
  val_cols: &Vec<(ColName, ColValN)>,
  selection: &Vec<ColName>,
) -> Vec<ColValN> {
  let mut row = Vec::<ColValN>::new();
  for col_name in selection {
    if let Some(col_val) = lookup_pos(key_cols, &col_name) {
      // `col_name` is a KeyCol.
      row.push(Some(pkey.cols.get(col_val).unwrap().clone()));
    } else if let Some(col_val) = lookup(val_cols, &col_name) {
      // `col_name` is a ValCol that has a value in storage.
      row.push(col_val.clone());
    } else {
      // `col_name` is a ValCol that does not not have a value in storage. This means
      // that its value is `None`, abstractly speaking.
      row.push(None);
    }
  }
  row
}

/// Convert a `GenericTable` to a `TableView` by selecting the columns in `selection`.
fn convert_to_table_view(
  snapshot_table: GenericTable,
  table_schema: &TableSchema,
  selection: &Vec<ColName>,
) -> TableView {
  let mut table_view = TableView::new(selection.clone());
  let mut it = snapshot_table.iter();
  let mut entry = it.next();
  while let Some(((pkey, ci), val)) = entry {
    assert!(ci.is_none()); // Recall that this should be the Presence Storage Row of `pkey`.
    let is_row_present = val.is_some();
    if is_row_present {
      // This row is both present and falls within the `key_bound`,
      // so we add it to `table_view`.
      let mut col_name_vals = Vec::<(ColName, ColValN)>::new();
      entry = it.next();
      while let Some(((_, Some(col_name)), val)) = entry {
        col_name_vals.push((col_name.clone(), val.clone()));
        entry = it.next();
      }

      table_view.add_row(select_row(&pkey, &table_schema.key_cols, &col_name_vals, &selection));
    } else {
      // Skip this row
      entry = it.next();
      while let Some(((_, Some(_)), _)) = entry {
        entry = it.next();
      }
    }
  }
  table_view
}
