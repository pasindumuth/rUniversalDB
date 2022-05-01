use crate::common::{
  lookup, lookup_pos, ColBound, KeyBound, PolyColBound, SingleBound, TableSchema, TabletKeyRange,
  Timestamp,
};
use crate::common::{ColName, ColType, ColVal, ColValN, PrimaryKey, TableView};
use std::cmp::max;
use std::collections::{BTreeMap, Bound};

#[cfg(test)]
#[path = "test/storage_test.rs"]
mod storage_test;

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

/// The type of Presence Snapshot, which is the fundamental object whose idempotence is preserved.
pub type PresenceSnapshot = BTreeMap<PrimaryKey, Vec<(ColName, ColValN)>>;

/// A constant that indicates that a row is present.
pub const PRESENCE_VALN: ColValN = Some(ColVal::Int(0));

/// Finds the Non-Strict Prior Version in `versions` at `timestamp`.
pub fn find_version<'a, V>(
  versions: &'a Vec<(Timestamp, Option<V>)>,
  timestamp: &Timestamp,
) -> Option<&'a (Timestamp, Option<V>)> {
  for version in versions.iter().rev() {
    let (t, _) = version;
    if t <= timestamp {
      return Some(version);
    }
  }
  return None;
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
  fn table_schema(&self) -> &TableSchema;
  fn storage(&self) -> &GenericMVTable;

  /// Combines `storage` along with potential updates to compute a `PresenceSnapshot`.
  fn compute_presence_snapshot(
    &self,
    key_region: &Vec<KeyBound>,
    val_cols: &Vec<ColName>,
    timestamp: &Timestamp,
  ) -> PresenceSnapshot;

  /// Amend `all_prior_versions` by adding the prior versions in `storage` over the given
  /// `key_bound` and `val_cols` at `timestamp`.
  ///
  /// The general strategy that we use to extract a subtable for `KeyBound` in `key_region` is
  /// to take the lower bound, get an iterator to it in `storage`, and then iterate forward,
  /// adding in every row that's within the `KeyBound` until we are beyond the upper bound of
  /// `KeyBound`. We first construct a `GenericTable` containing rows for every KeyBound,
  /// and then finally convert that to the return type.
  fn amend_all_prior_versions(
    &self,
    all_prior_versions: &mut BTreeMap<(PrimaryKey, Option<ColName>), (Timestamp, ColValN)>,
    key_bound: &KeyBound,
    val_cols: &Vec<ColName>,
    timestamp: &Timestamp,
  ) {
    let bound = compute_range_query(key_bound);
    // Iterate through storage rows.
    for ((pkey, ci), versions) in self.storage().range(bound.clone()) {
      // If this is a ValCol Cell that we do not care about, then skip it.
      if let Some(col_name) = ci {
        if !val_cols.contains(col_name) {
          continue;
        }
      }

      match check_inclusion(key_bound, pkey) {
        KeyBoundInclusionResult::Included => {
          // This row is present within the `key_bound`, so we  amend `snapshot_table`
          // unless there is already an entry that shadows this one.
          let storage_key = (pkey.clone(), ci.clone());
          if !all_prior_versions.contains_key(&storage_key) {
            if let Some(version) = find_version(versions, timestamp) {
              all_prior_versions.insert(storage_key, version.clone());
            }
          }
        }
        KeyBoundInclusionResult::Done => break,
        KeyBoundInclusionResult::Excluded => {}
      }
    }
  }

  /// Note that `key_region` does need to be disjoint. In addition, `column_region`
  /// need not be distinct.
  ///
  /// Preconditions:
  ///   1. For all `ColName`s in `column_region` that are not KeyCols, the Prior
  ///      Value at `timestamp` in `table_schema.val_cols` is `Some(_)`.
  fn compute_subtable(
    &self,
    key_region: &Vec<KeyBound>,
    column_region: &Vec<ColName>,
    timestamp: &Timestamp,
  ) -> Vec<(Vec<ColValN>, u64)> {
    // Select the `ColNames` in `column_region` that are not KeyCols.
    let mut val_cols = Vec::<ColName>::new();
    for col in column_region {
      if lookup(&self.table_schema().key_cols, col).is_none() {
        val_cols.push(col.clone());
      }
    }

    let presence_snapshot = self.compute_presence_snapshot(key_region, &val_cols, timestamp);
    presence_snapshot_to_table_view(presence_snapshot, self.table_schema(), column_region)
      .rows
      .into_iter()
      .collect()
  }
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
  fn table_schema(&self) -> &TableSchema {
    &self.table_schema
  }

  fn storage(&self) -> &GenericMVTable {
    &self.storage
  }

  /// Compute Non-Strict Prior Presence Snapshot.
  ///
  /// Preconditions:
  ///   1. For all `ColName`s in `column_region` that are not KeyCols, the Prior Value at
  ///      `timestamp` in `table_schema.val_cols` is `Some(_)`.
  fn compute_presence_snapshot(
    &self,
    key_region: &Vec<KeyBound>,
    val_cols: &Vec<ColName>,
    timestamp: &Timestamp,
  ) -> PresenceSnapshot {
    // Compute the non-strict prior version of all Presence Cells and ValCol Cells within
    // `key_bound` and val_cols.
    let mut all_prior_versions =
      BTreeMap::<(PrimaryKey, Option<ColName>), (Timestamp, ColValN)>::new();
    for key_bound in key_region {
      self.amend_all_prior_versions(&mut all_prior_versions, &key_bound, &val_cols, timestamp);
    }

    prior_versions_to_presence_snapshot(&self.table_schema, val_cols, timestamp, all_prior_versions)
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
  fn table_schema(&self) -> &TableSchema {
    &self.table_schema
  }

  fn storage(&self) -> &GenericMVTable {
    &self.storage
  }

  /// Compute Non-Strict Prior Presence Snapshot assuming that `update_views` gets compressed
  /// and inserted into `storage` at `timestamp`.
  ///
  /// Preconditions:
  ///   1. For all `ColName`s in `column_region` that are not KeyCols, the Prior Value at
  ///      `timestamp` in `table_schema.val_cols` is `Some(_)`.
  fn compute_presence_snapshot(
    &self,
    key_region: &Vec<KeyBound>,
    val_cols: &Vec<ColName>,
    timestamp: &Timestamp,
  ) -> PresenceSnapshot {
    // Compute the non-strict prior version of all Presence Cells and ValCol Cells within
    // `key_bound` and val_cols, assuming that `update_views` gets compressed
    // and inserted into `storage` at `timestamp`.
    let mut all_prior_versions =
      BTreeMap::<(PrimaryKey, Option<ColName>), (Timestamp, ColValN)>::new();
    for key_bound in key_region {
      let bound = compute_range_query(key_bound);
      // First, apply the UpdateViews to `snapshot_table`.
      let tier_bound = (Bound::Included(self.tier), Bound::Unbounded);
      for (_, generic_table) in self.update_views.range(tier_bound) {
        // Iterate over the the UpdateView Rows.
        for ((pkey, ci), val) in generic_table.range(bound.clone()) {
          // If this is a ValCol Cell that we do not care about, then skip it.
          if let Some(col_name) = ci {
            if !val_cols.contains(col_name) {
              continue;
            }
          }

          // Otherwise, see if the `pkey` is within the `key_bound`. If so, amend
          // `snapshot_table` unless there is already an entry that shadows this one.
          match check_inclusion(key_bound, pkey) {
            KeyBoundInclusionResult::Included => {
              let storage_key = (pkey.clone(), ci.clone());
              if !all_prior_versions.contains_key(&storage_key) {
                all_prior_versions.insert(storage_key, (timestamp.clone(), val.clone()));
              }
            }
            KeyBoundInclusionResult::Done => break,
            KeyBoundInclusionResult::Excluded => {}
          }
        }
      }

      // Add in the prior versions from storage.
      self.amend_all_prior_versions(&mut all_prior_versions, &key_bound, &val_cols, timestamp);
    }

    prior_versions_to_presence_snapshot(&self.table_schema, val_cols, timestamp, all_prior_versions)
  }
}

/// Compress the `update_views` by iterating from latest to earliest tier (i.e. lowest
/// to highet). We add in elements that are not already present (since they are shadowed
/// by the element already there).
pub fn compress_updates_views(update_views: BTreeMap<u32, GenericTable>) -> GenericTable {
  let mut compressed_table = GenericTable::new();
  for (_, generic_table) in update_views.into_iter() {
    for (key, value) in generic_table {
      if !compressed_table.contains_key(&key) {
        compressed_table.insert(key, value);
      }
    }
  }
  compressed_table
}

/// Add `(timestamp, value)` to `versions`, possibly replacing an existing version if the
/// timestamp already exists. Recall that `versions` must remain sorted in ascending order.
fn add_version(versions: &mut Vec<(Timestamp, ColValN)>, timestamp: Timestamp, value: ColValN) {
  for (i, (cur_timestamp, cur_value)) in versions.iter_mut().enumerate().rev() {
    if *cur_timestamp == timestamp {
      *cur_value = value;
      return;
    } else if *cur_timestamp < timestamp {
      versions.insert(i + 1, (timestamp, value));
      return;
    }
  }
  versions.insert(0, (timestamp, value));
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
      add_version(versions, timestamp.clone(), value);
    } else {
      storage.insert(key, vec![(timestamp.clone(), value)]);
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
  // To get a Bound that is immediately below all Storage Keys of interest,
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

/// Given the prior version of various Presence Cells and ValCols at `timestamp` (where
/// such a value is missing in `all_prior_version`), where the prior version of `val_cols`
/// in `table_schema.val_cols` is `Some(_)`, compute the `PresenceSnapshot`.
fn prior_versions_to_presence_snapshot(
  table_schema: &TableSchema,
  val_cols: &Vec<ColName>,
  timestamp: &Timestamp,
  all_prior_versions: BTreeMap<(PrimaryKey, Option<ColName>), (Timestamp, ColValN)>,
) -> PresenceSnapshot {
  // Assert the precondition
  debug_assert!((|| {
    for col in val_cols {
      if table_schema.val_cols.static_read(col, timestamp).is_none() {
        return false;
      }
    }
    return true;
  })());

  // Compute which `PrimaryKey`s are present
  let mut present_key_timestamps = BTreeMap::<PrimaryKey, Timestamp>::new();
  for ((pkey, ci), (timestamp, val)) in &all_prior_versions {
    if ci.is_none() && val == &PRESENCE_VALN {
      present_key_timestamps.insert(pkey.clone(), timestamp.clone());
    }
  }

  // Compute the timestamps that the `val_cols` were added at.
  let mut col_timestamps = BTreeMap::<ColName, Timestamp>::new();
  for col in val_cols {
    let (timestamp, _) = table_schema.val_cols.static_read_version(col, timestamp).unwrap();
    col_timestamps.insert(col.clone(), timestamp.clone());
  }

  // Compute the Presence Snapshot
  let mut presence_snapshot = BTreeMap::<PrimaryKey, Vec<(ColName, ColValN)>>::new();
  for (pkey, pkey_ts) in &present_key_timestamps {
    let mut snapshot_row = Vec::<(ColName, ColValN)>::new();
    for (col, col_ts) in &col_timestamps {
      let storage_key = (pkey.clone(), Some(col.clone()));
      // Compute the Resolved Value (where we ignore `val` if `timestamp` is too early).
      let resolved_val = if let Some((timestamp, val)) = all_prior_versions.get(&storage_key) {
        if timestamp >= max(pkey_ts, col_ts) {
          val.clone()
        } else {
          None
        }
      } else {
        None
      };
      snapshot_row.push((col.clone(), resolved_val));
    }
    presence_snapshot.insert(pkey.clone(), snapshot_row);
  }
  presence_snapshot
}

/// Convert a `PresenceSnapshot` to a `TableView` by selecting the columns in `selection`.
/// Note that every `ColName` in `selection` must either be in the key or the value in
/// `presence_snapshot`. Also note that `selection` need not be distinct.
fn presence_snapshot_to_table_view(
  presence_snapshot: PresenceSnapshot,
  table_schema: &TableSchema,
  selection: &Vec<ColName>,
) -> TableView {
  let mut table_view = TableView::new(selection.iter().cloned().map(|c| Some(c)).collect());
  for (pkey, col_name_vals) in presence_snapshot {
    let mut row = Vec::<ColValN>::new();
    for col_name in selection {
      if let Some(key_col_index) = lookup_pos(&table_schema.key_cols, &col_name) {
        // `col_name` is a KeyCol.
        row.push(Some(pkey.cols.get(key_col_index).unwrap().clone()));
      } else {
        // `col_name` is a ValCol.
        let col_val = lookup(&col_name_vals, &col_name).unwrap();
        row.push(col_val.clone());
      }
    }
    table_view.add_row(row);
  }
  table_view
}

// -----------------------------------------------------------------------------------------------
//  Sharding Utils
// -----------------------------------------------------------------------------------------------

/// Converts the `range_key` to a Storage Key that can be used as a lower bound when
/// querying the `GenericMVTable`.
fn range_to_storage_key(range_key: &ColVal) -> (PrimaryKey, Option<ColName>) {
  (PrimaryKey { cols: vec![range_key.clone()] }, None)
}

/// Computes the subset of `storage` that lies within `range`.
pub fn compute_range_storage(storage: &GenericMVTable, range: &TabletKeyRange) -> GenericMVTable {
  let start_bound = if let Some(start) = &range.start {
    Bound::Included(range_to_storage_key(start))
  } else {
    Bound::Unbounded
  };

  let end_bound = if let Some(end) = &range.end {
    Bound::Excluded(range_to_storage_key(end))
  } else {
    Bound::Unbounded
  };

  let mut target_storage = GenericMVTable::new();
  for (key, value) in storage.range((start_bound, end_bound)) {
    target_storage.insert(key.clone(), value.clone());
  }

  target_storage
}

/// This function modified `storage` leaving only the keys strictly before `range.start`.
/// In addition, this returns all keys on or after `range.end`. (Thus all keys within `range`
/// are deleted forever.)
pub fn remove_range(storage: &mut GenericMVTable, range: &TabletKeyRange) -> GenericMVTable {
  let mut remaining = if let Some(start) = &range.start {
    storage.split_off(&range_to_storage_key(start))
  } else {
    std::mem::take(storage)
  };

  if let Some(end) = &range.end {
    remaining.split_off(&range_to_storage_key(end))
  } else {
    GenericMVTable::new()
  }
}
