use crate::model::common::{
  ColumnName, ColumnType, ColumnValue, PrimaryKey, Row, Schema, TabletShape, Timestamp,
};
use crate::storage::multiversion_map::MultiVersionMap;
use std::collections::HashSet;

/// Terminology:
///
/// Row Key - The Primary Key value of a Row
/// Row Val - The value of the non-Primary Key columns of a Row
///           (in sorted order according to the schema layout)
/// Value Cell - A value of a non-Primary Key column when a given
///              Row is understood
/// Key Column - A column of a Primary Key column when a given Row
///              is understood
/// Value Column - A column of a non-Primary Key column when a given
///                Row is understood.

/// These are the values that the MVM maps to. The only difference between
/// this an `ColumnValue` is the presence of `Unit`, which is a special value
/// used to indicate that a row itself is present. We don't want to include `Unit`
/// in `ColumnValue` because it's not a SQL type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageValue {
  Int(i32),
  Bool(bool),
  String(String),
  Unit,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageError {
  BackwardsWrite,
  RowOutOfRange,
  MalformedKey,
  ColumnDNE,
  TypeError,
}

/// We define a convenient conversion function from
/// StorageValue to ColumnValue.
impl StorageValue {
  fn convert(self) -> ColumnValue {
    match self {
      StorageValue::Int(i32) => ColumnValue::Int(i32),
      StorageValue::Bool(bool) => ColumnValue::Bool(bool),
      StorageValue::String(string) => ColumnValue::String(string),
      StorageValue::Unit => panic!("Cannot convert `Unit` StorageValue to ColumnValue."),
    }
  }
}

/// We define a convenient conversion function from
/// ColumnValue to StorageValue.
impl ColumnValue {
  fn convert(self) -> StorageValue {
    match self {
      ColumnValue::Int(i32) => StorageValue::Int(i32),
      ColumnValue::Bool(bool) => StorageValue::Bool(bool),
      ColumnValue::String(string) => StorageValue::String(string),
    }
  }
}

#[derive(Debug)]
pub struct RelationalTablet {
  mvm: MultiVersionMap<(PrimaryKey, Option<ColumnName>), StorageValue>,
  tablet_shape: TabletShape,
  pub schema: Schema,
}

impl RelationalTablet {
  pub fn new(schema: Schema, tablet_shape: &TabletShape) -> RelationalTablet {
    RelationalTablet {
      mvm: MultiVersionMap::new(),
      tablet_shape: tablet_shape.clone(),
      schema,
    }
  }

  /// This checks to see if the provided row conforms to the schema. For this
  /// check to pass, the  Types of the elements in `row.key` and `row.value`
  /// must align with the Types of the elements in `schema.key_cols` and
  /// `schema.val_cols`.
  fn verify_row(&self, row: &Row) -> Result<(), StorageError> {
    self.verify_row_key(&row.key)?;
    self.verify_row_val(&row.val)
  }

  pub fn verify_row_key(&self, key: &PrimaryKey) -> Result<(), StorageError> {
    if key.cols.len() == self.schema.key_cols.len() {
      for (col_val, (col_type, _)) in key.cols.iter().zip(&self.schema.key_cols) {
        check_type_match(Some(&col_val), col_type)?;
      }
      Ok(())
    } else {
      Err(StorageError::MalformedKey)
    }
  }

  fn verify_row_val(&self, val: &Vec<Option<ColumnValue>>) -> Result<(), StorageError> {
    if val.len() == self.schema.val_cols.len() {
      for (col_val, (col_type, _)) in val.iter().zip(&self.schema.val_cols) {
        check_type_match((&col_val).as_ref(), col_type)?;
      }
      Ok(())
    } else {
      Err(StorageError::MalformedKey)
    }
  }

  /// Gets the ColumnType of the given col_name if it actually exists
  /// in schema.
  fn get_col_type(&self, col_name: &ColumnName) -> Option<ColumnType> {
    for (col_type, name) in &self.schema.val_cols {
      if name == col_name {
        return Some(col_type.clone());
      }
    }
    for (col_type, name) in &self.schema.key_cols {
      if name == col_name {
        return Some(col_type.clone());
      }
    }
    return None;
  }

  fn verify_vals(
    &self,
    val_cols: &Vec<(ColumnName, Option<ColumnValue>)>,
  ) -> Result<(), StorageError> {
    for (col_name, col_val) in val_cols {
      if let Some(col_type) = self.get_col_type(col_name) {
        check_type_match(col_val.as_ref(), &col_type)?;
      } else {
        return Err(StorageError::ColumnDNE);
      }
    }
    Ok(())
  }

  /// Checks if `key` is in the RelationalTablet's `table_shape`'s
  /// range.
  pub fn is_in_key_range(&self, key: &PrimaryKey) -> Result<(), StorageError> {
    if let Some(lower_bound) = &self.tablet_shape.range.start {
      if key < lower_bound {
        return Err(StorageError::RowOutOfRange);
      }
    }
    if let Some(upper_bound) = &self.tablet_shape.range.end {
      if key >= upper_bound {
        return Err(StorageError::RowOutOfRange);
      }
    }
    Ok(())
  }

  /// Inserts the row into the RelationalTablet. This method first checks
  /// to see if the `row`'s key columns and value columns conform to the
  /// schema. Then, it inserts the row into the MultiVersionMap. If either
  /// of these steps fails, we return false, otherwise we return true.
  pub fn insert_row(&mut self, row: &Row, timestamp: Timestamp) -> Result<(), String> {
    if self.verify_row(row).is_err() {
      return Err("The given row does not conform to the schema.".to_string());
    }

    if self.is_in_key_range(&row.key).is_err() {
      return Err("The given row's primary key isn't in the range of this tablet".to_string());
    }

    // If the row isn't present, and we can't make it present because
    // it's lat is too high, then the insertion fails.
    let mvm_key = (row.key.clone(), None);
    if self.mvm.static_read(&mvm_key, timestamp) == None {
      if self.mvm.get_lat(&mvm_key) >= timestamp {
        // This means the row doesn't exist at the timesstamp
        // and the lat is too high to reintroduce it.
        return Err(String::from(
          "The row doesn't exist at the given `timestamp` and can't be re-introduced.",
        ));
      } else {
        // Since the row doesn't exist at `timestamp`, that means
        // neither to any of the value cells. Since we can both introduce
        // the row and write to the value cells, we can perform the insert.
        self
          .mvm
          .write(&mvm_key, Some(StorageValue::Unit), timestamp)
          .unwrap();
      }
    } else {
      // Although the row is present at `timestamp`, we must make sure
      // we can actually write to the value cells.
      for (_, col_name) in &self.schema.val_cols {
        let mvm_key = (row.key.clone(), Some(col_name.clone()));
        if self.mvm.get_lat(&mvm_key) >= timestamp {
          // The lat of one of the value cells is too high.
          return Err(String::from(
            "The row exists, but one of the column's lat is too high.",
          ));
        }
      }
    }

    // At this point, the Row Key will surely be present in `mvm`,
    // and the Row Value will surely be writable.
    let zipped = self.schema.val_cols.iter().zip(&row.val);
    for ((_, col_name), val) in zipped {
      let mvm_key = (row.key.clone(), Some(col_name.clone()));
      self
        .mvm
        .write(&mvm_key, val.clone().map(|v| v.convert()), timestamp)
        .unwrap();
    }

    return Ok(());
  }

  /// A wrapper around mvm.write that will return a StorageError::BackwardsWrite
  /// if the write fails.
  fn write(
    &mut self,
    key: &(PrimaryKey, Option<ColumnName>),
    value: Option<StorageValue>,
    timestamp: Timestamp,
  ) -> Result<(), StorageError> {
    if self.mvm.write(key, value, timestamp).is_ok() {
      Ok(())
    } else {
      Err(StorageError::BackwardsWrite)
    }
  }

  /// This is a general function for inserting a row, or updating it if it
  /// already exists. The `key` must conform the the schema. If `val_cols_o`
  /// is `None`, it means we must delete the row. If it's `Some`, it means that
  /// we must upsert the row, where the Value Columns take on the values indicated
  /// by `val_cols_o`. There might be Value Columns missing here, indicating those
  /// should be NULL.
  ///
  /// If either `key` or `val_cols_o` doesn't conform to the schema (where there is
  /// an unknown column, or the `ColumnValue` provided doensn't match the type, etc),
  /// then we return an error. If the `key` is outside the range of this tablet,
  /// we also throw an error.
  ///
  /// If an Err is returned, then the MVT was not modified.
  pub fn upsert_row(
    &mut self,
    key: PrimaryKey,
    val_cols_o: Option<Vec<(ColumnName, Option<ColumnValue>)>>,
    timestamp: &Timestamp,
  ) -> Result<(), StorageError> {
    self.verify_row_key(&key)?;
    if let Some(val_cols) = val_cols_o {
      self.verify_vals(&val_cols)?;
      // First, make sure that the columns we want to write to can actually
      // be written to (i.e. don't incur a BackwardsWrite error).
      for (col_name, _) in &val_cols {
        if timestamp <= &self.mvm.get_lat(&(key.clone(), Some(col_name.clone()))) {
          return Err(StorageError::BackwardsWrite);
        }
      }
      // Now, the last place this upsert can fail is if the row doesn't
      // exist yet, but we can't bring it into existance.
      if let None = self.mvm.static_read(&(key.clone(), None), *timestamp) {
        // This means the row doesn't exist.
        self.write(&(key.clone(), None), Some(StorageValue::Unit), *timestamp)?;
      }
      // At this point, the upsert will surely succeed.
      for (col_name, col_val) in &val_cols {
        self
          .write(
            &(key.clone(), Some(col_name.clone())),
            col_val.clone().map(|v| v.convert()),
            *timestamp,
          )
          .unwrap();
      }
    } else {
      // This means we must delete the row.
      panic!(
        "TODO: implement. Make sure to write all column values as\
      deleted too. If all of these writes aren't possible, then don't modify\
      the tablet and return an error."
      )
    }
    Ok(())
  }

  /// Returns true iff the column name exists in the schema.
  pub fn col_name_exists(&self, col_name: &ColumnName) -> bool {
    for (_, name) in &self.schema.key_cols {
      if col_name == name {
        return true;
      }
    }
    for (_, name) in &self.schema.val_cols {
      if col_name == name {
        return true;
      }
    }
    false
  }

  /// Returns if the column names exists in the schema or not.
  pub fn col_names_exists(&self, val_cols: &Vec<ColumnName>) -> bool {
    for col_name in val_cols {
      if self.col_name_exists(col_name) {
        return true;
      }
    }
    false
  }

  /// This is a dumb function. It doesn't check if the ColumnName
  /// is actually part of the schema. It just appends col_name to key,
  /// and does a lookup in the mvm. Thus, whether the ColumnName in
  /// the Schema exists must be checked before.
  pub fn get_partial_val(
    &self,
    key: &PrimaryKey,
    col_name: &ColumnName,
    timestamp: &Timestamp,
  ) -> Option<ColumnValue> {
    if let Some(i) = self
      .schema
      .key_cols
      .iter()
      .position(|(_, name)| col_name == name)
    {
      // This means that the `col_name` was a key column.
      Some(key.cols.get(i).unwrap().clone())
    } else {
      // Otherwise, we try and see if `col_name` is a value column.
      self
        .mvm
        .static_read(&(key.clone(), Some(col_name.clone())), timestamp.clone())
        .map(|v| v.convert())
    }
  }

  /// This is essentially a snapshot read of all keys at the given timestamp.
  pub fn get_keys(&self, timestamp: &Timestamp) -> Vec<PrimaryKey> {
    let mut keys = HashSet::new();
    for (key, _) in self.mvm.map.keys() {
      if let Some(_) = self
        .mvm
        .static_read(&(key.clone(), None), timestamp.clone())
      {
        keys.insert(key.clone());
      }
    }

    return keys.into_iter().collect();
  }

  /// This function returns an error if the key doesn't conform
  /// to the schema. Otherwise, it returns None if the row doesn't
  /// exist at the timestamp, and the row if it does.
  pub fn read_row(
    &mut self,
    key: &PrimaryKey,
    timestamp: Timestamp,
  ) -> Result<Option<Row>, String> {
    if self.verify_row_key(key).is_err() {
      return Err(String::from(
        "The given key does not confrom to the schema.",
      ));
    }
    if self.mvm.read(&(key.clone(), None), timestamp) == None {
      return Ok(None);
    } else {
      let mut val_col_values = Vec::new();
      for (_, col_name) in &self.schema.val_cols {
        let mvm_key = (key.clone(), Some(col_name.clone()));
        val_col_values.push(self.mvm.read(&mvm_key, timestamp));
      }
      return Ok(Some(Row {
        key: key.clone(),
        val: val_col_values
          .iter()
          .map(|v| v.clone().map(|v| v.convert()))
          .collect(),
      }));
    }
  }
}

/// Returns false if the types don't match, and true otherwise.
fn check_type_match(
  value: Option<&ColumnValue>,
  col_type: &ColumnType,
) -> Result<(), StorageError> {
  match (value, col_type) {
    (Some(ColumnValue::Int(_)), ColumnType::Int) => Ok(()),
    (Some(ColumnValue::String(_)), ColumnType::String) => Ok(()),
    (Some(ColumnValue::Bool(_)), ColumnType::Bool) => Ok(()),
    (None, _) => Ok(()),
    _ => Err(StorageError::TypeError),
  }
}

#[cfg(test)]
mod tests {
  use crate::model::common::{
    ColumnName as CN, ColumnType as CT, ColumnValue as CV, PrimaryKey, Row, Schema, TabletKeyRange,
    TabletPath, TabletShape, Timestamp,
  };
  use crate::storage::relational_tablet::RelationalTablet;

  #[test]
  fn single_row_insert_test() {
    let table_shape = TabletShape {
      path: TabletPath {
        path: "".to_string(),
      },
      range: TabletKeyRange {
        start: None,
        end: None,
      },
    };
    let mut tablet = RelationalTablet::new(
      Schema {
        key_cols: vec![
          (CT::String, CN(String::from("name"))),
          (CT::Int, CN(String::from("age"))),
        ],
        val_cols: vec![
          (CT::String, CN(String::from("email"))),
          (CT::Int, CN(String::from("income"))),
        ],
      },
      &table_shape,
    );

    let k = PrimaryKey {
      cols: vec![CV::String(String::from("Fred")), CV::Int(20)],
    };
    let v1 = vec![Some(CV::String(String::from("e1"))), None];
    let v2 = vec![Some(CV::String(String::from("e1"))), Some(CV::Int(60_000))];
    let v3 = vec![Some(CV::String(String::from("e2"))), Some(CV::Int(80_000))];

    let row1 = Row {
      key: k.clone(),
      val: v1.clone(),
    };

    let row2 = Row {
      key: k.clone(),
      val: v2.clone(),
    };

    let row3 = Row {
      key: k.clone(),
      val: v2.clone(),
    };

    let row4 = Row {
      key: k.clone(),
      val: v3.clone(),
    };

    assert!(tablet.insert_row(&row1, Timestamp(2)).is_ok());
    assert!(tablet.insert_row(&row2, Timestamp(2)).is_err());
    assert!(tablet.insert_row(&row3, Timestamp(3)).is_ok());
    assert_eq!(tablet.read_row(&k, Timestamp(2)).unwrap().unwrap(), row1);
    assert_eq!(tablet.read_row(&k, Timestamp(4)).unwrap().unwrap(), row3);
    assert!(tablet.insert_row(&row4, Timestamp(4)).is_err());
    assert!(tablet.insert_row(&row4, Timestamp(5)).is_ok());
    assert_eq!(tablet.read_row(&k, Timestamp(6)).unwrap().unwrap(), row4);
  }
}
