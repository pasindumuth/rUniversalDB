use crate::model::common::{
  ColumnName, ColumnType, ColumnValue, PrimaryKey, Row, Schema, Timestamp,
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
  pub schema: Schema,
}

impl RelationalTablet {
  pub fn new(schema: Schema) -> RelationalTablet {
    RelationalTablet {
      mvm: MultiVersionMap::new(),
      schema,
    }
  }

  /// Returns false if the types don't match, and true otherwise.
  fn check_type_match(value: Option<&ColumnValue>, col_type: &ColumnType) -> bool {
    match (value, col_type) {
      (Some(ColumnValue::Int(_)), ColumnType::Int) => true,
      (Some(ColumnValue::String(_)), ColumnType::String) => true,
      (Some(ColumnValue::Bool(_)), ColumnType::Bool) => true,
      (None, _) => true,
      _ => false,
    }
  }

  /// This checks to see if the provided row conforms to the schema. For this
  /// check to pass, the  Types of the elements in `row.key` and `row.value`
  /// must align with the Types of the elements in `schema.key_cols` and
  /// `schema.val_cols`.
  fn verify_row(&self, row: &Row) -> bool {
    return self.verify_row_key(&row.key) && self.verify_row_val(&row.val);
  }

  fn verify_row_key(&self, key: &PrimaryKey) -> bool {
    if key.cols.len() == self.schema.key_cols.len() {
      for (col_val, (col_type, _)) in key.cols.iter().zip(&self.schema.key_cols) {
        if !RelationalTablet::check_type_match(Some(&col_val), col_type) {
          return false;
        }
      }
    }
    return true;
  }

  fn verify_row_val(&self, val: &Vec<Option<ColumnValue>>) -> bool {
    if val.len() == self.schema.val_cols.len() {
      for (col_val, (col_type, _)) in val.iter().zip(&self.schema.val_cols) {
        if !RelationalTablet::check_type_match((&col_val).as_ref(), col_type) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /// Inserts the row into the RelationalTablet. This method first checks
  /// to see if the `row`'s key columns and value columns conform to the
  /// schema. Then, it inserts the row into the MultiVersionMap. If either
  /// of these steps fails, we return false, otherwise we return true.
  pub fn insert_row(&mut self, row: &Row, timestamp: Timestamp) -> Result<(), String> {
    if !self.verify_row(row) {
      return Err(String::from(
        "The given row does not conform to the schema.",
      ));
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

  /// This function updates a subset of the Value Columns. The other
  /// Value Columns remain unchanged, including their `lat`s. This is a dumb
  /// function; it doesn't check to see if `key` or `col_name` are a part
  /// of the schema. In addition, this function may fail fatally if a
  /// backwards write is attempted.
  pub fn insert_partial_val(
    &mut self,
    key: PrimaryKey,
    col_name: ColumnName,
    val: Option<ColumnValue>,
    timestamp: &Timestamp,
  ) {
    self
      .mvm
      .write(&(key, Some(col_name)), val.map(|v| v.convert()), *timestamp)
      .unwrap();
  }

  /// Performs `insert_partial_val` for every pair in `partial_val`.
  pub fn insert_partial_vals(
    &mut self,
    key: PrimaryKey,
    partial_val: Vec<(ColumnName, Option<ColumnValue>)>,
    timestamp: &Timestamp,
  ) {
    for (col_name, col_val) in partial_val {
      self.insert_partial_val(key.clone(), col_name, col_val, timestamp);
    }
  }

  /// Performs `insert_partial_vals` if the `partial_val_o` is present,
  /// otherwise it deletes the row at `key`.
  pub fn insert_row_diff(
    &mut self,
    key: PrimaryKey,
    partial_val_o: Option<Vec<(ColumnName, Option<ColumnValue>)>>,
    timestamp: &Timestamp,
  ) {
    if let Some(partial_val) = partial_val_o {
      self.insert_partial_vals(key, partial_val, timestamp);
    } else {
      // This means we must delete the row.
      self.mvm.write(&(key, None), None, *timestamp).unwrap();
    }
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
    self
      .mvm
      .static_read(&(key.clone(), Some(col_name.clone())), timestamp.clone())
      .map(|v| v.convert())
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
    if !self.verify_row_key(key) {
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

#[cfg(test)]
mod tests {
  use crate::model::common::{
    ColumnName as CN, ColumnType as CT, ColumnValue as CV, PrimaryKey, Row, Schema, Timestamp,
  };
  use crate::storage::relational_tablet::RelationalTablet;

  #[test]
  fn single_row_insert_test() {
    let mut tablet = RelationalTablet::new(Schema {
      key_cols: vec![
        (CT::String, CN(String::from("name"))),
        (CT::Int, CN(String::from("age"))),
      ],
      val_cols: vec![
        (CT::String, CN(String::from("email"))),
        (CT::Int, CN(String::from("income"))),
      ],
    });

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
