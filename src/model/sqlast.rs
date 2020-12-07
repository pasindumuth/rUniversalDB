use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum SqlStmt {
  Select(SelectStmt),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SelectStmt {
  pub col_names: Vec<String>,
  pub table_name: String,
}
