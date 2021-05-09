use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// These are common PODs that form the core data objects
/// of the system.

// -------------------------------------------------------------------------------------------------
//  Relational Tablet
// -------------------------------------------------------------------------------------------------

/// A global identifier of a Tablet (across tables and databases).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TablePath {
  pub path: String,
}

/// The key range that a tablet manages. The `start` and `end` are
/// PrimaryKey types, which are convenient for splitting the key-space.
/// If either `start` or `end` is `None`, that means there is no bound
/// for that direction. This is a half-open interval, where `start`
/// is inclusive and `end` is exclusive.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TabletKeyRange {
  pub start: Option<PrimaryKey>,
  pub end: Option<PrimaryKey>,
}

/// A global identifier for a tablet.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TabletShape {
  pub path: TablePath,
  pub range: TabletKeyRange,
}

/// The types that the columns of a Relational Tablet can take on.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ColType {
  Int,
  Bool,
  String,
}

/// The values that the columns of a Relational Tablet can take on.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColValue {
  Int(i32),
  Bool(bool),
  String(String),
}

/// The name of a column.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColName(pub String);

/// The Primary Key of a Relational Tablet. Note that we don't use
/// Vec<Option<ColValue>> because values of a key column can't be NULL.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrimaryKey {
  pub cols: Vec<ColValue>,
}

/// The Schema of a Relational Tablet. This stays constant throughout the lifetime
/// of a Relational Tablet. If the USER wants to change the number of key-columns,
/// we implement that by creating a new Relational Tablet.
#[derive(Debug, Clone)]
pub struct Schema {
  pub key_cols: Vec<(ColType, ColName)>,
  pub val_cols: Vec<(ColType, ColName)>,
}

/// A Row of a Relational Tablet. The reason for Option<ColValue> is that None
/// represents the NULL value for a column.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Row {
  pub key: PrimaryKey,
  pub val: Vec<Option<ColValue>>,
}

// -------------------------------------------------------------------------------------------------
//  Transaction Data Structures
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableView {
  col_names: Box<[(ColName, ColType)]>,
  rows: BTreeMap<Box<[Option<ColValue>]>, u64>,
}

// -------------------------------------------------------------------------------------------------
//  Miscellaneous
// -------------------------------------------------------------------------------------------------

/// A global identifer of a network node. This includes Slaves, Clients, Admin
/// clients, and other nodes in the network.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EndpointId(pub String);

/// A global identfier of a Tablet.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TabletGroupId(pub String);

/// A global identfier of a Tablet.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(pub [u8; 8]);

/// A global identfier of a Slave.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlaveGroupId(pub String);

/// A request Id that globally identifies a request.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(pub String);

// -------------------------------------------------------------------------------------------------
//  Implementations
// -------------------------------------------------------------------------------------------------

impl TablePath {
  pub fn from(eid: &str) -> TablePath {
    TablePath { path: eid.to_string() }
  }
}

impl EndpointId {
  pub fn from(eid: &str) -> EndpointId {
    EndpointId(eid.to_string())
  }
}

impl RequestId {
  pub fn from(eid: &str) -> RequestId {
    RequestId(eid.to_string())
  }
}

pub fn table_shape(path: &str, start: Option<&str>, end: Option<&str>) -> TabletShape {
  TabletShape {
    path: TablePath::from(path),
    range: TabletKeyRange {
      start: start.map(|start| PrimaryKey { cols: vec![ColValue::String(String::from(start))] }),
      end: end.map(|end| PrimaryKey { cols: vec![ColValue::String(String::from(end))] }),
    },
  }
}

// -------------------------------------------------------------------------------------------------
// SQL
// -------------------------------------------------------------------------------------------------

pub mod iast {
  #[derive(Debug)]
  pub struct TableAlias {
    pub name: String,
    pub columns: Option<Vec<String>>,
  }

  #[derive(Debug)]
  pub enum ValExpr {
    ColumnRef { table_ref: Option<String>, col_ref: String },
    UnaryOp { op: UnaryOp, expr: Box<ValExpr> },
    BinaryOp { op: BinaryOp, left: Box<ValExpr>, right: Box<ValExpr> },
    Value { val: Value },
    Subquery { query: Box<Query> },
  }

  #[derive(Debug)]
  pub enum UnaryOp {
    Plus,
    Minus,
    Not,
    IsNull,
    IsNotNull,
  }

  #[derive(Debug)]
  pub enum BinaryOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus,
    StringConcat,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Spaceship,
    Eq,
    NotEq,
    And,
    Or,
  }

  #[derive(Debug)]
  pub enum Value {
    Number(String),
    QuotedString(String),
    Boolean(bool),
    Null,
  }

  #[derive(Debug)]
  pub struct Query {
    pub ctes: Vec<(TableAlias, Query)>,
    pub body: QueryBody,
  }

  #[derive(Debug)]
  pub enum QueryBody {
    Query(Box<Query>),
    SuperSimpleSelect(SuperSimpleSelect),
    Update(Update),
  }

  #[derive(Debug)]
  pub struct SuperSimpleSelect {
    pub projection: SelectClause, // The select clause
    pub from: (String, Option<TableAlias>),
    pub selection: ValExpr, // The where clause
  }

  #[derive(Debug)]
  pub enum SelectClause {
    SelectList(Vec<(String, Option<String>)>),
    Wildcard,
  }

  #[derive(Debug)]
  pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, ValExpr)>,
    pub selection: ValExpr,
  }
}
