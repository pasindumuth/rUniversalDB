use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// These are common PODs that form the core data objects
/// of the system.

// -------------------------------------------------------------------------------------------------
//  Relational Tablet
// -------------------------------------------------------------------------------------------------

/// A global identifier of a Table (across tables and databases).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TablePath(pub String);

/// A global identifier of a TransTable (across tables and databases).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransTableName(pub String);

/// The types that the columns of a Relational Tablet can take on.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ColType {
  Int,
  Bool,
  String,
}

/// The values that the columns of a Relational Tablet can take on.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColVal {
  Int(i32),
  Bool(bool),
  String(String),
}

/// This is a nullable `ColVal`. We use this alias for self-documentation
pub type ColValN = Option<ColVal>;

/// The name of a column.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColName(pub String);

/// The Primary Key of a Relational Tablet. Note that we don't use
/// Vec<Option<ColValue>> because values of a key column can't be NULL.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrimaryKey {
  pub cols: Vec<ColVal>,
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

// TODO: get rid of the Copy trait in the below. I never liked it.
/// A simple Timestamp type.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct Timestamp(pub u128);

/// A Type for the generation of a gossip message.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct Gen(pub u32);

// -------------------------------------------------------------------------------------------------
//  Key Regions
// -------------------------------------------------------------------------------------------------

// Generic single-side bound for an orderable value.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SingleBound<T> {
  Included(T),
  Excluded(T),
  Unbounded,
}

/// Here, the Variant that the `ColVal` in each `SingleBound` takes
/// on has to be the same (i.e. the `ColVal` has to be the same type).
#[derive(Debug)]
pub struct ColBound {
  pub start: SingleBound<ColVal>,
  pub end: SingleBound<ColVal>,
}

impl ColBound {
  pub fn new(start: SingleBound<ColVal>, end: SingleBound<ColVal>) -> ColBound {
    ColBound { start, end }
  }
}

/// A full Boundary for a `PrimaryKey`
#[derive(Debug)]
pub struct KeyBound {
  pub key_col_bounds: Vec<ColBound>,
}

/// ReadRegion for a query
#[derive(Debug)]
pub struct ReadRegion {
  pub col_region: Vec<ColName>,
  pub row_region: Vec<KeyBound>,
}

/// WriteRegion for a query
#[derive(Debug)]
pub struct WriteRegion {
  pub col_region: Vec<ColName>,
  pub row_region: Vec<KeyBound>,
}

// -------------------------------------------------------------------------------------------------
//  Transaction Data Structures
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableView {
  /// The keys are the rows, and the values are the number of repetitions.
  pub col_names: Vec<(ColName, ColType)>,
  /// The keys are the rows, and the values are the number of repetitions.
  pub rows: BTreeMap<Vec<ColValN>, u64>,
}

/// This is used to hold onto Tier that each TablePath is currently being
/// processed at. This is necessary for performing Intermediary Writes
/// and Reads properly.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TierMap {
  pub map: HashMap<TablePath, u32>,
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

/// This is often used when there is behavior that's common between Table and Slave.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeGroupId {
  Tablet(TabletGroupId),
  Slave(SlaveGroupId),
}

// -------------------------------------------------------------------------------------------------
//  Subquery Context
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TransTableLocationPrefix {
  pub source: NodeGroupId,
  pub query_id: QueryId,
  pub trans_table_name: TransTableName,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct ContextSchema {
  pub column_context_schema: Vec<ColName>,
  pub trans_table_context_schema: Vec<TransTableLocationPrefix>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct ContextRow {
  pub column_context_row: Vec<ColValN>,
  pub trans_table_context_row: Vec<u32>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub struct Context {
  pub context_schema: ContextSchema,
  pub context_rows: Vec<ContextRow>,
}

// -------------------------------------------------------------------------------------------------
//  Implementations
// -------------------------------------------------------------------------------------------------

impl TablePath {
  pub fn from(table: &str) -> TablePath {
    TablePath(table.to_string())
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

// -------------------------------------------------------------------------------------------------
// Processed SQL
// -------------------------------------------------------------------------------------------------

pub mod proc {
  use crate::model::common::iast::{BinaryOp, UnaryOp, Value};
  use crate::model::common::{ColName, TablePath, TransTableName};
  use serde::{Deserialize, Serialize};

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum TableRef {
    TablePath(TablePath),
    TransTableName(TransTableName),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum ValExpr {
    ColumnRef { col_ref: ColName },
    UnaryExpr { op: UnaryOp, expr: Box<ValExpr> },
    BinaryExpr { op: BinaryOp, left: Box<ValExpr>, right: Box<ValExpr> },
    Value { val: Value },
    Subquery { query: Box<GRQuery> },
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SuperSimpleSelect {
    pub projection: Vec<ColName>,
    pub from: TableRef,
    pub selection: ValExpr,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Update {
    pub table: TablePath,
    pub assignment: Vec<(ColName, ValExpr)>,
    pub selection: ValExpr,
  }

  // GR

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum GRQueryStage {
    SuperSimpleSelect(SuperSimpleSelect),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct GRQuery {
    pub trans_tables: Vec<(TransTableName, GRQueryStage)>,
    pub returning: TransTableName,
  }

  // MS

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum MSQueryStage {
    SuperSimpleSelect(SuperSimpleSelect),
    Update(Update),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct MSQuery {
    pub trans_tables: Vec<(TransTableName, MSQueryStage)>,
    pub returning: TransTableName,
  }
}

// -------------------------------------------------------------------------------------------------
// SQL
// -------------------------------------------------------------------------------------------------

pub mod iast {
  use serde::{Deserialize, Serialize};

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum ValExpr {
    ColumnRef { col_ref: String },
    UnaryExpr { op: UnaryOp, expr: Box<ValExpr> },
    BinaryExpr { op: BinaryOp, left: Box<ValExpr>, right: Box<ValExpr> },
    Value { val: Value },
    Subquery { query: Box<Query> },
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum UnaryOp {
    Plus,
    Minus,
    Not,
    IsNull,
    IsNotNull,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum Value {
    Number(String),
    QuotedString(String),
    Boolean(bool),
    Null,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Query {
    pub ctes: Vec<(String, Query)>,
    pub body: QueryBody,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum QueryBody {
    Query(Box<Query>),
    SuperSimpleSelect(SuperSimpleSelect),
    Update(Update),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SuperSimpleSelect {
    pub projection: Vec<String>, // The select clause
    pub from: String,
    pub selection: ValExpr, // The where clause
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, ValExpr)>,
    pub selection: ValExpr,
  }
}
