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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColName(pub String);

/// The Primary Key of a Relational Tablet. Note that we don't use
/// Vec<Option<ColValue>> because values of a key column can't be NULL.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrimaryKey {
  pub cols: Vec<ColVal>,
}

impl PrimaryKey {
  pub fn new(cols: Vec<ColVal>) -> PrimaryKey {
    PrimaryKey { cols }
  }
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

/// A simple Timestamp type.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct Timestamp(pub u128);

/// A Type used to represent a generation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct Gen(pub u64);

// -------------------------------------------------------------------------------------------------
//  Transaction Data Structures
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableView {
  /// The keys are the rows, and the values are the number of repetitions.
  pub col_names: Vec<ColName>,
  /// The keys are the rows, and the values are the number of repetitions.
  pub rows: BTreeMap<Vec<ColValN>, u64>,
}

/// A TablePath should appear here iff that Table is written to in the MSQuery.
/// The Tier for every such TablePath here is the Tier that MSTableRead should be
/// using to read. If the TransTable is corresponds to an Update, the Tier for TablePath
/// being updated should be one ahead (i.e. lower).
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

impl SlaveGroupId {
  pub fn to_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.clone())
  }
}

/// A global identfier of a Coord.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CoordGroupId(pub String);

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
//  QueryPath
// -------------------------------------------------------------------------------------------------

// SubNodePaths

// SubNodePaths point to a specific thread besides the main thread that is running in a
// server. Here, we define the `main` thread to be the one that messages come into.

/// `CTSubNodePath`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CTSubNodePath {
  Tablet(TabletGroupId),
  Coord(CoordGroupId),
}

trait IntoCTSubNodePath {
  fn into_ct(self) -> CTSubNodePath;
}

/// `TSubNodePath`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TSubNodePath {
  Tablet(TabletGroupId),
}

impl IntoCTSubNodePath for TSubNodePath {
  fn into_ct(self) -> CTSubNodePath {
    match self {
      TSubNodePath::Tablet(tid) => CTSubNodePath::Tablet(tid),
    }
  }
}

/// `CSubNodePath`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CSubNodePath {
  Coord(CoordGroupId),
}

impl IntoCTSubNodePath for CSubNodePath {
  fn into_ct(self) -> CTSubNodePath {
    match self {
      CSubNodePath::Coord(cid) => CTSubNodePath::Coord(cid),
    }
  }
}

// SlaveNodePath

/// `SlaveNodePath`
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlaveNodePath<SubNodePathT> {
  pub sid: SlaveGroupId,
  pub sub: SubNodePathT,
}

impl<SubNodePathT: IntoCTSubNodePath> SlaveNodePath<SubNodePathT> {
  pub fn into_ct(self) -> CTNodePath {
    SlaveNodePath { sid: self.sid, sub: self.sub.into_ct() }
  }
}

/// `CTNodePath`
pub type CTNodePath = SlaveNodePath<CTSubNodePath>;

/// `TNodePath`
pub type TNodePath = SlaveNodePath<TSubNodePath>;

/// `CNodePath`
pub type CNodePath = SlaveNodePath<CSubNodePath>;

// QueryPaths

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlaveQueryPath<SubNodePathT> {
  pub node_path: SlaveNodePath<SubNodePathT>,
  pub query_id: QueryId,
}

impl<SubNodePathT: IntoCTSubNodePath> SlaveQueryPath<SubNodePathT> {
  pub fn into_ct(self) -> CTQueryPath {
    CTQueryPath {
      node_path: CTNodePath { sid: self.node_path.sid, sub: self.node_path.sub.into_ct() },
      query_id: self.query_id,
    }
  }
}

/// `CTQueryPath`
pub type CTQueryPath = SlaveQueryPath<CTSubNodePath>;

/// `TQueryPath`
pub type TQueryPath = SlaveQueryPath<TSubNodePath>;

/// `CQueryPath`
pub type CQueryPath = SlaveQueryPath<CSubNodePath>;

// -------------------------------------------------------------------------------------------------
//  Paxos
// -------------------------------------------------------------------------------------------------

/// Used to identify a Leadership in a given PaxosGroup.
/// The default total ordering is works, since `Gen` is compared first.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeadershipId {
  pub gen: Gen,
  pub eid: EndpointId,
}

/// Used to identify a PaxosGroup in the system.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PaxosGroupId {
  Master,
  Slave(SlaveGroupId),
}

// -------------------------------------------------------------------------------------------------
//  Subquery Context
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransTableLocationPrefix {
  pub source: CTQueryPath,
  pub trans_table_name: TransTableName,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextSchema {
  pub column_context_schema: Vec<ColName>,
  pub trans_table_context_schema: Vec<TransTableLocationPrefix>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextRow {
  pub column_context_row: Vec<ColValN>,
  pub trans_table_context_row: Vec<usize>,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl ContextSchema {
  pub fn trans_table_names(&self) -> Vec<TransTableName> {
    self.trans_table_context_schema.iter().map(|prefix| prefix.trans_table_name.clone()).collect()
  }
}

impl Context {
  pub fn new(context_schema: ContextSchema) -> Context {
    Context { context_schema, context_rows: vec![] }
  }
}

impl TableView {
  pub fn new(col_names: Vec<ColName>) -> TableView {
    TableView { col_names, rows: Default::default() }
  }

  pub fn add_row(&mut self, row: Vec<ColValN>) {
    if let Some(count) = self.rows.get_mut(&row) {
      *count += 1;
    } else {
      self.rows.insert(row, 1);
    }
  }

  pub fn add_row_multi(&mut self, row: Vec<ColValN>, row_count: u64) {
    if let Some(count) = self.rows.get_mut(&row) {
      *count += row_count;
    } else {
      self.rows.insert(row, row_count);
    }
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
  use crate::model::common::{ColName, ColType, QueryId, TablePath, TransTableName};
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

  // DML

  // (We add DML parsed SQL data here for consistency. These don't appear in
  // `iast` because they can be constructed from SQL trivially.

  // TODO: Parse this up right
  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct CreateTable {
    pub table_path: TablePath,
    pub key_cols: Vec<(ColName, ColType)>,
    pub val_cols: Vec<(ColName, ColType)>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterOp {
    pub col_name: ColName,
    /// If the `ColName` is being deleted, then this is `None`. Otherwise, it takes
    /// on the target `ColType`.
    pub maybe_col_type: Option<ColType>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct AlterTable {
    pub table_path: TablePath,
    pub alter_op: AlterOp,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct DropTable {
    pub table_path: TablePath,
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
