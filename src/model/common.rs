use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
/// Note: We avoid defining a new type so we can use the arithmetic operators easily
pub type Timestamp = u128;

/// A Type used to represent a generation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub struct Gen(pub u64);

// -------------------------------------------------------------------------------------------------
//  Transaction Data Structures
// -------------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TableView {
  /// The keys are the rows, and the values are the number of repetitions.
  pub col_names: Vec<Option<ColName>>,
  /// The keys are the rows, and the values are the number of repetitions.
  pub rows: BTreeMap<Vec<ColValN>, u64>,
}

/// A TablePath should appear here iff that Table is written to in the MSQuery.
/// The Tier for every such TablePath here is the Tier that MSTableRead should be
/// using to read. If the TransTable created by the MSQuery stage that uses this
/// `TierMap` corresponds to an Update, the Tier for TablePath being updated should
/// be one ahead (i.e. lower).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TierMap {
  pub map: BTreeMap<TablePath, u32>,
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
pub struct QueryId(pub String);

/// A global identfier of a Slave.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlaveGroupId(pub String);

/// A global identfier of a Coord.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CoordGroupId(pub String);

/// A request ID used to help the sender cancel the query if they wanted to.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(pub String);

// -------------------------------------------------------------------------------------------------
//  PaxosGroupIdTrait
// -------------------------------------------------------------------------------------------------

pub trait PaxosGroupIdTrait {
  fn to_gid(&self) -> PaxosGroupId;
}

impl PaxosGroupIdTrait for SlaveGroupId {
  fn to_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.clone())
  }
}

impl PaxosGroupIdTrait for TNodePath {
  fn to_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.sid.clone())
  }
}

impl PaxosGroupIdTrait for CNodePath {
  fn to_gid(&self) -> PaxosGroupId {
    PaxosGroupId::Slave(self.sid.clone())
  }
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

pub trait IntoCTSubNodePath {
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

// Gen
impl Gen {
  pub fn next(&self) -> Gen {
    Gen(self.0 + 1)
  }

  pub fn inc(&mut self) {
    self.0 += 1;
  }
}

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
  pub column_context_schema: Vec<proc::ColumnRef>,
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
  pub fn new(col_names: Vec<Option<ColName>>) -> TableView {
    TableView { col_names, rows: Default::default() }
  }

  pub fn add_row(&mut self, row: Vec<ColValN>) {
    self.add_row_multi(row, 1);
  }

  pub fn add_row_multi(&mut self, row: Vec<ColValN>, row_count: u64) {
    if let Some(count) = self.rows.get_mut(&row) {
      *count += row_count;
    } else if row_count > 0 {
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
  use crate::model::common::iast::{BinaryOp, UnaryAggregateOp, UnaryOp, Value};
  use crate::model::common::{ColName, ColType, TablePath, TransTableName};
  use serde::{Deserialize, Serialize};

  // Basic types

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum GeneralSourceRef {
    TablePath(TablePath),
    TransTableName(TransTableName),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SimpleSource {
    pub source_ref: TablePath,
    pub alias: Option<String>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct GeneralSource {
    pub source_ref: GeneralSourceRef,
    pub alias: Option<String>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
  pub struct ColumnRef {
    pub table_name: Option<String>,
    pub col_name: ColName,
  }

  // Query Types

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum ValExpr {
    ColumnRef(ColumnRef),
    UnaryExpr { op: UnaryOp, expr: Box<ValExpr> },
    BinaryExpr { op: BinaryOp, left: Box<ValExpr>, right: Box<ValExpr> },
    Value { val: Value },
    Subquery { query: Box<GRQuery> },
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct UnaryAggregate {
    pub distinct: bool,
    pub op: UnaryAggregateOp,
    pub expr: ValExpr,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum SelectItem {
    ValExpr(ValExpr),
    UnaryAggregate(UnaryAggregate),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SuperSimpleSelect {
    pub distinct: bool,
    pub projection: Vec<(SelectItem, Option<ColName>)>,
    pub from: GeneralSource,
    pub selection: ValExpr,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Update {
    pub table: SimpleSource,
    pub assignment: Vec<(ColName, ValExpr)>,
    pub selection: ValExpr,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Insert {
    pub table: SimpleSource,
    /// The columns to insert to
    pub columns: Vec<ColName>,
    /// The values to insert (where the inner `Vec` is a row)
    pub values: Vec<Vec<Value>>,
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
    Insert(Insert),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct MSQuery {
    pub trans_tables: Vec<(TransTableName, MSQueryStage)>,
    pub returning: TransTableName,
  }

  // DML

  // (We add DML parsed SQL data here for consistency. These don't appear in
  // `iast` because they can be constructed from SQL trivially.

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

  // Implementations

  impl GeneralSource {
    pub fn name(&self) -> &String {
      if let Some(alias) = &self.alias {
        alias
      } else {
        match &self.source_ref {
          GeneralSourceRef::TablePath(TablePath(name)) => name,
          GeneralSourceRef::TransTableName(TransTableName(name)) => name,
        }
      }
    }
  }

  impl SimpleSource {
    pub fn to_read_source(&self) -> GeneralSource {
      GeneralSource {
        source_ref: GeneralSourceRef::TablePath(self.source_ref.clone()),
        alias: self.alias.clone(),
      }
    }
  }
}

// -------------------------------------------------------------------------------------------------
// SQL
// -------------------------------------------------------------------------------------------------

pub mod iast {
  use serde::{Deserialize, Serialize};

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum ValExpr {
    ColumnRef { table_name: Option<String>, col_name: String },
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
    Insert(Insert),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct TableRef {
    pub source_ref: String,
    pub alias: Option<String>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum UnaryAggregateOp {
    Count,
    Sum,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct UnaryAggregate {
    pub distinct: bool,
    pub op: UnaryAggregateOp,
    pub expr: ValExpr,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum SelectItem {
    ValExpr(ValExpr),
    UnaryAggregate(UnaryAggregate),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SuperSimpleSelect {
    pub distinct: bool,
    pub projection: Vec<(SelectItem, Option<String>)>, // The select clause
    pub from: TableRef,
    pub selection: ValExpr, // The where clause
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Update {
    pub table: TableRef,
    pub assignments: Vec<(String, ValExpr)>,
    pub selection: ValExpr,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Insert {
    pub table: TableRef,
    /// The columns to insert to
    pub columns: Vec<String>,
    /// The values to insert (where the inner `Vec` is a row)
    pub values: Vec<Vec<Value>>,
  }
}
