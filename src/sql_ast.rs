// -------------------------------------------------------------------------------------------------
// Processed SQL
// -------------------------------------------------------------------------------------------------

pub mod proc {
  use crate::common::{ColName, ColType, TablePath, TransTableName};
  use crate::sql_ast::iast::{BinaryOp, JoinType, UnaryAggregateOp, UnaryOp, Value};
  use serde::{Deserialize, Serialize};

  // Basic types

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SimpleSource {
    pub source_ref: TablePath,
    pub alias: String,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum GeneralSource {
    TablePath { table_path: TablePath, alias: String },
    TransTableName { trans_table_name: TransTableName, alias: String },
    JoinNode(JoinNode),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
  pub struct ColumnRef {
    pub table_name: String,
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
  pub enum SelectClause {
    SelectList(Vec<(SelectItem, Option<ColName>)>),
    Wildcard,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SuperSimpleSelect {
    pub distinct: bool,
    pub projection: SelectClause,
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
    pub values: Vec<Vec<ValExpr>>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Delete {
    pub table: SimpleSource,
    pub selection: ValExpr,
  }

  // Join

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum JoinNode {
    JoinInnerNode(JoinInnerNode),
    JoinLeaf(JoinLeaf),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct JoinInnerNode {
    pub left: Box<JoinNode>,
    pub right: Box<JoinNode>,
    pub join_type: JoinType,
    pub on: ValExpr,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct JoinLeaf {
    pub alias: String,
    pub lateral: bool,
    pub query: GRQuery,
  }

  // GR

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum GRQueryStage {
    SuperSimpleSelect(SuperSimpleSelect),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct GRQuery {
    pub trans_tables: Vec<(TransTableName, (Vec<Option<ColName>>, GRQueryStage))>,
    pub returning: TransTableName,
  }

  // MS

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum MSQueryStage {
    SuperSimpleSelect(SuperSimpleSelect),
    Update(Update),
    Insert(Insert),
    Delete(Delete),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct MSQuery {
    pub trans_tables: Vec<(TransTableName, (Vec<Option<ColName>>, MSQueryStage))>,
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

  // TODO: remove this
  impl GeneralSource {
    pub fn name(&self) -> &String {
      match self {
        GeneralSource::TablePath { alias, .. } => alias,
        GeneralSource::TransTableName { alias, .. } => alias,
        GeneralSource::JoinNode(_) => panic!(),
      }
    }

    pub fn to_table_path(&self) -> &TablePath {
      match &self {
        GeneralSource::TablePath { table_path, .. } => table_path,
        _ => panic!(),
      }
    }
  }

  impl SimpleSource {
    pub fn name(&self) -> &String {
      &self.alias
    }

    pub fn to_read_source(&self) -> GeneralSource {
      GeneralSource::TablePath { table_path: self.source_ref.clone(), alias: self.alias.clone() }
    }
  }
}

// -------------------------------------------------------------------------------------------------
// SQL
// -------------------------------------------------------------------------------------------------

pub mod iast {
  use crate::common::TablePath;
  use serde::{Deserialize, Serialize};

  // Expression

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum ValExpr {
    ColumnRef {
      table_name: Option<String>,
      col_name: String,
    },
    UnaryExpr {
      op: UnaryOp,
      expr: Box<ValExpr>,
    },
    BinaryExpr {
      op: BinaryOp,
      left: Box<ValExpr>,
      right: Box<ValExpr>,
    },
    Value {
      val: Value,
    },
    /// The `trans_table_name` is a convenience field we populate in the `query_converter`.
    Subquery {
      query: Box<Query>,
      trans_table_name: Option<String>,
    },
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

  // Join

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum JoinType {
    Inner,
    Outer,
    Left,
    Right,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum JoinNode {
    JoinInnerNode(JoinInnerNode),
    JoinLeaf(JoinLeaf),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct JoinInnerNode {
    pub left: Box<JoinNode>,
    pub right: Box<JoinNode>,
    pub join_type: JoinType,
    pub on: ValExpr,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct JoinLeaf {
    pub alias: Option<String>,
    pub source: JoinNodeSource,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum JoinNodeSource {
    Table(String),
    DerivedTable { query: Box<Query>, lateral: bool },
  }

  // Query

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
    Delete(Delete),
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
    Avg,
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
  pub enum SelectClause {
    SelectList(Vec<(SelectItem, Option<String>)>),
    Wildcard,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct SuperSimpleSelect {
    pub distinct: bool,
    pub projection: SelectClause,
    pub from: JoinNode,
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
    pub values: Vec<Vec<ValExpr>>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Delete {
    pub table: TableRef,
    pub selection: ValExpr,
  }

  // Implmentations

  impl JoinLeaf {
    pub fn join_leaf_name(&self) -> Option<&String> {
      if let Some(jln) = &self.alias {
        Some(jln)
      } else {
        match &self.source {
          JoinNodeSource::Table(table_name) => Some(table_name),
          JoinNodeSource::DerivedTable { .. } => None,
        }
      }
    }
  }
}
