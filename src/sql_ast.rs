// -------------------------------------------------------------------------------------------------
// Processed SQL
// -------------------------------------------------------------------------------------------------

pub mod proc {
  use crate::common::{ColName, ColType, TablePath, TransTableName};
  use crate::sql_ast::iast::{BinaryOp, JoinType, UnaryAggregateOp, UnaryOp, Value};
  use serde::{Deserialize, Serialize};
  use std::collections::BTreeMap;

  // Basic types

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct TableSource {
    pub table_path: TablePath,
    pub alias: String,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct TransTableSource {
    pub trans_table_name: TransTableName,
    pub alias: String,
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
  pub enum SelectExprItem {
    ValExpr(ValExpr),
    UnaryAggregate(UnaryAggregate),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum SelectItem {
    ExprWithAlias {
      item: SelectExprItem,
      alias: Option<ColName>,
    },
    /// Here, `source` is the particular Table Alias to in the `from` clause.
    /// (This is only useful for `JoinSelect`s).
    Wildcard {
      table_name: Option<String>,
    },
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct TableSelect {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: TableSource,
    pub selection: ValExpr,

    /// The TransTable Schema produced by this query
    pub schema: Vec<Option<ColName>>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct TransTableSelect {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: TransTableSource,
    pub selection: ValExpr,

    /// The TransTable Schema produced by this query
    pub schema: Vec<Option<ColName>>,
  }

  /// Expresses dependencies between `JoinNode`s
  pub type DependencyGraph = BTreeMap<String, String>;

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct JoinSelect {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: JoinNode,

    /// Maps nodes in the Join Tree to each other to express execution dependency.
    /// The identifiers look like `LRLR`, which represents the path down the Join Tree.
    /// In particular, dependencies are only between sibling `JoinNode`s and are not just
    /// restricted to leaves.
    pub dependency_graph: DependencyGraph,

    /// The TransTable Schema produced by this query
    pub schema: Vec<Option<ColName>>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Update {
    pub table: TableSource,
    pub assignment: Vec<(ColName, ValExpr)>,
    pub selection: ValExpr,

    /// The TransTable Schema produced by this query
    pub schema: Vec<Option<ColName>>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Insert {
    pub table: TableSource,
    /// The columns to insert to
    pub columns: Vec<ColName>,
    /// The values to insert (where the inner `Vec` is a row)
    pub values: Vec<Vec<ValExpr>>,

    /// The TransTable Schema produced by this query
    pub schema: Vec<Option<ColName>>,
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Delete {
    pub table: TableSource,
    pub selection: ValExpr,

    /// The TransTable Schema produced by this query
    pub schema: Vec<Option<ColName>>,
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

    /// Conjunctions that can be pushed down the `JoinTree` with no restriction.
    pub strong_conjunctions: Vec<ValExpr>,
    /// Conjunctions that can only be pushed down the `JoinTree` only after
    /// being careful of LEFT/RIGHT/OUTER JOINs.
    pub weak_conjunctions: Vec<ValExpr>,
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
    TableSelect(TableSelect),
    TransTableSelect(TransTableSelect),
    JoinSelect(JoinSelect),
  }

  impl GRQueryStage {
    pub fn schema(&self) -> &Vec<Option<ColName>> {
      match self {
        GRQueryStage::TableSelect(query) => &query.schema,
        GRQueryStage::TransTableSelect(query) => &query.schema,
        GRQueryStage::JoinSelect(query) => &query.schema,
      }
    }
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct GRQuery {
    pub trans_tables: Vec<(TransTableName, GRQueryStage)>,
    pub returning: TransTableName,
  }

  // MS

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum MSQueryStage {
    TableSelect(TableSelect),
    TransTableSelect(TransTableSelect),
    JoinSelect(JoinSelect),
    Update(Update),
    Insert(Insert),
    Delete(Delete),
  }

  impl MSQueryStage {
    pub fn schema(&self) -> &Vec<Option<ColName>> {
      match self {
        MSQueryStage::TableSelect(query) => &query.schema,
        MSQueryStage::TransTableSelect(query) => &query.schema,
        MSQueryStage::JoinSelect(query) => &query.schema,
        MSQueryStage::Update(query) => &query.schema,
        MSQueryStage::Insert(query) => &query.schema,
        MSQueryStage::Delete(query) => &query.schema,
      }
    }
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
    Subquery {
      query: Box<Query>,
      /// The `trans_table_name` is a convenience field we populate in the `query_converter`.
      /// It is supposed to be the be `TransTableName` that is returned by the `GRQuery` that
      /// this Subquery gets converted to later.
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

  impl JoinType {
    pub fn non_left(&self) -> bool {
      self == &JoinType::Right || self == &JoinType::Inner
    }

    pub fn non_right(&self) -> bool {
      self == &JoinType::Left || self == &JoinType::Inner
    }
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
    DerivedTable {
      query: Box<Query>,
      lateral: bool,
      /// The `trans_table_name` is a convenience field we populate in the `query_converter`.
      /// It is supposed to be the be `TransTableName` that is returned by the `GRQuery` that
      /// this Subquery gets converted to later.
      trans_table_name: Option<String>,
    },
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
    Select(Select),
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
  pub enum SelectExprItem {
    ValExpr(ValExpr),
    UnaryAggregate(UnaryAggregate),
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub enum SelectItem {
    ExprWithAlias {
      item: SelectExprItem,
      alias: Option<String>,
    },
    /// Here, `source` is the particular Table Alias to in the `from` clause.
    /// (This is only useful for `JoinSelect`s).
    Wildcard {
      table_name: Option<String>,
    },
  }

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct Select {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
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
