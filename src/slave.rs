use crate::common::{mk_qid, IOTypes, NetworkOut};
use crate::model::common::{
  iast, proc, ColName, ColType, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message::{
  ExternalAbortedData, ExternalMessage, ExternalQueryAbort, NetworkMessage, SlaveMessage,
};
use crate::multiversion_map::MVM;
use rand::RngCore;
use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::{HashMap, HashSet};

// -----------------------------------------------------------------------------------------------
//  Table Schema
// -----------------------------------------------------------------------------------------------

/// A struct to encode the Table Schema of a table. Recall that Key Columns (which forms
/// the PrimaryKey) can't change. However, Value Columns can change, and they do so in a
/// versioned fashion with an MVM.
#[derive(Debug, Clone)]
pub struct TableSchema {
  key_cols: Vec<(ColName, ColType)>,
  val_cols: MVM<ColName, ColType>,
}

impl TableSchema {
  pub fn new(key_cols: Vec<(ColName, ColType)>, val_cols: Vec<(ColName, ColType)>) -> TableSchema {
    // We construct the map underlying an MVM using the given `val_cols`. We just
    // set the version Timestamp to be 0, and also set the initial LAT to be 0.
    let mut mvm = MVM::<ColName, ColType>::new();
    for (col_name, col_type) in val_cols {
      mvm.write(&col_name, Some(col_type), Timestamp(0));
    }
    TableSchema { key_cols, val_cols: mvm }
  }
}

// -----------------------------------------------------------------------------------------------
//  Slave State
// -----------------------------------------------------------------------------------------------

/// A struct to track an ExternalQuery so that it can be replied to.
#[derive(Debug)]
struct ExternalQueryMetadata {
  request_id: RequestId,
  eid: EndpointId,
}

/// Handles creation and cancellation of External Queries
#[derive(Debug)]
struct ExternalQueryManager {
  /// External Request management
  external_query_map: HashMap<QueryId, ExternalQueryMetadata>,
  external_query_index: HashMap<RequestId, QueryId>, // This is used to cancel an External Request
}

impl ExternalQueryManager {
  fn new() -> ExternalQueryManager {
    ExternalQueryManager {
      external_query_map: HashMap::new(),
      external_query_index: HashMap::new(),
    }
  }

  fn add_request(&mut self, query_id: QueryId, from_eid: EndpointId, request_id: RequestId) {
    self.external_query_map.insert(
      query_id.clone(),
      ExternalQueryMetadata { request_id: request_id.clone(), eid: from_eid },
    );
    self.external_query_index.insert(request_id, query_id);
  }

  fn remove_with_request_id(&mut self, request_id: &RequestId) -> Option<ExternalQueryMetadata> {
    if let Some(query_id) = self.external_query_index.remove(request_id) {
      Some(self.external_query_map.remove(&query_id).unwrap())
    } else {
      None
    }
  }

  fn remove_with_query_id(&mut self, query_id: &QueryId) -> Option<ExternalQueryMetadata> {
    if let Some(metadata) = self.external_query_map.remove(query_id) {
      self.external_query_index.remove(&metadata.request_id).unwrap();
      Some(metadata)
    } else {
      None
    }
  }
}

/// The SlaveState that holds all the state of the Slave
#[derive(Debug)]
pub struct SlaveState<T: IOTypes> {
  /// IO Objects.
  rand: T::RngCoreT,
  network_output: T::NetworkOutT,
  tablet_forward_output: T::TabletForwardOutT,

  /// External Request management
  external_query_manager: ExternalQueryManager,

  /// Gossip
  gossip_gen: u32,
  gossiped_db_schema: HashMap<TablePath, TableSchema>,

  /// Distribution
  sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  /// Metadata
  this_slave_group_id: SlaveGroupId, // The SlaveGroupId of this Slave
}

impl<T: IOTypes> SlaveState<T> {
  pub fn new(
    rand: T::RngCoreT,
    network_output: T::NetworkOutT,
    tablet_forward_output: T::TabletForwardOutT,
    gossiped_db_schema: HashMap<TablePath, TableSchema>,
    sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, EndpointId>,
    this_slave_group_id: SlaveGroupId,
  ) -> SlaveState<T> {
    SlaveState {
      rand,
      network_output,
      tablet_forward_output,
      external_query_manager: ExternalQueryManager::new(),
      gossip_gen: 0,
      gossiped_db_schema,
      sharding_config,
      tablet_address_config,
      slave_address_config,
      this_slave_group_id,
    }
  }

  /// Normally, the sender's metadata would be buried in the message itself.
  /// However, we also have to handle client messages here, we need the EndpointId
  /// passed in explicitly.
  pub fn handle_incoming_message(&mut self, from_eid: EndpointId, msg: SlaveMessage) {
    match &msg {
      SlaveMessage::PerformExternalQuery(exteral_query) => {
        // Create and store a ExternalQueryMetadata to keep track of the new request
        let query_id = mk_qid(&mut self.rand);
        self.external_query_manager.add_request(
          query_id.clone(),
          from_eid.clone(),
          exteral_query.request_id.clone(),
        );

        // Parse
        let sql = &exteral_query.query;
        let dialect = GenericDialect {};
        match Parser::parse_sql(&dialect, sql) {
          Ok(ast) => {
            println!("AST: {:#?}", ast);
            // Convert to internal AST
            let iast = convert_ast(&ast);
            println!("iAST: {:#?}", iast);
          }
          Err(e) => {
            println!("Error!!!!");
            // Extract error string
            let err_string = match e {
              TokenizerError(s) => s,
              ParserError(s) => s,
            };

            // Reply to the client with the error
            if let Some(metadata) = self.external_query_manager.remove_with_query_id(&query_id) {
              let response = ExternalQueryAbort {
                request_id: metadata.request_id.clone(),
                error: ExternalAbortedData::ParseError(err_string),
              };
              self.network_output.send(
                &metadata.eid,
                NetworkMessage::External(ExternalMessage::ExternalQueryAbort(response)),
              );
            }
          }
        }
      }
      SlaveMessage::CancelExternalQuery(_) => panic!("support"),
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Convert from sqlparser AST to internal AST
// -----------------------------------------------------------------------------------------------

/// This function converts the sqlparser AST into our own internal
/// AST, `Query`. Recall that we can transform all DML and DQL transactions
/// together into a single Query, which is what we do here.
fn convert_ast(raw_query: &Vec<ast::Statement>) -> iast::Query {
  assert!(raw_query.len() == 1, "Only one SQL statement support atm.");
  let stmt = &raw_query[0];
  match stmt {
    ast::Statement::Query(query) => return convert_query(query),
    _ => panic!("Unsupported ast::Statement {:?}", stmt),
  }
}

fn convert_query(query: &ast::Query) -> iast::Query {
  let mut ictes = Vec::<(String, iast::Query)>::new();
  if let Some(with) = &query.with {
    for cte in &with.cte_tables {
      ictes.push((cte.alias.name.value.clone(), convert_query(&cte.query)));
    }
  }
  let body = match &query.body {
    ast::SetExpr::Query(child_query) => {
      iast::QueryBody::Query(Box::new(convert_query(child_query)))
    }
    ast::SetExpr::Select(select) => {
      let from_clause = &select.from;
      assert!(from_clause.len() == 1, "Joins with ',' not supported");
      assert!(from_clause[0].joins.is_empty(), "Joins not supported");
      if let ast::TableFactor::Table { name, .. } = &from_clause[0].relation {
        let ast::ObjectName(idents) = name;
        assert!(idents.len() == 1, "Multi-part table references not supported");
        iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
          projection: convert_select_clause(&select.projection),
          from: idents[0].value.clone(),
          selection: if let Some(selection) = &select.selection {
            convert_expr(selection)
          } else {
            iast::ValExpr::Value { val: iast::Value::Boolean(true) }
          },
        })
      } else {
        panic!("TableFactor {:?} not supported", &from_clause[0].relation);
      }
    }
    ast::SetExpr::Insert(stmt) => {
      if let ast::Statement::Update { table_name, assignments, selection } = stmt {
        let ast::ObjectName(idents) = table_name;
        assert!(idents.len() == 1, "Multi-part table references not supported");
        iast::QueryBody::Update(iast::Update {
          table: idents[0].value.clone(),
          assignments: assignments
            .iter()
            .map(|a| (a.id.value.clone(), convert_expr(&a.value)))
            .collect(),
          selection: if let Some(selection) = selection {
            convert_expr(selection)
          } else {
            iast::ValExpr::Value { val: iast::Value::Boolean(true) }
          },
        })
      } else {
        panic!("Statement type {:?} not supported", stmt)
      }
    }
    _ => panic!("Other stuff not supported"),
  };
  iast::Query { ctes: ictes, body }
}

fn convert_select_clause(select_clause: &Vec<ast::SelectItem>) -> Vec<String> {
  let mut select_list = Vec::<String>::new();
  for item in select_clause {
    match &item {
      ast::SelectItem::UnnamedExpr(expr) => {
        if let ast::Expr::Identifier(ident) = expr {
          select_list.push(ident.value.clone());
        } else {
          panic!("{:?} is not supported in SelectItem", item);
        }
      }
      _ => {
        panic!("{:?} is not supported in SelectItem", item);
      }
    }
  }
  select_list
}

fn convert_value(value: &ast::Value) -> iast::Value {
  match &value {
    ast::Value::Number(num, _) => iast::Value::Number(num.clone()),
    ast::Value::SingleQuotedString(string) => iast::Value::QuotedString(string.clone()),
    ast::Value::DoubleQuotedString(string) => iast::Value::QuotedString(string.clone()),
    ast::Value::Boolean(bool) => iast::Value::Boolean(bool.clone()),
    ast::Value::Null => iast::Value::Null,
    _ => panic!("Value type {:?} not supported.", value),
  }
}

fn convert_expr(expr: &ast::Expr) -> iast::ValExpr {
  match expr {
    ast::Expr::Identifier(ident) => iast::ValExpr::ColumnRef { col_ref: ident.value.clone() },
    ast::Expr::CompoundIdentifier(idents) => {
      assert!(idents.len() == 2, "The only prefix fix for a column should be the table.");
      iast::ValExpr::ColumnRef { col_ref: idents[1].value.clone() }
    }
    ast::Expr::UnaryOp { op, expr } => {
      let iop = match op {
        ast::UnaryOperator::Minus => iast::UnaryOp::Minus,
        ast::UnaryOperator::Plus => iast::UnaryOp::Plus,
        ast::UnaryOperator::Not => iast::UnaryOp::Not,
        _ => panic!("UnaryOperator {:?} not supported", op),
      };
      iast::ValExpr::UnaryExpr { op: iop, expr: Box::new(convert_expr(expr)) }
    }
    ast::Expr::IsNull(expr) => {
      iast::ValExpr::UnaryExpr { op: iast::UnaryOp::IsNull, expr: Box::new(convert_expr(expr)) }
    }
    ast::Expr::IsNotNull(expr) => {
      iast::ValExpr::UnaryExpr { op: iast::UnaryOp::IsNotNull, expr: Box::new(convert_expr(expr)) }
    }
    ast::Expr::BinaryOp { op, left, right } => {
      let iop = match op {
        ast::BinaryOperator::Plus => iast::BinaryOp::Plus,
        ast::BinaryOperator::Minus => iast::BinaryOp::Minus,
        ast::BinaryOperator::Multiply => iast::BinaryOp::Multiply,
        ast::BinaryOperator::Divide => iast::BinaryOp::Divide,
        ast::BinaryOperator::Modulus => iast::BinaryOp::Modulus,
        ast::BinaryOperator::StringConcat => iast::BinaryOp::StringConcat,
        ast::BinaryOperator::Gt => iast::BinaryOp::Gt,
        ast::BinaryOperator::Lt => iast::BinaryOp::Lt,
        ast::BinaryOperator::GtEq => iast::BinaryOp::GtEq,
        ast::BinaryOperator::LtEq => iast::BinaryOp::LtEq,
        ast::BinaryOperator::Spaceship => iast::BinaryOp::Spaceship,
        ast::BinaryOperator::Eq => iast::BinaryOp::Eq,
        ast::BinaryOperator::NotEq => iast::BinaryOp::NotEq,
        ast::BinaryOperator::And => iast::BinaryOp::And,
        ast::BinaryOperator::Or => iast::BinaryOp::Or,
        _ => panic!("BinaryOperator {:?} not supported", op),
      };
      iast::ValExpr::BinaryExpr {
        op: iop,
        left: Box::new(convert_expr(left)),
        right: Box::new(convert_expr(right)),
      }
    }
    ast::Expr::Value(value) => iast::ValExpr::Value { val: convert_value(value) },
    ast::Expr::Subquery(query) => iast::ValExpr::Subquery { query: Box::new(convert_query(query)) },
    _ => panic!("Expr {:?} not supported", expr),
  }
}
