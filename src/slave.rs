use crate::common::{mk_qid, Clock, GossipData, IOTypes, NetworkOut, TableSchema};
use crate::model::common::{
  iast, proc, ColName, ColType, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message::{
  ExternalAbortedData, ExternalMessage, ExternalQueryAbort, NetworkMessage, PerformExternalQuery,
  SlaveMessage,
};
use crate::multiversion_map::MVM;
use crate::query_converter::convert_to_msquery;
use rand::RngCore;
use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::{HashMap, HashSet};

// -----------------------------------------------------------------------------------------------
//  MSQueryCoordinationES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
struct MSQueryCoordinationES {
  request_id: RequestId,
  sender_path: EndpointId,
  timestamp: Timestamp,
  query: proc::MSQuery,
}

#[derive(Debug)]
enum FullMSQueryCoordinationES {
  QueryReplanning {
    timestamp: Timestamp,
    query: proc::MSQuery,
    orig_query: PerformExternalQuery,
  },
  MasterQueryReplanning {
    master_query_id: QueryId,
    // The below are piped from QueryReplanning
    timestamp: Timestamp,
    query: proc::MSQuery,
    orig_query: PerformExternalQuery,
  },
  Executing {
    status: MSQueryCoordinationES,
  },
}

// -----------------------------------------------------------------------------------------------
//  Slave State
// -----------------------------------------------------------------------------------------------

/// The SlaveState that holds all the state of the Slave
#[derive(Debug)]
pub struct SlaveState<T: IOTypes> {
  /// IO Objects.
  rand: T::RngCoreT,
  clock: T::ClockT,
  network_output: T::NetworkOutT,
  tablet_forward_output: T::TabletForwardOutT,

  /// Metadata
  this_slave_group_id: SlaveGroupId,

  /// Gossip
  gossip: GossipData,

  /// Distribution
  sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  /// External query management
  external_request_id_map: HashMap<RequestId, QueryId>,
  ms_coord_statuses: HashMap<QueryId, FullMSQueryCoordinationES>,
}

impl<T: IOTypes> SlaveState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
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
      clock,
      network_output,
      tablet_forward_output,
      this_slave_group_id,
      gossip: GossipData { gossip_gen: 0, gossiped_db_schema },
      sharding_config,
      tablet_address_config,
      slave_address_config,
      external_request_id_map: HashMap::new(),
      ms_coord_statuses: HashMap::new(),
    }
  }

  /// Normally, the sender's metadata would be buried in the message itself.
  /// However, we also have to handle client messages here, we need the EndpointId
  /// passed in explicitly.
  pub fn handle_incoming_message(&mut self, msg: SlaveMessage) {
    match msg {
      SlaveMessage::PerformExternalQuery(external_query) => {
        if self.external_request_id_map.contains_key(&external_query.request_id) {
          // Duplicate RequestId; respond with an abort.
          self.network_output.send(
            &external_query.sender_path,
            NetworkMessage::External(ExternalMessage::ExternalQueryAbort(ExternalQueryAbort {
              request_id: external_query.request_id,
              payload: ExternalAbortedData::NonUniqueRequestId,
            })),
          )
        } else {
          // Parse the SQL
          match Parser::parse_sql(&GenericDialect {}, &external_query.query) {
            Ok(ast) => {
              let internal_ast = convert_ast(&ast);
              match convert_to_msquery(&self.gossip.gossiped_db_schema, internal_ast) {
                Ok(ms_query) => {
                  let query_id = mk_qid(&mut self.rand);
                  self
                    .external_request_id_map
                    .insert(external_query.request_id.clone(), query_id.clone());
                  self.ms_coord_statuses.insert(
                    query_id,
                    FullMSQueryCoordinationES::QueryReplanning {
                      timestamp: self.clock.now(),
                      query: ms_query,
                      orig_query: external_query.clone(),
                    },
                  );
                }
                Err(payload) => self.network_output.send(
                  &external_query.sender_path,
                  NetworkMessage::External(ExternalMessage::ExternalQueryAbort(
                    ExternalQueryAbort { request_id: external_query.request_id, payload },
                  )),
                ),
              }
            }
            Err(e) => {
              // Extract error string
              let err_string = match e {
                TokenizerError(s) => s,
                ParserError(s) => s,
              };
              self.network_output.send(
                &external_query.sender_path,
                NetworkMessage::External(ExternalMessage::ExternalQueryAbort(ExternalQueryAbort {
                  request_id: external_query.request_id,
                  payload: ExternalAbortedData::ParseError(err_string),
                })),
              );
            }
          }
        }
      }
      SlaveMessage::CancelExternalQuery(_) => panic!("TODO: support"),
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
