use crate::model::common::{iast, ColName, ColType};
use crate::model::common::{proc, TablePath};
use sqlparser::ast;
use sqlparser::ast::{ColumnOption, DataType, Query, Statement};
use sqlparser::test_utils::table;

// TODO: We should return a Result if there is a conversion failure to MSQuery.

// -----------------------------------------------------------------------------------------------
//  DQL Query Parsing
// -----------------------------------------------------------------------------------------------

/// This function converts the sqlparser AST into our own internal
/// AST, `Query`. Recall that we can transform all DML and DQL transactions
/// together into a single Query, which is what we do here.
pub fn convert_ast(raw_query: Vec<ast::Statement>) -> iast::Query {
  assert_eq!(raw_query.len(), 1, "Only one SQL statement support atm.");
  let stmt = raw_query.into_iter().next().unwrap();
  match stmt {
    ast::Statement::Query(query) => convert_query(*query),
    ast::Statement::Insert { table_name, columns, source, .. } => {
      iast::Query { ctes: vec![], body: convert_insert(table_name, columns, source) }
    }
    ast::Statement::Update { table_name, assignments, selection } => {
      iast::Query { ctes: vec![], body: convert_update(table_name, assignments, selection) }
    }
    _ => panic!("Unsupported ast::Statement {:?}", stmt),
  }
}

fn convert_query(query: ast::Query) -> iast::Query {
  let mut ictes = Vec::<(String, iast::Query)>::new();
  if let Some(with) = query.with {
    for cte in with.cte_tables {
      ictes.push((cte.alias.name.value.clone(), convert_query(cte.query)));
    }
  }
  let body = match query.body {
    ast::SetExpr::Query(child_query) => {
      iast::QueryBody::Query(Box::new(convert_query(*child_query)))
    }
    ast::SetExpr::Select(select) => {
      let from_clause = &select.from;
      assert_eq!(from_clause.len(), 1, "Joins with ',' not supported");
      assert!(from_clause[0].joins.is_empty(), "Joins not supported");
      if let ast::TableFactor::Table { name, .. } = &from_clause[0].relation {
        let ast::ObjectName(idents) = name;
        assert_eq!(idents.len(), 1, "Multi-part table references not supported");
        iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
          projection: convert_select_clause(&select.projection),
          from: idents[0].value.clone(),
          selection: if let Some(selection) = select.selection {
            convert_expr(selection)
          } else {
            iast::ValExpr::Value { val: iast::Value::Boolean(true) }
          },
        })
      } else {
        panic!("TableFactor {:?} not supported", &from_clause[0].relation);
      }
    }
    ast::SetExpr::Insert(stmt) => match stmt {
      Statement::Insert { table_name, columns, source, .. } => {
        convert_insert(table_name, columns, source)
      }
      Statement::Update { table_name, assignments, selection } => {
        convert_update(table_name, assignments, selection)
      }
      _ => panic!("Unsupported ast::Statement {:?}", stmt),
    },
    _ => panic!("Other stuff not supported"),
  };
  iast::Query { ctes: ictes, body }
}

fn convert_insert(
  table_name: ast::ObjectName,
  columns: Vec<ast::Ident>,
  source: Box<Query>,
) -> iast::QueryBody {
  if let ast::SetExpr::Values(values) = source.body {
    // Construct values
    let mut i_values = Vec::<Vec<iast::Value>>::new();
    for row in values.0 {
      let mut i_row = Vec::<iast::Value>::new();
      for elem in row {
        if let iast::ValExpr::Value { val } = convert_expr(elem) {
          i_row.push(val);
        } else {
          panic!("Only literal are supported in the VALUES clause.");
        }
      }
      i_values.push(i_row);
    }
    // Construct Table name
    let ast::ObjectName(idents) = table_name;
    assert_eq!(idents.len(), 1, "Multi-part table references not supported");
    let i_table = idents.into_iter().next().unwrap().value;
    // Construct Columns
    let mut i_columns = Vec::<String>::new();
    for col in columns {
      i_columns.push(col.value)
    }
    iast::QueryBody::Insert(iast::Insert { table: i_table, columns: i_columns, values: i_values })
  } else {
    panic!("Non VALUEs clause in Insert is unsupported.");
  }
}

fn convert_update(
  table_name: ast::ObjectName,
  assignments: Vec<ast::Assignment>,
  selection: Option<ast::Expr>,
) -> iast::QueryBody {
  let ast::ObjectName(idents) = table_name;
  assert_eq!(idents.len(), 1, "Multi-part table references not supported");
  iast::QueryBody::Update(iast::Update {
    table: idents[0].value.clone(),
    assignments: assignments
      .into_iter()
      .map(|a| (a.id.value.clone(), convert_expr(a.value)))
      .collect(),
    selection: if let Some(selection) = selection {
      convert_expr(selection)
    } else {
      iast::ValExpr::Value { val: iast::Value::Boolean(true) }
    },
  })
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

fn convert_value(value: ast::Value) -> iast::Value {
  match &value {
    ast::Value::Number(num, _) => iast::Value::Number(num.clone()),
    ast::Value::SingleQuotedString(string) => iast::Value::QuotedString(string.clone()),
    ast::Value::DoubleQuotedString(string) => iast::Value::QuotedString(string.clone()),
    ast::Value::Boolean(bool) => iast::Value::Boolean(bool.clone()),
    ast::Value::Null => iast::Value::Null,
    _ => panic!("Value type {:?} not supported.", value),
  }
}

fn convert_expr(expr: ast::Expr) -> iast::ValExpr {
  match expr {
    ast::Expr::Identifier(ident) => iast::ValExpr::ColumnRef { col_ref: ident.value.clone() },
    ast::Expr::CompoundIdentifier(idents) => {
      assert_eq!(idents.len(), 2, "The only prefix fix for a column should be the table.");
      iast::ValExpr::ColumnRef { col_ref: idents[1].value.clone() }
    }
    ast::Expr::UnaryOp { op, expr } => {
      let iop = match op {
        ast::UnaryOperator::Minus => iast::UnaryOp::Minus,
        ast::UnaryOperator::Plus => iast::UnaryOp::Plus,
        ast::UnaryOperator::Not => iast::UnaryOp::Not,
        _ => panic!("UnaryOperator {:?} not supported", op),
      };
      iast::ValExpr::UnaryExpr { op: iop, expr: Box::new(convert_expr(*expr)) }
    }
    ast::Expr::IsNull(expr) => {
      iast::ValExpr::UnaryExpr { op: iast::UnaryOp::IsNull, expr: Box::new(convert_expr(*expr)) }
    }
    ast::Expr::IsNotNull(expr) => {
      iast::ValExpr::UnaryExpr { op: iast::UnaryOp::IsNotNull, expr: Box::new(convert_expr(*expr)) }
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
        left: Box::new(convert_expr(*left)),
        right: Box::new(convert_expr(*right)),
      }
    }
    ast::Expr::Value(value) => iast::ValExpr::Value { val: convert_value(value) },
    ast::Expr::Subquery(query) => {
      iast::ValExpr::Subquery { query: Box::new(convert_query(*query)) }
    }
    _ => panic!("Expr {:?} not supported", expr),
  }
}

// -----------------------------------------------------------------------------------------------
//  DQL Query Parsing
// -----------------------------------------------------------------------------------------------

pub enum DDLQuery {
  Create(proc::CreateTable),
  Alter(proc::AlterTable),
  Drop(proc::DropTable),
}

/// This function converts the sqlparser AST into an internal DDL struct.
pub fn convert_ddl_ast(raw_query: &Vec<ast::Statement>) -> DDLQuery {
  assert_eq!(raw_query.len(), 1, "Only one SQL statement support atm.");
  let stmt = &raw_query[0];
  match stmt {
    ast::Statement::CreateTable { name, columns, .. } => {
      let table_path = TablePath(name.0.get(0).unwrap().value.clone());
      let mut key_cols = Vec::<(ColName, ColType)>::new();
      let mut val_cols = Vec::<(ColName, ColType)>::new();
      for col in columns {
        let col_name = ColName(col.name.value.clone());
        let col_type = match &col.data_type {
          DataType::Varchar(_) => ColType::String,
          DataType::Int => ColType::Int,
          DataType::Boolean => ColType::Bool,
          _ => panic!("Unsupported Create Table datatype {:?}", col.data_type),
        };
        let mut is_key_col = false;
        for option_def in &col.options {
          if let ColumnOption::Unique { is_primary, .. } = &option_def.option {
            if *is_primary {
              is_key_col = true;
            }
          }
        }
        if is_key_col {
          key_cols.push((col_name, col_type));
        } else {
          val_cols.push((col_name, col_type));
        }
      }
      DDLQuery::Create(proc::CreateTable { table_path, key_cols, val_cols })
    }
    ast::Statement::Drop { names, .. } => {
      let name = names.get(0).unwrap().clone();
      let table_path = TablePath(name.0.get(0).unwrap().value.clone());
      DDLQuery::Drop(proc::DropTable { table_path })
    }
    ast::Statement::AlterTable { name, operation } => match operation {
      ast::AlterTableOperation::AddColumn { column_def } => DDLQuery::Alter(proc::AlterTable {
        table_path: TablePath(name.0.get(0).unwrap().value.clone()),
        alter_op: proc::AlterOp {
          col_name: ColName(column_def.name.value.clone()),
          maybe_col_type: Some(convert_data_type(&column_def.data_type)),
        },
      }),
      ast::AlterTableOperation::DropColumn { column_name, .. } => {
        DDLQuery::Alter(proc::AlterTable {
          table_path: TablePath(name.0.get(0).unwrap().value.clone()),
          alter_op: proc::AlterOp {
            col_name: ColName(column_name.value.clone()),
            maybe_col_type: None,
          },
        })
      }
      _ => panic!("Unsupported ast::Statement {:?}", stmt),
    },
    _ => panic!("Unsupported ast::Statement {:?}", stmt),
  }
}

pub fn convert_data_type(raw_data_type: &ast::DataType) -> ColType {
  match raw_data_type {
    DataType::Int => ColType::Int,
    DataType::Boolean => ColType::Bool,
    DataType::String => ColType::String,
    _ => panic!("Unsupported ast::DataType {:?}", raw_data_type),
  }
}
