use crate::model::common::{iast, ColName, ColType};
use crate::model::common::{proc, TablePath};
use sqlparser::ast;
use sqlparser::test_utils::table;

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

/// Gets a table reference from a list of identifiers. Since we do not support multi-part
/// table references, we return an error in that case.
fn get_table_ref(idents: Vec<ast::Ident>) -> Result<String, String> {
  if idents.len() != 1 {
    Err(format!("Multi-part table references not supported"))
  } else {
    Ok(idents.into_iter().next().unwrap().value)
  }
}

// -----------------------------------------------------------------------------------------------
//  DQL Query Parsing
// -----------------------------------------------------------------------------------------------

/// Converts the Rust `sqlparser` AST into our own internal AST, `iast::Query`. Recall that
/// we can transform all DML and DQL transactions together into a single Query, which is
/// what we do here.
pub fn convert_ast(raw_query: Vec<ast::Statement>) -> Result<iast::Query, String> {
  if raw_query.is_empty() {
    return Err(format!("A SQL Transaction with no stages is not supported."));
  }

  let mut it = raw_query.into_iter().rev().enumerate();
  let (_, final_stmt) = it.next().unwrap();

  // Add all prior stages as CTEs by setting their results to Transient Tables.
  let mut ctes = Vec::<(String, iast::Query)>::new();
  while let Some((idx, stmt)) = it.next() {
    ctes.push((format!("\\rtt{:?}", idx), convert_stage(stmt)?));
    it.next();
  }

  // Add the final stage to the query
  let mut ret_query = convert_stage(final_stmt)?;
  ctes.extend(ret_query.ctes);
  ret_query.ctes = ctes;
  Ok(ret_query)
}

pub fn convert_stage(stmt: ast::Statement) -> Result<iast::Query, String> {
  match stmt {
    ast::Statement::Query(query) => convert_query(*query),
    ast::Statement::Insert { table_name, columns, source, .. } => {
      Ok(iast::Query { ctes: vec![], body: convert_insert(table_name, columns, source)? })
    }
    ast::Statement::Update { table_name, assignments, selection } => {
      Ok(iast::Query { ctes: vec![], body: convert_update(table_name, assignments, selection)? })
    }
    _ => Err(format!("Unsupported ast::Statement {:?}", stmt)),
  }
}

fn convert_query(query: ast::Query) -> Result<iast::Query, String> {
  let mut ictes = Vec::<(String, iast::Query)>::new();
  if let Some(with) = query.with {
    for cte in with.cte_tables {
      ictes.push((cte.alias.name.value, convert_query(cte.query)?));
    }
  }
  let body = match query.body {
    ast::SetExpr::Query(child_query) => {
      iast::QueryBody::Query(Box::new(convert_query(*child_query)?))
    }
    ast::SetExpr::Select(select) => {
      let from_clause = select.from;
      if from_clause.len() != 1 {
        return Err(format!("Joins with ',' not supported"));
      }
      if !from_clause[0].joins.is_empty() {
        return Err(format!("Joins not supported"));
      }
      let relation = from_clause.into_iter().next().unwrap().relation;
      if let ast::TableFactor::Table { name, .. } = relation {
        iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
          projection: convert_select_clause(select.projection)?,
          from: get_table_ref(name.0)?,
          selection: if let Some(selection) = select.selection {
            convert_expr(selection)?
          } else {
            iast::ValExpr::Value { val: iast::Value::Boolean(true) }
          },
        })
      } else {
        return Err(format!("TableFactor {:?} not supported", relation));
      }
    }
    ast::SetExpr::Insert(stmt) => match stmt {
      ast::Statement::Insert { table_name, columns, source, .. } => {
        convert_insert(table_name, columns, source)?
      }
      ast::Statement::Update { table_name, assignments, selection } => {
        convert_update(table_name, assignments, selection)?
      }
      _ => return Err(format!("Unsupported ast::Statement {:?}", stmt)),
    },
    _ => return Err(format!("Other stuff not supported")),
  };
  Ok(iast::Query { ctes: ictes, body })
}

fn convert_insert(
  table_name: ast::ObjectName,
  columns: Vec<ast::Ident>,
  source: Box<ast::Query>,
) -> Result<iast::QueryBody, String> {
  if let ast::SetExpr::Values(values) = source.body {
    // Construct values
    let mut i_values = Vec::<Vec<iast::Value>>::new();
    for row in values.0 {
      let mut i_row = Vec::<iast::Value>::new();
      for elem in row {
        if let iast::ValExpr::Value { val } = convert_expr(elem)? {
          i_row.push(val);
        } else {
          return Err(format!("Only literal are supported in the VALUES clause."));
        }
      }
      i_values.push(i_row);
    }
    // Construct Table name
    let i_table = get_table_ref(table_name.0)?;
    // Construct Columns
    let mut i_columns = Vec::<String>::new();
    for col in columns {
      i_columns.push(col.value)
    }
    Ok(iast::QueryBody::Insert(iast::Insert {
      table: i_table,
      columns: i_columns,
      values: i_values,
    }))
  } else {
    Err(format!("Non VALUEs clause in Insert is unsupported."))
  }
}

fn convert_update(
  table_name: ast::ObjectName,
  assignments: Vec<ast::Assignment>,
  selection: Option<ast::Expr>,
) -> Result<iast::QueryBody, String> {
  Ok(iast::QueryBody::Update(iast::Update {
    table: get_table_ref(table_name.0)?,
    assignments: {
      let mut internal_assignments = Vec::<(String, iast::ValExpr)>::new();
      for a in assignments {
        internal_assignments.push((a.id.value, convert_expr(a.value)?))
      }
      internal_assignments
    },
    selection: if let Some(selection) = selection {
      convert_expr(selection)?
    } else {
      iast::ValExpr::Value { val: iast::Value::Boolean(true) }
    },
  }))
}

fn convert_select_clause(select_clause: Vec<ast::SelectItem>) -> Result<Vec<String>, String> {
  let mut select_list = Vec::<String>::new();
  for item in select_clause {
    match &item {
      ast::SelectItem::UnnamedExpr(expr) => {
        if let ast::Expr::Identifier(ident) = expr {
          select_list.push(ident.value.clone());
        } else {
          return Err(format!("{:?} is not supported in SelectItem", item));
        }
      }
      _ => return Err(format!("{:?} is not supported in SelectItem", item)),
    }
  }
  Ok(select_list)
}

fn convert_value(value: ast::Value) -> Result<iast::Value, String> {
  match value {
    ast::Value::Number(num, _) => Ok(iast::Value::Number(num)),
    ast::Value::SingleQuotedString(string) => Ok(iast::Value::QuotedString(string)),
    ast::Value::DoubleQuotedString(string) => Ok(iast::Value::QuotedString(string)),
    ast::Value::Boolean(bool) => Ok(iast::Value::Boolean(bool)),
    ast::Value::Null => Ok(iast::Value::Null),
    _ => Err(format!("Value type {:?} not supported.", value)),
  }
}

fn convert_expr(expr: ast::Expr) -> Result<iast::ValExpr, String> {
  Ok(match expr {
    ast::Expr::Identifier(ident) => iast::ValExpr::ColumnRef { col_ref: ident.value },
    ast::Expr::CompoundIdentifier(idents) => {
      iast::ValExpr::ColumnRef { col_ref: get_table_ref(idents)? }
    }
    ast::Expr::UnaryOp { op, expr } => {
      let iop = match op {
        ast::UnaryOperator::Minus => iast::UnaryOp::Minus,
        ast::UnaryOperator::Plus => iast::UnaryOp::Plus,
        ast::UnaryOperator::Not => iast::UnaryOp::Not,
        _ => return Err(format!("UnaryOperator {:?} not supported", op)),
      };
      iast::ValExpr::UnaryExpr { op: iop, expr: Box::new(convert_expr(*expr)?) }
    }
    ast::Expr::IsNull(expr) => {
      iast::ValExpr::UnaryExpr { op: iast::UnaryOp::IsNull, expr: Box::new(convert_expr(*expr)?) }
    }
    ast::Expr::IsNotNull(expr) => iast::ValExpr::UnaryExpr {
      op: iast::UnaryOp::IsNotNull,
      expr: Box::new(convert_expr(*expr)?),
    },
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
        _ => return Err(format!("BinaryOperator {:?} not supported", op)),
      };
      iast::ValExpr::BinaryExpr {
        op: iop,
        left: Box::new(convert_expr(*left)?),
        right: Box::new(convert_expr(*right)?),
      }
    }
    ast::Expr::Nested(expr) => convert_expr(*expr)?,
    ast::Expr::Value(value) => iast::ValExpr::Value { val: convert_value(value)? },
    ast::Expr::Subquery(query) => {
      iast::ValExpr::Subquery { query: Box::new(convert_query(*query)?) }
    }
    _ => return Err(format!("Expr {:?} not supported", expr)),
  })
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
pub fn convert_ddl_ast(raw_query: Vec<ast::Statement>) -> Result<DDLQuery, String> {
  if raw_query.len() != 1 {
    return Err(format!("Only one SQL statement support atm."));
  }
  let stmt = raw_query.into_iter().next().unwrap();
  match &stmt {
    ast::Statement::CreateTable { name, columns, .. } => {
      let table_path = TablePath(get_table_ref(name.0.clone())?);
      let mut key_cols = Vec::<(ColName, ColType)>::new();
      let mut val_cols = Vec::<(ColName, ColType)>::new();
      for col in columns {
        let col_name = ColName(col.name.value.clone());
        let col_type = match &col.data_type {
          ast::DataType::Varchar(_) => ColType::String,
          ast::DataType::Int => ColType::Int,
          ast::DataType::Boolean => ColType::Bool,
          _ => return Err(format!("Unsupported Create Table datatype {:?}", col.data_type)),
        };
        let mut is_key_col = false;
        for option_def in &col.options {
          if let ast::ColumnOption::Unique { is_primary, .. } = &option_def.option {
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
      Ok(DDLQuery::Create(proc::CreateTable { table_path, key_cols, val_cols }))
    }
    ast::Statement::Drop { names, .. } => {
      let name = names.into_iter().next().unwrap();
      let table_path = TablePath(get_table_ref(name.0.clone())?);
      Ok(DDLQuery::Drop(proc::DropTable { table_path }))
    }
    ast::Statement::AlterTable { name, operation } => match operation {
      ast::AlterTableOperation::AddColumn { column_def } => Ok(DDLQuery::Alter(proc::AlterTable {
        table_path: TablePath(get_table_ref(name.0.clone())?),
        alter_op: proc::AlterOp {
          col_name: ColName(column_def.name.value.clone()),
          maybe_col_type: Some(convert_data_type(&column_def.data_type)?),
        },
      })),
      ast::AlterTableOperation::DropColumn { column_name, .. } => {
        Ok(DDLQuery::Alter(proc::AlterTable {
          table_path: TablePath(get_table_ref(name.0.clone())?),
          alter_op: proc::AlterOp {
            col_name: ColName(column_name.value.clone()),
            maybe_col_type: None,
          },
        }))
      }
      _ => Err(format!("Unsupported ast::Statement {:?}", stmt)),
    },
    _ => Err(format!("Unsupported ast::Statement {:?}", stmt)),
  }
}

pub fn convert_data_type(raw_data_type: &ast::DataType) -> Result<ColType, String> {
  match raw_data_type {
    ast::DataType::Int => Ok(ColType::Int),
    ast::DataType::Boolean => Ok(ColType::Bool),
    ast::DataType::String => Ok(ColType::String),
    _ => Err(format!("Unsupported ast::DataType {:?}", raw_data_type)),
  }
}
