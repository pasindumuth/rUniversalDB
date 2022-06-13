use crate::common::TablePath;
use crate::common::{ColName, ColType};
use crate::sql_ast::{iast, proc};
use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::test_utils::table;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

/// Gets a table name used in a DDL Query. This only supports one part in the name.
fn get_table_name(idents: Vec<ast::Ident>) -> Result<String, String> {
  if idents.len() == 1 {
    Ok(idents.into_iter().next().unwrap().value)
  } else {
    Err(format!("Invalid Table name {:?}.", idents))
  }
}

/// Gets a table reference from a list of identifiers. Since we do not support multi-part
/// table references, we return an error in that case.
fn get_table_ref(
  idents: Vec<ast::Ident>,
  alias: Option<ast::TableAlias>,
) -> Result<iast::TableRef, String> {
  Ok(iast::TableRef {
    source_ref: get_table_name(idents)?,
    alias: alias.map(|table_alias| table_alias.name.value),
  })
}

/// Gets a table reference from a list of identifiers. Since we do not support multi-part
/// table references, we return an error in that case.
fn get_column_ref(idents: Vec<ast::Ident>) -> Result<iast::ValExpr, String> {
  if idents.len() == 1 {
    Ok(iast::ValExpr::ColumnRef {
      table_name: None,
      col_name: idents.into_iter().next().unwrap().value,
    })
  } else if idents.len() == 2 {
    let mut iter = idents.into_iter();
    Ok(iast::ValExpr::ColumnRef {
      table_name: Some(iter.next().unwrap().value),
      col_name: iter.next().unwrap().value,
    })
  } else {
    Err(format!("Column Reference {:?} not supported.", idents))
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

  let mut it = raw_query.into_iter().enumerate();
  let (_, final_stmt) = it.next_back().unwrap();

  // Add all prior stages as CTEs by setting their results to Transient Tables.
  let mut ctes = Vec::<(String, iast::Query)>::new();
  while let Some((idx, stmt)) = it.next() {
    ctes.push((format!("\\rtt{:?}", idx), convert_stage(stmt)?));
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
    ast::Statement::Delete { table_name, selection, .. } => {
      Ok(iast::Query { ctes: vec![], body: convert_delete(table_name, selection)? })
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
    ast::SetExpr::Select(select) => convert_select(*select)?,
    ast::SetExpr::Insert(stmt) => match stmt {
      ast::Statement::Insert { table_name, columns, source, .. } => {
        convert_insert(table_name, columns, source)?
      }
      ast::Statement::Update { table_name, assignments, selection } => {
        convert_update(table_name, assignments, selection)?
      }
      ast::Statement::Delete { table_name, selection } => convert_delete(table_name, selection)?,
      _ => return Err(format!("Unsupported ast::Statement {:?}", stmt)),
    },
    _ => return Err(format!("Other stuff not supported")),
  };
  Ok(iast::Query { ctes: ictes, body })
}

// -----------------------------------------------------------------------------------------------
//  Select
// -----------------------------------------------------------------------------------------------

fn convert_select(select: ast::Select) -> Result<iast::QueryBody, String> {
  Ok(iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
    distinct: select.distinct,
    projection: convert_select_clause(select.projection)?,
    from: convert_from(select.from)?,
    selection: if let Some(selection) = select.selection {
      convert_expr(selection)?
    } else {
      iast::ValExpr::Value { val: iast::Value::Boolean(true) }
    },
  }))
}

fn convert_select_clause(
  select_clause: Vec<ast::SelectItem>,
) -> Result<Vec<iast::SelectItem>, String> {
  fn select_item(expr: ast::Expr) -> Result<iast::SelectExprItem, String> {
    // We hande `func` as a special case, since `convert_expr` ignores it.
    if let ast::Expr::Function(func) = expr {
      let func_name = &func.name.0.get(0).unwrap().value.clone();
      let op = match &func_name.to_lowercase()[..] {
        "count" => iast::UnaryAggregateOp::Count,
        "sum" => iast::UnaryAggregateOp::Sum,
        "avg" => iast::UnaryAggregateOp::Avg,
        _ => return Err(format!("{:?} aggregate function", func_name)),
      };
      let expr = cast!(ast::FunctionArg::Unnamed, func.args.get(0).unwrap()).unwrap();
      let item = iast::SelectExprItem::UnaryAggregate(iast::UnaryAggregate {
        distinct: func.distinct,
        op,
        expr: convert_expr(expr.clone())?,
      });
      Ok(item)
    } else {
      Ok(iast::SelectExprItem::ValExpr(convert_expr(expr)?))
    }
  }

  let mut select_list = Vec::<iast::SelectItem>::new();
  for item in select_clause {
    select_list.push(match item.clone() {
      ast::SelectItem::UnnamedExpr(expr) => {
        iast::SelectItem::ExprWithAlias { item: select_item(expr)?, alias: None }
      }
      ast::SelectItem::ExprWithAlias { expr, alias } => {
        iast::SelectItem::ExprWithAlias { item: select_item(expr)?, alias: Some(alias.value) }
      }
      ast::SelectItem::Wildcard => iast::SelectItem::Wildcard { table_name: None },
      ast::SelectItem::QualifiedWildcard(name) => {
        iast::SelectItem::Wildcard { table_name: Some(name.0.get(0).unwrap().value.clone()) }
      }
    });
  }

  Ok(select_list)
}

// -----------------------------------------------------------------------------------------------
//  Join
// -----------------------------------------------------------------------------------------------

fn convert_from(mut from: Vec<ast::TableWithJoins>) -> Result<iast::JoinNode, String> {
  if from.is_empty() {
    Err(format!("The 'from' clause {:?} must have at least one table.", from))
  } else {
    let mut from_iter = from.into_iter();
    let mut join_node = Some(convert_joins(from_iter.next().unwrap())?);

    // Recall that `join` is left-associative.
    for table_with_join in from_iter {
      let cur_join_node = join_node.take().unwrap();
      join_node = Some(iast::JoinNode::JoinInnerNode(iast::JoinInnerNode {
        left: Box::new(cur_join_node),
        right: Box::new(convert_joins(table_with_join)?),
        join_type: iast::JoinType::Inner,
        on: iast::ValExpr::Value { val: iast::Value::Boolean(true) },
      }));
    }

    Ok(join_node.unwrap())
  }
}

fn convert_joins(table_with_join: ast::TableWithJoins) -> Result<iast::JoinNode, String> {
  let mut join_node = Some(convert_table_factor(table_with_join.relation)?);

  // Recall that `join` is left-associative.
  for join in table_with_join.joins {
    let cur_join_node = join_node.take().unwrap();
    let (join_type, on) = convert_join_op(join.join_operator)?;
    join_node = Some(iast::JoinNode::JoinInnerNode(iast::JoinInnerNode {
      left: Box::new(cur_join_node),
      right: Box::new(convert_table_factor(join.relation)?),
      join_type,
      on,
    }));
  }

  Ok(join_node.unwrap())
}

fn convert_join_op(join_op: ast::JoinOperator) -> Result<(iast::JoinType, iast::ValExpr), String> {
  match join_op {
    ast::JoinOperator::Inner(join_constraint) => {
      Ok((iast::JoinType::Inner, convert_join_constraint(join_constraint)?))
    }
    ast::JoinOperator::LeftOuter(join_constraint) => {
      Ok((iast::JoinType::Left, convert_join_constraint(join_constraint)?))
    }
    ast::JoinOperator::RightOuter(join_constraint) => {
      Ok((iast::JoinType::Right, convert_join_constraint(join_constraint)?))
    }
    ast::JoinOperator::FullOuter(join_constraint) => {
      Ok((iast::JoinType::Outer, convert_join_constraint(join_constraint)?))
    }
    ast::JoinOperator::CrossJoin => {
      Ok((iast::JoinType::Outer, iast::ValExpr::Value { val: iast::Value::Boolean(true) }))
    }
    join_op => Err(format!("Unsupported join type: {:?}", join_op)),
  }
}

fn convert_join_constraint(join_constraint: ast::JoinConstraint) -> Result<iast::ValExpr, String> {
  match join_constraint {
    ast::JoinConstraint::On(on) => Ok(convert_expr(on)?),
    ast::JoinConstraint::None => Ok(iast::ValExpr::Value { val: iast::Value::Boolean(true) }),
    join_constraint => Err(format!("Unsupported join constraint: {:?}", join_constraint)),
  }
}

fn convert_table_factor(factor: ast::TableFactor) -> Result<iast::JoinNode, String> {
  match factor {
    ast::TableFactor::Table { name, alias, .. } => {
      let ast::ObjectName(idents) = name;
      Ok(iast::JoinNode::JoinLeaf(iast::JoinLeaf {
        alias: alias.map(|table_alias| table_alias.name.value),
        source: iast::JoinNodeSource::Table(get_table_name(idents)?),
      }))
    }
    ast::TableFactor::Derived { lateral, subquery, alias } => {
      Ok(iast::JoinNode::JoinLeaf(iast::JoinLeaf {
        alias: alias.map(|table_alias| table_alias.name.value),
        source: iast::JoinNodeSource::DerivedTable {
          query: Box::new(convert_query(*subquery)?),
          lateral,
        },
      }))
    }
    ast::TableFactor::TableFunction { .. } => Err(format!("Table functions are not supported.")),
    ast::TableFactor::NestedJoin(table_with_joins) => convert_joins(*table_with_joins),
  }
}

// -----------------------------------------------------------------------------------------------
//  Insert
// -----------------------------------------------------------------------------------------------

fn convert_insert(
  table_name: ast::ObjectName,
  columns: Vec<ast::Ident>,
  source: Box<ast::Query>,
) -> Result<iast::QueryBody, String> {
  if let ast::SetExpr::Values(values) = source.body {
    // Construct values
    let mut i_values = Vec::<Vec<iast::ValExpr>>::new();
    for row in values.0 {
      let mut i_row = Vec::<iast::ValExpr>::new();
      for elem in row {
        i_row.push(convert_expr(elem)?);
      }
      i_values.push(i_row);
    }
    // Construct Table name
    let i_table = get_table_ref(table_name.0, None)?;
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

// -----------------------------------------------------------------------------------------------
//  Update
// -----------------------------------------------------------------------------------------------

fn convert_update(
  table_name: ast::ObjectName,
  assignments: Vec<ast::Assignment>,
  selection: Option<ast::Expr>,
) -> Result<iast::QueryBody, String> {
  Ok(iast::QueryBody::Update(iast::Update {
    table: get_table_ref(table_name.0, None)?,
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

// -----------------------------------------------------------------------------------------------
//  Delete
// -----------------------------------------------------------------------------------------------

fn convert_delete(
  table_name: ast::ObjectName,
  selection: Option<ast::Expr>,
) -> Result<iast::QueryBody, String> {
  Ok(iast::QueryBody::Delete(iast::Delete {
    table: get_table_ref(table_name.0, None)?,
    selection: if let Some(selection) = selection {
      convert_expr(selection)?
    } else {
      iast::ValExpr::Value { val: iast::Value::Boolean(true) }
    },
  }))
}

// -----------------------------------------------------------------------------------------------
//  Expression
// -----------------------------------------------------------------------------------------------

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

pub fn convert_expr(expr: ast::Expr) -> Result<iast::ValExpr, String> {
  Ok(match expr {
    ast::Expr::Identifier(ident) => get_column_ref(vec![ident])?,
    ast::Expr::CompoundIdentifier(idents) => get_column_ref(idents)?,
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
      iast::ValExpr::Subquery { query: Box::new(convert_query(*query)?), trans_table_name: None }
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
    ast::Statement::CreateTable { name, columns, constraints, .. } => {
      // Every table needs at most one PRIMARY KEY declaration, either for a single column in its
      // Column Definition, or as a Table Constraint with potentially multiple columns. We track
      // whether we already encoutered such a declaration for error detection.
      let mut key_col_delcared = false;

      // Read in all columns, marking whether it is primary or not.
      let mut all_cols = BTreeMap::<ColName, (ColType, bool)>::new();
      for col in columns {
        // Read the name and type
        let col_name = ColName(col.name.value.clone());
        let col_type = convert_data_type(&col.data_type)?;
        // Read whether this is declared as a PRIMARY KEY or not
        let mut is_key_col = false;
        for option_def in &col.options {
          match &option_def.option {
            ast::ColumnOption::Unique { is_primary, .. } => {
              if *is_primary {
                is_key_col = true;
              }
            }
            _ => return Err(format!("Unsupported column option {:?}.", option_def)),
          }
        }
        if is_key_col {
          // Check whether a PRIMARY KEY declaration had already occurred.
          if key_col_delcared {
            return Err(format!("Cannot have multiple PRIMARY KEY declarations in a table."));
          } else {
            key_col_delcared = true;
          }
        }
        all_cols.insert(col_name, (col_type, is_key_col));
      }

      // Next, process the Table Constraints, handing PRIMARY KEY constraints.
      for constraint in constraints {
        match constraint {
          ast::TableConstraint::Unique { columns, is_primary, .. } => {
            if *is_primary {
              if key_col_delcared {
                return Err(format!("Cannot have multiple PRIMARY KEY declarations in a table."));
              } else {
                key_col_delcared = true;
                for col in columns {
                  let col_name = ColName(col.value.clone());
                  if let Some((_, primary)) = all_cols.get_mut(&col_name) {
                    if *primary {
                      // This can only occur if this Table Constraint itself has a key repeated
                      return Err(format!(
                        "Cannot have multiple PRIMARY KEY declarations in a table."
                      ));
                    } else {
                      *primary = true;
                    }
                  } else {
                    return Err(format!(
                      "Column {:?} in Table Constraint {:?} does not exist",
                      col_name, constraint
                    ));
                  }
                }
              }
            } else {
              return Err(format!("UNIQUE constraints not supported."));
            }
          }
          _ => {
            return Err(format!("Unsupported table constraint in CREATE TABLE: {:?}.", constraint))
          }
        }
      }

      // Construct and return the DDLQuery
      let table_path = TablePath(get_table_name(name.0.clone())?);
      let mut key_cols = Vec::<(ColName, ColType)>::new();
      let mut val_cols = Vec::<(ColName, ColType)>::new();
      for (col_name, (col_type, primary)) in all_cols {
        if primary {
          key_cols.push((col_name, col_type));
        } else {
          val_cols.push((col_name, col_type));
        }
      }
      Ok(DDLQuery::Create(proc::CreateTable { table_path, key_cols, val_cols }))
    }
    ast::Statement::Drop { names, .. } => {
      let name = names.into_iter().next().unwrap();
      let table_path = TablePath(get_table_name(name.0.clone())?);
      Ok(DDLQuery::Drop(proc::DropTable { table_path }))
    }
    ast::Statement::AlterTable { name, operation } => match operation {
      ast::AlterTableOperation::AddColumn { column_def } => Ok(DDLQuery::Alter(proc::AlterTable {
        table_path: TablePath(get_table_name(name.0.clone())?),
        alter_op: proc::AlterOp {
          col_name: ColName(column_def.name.value.clone()),
          maybe_col_type: Some(convert_data_type(&column_def.data_type)?),
        },
      })),
      ast::AlterTableOperation::DropColumn { column_name, .. } => {
        Ok(DDLQuery::Alter(proc::AlterTable {
          table_path: TablePath(get_table_name(name.0.clone())?),
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
    ast::DataType::Varchar(_) => Ok(ColType::String),
    _ => Err(format!("Unsupported ast::DataType {:?}", raw_data_type)),
  }
}

/// Computes whether this SQL Query is a DDL query by attempting to parse it as such.
pub fn is_ddl(query: &str) -> bool {
  match Parser::parse_sql(&GenericDialect {}, &query) {
    Ok(parsed_ast) => convert_ddl_ast(parsed_ast).is_ok(),
    Err(_) => false,
  }
}
