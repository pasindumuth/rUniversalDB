use crate::simulation::Simulation;
use runiversal::common::utils::mk_tid;
use runiversal::model::common::{
  ColumnName, ColumnValue, EndpointId, PrimaryKey, RequestId, Schema, SelectView, Timestamp,
};
use runiversal::model::message::{
  AdminMessage, AdminRequest, AdminResponse, NetworkMessage, SlaveMessage,
};
use runiversal::model::sqlast::{SqlStmt, ValExpr};
use runiversal::sql::parser::parse_sql;
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;

pub fn add_req_res(
  sim: &mut Simulation,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
  req: AdminRequest,
  expected_res: AdminResponse,
  expected_res_map: &mut HashMap<RequestId, NetworkMessage>,
  rid: RequestId,
) {
  sim.add_msg(
    NetworkMessage::Slave(SlaveMessage::AdminRequest { req }),
    &from_eid,
    &to_eid,
  );
  expected_res_map.insert(
    rid,
    NetworkMessage::Admin(AdminMessage::AdminResponse { res: expected_res }),
  );
}

/// This function simulates a sequential request-response session
/// between a client `from_eid` to a singel server `to_eid`. Here,
/// `req_res` contains pairs of requests and responses. For every pair,
/// we send the request, simulate everything until we get a response,
/// and then verify the response is what we expected.
pub fn exec_seq_session(
  sim: &mut Simulation,
  schema: Schema,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
  req_res: Vec<(&str, Timestamp, Result<SelectView, String>)>,
) -> Result<(), String> {
  for (query, timestamp, expected_result) in req_res {
    let mut expected_res_map = HashMap::new();
    let rid = sim.mk_request_id();
    let tid = mk_tid(&mut sim.rng);
    // Importantly, we run a preprocessing step that fully qualifies all
    // columns, where the table used is the nearest ancestor possible.
    // The Slaves must eventually do this.
    let sql = qualify_closest_scope(&schema, parse_sql(&query.to_string()).unwrap());
    add_req_res(
      sim,
      &from_eid,
      &to_eid,
      AdminRequest::SqlQuery {
        rid: rid.clone(),
        tid,
        sql,
        timestamp,
      },
      AdminResponse::SqlQuery {
        rid: rid.clone(),
        result: expected_result,
      },
      &mut expected_res_map,
      rid,
    );
    sim.simulate_all();
    check_expected_res(sim, &expected_res_map)?;
  }
  Ok(())
}

/// Checks to see if the messages in expected_res_map were
/// actually sent out by the Simulation.
pub fn check_expected_res(
  sim: &Simulation,
  expected_res_map: &HashMap<RequestId, NetworkMessage>,
) -> Result<(), String> {
  let mut res_map = HashMap::new();
  for (_, msgs) in sim.get_responses() {
    for msg in msgs {
      match msg {
        NetworkMessage::Admin(AdminMessage::AdminResponse { res }) => {
          let rid = match res {
            AdminResponse::Insert { rid, .. } => rid,
            AdminResponse::Read { rid, .. } => rid,
            AdminResponse::SqlQuery { rid, .. } => rid,
          };
          res_map.insert(rid.clone(), msg.clone());
        }
        _ => {}
      }
    }
  }
  for (request_id, expected_res) in expected_res_map.iter() {
    if let Some(res) = res_map.get(request_id) {
      if res != expected_res {
        return Err(format!(
          "True Response {:#?} does not match expected Response {:#?}",
          res, expected_res
        ));
      }
    } else {
      return Err(format!(
        "Response for RequestId {:#?} doesn't exist",
        request_id
      ));
    }
  }
  Ok(())
}

/// Highly convenient function for creating ColumnValue::String values.
pub fn cvs(s: &str) -> ColumnValue {
  ColumnValue::String(s.to_string())
}

/// Highly convenient function for creating ColumnValue::Int values.
pub fn cvi(i: i32) -> ColumnValue {
  ColumnValue::Int(i)
}

/// Highly convenient function for creating ColumnNames.
pub fn cn(s: &str) -> ColumnName {
  ColumnName(s.to_string())
}

/// This function expands out our View making DSL into a `SelectView`,
/// which is what all Sql queries return.
pub fn make_view(
  col_names: (Vec<ColumnName>, Vec<ColumnName>),
  col_vals: Vec<Vec<ColumnValue>>,
) -> SelectView {
  let mut res = BTreeMap::<PrimaryKey, Vec<(ColumnName, Option<ColumnValue>)>>::new();
  for row in &col_vals {
    assert_eq!(col_names.0.len() + col_names.1.len(), row.len());
    let mut key = PrimaryKey { cols: vec![] };
    let mut col_name_vals = Vec::new();
    for i in 0..col_names.0.len() {
      key.cols.push(row[i].clone());
      col_name_vals.push((col_names.0[i].clone(), Some(row[i].clone())))
    }
    for i in 0..col_names.1.len() {
      col_name_vals.push((
        col_names.1[i].clone(),
        Some(row[col_names.0.len() + i].clone()),
      ))
    }
    res.insert(key, col_name_vals);
  }
  res
}

/// This function qualifies all `Column` nodes in the SqlStmt AST with the nearest
/// ancestral table whose schema contains the column name.
///
/// Note that this function is fairly trivial right now since we have all tables
/// sharing the same `schema`.
pub fn qualify_closest_scope(schema: &Schema, mut sql: SqlStmt) -> SqlStmt {
  // Adds the columns in the given table to the context.
  fn expand_context(context: &mut BTreeMap<String, Vec<String>>, schema: &Schema, table: &String) {
    for (_, col_name) in schema.key_cols.iter().chain(schema.val_cols.iter()) {
      if let Some(tables) = context.get_mut(&col_name.0) {
        tables.push(table.clone());
      } else {
        context.insert(col_name.0.clone(), vec![table.clone()]);
      }
    }
  }
  // Removes the columns in the table from the context.
  fn remove_context(context: &mut BTreeMap<String, Vec<String>>, schema: &Schema) {
    for (_, col_name) in schema.key_cols.iter().chain(schema.val_cols.iter()) {
      context.get_mut(&col_name.0).unwrap().pop().unwrap();
    }
  }
  // Here, `context` maps a set of column names to the tables that have
  // a column of that name, where the last table is the closest.
  fn qualify_expr(
    context: &mut BTreeMap<String, Vec<String>>,
    expr: &mut ValExpr,
    schema: &Schema,
  ) {
    match expr {
      ValExpr::BinaryExpr { lhs, rhs, .. } => {
        qualify_expr(context, lhs, schema);
        qualify_expr(context, rhs, schema);
      }
      ValExpr::Subquery(subquery) => {
        expand_context(context, schema, &subquery.table_name);
        qualify_expr(context, &mut subquery.where_clause, schema);
        remove_context(context, schema);
      }
      ValExpr::Column(qual_col) => {
        if qual_col.qualifier.is_none() {
          if let Some(tables) = context.get(&qual_col.col_name) {
            if let Some(last) = tables.last() {
              qual_col.qualifier = Some(last.clone());
            }
          }
        }
      }
      _ => {}
    }
  }
  match &mut sql {
    SqlStmt::Insert(insert_stmt) => {
      let mut context = BTreeMap::new();
      expand_context(&mut context, schema, &insert_stmt.table_name);
      for row_exprs in &mut insert_stmt.insert_vals {
        for expr in row_exprs {
          qualify_expr(&mut context, expr, schema);
        }
      }
    }
    SqlStmt::Update(update_stmt) => {
      let mut context = BTreeMap::new();
      expand_context(&mut context, schema, &update_stmt.table_name);
      qualify_expr(&mut context, &mut update_stmt.set_val, schema);
      qualify_expr(&mut context, &mut update_stmt.where_clause, schema);
    }
    SqlStmt::Select(select_stmt) => {
      let mut context = BTreeMap::new();
      expand_context(&mut context, schema, &select_stmt.table_name);
      qualify_expr(&mut context, &mut select_stmt.where_clause, schema);
    }
  }
  return sql;
}

#[cfg(test)]
mod tests {
  use crate::test_utils::{cn, cvi, cvs, make_view, qualify_closest_scope};
  use runiversal::model::common::{ColumnName, ColumnType, ColumnValue, PrimaryKey, Schema};
  use runiversal::sql::parser::parse_sql;
  use std::collections::BTreeMap;
  use std::iter::FromIterator;

  fn schema() -> Schema {
    Schema {
      key_cols: vec![(ColumnType::String, ColumnName(String::from("key")))],
      val_cols: vec![(ColumnType::Int, ColumnName(String::from("value")))],
    }
  }

  /// Tests if performing qualification on UPDATE
  /// statements work properly.
  #[test]
  fn test_qualify_closest_scope_update() {
    assert_eq!(
      parse_sql(
        &r#"
          UPDATE table3
          SET value = table3.value + 1
          WHERE table3.value = (
            SELECT value
            FROM table2
            WHERE table2.key = "hi" AND table1.value = 3
          )
        "#
        .to_string(),
      )
      .unwrap(),
      qualify_closest_scope(
        &schema(),
        parse_sql(
          &r#"
            UPDATE table3
            SET value = value + 1
            WHERE value = (
              SELECT value
              FROM table2
              WHERE key = "hi" AND table1.value = 3
            )
          "#
          .to_string(),
        )
        .unwrap()
      )
    );
  }

  /// Tests if performing qualification on INSERT
  /// statements work properly.
  #[test]
  fn test_qualify_closest_scope_insert() {
    assert_eq!(
      parse_sql(
        &r#"
          INSERT INTO table2 (key, value)
          VALUES (
            (
              SELECT key
              FROM table1
              WHERE table1.value = 2
            ),
            (
              SELECT value
              FROM table1
              WHERE table1.key = "hi"
            ) + 1
          )
        "#
        .to_string(),
      )
      .unwrap(),
      qualify_closest_scope(
        &schema(),
        parse_sql(
          &r#"
            INSERT INTO table2 (key, value)
            VALUES (
              (
                SELECT key
                FROM table1
                WHERE value = 2
              ),
              (
                SELECT value
                FROM table1
                WHERE key = "hi"
              ) + 1
            )
          "#
          .to_string(),
        )
        .unwrap()
      )
    );
  }

  /// Tests if performing qualification on SELECT
  /// statements work properly.
  #[test]
  fn test_qualify_closest_scope_select() {
    assert_eq!(
      parse_sql(
        &r#"
          SELECT key
          FROM table1
          WHERE table1.value = 2
        "#
        .to_string(),
      )
      .unwrap(),
      qualify_closest_scope(
        &schema(),
        parse_sql(
          &r#"
            SELECT key
            FROM table1
            WHERE value = 2
          "#
          .to_string(),
        )
        .unwrap()
      )
    );
  }

  #[test]
  fn test_make_view() {
    assert_eq!(
      BTreeMap::from_iter(
        vec![
          (
            PrimaryKey {
              cols: vec![ColumnValue::String("aello".to_string())],
            },
            vec![
              (ColumnName("key".to_string()), Some(cvs("aello"))),
              (ColumnName("value".to_string()), Some(cvi(1))),
            ],
          ),
          (
            PrimaryKey {
              cols: vec![ColumnValue::String("hello".to_string())],
            },
            vec![
              (ColumnName("key".to_string()), Some(cvs("hello"))),
              (ColumnName("value".to_string()), Some(cvi(5))),
            ],
          ),
          (
            PrimaryKey {
              cols: vec![ColumnValue::String("kello".to_string())],
            },
            vec![
              (ColumnName("key".to_string()), Some(cvs("kello"))),
              (ColumnName("value".to_string()), Some(cvi(3))),
            ],
          ),
          (
            PrimaryKey {
              cols: vec![ColumnValue::String("rello".to_string())],
            },
            vec![
              (ColumnName("key".to_string()), Some(cvs("rello"))),
              (ColumnName("value".to_string()), Some(cvi(5))),
            ],
          ),
        ]
        .into_iter(),
      ),
      make_view(
        (vec![cn("key")], vec![(cn("value"))]),
        vec![
          vec![cvs("aello"), cvi(1)],
          vec![cvs("hello"), cvi(5)],
          vec![cvs("kello"), cvi(3)],
          vec![cvs("rello"), cvi(5)]
        ]
      )
    );
  }
}
