use crate::common::{TablePath, TransTableName};
use crate::message as msg;
use crate::query_converter::{flatten_top_level_query, rename_trans_tables_query_r, RenameContext};
use crate::sql_ast::{iast, proc};

// -----------------------------------------------------------------------------------------------
//  Common
// -----------------------------------------------------------------------------------------------

fn basic_select(table_ref: &str) -> iast::SuperSimpleSelect {
  iast::SuperSimpleSelect {
    distinct: false,
    projection: iast::SelectClause::SelectList(vec![]),
    from: iast::TableRef { source_ref: table_ref.to_string(), alias: None },
    selection: iast::ValExpr::Value { val: iast::Value::Boolean(true) },
  }
}

fn basic_select_query(ctes: Vec<(&str, iast::Query)>, table_ref: &str) -> iast::Query {
  iast::Query {
    ctes: ctes.iter().map(|(name, query)| (name.to_string(), query.clone())).collect(),
    body: iast::QueryBody::SuperSimpleSelect(basic_select(table_ref)),
  }
}

// -----------------------------------------------------------------------------------------------
//  Renaming
// -----------------------------------------------------------------------------------------------

// This test simply checks that TransTables that are shadowed in the
// original Query are still renamed properly, where references of that
// TransTable are also renamed to properly.
#[test]
fn test_basic_rename() {
  let mut in_query = basic_select_query(
    vec![
      ("tt1", basic_select_query(vec![], "t2")),
      ("tt2", basic_select_query(vec![("tt1", basic_select_query(vec![], "tt1"))], "tt1")),
    ],
    "tt2",
  );

  // Rename TransTables
  let mut ctx = RenameContext {
    trans_table_map: Default::default(),
    counter: 0,
    table_names: Default::default(),
  };
  rename_trans_tables_query_r(&mut ctx, &mut in_query);

  let expected = iast::Query {
    ctes: vec![
      (
        "tt\\0\\tt1".to_string(),
        iast::Query {
          ctes: vec![],
          body: iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
            distinct: false,
            projection: iast::SelectClause::SelectList(vec![]),
            from: iast::TableRef { source_ref: "t2".to_string(), alias: None },
            selection: iast::ValExpr::Value { val: iast::Value::Boolean(true) },
          }),
        },
      ),
      (
        "tt\\2\\tt2".to_string(),
        iast::Query {
          ctes: vec![(
            "tt\\1\\tt1".to_string(),
            iast::Query {
              ctes: vec![],
              body: iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
                distinct: false,
                projection: iast::SelectClause::SelectList(vec![]),
                from: iast::TableRef {
                  source_ref: "tt\\0\\tt1".to_string(),
                  alias: Some("tt1".to_string()),
                },
                selection: iast::ValExpr::Value { val: iast::Value::Boolean(true) },
              }),
            },
          )],
          body: iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
            distinct: false,
            projection: iast::SelectClause::SelectList(vec![]),
            from: iast::TableRef {
              source_ref: "tt\\1\\tt1".to_string(),
              alias: Some("tt1".to_string()),
            },
            selection: iast::ValExpr::Value { val: iast::Value::Boolean(true) },
          }),
        },
      ),
    ],
    body: iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
      distinct: false,
      projection: iast::SelectClause::SelectList(vec![]),
      from: iast::TableRef { source_ref: "tt\\2\\tt2".to_string(), alias: Some("tt2".to_string()) },
      selection: iast::ValExpr::Value { val: iast::Value::Boolean(true) },
    }),
  };

  // Verify the result.
  assert_eq!(in_query, expected);
}

// -----------------------------------------------------------------------------------------------
//  Flattening
// -----------------------------------------------------------------------------------------------

// This tests for a basic flattening of the Query.
#[test]
fn test_basic_flatten() {
  let query = basic_select_query(
    vec![
      ("tt\\0\\tt1", basic_select_query(vec![], "t2")),
      (
        "tt\\2\\tt2",
        basic_select_query(
          vec![("tt\\1\\tt1", basic_select_query(vec![], "tt\\0\\tt1"))],
          "tt\\1\\tt1",
        ),
      ),
    ],
    "tt\\2\\tt2",
  );

  let expected: Result<proc::MSQuery, msg::ExternalAbortedData> = Ok(proc::MSQuery {
    trans_tables: vec![
      (
        TransTableName("tt\\0\\tt1".to_string()),
        proc::MSQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
          distinct: false,
          projection: proc::SelectClause::SelectList(vec![]),
          from: proc::GeneralSource {
            source_ref: proc::GeneralSourceRef::TablePath(TablePath("t2".to_string())),
            alias: None,
          },
          selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
        }),
      ),
      (
        TransTableName("tt\\1\\tt1".to_string()),
        proc::MSQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
          distinct: false,
          projection: proc::SelectClause::SelectList(vec![]),
          from: proc::GeneralSource {
            source_ref: proc::GeneralSourceRef::TransTableName(TransTableName(
              "tt\\0\\tt1".to_string(),
            )),
            alias: None,
          },
          selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
        }),
      ),
      (
        TransTableName("tt\\2\\tt2".to_string()),
        proc::MSQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
          distinct: false,
          projection: proc::SelectClause::SelectList(vec![]),
          from: proc::GeneralSource {
            source_ref: proc::GeneralSourceRef::TransTableName(TransTableName(
              "tt\\1\\tt1".to_string(),
            )),
            alias: None,
          },
          selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
        }),
      ),
      (
        TransTableName("tt\\3\\".to_string()),
        proc::MSQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
          distinct: false,
          projection: proc::SelectClause::SelectList(vec![]),
          from: proc::GeneralSource {
            source_ref: proc::GeneralSourceRef::TransTableName(TransTableName(
              "tt\\2\\tt2".to_string(),
            )),
            alias: None,
          },
          selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
        }),
      ),
    ]
    .into_iter()
    .collect(),
    returning: TransTableName("tt\\3\\".to_string()),
  });
  assert_eq!(flatten_top_level_query(&query, &mut 3), expected);
}
