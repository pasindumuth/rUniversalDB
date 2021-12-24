use crate::serial_test_utils::{setup, TestContext};
use crate::simulation::Simulation;
use rand::seq::SliceRandom;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{mk_rid, mk_t, read_index, TableSchema, Timestamp};
use runiversal::master::FullDBSchema;
use runiversal::model::common::{
  ColName, EndpointId, Gen, RequestId, SlaveGroupId, TablePath, TableView,
};
use runiversal::model::message as msg;
use runiversal::model::message::ExternalAbortedData;
use runiversal::paxos::PaxosConfig;
use runiversal::simulation_utils::mk_slave_eid;
use runiversal::test_utils::{cno, cvi, cvs, mk_eid, mk_seed, mk_sid};
use sqlformat;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  QueryGenCtx
// -----------------------------------------------------------------------------------------------

struct QueryGenCtx<'a> {
  rand: &'a mut XorShiftRng,
  timestamp: Timestamp,
  table_schemas: &'a BTreeMap<TablePath, &'a TableSchema>,
  trans_table_counter: &'a mut u32,
  trans_table_schemas: &'a mut BTreeMap<String, Vec<Option<ColName>>>,
  column_context: &'a mut BTreeMap<(String, String), u32>,
}

impl<'a> QueryGenCtx<'a> {
  /// Construct an INSERT query.
  fn mk_insert(&mut self) -> Option<String> {
    // Choose a Table to insert to
    let num_sources = self.table_schemas.len();
    if num_sources == 0 {
      return None;
    }
    let mut source_idx = self.rand.next_u32() as usize % num_sources;
    let (source, key_cols, mut val_cols): (String, Vec<ColName>, Vec<Option<ColName>>) = {
      let (source, schema) = read_index(self.table_schemas, source_idx).unwrap();
      let key_cols: Vec<ColName> = schema.key_cols.iter().map(|(c, _)| c).cloned().collect();
      let val_col_map = schema.val_cols.static_snapshot_read(&self.timestamp);
      let val_cols: Vec<Option<ColName>> = val_col_map.keys().map(|c| Some(c.clone())).collect();
      (source.0.clone(), key_cols, val_cols)
    };

    // Take some non-empty prefix of all val_cols to insert to
    let mut present_val_cols = Vec::<ColName>::new();
    for col in val_cols {
      if let Some(col) = col {
        present_val_cols.push(col);
      }
    }
    present_val_cols[..].shuffle(self.rand);
    if present_val_cols.is_empty() {
      return None;
    }
    let num_chosen_cols = (self.rand.next_u32() as usize % present_val_cols.len()) + 1;
    let chosen_cols: Vec<ColName> = present_val_cols.into_iter().take(num_chosen_cols).collect();

    // Construct the insert Schema
    let mut insert_cols = Vec::<String>::new();
    for key_col in &key_cols {
      insert_cols.push(key_col.0.clone());
    }
    for val_col in &chosen_cols {
      insert_cols.push(val_col.0.clone());
    }

    // Construct the Insert Rows
    let num_rows = (self.rand.next_u32() % 10) + 1; // We only insert at most 10 rows at a time
    let row_len = key_cols.len() + chosen_cols.len();
    let mut rows = Vec::<String>::new();
    for _ in 0..num_rows {
      let mut values = Vec::<String>::new();
      for _ in 0..row_len {
        values.push(format!("{}", self.rand.next_u32() % 100));
      }
      rows.push(format!("({})", values.join(", ")));
    }

    Some(format!(
      " INSERT INTO {} ({})
        VALUES {}
      ",
      source,
      insert_cols.join(", "),
      rows.join(",\n")
    ))
  }

  /// Util for amending columns from `column_context`.
  fn add_to_column_context(&mut self, all_cols: &Vec<ColName>, source_name: &String) {
    for col in all_cols {
      let col_ref = (source_name.clone(), col.0.clone());
      if !self.column_context.contains_key(&col_ref) {
        self.column_context.insert(col_ref.clone(), 0);
      }
      *self.column_context.get_mut(&col_ref).unwrap() += 1;
    }
  }

  /// Util for removing columns from `column_context`.
  fn remove_from_column_context(&mut self, all_cols: &Vec<ColName>, source_name: &String) {
    for col in all_cols {
      let col_ref = (source_name.clone(), col.0.clone());
      let count = self.column_context.get_mut(&col_ref).unwrap();
      *count -= 1;
      if *count == 0 {
        self.column_context.remove(&col_ref);
      }
    }
  }

  /// Create a ValExpr that evaluate to a singe Int value. We do not use Binary operations
  /// here for simplicity. Here, `depth` is the depth that this expression would be used at.
  fn mk_single_val_expr(
    &mut self,
    depth: u32,
    source: &String,
    alias_opt: &Option<String>,
    all_cols: &Vec<ColName>,
  ) -> Option<String> {
    // We randomly choose what type of expression this should be.
    let val_type = self.rand.next_u32() % 3;
    let val = if val_type == 0 {
      // Use a single literal value.
      format!("{}", self.rand.next_u32() % 100)
    } else if val_type == 1 || self.column_context.is_empty() {
      // Use a subquery.
      let source_name = if let Some(alias) = &alias_opt { alias } else { &source };
      self.add_to_column_context(&all_cols, source_name);
      let (_, cte) = self.mk_cte_query(depth + 1, true)?;
      self.remove_from_column_context(&all_cols, source_name);
      format!("({})", cte)
    } else {
      // Use an external column. From the else-if, we see column_context is non-empty.
      let idx = self.rand.next_u32() as usize % self.column_context.len();
      let (alias, col) = read_index(self.column_context, idx).unwrap().0.clone();
      // Potentially qualify the column before using it. Note that if we do not qualify,
      // it might get shadowed by a column in this Data Source.
      if self.rand.next_u32() % 2 == 0 {
        format!("{}.{}", alias, col)
      } else {
        format!("{}", col)
      }
    };
    Some(val)
  }

  /// Construct a WHERE clause boolean expression by using comparison operators on the `key_cols`
  /// and `val_cols`. These are compared against literal values, subqueries, external ColumnRefs.
  fn mk_where_clause(
    &mut self,
    depth: u32,
    source: &String,
    alias_opt: &Option<String>,
    key_cols: &Vec<ColName>,
    val_cols: &Vec<Option<ColName>>,
    all_cols: &Vec<ColName>,
  ) -> Option<String> {
    let mut bound_exprs = Vec::<String>::new();

    let mut cols_to_bound = Vec::<&String>::new();
    // Amend bound_exprs for some prefix of key_cols. We use a prefix so that the KeyBound
    // used by TP is non-trivial.
    for i in 0..(self.rand.next_u32() as usize % (key_cols.len() + 1)) {
      cols_to_bound.push(&key_cols.get(i).unwrap().0);
    }

    // Amend bound_exprs with some val_cols.
    for i in 0..(self.rand.next_u32() as usize % (val_cols.len() + 1)) {
      if let Some(val_col) = val_cols.get(i).unwrap() {
        cols_to_bound.push(&val_col.0);
      }
    }

    // Amend bound_exprs with comparison expressions for all `cols_to_bound`.
    for col in cols_to_bound {
      // We create multiple bound expressions for the col. We create at most 3.
      for _ in 0..(self.rand.next_u32() % 3) {
        // We randomly choose between the 6 comparison operators.
        let op = match self.rand.next_u32() % 6 {
          0 => "=",
          1 => "!=",
          2 => "<",
          3 => "<=",
          4 => ">=",
          5 => ">",
          _ => panic!(),
        };

        // We randomly choose what should appear on the other side.
        let compare_val = self.mk_single_val_expr(depth, source, alias_opt, all_cols)?;
        bound_exprs.push(format!("({} {} {})", col, op, compare_val))
      }
    }

    // Construct the WHERE clause
    let mut it = bound_exprs.into_iter();
    let where_clause = if let Some(expr) = it.next() {
      // Construct a boolean expression with a mixture of AND and OR
      let mut exprs_with_ops = Vec::<String>::new();
      exprs_with_ops.push(expr);
      while let Some(next_expr) = it.next() {
        if self.rand.next_u32() % 2 == 0 {
          exprs_with_ops.push("OR".to_string());
        } else {
          exprs_with_ops.push("AND".to_string());
        }
        exprs_with_ops.push(next_expr);
      }
      exprs_with_ops.join(" ")
    } else {
      "true".to_string()
    };

    Some(where_clause)
  }

  /// Construct a SELECT query.
  ///
  /// Here, `trans_table_schemas` are all TransTables that can be used as a Data Source in
  /// this query.
  ///
  /// Here, `column_context` are the column references that can be used by the current query
  /// from the ancestral data sources. The first `String` is the Data Source name, and the second
  /// is the column name. This is mapped to a count, which is incremented and decremented due to
  /// shadowing accordingly. Note that the count is never 0 (the key would be removed by then).
  ///
  /// Here, if `make_single_value` is `true`, then the returned query must have one element in
  /// the SELECT clause, and it is an aggregate.
  ///
  /// Post Conditions (only applies if Some(_) is returned, otherwise anything does):
  ///   1. The `trans_table_schemas` and `column_context` must remain unchanged.
  ///   2. Genrally, query that is returned must be type-correction and column references must
  ///      refer to an actual column in scope. Same with Data Source references.
  ///        a. All TransTables that are used appear in the returned query must either be defined
  ///           within the query at a suitable location, or be from `trans_table_schemas`.
  ///        b. All column references must refer to an ancestral Data Source used within the query,
  ///           or be from `column_context`.
  fn mk_select(
    &mut self,
    depth: u32,
    make_single_value: bool,
  ) -> Option<(Vec<Option<ColName>>, String)> {
    // Choose a random Data Source for the SELECT.
    let num_sources = self.trans_table_schemas.len() + self.table_schemas.len();
    if num_sources == 0 {
      return None;
    }
    let mut source_idx = self.rand.next_u32() as usize % num_sources;
    let (source, key_cols, val_cols): (String, Vec<ColName>, Vec<Option<ColName>>) =
      if let Some((source, schema)) = read_index(self.trans_table_schemas, source_idx) {
        (source.clone(), Vec::<ColName>::new(), schema.clone())
      } else {
        let (source, schema) =
          read_index(self.table_schemas, source_idx - self.trans_table_schemas.len()).unwrap();
        let key_cols: Vec<ColName> = schema.key_cols.iter().map(|(c, _)| c).cloned().collect();
        let val_col_map = schema.val_cols.static_snapshot_read(&self.timestamp);
        let val_cols: Vec<Option<ColName>> = val_col_map.keys().map(|c| Some(c.clone())).collect();
        (source.0.clone(), key_cols, val_cols)
      };

    // Potentially generate an alias to use as the Data Source name.
    let alias_opt = if self.rand.next_u32() % 2 == 0 {
      // Generate a random alias. We do this in a way that is not totally immune from collisions.
      Some(format!("a{}_{}", (self.rand.next_u32() % 10), source))
    } else {
      None
    };

    // Collect all columns in the table into one vector
    let mut all_cols = Vec::<ColName>::new();
    for col in &key_cols {
      all_cols.push(col.clone());
    }
    for col in &val_cols {
      if let Some(col) = col {
        all_cols.push(col.clone());
      }
    }

    // Construct the WHERE clause
    let where_clause =
      self.mk_where_clause(depth, &source, &alias_opt, &key_cols, &val_cols, &all_cols)?;

    // Construct the SELECT clause
    let mut schema = Vec::<Option<ColName>>::new();
    let mut select_clause = Vec::<String>::new();
    if make_single_value {
      // Choose a random column in the schema and aggregate it with sum, using its name as an alias.
      let col = all_cols.get(self.rand.next_u32() as usize % all_cols.len()).unwrap();
      select_clause.push(format!("sum({}) AS {}", &col.0, &col.0));
      schema.push(Some(col.clone()));
    } else {
      // Choose a random, non-empty set of columns as the elements in the SELECT clause
      let num_cols = (self.rand.next_u32() as usize % all_cols.len()) + 1;
      for _ in 0..num_cols {
        let col = all_cols.remove(self.rand.next_u32() as usize % all_cols.len());
        select_clause.push(format!("{}", &col.0));
        schema.push(Some(col.clone()));
      }
    }

    // Construct the FROM clause
    let from_clause = if let Some(alias) = &alias_opt {
      format!("{} AS {}", source, alias)
    } else {
      format!("{}", source)
    };

    let query = format!(
      "SELECT {}
       FROM {}
       WHERE {}",
      select_clause.join(", "),
      from_clause,
      where_clause
    );
    Some((schema, query))
  }

  /// Constructs a general CTE query.
  ///
  /// Most of the parameters here have the same description as `mk_select`. The Post
  /// Conditions are also the same.
  fn mk_cte_query(
    &mut self,
    depth: u32,
    make_single_value: bool,
  ) -> Option<(Vec<Option<ColName>>, String)> {
    // Generate CTEs. We reduce the number of CTEs for every level of depth to avoid
    // exponential blow-up of the query.
    const MAX_DEPTH: u32 = 3;
    let num_ctes = if depth >= MAX_DEPTH { 0 } else { self.rand.next_u32() % (MAX_DEPTH - depth) };

    // If there are CTEs to create, we create a query using WITH.
    if num_ctes > 0 {
      let mut new_cte_names = Vec::<String>::new(); // Used for cleaning up `trans_table_schemas`
      let mut new_ctes = Vec::<String>::new(); // The new CTEs to put into WITH
      for _ in 0..num_ctes {
        let new_cte_name = format!("tt{}", self.trans_table_counter);
        *self.trans_table_counter += 1;
        let (new_schema, new_cte) = self.mk_cte_query(depth + 1, false)?;
        new_ctes.push(format!("{} AS ({})", new_cte_name, new_cte));
        new_cte_names.push(new_cte_name.clone());
        self.trans_table_schemas.insert(new_cte_name, new_schema);
      }

      // Construct the SELECT with the amended TableSchema.
      let (schema, select) = self.mk_select(depth, make_single_value)?;

      let query = format!(
        "WITH
          {}
         {}
        ",
        new_ctes.join(",\n"),
        select
      );

      // Remove the newly added TransTables.
      for new_cte in new_cte_names {
        self.trans_table_schemas.remove(&new_cte);
      }

      Some((schema, query))
    } else {
      // Otherwise, we simply create a query using SELECT.
      self.mk_select(depth, make_single_value)
    }
  }

  /// Construct an UPDATE query.
  fn mk_update(&mut self, depth: u32) -> Option<String> {
    // Choose a Table to update
    let num_sources = self.table_schemas.len();
    if num_sources == 0 {
      return None;
    }
    let mut source_idx = self.rand.next_u32() as usize % num_sources;
    let (source, key_cols, mut val_cols): (String, Vec<ColName>, Vec<Option<ColName>>) = {
      let (source, schema) = read_index(self.table_schemas, source_idx).unwrap();
      let key_cols: Vec<ColName> = schema.key_cols.iter().map(|(c, _)| c).cloned().collect();
      let val_col_map = schema.val_cols.static_snapshot_read(&self.timestamp);
      let val_cols: Vec<Option<ColName>> = val_col_map.keys().map(|c| Some(c.clone())).collect();
      (source.0.clone(), key_cols, val_cols)
    };

    // Take some random, non-empty set of val_cols to use in the SET clause
    let mut present_val_cols = Vec::<ColName>::new();
    for col in val_cols.clone() {
      if let Some(col) = col {
        present_val_cols.push(col);
      }
    }
    present_val_cols[..].shuffle(self.rand);
    if present_val_cols.is_empty() {
      return None;
    }
    let num_chosen_cols = (self.rand.next_u32() as usize % present_val_cols.len()) + 1;
    let chosen_cols: Vec<ColName> = present_val_cols.into_iter().take(num_chosen_cols).collect();

    // Potentially generate an alias to use as the Data Source name.
    //
    // NOTE: although Postgres allows for aliases here, the `sqlparser` library does not. Thus,
    // we take this to be None every time.
    let alias_opt = None;

    // Collect all columns in the table into one vector
    let mut all_cols = Vec::<ColName>::new();
    for col in &key_cols {
      all_cols.push(col.clone());
    }
    for col in &val_cols {
      if let Some(col) = col {
        all_cols.push(col.clone());
      }
    }

    // Construct the SET clause
    let mut assignments = Vec::<String>::new();
    for col in chosen_cols {
      // Randomly generate a value to set to.
      let set_val = self.mk_single_val_expr(depth, &source, &alias_opt, &all_cols)?;
      assignments.push(format!("{} = {}", col.0, set_val));
    }

    // Construct the WHERE clause
    let where_clause =
      self.mk_where_clause(depth, &source, &alias_opt, &key_cols, &val_cols, &all_cols)?;

    // Construct the UPDATE clause
    let update_clause = if let Some(alias) = &alias_opt {
      format!("{} AS {}", source, alias)
    } else {
      format!("{}", source)
    };

    let query = format!(
      "UPDATE {}
       SET {}
       WHERE {}",
      update_clause,
      assignments.join(", "),
      where_clause
    );

    Some(query)
  }
}

// -----------------------------------------------------------------------------------------------
//  QueryGenerator
// -----------------------------------------------------------------------------------------------

struct QueryGenerator {
  table_counter: u32,
  column_counter: BTreeMap<String, u32>,
}

enum QueryType {
  /// Different variants of DDL queries
  CreateNewTable,
  RecreateTable,
  DropTable,
  CreateNewColumn,
  RecreateColumn,
  DeleteColumm,
  /// A DQL or DML query
  TPQuery,
}

impl QueryGenerator {
  fn new() -> QueryGenerator {
    QueryGenerator { table_counter: 0, column_counter: Default::default() }
  }

  /// Chooses a QueryType based on some target probability distribution.
  fn choose_query_type(_: &mut XorShiftRng) -> QueryType {
    // TODO implement
    QueryType::TPQuery
  }

  /// Create a TP Query.
  fn mk_tp_query(&mut self, sim: &mut Simulation) -> Option<String> {
    let timestamp = sim.true_timestamp().clone();

    // Create a new RNG for query generation
    let mut rand = XorShiftRng::from_seed(mk_seed(&mut sim.rand));

    // Extract all current TableSchemas
    let full_db_schema = sim.full_db_schema();
    let cur_tables = full_db_schema.table_generation.static_snapshot_read(&timestamp);
    let mut table_schemas = BTreeMap::<TablePath, &TableSchema>::new();
    for (table_path, gen) in cur_tables {
      let table_schema = full_db_schema.db_schema.get(&(table_path.clone(), gen)).unwrap();
      table_schemas.insert(table_path, table_schema);
    }

    // Construct a Query generation ctx
    let mut ctx = QueryGenCtx {
      rand: &mut rand,
      timestamp,
      table_schemas: &table_schemas,
      trans_table_counter: &mut 0,
      trans_table_schemas: &mut BTreeMap::<String, Vec<Option<ColName>>>::new(),
      column_context: &mut BTreeMap::<(String, String), u32>::new(),
    };

    // We add at most 10 stages to the query. We choose write queries for all stages
    // prior to the last.
    let mut stages = Vec::<String>::new();
    for _ in 0..(ctx.rand.next_u32() % 10) {
      let stage = match ctx.rand.next_u32() % 2 {
        0 => ctx.mk_insert()?,
        1 => ctx.mk_update(0)?,
        _ => panic!(),
      };
      stages.push(format!("{};", stage));
    }
    let stage = match ctx.rand.next_u32() % 2 {
      0 => ctx.mk_update(0)?,
      1 => ctx.mk_cte_query(0, false)?.1,
      _ => panic!(),
    };
    stages.push(format!("{};", stage));

    Some(stages.join("\n"))
  }

  fn mk_query(&mut self, sim: &mut Simulation) -> Option<String> {
    // Get an approximation of the latest `FullDBSchema`, which is sufficient
    // for generating a Query.
    match Self::choose_query_type(&mut sim.rand) {
      QueryType::CreateNewTable => {
        // Create a new Table
        let new_table_name = format!("table{}", self.table_counter);

        let num_key_cols = sim.rand.next_u32() % 5; // We only have at most 5 KeyCols
        let num_val_cols = sim.rand.next_u32() % 5; // We only have at most 5 ValCols initially

        let mut key_defs = Vec::<String>::new();
        for i in 0..num_key_cols {
          key_defs.push(format!("k{} INT PRIMARY KEY", i));
        }

        let mut val_defs = Vec::<String>::new();
        for i in 0..num_val_cols {
          val_defs.push(format!("v{} INT", i));
        }

        // Update metadata
        self.table_counter += 1;
        self.column_counter.insert(new_table_name.clone(), num_key_cols + num_val_cols);

        // Return the query
        Some(format!(
          " CREATE TABLE {} (
              {},
              {}
            );
          ",
          new_table_name,
          key_defs.join(",\n"),
          val_defs.join(",\n"),
        ))
      }
      QueryType::RecreateTable => {
        // Create a Table that had previously been dropped
        Some(String::new())
      }
      QueryType::DropTable => {
        // Drop a Table
        Some(String::new())
      }
      QueryType::CreateNewColumn => {
        // Create a new Column
        Some(String::new())
      }
      QueryType::RecreateColumn => {
        // Create a Column that had previously been dropped
        Some(String::new())
      }
      QueryType::DeleteColumm => {
        // Delete a Column
        Some(String::new())
      }
      QueryType::TPQuery => self.mk_tp_query(sim),
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

/// Replay the requests that succeeded in timestamp order serially, and very that
/// the results are the same.
fn verify_req_res(
  rand: &mut XorShiftRng,
  req_res_map: BTreeMap<RequestId, (msg::PerformExternalQuery, msg::ExternalMessage)>,
) -> Option<(Timestamp, u32, u32)> {
  let (mut sim, mut ctx) = setup(mk_seed(rand));
  let mut sorted_success_res =
    BTreeMap::<Timestamp, (msg::PerformExternalQuery, msg::ExternalQuerySuccess)>::new();
  let total_queries = req_res_map.len() as u32;
  for (_, (req, res)) in req_res_map {
    if let msg::ExternalMessage::ExternalQuerySuccess(success) = res {
      if !sorted_success_res.insert(success.timestamp.clone(), (req, success)).is_none() {
        // Here, two responses had the same timestamp. We cannot replay this, so we
        // simply skip this test.
        return None;
      }
    }
  }

  setup_tables(&mut sim, &mut ctx);
  sim.remove_all_responses(); // Clear the DDL response.

  let successful_queries = sorted_success_res.len() as u32;
  for (_, (req, res)) in sorted_success_res {
    ctx.execute_query(&mut sim, req.query.as_str(), 10000, res.result);
  }

  Some((sim.true_timestamp().clone(), total_queries, successful_queries))
}

// -----------------------------------------------------------------------------------------------
//  Print Utils
// -----------------------------------------------------------------------------------------------

/// Formats a raw, generated SQL string and formats it for printing.
fn format_sql(query: &str) -> String {
  sqlformat::format(
    &query,
    &sqlformat::QueryParams::None,
    sqlformat::FormatOptions {
      indent: sqlformat::Indent::Spaces(1),
      uppercase: false,
      lines_between_queries: 1,
    },
  )
}

// -----------------------------------------------------------------------------------------------
//  Setup Utils
// -----------------------------------------------------------------------------------------------

fn setup_tables(sim: &mut Simulation, ctx: &mut TestContext) {
  {
    ctx.send_ddl_query(
      sim,
      " CREATE TABLE table1 (
          k11 INT,
          k12 INT,
          v11 INT,
          v12 INT,
          v13 INT,
          PRIMARY KEY (k11, k12)
        );
      ",
      10000,
    );
  }

  {
    ctx.send_ddl_query(
      sim,
      " CREATE TABLE table2 (
          k21 INT PRIMARY KEY,
          v21 INT,
          v22 INT
        );
      ",
      10000,
    );
  }

  let mki = |i: i32| Some(cvi(i));

  {
    let mut exp_result =
      TableView::new(vec![cno("k11"), cno("k12"), cno("v11"), cno("v12"), cno("v13")]);
    exp_result.add_row(vec![mki(0), mki(1), mki(2), mki(3), mki(4)]);
    exp_result.add_row(vec![mki(1), mki(2), mki(3), mki(4), mki(5)]);
    exp_result.add_row(vec![mki(2), mki(3), mki(4), mki(5), mki(6)]);
    exp_result.add_row(vec![mki(3), mki(4), mki(5), mki(6), mki(7)]);
    exp_result.add_row(vec![mki(4), mki(5), mki(6), mki(7), mki(8)]);
    ctx.execute_query(
      sim,
      " INSERT INTO table1 (k11, k12, v11, v12, v13)
        VALUES (0, 1, 2, 3, 4),
               (1, 2, 3, 4, 5),
               (2, 3, 4, 5, 6),
               (3, 4, 5, 6, 7),
               (4, 5, 6, 7, 8);
      ",
      10000,
      exp_result,
    );
  }

  {
    let mut exp_result = TableView::new(vec![cno("k21"), cno("v21"), cno("v22")]);
    exp_result.add_row(vec![mki(0), mki(2), mki(3)]);
    exp_result.add_row(vec![mki(1), mki(3), mki(4)]);
    exp_result.add_row(vec![mki(2), mki(4), mki(5)]);
    exp_result.add_row(vec![mki(3), mki(5), mki(6)]);
    exp_result.add_row(vec![mki(4), mki(6), mki(7)]);
    ctx.execute_query(
      sim,
      " INSERT INTO table2 (k21, v21, v22)
        VALUES (0, 2, 3),
               (1, 3, 4),
               (2, 4, 5),
               (3, 5, 6),
               (4, 6, 7);
      ",
      10000,
      exp_result,
    );
  }
}

// -----------------------------------------------------------------------------------------------
//  test_all_advanced_parallel
// -----------------------------------------------------------------------------------------------

pub fn test_all_advanced_parallel(rand: &mut XorShiftRng) {
  for i in 0..10 {
    println!("Running round {:?}", i);
    advanced_parallel_test(mk_seed(rand));
  }
}

pub fn advanced_parallel_test(seed: [u8; 16]) {
  let master_address_config: Vec<EndpointId> = vec![mk_eid("me0")];
  let slave_address_config: BTreeMap<SlaveGroupId, Vec<EndpointId>> = vec![
    (mk_sid("s0"), vec![mk_slave_eid(0)]),
    (mk_sid("s1"), vec![mk_slave_eid(1)]),
    (mk_sid("s2"), vec![mk_slave_eid(2)]),
    (mk_sid("s3"), vec![mk_slave_eid(3)]),
    (mk_sid("s4"), vec![mk_slave_eid(4)]),
  ]
  .into_iter()
  .collect();

  // We create 3 clients.
  let mut sim =
    Simulation::new(seed, 3, slave_address_config, master_address_config, PaxosConfig::test(), 1);

  let mut ctx = TestContext::new();

  // Setup Tables
  setup_tables(&mut sim, &mut ctx);
  sim.remove_all_responses(); // Clear the DDL response.

  // Create the QueryGenerator
  let mut query_generator = QueryGenerator::new();

  // Run the simulation
  let client_eids: Vec<_> = sim.get_all_responses().keys().cloned().collect();
  let mut req_map = BTreeMap::<EndpointId, BTreeMap<RequestId, msg::PerformExternalQuery>>::new();
  for eid in &client_eids {
    req_map.insert(eid.clone(), BTreeMap::new());
  }

  let mut req_res_map =
    BTreeMap::<RequestId, (msg::PerformExternalQuery, msg::ExternalMessage)>::new();

  const SIM_DURATION: u128 = 1000; // The duration that we run the simulation
  let sim_duration = mk_t(SIM_DURATION);
  while sim.true_timestamp() < &sim_duration {
    // Generate a random query
    let query = query_generator.mk_tp_query(&mut sim).unwrap();

    // Construct a request and populate `req_map`
    let request_id = mk_rid(&mut sim.rand);
    let client_idx = sim.rand.next_u32() as usize % client_eids.len();
    let client_eid = client_eids.get(client_idx).unwrap();

    let perform = msg::PerformExternalQuery {
      sender_eid: client_eid.clone(),
      request_id: request_id.clone(),
      query: query.to_string(),
    };
    req_map.get_mut(client_eid).unwrap().insert(request_id, perform.clone());

    // Send the request and simulate
    let slave_idx = sim.rand.next_u32() % client_eids.len() as u32;
    let slave_eid = mk_slave_eid(slave_idx);
    sim.add_msg(
      msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
        msg::SlaveExternalReq::PerformExternalQuery(perform),
      )),
      client_eid,
      &slave_eid,
    );

    let sim_duration = sim.rand.next_u32() % 50; // simulation only 50 ms at a time
    sim.simulate_n_ms(sim_duration);

    // Move any new responses to to `req_res_map`.
    for (eid, responses) in sim.remove_all_responses() {
      for res in responses {
        let external = cast!(msg::NetworkMessage::External, res).unwrap();
        let request_id = match &external {
          msg::ExternalMessage::ExternalQuerySuccess(success) => &success.request_id,
          msg::ExternalMessage::ExternalQueryAborted(aborted) => match &aborted.payload {
            ExternalAbortedData::ParseError(error) => {
              println!("Invalid query! Parse error: {:?}", error);
              println!("Query: {}\n", format_sql(&query));
              panic!();
            }
            ExternalAbortedData::QueryPlanningError(error) => {
              println!("Invalid query! QueryPlanning error: {:?}", error);
              println!("Query: {}\n", format_sql(&query));
              panic!();
            }
            _ => &aborted.request_id,
          },
          _ => panic!(),
        };

        let req = req_map.get_mut(&eid).unwrap().remove(request_id).unwrap();
        req_res_map.insert(request_id.clone(), (req, external));
      }
    }
  }

  // Verify the responses are correct
  if let Some((true_time, total_queries, successful_queries)) =
    verify_req_res(&mut sim.rand, req_res_map)
  {
    println!(
      "Test 'test_all_advanced_parallel' Passed! Replay time taken: {:?}ms.
       Total Queries: {:?}, Succeeded: {:?}",
      true_time.time_ms, total_queries, successful_queries
    );
  } else {
    println!("Skipped Test 'test_all_advanced_parallel' due to Timestamp Conflict");
  }
}
