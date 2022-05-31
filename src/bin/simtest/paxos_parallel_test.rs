use crate::serial_test_utils::{mk_general_sim, setup, simulate_until_clean, TestContext};
use crate::simulation::Simulation;
use crate::stats::Stats;
use rand::seq::SliceRandom;
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::{
  mk_rid, mk_t, mk_tid, read_index, ColType, ColVal, TableSchema, TabletGroupId, TabletKeyRange,
  Timestamp, ALPHABET,
};
use runiversal::common::{ColName, TablePath};
use runiversal::common::{
  EndpointId, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, RequestId, SlaveGroupId,
};
use runiversal::message as msg;
use runiversal::paxos::PaxosConfig;
use runiversal::shard_split_tm_es::STRange;
use runiversal::simulation_utils::mk_slave_eid;
use runiversal::sql_ast::iast;
use runiversal::sql_parser::convert_ast;
use runiversal::test_utils::{mk_seed, mk_sid};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::test_utils::table;
use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

// -----------------------------------------------------------------------------------------------
//  Query Generation Utils
// -----------------------------------------------------------------------------------------------

fn mk_uint(r: &mut XorShiftRng, abs: u32) -> u32 {
  r.next_u32() % abs
}

fn mk_int(r: &mut XorShiftRng, abs: u32) -> i32 {
  let val = (r.next_u32() % abs) as i32;
  if r.next_u32() % 2 == 0 {
    -val
  } else {
    val
  }
}

/// Get a random element of `v` if it is non-empty.
fn rand_elem<'a, T>(v: &'a Vec<T>, r: &mut XorShiftRng) -> Option<&'a T> {
  if v.is_empty() {
    None
  } else {
    let idx = (r.next_u32() as usize % v.len());
    v.get(idx)
  }
}

struct QueryGenCtx<'a> {
  rand: &'a mut XorShiftRng,
  timestamp: Timestamp,
  table_schemas: &'a BTreeMap<TablePath, &'a TableSchema>,
  sharding_config: &'a BTreeMap<TablePath, &'a Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: &'a BTreeMap<TabletGroupId, SlaveGroupId>,
}

/// Add Tables will have one Primary Key column of `Int` type. All ValCol types will
/// also be `Int`. We limit the system to 10 tables, named "t0" ... "t9", and we limit the
/// set of columns in a table to 10 ValCols, named "c0" ... "c9". There also only 10 KeyCols
/// names we get to choose from: "k0" ... "k9"
impl<'a> QueryGenCtx<'a> {
  /// The absolute bound on what an integer value can take on; all ColVal::Int values
  /// `x` must be `-INT_BOUND < x < INT_BOUND`.
  const INT_BOUND: u32 = 100;

  fn mk_create_table(&mut self) -> Option<String> {
    let r = &mut self.rand;

    // Compute all possible TablePaths and shuffle it.
    let mut all_table_paths = Vec::<TablePath>::new();
    for i in 0..10 {
      all_table_paths.push(TablePath(format!("t{}", i)));
    }

    all_table_paths[..].shuffle(r);

    // Choose a TablePath that does not already exist and create it.
    for table_path in all_table_paths {
      if !self.table_schemas.contains_key(&table_path) {
        // Create KeyCol
        let key_col = format!("k{}", r.next_u32() % 10);

        // Create ValCols
        let mut all_val_cols = Vec::<String>::new();
        for i in 0..10 {
          all_val_cols.push(format!("c{}", i));
        }
        all_val_cols[..].shuffle(r);

        // We choose betwee [0, 10], both ends inclusive.
        let num_val_cols = r.next_u32() as usize % (10 + 1);
        let val_cols: Vec<String> = all_val_cols.into_iter().take(num_val_cols).collect();

        // Create the query
        let mut val_col_defs = Vec::<String>::new();
        for col in val_cols {
          val_col_defs.push(format!("{} INT", col));
        }

        let query = format!(
          "CREATE TABLE {} (
              {} INT PRIMARY KEY,
              {}
           );
          ",
          table_path.0,
          key_col,
          val_col_defs.join(", ")
        );

        return Some(query);
      }
    }

    // If we get here, then all possible Table already exist.
    None
  }

  fn mk_drop_table(&mut self) -> Option<String> {
    let r = &mut self.rand;

    // Find a Table to drop
    let cur_table_paths: Vec<TablePath> = self.table_schemas.keys().cloned().collect();
    let table_path = rand_elem(&cur_table_paths, r)?;

    // Create the query
    let query = format!("DROP TABLE {};", table_path.0);
    Some(query)
  }

  fn mk_add_col(&mut self) -> Option<String> {
    let r = &mut self.rand;

    // Find a Table to add a column to
    let cur_table_paths: Vec<TablePath> = self.table_schemas.keys().cloned().collect();
    let table_path = rand_elem(&cur_table_paths, r)?;
    let table_schema = self.table_schemas.get(&table_path).unwrap();

    // Compute all possible ValCol ColNames and shuffle it.
    let mut all_val_cols = Vec::<ColName>::new();
    for i in 0..10 {
      all_val_cols.push(ColName(format!("c{}", i)));
    }

    all_val_cols[..].shuffle(r);

    // Choose a ColName that does not already exist and add it
    let cur_val_cols = table_schema.val_cols.static_snapshot_read(&self.timestamp);
    for col in all_val_cols {
      if !cur_val_cols.contains_key(&col) {
        // Create the query
        let query = format!(
          "ALTER TABLE {}
           ADD COLUMN {} INT;
          ",
          table_path.0, col.0,
        );

        return Some(query);
      }
    }

    // If we get here, then all possible ValCols already exist for `table_path`.
    None
  }

  fn mk_drop_col(&mut self) -> Option<String> {
    let r = &mut self.rand;

    // Find a Table to drop a column from
    let cur_table_paths: Vec<TablePath> = self.table_schemas.keys().cloned().collect();
    let table_path = rand_elem(&cur_table_paths, r)?;
    let table_schema = self.table_schemas.get(&table_path).unwrap();

    // Choose a ColName that does not already exist and add it
    let mut cur_val_cols: Vec<ColName> =
      table_schema.val_cols.static_snapshot_read(&self.timestamp).into_keys().collect();
    let col = rand_elem(&cur_val_cols, r)?;

    // Create the query
    let query = format!(
      "ALTER TABLE {}
       DROP COLUMN {};
      ",
      table_path.0, col.0,
    );

    Some(query)
  }

  fn mk_insert(&mut self) -> Option<String> {
    // Choose a random Table to Insert to.
    let (source, mut key_cols, mut val_cols) = self.pick_random_table()?;
    let r = &mut self.rand;

    // Take some prefix of all val_cols to insert to
    val_cols[..].shuffle(r);
    let num_chosen_cols = r.next_u32() as usize % (val_cols.len() + 1);
    let chosen_cols: Vec<ColName> = val_cols.into_iter().take(num_chosen_cols).collect();

    // Construct the insert Schema
    let mut insert_cols = Vec::<String>::new();
    for key_col in &key_cols {
      insert_cols.push(key_col.0.clone());
    }
    for val_col in &chosen_cols {
      insert_cols.push(val_col.0.clone());
    }

    // Construct the Insert Rows
    let num_rows = (r.next_u32() % 10) + 1; // We only insert at most 10 rows at a time
    let row_len = key_cols.len() + chosen_cols.len();
    let mut rows = Vec::<String>::new();
    for _ in 0..num_rows {
      let mut values = Vec::<String>::new();
      for _ in 0..row_len {
        // Recall that all columns have Int type
        values.push(format!("{}", mk_int(r, Self::INT_BOUND)));
      }
      rows.push(format!("({})", values.join(", ")));
    }

    Some(format!(
      " INSERT INTO {} ({})
        VALUES {};
      ",
      source,
      insert_cols.join(", "),
      rows.join(",\n")
    ))
  }

  fn mk_update(&mut self) -> Option<String> {
    // Choose a random Table to Update.
    let (source, mut key_cols, mut val_cols) = self.pick_random_table()?;
    let r = &mut self.rand;

    // Recall that there is only one KeyCol in all Tables
    let key_col = key_cols.into_iter().next().unwrap();
    val_cols[..].shuffle(r);
    let mut val_col_it = val_cols.into_iter();

    let query_type = r.next_u32() % 3;
    let query = if query_type == 0 {
      let val_col = val_col_it.next()?;
      format!(
        " UPDATE {source}
          SET {val_col} = {val_col} + {x1}
          WHERE {key_col} >= {x2} AND ({val_col} + {x1} < {INT_BOUND});
        ",
        source = source,
        val_col = val_col.0,
        key_col = key_col.0,
        x1 = mk_uint(r, 5),
        x2 = mk_int(r, Self::INT_BOUND),
        INT_BOUND = Self::INT_BOUND
      )
    } else if query_type == 1 {
      let val_col = val_col_it.next()?;
      format!(
        " UPDATE {source}
          SET {val_col} = {val_col} - {x1}
          WHERE {key_col} >= {x2} AND ({val_col} - {x1} > -{INT_BOUND});
        ",
        source = source,
        val_col = val_col.0,
        key_col = key_col.0,
        x1 = mk_uint(r, 5),
        x2 = mk_int(r, Self::INT_BOUND),
        INT_BOUND = Self::INT_BOUND
      )
    } else if query_type == 2 {
      let set_val_col = val_col_it.next()?;
      let filter_val_col = val_col_it.next()?;
      format!(
        " UPDATE {source}
          SET {set_val_col} = {x1}
          WHERE {filter_val_col} >= {x2};
        ",
        source = source,
        set_val_col = set_val_col.0,
        filter_val_col = filter_val_col.0,
        x1 = mk_int(r, Self::INT_BOUND),
        x2 = mk_int(r, Self::INT_BOUND)
      )
    } else {
      panic!()
    };

    Some(query)
  }

  fn mk_delete(&mut self) -> Option<String> {
    // Choose a random Table to Delete from.
    let (source, mut key_cols, mut val_cols) = self.pick_random_table()?;
    let r = &mut self.rand;

    // Recall that there is only one KeyCol in all Tables
    let key_col = key_cols.into_iter().next().unwrap();
    val_cols[..].shuffle(r);
    let mut val_col_it = val_cols.into_iter();

    let query_type = r.next_u32() % 3;
    let query = if query_type == 0 {
      let val_col = val_col_it.next()?;
      format!(
        " DELETE
          FROM {source}
          WHERE {val_col} >= {x1};
        ",
        source = source,
        val_col = val_col.0,
        x1 = mk_int(r, Self::INT_BOUND)
      )
    } else if query_type == 1 {
      let val_col = val_col_it.next()?;
      format!(
        " DELETE
          FROM {source}
          WHERE {val_col} >= {x1} AND {val_col} < {x2};
        ",
        source = source,
        val_col = val_col.0,
        x1 = mk_int(r, Self::INT_BOUND / 2),
        x2 = mk_int(r, Self::INT_BOUND / 2) + Self::INT_BOUND as i32
      )
    } else if query_type == 2 {
      format!(
        " DELETE
          FROM {source}
          WHERE {key_col} >= {x1} AND {key_col} < {x2};
        ",
        source = source,
        key_col = key_col.0,
        x1 = mk_int(r, Self::INT_BOUND / 2),
        x2 = mk_int(r, Self::INT_BOUND / 2) + Self::INT_BOUND as i32
      )
    } else {
      panic!()
    };

    Some(query)
  }

  fn mk_select(&mut self) -> Option<String> {
    // Choose a random Table to Read from.
    let (source, mut key_cols, mut val_cols) = self.pick_random_table()?;
    let r = &mut self.rand;

    // Recall that there is only one KeyCol in all Tables
    let key_col = key_cols.into_iter().next().unwrap();
    val_cols[..].shuffle(r);
    let mut val_col_it = val_cols.into_iter();

    let query_type = r.next_u32() % 4;
    let query = if query_type == 0 {
      let proj_val_col = val_col_it.next()?;
      let filter_val_col = val_col_it.next()?;
      format!(
        " SELECT {proj_val_col}
          FROM {source}
          WHERE {filter_val_col} >= {x1};
        ",
        source = source,
        proj_val_col = proj_val_col.0,
        filter_val_col = filter_val_col.0,
        x1 = mk_int(r, Self::INT_BOUND)
      )
    } else if query_type == 1 {
      let filter_val_col = val_col_it.next()?;
      format!(
        " SELECT {key_col}
          FROM {source}
          WHERE {filter_val_col} >= {x1} AND {filter_val_col} < {x2};
        ",
        source = source,
        key_col = key_col.0,
        filter_val_col = filter_val_col.0,
        x1 = mk_int(r, Self::INT_BOUND / 2),
        x2 = mk_int(r, Self::INT_BOUND / 2) + Self::INT_BOUND as i32
      )
    } else if query_type == 2 {
      let proj_val_col = val_col_it.next()?;
      format!(
        " SELECT {proj_val_col}
          FROM {source}
          WHERE {key_col} >= {x1} AND {key_col} < {x2};
        ",
        source = source,
        proj_val_col = proj_val_col.0,
        key_col = key_col.0,
        x1 = mk_int(r, Self::INT_BOUND / 2),
        x2 = mk_int(r, Self::INT_BOUND / 2) + Self::INT_BOUND as i32
      )
    } else if query_type == 3 {
      format!(
        " SELECT *
          FROM {source}
          WHERE {key_col} >= {x1} AND {key_col} < {x2};
        ",
        source = source,
        key_col = key_col.0,
        x1 = mk_int(r, Self::INT_BOUND / 2),
        x2 = mk_int(r, Self::INT_BOUND / 2) + Self::INT_BOUND as i32
      )
    } else {
      panic!()
    };

    Some(query)
  }

  /// This create queries that touch 2 tables, containing CTEs and subqueries.
  fn mk_advanced_query(&mut self) -> Option<String> {
    let (source1, mut key_cols1, mut val_cols1) = self.pick_random_table()?;
    let (source2, mut key_cols2, mut val_cols2) = self.pick_random_table()?;
    let r = &mut self.rand;

    // Recall that there is only one KeyCol in all Tables
    let key_col1 = key_cols1.into_iter().next().unwrap();
    val_cols1[..].shuffle(r);
    let mut val_col_it1 = val_cols1.into_iter();

    let key_col2 = key_cols2.into_iter().next().unwrap();
    val_cols2[..].shuffle(r);
    let mut val_col_it2 = val_cols2.into_iter();

    let query_type = r.next_u32() % 2;
    let query = if query_type == 0 {
      let proj_val_col11 = val_col_it1.next()?;
      let proj_val_col12 = val_col_it1.next()?;
      format!(
        " WITH
            tt1 AS (SELECT {proj_val_col11} AS c11, {proj_val_col12} AS c12
                    FROM {source1}
                    WHERE {key_col1} >= {x1} AND {key_col1} < {x2})
            SELECT c11
            FROM tt1;
        ",
        source1 = source1,
        proj_val_col11 = proj_val_col11.0,
        proj_val_col12 = proj_val_col12.0,
        key_col1 = key_col1.0,
        x1 = mk_int(r, Self::INT_BOUND / 2),
        x2 = mk_int(r, Self::INT_BOUND / 2) + Self::INT_BOUND as i32,
      )
    } else if query_type == 1 {
      let proj_val_col11 = val_col_it1.next()?;
      let proj_val_col12 = val_col_it1.next()?;
      let proj_val_col21 = val_col_it2.next()?;
      let proj_val_col22 = val_col_it2.next()?;
      format!(
        " WITH
            tt1 AS (SELECT {proj_val_col11} AS c11, {proj_val_col12} AS c12
                    FROM {source1}
                    WHERE {key_col1} >= {x1} AND {key_col1} < {x2}),
            tt2 AS (SELECT {proj_val_col21} AS c21, {proj_val_col22} AS c22
                    FROM {source2}
                    WHERE {key_col2} < 
                     (SELECT AVG(c11)
                      FROM tt1
                      WHERE c12 < {x3})
                     OR
                     ({key_col2} < 0))
            SELECT c21
            FROM tt2;
        ",
        source1 = source1,
        proj_val_col11 = proj_val_col11.0,
        proj_val_col12 = proj_val_col12.0,
        key_col1 = key_col1.0,
        source2 = source2,
        proj_val_col21 = proj_val_col21.0,
        proj_val_col22 = proj_val_col22.0,
        key_col2 = key_col2.0,
        x1 = mk_int(r, Self::INT_BOUND / 2),
        x2 = mk_int(r, Self::INT_BOUND / 2) + Self::INT_BOUND as i32,
        x3 = mk_int(r, Self::INT_BOUND)
      )
    } else {
      panic!()
    };

    Some(query)
  }

  /// Since the different stages in a Multi-Stage Transaction do not interact with one
  /// another (e.g. prior Stages do not define a TransTable for subsequent Stages), generating
  /// one is just a matter of generating a sequence of Single-Stage Transactions.
  fn mk_multi_stage(&mut self) -> Option<String> {
    let num_stages = (self.rand.next_u32() % 6) + 1;
    let mut stages = Vec::<String>::new();
    for _ in 0..num_stages {
      let stage_type = self.rand.next_u32() % 4;
      let stage = match stage_type {
        0 => self.mk_insert()?,
        1 => self.mk_update()?,
        2 => self.mk_delete()?,
        3 => self.mk_select()?,
        _ => panic!(),
      };
      stages.push(stage);
    }
    Some(stages.join("\n"))
  }

  /// Picks an existing Tablets and creates a `ShardingOp` to split it.
  fn mk_sharding_query(&mut self) -> Option<msg::ShardingOp> {
    // Pick an existing Tablet randomly.
    let (table_path, schema) = Self::pick_random_table_path(&mut self.rand, &self.table_schemas)?;
    let sharding = self.sharding_config.get(table_path).unwrap();
    let shard_idx = self.rand.next_u32() as usize % sharding.len();
    let (range, old_tid) = sharding.get(shard_idx).unwrap();
    let old_sid = self.tablet_address_config.get(old_tid).unwrap();

    // Split the range and define the location of a new Tablet.
    let num_tablets = self.tablet_address_config.len(); // `num_tablets` is certain non-zero.
    let mut tablet_idx = self.rand.next_u32() as usize % num_tablets;
    let (_, new_sid) = read_index(self.tablet_address_config, tablet_idx)?;
    let new_tid = mk_tid(&mut self.rand);

    // Split the range
    let (_, first_key_col) = schema.key_cols.first()?;
    let (old_range, new_range) = match first_key_col {
      ColType::Int => {
        let mid: i32 = match (&range.start, &range.end) {
          (Some(ColVal::Int(start)), Some(ColVal::Int(end))) => {
            if start == end {
              return None;
            } else {
              assert!(start < end);
              (start + end) / 2
            }
          }
          (Some(ColVal::Int(start)), None) => *start + 10, // We simply add 10 in this case.
          (None, Some(ColVal::Int(end))) => *end - 10,     // We simply subtract 10 in this case.
          (None, None) => 0,
          _ => panic!(),
        };
        (
          TabletKeyRange { start: range.start.clone(), end: Some(ColVal::Int(mid.clone())) },
          TabletKeyRange { start: Some(ColVal::Int(mid)), end: range.end.clone() },
        )
      }
      // We do not attempt to shard a Tablet that start with a boolean key.
      ColType::Bool => return None,
      ColType::String => {
        let (start_idx, end_idx) = match (&range.start, &range.end) {
          (Some(ColVal::String(start)), Some(ColVal::String(end))) => {
            if start.is_empty() || end.is_empty() {
              return None;
            } else {
              (
                ALPHABET.find(start.chars().next().unwrap()).unwrap(),
                ALPHABET.find(end.chars().next().unwrap()).unwrap(),
              )
            }
          }
          (Some(ColVal::String(start)), None) => {
            if start.is_empty() {
              return None;
            } else {
              (ALPHABET.find(start.chars().next().unwrap()).unwrap(), ALPHABET.len() - 1)
            }
          }
          (None, Some(ColVal::String(end))) => {
            if end.is_empty() {
              return None;
            } else {
              (0, ALPHABET.find(end.chars().next().unwrap()).unwrap())
            }
          }
          (None, None) => (0, ALPHABET.len() - 1),
          _ => panic!(),
        };

        // Take the character in the middle.
        let mid = if start_idx == end_idx {
          return None;
        } else {
          assert!(start_idx < end_idx);
          ALPHABET.chars().nth((start_idx + end_idx) / 2).unwrap().to_string()
        };

        (
          TabletKeyRange { start: range.start.clone(), end: Some(ColVal::String(mid.clone())) },
          TabletKeyRange { start: Some(ColVal::String(mid)), end: range.end.clone() },
        )
      }
    };

    Some(msg::ShardingOp::Split(msg::SplitShardingOp {
      table_path: table_path.clone(),
      target_old: STRange { sid: old_sid.clone(), tid: old_tid.clone(), range: old_range.clone() },
      target_new: STRange { sid: new_sid.clone(), tid: new_tid.clone(), range: new_range.clone() },
    }))
  }

  /// Choose a random table and return its name, KeyCols, and ValCols
  fn pick_random_table(&mut self) -> Option<(String, Vec<ColName>, Vec<ColName>)> {
    let (source, schema) = Self::pick_random_table_path(&mut self.rand, &self.table_schemas)?;
    let key_cols: Vec<ColName> = schema.key_cols.iter().map(|(c, _)| c).cloned().collect();
    let val_col_map = schema.val_cols.static_snapshot_read(&self.timestamp);
    let val_cols: Vec<ColName> = val_col_map.into_keys().collect();
    Some((source.0.clone(), key_cols, val_cols))
  }

  /// Choose a random table and return its `TabletPath` and `TableSchema`.
  fn pick_random_table_path<'b, 'c>(
    rand: &'c mut XorShiftRng,
    table_schemas: &'b BTreeMap<TablePath, &'b TableSchema>,
  ) -> Option<(&'b TablePath, &'b &'b TableSchema)> {
    let num_sources = table_schemas.len();
    if num_sources == 0 {
      None
    } else {
      let mut source_idx = rand.next_u32() as usize % num_sources;
      read_index(table_schemas, source_idx)
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
enum Request {
  DDLQuery(msg::PerformExternalDDLQuery),
  Sharding(msg::PerformExternalSharding),
  Query(msg::PerformExternalQuery),
}

/// Results of `verify_req_res`, which contains extra statistics useful for checking
/// non-triviality of the test.
struct VerifyResult {
  replay_duration: Timestamp,
  total_queries: u32,
  successful_queries: u32,
  num_multi_stage: u32,
  stats_ncte_1stage: Vec<AvgCounter>,
  queries_cancelled: u32,
  ddl_queries_cancelled: u32,
  sharding_success: u32,
  sharding_aborted: u32,
}

struct AvgCounter {
  num_elems: u32,
  sum: i32,
}

impl AvgCounter {
  fn add_entry(&mut self, val: i32) {
    self.num_elems += 1;
    self.sum += val;
  }

  fn avg(&self) -> Option<f32> {
    if self.num_elems == 0 {
      None
    } else {
      Some(self.sum as f32 / self.num_elems as f32)
    }
  }
}

/// Replay the requests that succeeded in timestamp order serially, and verify that
/// the results are the same. The `full_req_res_map` contains the set of all `Request`s
/// sent, along with a response (if one was sent back). Recall that some requests might not get
/// a response due to nodes dying. We also have `success_write_reqs`, which contains exactly
/// queries that succeeded which might do some kind of write (i.e. DDL Queries and Multi-Stage
/// Queries). Any query excluded from `success_write_reqs` cannot do any sort of write.
///
/// Note: We do not replay Sharding queries.
fn verify_req_res(
  rand: &mut XorShiftRng,
  full_req_res_map: BTreeMap<RequestId, (Request, Option<msg::ExternalMessage>)>,
  success_write_reqs: BTreeMap<RequestId, Timestamp>,
) -> Option<VerifyResult> {
  // Pairs up the Query and DDLQuery request-response pairs.
  enum SuccessPair {
    Query(msg::PerformExternalQuery, Option<msg::ExternalQuerySuccess>),
    DDLQuery(msg::PerformExternalDDLQuery, Option<msg::ExternalDDLQuerySuccess>),
  }

  // Setup some stats
  let mut queries_cancelled = 0;
  let mut ddl_queries_cancelled = 0;
  let mut sharding_success = 0;
  let mut sharding_aborted = 0;
  let total_queries = full_req_res_map.len() as u32;

  // This map contains the set of queries we need to execute in the replay. Whether we check
  // the output depends on if the result in the `SuccessPair` is Some(_) or not.
  let mut sorted_success_res = BTreeMap::<Timestamp, SuccessPair>::new();
  for (rid, (req, maybe_res)) in full_req_res_map {
    // First, handle the case we get a response.
    if let Some(res) = maybe_res {
      match (req, res) {
        (Request::Query(_), msg::ExternalMessage::ExternalQueryAborted(abort)) => {
          // For aborts, we merely ensure they did not occur trivially (i.e. ParseError).
          match abort.payload {
            msg::ExternalAbortedData::ParseError(parse_error) => {
              panic!("Query Parse Error: {:?}", parse_error);
            }
            msg::ExternalAbortedData::CancelConfirmed => {
              queries_cancelled += 1;
            }
            _ => {}
          }
        }
        (Request::Query(req), msg::ExternalMessage::ExternalQuerySuccess(success)) => {
          // Add in the request-response pair. Abort this test if `Timestamp` already exists.
          if !sorted_success_res
            .insert(success.timestamp.clone(), SuccessPair::Query(req, Some(success)))
            .is_none()
          {
            return None;
          }
        }
        (Request::DDLQuery(_), msg::ExternalMessage::ExternalDDLQueryAborted(abort)) => {
          // For aborts, we merely ensure they did not occur trivially (i.e. ParseError).
          match abort.payload {
            msg::ExternalDDLQueryAbortData::ParseError(parse_error) => {
              panic!("Query Parse Error: {:?}", parse_error);
            }
            msg::ExternalDDLQueryAbortData::CancelConfirmed => {
              queries_cancelled += 1;
            }
            _ => {}
          }
        }
        (Request::DDLQuery(req), msg::ExternalMessage::ExternalDDLQuerySuccess(success)) => {
          // Add in the request-response pair. Abort this test if `Timestamp` already exists.
          if !sorted_success_res
            .insert(success.timestamp.clone(), SuccessPair::DDLQuery(req, Some(success)))
            .is_none()
          {
            return None;
          }
        }
        (Request::Sharding(_), msg::ExternalMessage::ExternalShardingSuccess(_)) => {
          sharding_success += 1;
        }
        (Request::Sharding(_), msg::ExternalMessage::ExternalShardingAborted(_)) => {
          sharding_aborted += 1;
        }
        _ => panic!(),
      }
    } else {
      // If we did not get a response, the request might still have succeeded in the system.
      // This matter if the request was a write. We use `success_write_reqs` to account for it.
      if let Some(timestamp) = success_write_reqs.get(&rid) {
        match req {
          Request::Query(req) => {
            // Add in the request with an unknown response. Abort this test if
            // `Timestamp` already exists.
            if !sorted_success_res
              .insert(timestamp.clone(), SuccessPair::Query(req, None))
              .is_none()
            {
              return None;
            }
          }
          Request::DDLQuery(req) => {
            // Add in the request with an unknown response. Abort this test if
            // `Timestamp` already exists.
            if !sorted_success_res
              .insert(timestamp.clone(), SuccessPair::DDLQuery(req, None))
              .is_none()
            {
              return None;
            }
          }
          Request::Sharding(_) => {
            // We do not replay Sharding queries during Replay.
          }
        }
      }
    }
  }

  // Compute various statistics
  let mut num_multi_stage = 0;
  let mut stats_ncte_1stage = Vec::new();
  for (_, pair) in &sorted_success_res {
    if let SuccessPair::Query(req, Some(res)) = pair {
      // Here, the `req` is expected to be a DML or DQL (not DDL).
      let parsed_ast = Parser::parse_sql(&GenericDialect {}, &req.query).unwrap();
      let num_stages = parsed_ast.len();

      // See if this is a multi-stage transaction.
      if num_stages > 1 {
        num_multi_stage += 1;
      } else {
        // Otherwise, collect stats for Single-Stage queries.
        let ast = convert_ast(parsed_ast).unwrap();
        match ast.body {
          iast::QueryBody::SuperSimpleSelect(_) => {
            let i = ast.ctes.len();
            for _ in stats_ncte_1stage.len()..(i + 1) {
              stats_ncte_1stage.push(AvgCounter { num_elems: 0, sum: 0 });
            }
            stats_ncte_1stage.get_mut(i).unwrap().add_entry(res.result.data.rows.len() as i32);
          }
          _ => {}
        }
      }
    }
  }

  // Run the Replay
  let (mut sim, mut ctx) = setup(mk_seed(rand));
  let successful_queries = sorted_success_res.len() as u32;
  for (_, pair) in sorted_success_res {
    match pair {
      SuccessPair::Query(req, res) => {
        if let Some(res) = res {
          ctx.execute_query(&mut sim, req.query.as_str(), 10000, res.result);
        } else {
          ctx.execute_query_simple(&mut sim, req.query.as_str(), 10000);
        }
      }
      SuccessPair::DDLQuery(req, _) => {
        ctx.send_ddl_query(&mut sim, req.query.as_str(), 10000);
      }
    }
  }

  Some(VerifyResult {
    replay_duration: sim.true_timestamp().clone(),
    total_queries,
    successful_queries,
    num_multi_stage,
    stats_ncte_1stage,
    queries_cancelled,
    ddl_queries_cancelled,
    sharding_success,
    sharding_aborted,
  })
}

/// Abstract interface for writing (useful for concurrency).
pub trait Writer {
  fn println(&mut self, s: String);

  fn flush(&mut self);
}

pub struct ParallelTestStats {
  pub all_stats: Vec<Stats>,
  pub all_reconfig_stats: Vec<Stats>,
  pub all_sharding_stats: Vec<Stats>,
}

// -----------------------------------------------------------------------------------------------
//  test_all_parallel
// -----------------------------------------------------------------------------------------------

pub fn test_all_basic_parallel<WriterT: Writer>(
  rand: &mut XorShiftRng,
  w: &mut WriterT,
  rounds: u32,
) -> Vec<Stats> {
  // Accumulats message statistics for every round.
  let mut stats_basic = Vec::<Stats>::new();

  // Execute the rounds.
  for i in 0..rounds {
    let start_t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    w.println(format!("Running round {:?}", i));
    let mut maybe_stats = parallel_test(mk_seed(rand), 1, 0, false, w);
    w.flush();
    let end_t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    if let Some(mut stats) = maybe_stats {
      stats.duration = (end_t - start_t) as u32;
      stats_basic.push(stats);
    }
  }

  stats_basic
}

pub fn test_all_paxos_parallel<WriterT: Writer>(
  rand: &mut XorShiftRng,
  w: &mut WriterT,
  rounds: u32,
) -> ParallelTestStats {
  // Accumulats message statistics for every round.
  let mut all_stats = Vec::<Stats>::new();
  let mut all_reconfig_stats = Vec::<Stats>::new();
  let mut all_sharding_stats = Vec::<Stats>::new();

  // Execute the rounds.
  for i in 0..rounds {
    let start_t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    w.println(format!("Running round {:?}", 3 * i));
    let mut maybe_stats = parallel_test(mk_seed(rand), 5, 0, false, w);
    w.flush();
    let end_t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    if let Some(mut stats) = maybe_stats {
      stats.duration = (end_t - start_t) as u32;
      all_stats.push(stats);
    }

    let start_t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    w.println(format!("Running reconfig round {:?}", 3 * i + 1));
    let maybe_stats = parallel_test(mk_seed(rand), 5, 10, false, w);
    w.flush();
    let end_t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    if let Some(mut stats) = maybe_stats {
      stats.duration = (end_t - start_t) as u32;
      all_reconfig_stats.push(stats);
    }

    let start_t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    w.println(format!("Running sharding round {:?}", 3 * i + 2));
    let maybe_stats = parallel_test(mk_seed(rand), 5, 10, true, w);
    w.flush();
    let end_t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
    if let Some(mut stats) = maybe_stats {
      stats.duration = (end_t - start_t) as u32;
      all_sharding_stats.push(stats);
    }
  }

  // Return various statistics
  ParallelTestStats { all_stats, all_reconfig_stats, all_sharding_stats }
}

pub fn parallel_test<WriterT: Writer>(
  seed: [u8; 16],
  num_paxos_nodes: u32,
  num_reconfig_free_nodes: u32,
  do_sharding: bool,
  w: &mut WriterT,
) -> Option<Stats> {
  w.println(format!("seed: {:?}", seed));
  let mut sim = mk_general_sim(seed, 3, 5, num_paxos_nodes, 100, num_reconfig_free_nodes);

  // Run the simulation
  let client_eids: Vec<_> = sim.get_all_responses().keys().cloned().collect();
  let sids: Vec<_> = sim.full_db_schema().slave_address_config.keys().cloned().collect();
  let gids: Vec<_> = sim.leader_map.keys().cloned().collect();

  // These 2 are kept in sync, where the set of RequestIds in each map are always the same.
  // In both, the EndpointId is that of the client that send the request.
  let mut req_lid_map = BTreeMap::<RequestId, (EndpointId, PaxosGroupId, LeadershipId)>::new();
  let mut req_map = BTreeMap::<EndpointId, BTreeMap<RequestId, Request>>::new();
  for eid in &client_eids {
    req_map.insert(eid.clone(), BTreeMap::new());
  }

  // Elements from the above are moved here as responses arrive.
  let mut req_res_map = BTreeMap::<RequestId, (Request, msg::ExternalMessage)>::new();

  const SIM_DURATION: u128 = 5000; // The duration that we run the simulation
  let sim_duration = mk_t(SIM_DURATION);
  for iteration in 0.. {
    let timestamp = sim.true_timestamp().clone();
    if timestamp >= sim_duration {
      break;
    }

    const NUM_WARMUP_ITERATIONS: u32 = 5;

    // Decide whether to send a query or to send a cancellation. We usually send a query with high
    // probability, but we must certainly send a query if we have not warmed up or if there are
    // no existing queries to cancel.
    let do_query = (sim.rand.next_u32() % 100) < 85;
    if do_query || iteration < NUM_WARMUP_ITERATIONS || req_lid_map.is_empty() {
      // Create a new RNG for query generation
      let mut rand = XorShiftRng::from_seed(mk_seed(&mut sim.rand));

      // Extract all current TableSchemas
      let full_db_schema = sim.full_db_schema();
      let cur_tables = full_db_schema.table_generation.static_snapshot_read(&timestamp);
      let mut table_schemas = BTreeMap::<TablePath, &TableSchema>::new();
      let mut sharding_config = BTreeMap::<TablePath, &Vec<(TabletKeyRange, TabletGroupId)>>::new();
      for (table_path, full_gen) in cur_tables {
        let gen = full_gen.0.clone();
        let table_schema = full_db_schema.db_schema.get(&(table_path.clone(), gen)).unwrap();
        table_schemas.insert(table_path.clone(), table_schema);
        let sharding = full_db_schema.sharding_config.get(&(table_path.clone(), full_gen)).unwrap();
        sharding_config.insert(table_path, sharding);
      }

      // Construct a Query generation ctx
      let mut gen_ctx = QueryGenCtx {
        rand: &mut rand,
        timestamp,
        table_schemas: &table_schemas,
        sharding_config: &sharding_config,
        tablet_address_config: &full_db_schema.tablet_address_config,
      };

      // Used to wrap the Queries that `QueryGenCtx` generates.
      enum GenQuery {
        DDLQuery(String),
        Sharding(msg::ShardingOp),
        Query(String),
      }

      // Generate a Query. Try 5 times to generate it
      let mut maybe_query_data: Option<GenQuery> = None;
      for _ in 0..5 {
        maybe_query_data = if iteration < NUM_WARMUP_ITERATIONS / 2 {
          // For the first few iterations, we create some tables.
          gen_ctx.mk_create_table().map(|x| GenQuery::DDLQuery(x))
        } else if iteration < NUM_WARMUP_ITERATIONS {
          // For the next few iterations, we populate that ables.
          gen_ctx.mk_insert().map(|x| GenQuery::Query(x))
        } else {
          // Otherwise, we randomly generate any type of query chosen using a hard-coded
          // distribution. We define the distribution as a constant vector that specifies
          // the relative probabilities.
          let dist: [u32; 11] =
            [5, 4, 5, 5, 30, 20, 5, 40, 15, 10, if do_sharding { 10 } else { 0 }];

          // Select an `idx` into dist based on its probability distribution.
          let mut i: u32 = gen_ctx.rand.next_u32() % dist.iter().sum::<u32>();
          let mut idx: usize = 0;
          while idx < dist.len() && i >= dist[idx] {
            i -= dist[idx];
            idx += 1;
          }

          // Map the selected `idx` to the corresponding query.
          match idx {
            0 => gen_ctx.mk_create_table().map(|x| GenQuery::DDLQuery(x)),
            1 => gen_ctx.mk_drop_table().map(|x| GenQuery::DDLQuery(x)),
            2 => gen_ctx.mk_add_col().map(|x| GenQuery::DDLQuery(x)),
            3 => gen_ctx.mk_drop_col().map(|x| GenQuery::DDLQuery(x)),
            4 => gen_ctx.mk_insert().map(|x| GenQuery::Query(x)),
            5 => gen_ctx.mk_update().map(|x| GenQuery::Query(x)),
            6 => gen_ctx.mk_delete().map(|x| GenQuery::Query(x)),
            7 => gen_ctx.mk_select().map(|x| GenQuery::Query(x)),
            8 => gen_ctx.mk_multi_stage().map(|x| GenQuery::Query(x)),
            9 => gen_ctx.mk_advanced_query().map(|x| GenQuery::Query(x)),
            10 => gen_ctx.mk_sharding_query().map(|x| GenQuery::Sharding(x)),
            _ => panic!(),
          }
        };

        if maybe_query_data.is_some() {
          break;
        }
      }

      // Construct a request and populate `req_map`
      let request_id = mk_rid(&mut sim.rand);
      let client_idx = sim.rand.next_u32() as usize % client_eids.len();
      let client_eid = client_eids.get(client_idx).unwrap();

      match maybe_query_data {
        None => {}
        Some(GenQuery::DDLQuery(query)) => {
          let perform = msg::PerformExternalDDLQuery {
            sender_eid: client_eid.clone(),
            request_id: request_id.clone(),
            query,
          };
          req_map
            .get_mut(client_eid)
            .unwrap()
            .insert(request_id.clone(), Request::DDLQuery(perform.clone()));

          // Record the leadership of the Master PaxosGroup and then send the `perform` to it.
          let gid = PaxosGroupId::Master;
          let cur_lid = sim.leader_map.get(&gid).unwrap().clone();
          req_lid_map.insert(request_id, (client_eid.clone(), gid, cur_lid.clone()));
          sim.add_msg(
            msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
              msg::MasterExternalReq::PerformExternalDDLQuery(perform),
            )),
            client_eid,
            &cur_lid.eid,
          );
        }
        Some(GenQuery::Sharding(sharding_op)) => {
          let perform = msg::PerformExternalSharding {
            sender_eid: client_eid.clone(),
            request_id: request_id.clone(),
            op: sharding_op,
          };
          req_map
            .get_mut(client_eid)
            .unwrap()
            .insert(request_id.clone(), Request::Sharding(perform.clone()));

          // Record the leadership of the Master PaxosGroup and then send the `perform` to it.
          let gid = PaxosGroupId::Master;
          let cur_lid = sim.leader_map.get(&gid).unwrap().clone();
          req_lid_map.insert(request_id, (client_eid.clone(), gid, cur_lid.clone()));
          sim.add_msg(
            msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
              msg::MasterExternalReq::PerformExternalSharding(perform),
            )),
            client_eid,
            &cur_lid.eid,
          );
        }
        Some(GenQuery::Query(query)) => {
          let perform = msg::PerformExternalQuery {
            sender_eid: client_eid.clone(),
            request_id: request_id.clone(),
            query,
          };
          req_map
            .get_mut(client_eid)
            .unwrap()
            .insert(request_id.clone(), Request::Query(perform.clone()));

          // Choose a SlaveGroupId, record its leadership, and then send the `perform` to it.
          let slave_idx = sim.rand.next_u32() as usize % sids.len();
          let sid = sids.get(slave_idx).unwrap();
          let gid = sid.to_gid();
          let cur_lid = sim.leader_map.get(&gid).unwrap().clone();
          req_lid_map.insert(request_id, (client_eid.clone(), gid, cur_lid.clone()));
          sim.add_msg(
            msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
              msg::SlaveExternalReq::PerformExternalQuery(perform),
            )),
            client_eid,
            &cur_lid.eid,
          );
        }
      }
    } else {
      // Otherwise, send out a cancellation. We try 5 times to find a request whose
      // original Leadership still seems to be alive.
      for _ in 0..5 {
        // First, pick a random pending query to cancel.
        let idx = sim.rand.next_u32() as usize % req_lid_map.len();
        let (request_id, (client_eid, gid, lid)) = read_index(&req_lid_map, idx).unwrap();
        // Check if the Leadership is still the same to avoid sending trivial cancellations
        // (recall that a cancellation arriving at a non-leader is simply ignored).
        if lid == sim.leader_map.get(gid).unwrap() {
          if &PaxosGroupId::Master == gid {
            // Here, we send a DDL query cancellation.
            sim.add_msg(
              msg::NetworkMessage::Master(msg::MasterMessage::MasterExternalReq(
                msg::MasterExternalReq::CancelExternalDDLQuery(msg::CancelExternalDDLQuery {
                  sender_eid: client_eid.clone(),
                  request_id: request_id.clone(),
                }),
              )),
              client_eid,
              &lid.eid,
            );
          } else {
            // Here, we send a query cancellation.
            sim.add_msg(
              msg::NetworkMessage::Slave(msg::SlaveMessage::SlaveExternalReq(
                msg::SlaveExternalReq::CancelExternalQuery(msg::CancelExternalQuery {
                  sender_eid: client_eid.clone(),
                  request_id: request_id.clone(),
                }),
              )),
              client_eid,
              &lid.eid,
            );
          }
          break;
        }
      }
    }

    // Potentially start a Leadership change in a node by randomly choosing a PaxosGroupId.
    // Note: that we only do this if the PaxosGroups have more than 1 element.
    if num_paxos_nodes > 1 {
      if !sim.is_leadership_changing() {
        if sim.rand.next_u32() % 5 == 0 {
          let gid = gids.get(sim.rand.next_u32() as usize % gids.len()).unwrap();
          sim.start_leadership_change(gid.clone());
        }
      }
    }

    // Simulate for at-most 50 ms at a time
    let sim_duration = sim.rand.next_u32() % 50;
    sim.simulate_n_ms(sim_duration);

    // Move any new responses to to `req_res_map`.
    for (eid, responses) in sim.remove_all_responses() {
      for res in responses {
        let external = cast!(msg::NetworkMessage::External, res).unwrap();
        let request_id = match &external {
          msg::ExternalMessage::ExternalQuerySuccess(success) => &success.request_id,
          msg::ExternalMessage::ExternalQueryAborted(aborted) => &aborted.request_id,
          msg::ExternalMessage::ExternalDDLQuerySuccess(success) => &success.request_id,
          msg::ExternalMessage::ExternalDDLQueryAborted(aborted) => &aborted.request_id,
          msg::ExternalMessage::ExternalShardingSuccess(success) => &success.request_id,
          msg::ExternalMessage::ExternalShardingAborted(aborted) => &aborted.request_id,
          msg::ExternalMessage::ExternalMetadataResponse(_) => panic!(),
        };

        let req = req_map.get_mut(&eid).unwrap().remove(request_id).unwrap();
        req_lid_map.remove(request_id);
        req_res_map.insert(request_id.clone(), (req, external));
      }
    }
  }

  // Iterate for some time limit to receiving responses
  const RESPONSE_TIME_LIMIT: u128 = 10000;
  let end_time = sim.true_timestamp().add(mk_t(RESPONSE_TIME_LIMIT));
  while sim.true_timestamp() < &end_time {
    // Next, we see if all unresponded requests have an old Leadership or not.
    let mut all_old = true;
    for (_, (_, gid, lid)) in &req_lid_map {
      let cur_lid = sim.leader_map.get(&gid).unwrap();
      if cur_lid.gen <= lid.gen {
        all_old = false;
        break;
      }
    }

    // Break out if we are done.
    if all_old {
      break;
    } else {
      // Otherwise, simulate for 50ms.
      sim.simulate_n_ms(50);

      // Move any new responses to to `req_res_map`.
      for (eid, responses) in sim.remove_all_responses() {
        for res in responses {
          let external = cast!(msg::NetworkMessage::External, res).unwrap();
          let request_id = match &external {
            msg::ExternalMessage::ExternalQuerySuccess(success) => &success.request_id,
            msg::ExternalMessage::ExternalQueryAborted(aborted) => &aborted.request_id,
            msg::ExternalMessage::ExternalDDLQuerySuccess(success) => &success.request_id,
            msg::ExternalMessage::ExternalDDLQueryAborted(aborted) => &aborted.request_id,
            msg::ExternalMessage::ExternalShardingSuccess(success) => &success.request_id,
            msg::ExternalMessage::ExternalShardingAborted(aborted) => &aborted.request_id,
            msg::ExternalMessage::ExternalMetadataResponse(_) => panic!(),
          };

          let req = req_map.get_mut(&eid).unwrap().remove(request_id).unwrap();
          req_lid_map.remove(request_id);
          req_res_map.insert(request_id.clone(), (req, external));
        }
      }
    }
  }

  // Simulate more for a cooldown time and verify that all resources get cleaned up.
  if !simulate_until_clean(&mut sim, 10000) {
    sim.check_resources_clean(true); // Fail, pointing to where the leak is.
  }

  // Verify the responses are correct

  let mut full_req_res_map = BTreeMap::<RequestId, (Request, Option<msg::ExternalMessage>)>::new();
  // Add in the request-response pairs that responded.
  for (rid, (req, res)) in req_res_map {
    full_req_res_map.insert(rid, (req, Some(res)));
  }
  // Add in the requests that did not respond.
  for (_, rid_req_map) in req_map {
    for (rid, req) in rid_req_map {
      // Assert mutual exclusion.
      assert!(!full_req_res_map.insert(rid, (req, None)).is_some());
    }
  }

  let success_write_reqs = sim.get_success_write_reqs();
  if let Some(res) = verify_req_res(&mut sim.rand, full_req_res_map, success_write_reqs) {
    // Count the number of Leadership changes.
    let mut num_leadership_changes = 0;
    for (_, lid) in &sim.leader_map {
      num_leadership_changes += lid.gen.0;
    }

    let (master_reconfig_count, slave_reconfig_count) = sim.get_num_reconfigs();

    let mut select_stats_strs = Vec::<String>::new();
    for (i, stat) in res.stats_ncte_1stage.into_iter().enumerate() {
      select_stats_strs.push(format!(
        "# Select {}CTE1Stage: {}, Avg. Selected Rows: {:?}",
        i,
        stat.num_elems,
        stat.avg()
      ));
    }
    let select_stats_str = select_stats_strs.join("\n       ");

    w.println(format!(
      "Test 'test_all_paxos_parallel' Passed! Replay time taken: {duration:?}ms.
       Total Queries: {total}, Succeeded: {succeeded}, Leadership Changes: {lid_changes}, 
       # Master Reconfigs: {master_reconfig_count}, # Slave Reconfigs: {slave_reconfig_count},
       {select_stats_str}
       # Multi-Stage: {num_multi_stage},
       # Query Cancels: {queries_cancelled}, # DDL Query Cancels: {ddl_queries_cancelled},
       # Sharding Success: {sharding_success}, # Sharding Aborted: {sharding_aborted}",
      duration = res.replay_duration.time_ms,
      total = res.total_queries,
      succeeded = res.successful_queries,
      lid_changes = num_leadership_changes,
      master_reconfig_count = master_reconfig_count,
      slave_reconfig_count = slave_reconfig_count,
      select_stats_str = select_stats_str,
      num_multi_stage = res.num_multi_stage,
      queries_cancelled = res.queries_cancelled,
      ddl_queries_cancelled = res.ddl_queries_cancelled,
      sharding_success = res.sharding_success,
      sharding_aborted = res.sharding_aborted,
    ));
    Some(sim.get_stats().clone())
  } else {
    w.println(format!("Skipped Test 'test_all_paxos_parallel' due to Timestamp Conflict"));
    None
  }
}
