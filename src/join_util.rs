use crate::col_usage::{
  alias_collecting_cb, external_col_collecting_cb, external_trans_table_collecting_cb,
  gr_query_collecting_cb, trans_table_collecting_cb, QueryElement, QueryIterator,
};
use crate::common::{ColValN, Context, ContextRow, ContextSchema, TransTableName};
use crate::server::GeneralColumnRef;
use crate::sql_ast::proc;
use std::collections::{BTreeMap, BTreeSet};

// TODO: the below are used to help construct queries.
pub enum Location {
  Parent(usize),
  First(usize),
  Second(usize),
}

pub type Locations = (Vec<Location>, Vec<usize>);

pub fn make_parent_row(
  first_schema: &Vec<GeneralColumnRef>,
  first_row: &Vec<ColValN>,
  second_schema: &Vec<GeneralColumnRef>,
  second_row: &Vec<ColValN>,
  // The ultimate schema to build a row to.
  parent_schema: &Vec<GeneralColumnRef>,
) -> Vec<ColValN> {
  // Here, we include the given pair of rows in `table_view`.
  let mut joined_row = Vec::<ColValN>::new();
  'parent: for parent_general_col_ref in parent_schema {
    // Search `first_row`
    for (i, first_general_col_ref) in first_schema.iter().enumerate() {
      if parent_general_col_ref == first_general_col_ref {
        joined_row.push(first_row.get(i).unwrap().clone());
        continue 'parent;
      }
    }

    // Search `second_row`
    for (i, second_general_col_ref) in second_schema.iter().enumerate() {
      if parent_general_col_ref == second_general_col_ref {
        joined_row.push(second_row.get(i).unwrap().clone());
        continue 'parent;
      }
    }
  }
  joined_row
}

pub fn initialize_contexts(
  parent_context_schema: &ContextSchema,
  first_schema: &Vec<GeneralColumnRef>,
  second_schema: &Vec<GeneralColumnRef>,
  gr_queries: &Vec<proc::GRQuery>,
) -> (Vec<Context>, Vec<Locations>) {
  initialize_contexts_general(
    parent_context_schema,
    first_schema,
    second_schema,
    gr_queries.iter().map(|gr_query| QueryElement::GRQuery(gr_query)).collect(),
  )
}

pub fn initialize_contexts_general_once(
  parent_context_schema: &ContextSchema,
  first_schema: &Vec<GeneralColumnRef>,
  second_schema: &Vec<GeneralColumnRef>,
  elem: QueryElement,
) -> (Context, Locations) {
  let (column_context_schema, trans_tables) = compute_children_general(elem);
  let mut context_schema =
    ContextSchema { column_context_schema, trans_table_context_schema: vec![] };
  // Recall that `parent_eval_data.context` will certainly contain the
  // necessary TransTables, including the ones introduced by prior stages
  // in this GRQuery. See the start of `process_gr_query_stage`.
  for trans_table in trans_tables {
    let prefix = parent_context_schema
      .trans_table_context_schema
      .iter()
      .find(|prefix| prefix.trans_table_name == trans_table)
      .unwrap();
    context_schema.trans_table_context_schema.push(prefix.clone());
  }
  let context = Context::new(context_schema);
  let context_schema = &context.context_schema;

  let mut column_ref_locations = Vec::<Location>::new();
  let mut trans_table_locations = Vec::<usize>::new();

  // Find the `Location` of each `col_ref` in `column_context_schema`.
  'parent: for cur_col_ref in &context_schema.column_context_schema {
    // Search parents `column_context_schema`
    for (index, col_ref) in parent_context_schema.column_context_schema.iter().enumerate() {
      if col_ref == cur_col_ref {
        column_ref_locations.push(Location::Parent(index));
        continue 'parent;
      }
    }

    // Search `first_schema`
    for (index, generic_col_ref) in first_schema.iter().enumerate() {
      if let GeneralColumnRef::Named(col_ref) = generic_col_ref {
        if col_ref == cur_col_ref {
          column_ref_locations.push(Location::First(index));
          continue 'parent;
        }
      }
    }

    // Search `second_schema`
    for (index, generic_col_ref) in second_schema.iter().enumerate() {
      if let GeneralColumnRef::Named(col_ref) = generic_col_ref {
        if col_ref == cur_col_ref {
          column_ref_locations.push(Location::Second(index));
          continue 'parent;
        }
      }
    }

    // We should have found the `cur_col_ref``
    debug_assert!(false);
  }

  // Find the `Location` of each `col_ref` in `column_context_schema`.
  'parent: for cur_prefix in &context_schema.trans_table_context_schema {
    // Search parents `column_context_schema`
    for (index, prefix) in parent_context_schema.trans_table_context_schema.iter().enumerate() {
      if prefix == cur_prefix {
        trans_table_locations.push(index);
        continue 'parent;
      }
    }

    // We should have found the `cur_prefix`.
    debug_assert!(false);
  }

  // Amend `locations`.
  (context, (column_ref_locations, trans_table_locations))
}

pub fn initialize_contexts_general(
  parent_context_schema: &ContextSchema,
  first_schema: &Vec<GeneralColumnRef>,
  second_schema: &Vec<GeneralColumnRef>,
  elems: Vec<QueryElement>,
) -> (Vec<Context>, Vec<Locations>) {
  // Construct the initial Contexts, including the ContextSchemas
  let mut contexts = Vec::<Context>::new();
  let mut all_locations = Vec::<Locations>::new();
  for elem in elems {
    let (context, locations) =
      initialize_contexts_general_once(parent_context_schema, first_schema, second_schema, elem);
    contexts.push(context);
    all_locations.push(locations);
  }

  (contexts, all_locations)
}

pub fn add_vals(
  col_map: &mut BTreeMap<proc::ColumnRef, ColValN>,
  col_refs: &Vec<proc::ColumnRef>,
  col_vals: &Vec<ColValN>,
) {
  for (col_ref, col_val) in col_refs.iter().zip(col_vals.iter()) {
    col_map.insert(col_ref.clone(), col_val.clone());
  }
}

pub fn add_vals_general(
  col_map: &mut BTreeMap<proc::ColumnRef, ColValN>,
  general_col_refs: &Vec<GeneralColumnRef>,
  col_vals: &Vec<ColValN>,
) {
  for (general_col_ref, col_val) in general_col_refs.iter().zip(col_vals.iter()) {
    if let GeneralColumnRef::Named(col_ref) = general_col_ref {
      col_map.insert(col_ref.clone(), col_val.clone());
    }
  }
}

pub fn mk_context_row(
  (column_ref_locations, trans_table_locations): &Locations,
  parent_context_row: &ContextRow,
  first_row: &Vec<ColValN>,
  second_row: &Vec<ColValN>,
) -> ContextRow {
  let parent_column_context_row = &parent_context_row.column_context_row;
  let parent_trans_table_context_row = &parent_context_row.trans_table_context_row;

  // Construct a ContextRow
  let mut context_row = ContextRow::default();
  for location in column_ref_locations {
    let (row, index) = match location {
      Location::Parent(index) => (parent_column_context_row, *index),
      Location::First(index) => (first_row, *index),
      Location::Second(index) => (second_row, *index),
    };
    context_row.column_context_row.push(row.get(index).unwrap().clone());
  }
  for index in trans_table_locations {
    let idx = parent_trans_table_context_row.get(*index).unwrap().clone();
    context_row.trans_table_context_row.push(idx);
  }
  context_row
}

pub fn extract_subqueries(
  parent_inner: &proc::JoinInnerNode,
) -> (Vec<proc::GRQuery>, Vec<proc::GRQuery>) {
  // Collect all GRQuerys in the conjunctions.
  let it = QueryIterator::new_top_level();
  let mut weak_gr_queries = Vec::<proc::GRQuery>::new();
  for expr in &parent_inner.weak_conjunctions {
    it.iterate_expr(&mut gr_query_collecting_cb(&mut weak_gr_queries), expr);
  }
  let mut strong_gr_queries = Vec::<proc::GRQuery>::new();
  for expr in &parent_inner.strong_conjunctions {
    it.iterate_expr(&mut gr_query_collecting_cb(&mut strong_gr_queries), expr);
  }

  (weak_gr_queries, strong_gr_queries)
}

pub fn compute_children_general(elem: QueryElement) -> (Vec<proc::ColumnRef>, Vec<TransTableName>) {
  // Collect the external `ColumnRef`s
  let mut col_names = Vec::<proc::ColumnRef>::new();
  {
    let it = QueryIterator::new();
    let mut alias_container = BTreeSet::<String>::new();
    it.iterate_general(&mut alias_collecting_cb(&mut alias_container), elem.clone());
    let mut external_cols_set = BTreeSet::<proc::ColumnRef>::new();
    it.iterate_general(
      &mut external_col_collecting_cb(&alias_container, &mut external_cols_set),
      elem.clone(),
    );
    col_names.extend(external_cols_set.into_iter());
  }

  // Collect the external `TransTableName`s
  let mut trans_table_names = Vec::<TransTableName>::new();
  {
    let it = QueryIterator::new();
    let mut trans_table_container = BTreeSet::<TransTableName>::new();
    it.iterate_general(&mut trans_table_collecting_cb(&mut trans_table_container), elem.clone());
    let mut external_trans_table = BTreeSet::<TransTableName>::new();
    it.iterate_general(
      &mut external_trans_table_collecting_cb(&trans_table_container, &mut external_trans_table),
      elem.clone(),
    );
    trans_table_names.extend(external_trans_table.into_iter());
  }

  (col_names, trans_table_names)
}
