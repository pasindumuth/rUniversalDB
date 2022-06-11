use crate::common::{ColName, TablePath, TransTableName};
use crate::master_query_planning_es::{ColUsageErrorTrait, DBSchemaView, ReqTablePresenceError};
use crate::message as msg;
use crate::sql_ast::{iast, proc};
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;

#[path = "test/query_converter_test.rs"]
pub mod query_converter_test;

pub fn convert_to_msquery<ErrorT: ReqTablePresenceError, ViewT: DBSchemaView<ErrorT = ErrorT>>(
  view: &mut ViewT,
  mut query: iast::Query,
) -> Result<proc::MSQuery, ErrorT> {
  if !validate_under_query(&query) {
    // TODO: do properly.
    return Err(ErrorT::mk_error(TablePath("First validation Error".to_string())));
  }

  // Add aliases
  process_under_query(&mut query);

  // Rename TransTables
  let mut ctx = RenameContext { trans_table_map: BTreeMap::new(), counter: 0 };
  rename_under_query(&mut ctx, &mut query);

  // Rename Aliases
  let mut ctx = AliasRenameContext { alias_rename_map: BTreeMap::new(), counter: ctx.counter };
  if let Err(e) = alias_rename_under_query(&mut ctx, (), &mut query) {
    return Err(ErrorT::mk_error(TablePath(format!("{:#?}", e)))); // TODO: do properly.
  }

  // Resolve Columns
  let mut resolver =
    ColResolver { col_usage_map: Default::default(), trans_table_map: Default::default(), view };
  let (_, unresolved) = resolver.resolve_cols_under_query(&mut query)?;
  if !unresolved.free_cols.is_empty() || !unresolved.qualified_cols.is_empty() {
    // TODO: do properly.
    return Err(ErrorT::mk_error(TablePath(
      "Some ColumnRefs do not refer to a real column".to_string(),
    )));
  }

  // Convert to MSQUery

  // Next, we flatten the `renamed_query` to produce an MSQuery. Since we renamed
  // all TransTable references, this won't change the semantics.
  let mut ctx = ConversionContext { col_usage_map: resolver.col_usage_map, counter: ctx.counter };
  match ctx.flatten_top_level_query(&query) {
    Ok(ms_query) => Ok(ms_query),
    Err(e) => {
      return Err(ErrorT::mk_error(TablePath(format!("{:#?}", e)))); // TODO: do properly.
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Validation
// -----------------------------------------------------------------------------------------------

/// Iterates through every Join Tree (i.e. `from` clause) and performs
/// various validations:
///   1. Checks that any Lateral Derived Tables are not on the left of a JOIN.
///   2. Checks that every Derived Table (in the JoinLeafs) have an alias.
///   3. Checks that every JoinLeaf has a unique JoinLeaf Name (JLN) in the Join Tree.
fn validate_under_query(query: &iast::Query) -> bool {
  fn validate_under_expr(expr: &iast::ValExpr) -> bool {
    match expr {
      iast::ValExpr::ColumnRef { .. } => true,
      iast::ValExpr::UnaryExpr { expr, .. } => validate_under_expr(expr),
      iast::ValExpr::BinaryExpr { left, right, .. } => {
        validate_under_expr(left) && validate_under_expr(right)
      }
      iast::ValExpr::Value { .. } => true,
      iast::ValExpr::Subquery { query } => validate_under_query(query),
    }
  }

  // Check that Join Trees under the Derived Tables in the `join_node` are also valid.
  fn validate_under_join_tree(join_node: &iast::JoinNode) -> bool {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        validate_under_join_tree(&inner.left)
          && validate_under_join_tree(&inner.right)
          && validate_under_expr(&inner.on)
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if let iast::JoinNodeSource::DerivedTable { query, .. } = &leaf.source {
          validate_under_query(query)
        } else {
          true
        }
      }
    }
  }

  for (_, child_query) in &query.ctes {
    if !validate_under_query(child_query) {
      return false;
    }
  }

  match &query.body {
    iast::QueryBody::Query(child_query) => {
      if !validate_under_query(child_query) {
        return false;
      }
    }
    iast::QueryBody::SuperSimpleSelect(select) => {
      // Validate the JoinTree without validating child queries within.
      if !validate_join_tree(&select.from) {
        return false;
      }

      // Validate Projection Clause
      match &select.projection {
        iast::SelectClause::SelectList(select_list) => {
          for (select_item, _) in select_list {
            match select_item {
              iast::SelectItem::ValExpr(val_expr) => {
                if !validate_under_expr(val_expr) {
                  return false;
                }
              }
              iast::SelectItem::UnaryAggregate(unary_agg) => {
                if !validate_under_expr(&unary_agg.expr) {
                  return false;
                }
              }
            }
          }
        }
        iast::SelectClause::Wildcard => {}
      }

      // Validate Where Clause
      if !validate_under_expr(&select.selection) {
        return false;
      }

      // Validate child queries within the Join Tree
      if !validate_under_join_tree(&select.from) {
        return false;
      }
    }
    iast::QueryBody::Update(update) => {
      for (_, expr) in &update.assignments {
        if !validate_under_expr(expr) {
          return false;
        }
      }

      if !validate_under_expr(&update.selection) {
        return false;
      }
    }
    iast::QueryBody::Insert(insert) => {
      for row in &insert.values {
        for val in row {
          if !validate_under_expr(val) {
            return false;
          }
        }
      }
    }
    iast::QueryBody::Delete(delete) => {
      if !validate_under_expr(&delete.selection) {
        return false;
      }
    }
  };

  true
}

/// Run all validations for a Join Tree.
fn validate_join_tree(join_node: &iast::JoinNode) -> bool {
  validate_lateral(join_node) && validate_aliases(join_node)
}

/// Check that there are no left Lateral Derived Tables in `join_node` without
/// digging into the subqueries.
fn validate_lateral(join_node: &iast::JoinNode) -> bool {
  fn validate_lateral_r(is_left: bool, join_node: &iast::JoinNode) -> bool {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        validate_lateral_r(true, &inner.left) && validate_lateral_r(false, &inner.right)
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if let iast::JoinNodeSource::DerivedTable { lateral, .. } = &leaf.source {
          !(*lateral && is_left)
        } else {
          true
        }
      }
    }
  }

  // We pass in `true` for the case that `from` is just a `JoinLeaf`. This checks
  // to make sure that `lateral` is `false` in this case.
  validate_lateral_r(true, join_node)
}

/// Ensure that every JoinLeaf has a JoinLeaf Name (JLN) by making sure ever Derived
/// Table has an alias, and makes sure every JLN is unique.
fn validate_aliases(join_node: &iast::JoinNode) -> bool {
  fn validate_aliases_r<'a>(
    seen_jlns: &mut BTreeSet<&'a String>,
    join_node: &'a iast::JoinNode,
  ) -> bool {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        validate_aliases_r(seen_jlns, &inner.left) && validate_aliases_r(seen_jlns, &inner.right)
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if let Some(jln) = leaf.join_leaf_name() {
          if seen_jlns.contains(jln) {
            false
          } else {
            seen_jlns.insert(jln);
            true
          }
        } else {
          // If the JLN does not exist, then we return false.
          false
        }
      }
    }
  }

  validate_aliases_r(&mut BTreeSet::new(), join_node)
}

// -----------------------------------------------------------------------------------------------
//  Ensure Aliases Present
// -----------------------------------------------------------------------------------------------

/// For every JoinLeaf, add an alias containing the JLN if there is no alias present.
fn process_under_query(query: &mut iast::Query) {
  fn process_under_expr(expr: &mut iast::ValExpr) {
    match expr {
      iast::ValExpr::ColumnRef { .. } => {}
      iast::ValExpr::UnaryExpr { expr, .. } => process_under_expr(expr),
      iast::ValExpr::BinaryExpr { left, right, .. } => {
        process_under_expr(left);
        process_under_expr(right);
      }
      iast::ValExpr::Value { .. } => {}
      iast::ValExpr::Subquery { query } => process_under_query(query),
    }
  }

  // Check that Join Trees under the Derived Tables in the `join_node` are also valid.
  fn process_under_join_tree(join_node: &mut iast::JoinNode) {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        process_under_join_tree(&mut inner.left);
        process_under_join_tree(&mut inner.right);
        process_under_expr(&mut inner.on);
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if leaf.alias.is_none() {
          // By now, `join_leaf_name` will surely be present.
          let new_alias = leaf.join_leaf_name().unwrap().clone();
          leaf.alias.replace(new_alias);
        }
        if let iast::JoinNodeSource::DerivedTable { query, .. } = &mut leaf.source {
          process_under_query(query);
        }
      }
    }
  }

  for (_, child_query) in &mut query.ctes {
    process_under_query(child_query);
  }

  match &mut query.body {
    iast::QueryBody::Query(child_query) => process_under_query(child_query),
    iast::QueryBody::SuperSimpleSelect(select) => {
      // Process Join Tree
      process_under_join_tree(&mut select.from);

      // Process Projection Clause
      match &mut select.projection {
        iast::SelectClause::SelectList(select_list) => {
          for (select_item, _) in select_list {
            match select_item {
              iast::SelectItem::ValExpr(val_expr) => {
                process_under_expr(val_expr);
              }
              iast::SelectItem::UnaryAggregate(unary_agg) => {
                process_under_expr(&mut unary_agg.expr);
              }
            }
          }
        }
        iast::SelectClause::Wildcard => {}
      }

      // Process Where Clause
      process_under_expr(&mut select.selection);
    }
    iast::QueryBody::Update(update) => {
      if update.table.alias.is_none() {
        update.table.alias = Some(update.table.source_ref.clone());
      }

      for (_, expr) in &mut update.assignments {
        process_under_expr(expr);
      }

      process_under_expr(&mut update.selection);
    }
    iast::QueryBody::Insert(insert) => {
      if insert.table.alias.is_none() {
        insert.table.alias = Some(insert.table.source_ref.clone());
      }

      for row in &mut insert.values {
        for val in row {
          process_under_expr(val);
        }
      }
    }
    iast::QueryBody::Delete(delete) => {
      if delete.table.alias.is_none() {
        delete.table.alias = Some(delete.table.source_ref.clone());
      }

      process_under_expr(&mut delete.selection);
    }
  };
}

// -----------------------------------------------------------------------------------------------
//  Utilities
// -----------------------------------------------------------------------------------------------

/// Make a unique name for the TransTable
fn unique_tt_name(counter: &mut u32, trans_table_name: &String) -> String {
  *counter += 1;
  format!("tt\\{}\\{}", *counter - 1, trans_table_name)
}

/// Make a unique name for the TransTable
fn unique_alias_name(counter: &mut u32, table_name: &String) -> String {
  *counter += 1;
  format!("ali\\{}\\{}", *counter - 1, table_name)
}

/// Check if a `table_name` is a TransTable, assuming that it would already have been made unique.
fn to_source(table_name: &String, alias: String) -> proc::GeneralSource {
  if table_name.len() >= 3 && &table_name[..3] == "tt\\" {
    proc::GeneralSource::TransTableName {
      trans_table_name: TransTableName(table_name.clone()),
      alias,
    }
  } else {
    proc::GeneralSource::TablePath { table_path: TablePath(table_name.clone()), alias }
  }
}

fn push_rename(
  rename_stack_map: &mut BTreeMap<String, Vec<String>>,
  old_name: &String,
  new_name: String,
) {
  if let Some(rename_stack) = rename_stack_map.get_mut(old_name) {
    rename_stack.push(new_name);
  } else {
    rename_stack_map.insert(old_name.clone(), vec![new_name]);
  }
}

fn pop_rename(rename_stack_map: &mut BTreeMap<String, Vec<String>>, old_name: &String) {
  if let Some(rename_stack) = rename_stack_map.get_mut(old_name) {
    rename_stack.pop();
    if rename_stack.is_empty() {
      rename_stack_map.remove(old_name);
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Rename TransTables
// -----------------------------------------------------------------------------------------------

struct RenameContext {
  /// This stays unmutated across a function call.
  trans_table_map: BTreeMap<String, Vec<String>>,
  /// This is incremented.
  counter: u32,
}

fn rename_query(query: &mut iast::Query) {
  let mut ctx = RenameContext { trans_table_map: Default::default(), counter: 0 };
  rename_under_query(&mut ctx, query);
}

/// Renames all TransTables to have a globally unique name. Recall that all JoinLeaf
/// aliases are present. Since we do not rename the aliases here, we also do not need
/// to rename any `ColumnRef`s here.
///
/// This functions renames the TransTables in `query` by prepending 'tt\\n\\',
/// where 'n' is a counter the increments by 1 for every TransTable. It updates
/// TransTable usages in `SuperSimpleSelects`, since writes are not allows to use
/// TransTables (which we validate during Flattening later).
///
/// Note: this function leaves the `ctx.trans_table_map` that is passed in unmodified.
fn rename_under_query(ctx: &mut RenameContext, query: &mut iast::Query) {
  fn rename_under_expr(ctx: &mut RenameContext, expr: &mut iast::ValExpr) {
    match expr {
      iast::ValExpr::ColumnRef { .. } => {}
      iast::ValExpr::UnaryExpr { expr, .. } => rename_under_expr(ctx, expr),
      iast::ValExpr::BinaryExpr { left, right, .. } => {
        rename_under_expr(ctx, left);
        rename_under_expr(ctx, right);
      }
      iast::ValExpr::Value { .. } => {}
      iast::ValExpr::Subquery { query } => rename_under_query(ctx, query),
    }
  }

  // Check that Join Trees under the Derived Tables in the `join_node` are also valid.
  fn rename_under_join_tree(ctx: &mut RenameContext, join_node: &mut iast::JoinNode) {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        rename_under_join_tree(ctx, &mut inner.left);
        rename_under_join_tree(ctx, &mut inner.right);
        rename_under_expr(ctx, &mut inner.on);
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        // If the table source is a TransTable, then we rename it.
        if let iast::JoinNodeSource::Table(table_name) = &mut leaf.source {
          if ctx.trans_table_map.contains_key(table_name) {
            let new_name = ctx.trans_table_map.get(table_name).unwrap().last().unwrap();
            *table_name = new_name.clone();
          }
        }
        if let iast::JoinNodeSource::DerivedTable { query, .. } = &mut leaf.source {
          rename_under_query(ctx, query);
        }
      }
    }
  }

  let mut trans_table_map = BTreeMap::<String, String>::new(); // Maps new name to old name.
  for (trans_table_name, cte_query) in &mut query.ctes {
    rename_under_query(ctx, cte_query); // Recurse

    // Add the TransTable name and its new name to the context.
    let renamed_trans_table_name = unique_tt_name(&mut ctx.counter, trans_table_name);
    push_rename(&mut ctx.trans_table_map, trans_table_name, renamed_trans_table_name.clone());
    trans_table_map.insert(renamed_trans_table_name.clone(), trans_table_name.clone());
    *trans_table_name = renamed_trans_table_name; // Rename the TransTable
  }

  match &mut query.body {
    iast::QueryBody::Query(child_query) => rename_under_query(ctx, child_query),
    iast::QueryBody::SuperSimpleSelect(select) => {
      // Process Join Tree
      rename_under_join_tree(ctx, &mut select.from);

      // Process Projection Clause
      match &mut select.projection {
        iast::SelectClause::SelectList(select_list) => {
          for (select_item, _) in select_list {
            match select_item {
              iast::SelectItem::ValExpr(val_expr) => {
                rename_under_expr(ctx, val_expr);
              }
              iast::SelectItem::UnaryAggregate(unary_agg) => {
                rename_under_expr(ctx, &mut unary_agg.expr);
              }
            }
          }
        }
        iast::SelectClause::Wildcard => {}
      }

      // Process Where Clause
      rename_under_expr(ctx, &mut select.selection);
    }
    iast::QueryBody::Update(update) => {
      for (_, expr) in &mut update.assignments {
        rename_under_expr(ctx, expr);
      }

      rename_under_expr(ctx, &mut update.selection);
    }
    iast::QueryBody::Insert(insert) => {
      for row in &mut insert.values {
        for val in row {
          rename_under_expr(ctx, val);
        }
      }
    }
    iast::QueryBody::Delete(delete) => {
      rename_under_expr(ctx, &mut delete.selection);
    }
  };

  // Remove the TransTables defined by this Query from the ctx.
  for (trans_table_name, _) in &query.ctes {
    let orig_name = trans_table_map.get(trans_table_name).unwrap();
    pop_rename(&mut ctx.trans_table_map, orig_name);
  }
}

// -----------------------------------------------------------------------------------------------
//  Rename all Table Aliases
// -----------------------------------------------------------------------------------------------

struct AliasRenameContext {
  /// This maps the old alias of a JoinLeaf to what it should be renamed to.
  /// The value is a `Vec` due to shadowing; at any given `ColumnRef` in the query,
  /// the new name should be the last element in the stack.
  ///
  /// This stays unmutated across a function call.
  alias_rename_map: BTreeMap<String, Vec<String>>,
  /// This is incremented monotincally.
  counter: u32,
}

fn alias_rename_query<SaltT>(query: &mut iast::Query, s: SaltT) -> Result<SaltT, String> {
  let mut ctx = AliasRenameContext { alias_rename_map: Default::default(), counter: 0 };
  alias_rename_under_query(&mut ctx, s, query)
}

/// Renames all Table aliases in the JoinLeafs. This means we also rename all
/// qualified `ColumnRef`s that used the old name.
///
/// To do this, this function simply iterates through all `ColumnRef`s, making sure
/// that every possible `table_name` that can qualify the `ColumnRef` is present in the
/// `AliasRenameContext`. If a `ColumnRef` has a `table_name` that does not exist,
/// this is a fatal error and we return an error.
///
/// This functions renames all aliases by prepending 'ali\\n\\', where 'n' is a
/// counter the increments by 1 for every TransTable.
///
/// Note: this function leaves the `ctx.alias_rename_map` that is passed in unmodified.
fn alias_rename_under_query<SaltT>(
  ctx: &mut AliasRenameContext,
  s: SaltT,
  query: &mut iast::Query,
) -> Result<SaltT, String> {
  // Basic Helpers

  // Renames the alias in all `JoinLeaf`s and creates a map that maps back to the old name.
  fn alias_rename_generation(
    ctx: &mut AliasRenameContext,
    join_node: &mut iast::JoinNode,
  ) -> BTreeMap<String, String> {
    fn alias_rename_generation_r(
      ctx: &mut AliasRenameContext,
      join_node: &mut iast::JoinNode,
      name_map: &mut BTreeMap<String, String>,
    ) {
      match join_node {
        iast::JoinNode::JoinInnerNode(inner) => {
          alias_rename_generation_r(ctx, &mut inner.left, name_map);
          alias_rename_generation_r(ctx, &mut inner.right, name_map);
        }
        iast::JoinNode::JoinLeaf(leaf) => {
          let old_name = std::mem::take(leaf.alias.as_mut().unwrap());
          let new_name = unique_alias_name(&mut ctx.counter, &old_name);
          name_map.insert(new_name.clone(), old_name);
          leaf.alias = Some(new_name);
        }
      }
    }

    let mut name_map = BTreeMap::<String, String>::new();
    alias_rename_generation_r(ctx, join_node, &mut name_map);
    name_map
  }

  // Given a `JoinNode` that was (potentially transitively) renamed with the above, this
  // adds the set of renames under this `JoinNode` to `ctx`.
  fn add_renames_in_node(
    ctx: &mut AliasRenameContext,
    name_map: &BTreeMap<String, String>,
    join_node: &iast::JoinNode,
  ) {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        add_renames_in_node(ctx, name_map, &inner.left);
        add_renames_in_node(ctx, name_map, &inner.right);
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        let new_name = leaf.alias.as_ref().unwrap();
        let orig_name = name_map.get(new_name).unwrap();
        push_rename(&mut ctx.alias_rename_map, orig_name, new_name.clone());
      }
    }
  }

  fn remove_renames_in_node(
    ctx: &mut AliasRenameContext,
    name_map: &BTreeMap<String, String>,
    join_node: &iast::JoinNode,
  ) {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        remove_renames_in_node(ctx, name_map, &inner.left);
        remove_renames_in_node(ctx, name_map, &inner.right);
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        let new_name = leaf.alias.as_ref().unwrap();
        let orig_name = name_map.get(new_name).unwrap();
        pop_rename(&mut ctx.alias_rename_map, orig_name);
      }
    }
  }

  // Creates a new name for a `table_ref` and immediately adds it to `ctx.alias_rename_map`.
  fn rename_table_ref(ctx: &mut AliasRenameContext, table_ref: &mut iast::TableRef) -> String {
    let old_name = std::mem::take(table_ref.alias.as_mut().unwrap());
    let new_name = unique_alias_name(&mut ctx.counter, &old_name);
    push_rename(&mut ctx.alias_rename_map, &old_name, new_name.clone());
    table_ref.alias = Some(new_name);
    old_name
  }

  // Rename helpers

  fn alias_rename_under_expr<SaltT>(
    ctx: &mut AliasRenameContext,
    s: SaltT,
    expr: &mut iast::ValExpr,
  ) -> Result<SaltT, String> {
    match expr {
      iast::ValExpr::ColumnRef { table_name, .. } => {
        if let Some(table_name) = table_name {
          if let Some(rename_stack) = ctx.alias_rename_map.get(table_name) {
            *table_name = rename_stack.last().unwrap().clone();
            Ok(s)
          } else {
            // If the table_name is not in scope, then this is an error.
            Err(format!("Table name {} does not exist.", table_name))
          }
        } else {
          Ok(s)
        }
      }
      iast::ValExpr::UnaryExpr { expr, .. } => alias_rename_under_expr(ctx, s, expr),
      iast::ValExpr::BinaryExpr { left, right, .. } => {
        let s = alias_rename_under_expr(ctx, s, left)?;
        alias_rename_under_expr(ctx, s, right)
      }
      iast::ValExpr::Value { .. } => Ok(s),
      iast::ValExpr::Subquery { query } => alias_rename_under_query(ctx, s, query),
    }
  }

  // This function renames all `ColumnRef`s that appears underneath the `join_node`.
  // Note: This function leaves `ctx.alias_rename_map` unmodified.
  fn alias_rename_under_join_tree<SaltT>(
    ctx: &mut AliasRenameContext,
    s: SaltT,
    name_map: &BTreeMap<String, String>,
    join_node: &mut iast::JoinNode,
  ) -> Result<SaltT, String> {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        let s = alias_rename_under_join_tree(ctx, s, name_map, &mut inner.left)?;

        // If the right child is a Lateral Derived Table, we need to add the renames
        // from the left child.
        let s = if let iast::JoinNode::JoinLeaf(iast::JoinLeaf {
          source: iast::JoinNodeSource::DerivedTable { lateral: true, .. },
          ..
        }) = inner.right.as_ref()
        {
          add_renames_in_node(ctx, name_map, &inner.left);
          let s = alias_rename_under_join_tree(ctx, s, name_map, &mut inner.right)?;
          remove_renames_in_node(ctx, name_map, &inner.left);
          s
        } else {
          s
        };

        // For the ON clause, renames from both sides must be added.
        add_renames_in_node(ctx, name_map, &inner.left);
        add_renames_in_node(ctx, name_map, &inner.right);
        let s = alias_rename_under_expr(ctx, s, &mut inner.on)?;
        remove_renames_in_node(ctx, name_map, &inner.left);
        remove_renames_in_node(ctx, name_map, &inner.right);
        Ok(s)
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if let iast::JoinNodeSource::DerivedTable { query, .. } = &mut leaf.source {
          alias_rename_under_query(ctx, s, query)
        } else {
          Ok(s)
        }
      }
    }
  }

  // Start the function

  let mut s_opt = Some(s);
  for (_, cte_query) in &mut query.ctes {
    let s = s_opt.take().unwrap();
    s_opt = Some(alias_rename_under_query(ctx, s, cte_query)?);
  }
  let s = s_opt.take().unwrap();

  match &mut query.body {
    iast::QueryBody::Query(child_query) => alias_rename_under_query(ctx, s, child_query),
    iast::QueryBody::SuperSimpleSelect(select) => {
      // First, rename all `JoinLeaf` aliases without renaming ColumnRefs
      let name_map = alias_rename_generation(ctx, &mut select.from);

      // Rename the `ColumnRef`s in the JoinTree.
      let s = alias_rename_under_join_tree(ctx, s, &name_map, &mut select.from)?;

      // Before processing the `ValExpr`s in the query, we add all renames introduced by the
      // `from` clause since they will be in scope. We also make sure to remove these afterwards.
      add_renames_in_node(ctx, &name_map, &select.from);

      // Process Projection
      let mut s_opt = Some(s);
      match &mut select.projection {
        iast::SelectClause::SelectList(select_list) => {
          for (select_item, _) in select_list {
            let s = s_opt.take().unwrap();
            s_opt = Some(match select_item {
              iast::SelectItem::ValExpr(val_expr) => alias_rename_under_expr(ctx, s, val_expr)?,
              iast::SelectItem::UnaryAggregate(unary_agg) => {
                alias_rename_under_expr(ctx, s, &mut unary_agg.expr)?
              }
            });
          }
        }
        iast::SelectClause::Wildcard => {}
      }
      let s = s_opt.take().unwrap();

      // Proces Where Clause
      let s = alias_rename_under_expr(ctx, s, &mut select.selection)?;

      remove_renames_in_node(ctx, &name_map, &select.from);
      Ok(s)
    }
    iast::QueryBody::Update(update) => {
      let old_name = rename_table_ref(ctx, &mut update.table);

      // Process Assignment
      let mut s_opt = Some(s);
      for (_, expr) in &mut update.assignments {
        let s = s_opt.take().unwrap();
        s_opt = Some(alias_rename_under_expr(ctx, s, expr)?);
      }
      let s = s_opt.take().unwrap();

      // Proces Where Clause
      let s = alias_rename_under_expr(ctx, s, &mut update.selection)?;

      pop_rename(&mut ctx.alias_rename_map, &old_name);
      Ok(s)
    }
    iast::QueryBody::Insert(insert) => {
      let old_name = rename_table_ref(ctx, &mut insert.table);

      // Process Inset Values
      let mut s_opt = Some(s);
      for row in &mut insert.values {
        for val in row {
          let s = s_opt.take().unwrap();
          s_opt = Some(alias_rename_under_expr(ctx, s, val)?);
        }
      }
      let s = s_opt.take().unwrap();

      pop_rename(&mut ctx.alias_rename_map, &old_name);
      Ok(s)
    }
    iast::QueryBody::Delete(delete) => {
      let old_name = rename_table_ref(ctx, &mut delete.table);

      // Process Inset Values
      let s = alias_rename_under_expr(ctx, s, &mut delete.selection)?;

      pop_rename(&mut ctx.alias_rename_map, &old_name);
      Ok(s)
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Column Resolution
// -----------------------------------------------------------------------------------------------

enum SchemaSource {
  /// This is used for `JoinLeaf`s that are Derived Tables and TransTables.
  StaticSchema(Vec<Option<String>>),
  /// This is used for `JoinLeaf`s that are regular Tables.
  TablePath(TablePath),
}

enum ColUsageCols {
  /// Indicates that only the listed columns for the table are used.
  Cols(Vec<String>),
  /// Indicates that all columns for the Table are used.
  /// Note: Since a schema can have duplicate column names, only `All` can
  /// simultaneously return data from such columns.  
  All,
}

struct UnresolvedColRefs<'a> {
  qualified_cols: BTreeMap<String, Vec<String>>,
  free_cols: BTreeMap<String, Vec<&'a mut Option<String>>>,
}

impl<'a> UnresolvedColRefs<'a> {
  fn new<'b>() -> UnresolvedColRefs<'b> {
    UnresolvedColRefs { qualified_cols: Default::default(), free_cols: Default::default() }
  }

  fn merge(&mut self, that: UnresolvedColRefs<'a>) {
    for (col, table_refs) in that.free_cols {
      if let Some(cur_table_refs) = self.free_cols.get_mut(&col) {
        cur_table_refs.extend(table_refs.into_iter());
      } else {
        self.free_cols.insert(col, table_refs);
      }
    }

    for (jln, cols) in that.qualified_cols {
      if let Some(cur_cols) = self.qualified_cols.get_mut(&jln) {
        cur_cols.extend(cols.into_iter());
      } else {
        self.qualified_cols.insert(jln, cols);
      }
    }
  }
}

/// `trans_table_map` maps the name of a TransTable that we have already visited
/// to the schema that we computed for it.
///
/// `col_usage_map` maps the set of columns that we need to read from each JLN.
struct ColResolver<'a, ViewT: DBSchemaView> {
  col_usage_map: BTreeMap<String, ColUsageCols>,
  trans_table_map: BTreeMap<String, Vec<Option<String>>>,

  /// DBSchema to use
  view: &'a mut ViewT,
}

impl<'b, ErrorT: ReqTablePresenceError, ViewT: DBSchemaView<ErrorT = ErrorT>>
  ColResolver<'b, ViewT>
{
  fn resolve_cols_under_query<'a>(
    &mut self,
    query: &'a mut iast::Query,
  ) -> Result<(Vec<Option<String>>, UnresolvedColRefs<'a>), ErrorT> {
    let mut unresolved = UnresolvedColRefs::<'a>::new();

    // Process CTEs
    for (trans_table_name, child_query) in &mut query.ctes {
      let (schema, cur_unresolved) = self.resolve_cols_under_query::<'a>(child_query)?;
      unresolved.merge(cur_unresolved);
      self.trans_table_map.insert(trans_table_name.clone(), schema);
    }

    match &mut query.body {
      iast::QueryBody::Query(child_query) => {
        let (schema, child_unresolved) = self.resolve_cols_under_query(child_query)?;
        unresolved.merge(child_unresolved);

        return Ok((schema, unresolved));
      }
      iast::QueryBody::SuperSimpleSelect(select) => {
        let (_, jlns, join_node_cols, mut cur_unresolved) =
          self.resolve_cols_under_join_node(&mut select.from)?;
        unresolved.merge(cur_unresolved);

        // Resolve WHERE clause
        self.process_expr(&mut unresolved, &join_node_cols, &mut select.selection)?;

        // Resolve SELECT clause
        let mut projection = Vec::<Option<String>>::new();
        match &mut select.projection {
          iast::SelectClause::SelectList(select_list) => {
            for (select_item, alias) in select_list {
              // Amend the projection schema.
              if let Some(col) = alias {
                projection.push(Some(col.clone()));
              } else if let iast::SelectItem::ValExpr(iast::ValExpr::ColumnRef {
                col_name, ..
              }) = select_item
              {
                projection.push(Some(col_name.clone()));
              } else {
                projection.push(None);
              }

              // Evaluate the Select expression:
              let expr = match select_item {
                iast::SelectItem::ValExpr(expr) => expr,
                iast::SelectItem::UnaryAggregate(expr) => &mut expr.expr,
              };
              self.process_expr(&mut unresolved, &join_node_cols, expr)?;
            }
          }
          iast::SelectClause::Wildcard => {
            for jln in &jlns {
              match join_node_cols.get(jln).unwrap() {
                SchemaSource::StaticSchema(schema) => {
                  projection.extend(schema.iter().cloned());
                }
                SchemaSource::TablePath(table_path) => {
                  for ColName(col) in self.view.get_all_cols(table_path)? {
                    projection.push(Some(col));
                  }
                }
              }

              // We also update `col_usage_nodes`.
              self.set_col_usage_all(jln);
            }
          }
        }

        return Ok((projection, unresolved));
      }
      iast::QueryBody::Update(update) => {
        let join_node_cols = self.mk_join_node_cols(&update.table);

        // Compute the schema
        let table_path = TablePath(update.table.source_ref.clone());
        let mut projection = Vec::<Option<String>>::new();
        for (ColName(col), _) in self.view.key_cols(&table_path)? {
          projection.push(Some(col.clone()));
        }
        for (col, _) in &update.assignments {
          projection.push(Some(col.clone()));
        }

        // Process WHERE
        self.process_expr(&mut unresolved, &join_node_cols, &mut update.selection)?;

        // Process SET
        for (_, expr) in &mut update.assignments {
          self.process_expr(&mut unresolved, &join_node_cols, expr)?;
        }

        return Ok((projection, unresolved));
      }
      iast::QueryBody::Insert(insert) => {
        let join_node_cols = self.mk_join_node_cols(&insert.table);

        // Compute the schema
        let projection: Vec<_> = insert.columns.iter().map(|col| Some(col.clone())).collect();

        // Process VALUES
        for row in &mut insert.values {
          for expr in row {
            self.process_expr(&mut unresolved, &join_node_cols, expr)?;
          }
        }

        return Ok((projection, unresolved));
      }
      iast::QueryBody::Delete(delete) => {
        let join_node_cols = self.mk_join_node_cols(&delete.table);

        // Process WHERE
        self.process_expr(&mut unresolved, &join_node_cols, &mut delete.selection)?;

        return Ok((vec![], unresolved));
      }
    }
  }

  /// This also verified that every TablePath in the `JoinLeaf`s that need to be present
  /// in the `DBSchemaView` actually are.
  ///
  /// The first `bool` indicates if `join_node` was a Lateral Derived Table.
  fn resolve_cols_under_join_node<'a>(
    &mut self,
    join_node: &'a mut iast::JoinNode,
  ) -> Result<(bool, Vec<String>, BTreeMap<String, SchemaSource>, UnresolvedColRefs<'a>), ErrorT>
  {
    let mut unresolved = UnresolvedColRefs::new();
    let mut join_node_cols = BTreeMap::<String, SchemaSource>::new();

    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        let (_, left_jlns, left_join_node_cols, left_unresolved) =
          self.resolve_cols_under_join_node(&mut inner.left)?;
        let (lateral, right_jlns, right_join_node_cols, mut right_unresolved) =
          self.resolve_cols_under_join_node(&mut inner.right)?;

        // Resolved ColumnRefs from Lateral Derived Tables
        join_node_cols.extend(left_join_node_cols);
        if lateral {
          self.resolve_columns(&join_node_cols, &mut right_unresolved)?;
        }

        // Resolve ON clause
        let mut on_unresolved = self.resolve_cols_under_val_expr(&mut inner.on)?;
        join_node_cols.extend(right_join_node_cols.into_iter());
        self.resolve_columns(&join_node_cols, &mut on_unresolved)?;

        // Merge data
        unresolved.merge(left_unresolved);
        unresolved.merge(right_unresolved);
        unresolved.merge(on_unresolved);

        Ok((
          false,
          left_jlns.into_iter().chain(right_jlns.into_iter()).collect(),
          join_node_cols,
          unresolved,
        ))
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        let jln = get_jln(leaf);
        let lateral = match &mut leaf.source {
          iast::JoinNodeSource::Table(table_name) => {
            // If the source is a TransTable, then it must already
            // be present in `trans_table_map`.
            if let Some(schema) = self.trans_table_map.get(table_name) {
              join_node_cols.insert(jln.clone(), SchemaSource::StaticSchema(schema.clone()));
            } else {
              let table_path = TablePath(table_name.clone());
              if !self.view.contains_table(&table_path)? {
                return Err(ErrorT::mk_error(table_path));
              } else {
                join_node_cols.insert(jln.clone(), SchemaSource::TablePath(table_path));
              }
            }
            false
          }
          iast::JoinNodeSource::DerivedTable { query, lateral } => {
            let (schema, cur_unresolved) = self.resolve_cols_under_query(query)?;
            join_node_cols.insert(jln.clone(), SchemaSource::StaticSchema(schema));
            unresolved.merge(cur_unresolved);
            *lateral
          }
        };

        Ok((lateral, vec![jln], join_node_cols, unresolved))
      }
    }
  }

  fn resolve_cols_under_val_expr<'a>(
    &mut self,
    expr: &'a mut iast::ValExpr,
  ) -> Result<UnresolvedColRefs<'a>, ErrorT> {
    let mut unresolved = UnresolvedColRefs::new();

    match expr {
      iast::ValExpr::ColumnRef { table_name, col_name } => {
        if let Some(table_name) = &table_name {
          // Amend the col_usage_map
          if let Some(cols) = unresolved.qualified_cols.get_mut(table_name) {
            cols.push(col_name.clone());
          } else {
            unresolved.qualified_cols.insert(table_name.clone(), vec![col_name.clone()]);
          }
        } else {
          // Amend unqualified if the `table_name`` is not present.
          if let Some(table_name_refs) = unresolved.free_cols.get_mut(col_name) {
            table_name_refs.push(table_name);
          } else {
            unresolved.free_cols.insert(col_name.clone(), vec![table_name]);
          }
        }
      }
      iast::ValExpr::UnaryExpr { expr, .. } => {
        unresolved.merge(self.resolve_cols_under_val_expr(expr)?);
      }
      iast::ValExpr::BinaryExpr { left, right, .. } => {
        unresolved.merge(self.resolve_cols_under_val_expr(left)?);
        unresolved.merge(self.resolve_cols_under_val_expr(right)?);
      }
      iast::ValExpr::Value { .. } => {}
      iast::ValExpr::Subquery { query, .. } => {
        unresolved.merge(self.resolve_cols_under_query(query)?.1);
      }
    }

    Ok(unresolved)
  }

  // Utils

  fn process_expr<'a>(
    &mut self,
    unresolved: &mut UnresolvedColRefs<'a>,
    join_node_cols: &BTreeMap<String, SchemaSource>,
    expr: &'a mut iast::ValExpr,
  ) -> Result<(), ErrorT> {
    let mut cur_unresolved = self.resolve_cols_under_val_expr(expr)?;
    self.resolve_columns(&join_node_cols, &mut cur_unresolved)?;
    unresolved.merge(cur_unresolved);
    Ok(())
  }

  fn mk_join_node_cols(&mut self, table_ref: &iast::TableRef) -> BTreeMap<String, SchemaSource> {
    let table_path = TablePath(table_ref.source_ref.clone());
    let mut join_node_cols = BTreeMap::<String, SchemaSource>::new();
    join_node_cols.insert(table_ref.alias.clone().unwrap(), SchemaSource::TablePath(table_path));
    join_node_cols
  }

  fn amend_col_usage(&mut self, jln: &String, col_name: String) {
    if let Some(cur_cols) = self.col_usage_map.get_mut(jln) {
      match cur_cols {
        ColUsageCols::Cols(cols) => {
          cols.push(col_name);
        }
        // Here, all columns are already included.
        ColUsageCols::All => {}
      }
    } else {
      self.col_usage_map.insert(jln.clone(), ColUsageCols::Cols(vec![col_name]));
    }
  }

  fn set_col_usage_all(&mut self, jln: &String) {
    if let Some(cur_cols) = self.col_usage_map.get_mut(jln) {
      *cur_cols = ColUsageCols::All;
    } else {
      self.col_usage_map.insert(jln.clone(), ColUsageCols::All);
    }
  }

  /// If the columns in `unqualified` appear in the `join_node_cols`,
  /// then they are resolved and the corresponding element in `self.col_usage_map`
  /// is also populated.
  fn resolve_columns<'a>(
    &mut self,
    join_node_cols: &BTreeMap<String, SchemaSource>,
    unresolved: &mut UnresolvedColRefs<'a>,
  ) -> Result<(), ErrorT> {
    // iterate through latter, lookup keys in the values of former. If present, modify latter.
    let mut resolved_free_cols = Vec::<String>::new();
    for (col_name, table_name_refs) in &mut unresolved.free_cols {
      // Search `join_node_cols` for the `col_name`, resolving it if present.
      for (jln, schema_source) in join_node_cols {
        // See if this `schema_source` contains `col_name`.
        let does_contain_col = match schema_source {
          SchemaSource::StaticSchema(schema) => {
            let mut num_matches = 0;
            for maybe_col in schema {
              if maybe_col.as_ref() == Some(col_name) {
                num_matches += 1;
              }
            }

            // If more than one element of `schema` matches `jln`, this is an
            // "ambiguous column" error.
            if num_matches > 1 {
              // TODO: do properly.
              return Err(ErrorT::mk_error(TablePath("Ambiguous Column Error".to_string())));
            } else {
              num_matches == 1
            }
          }
          SchemaSource::TablePath(table_name) => {
            self.view.contains_col(table_name, &ColName(col_name.clone()))?
          }
        };

        // Check if we have successfully resolved `col_name`.
        if does_contain_col {
          for table_name_ref in table_name_refs {
            table_name_ref.replace(jln.clone());
          }

          // Amend the col_usage_map
          self.amend_col_usage(jln, col_name.clone());

          // Mark resolved.
          resolved_free_cols.push(col_name.clone());
          break;
        }
      }
    }

    // Remove column names that were resolved successfully.
    for col_name in resolved_free_cols {
      unresolved.free_cols.remove(&col_name);
    }

    // Next, resolve the qualified columns, amending `col_usage_map` as necessary or
    // throwing an if the JLN does not have the table column.
    let mut resolved_qualified_cols = Vec::<String>::new();
    for (jln, cols) in &unresolved.qualified_cols {
      let mut resolved = false;
      if let Some(schema_source) = join_node_cols.get(jln) {
        for col in cols {
          // Verify that the `col` is in the schema, returning an error otherwise.
          if !match schema_source {
            SchemaSource::StaticSchema(schema) => schema.contains(&Some(col.clone())),
            SchemaSource::TablePath(table_path) => {
              self.view.contains_col(table_path, &ColName(col.clone()))?
            }
          } {
            return Err(ErrorT::mk_error(TablePath("Missing Column".to_string())));
          } else {
            // Otherwise, amend `col_usage_map` accordingly.
            self.amend_col_usage(jln, col.clone());

            // Mark resolved.
            resolved = true;
          }
        }
      }

      if resolved {
        resolved_qualified_cols.push(jln.clone());
      }
    }

    // Remove column names that were resolved successfully.
    for jln in resolved_qualified_cols {
      unresolved.qualified_cols.remove(&jln);
    }

    Ok(())
  }
}

// Utils

fn get_jln(leaf: &iast::JoinLeaf) -> String {
  leaf.alias.as_ref().unwrap().clone()
}

// -----------------------------------------------------------------------------------------------
//  Query to MSQuery
// -----------------------------------------------------------------------------------------------

struct ConversionContext {
  col_usage_map: BTreeMap<String, ColUsageCols>,
  counter: u32,
}

impl ConversionContext {
  /// Flattens the `query` into a into a `MSQuery`.
  fn flatten_top_level_query(
    &mut self,
    query: &iast::Query,
  ) -> Result<proc::MSQuery, msg::ExternalAbortedData> {
    let aux_table_name = unique_tt_name(&mut self.counter, &"".to_string());
    let mut ms_query = proc::MSQuery {
      trans_tables: Vec::default(),
      returning: TransTableName(aux_table_name.clone()),
    };
    self.flatten_top_level_query_r(&aux_table_name, query, &mut ms_query.trans_tables)?;
    Ok(ms_query)
  }

  /// Flattens the `query` into a into a `trans_table_map`. For the TableView
  /// produced by the query itself, create an auxiliary TransTable with the
  /// name of `assignment_name` and add it into the map as well.
  /// Note: we need `counter` because we need to create auxiliary TransTables
  /// for the query bodies.
  fn flatten_top_level_query_r(
    &mut self,
    assignment_name: &String,
    query: &iast::Query,
    trans_table_map: &mut Vec<(TransTableName, proc::MSQueryStage)>,
  ) -> Result<(), msg::ExternalAbortedData> {
    // First, have the CTEs flatten their Querys and add their TransTables to the map.
    for (trans_table_name, cte_query) in &query.ctes {
      self.flatten_top_level_query_r(trans_table_name, cte_query, trans_table_map)?;
    }

    // Then, add this QueryBody as a TransTable
    match &query.body {
      iast::QueryBody::Query(child_query) => {
        self.flatten_top_level_query_r(assignment_name, child_query, trans_table_map)
      }
      iast::QueryBody::SuperSimpleSelect(select) => {
        trans_table_map.push((
          TransTableName(assignment_name.clone()),
          proc::MSQueryStage::SuperSimpleSelect(self.flatten_select(select)?),
        ));
        Ok(())
      }
      iast::QueryBody::Update(update) => {
        let mut ms_update = proc::Update {
          table: proc::SimpleSource {
            source_ref: TablePath(update.table.source_ref.clone()),
            alias: update.table.alias.clone().unwrap(),
          },
          assignment: Vec::new(),
          selection: self.flatten_val_expr_r(&update.selection)?,
        };
        for (col_name, val_expr) in &update.assignments {
          ms_update.assignment.push((ColName(col_name.clone()), self.flatten_val_expr_r(val_expr)?))
        }
        trans_table_map
          .push((TransTableName(assignment_name.clone()), proc::MSQueryStage::Update(ms_update)));
        Ok(())
      }
      iast::QueryBody::Insert(insert) => {
        let mut ms_insert = proc::Insert {
          table: proc::SimpleSource {
            source_ref: TablePath(insert.table.source_ref.clone()),
            alias: insert.table.alias.clone().unwrap(),
          },
          columns: insert.columns.iter().map(|x| ColName(x.clone())).collect(),
          values: Vec::new(),
        };
        for row in &insert.values {
          let mut p_row = Vec::<proc::ValExpr>::new();
          for val_expr in row {
            p_row.push(self.flatten_val_expr_r(val_expr)?);
          }
          ms_insert.values.push(p_row);
        }

        trans_table_map
          .push((TransTableName(assignment_name.clone()), proc::MSQueryStage::Insert(ms_insert)));
        Ok(())
      }
      iast::QueryBody::Delete(delete) => {
        let ms_delete = proc::Delete {
          table: proc::SimpleSource {
            source_ref: TablePath(delete.table.source_ref.clone()),
            alias: delete.table.alias.clone().unwrap(),
          },
          selection: self.flatten_val_expr_r(&delete.selection)?,
        };
        trans_table_map
          .push((TransTableName(assignment_name.clone()), proc::MSQueryStage::Delete(ms_delete)));
        Ok(())
      }
    }
  }

  fn flatten_val_expr_r(
    &mut self,
    val_expr: &iast::ValExpr,
  ) -> Result<proc::ValExpr, msg::ExternalAbortedData> {
    match val_expr {
      iast::ValExpr::ColumnRef { table_name, col_name } => {
        Ok(proc::ValExpr::ColumnRef(proc::ColumnRef {
          table_name: table_name.clone().unwrap(),
          col_name: ColName(col_name.clone()),
        }))
      }
      iast::ValExpr::UnaryExpr { op, expr } => Ok(proc::ValExpr::UnaryExpr {
        op: op.clone(),
        expr: Box::new(self.flatten_val_expr_r(expr)?),
      }),
      iast::ValExpr::BinaryExpr { op, left, right } => Ok(proc::ValExpr::BinaryExpr {
        op: op.clone(),
        left: Box::new(self.flatten_val_expr_r(left)?),
        right: Box::new(self.flatten_val_expr_r(right)?),
      }),
      iast::ValExpr::Value { val } => Ok(proc::ValExpr::Value { val: val.clone() }),
      iast::ValExpr::Subquery { query } => {
        // Notice that we don't actually need anything after the backslash in the
        // new TransTable name. We only keep it for the original TransTables for
        // debugging purposes.
        let aux_table_name = unique_tt_name(&mut self.counter, &"".to_string());
        let mut gr_query = proc::GRQuery {
          trans_tables: Vec::default(),
          returning: TransTableName(aux_table_name.clone()),
        };
        self.flatten_sub_query_r(&aux_table_name, &query, &mut gr_query.trans_tables)?;
        Ok(proc::ValExpr::Subquery { query: Box::from(gr_query) })
      }
    }
  }

  fn flatten_sub_query_r(
    &mut self,
    assignment_name: &String,
    query: &iast::Query,
    trans_table_map: &mut Vec<(TransTableName, proc::GRQueryStage)>,
  ) -> Result<(), msg::ExternalAbortedData> {
    // First, have the CTEs flatten their Querys and add their TransTables to the map.
    for (trans_table_name, cte_query) in &query.ctes {
      self.flatten_sub_query_r(trans_table_name, cte_query, trans_table_map)?;
    }

    // Then, add this QueryBody as a TransTable
    match &query.body {
      iast::QueryBody::Query(child_query) => {
        self.flatten_sub_query_r(assignment_name, child_query, trans_table_map)
      }
      iast::QueryBody::SuperSimpleSelect(select) => {
        trans_table_map.push((
          TransTableName(assignment_name.clone()),
          proc::GRQueryStage::SuperSimpleSelect(self.flatten_select(select)?),
        ));
        Ok(())
      }
      iast::QueryBody::Update(_) => {
        Err(msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidUpdate))
      }
      iast::QueryBody::Insert(_) => {
        Err(msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidInsert))
      }
      iast::QueryBody::Delete(_) => {
        Err(msg::ExternalAbortedData::QueryPlanningError(msg::QueryPlanningError::InvalidDelete))
      }
    }
  }

  fn flatten_select(
    &mut self,
    select: &iast::SuperSimpleSelect,
  ) -> Result<proc::SuperSimpleSelect, msg::ExternalAbortedData> {
    let p_projection = match &select.projection {
      iast::SelectClause::SelectList(select_list) => {
        let mut p_select_list = Vec::<(proc::SelectItem, Option<ColName>)>::new();
        for (item, alias) in select_list {
          let select_item = match item {
            iast::SelectItem::ValExpr(val_expr) => {
              proc::SelectItem::ValExpr(self.flatten_val_expr_r(val_expr)?)
            }
            iast::SelectItem::UnaryAggregate(unary_agg) => {
              proc::SelectItem::UnaryAggregate(proc::UnaryAggregate {
                distinct: unary_agg.distinct,
                op: unary_agg.op.clone(),
                expr: self.flatten_val_expr_r(&unary_agg.expr)?,
              })
            }
          };
          p_select_list.push((select_item, alias.clone().map(|x| ColName(x))))
        }
        proc::SelectClause::SelectList(p_select_list)
      }
      iast::SelectClause::Wildcard => proc::SelectClause::Wildcard,
    };

    let from = match &select.from {
      iast::JoinNode::JoinLeaf(iast::JoinLeaf {
        source: iast::JoinNodeSource::Table(table_name),
        alias,
      }) => {
        // If the FROM clause is just a single table, then handle this clase differently.
        to_source(table_name, alias.clone().unwrap())
      }
      _ => proc::GeneralSource::JoinNode(self.flatten_join_node(&select.from)?),
    };

    Ok(proc::SuperSimpleSelect {
      distinct: select.distinct,
      projection: p_projection,
      from,
      selection: self.flatten_val_expr_r(&select.selection)?,
    })
  }

  /// Converts the Join Tree analogously, except the JoinLeafs are converted into GRQuerys
  fn flatten_join_node(
    &mut self,
    join_node: &iast::JoinNode,
  ) -> Result<proc::JoinNode, msg::ExternalAbortedData> {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        Ok(proc::JoinNode::JoinInnerNode(proc::JoinInnerNode {
          left: Box::new(self.flatten_join_node(&inner.left)?),
          right: Box::new(self.flatten_join_node(&inner.right)?),
          join_type: inner.join_type.clone(),
          on: self.flatten_val_expr_r(&inner.on)?,
        }))
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        // Construct GRQuery, except with a missing stage for `selection_table_name`.
        let selection_table_name = unique_tt_name(&mut self.counter, &"".to_string());
        let mut gr_query = proc::GRQuery {
          trans_tables: Vec::default(),
          returning: TransTableName(selection_table_name.clone()),
        };

        // Get the table to read from using the `source`.
        let (table_name, lateral) = match &leaf.source {
          iast::JoinNodeSource::Table(table_name) => (table_name.clone(), false),
          iast::JoinNodeSource::DerivedTable { query, lateral } => {
            let aux_table_name = unique_tt_name(&mut self.counter, &"".to_string());
            self.flatten_sub_query_r(&aux_table_name, &query, &mut gr_query.trans_tables)?;
            (aux_table_name, *lateral)
          }
        };

        // Start generating the `selection_table_name` by construct the alias.
        let alias = unique_alias_name(&mut self.counter, &"".to_string());

        // Construct projection
        let col_usage_cols = self.col_usage_map.get(leaf.alias.as_ref().unwrap()).unwrap();
        let select_clause = match col_usage_cols {
          ColUsageCols::Cols(cols) => {
            let mut select_list = Vec::<(proc::SelectItem, Option<ColName>)>::new();
            for col in cols {
              select_list.push((
                proc::SelectItem::ValExpr(proc::ValExpr::ColumnRef(proc::ColumnRef {
                  table_name: alias.clone(),
                  col_name: ColName(col.clone()),
                })),
                None,
              ))
            }
            proc::SelectClause::SelectList(select_list)
          }
          ColUsageCols::All => proc::SelectClause::Wildcard,
        };

        // Generate `selection_table_name` and add it into `gr_query`.
        gr_query.trans_tables.push((
          TransTableName(selection_table_name),
          proc::GRQueryStage::SuperSimpleSelect(proc::SuperSimpleSelect {
            distinct: false,
            projection: select_clause,
            from: to_source(&table_name, alias),
            selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
          }),
        ));

        Ok(proc::JoinNode::JoinLeaf(proc::JoinLeaf {
          alias: leaf.alias.clone().unwrap(),
          lateral,
          query: gr_query,
        }))
      }
    }
  }
}
