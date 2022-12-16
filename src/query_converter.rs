use crate::col_usage::{QueryElement, QueryElementMut, QueryIterator, QueryIteratorMut};
use crate::common::{lookup, remove_indices, ColName, ReadOnlySet, TablePath, TransTableName};
use crate::master_query_planning_es::{DBSchemaView, ErrorTrait};
use crate::message as msg;
use crate::sql_ast::{iast, proc};
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};

#[path = "test/query_converter_test.rs"]
pub mod query_converter_test;

pub fn convert_to_msquery<ErrorT: ErrorTrait + Debug, ViewT: DBSchemaView<ErrorT = ErrorT>>(
  view: &mut ViewT,
  mut query: iast::Query,
) -> Result<proc::MSQuery, ErrorT> {
  // Validate Join Trees
  validate_under_query(&query)?;

  // Add aliases
  process_under_query(&mut query);

  // Rename TransTables
  let mut ctx = RenameContext { trans_table_map: BTreeMap::new(), counter: 0 };
  rename_under_query(&mut ctx, &mut query);

  // Rename Aliases
  let mut ctx = AliasRenameContext { alias_rename_map: BTreeMap::new(), counter: ctx.counter };
  alias_rename_under_query(&mut ctx, &mut query)?;

  // Resolve Columns
  let mut ctx = ColResolver {
    col_usage_map: Default::default(),
    trans_table_map: Default::default(),
    counter: ctx.counter,
    view,
  };
  let aux_table_name = ctx.resolve_cols(&mut query)?;

  // Convert to MSQUery
  let mut ctx = ConversionContext {
    col_usage_map: ctx.col_usage_map,
    trans_table_map: ctx.trans_table_map,
    counter: ctx.counter,
    view,
  };
  ctx.flatten_top_level_query(&query, aux_table_name)
}

// -----------------------------------------------------------------------------------------------
//  Validation
// -----------------------------------------------------------------------------------------------

/// Iterates through every Join Tree (i.e. `from` clause) and performs
/// various validations:
///   1. Checks that any Lateral Derived Tables are not on the left of a JOIN.
///   2. Checks that every Derived Table (in the JoinLeafs) have an alias.
///   3. Checks that every JoinLeaf has a unique JoinLeaf Name (JLN) in the Join Tree.
fn validate_under_query<ErrorT: ErrorTrait>(query: &iast::Query) -> Result<(), ErrorT> {
  fn validate_under_expr<ErrorT: ErrorTrait>(expr: &iast::ValExpr) -> Result<(), ErrorT> {
    match expr {
      iast::ValExpr::ColumnRef { .. } => Ok(()),
      iast::ValExpr::UnaryExpr { expr, .. } => validate_under_expr(expr),
      iast::ValExpr::BinaryExpr { left, right, .. } => {
        validate_under_expr(left)?;
        validate_under_expr(right)
      }
      iast::ValExpr::Value { .. } => Ok(()),
      iast::ValExpr::Subquery { query, .. } => validate_under_query(query),
    }
  }

  // Check that Join Trees under the Derived Tables in the `join_node` are also valid.
  fn validate_under_join_tree<ErrorT: ErrorTrait>(
    join_node: &iast::JoinNode,
  ) -> Result<(), ErrorT> {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        validate_under_join_tree(&inner.left)?;
        validate_under_join_tree(&inner.right)?;
        validate_under_expr(&inner.on)
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if let iast::JoinNodeSource::DerivedTable { query, .. } = &leaf.source {
          validate_under_query(query)
        } else {
          Ok(())
        }
      }
    }
  }

  for (_, child_query) in &query.ctes {
    validate_under_query(child_query)?;
  }

  match &query.body {
    iast::QueryBody::Query(child_query) => {
      validate_under_query(child_query)?;
    }
    iast::QueryBody::Select(select) => {
      // Validate the JoinTree without validating child queries within.
      validate_join_tree(&select.from)?;

      // Validate Projection Clause
      for item in &select.projection {
        match item {
          iast::SelectItem::ExprWithAlias { item, .. } => {
            validate_under_expr(match item {
              iast::SelectExprItem::ValExpr(expr) => expr,
              iast::SelectExprItem::UnaryAggregate(unary_agg) => &unary_agg.expr,
            })?;
          }
          iast::SelectItem::Wildcard { .. } => {}
        }
      }

      // Validate Where Clause
      validate_under_expr(&select.selection)?;

      // Validate child queries within the Join Tree
      validate_under_join_tree(&select.from)?;
    }
    iast::QueryBody::Update(update) => {
      for (_, expr) in &update.assignments {
        validate_under_expr(expr)?;
      }

      validate_under_expr(&update.selection)?;
    }
    iast::QueryBody::Insert(insert) => {
      for row in &insert.values {
        for val in row {
          validate_under_expr(val)?;
        }
      }
    }
    iast::QueryBody::Delete(delete) => {
      validate_under_expr(&delete.selection)?;
    }
  };

  Ok(())
}

/// Run all validations for a Join Tree.
fn validate_join_tree<ErrorT: ErrorTrait>(join_node: &iast::JoinNode) -> Result<(), ErrorT> {
  validate_lateral(join_node)?;
  validate_aliases(join_node)
}

/// Check that there are no left Lateral Derived Tables in `join_node` without
/// digging into the subqueries.
fn validate_lateral<ErrorT: ErrorTrait>(join_node: &iast::JoinNode) -> Result<(), ErrorT> {
  fn validate_lateral_r<ErrorT: ErrorTrait>(
    parent_join_type: &iast::JoinType,
    is_left: bool,
    join_node: &iast::JoinNode,
  ) -> Result<(), ErrorT> {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        validate_lateral_r(&inner.join_type, true, &inner.left)?;
        validate_lateral_r(&inner.join_type, false, &inner.right)
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if let iast::JoinNodeSource::DerivedTable { lateral, .. } = &leaf.source {
          if *lateral {
            // If a leaf is LATERAL, the it cannot be a left child and the parent's
            // join type cannot be a RIGHT Join; that is illegal SQL.
            // TODO: technically, we *can* have RIGHT JOIN LATERAL as long as the right side
            //  does not actually any column references to the left, allowing us to handle it
            //  as a regular RIGHT JOIN. But we ignore this inconvenience for now.
            if is_left || parent_join_type == &iast::JoinType::Right {
              return Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidLateralJoin));
            }
          }
        }
        Ok(())
      }
    }
  }

  match join_node {
    iast::JoinNode::JoinInnerNode(inner) => {
      validate_lateral_r(&inner.join_type, true, &inner.left)?;
      validate_lateral_r(&inner.join_type, false, &inner.right)
    }
    iast::JoinNode::JoinLeaf(leaf) => {
      if let iast::JoinNodeSource::DerivedTable { lateral, .. } = &leaf.source {
        if *lateral {
          // If the root JoinNode is a JoinLeaf, it is not allowed to be have LATERAL.
          return Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidLateralJoin));
        }
      }
      Ok(())
    }
  }
}

/// Ensure that every JoinLeaf has a JoinLeaf Name (JLN) by making sure ever Derived
/// Table has an alias, and makes sure every JLN is unique.
fn validate_aliases<ErrorT: ErrorTrait>(join_node: &iast::JoinNode) -> Result<(), ErrorT> {
  fn validate_aliases_r<'a, ErrorT: ErrorTrait>(
    seen_jlns: &mut BTreeSet<&'a String>,
    join_node: &'a iast::JoinNode,
  ) -> Result<(), ErrorT> {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        validate_aliases_r(seen_jlns, &inner.left)?;
        validate_aliases_r(seen_jlns, &inner.right)
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if let Some(jln) = leaf.join_leaf_name() {
          if seen_jlns.contains(jln) {
            Err(ErrorT::mk_error(msg::QueryPlanningError::NonUniqueJoinLeafName))
          } else {
            seen_jlns.insert(jln);
            Ok(())
          }
        } else {
          Err(ErrorT::mk_error(msg::QueryPlanningError::NonAliasedDerivedTable))
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
      iast::ValExpr::Subquery { query, .. } => process_under_query(query),
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
    iast::QueryBody::Select(select) => {
      // Process Join Tree
      process_under_join_tree(&mut select.from);

      // Process Projection Clause
      for item in &mut select.projection {
        match item {
          iast::SelectItem::ExprWithAlias { item, .. } => {
            process_under_expr(match item {
              iast::SelectExprItem::ValExpr(expr) => expr,
              iast::SelectExprItem::UnaryAggregate(unary_agg) => &mut unary_agg.expr,
            });
          }
          iast::SelectItem::Wildcard { .. } => {}
        }
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
      iast::ValExpr::Subquery { query, .. } => rename_under_query(ctx, query),
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
    iast::QueryBody::Select(select) => {
      // Process Join Tree
      rename_under_join_tree(ctx, &mut select.from);

      // Process Projection Clause
      for item in &mut select.projection {
        match item {
          iast::SelectItem::ExprWithAlias { item, .. } => {
            rename_under_expr(
              ctx,
              match item {
                iast::SelectExprItem::ValExpr(expr) => expr,
                iast::SelectExprItem::UnaryAggregate(unary_agg) => &mut unary_agg.expr,
              },
            );
          }
          iast::SelectItem::Wildcard { .. } => {}
        }
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
fn alias_rename_under_query<ErrorT: ErrorTrait>(
  ctx: &mut AliasRenameContext,
  query: &mut iast::Query,
) -> Result<(), ErrorT> {
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

  fn alias_rename_under_expr<ErrorT: ErrorTrait>(
    ctx: &mut AliasRenameContext,
    expr: &mut iast::ValExpr,
  ) -> Result<(), ErrorT> {
    match expr {
      iast::ValExpr::ColumnRef { table_name, .. } => {
        if let Some(table_name) = table_name {
          if let Some(rename_stack) = ctx.alias_rename_map.get(table_name) {
            *table_name = rename_stack.last().unwrap().clone();
            Ok(())
          } else {
            Err(ErrorT::mk_error(msg::QueryPlanningError::NonExistentTableQualification))
          }
        } else {
          Ok(())
        }
      }
      iast::ValExpr::UnaryExpr { expr, .. } => alias_rename_under_expr(ctx, expr),
      iast::ValExpr::BinaryExpr { left, right, .. } => {
        alias_rename_under_expr(ctx, left)?;
        alias_rename_under_expr(ctx, right)
      }
      iast::ValExpr::Value { .. } => Ok(()),
      iast::ValExpr::Subquery { query, .. } => alias_rename_under_query(ctx, query),
    }
  }

  // This function renames all `ColumnRef`s that appears underneath the `join_node`.
  // Note: This function leaves `ctx.alias_rename_map` unmodified.
  fn alias_rename_under_join_tree<ErrorT: ErrorTrait>(
    ctx: &mut AliasRenameContext,
    name_map: &BTreeMap<String, String>,
    join_node: &mut iast::JoinNode,
  ) -> Result<(), ErrorT> {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        alias_rename_under_join_tree(ctx, name_map, &mut inner.left)?;

        // If the right child is a Lateral Derived Table, we need to add the renames
        // from the left child.
        if let iast::JoinNode::JoinLeaf(iast::JoinLeaf {
          source: iast::JoinNodeSource::DerivedTable { lateral: true, .. },
          ..
        }) = inner.right.as_ref()
        {
          add_renames_in_node(ctx, name_map, &inner.left);
          alias_rename_under_join_tree(ctx, name_map, &mut inner.right)?;
          remove_renames_in_node(ctx, name_map, &inner.left);
        };

        // For the ON clause, renames from both sides must be added.
        add_renames_in_node(ctx, name_map, &inner.left);
        add_renames_in_node(ctx, name_map, &inner.right);
        alias_rename_under_expr(ctx, &mut inner.on)?;
        remove_renames_in_node(ctx, name_map, &inner.left);
        remove_renames_in_node(ctx, name_map, &inner.right);
        Ok(())
      }
      iast::JoinNode::JoinLeaf(leaf) => {
        if let iast::JoinNodeSource::DerivedTable { query, .. } = &mut leaf.source {
          alias_rename_under_query(ctx, query)
        } else {
          Ok(())
        }
      }
    }
  }

  // Start the function
  for (_, cte_query) in &mut query.ctes {
    alias_rename_under_query(ctx, cte_query)?
  }

  match &mut query.body {
    iast::QueryBody::Query(child_query) => alias_rename_under_query(ctx, child_query),
    iast::QueryBody::Select(select) => {
      // First, rename all `JoinLeaf` aliases without renaming ColumnRefs
      let name_map = alias_rename_generation(ctx, &mut select.from);

      // Rename the `ColumnRef`s in the JoinTree.
      alias_rename_under_join_tree(ctx, &name_map, &mut select.from)?;

      // Before processing the `ValExpr`s in the query, we add all renames introduced by the
      // `from` clause since they will be in scope. We also make sure to remove these afterwards.
      add_renames_in_node(ctx, &name_map, &select.from);

      // Process Projection
      for item in &mut select.projection {
        match item {
          iast::SelectItem::ExprWithAlias { item, .. } => {
            alias_rename_under_expr(
              ctx,
              match item {
                iast::SelectExprItem::ValExpr(expr) => expr,
                iast::SelectExprItem::UnaryAggregate(unary_agg) => &mut unary_agg.expr,
              },
            )?;
          }
          iast::SelectItem::Wildcard { .. } => {}
        }
      }

      // Proces Where Clause
      alias_rename_under_expr(ctx, &mut select.selection)?;

      remove_renames_in_node(ctx, &name_map, &select.from);
      Ok(())
    }
    iast::QueryBody::Update(update) => {
      let old_name = rename_table_ref(ctx, &mut update.table);

      // Process Assignment
      for (_, expr) in &mut update.assignments {
        alias_rename_under_expr(ctx, expr)?;
      }

      // Proces Where Clause
      alias_rename_under_expr(ctx, &mut update.selection)?;

      pop_rename(&mut ctx.alias_rename_map, &old_name);
      Ok(())
    }
    iast::QueryBody::Insert(insert) => {
      let old_name = rename_table_ref(ctx, &mut insert.table);

      // Process Inset Values
      for row in &mut insert.values {
        for val in row {
          alias_rename_under_expr(ctx, val)?;
        }
      }

      pop_rename(&mut ctx.alias_rename_map, &old_name);
      Ok(())
    }
    iast::QueryBody::Delete(delete) => {
      let old_name = rename_table_ref(ctx, &mut delete.table);

      // Process Inset Values
      alias_rename_under_expr(ctx, &mut delete.selection)?;

      pop_rename(&mut ctx.alias_rename_map, &old_name);
      Ok(())
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Column Resolution
// -----------------------------------------------------------------------------------------------
// This is where most of the heavy lifting happens, were columns are resolved and
// where `DBSchemaView` is constructed (which, recall, forms the core of the QueryPlanning).

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
  /// Maps the `table_name`s that appear as qualifications in qualified `ColumnRef`s
  /// to all `col_name`s that appear in these `ColumnRef`s. Once the `table_name`
  /// is matched with some ancestral JLN, it is removed.
  qualified_cols: BTreeMap<String, Vec<String>>,
  /// Maps an the `col_name` of an unqualified `ColumnRef` to the missing
  /// instance of the qualification. Actually, it maps the `col_name` to
  /// *all* such missing instances of the qualification if the same `col_name` has
  /// been seen multiple times. This way, once we know figure out the JLN that
  /// Free `col_name` resolves to, we can qualify all such `ColumnRef`s conveniently
  /// (after which the `col_name` will be removed from the map).
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

struct ColResolver<'a, ViewT: DBSchemaView> {
  /// Maps each JLN to the set of columns that we need to read from it.
  col_usage_map: BTreeMap<String, ColUsageCols>,
  /// Maps the name of a TransTable that we have already visited to the schema that we
  /// computed for it.
  trans_table_map: BTreeMap<String, Vec<Option<String>>>,
  /// Counter for generating `aux_table_name`s.
  counter: u32,
  /// DBSchema to use
  view: &'a mut ViewT,
}

impl<'b, ErrorT: ErrorTrait, ViewT: DBSchemaView<ErrorT = ErrorT>> ColResolver<'b, ViewT> {
  /// The main purpose of this function is to take very unqualified column in `query`
  /// and then add a qualification to it, according to what (ancestral) table that it would
  /// refer to. In the process, this function populates `col_usage_map` for every single JLN
  /// that appears under `query`, and also, recalling that by this point, every JLN is globally
  /// unique. It also computes the schema for all TransTables under `query` and then populates
  /// `trans_table_map` with it. These maps are important productcs of this algorithm.
  ///
  /// Recall that by this point, all JLNs are unique, all TransTableNames are unique, and all
  /// qualified `ColumnRef`'s qualification (i.e. JLNs) exist. However, the resolution
  /// may still fail for qualified `ColumnRef`s if it is ambiguous (i.e. the `col_name` appears
  /// multiple times in the `table_name` (which could happen if for TransTables created
  /// by SELECT *, * , for instance)). We handle this here.
  ///
  /// This returns the auxiliary TransTable name for which the top-level QueryBody was placed in.
  /// It returns an error if any of the above fails, or if there are some unresolved `ColumnRef`s.
  fn resolve_cols(&mut self, query: &mut iast::Query) -> Result<String, ErrorT> {
    let (schema, mut unresolved) = self.resolve_cols_under_query(query)?;

    // Add the top-level schema as a TransTable as well using an auxiliary TransTable name.
    let aux_table_name = unique_tt_name(&mut self.counter, &"".to_string());
    self.trans_table_map.insert(aux_table_name.clone(), schema);

    // Check if there are any columns that were unresolved.
    if let Some(col) = if let Some(entry) = unresolved.free_cols.first_entry() {
      Some(entry.key().clone())
    } else if let Some(entry) = unresolved.qualified_cols.first_entry() {
      entry.remove().into_iter().next()
    } else {
      None
    } {
      Err(ErrorT::mk_error(msg::QueryPlanningError::NonExistentColumn(col)))
    } else {
      Ok(aux_table_name)
    }
  }

  /// Same as `resolve_cols`, except we don't expect all `ColumnRef`s to be resolved
  /// yet, and so we return them as `UnresolvedColRefs`. We also return the schema
  /// implies by the `query`.
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

        Ok((schema, unresolved))
      }
      iast::QueryBody::Select(select) => {
        let (_, jlns, join_node_cols, mut cur_unresolved) =
          self.resolve_cols_under_join_node(&mut select.from)?;
        unresolved.merge(cur_unresolved);

        // Resolve WHERE clause
        self.process_expr(&mut unresolved, &join_node_cols, &mut select.selection)?;

        // Resolve SELECT clause
        let mut projection = Vec::<Option<String>>::new();

        for item in &mut select.projection {
          match item {
            iast::SelectItem::ExprWithAlias { item, alias } => {
              // Amend the projection schema.
              if let Some(col) = alias {
                projection.push(Some(col.clone()));
              } else if let iast::SelectExprItem::ValExpr(iast::ValExpr::ColumnRef {
                col_name,
                ..
              }) = item
              {
                projection.push(Some(col_name.clone()));
              } else {
                projection.push(None);
              }

              // Evaluate the Select expression:
              let expr = match item {
                iast::SelectExprItem::ValExpr(expr) => expr,
                iast::SelectExprItem::UnaryAggregate(expr) => &mut expr.expr,
              };
              self.process_expr(&mut unresolved, &join_node_cols, expr)?;
            }
            iast::SelectItem::Wildcard { table_name } => {
              // For qualified Wildcards, add the columns from the appropriate `JoinLeaf`.
              if let Some(table_name) = table_name {
                if let Some(schema_source) = join_node_cols.get(table_name) {
                  match schema_source {
                    SchemaSource::StaticSchema(schema) => {
                      projection.extend(schema.iter().cloned());
                    }
                    SchemaSource::TablePath(table_path) => {
                      for ColName(col) in self.view.get_all_cols(table_path)? {
                        projection.push(Some(col));
                      }
                    }
                  }

                  // We also update `col_usage_map`.
                  self.set_col_usage_all(table_name);
                } else {
                  // This means that `source` does not exist in the Join Tree,
                  // so we return an error
                  return Err(ErrorT::mk_error(
                    msg::QueryPlanningError::InvalidWildcardQualification,
                  ));
                }
              } else {
                // For unqualified wildcards, add the columns from all `JoinLeaf`s.
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

                  // We also update `col_usage_map`.
                  self.set_col_usage_all(jln);
                }
              }
            }
          }
        }
        Ok((projection, unresolved))
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

        Ok((projection, unresolved))
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

        Ok((projection, unresolved))
      }
      iast::QueryBody::Delete(delete) => {
        let join_node_cols = self.mk_join_node_cols(&delete.table);

        // Process WHERE
        self.process_expr(&mut unresolved, &join_node_cols, &mut delete.selection)?;

        Ok((vec![], unresolved))
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
                return Err(ErrorT::mk_error(msg::QueryPlanningError::TablesDNE(table_path)));
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
      iast::ValExpr::Subquery { query, trans_table_name } => {
        let (schema, mut cur_unresolved) = self.resolve_cols_under_query(query)?;

        // Add the top-level schema as a TransTable as well using an auxiliary TransTable name.
        let aux_table_name = unique_tt_name(&mut self.counter, &"".to_string());
        self.trans_table_map.insert(aux_table_name.clone(), schema);
        *trans_table_name = Some(aux_table_name);

        unresolved.merge(cur_unresolved);
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
              return Err(ErrorT::mk_error(msg::QueryPlanningError::AmbiguousColumnRef));
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
            return Err(ErrorT::mk_error(msg::QueryPlanningError::NonExistentColumn(col.clone())));
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

enum SelectEnum {
  TableSelect(proc::TableSelect),
  TransTableSelect(proc::TransTableSelect),
  JoinSelect(proc::JoinSelect),
}

struct ConversionContext<'a, ViewT: DBSchemaView> {
  col_usage_map: BTreeMap<String, ColUsageCols>,
  trans_table_map: BTreeMap<String, Vec<Option<String>>>,
  counter: u32,

  /// DBSchema to use
  view: &'a mut ViewT,
}

impl<'b, ErrorT: ErrorTrait + Debug, ViewT: 'b + DBSchemaView<ErrorT = ErrorT>>
  ConversionContext<'b, ViewT>
{
  /// Transforms the `query` into a into a `MSQuery`, which differes from `query` in a few
  /// ways. First, the CTEs are flattened. Recall that this okay, since we renamed
  /// all TransTable references, and so this does not change the semantics of the query.
  /// Also, we distinguish between `Select`s that contain a JOIN, and those that do not so
  /// that they can be processed differently by the system. Finally, for `Select`s with a JOIN
  /// (i.e. `JoinSelect`s), notice that the `JoinLeaf`s are all `GRQuery`s.
  ///
  /// Recall that the difference between an `MSQuery`
  fn flatten_top_level_query(
    &mut self,
    query: &iast::Query,
    aux_table_name: String,
  ) -> Result<proc::MSQuery, ErrorT> {
    let mut ms_query = proc::MSQuery {
      trans_tables: Vec::default(),
      returning: TransTableName(aux_table_name.clone()),
    };
    self.flatten_top_level_query_r(&aux_table_name, query, &mut ms_query.trans_tables)?;
    Ok(ms_query)
  }

  /// Flattens the `query` into a `trans_table_map`. For the `TableView`
  /// produced by the query itself, we create an auxiliary TransTable with the
  /// name of `assignment_name` and add it into the map as well.
  /// Note: we need `counter` because we need to create auxiliary TransTables
  /// for the `GRQuery`s that we generate to be the leaves of the JoinTrees.
  fn flatten_top_level_query_r(
    &mut self,
    assignment_name: &String,
    query: &iast::Query,
    trans_table_map: &mut Vec<(TransTableName, proc::MSQueryStage)>,
  ) -> Result<(), ErrorT> {
    // First, have the CTEs flatten their Querys and add their TransTables to the map.
    for (trans_table_name, cte_query) in &query.ctes {
      self.flatten_top_level_query_r(trans_table_name, cte_query, trans_table_map)?;
    }

    // Then, add this QueryBody as a TransTable
    match &query.body {
      iast::QueryBody::Query(child_query) => {
        self.flatten_top_level_query_r(assignment_name, child_query, trans_table_map)
      }
      iast::QueryBody::Select(select) => {
        let ms_select = self.flatten_select(assignment_name, select)?;
        self.validate_select(&ms_select)?;
        let stage = match ms_select {
          SelectEnum::TableSelect(select) => proc::MSQueryStage::TableSelect(select),
          SelectEnum::TransTableSelect(select) => proc::MSQueryStage::TransTableSelect(select),
          SelectEnum::JoinSelect(select) => proc::MSQueryStage::JoinSelect(select),
        };
        trans_table_map.push((TransTableName(assignment_name.clone()), stage));
        Ok(())
      }
      iast::QueryBody::Update(update) => {
        let mut ms_update = proc::Update {
          table: proc::TableSource {
            table_path: TablePath(update.table.source_ref.clone()),
            alias: update.table.alias.clone().unwrap(),
          },
          assignment: Vec::new(),
          selection: self.flatten_val_expr_r(&update.selection)?,
          schema: self.compute_schema(assignment_name),
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
          table: proc::TableSource {
            table_path: TablePath(insert.table.source_ref.clone()),
            alias: insert.table.alias.clone().unwrap(),
          },
          columns: insert.columns.iter().map(|x| ColName(x.clone())).collect(),
          values: Vec::new(),
          schema: self.compute_schema(assignment_name),
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
          table: proc::TableSource {
            table_path: TablePath(delete.table.source_ref.clone()),
            alias: delete.table.alias.clone().unwrap(),
          },
          selection: self.flatten_val_expr_r(&delete.selection)?,
          schema: self.compute_schema(assignment_name),
        };
        trans_table_map
          .push((TransTableName(assignment_name.clone()), proc::MSQueryStage::Delete(ms_delete)));
        Ok(())
      }
    }
  }

  fn flatten_val_expr_r(&mut self, val_expr: &iast::ValExpr) -> Result<proc::ValExpr, ErrorT> {
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
      iast::ValExpr::Subquery { query, trans_table_name } => {
        // Notice that we don't actually need anything after the backslash in the
        // new TransTable name. We only keep it for the original TransTables for
        // debugging purposes.
        let aux_table_name = trans_table_name.as_ref().unwrap();
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
  ) -> Result<(), ErrorT> {
    // First, have the CTEs flatten their Querys and add their TransTables to the map.
    for (trans_table_name, cte_query) in &query.ctes {
      self.flatten_sub_query_r(trans_table_name, cte_query, trans_table_map)?;
    }

    // Then, add this QueryBody as a TransTable
    match &query.body {
      iast::QueryBody::Query(child_query) => {
        self.flatten_sub_query_r(assignment_name, child_query, trans_table_map)
      }
      iast::QueryBody::Select(select) => {
        let ms_select = self.flatten_select(assignment_name, select)?;
        self.validate_select(&ms_select)?;
        let stage = match ms_select {
          SelectEnum::TableSelect(select) => proc::GRQueryStage::TableSelect(select),
          SelectEnum::TransTableSelect(select) => proc::GRQueryStage::TransTableSelect(select),
          SelectEnum::JoinSelect(select) => proc::GRQueryStage::JoinSelect(select),
        };
        trans_table_map.push((TransTableName(assignment_name.clone()), stage));
        Ok(())
      }
      iast::QueryBody::Update(_) => Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidUpdate)),
      iast::QueryBody::Insert(_) => Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidInsert)),
      iast::QueryBody::Delete(_) => Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidDelete)),
    }
  }

  fn flatten_select(
    &mut self,
    assignment_name: &String,
    select: &iast::Select,
  ) -> Result<SelectEnum, ErrorT> {
    let mut p_projection = Vec::<proc::SelectItem>::new();
    for item in &select.projection {
      p_projection.push(match item {
        iast::SelectItem::ExprWithAlias { item, alias } => proc::SelectItem::ExprWithAlias {
          item: match item {
            iast::SelectExprItem::ValExpr(val_expr) => {
              proc::SelectExprItem::ValExpr(self.flatten_val_expr_r(val_expr)?)
            }
            iast::SelectExprItem::UnaryAggregate(unary_agg) => {
              proc::SelectExprItem::UnaryAggregate(proc::UnaryAggregate {
                distinct: unary_agg.distinct,
                op: unary_agg.op.clone(),
                expr: self.flatten_val_expr_r(&unary_agg.expr)?,
              })
            }
          },
          alias: alias.clone().map(|x| ColName(x)),
        },
        iast::SelectItem::Wildcard { table_name } => {
          proc::SelectItem::Wildcard { table_name: table_name.clone() }
        }
      });
    }

    // A helper function to map the `JoinLeaf`s' alias of a real Table
    // to the `TablePath` of the Table.
    fn mk_jln_to_table_map_r(
      trans_table_map: &BTreeMap<String, Vec<Option<String>>>,
      node: &iast::JoinNode,
      jln_to_table_map: &mut BTreeMap<String, TablePath>,
    ) {
      match node {
        iast::JoinNode::JoinInnerNode(inner) => {
          mk_jln_to_table_map_r(trans_table_map, inner.left.deref(), jln_to_table_map);
          mk_jln_to_table_map_r(trans_table_map, inner.right.deref(), jln_to_table_map);
        }
        iast::JoinNode::JoinLeaf(leaf) => {
          // Check that the `JoinLeaf` refers to a real Table.
          if let iast::JoinNodeSource::Table(table_name) = &leaf.source {
            if !trans_table_map.contains_key(table_name) {
              // Recall that all `JoinLeaf`s have a non-None alias.
              jln_to_table_map.insert(leaf.alias.clone().unwrap(), TablePath(table_name.clone()));
            }
          }
        }
      }
    }

    // Build a map of `JoinLeaf`s' aliases to real Table `TablePath`s.
    let mut jln_to_table_map = BTreeMap::<String, TablePath>::new();
    mk_jln_to_table_map_r(&self.trans_table_map, &select.from, &mut jln_to_table_map);

    match &select.from {
      iast::JoinNode::JoinLeaf(iast::JoinLeaf {
        source: iast::JoinNodeSource::Table(table_name),
        alias,
      }) => {
        if self.trans_table_map.contains_key(table_name) {
          Ok(SelectEnum::TransTableSelect(proc::TransTableSelect {
            distinct: select.distinct,
            projection: p_projection,
            from: proc::TransTableSource {
              trans_table_name: TransTableName(table_name.clone()),
              alias: alias.clone().unwrap(),
            },
            selection: self.flatten_val_expr_r(&select.selection)?,
            schema: self.compute_schema(assignment_name),
          }))
        } else {
          Ok(SelectEnum::TableSelect(proc::TableSelect {
            distinct: select.distinct,
            projection: p_projection,
            from: proc::TableSource {
              table_path: TablePath(table_name.clone()),
              alias: alias.clone().unwrap(),
            },
            selection: self.flatten_val_expr_r(&select.selection)?,
            schema: self.compute_schema(assignment_name),
          }))
        }
      }
      iast::JoinNode::JoinLeaf(join_leaf) => {
        // Handle the case where the `JoinNodeSource` is a `DerivedTable`.
        let mut p_join_leaf = self.flatten_join_leaf(join_leaf)?;

        // Push in the `selection` of the selection into the JoinLeaf, since
        // proc::JoinSelect does not have a `selection` field.
        Self::push_expr_leaf(self.flatten_val_expr_r(&select.selection)?, &mut p_join_leaf);

        let mut p_select = proc::JoinSelect {
          distinct: select.distinct,
          projection: p_projection,
          from: proc::JoinNode::JoinLeaf(p_join_leaf),
          dependency_graph: BTreeMap::new(),
          schema: self.compute_schema(assignment_name),
        };

        // Add the dependencies for LATERAL JOINs, which is not optional.
        Self::add_dependencies_for_lateral_joins(
          String::new(),
          &mut p_select.from,
          &mut p_select.dependency_graph,
        );

        // Optimize
        self.optimize_all_levels(
          &jln_to_table_map,
          &mut p_select.from,
          &mut p_select.dependency_graph,
        );

        Ok(SelectEnum::JoinSelect(p_select))
      }
      iast::JoinNode::JoinInnerNode(inner) => {
        let mut p_inner = self.flatten_join_inner_node(inner)?;

        // Convert the WHERE clause of the query and add it as the Strong Conjunctions
        // if the top-level `p_inner`. (The fact that we can treat the WHERE clause
        // like this is a happy coincidence of the Strong/Weak Conjunction system.)
        p_inner.strong_conjunctions =
          Self::split_into_conjunctions(self.flatten_val_expr_r(&select.selection)?);

        let mut p_select = proc::JoinSelect {
          distinct: select.distinct,
          projection: p_projection,
          from: proc::JoinNode::JoinInnerNode(p_inner),
          dependency_graph: BTreeMap::new(),
          schema: self.compute_schema(assignment_name),
        };

        // Add the dependencies for LATERAL JOINs, which is not optional.
        Self::add_dependencies_for_lateral_joins(
          String::new(),
          &mut p_select.from,
          &mut p_select.dependency_graph,
        );

        // Optimize
        self.optimize_all_levels(
          &jln_to_table_map,
          &mut p_select.from,
          &mut p_select.dependency_graph,
        );

        Ok(SelectEnum::JoinSelect(p_select))
      }
    }
  }

  /// Transform `expr` into an equivalent list of conjunctions.
  fn split_into_conjunctions(expr: proc::ValExpr) -> Vec<proc::ValExpr> {
    let mut conjunctions = Vec::<proc::ValExpr>::new();
    fn split_into_conjunctions_r(expr: proc::ValExpr, conjunctions: &mut Vec<proc::ValExpr>) {
      // If `expr` is a conjunction, split it up and recurse
      if let proc::ValExpr::BinaryExpr { op: iast::BinaryOp::And, left, right } = expr {
        split_into_conjunctions_r(*left, conjunctions);
        split_into_conjunctions_r(*right, conjunctions);
      } else {
        // Otherwise, `expr` itself is an leaf of the conjunction tree, and so
        // we add it to `conjunctions.
        conjunctions.push(expr);
      }
    }

    split_into_conjunctions_r(expr, &mut conjunctions);
    return conjunctions;
  }

  /// Converts `JoinInnerNode`.
  fn flatten_join_inner_node(
    &mut self,
    inner: &iast::JoinInnerNode,
  ) -> Result<proc::JoinInnerNode, ErrorT> {
    Ok(proc::JoinInnerNode {
      left: Box::new(self.flatten_join_node(&inner.left)?),
      right: Box::new(self.flatten_join_node(&inner.right)?),
      join_type: inner.join_type.clone(),
      strong_conjunctions: Vec::new(),
      weak_conjunctions: Self::split_into_conjunctions(self.flatten_val_expr_r(&inner.on)?),
    })
  }

  /// Converts `JoinLeaf`.
  fn flatten_join_leaf(&mut self, leaf: &iast::JoinLeaf) -> Result<proc::JoinLeaf, ErrorT> {
    // A helper function that conveniently constructs the outputs with the given inputs.
    fn cols_to_schema(
      alias: &String,
      cols: &Vec<String>,
    ) -> (Vec<Option<ColName>>, Vec<proc::SelectItem>) {
      let mut schema = Vec::<Option<ColName>>::new();
      let mut select_list = Vec::<proc::SelectItem>::new();
      for col in cols {
        schema.push(Some(ColName(col.clone())));
        select_list.push(
          (proc::SelectItem::ExprWithAlias {
            item: proc::SelectExprItem::ValExpr(proc::ValExpr::ColumnRef(proc::ColumnRef {
              table_name: alias.clone(),
              col_name: ColName(col.clone()),
            })),
            alias: None,
          }),
        )
      }
      (schema, select_list)
    }

    // Next, generate the parts of a `GRQueryES` that will comprise the `JoinLeaf` that we
    // are building. We completely manufacture the final stage, and this is the alias.
    let alias = unique_alias_name(&mut self.counter, &"".to_string());

    // Build the parts of the `GRQueryES`.
    let (select, mut trans_tables, lateral) = match &leaf.source {
      iast::JoinNodeSource::Table(table_name) => {
        // Get the columns that are used by the JoinLeaf.
        let col_usage_cols = self.col_usage_map.get(leaf.alias.as_ref().unwrap()).unwrap();

        // In this case, we construct a single `GRQueryStage` that simply reads from
        // `table_name`, extracting out only the columns requested in `col_usage_cols`.
        let select = if self.trans_table_map.contains_key(table_name) {
          let trans_table_name = TransTableName(table_name.clone());
          let (schema, projection) = match col_usage_cols {
            ColUsageCols::Cols(cols) => cols_to_schema(&alias, cols),
            ColUsageCols::All => (
              self.compute_schema(table_name),
              vec![proc::SelectItem::Wildcard { table_name: None }],
            ),
          };

          proc::GRQueryStage::TransTableSelect(proc::TransTableSelect {
            distinct: false,
            projection,
            from: proc::TransTableSource { trans_table_name, alias },
            selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
            schema,
          })
        } else {
          let table_path = TablePath(table_name.clone());
          let (schema, projection) = match col_usage_cols {
            ColUsageCols::Cols(cols) => cols_to_schema(&alias, cols),
            ColUsageCols::All => {
              let mut schema = Vec::<Option<ColName>>::new();
              for col in self.view.get_all_cols(&table_path)? {
                schema.push(Some(col));
              }
              (schema, vec![proc::SelectItem::Wildcard { table_name: None }])
            }
          };

          proc::GRQueryStage::TableSelect(proc::TableSelect {
            distinct: false,
            projection,
            from: proc::TableSource { table_path, alias },
            selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
            schema,
          })
        };

        (select, vec![], false)
      }
      iast::JoinNodeSource::DerivedTable { query, lateral } => {
        // In this case, we expand out the CTEs in the `query` into stages for the
        // `GRQueryES` we are constructing.

        let mut trans_tables = Vec::<(TransTableName, proc::GRQueryStage)>::new();

        let aux_table_name = unique_tt_name(&mut self.counter, &"".to_string());
        self.flatten_sub_query_r(&aux_table_name, &query, &mut trans_tables)?;
        let aux_table_name = TransTableName(aux_table_name.clone());

        // Get the columns that are used by the JoinLeaf.
        let col_usage_cols = self.col_usage_map.get(leaf.alias.as_ref().unwrap()).unwrap();

        let (schema, projection) = match col_usage_cols {
          ColUsageCols::Cols(cols) => cols_to_schema(&alias, cols),
          ColUsageCols::All => {
            let stage = lookup(&trans_tables, &aux_table_name).unwrap();
            (stage.schema().clone(), vec![proc::SelectItem::Wildcard { table_name: None }])
          }
        };

        let select = proc::GRQueryStage::TransTableSelect(proc::TransTableSelect {
          distinct: false,
          projection,
          from: proc::TransTableSource { trans_table_name: aux_table_name, alias },
          selection: proc::ValExpr::Value { val: iast::Value::Boolean(true) },
          schema,
        });

        (select, trans_tables, *lateral)
      }
    };

    // Finally, assemble the GRQuery and return it as a JoinLeaf.
    let returning = TransTableName(unique_tt_name(&mut self.counter, &"".to_string()));
    trans_tables.push((returning.clone(), select));
    return Ok(proc::JoinLeaf {
      alias: leaf.alias.clone().unwrap(),
      lateral,
      query: proc::GRQuery { trans_tables, returning },
    });
  }

  /// Converts the Join Tree analogously, except the JoinLeafs are converted into GRQuerys
  fn flatten_join_node(&mut self, join_node: &iast::JoinNode) -> Result<proc::JoinNode, ErrorT> {
    match join_node {
      iast::JoinNode::JoinInnerNode(inner) => {
        Ok(proc::JoinNode::JoinInnerNode(self.flatten_join_inner_node(inner)?))
      }
      iast::JoinNode::JoinLeaf(leaf) => Ok(proc::JoinNode::JoinLeaf(self.flatten_join_leaf(leaf)?)),
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  Utilities
  // -----------------------------------------------------------------------------------------------

  /// Lookup the schema in the `trans_table_map`.
  fn compute_schema(&self, assignment_name: &String) -> Vec<Option<ColName>> {
    let mut schema = Vec::<Option<ColName>>::new();
    for col in self.trans_table_map.get(assignment_name).unwrap() {
      schema.push(col.as_ref().map(|val| ColName(val.clone())));
    }
    schema
  }

  /// Validates the `Select`.
  pub fn validate_select(&mut self, select: &SelectEnum) -> Result<(), ErrorT> {
    let mut val_expr_count = 0;
    let mut unary_agg_count = 0;
    let mut wildcard_count = 0;
    for item in match select {
      SelectEnum::TableSelect(select) => &select.projection,
      SelectEnum::TransTableSelect(select) => &select.projection,
      SelectEnum::JoinSelect(select) => &select.projection,
    } {
      match item {
        proc::SelectItem::ExprWithAlias { item, .. } => match item {
          proc::SelectExprItem::ValExpr(_) => {
            val_expr_count += 1;
          }
          proc::SelectExprItem::UnaryAggregate(_) => {
            unary_agg_count += 1;
          }
        },
        proc::SelectItem::Wildcard { .. } => {
          wildcard_count += 1;
        }
      }
    }

    // Recall that for now, the SELECT can only have an aggregation *only if* all
    // `SelectItem`s are aggregations.
    if unary_agg_count > 0 && (val_expr_count > 0 || wildcard_count > 0) {
      return Err(ErrorT::mk_error(msg::QueryPlanningError::InvalidSelectClause));
    }

    Ok(())
  }

  /// This function creates all the necessary dependencies between `JoinLeaf`s
  /// that are LATERAL JOINs.
  fn add_dependencies_for_lateral_joins(
    cur_path: String,
    node: &mut proc::JoinNode,
    graph: &mut proc::DependencyGraph,
  ) {
    if let proc::JoinNode::JoinInnerNode(inner) = node {
      let left_path = cur_path.clone() + "L";
      let right_path = cur_path + "R";

      // If the RHS is a JoinLeaf and the Join is a LATERAL Join, then we
      // add a dependency immediately. Notice that this would not result in cycles.
      if let proc::JoinNode::JoinLeaf(proc::JoinLeaf { lateral: true, .. }) = inner.right.deref() {
        graph.insert(left_path.clone(), right_path.clone());
      }

      // Recurse.
      Self::add_dependencies_for_lateral_joins(left_path, inner.left.deref_mut(), graph);
      Self::add_dependencies_for_lateral_joins(right_path, inner.right.deref_mut(), graph);
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  Join Optimization Utilities
  // -----------------------------------------------------------------------------------------------

  /// Copies down all conjunctions as far as possible according to the two rules.
  fn optimize_all_levels(
    &mut self,
    jln_to_table_map: &BTreeMap<String, TablePath>,
    node: &mut proc::JoinNode,
    graph: &mut proc::DependencyGraph,
  ) {
    // We loop until a call to `optimize_all_levels_once` returns false, which indicates
    // that subsequent calls will have no effect.
    while self.optimize_all_levels_once(jln_to_table_map, String::new(), node, graph) {}
  }

  /// Essentially calls `optimize_top_level_once` from the top of the Join Tree rooted
  /// at `node`, all the way to the bottom.
  ///
  /// This function returns `true` iff the `node` or `graph` were mutated.
  fn optimize_all_levels_once(
    &mut self,
    jln_to_table_map: &BTreeMap<String, TablePath>,
    cur_path: String,
    node: &mut proc::JoinNode,
    graph: &mut proc::DependencyGraph,
  ) -> bool {
    // A boolean to track whether `node` or `graph` was modified in this optimization round.
    let mut did_anything_change = false;

    // Optimize top level
    did_anything_change |=
      self.optimize_top_level_once(jln_to_table_map, cur_path.clone(), node, graph);

    if let proc::JoinNode::JoinInnerNode(inner) = node {
      // Recurse on both sides
      did_anything_change |= self.optimize_all_levels_once(
        jln_to_table_map,
        cur_path.clone() + "L",
        inner.left.deref_mut(),
        graph,
      );
      did_anything_change |= self.optimize_all_levels_once(
        jln_to_table_map,
        cur_path.clone() + "R",
        inner.right.deref_mut(),
        graph,
      );
    }

    did_anything_change
  }

  /// Copies Strong/Weak Conjunctions one level down the `node` tree from the top `node`
  /// according to the two rules, as well as the presence of LATERAL JOINs. We also build
  /// up the `graph` to express execution dependencies.
  ///
  /// This function returns `true` iff the `node` or `graph` were mutated.
  fn optimize_top_level_once(
    &mut self,
    jln_to_table_map: &BTreeMap<String, TablePath>,
    cur_path: String,
    node: &mut proc::JoinNode,
    graph: &mut proc::DependencyGraph,
  ) -> bool {
    // A boolean to track whether `node` or `graph` was modified in this optimization round.
    let mut did_anything_change = false;

    // We only need to do something if the `node` is a JoinInnerNode.
    if let proc::JoinNode::JoinInnerNode(inner) = node {
      let left_path = cur_path.clone() + "L";
      let right_path = cur_path.clone() + "R";

      // Collect the JLNs from both sides.
      let left_jlns = collect_jlns(inner.left.deref());
      let right_jlns = collect_jlns(inner.right.deref());

      // Create a helper function that pushes a conjunction down to the left or right side
      // of `inner`, making sure to add a dependency if necessary.
      enum JoinSide {
        Right,
        Left,
      }

      let join_type = &inner.join_type;
      let right = &mut inner.right.deref_mut();
      let left = &mut inner.left.deref_mut();

      // This function perfoms every necessary check to gaurantee that if the given `conjunction`
      // is pushed to the given `side`, then none of the rules are violdated.
      let mut try_push_expr =
        |conjunction: &proc::ValExpr, is_strong: bool, side: JoinSide| -> bool {
          let mut did_push_succeed = false;
          match side {
            // Try pushing the conjunction to the Right side.
            JoinSide::Right => {
              // If the conjunction is Weak, then we need to check that the JOIN is not a RIGHT JOIN.
              if !is_strong && join_type.non_right() {
                // Next, check whether a right-to-left dependency would need to be added.
                if Self::does_expr_use_any_jlns(conjunction, &left_jlns) {
                  // If so, we make sure that there is not already a left-to-right dependency.
                  if !graph.contains_key(&left_path) {
                    // Now, we are clear to push the conjunction and update the dependency.
                    Self::push_expr(conjunction.clone(), right.deref_mut());
                    graph.insert(right_path.clone(), left_path.clone());
                    did_push_succeed = true;
                  }
                } else {
                  // Otherwise, we can simply push the conjunction unconditionally.
                  Self::push_expr(conjunction.clone(), right.deref_mut());
                  did_push_succeed = true;
                }
              }

              // If the conjunction is Strong, then we need to check that the JOIN is not a LEFT JOIN.
              if is_strong && join_type.non_left() {
                // If so, we can simply push the conjunction unconditionally.
                Self::push_expr(conjunction.clone(), right.deref_mut());
                did_push_succeed = true;
              }
            }
            // Try pushing the conjunction to the Left side.
            JoinSide::Left => {
              // If the conjunction is Weak, then we need to check that the JOIN is not a LEFT JOIN.
              if !is_strong && join_type.non_left() {
                // Next, check whether a left-to-right dependency would need to be added.
                if Self::does_expr_use_any_jlns(conjunction, &right_jlns) {
                  // If so, we make sure that there is not already a right-to-left dependency.
                  if !graph.contains_key(&right_path) {
                    // Now, we are clear to push the conjunction and update the dependency.
                    Self::push_expr(conjunction.clone(), left.deref_mut());
                    graph.insert(left_path.clone(), right_path.clone());
                    did_push_succeed = true;
                  }
                } else {
                  // Otherwise, we can simply push the conjunction unconditionally.
                  Self::push_expr(conjunction.clone(), left.deref_mut());
                  did_push_succeed = true;
                }
              }

              // If the conjunction is Strong, then we need to check that the JOIN is not a RIGHT JOIN.
              if is_strong && join_type.non_right() {
                // If so, we can simply push the conjunction unconditionally.
                Self::push_expr(conjunction.clone(), left.deref_mut());
                did_push_succeed = true;
              }
            }
          };

          did_anything_change |= did_push_succeed;
          did_push_succeed
        };

      // Define function that iterates through Conjunctions and tries to push down
      // conjunctions to a side only if it does not use columns from the other side. Since
      // this would not introduce any dependencies, we can simply try to push down as many
      // conjunctions as possible. (Recall that introducing a dependency can result in a
      // very expensive query if not done judiciously.)
      let mut apply_rule_1 = |conjunctions: &mut Vec<proc::ValExpr>, is_strong: bool| {
        let mut removed_indices = BTreeSet::<usize>::new();
        for (i, conjunction) in conjunctions.iter().enumerate() {
          if !Self::does_expr_use_any_jlns(conjunction, &left_jlns) {
            // Here, pushing down the conjunction to the right would not
            // produce a dependency, so we do it.
            if try_push_expr(conjunction, is_strong, JoinSide::Right) {
              removed_indices.insert(i);
            }
          } else if !Self::does_expr_use_any_jlns(conjunction, &right_jlns) {
            // Here, pushing down the conjunction to the left would not
            // produce a dependency, so we do it.
            if try_push_expr(conjunction, is_strong, JoinSide::Left) {
              removed_indices.insert(i);
            }
          }
        }
        remove_indices(conjunctions, removed_indices);
      };

      // Apply Rule 1
      apply_rule_1(&mut inner.strong_conjunctions, true);
      apply_rule_1(&mut inner.weak_conjunctions, false);

      // Define function that iterates through Conjunctions and tries to push down conjunctions
      // that *would* introduce a dependency. We do this judiciously so that overall, the
      // dependecy would increase performance, not decrease it.
      let mut apply_rule_2 = |conjunctions: &mut Vec<proc::ValExpr>, is_strong: bool| {
        let mut removed_indices = BTreeSet::<usize>::new();
        // Iterate through the Conjunctions and apply Rule 2.
        for (i, conjunction) in conjunctions.iter().enumerate() {
          // Check that the conjunction is an equality check.
          if let proc::ValExpr::BinaryExpr { op: iast::BinaryOp::Eq, left, right } = conjunction {
            // Define function to check each of the left and right side separately.
            let mut process_expr_side = |expr_side: &proc::ValExpr| {
              // Check that the `expr_side` is a single ColumnRef
              if let proc::ValExpr::ColumnRef(col_ref) = expr_side {
                // Check that the table referred to by `col_ref` is a real Table.
                if let Some(table_path) = jln_to_table_map.get(&col_ref.table_name) {
                  // Check that the ColumnRef refers to the first key column of this real Table.
                  let key_cols = self.view.key_cols(table_path).unwrap();
                  if let Some((col_name, _)) = key_cols.first() {
                    if col_name == &col_ref.col_name {
                      if left_jlns.contains(&col_ref.table_name) {
                        // If the real Table is on the left side, we
                        // push the conjunction to the left.
                        if try_push_expr(conjunction, is_strong, JoinSide::Left) {
                          removed_indices.insert(i);
                        }
                      } else if right_jlns.contains(&col_ref.table_name) {
                        // If the real Table is on the right side, we
                        // push the conjunction to the right.
                        if try_push_expr(conjunction, is_strong, JoinSide::Right) {
                          removed_indices.insert(i);
                        }
                      }

                      // Note that the real Table might not be on any side of this JoinInnerNode,
                      // in which case we do nothing.
                    }
                  }
                }
              }
            };

            // Process each side of the conjunction separately.
            process_expr_side(left.deref());
            process_expr_side(right.deref());
          }
        }
        remove_indices(conjunctions, removed_indices);
      };

      // Apply Rule 2
      apply_rule_2(&mut inner.strong_conjunctions, true);
      apply_rule_2(&mut inner.weak_conjunctions, false);
    }

    did_anything_change
  }

  /// Checks whether `expr` has any `ColumnRef`s with `table_name` in `jlns`.
  fn does_expr_use_any_jlns(expr: &proc::ValExpr, jlns: &Vec<String>) -> bool {
    let mut expr_does_use_jlns = false;
    let query_iterator = QueryIterator::new();
    query_iterator.iterate_expr(
      &mut |elem| {
        if let QueryElement::ValExpr(proc::ValExpr::ColumnRef(col_ref)) = elem {
          if jlns.contains(&col_ref.table_name) {
            expr_does_use_jlns = true;
          }
        }
      },
      expr,
    );

    expr_does_use_jlns
  }

  /// Push the `expr` down to the `node` according to Rule 1/2.
  fn push_expr(expr: proc::ValExpr, node: &mut proc::JoinNode) {
    match node {
      proc::JoinNode::JoinInnerNode(inner) => {
        inner.strong_conjunctions.push(expr);
      }
      proc::JoinNode::JoinLeaf(leaf) => {
        Self::push_expr_leaf(expr, leaf);
      }
    }
  }

  /// Pushes down `expr` into the WHERE clause of the final `leaf`. Recall that
  /// `leaf` will be constructed from `flatten_join_leaf`. Notice for the final stage,
  /// each `SelectItem` is just a `ColumnRef` of a column in the table in the FROM clause.
  /// This makes pushing down an `expr` into a straightforward matter.
  fn push_expr_leaf(mut expr: proc::ValExpr, leaf: &mut proc::JoinLeaf) {
    // Get the final stage,
    let final_gr_query_stage = &mut leaf.query.trans_tables.last_mut().unwrap().1;
    let (alias, selection) = match final_gr_query_stage {
      proc::GRQueryStage::TableSelect(select) => (&select.from.alias, &mut select.selection),
      proc::GRQueryStage::TransTableSelect(select) => (&select.from.alias, &mut select.selection),
      proc::GRQueryStage::JoinSelect(_) => panic!(),
    };

    // Convert `ColumnRef`s in `expr` that refer to `leaf` (which will be prefixed with
    // `old_alias`) to now use the `new_alias`.
    let old_alias = leaf.alias.clone();
    let new_alias = alias.clone();
    let query_iterator = QueryIteratorMut::new();
    query_iterator.iterate_expr(
      &mut |elem| {
        if let QueryElementMut::ValExpr(proc::ValExpr::ColumnRef(col_ref)) = elem {
          if col_ref.table_name == old_alias {
            col_ref.table_name = new_alias.clone();
          }
        }
      },
      &mut expr,
    );

    // Finally, push the modified `expr` into the WHERE clause of the `final_stage` by
    // AND-ing it with the current WHERE clause.
    let cur_selection = std::mem::replace(
      selection,
      proc::ValExpr::Value { val: iast::Value::Boolean(false) }, // Some temporary sentinal.
    );
    *selection = proc::ValExpr::BinaryExpr {
      op: iast::BinaryOp::And,
      left: Box::new(expr),
      right: Box::new(cur_selection),
    };
  }
}

// -----------------------------------------------------------------------------------------------
//  General Join Utilities
// -----------------------------------------------------------------------------------------------

/// Collect the Join Leaf Names (i.e. aliases) under `node`.
pub fn collect_jlns(node: &proc::JoinNode) -> Vec<String> {
  fn collect_jlns_r(node: &proc::JoinNode, jlns: &mut Vec<String>) {
    match node {
      proc::JoinNode::JoinInnerNode(inner) => {
        collect_jlns_r(&inner.left, jlns);
        collect_jlns_r(&inner.right, jlns);
      }
      proc::JoinNode::JoinLeaf(leaf) => jlns.push(leaf.alias.clone()),
    }
  }
  let mut jlns = Vec::<String>::new();
  collect_jlns_r(node, &mut jlns);
  return jlns;
}
