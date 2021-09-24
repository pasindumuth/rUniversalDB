use crate::col_usage::{
  collect_select_subqueries, collect_top_level_cols, nodes_external_cols,
  nodes_external_trans_tables, ColUsagePlanner,
};
use crate::common::{lookup_pos, mk_qid, IOTypes, NetworkOut, OrigP, QueryESResult, QueryPlan};
use crate::expression::{is_true, EvalError};
use crate::gr_query_es::{GRExecutionS, GRQueryConstructorView, GRQueryES, GRQueryPlan};
use crate::model::common::{
  proc, ColName, ColType, ColValN, ContextRow, ContextSchema, Gen, TableView, Timestamp,
  TransTableName,
};
use crate::model::common::{Context, QueryId, QueryPath, TierMap, TransTableLocationPrefix};
use crate::model::message as msg;
use crate::server::{
  evaluate_super_simple_select, mk_eval_error, CommonQuery, ContextConstructor, LocalTable,
  ServerContext,
};
use crate::tablet::{
  compute_contexts, Executing, QueryReplanningSqlView, SingleSubqueryStatus, SubqueryFinished,
  SubqueryPending,
};
use crate::trans_table_read_es::TransTableAction::Wait;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::ops::Deref;
use std::rc::Rc;

pub trait TransTableSource {
  fn get_instance(&self, prefix: &TransTableName, idx: usize) -> &TableView;
  fn get_schema(&self, prefix: &TransTableName) -> Vec<ColName>;
}

#[derive(Debug)]
pub enum TransExecutionS {
  Start,
  GossipDataWaiting,
  Executing(Executing),
  Done,
}

#[derive(Debug)]
pub struct TransTableReadES {
  pub root_query_path: QueryPath,
  pub location_prefix: TransTableLocationPrefix,
  pub context: Rc<Context>,

  // Fields needed for responding.
  pub sender_path: QueryPath,
  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,

  // Dynamically evolving fields.
  pub new_rms: HashSet<QueryPath>,
  pub state: TransExecutionS,

  // Convenience fields
  pub timestamp: Timestamp, // The timestamp read from the GRQueryES
}

pub enum TransTableAction {
  /// This tells the parent Server to wait.
  Wait,
  /// This tells the parent Server to perform subqueries.
  SendSubqueries(Vec<GRQueryES>),
  /// Indicates the ES succeeded with the given result.
  Success(QueryESResult),
  /// Indicates the ES failed with a QueryError.
  QueryError(msg::QueryError),
}

// -----------------------------------------------------------------------------------------------
//  LocalTable
// -----------------------------------------------------------------------------------------------

struct TransLocalTable<'a, SourceT: TransTableSource> {
  /// The TransTableSource the ES is operating on.
  trans_table_source: &'a SourceT,
  trans_table_name: &'a TransTableName,
  /// The schema read TransTable.
  schema: Vec<ColName>,
}

impl<'a, SourceT: TransTableSource> TransLocalTable<'a, SourceT> {
  fn new(
    trans_table_source: &'a SourceT,
    trans_table_name: &'a TransTableName,
  ) -> TransLocalTable<'a, SourceT> {
    TransLocalTable {
      trans_table_source,
      trans_table_name,
      schema: trans_table_source.get_schema(trans_table_name),
    }
  }
}

impl<'a, SourceT: TransTableSource> LocalTable for TransLocalTable<'a, SourceT> {
  fn contains_col(&self, col: &ColName) -> bool {
    self.schema.contains(col)
  }

  fn get_rows(
    &self,
    parent_context_schema: &ContextSchema,
    parent_context_row: &ContextRow,
    col_names: &Vec<ColName>,
  ) -> Result<Vec<(Vec<ColValN>, u64)>, EvalError> {
    // First, we look up the TransTableInstance
    let trans_table_name_pos = parent_context_schema
      .trans_table_context_schema
      .iter()
      .position(|prefix| &prefix.trans_table_name == self.trans_table_name)
      .unwrap();
    let trans_table_instance_pos =
      parent_context_row.trans_table_context_row.get(trans_table_name_pos).unwrap();
    let trans_table_instance =
      self.trans_table_source.get_instance(&self.trans_table_name, *trans_table_instance_pos);

    // Next, we select the desired columns and compress them before returning it.
    let mut sub_view = TableView::new(col_names.clone());
    for (row, count) in &trans_table_instance.rows {
      let mut new_row = Vec::<ColValN>::new();
      for col in col_names {
        let pos = trans_table_instance.col_names.iter().position(|cur_col| cur_col == col).unwrap();
        new_row.push(row.get(pos).unwrap().clone());
      }
      sub_view.add_row_multi(new_row, *count);
    }

    Ok(sub_view.rows.into_iter().collect())
  }
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl TransTableReadES {
  pub fn start<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    self.start_trans_table_read_es(ctx, trans_table_source)
  }

  /// Constructs and returns subqueries.
  fn start_trans_table_read_es<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    assert!(matches!(&self.state, &TransExecutionS::Start));
    // Here, we first construct all of the subquery Contexts using the
    // ContextConstructor, and then we construct GRQueryESs.

    // Compute children.
    let mut children = Vec::<(Vec<ColName>, Vec<TransTableName>)>::new();
    for child in &self.query_plan.col_usage_node.children {
      children.push((nodes_external_cols(child), nodes_external_trans_tables(child)));
    }

    // Create the child context. Recall that we are able to unwrap `compute_contexts`
    // for the case TransTables since there is no KeyBound Computation.
    let trans_table_name = &self.location_prefix.trans_table_name;
    let local_table = TransLocalTable::new(trans_table_source, trans_table_name);
    let child_contexts = compute_contexts(self.context.deref(), local_table, children).unwrap();

    // Finally, compute the GRQueryESs.
    let subquery_view = GRQueryConstructorView {
      root_query_path: &self.root_query_path,
      timestamp: &self.timestamp,
      sql_query: &self.sql_query,
      query_plan: &self.query_plan,
      query_id: &self.query_id,
      context: &self.context,
    };
    let mut gr_query_ess = Vec::<GRQueryES>::new();
    for (subquery_idx, child_context) in child_contexts.into_iter().enumerate() {
      gr_query_ess.push(subquery_view.mk_gr_query_es(
        mk_qid(&mut ctx.rand),
        Rc::new(child_context),
        subquery_idx,
      ));
    }

    if gr_query_ess.is_empty() {
      // Since there are no subqueries, we can go straight to finishing the ES.
      self.finish_trans_table_read_es(ctx, trans_table_source)
    } else {
      // Here, we have computed all GRQueryESs, and we can now add them to
      // `subquery_status`.
      let mut subqueries = Vec::<SingleSubqueryStatus>::new();
      for gr_query_es in &gr_query_ess {
        subqueries.push(SingleSubqueryStatus::Pending(SubqueryPending {
          context: gr_query_es.context.clone(),
          query_id: gr_query_es.query_id.clone(),
        }));
      }

      // Move the ES to the Executing state.
      self.state = TransExecutionS::Executing(Executing {
        completed: 0,
        subqueries,
        row_region: vec![], // This doesn't make sense for TransTables...
      });

      // Return the subqueries so that they can be driven from the parent Server.
      TransTableAction::SendSubqueries(gr_query_ess)
    }
  }

  /// This is can be called both for if a subquery fails, or if there is a LateralError
  /// due to the ES owning the TransTable disappears. This simply responds to the sender
  /// and Exits and Clean Ups this ES.
  pub fn handle_internal_query_error<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    query_error: msg::QueryError,
  ) -> TransTableAction {
    self.exit_and_clean_up(ctx);
    TransTableAction::QueryError(query_error)
  }

  /// Handles a Subquery completing
  pub fn handle_subquery_done<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut ServerContext<T>,
    trans_table_source: &SourceT,
    subquery_id: QueryId,
    subquery_new_rms: HashSet<QueryPath>,
    (_, table_views): (Vec<ColName>, Vec<TableView>),
  ) -> TransTableAction {
    // Add the subquery results into the TableReadES.
    self.new_rms.extend(subquery_new_rms);
    let executing_state = cast!(TransExecutionS::Executing, &mut self.state).unwrap();
    let subquery_idx = executing_state.find_subquery(&subquery_id).unwrap();
    let single_status = executing_state.subqueries.get_mut(subquery_idx).unwrap();
    let context = &cast!(SingleSubqueryStatus::Pending, single_status).unwrap().context.clone();
    *single_status = SingleSubqueryStatus::Finished(SubqueryFinished {
      context: context.clone(),
      result: table_views,
    });
    executing_state.completed += 1;

    // If all subqueries have been evaluated, finish the TransTableReadES
    // and respond to the client.
    let num_subqueries = executing_state.subqueries.len();
    if executing_state.completed < num_subqueries {
      TransTableAction::Wait
    } else {
      self.finish_trans_table_read_es(ctx, trans_table_source)
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  fn finish_trans_table_read_es<T: IOTypes, SourceT: TransTableSource>(
    &mut self,
    _: &mut ServerContext<T>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    let executing_state = cast!(TransExecutionS::Executing, &mut self.state).unwrap();

    // Compute children.
    let mut children = Vec::<(Vec<ColName>, Vec<TransTableName>)>::new();
    let mut subquery_results = Vec::<Vec<TableView>>::new();
    for single_status in &executing_state.subqueries {
      let result = cast!(SingleSubqueryStatus::Finished, single_status).unwrap();
      let context_schema = &result.context.context_schema;
      children
        .push((context_schema.column_context_schema.clone(), context_schema.trans_table_names()));
      subquery_results.push(result.result.clone());
    }

    // Create the ContextConstructor.
    let context_constructor = ContextConstructor::new(
      self.context.context_schema.clone(),
      TransLocalTable::new(trans_table_source, &self.location_prefix.trans_table_name),
      children,
    );

    // These are all of the `ColNames` we need in order to evaluate the Select.
    let mut top_level_cols_set = HashSet::<ColName>::new();
    top_level_cols_set.extend(collect_top_level_cols(&self.sql_query.selection));
    top_level_cols_set.extend(self.sql_query.projection.clone());
    let top_level_col_names = Vec::from_iter(top_level_cols_set.into_iter());

    // Finally, iterate over the Context Rows of the subqueries and compute the final values.
    let mut res_table_views = Vec::<TableView>::new();
    for _ in 0..self.context.context_rows.len() {
      res_table_views.push(TableView::new(self.sql_query.projection.clone()));
    }

    let eval_res = context_constructor.run(
      &self.context.context_rows,
      top_level_col_names.clone(),
      &mut |context_row_idx: usize,
            top_level_col_vals: Vec<ColValN>,
            contexts: Vec<(ContextRow, usize)>,
            count: u64| {
        // First, we extract the subquery values using the child Context indices.
        let mut subquery_vals = Vec::<TableView>::new();
        for (subquery_idx, (_, child_context_idx)) in contexts.iter().enumerate() {
          let val = subquery_results.get(subquery_idx).unwrap().get(*child_context_idx).unwrap();
          subquery_vals.push(val.clone());
        }

        // Now, we evaluate all expressions in the SQL query and amend the
        // result to this TableView (if the WHERE clause evaluates to true).
        let evaluated_select = evaluate_super_simple_select(
          &self.sql_query,
          &top_level_col_names,
          &top_level_col_vals,
          &subquery_vals,
        )?;
        if is_true(&evaluated_select.selection)? {
          // This means that the current row should be selected for the result. Thus, we take
          // the values of the project columns and insert it into the appropriate TableView.
          let mut res_row = Vec::<ColValN>::new();
          for res_col_name in &self.sql_query.projection {
            let idx = top_level_col_names.iter().position(|k| res_col_name == k).unwrap();
            res_row.push(top_level_col_vals.get(idx).unwrap().clone());
          }

          res_table_views[context_row_idx].add_row_multi(res_row, count);
        };
        Ok(())
      },
    );

    if let Err(eval_error) = eval_res {
      self.state = TransExecutionS::Done;
      return TransTableAction::QueryError(mk_eval_error(eval_error));
    }

    // Signal Success and return the data.
    self.state = TransExecutionS::Done;
    TransTableAction::Success(QueryESResult {
      result: (self.sql_query.projection.clone(), res_table_views),
      new_rms: self.new_rms.iter().cloned().collect(),
    })
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut ServerContext<T>) {
    match &self.state {
      TransExecutionS::Start => {}
      TransExecutionS::GossipDataWaiting => {}
      TransExecutionS::Executing(executing) => {
        // Here, we need to cancel every Subquery.
        for single_status in &executing.subqueries {
          match single_status {
            SingleSubqueryStatus::LockingSchemas(_) => panic!(),
            SingleSubqueryStatus::PendingReadRegion(_) => panic!(),
            SingleSubqueryStatus::Pending(_) => {}
            SingleSubqueryStatus::Finished(_) => {}
          }
        }
      }
      TransExecutionS::Done => {}
    }
    self.state = TransExecutionS::Done
  }

  /// Get the `TransTableLocationPrefix` of this ES.
  pub fn location_prefix(&self) -> &TransTableLocationPrefix {
    &self.location_prefix
  }

  /// Get the `QueryPath` of the sender of this ES.
  pub fn sender_path(&self) -> QueryPath {
    self.sender_path.clone()
  }

  /// Get the `QueryId` of the sender of this ES.
  pub fn query_id(&self) -> QueryId {
    self.query_id.clone()
  }
}
