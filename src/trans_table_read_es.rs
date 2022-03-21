use crate::col_usage::{
  collect_top_level_cols, compute_select_schema, nodes_external_cols, nodes_external_trans_tables,
};
use crate::common::{mk_qid, CoreIOCtx, QueryESResult, QueryPlan, Timestamp};
use crate::expression::{is_true, EvalError};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::model::common::{
  proc, CQueryPath, ColName, ColValN, ContextRow, ContextSchema, TQueryPath, TableView,
  TransTableName,
};
use crate::model::common::{CTQueryPath, Context, QueryId, TransTableLocationPrefix};
use crate::model::message as msg;
use crate::server::{
  evaluate_super_simple_select, mk_eval_error, CTServerContext, ContextConstructor, LocalTable,
  ServerContextBase,
};
use crate::table_read_es::{check_gossip, fully_evaluate_select};
use crate::tablet::{
  compute_contexts, Executing, SingleSubqueryStatus, SubqueryFinished, SubqueryPending,
};
use std::collections::BTreeSet;
use std::iter::FromIterator;
use std::ops::Deref;
use std::rc::Rc;

pub trait TransTableSource {
  fn get_instance(&self, prefix: &TransTableName, idx: usize) -> &TableView;
  fn get_schema(&self, prefix: &TransTableName) -> Vec<Option<ColName>>;
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
  pub root_query_path: CQueryPath,
  pub location_prefix: TransTableLocationPrefix,
  pub context: Rc<Context>,

  // Fields needed for responding.
  pub sender_path: CTQueryPath,
  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::SuperSimpleSelect,
  pub query_plan: QueryPlan,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
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
  /// The `GeneralSource` in the Data Source of the Query.
  source: &'a proc::GeneralSource,
  /// The schema read TransTable.
  schema: Vec<Option<ColName>>,
}

impl<'a, SourceT: TransTableSource> TransLocalTable<'a, SourceT> {
  fn new(
    trans_table_source: &'a SourceT,
    trans_table_name: &'a TransTableName,
    source: &'a proc::GeneralSource,
  ) -> TransLocalTable<'a, SourceT> {
    TransLocalTable {
      trans_table_source,
      source,
      trans_table_name,
      schema: trans_table_source.get_schema(trans_table_name),
    }
  }
}

impl<'a, SourceT: TransTableSource> LocalTable for TransLocalTable<'a, SourceT> {
  fn contains_col(&self, col: &ColName) -> bool {
    self.schema.contains(&Some(col.clone()))
  }

  fn source(&self) -> &proc::GeneralSource {
    self.source
  }

  fn get_rows(
    &self,
    parent_context_schema: &ContextSchema,
    parent_context_row: &ContextRow,
    col_names: &Vec<ColName>,
  ) -> Vec<(Vec<ColValN>, u64)> {
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
    let mut sub_view =
      TableView::new(col_names.iter().cloned().map(|col| Some(col.clone())).collect());
    for (row, count) in &trans_table_instance.rows {
      let mut new_row = Vec::<ColValN>::new();
      for col in col_names {
        let pos = trans_table_instance
          .col_names
          .iter()
          .position(
            |maybe_cur_col| {
              if let Some(cur_col) = maybe_cur_col {
                cur_col == col
              } else {
                false
              }
            },
          )
          .unwrap();
        new_row.push(row.get(pos).unwrap().clone());
      }
      sub_view.add_row_multi(new_row, *count);
    }

    sub_view.rows.into_iter().collect()
  }
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl TransTableReadES {
  pub fn start<IO: CoreIOCtx, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut CTServerContext<IO>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    self.check_gossip_data(ctx, trans_table_source)
  }

  /// Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut CTServerContext<IO>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    // If the GossipData is valid, then act accordingly.
    if check_gossip(&ctx.gossip.get(), &self.query_plan) {
      // We start locking the regions.
      self.start_trans_table_read_es(ctx, trans_table_source)
    } else {
      // If not, we go to GossipDataWaiting
      self.state = TransExecutionS::GossipDataWaiting;

      // Request a GossipData from the Master to help stimulate progress.
      let sender_path = ctx.this_sid.clone();
      ctx.send_to_master(msg::MasterRemotePayload::MasterGossipRequest(msg::MasterGossipRequest {
        sender_path,
      }));

      return TransTableAction::Wait;
    }
  }

  /// Here, we GossipData gets delivered.
  pub fn gossip_data_changed<IO: CoreIOCtx, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut CTServerContext<IO>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    if let TransExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, trans_table_source)
    } else {
      // Do nothing
      TransTableAction::Wait
    }
  }

  /// Constructs and returns subqueries.
  fn start_trans_table_read_es<IO: CoreIOCtx, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut CTServerContext<IO>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    // Here, we first construct all of the subquery Contexts using the
    // ContextConstructor, and then we construct GRQueryESs.

    // Compute children.
    let mut children = Vec::<(Vec<proc::ColumnRef>, Vec<TransTableName>)>::new();
    for child in &self.query_plan.col_usage_node.children {
      children.push((nodes_external_cols(child), nodes_external_trans_tables(child)));
    }

    // Create the child context.
    let child_contexts = compute_contexts(
      self.context.deref(),
      TransLocalTable::new(
        trans_table_source,
        &self.location_prefix.trans_table_name,
        &self.query_plan.col_usage_node.source,
      ),
      children,
    );

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
        mk_qid(ctx.io_ctx.rand()),
        Rc::new(child_context),
        subquery_idx,
      ));
    }

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
    self.state = TransExecutionS::Executing(Executing { completed: 0, subqueries });

    if gr_query_ess.is_empty() {
      // Since there are no subqueries, we can go straight to finishing the ES.
      self.finish_trans_table_read_es(ctx, trans_table_source)
    } else {
      // Return the subqueries so that they can be driven from the parent Server.
      TransTableAction::SendSubqueries(gr_query_ess)
    }
  }

  /// This is can be called both for if a subquery fails, or if there is a LateralError
  /// due to the ES owning the TransTable disappears. This simply responds to the sender
  /// and Exits and Clean Ups this ES.
  pub fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CTServerContext<IO>,
    query_error: msg::QueryError,
  ) -> TransTableAction {
    self.exit_and_clean_up(ctx);
    TransTableAction::QueryError(query_error)
  }

  /// Handles a Subquery completing
  pub fn handle_subquery_done<IO: CoreIOCtx, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut CTServerContext<IO>,
    trans_table_source: &SourceT,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    (_, table_views): (Vec<Option<ColName>>, Vec<TableView>),
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
  fn finish_trans_table_read_es<IO: CoreIOCtx, SourceT: TransTableSource>(
    &mut self,
    _: &mut CTServerContext<IO>,
    trans_table_source: &SourceT,
  ) -> TransTableAction {
    let executing_state = cast!(TransExecutionS::Executing, &mut self.state).unwrap();

    // Compute children.
    let mut children = Vec::<(Vec<proc::ColumnRef>, Vec<TransTableName>)>::new();
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
      TransLocalTable::new(
        trans_table_source,
        &self.location_prefix.trans_table_name,
        &self.query_plan.col_usage_node.source,
      ),
      children,
    );

    // Evaluate
    let eval_res = fully_evaluate_select(
      context_constructor,
      &self.context.deref(),
      subquery_results,
      &self.sql_query,
    );

    match eval_res {
      Ok((select_schema, res_table_views)) => {
        // Signal Success and return the data.
        self.state = TransExecutionS::Done;
        TransTableAction::Success(QueryESResult {
          result: (select_schema, res_table_views),
          new_rms: self.new_rms.iter().cloned().collect(),
        })
      }
      Err(eval_error) => {
        self.state = TransExecutionS::Done;
        TransTableAction::QueryError(mk_eval_error(eval_error))
      }
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _: &mut CTServerContext<IO>) {
    self.state = TransExecutionS::Done
  }

  /// Get the `TransTableLocationPrefix` of this ES.
  pub fn location_prefix(&self) -> &TransTableLocationPrefix {
    &self.location_prefix
  }
}
