use crate::common::{mk_qid, remove_item, CoreIOCtx, QueryESResult, QueryPlan, Timestamp};
use crate::common::{
  CQueryPath, ColName, ColValN, ContextRow, ContextSchema, PaxosGroupId, PaxosGroupIdTrait,
  SlaveGroupId, TQueryPath, TableView, TransTableName,
};
use crate::common::{CTQueryPath, Context, QueryId, TransTableLocationPrefix};
use crate::expression::{is_true, EvalError};
use crate::gr_query_es::{GRQueryConstructorView, GRQueryES};
use crate::message as msg;
use crate::server::{
  mk_eval_error, CTServerContext, ContextConstructor, LocalColumnRef, LocalTable, ServerContextBase,
};
use crate::sql_ast::proc;
use crate::table_read_es::{check_gossip, fully_evaluate_select};
use crate::tablet::{
  compute_children, compute_contexts, Executing, TPESAction, TPESBase, TabletContext,
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
  pub sql_query: proc::TransTableSelect,
  pub query_plan: QueryPlan,

  // Dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub state: TransExecutionS,
  pub child_queries: Vec<QueryId>,

  // Convenience fields
  pub timestamp: Timestamp, // The timestamp read from the GRQueryES
}

// -----------------------------------------------------------------------------------------------
//  LocalTable
// -----------------------------------------------------------------------------------------------

struct TransLocalTable<'a, SourceT: TransTableSource> {
  /// The TransTableSource the ES is operating on.
  trans_table_source: &'a SourceT,
  trans_table_name: &'a TransTableName,
  /// The `GeneralSource` in the Data Source of the Query.
  source: &'a proc::TransTableSource,
  /// The schema read TransTable.
  schema: Vec<Option<ColName>>,
}

impl<'a, SourceT: TransTableSource> TransLocalTable<'a, SourceT> {
  fn new(
    trans_table_source: &'a SourceT,
    trans_table_name: &'a TransTableName,
    source: &'a proc::TransTableSource,
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
  fn source_name(&self) -> &String {
    &self.source.alias
  }

  fn schema(&self) -> &Vec<Option<ColName>> {
    &self.schema
  }

  fn get_rows(
    &self,
    parent_context_schema: &ContextSchema,
    parent_context_row: &ContextRow,
    local_col_refs: &Vec<LocalColumnRef>,
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
    let mut sub_view = TableView::new();
    for (row, count) in &trans_table_instance.rows {
      let mut new_row = Vec::<ColValN>::new();
      for col_ref in local_col_refs {
        let pos = match col_ref {
          LocalColumnRef::Named(col) => self
            .schema
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
            .unwrap(),
          LocalColumnRef::Unnamed(index) => *index,
        };
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
  pub fn sender_sid(&self) -> &SlaveGroupId {
    &self.sender_path.node_path.sid
  }
  pub fn query_id(&self) -> &QueryId {
    &self.query_id
  }
  pub fn ctx_query_id(&self) -> Option<&QueryId> {
    Some(&self.location_prefix.source.query_id)
  }

  pub fn start<IO: CoreIOCtx, Ctx: CTServerContext, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    trans_table_source: &SourceT,
  ) -> TPESAction {
    self.check_gossip_data(ctx, io_ctx, trans_table_source)
  }

  /// Check if the `sharding_config` in the GossipData contains the necessary data, moving on if so.
  fn check_gossip_data<IO: CoreIOCtx, Ctx: CTServerContext, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    trans_table_source: &SourceT,
  ) -> TPESAction {
    // If the GossipData is valid, then act accordingly.
    if check_gossip(&ctx.gossip().get(), &self.query_plan) {
      // We start locking the regions.
      self.start_trans_table_read_es(ctx, io_ctx, trans_table_source)
    } else {
      // If not, we go to GossipDataWaiting
      self.state = TransExecutionS::GossipDataWaiting;

      // Request a GossipData from the Master to help stimulate progress.
      let sender_path = ctx.this_sid().clone();
      ctx.send_to_master(
        io_ctx,
        msg::MasterRemotePayload::MasterGossipRequest(msg::MasterGossipRequest { sender_path }),
      );

      return TPESAction::Wait;
    }
  }

  /// Here, we GossipData gets delivered.
  pub fn gossip_data_changed<IO: CoreIOCtx, Ctx: CTServerContext, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    trans_table_source: &SourceT,
  ) -> TPESAction {
    if let TransExecutionS::GossipDataWaiting = self.state {
      // Verify is GossipData is now recent enough.
      self.check_gossip_data(ctx, io_ctx, trans_table_source)
    } else {
      // Do nothing
      TPESAction::Wait
    }
  }

  /// Constructs and returns subqueries.
  fn start_trans_table_read_es<IO: CoreIOCtx, Ctx: CTServerContext, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    trans_table_source: &SourceT,
  ) -> TPESAction {
    // Here, we first construct all of the subquery Contexts using the
    // ContextConstructor, and then we construct GRQueryESs.

    // Compute children.
    let children = compute_children(&self.sql_query);

    // Create the child context.
    let child_contexts = compute_contexts(
      self.context.deref(),
      TransLocalTable::new(
        trans_table_source,
        &self.location_prefix.trans_table_name,
        &self.sql_query.from,
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
        mk_qid(io_ctx.rand()),
        Rc::new(child_context),
        subquery_idx,
      ));
    }

    // Move the ES to the Executing state.
    self.state = TransExecutionS::Executing(Executing::create(&gr_query_ess));
    let exec = cast!(TransExecutionS::Executing, &mut self.state).unwrap();

    // See if we are already finished (due to having no subqueries).
    if exec.is_complete() {
      self.finish_trans_table_read_es(ctx, io_ctx, trans_table_source)
    } else {
      // Otherwise, return the subqueries.
      TPESAction::SendSubqueries(gr_query_ess)
    }
  }

  /// This is can be called both for if a subquery fails, or if there is a LateralError
  /// due to the ES owning the TransTable disappears. This simply responds to the sender
  /// and Exits and Clean Ups this ES.
  pub fn handle_internal_query_error<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> TPESAction {
    self.exit_and_clean_up(ctx, io_ctx);
    TPESAction::QueryError(query_error)
  }

  /// Handles a Subquery completing
  pub fn handle_subquery_done<IO: CoreIOCtx, Ctx: CTServerContext, SourceT: TransTableSource>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    trans_table_source: &SourceT,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    (_, table_views): (Vec<Option<ColName>>, Vec<TableView>),
  ) -> TPESAction {
    // Add the subquery results into the TableReadES.
    self.new_rms.extend(subquery_new_rms);
    let exec = cast!(TransExecutionS::Executing, &mut self.state).unwrap();
    exec.add_subquery_result(subquery_id, table_views);

    // See if we are finished (due to computing all subqueries).
    if exec.is_complete() {
      self.finish_trans_table_read_es(ctx, io_ctx, trans_table_source)
    } else {
      // Otherwise, we wait.
      TPESAction::Wait
    }
  }

  /// Handles a ES finishing with all subqueries results in.
  fn finish_trans_table_read_es<IO: CoreIOCtx, Ctx: CTServerContext, SourceT: TransTableSource>(
    &mut self,
    _: &mut Ctx,
    _: &mut IO,
    trans_table_source: &SourceT,
  ) -> TPESAction {
    let exec = cast!(TransExecutionS::Executing, &mut self.state).unwrap();

    // Compute children.
    let (children, subquery_results) = std::mem::take(exec).get_results();

    // Create the ContextConstructor.
    let context_constructor = ContextConstructor::new(
      self.context.context_schema.clone(),
      TransLocalTable::new(
        trans_table_source,
        &self.location_prefix.trans_table_name,
        &self.sql_query.from,
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
      Ok(res_table_views) => {
        // Signal Success and return the data.
        self.state = TransExecutionS::Done;
        TPESAction::Success(QueryESResult {
          result: res_table_views,
          new_rms: self.new_rms.iter().cloned().collect(),
        })
      }
      Err(eval_error) => {
        self.state = TransExecutionS::Done;
        TPESAction::QueryError(mk_eval_error(eval_error))
      }
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    _: &mut Ctx,
    _: &mut IO,
  ) {
    self.state = TransExecutionS::Done
  }

  /// Get the `TransTableLocationPrefix` of this ES.
  pub fn location_prefix(&self) -> &TransTableLocationPrefix {
    &self.location_prefix
  }

  pub fn remove_subquery(&mut self, subquery_id: &QueryId) {
    remove_item(&mut self.child_queries, subquery_id)
  }

  pub fn deregister<SourceT: TransTableSource>(
    self,
    _: &SourceT,
  ) -> (QueryId, CTQueryPath, Vec<QueryId>) {
    (self.query_id, self.sender_path, self.child_queries)
  }
}

// -----------------------------------------------------------------------------------------------
//  Implementation of TPESBase
// -----------------------------------------------------------------------------------------------

impl TPESBase for TransTableReadES {
  type ESContext = GRQueryES;

  fn sender_sid(&self) -> &SlaveGroupId {
    TransTableReadES::sender_sid(self)
  }
  fn query_id(&self) -> &QueryId {
    TransTableReadES::query_id(self)
  }
  fn ctx_query_id(&self) -> Option<&QueryId> {
    TransTableReadES::ctx_query_id(self)
  }

  fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es_ctx: &mut Self::ESContext,
  ) -> TPESAction {
    TransTableReadES::start(self, ctx, io_ctx, es_ctx)
  }

  fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es_ctx: &mut Self::ESContext,
  ) -> TPESAction {
    TransTableReadES::gossip_data_changed(self, ctx, io_ctx, es_ctx)
  }

  fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    query_error: msg::QueryError,
  ) -> TPESAction {
    TransTableReadES::handle_internal_query_error(self, ctx, io_ctx, query_error)
  }

  fn handle_subquery_done<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IO,
    es_ctx: &mut Self::ESContext,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    results: (Vec<Option<ColName>>, Vec<TableView>),
  ) -> TPESAction {
    TransTableReadES::handle_subquery_done(
      self,
      ctx,
      io_ctx,
      es_ctx,
      subquery_id,
      subquery_new_rms,
      results,
    )
  }

  fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, ctx: &mut TabletContext, io_ctx: &mut IO) {
    TransTableReadES::exit_and_clean_up(self, ctx, io_ctx)
  }

  fn deregister(self, es_ctx: &mut Self::ESContext) -> (QueryId, CTQueryPath, Vec<QueryId>) {
    TransTableReadES::deregister(self, es_ctx)
  }

  fn remove_subquery(&mut self, subquery_id: &QueryId) {
    TransTableReadES::remove_subquery(self, subquery_id)
  }
}
