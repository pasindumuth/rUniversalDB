use crate::col_usage::{
  alias_collecting_cb, external_col_collecting_cb, external_trans_table_collecting_cb,
  gr_query_collecting_cb, trans_table_collecting_cb, QueryElement, QueryIterator,
};
use crate::common::{
  lookup, lookup_pos, merge_table_views, mk_qid, rand_string, unexpected_branch, CTQueryPath,
  ColVal, ColValN, CoreIOCtx, FullGen, OrigP, QueryPlan, ReadOnlySet, Timestamp,
};
use crate::common::{
  CQueryPath, ColName, Context, ContextRow, ContextSchema, Gen, LeadershipId, PaxosGroupIdTrait,
  QueryId, SlaveGroupId, TQueryPath, TablePath, TableView, TierMap, TransTableLocationPrefix,
  TransTableName,
};
use crate::expression::{construct_cexpr, evaluate_c_expr, is_true, EvalError};
use crate::join_read_es::JoinReadES;
use crate::join_util::compute_children_general;
use crate::master_query_planning_es::{ColPresenceReq, ErrorTrait};
use crate::message as msg;
use crate::query_converter::collect_jlns;
use crate::server::{
  extract_subquery_vals, CTServerContext, CommonQuery, EvaluatedSuperSimpleSelect,
  GeneralColumnRef, ServerContextBase, UnnamedColumnRef,
};
use crate::sql_ast::{iast, proc};
use crate::table_read_es::perform_aggregation;
use crate::tablet::Executing;
use crate::tm_status::{SendHelper, TMStatus};
use crate::trans_table_read_es::TransTableSource;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  GRQuery Actions
// -----------------------------------------------------------------------------------------------
pub enum InternalError {
  InternalColumnsDNE(Vec<ColName>),
  QueryError(msg::QueryError),
}

pub struct GRQueryResult {
  pub new_rms: BTreeSet<TQueryPath>,
  pub result: Vec<TableView>,
}

pub enum GRQueryAction {
  /// This tells the parent Server to execute the given TMStatus.
  ExecuteTMStatus(TMStatus),
  /// This tells the parent Server to execute the given JoinReadES.
  ExecuteJoinReadES(JoinReadES),
  /// This tells the parent Server that this GRQueryES has completed
  /// successfully (having already responded, etc).
  Success(GRQueryResult),
  /// This indicates that the GRQueryES failed, where a child query responded with a QueryError.
  /// Here, the GRQueryES can just be trivially erased from the parent.
  QueryError(msg::QueryError),
}

// -----------------------------------------------------------------------------------------------
//  GRQueryES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct ReadStage {
  stage_idx: usize,
  /// This fields maps the indices of the GRQueryES Context to that of the Context
  /// in this TMStatus. We cache this here since it is computed when the child
  /// context is computed.
  parent_to_child_context_map: Vec<usize>,
  stage_query_id: QueryId,
}

#[derive(Debug)]
pub enum GRExecutionS {
  Start,
  ReadStage(ReadStage),
  Done,
}

#[derive(Debug)]
pub struct GRQueryES {
  /// This is only here so it can be forwarded to child queries.
  pub root_query_path: CQueryPath,
  /// This is only here so it can be forwarded to child queries.
  pub timestamp: Timestamp,
  pub context: Rc<Context>,

  /// The elements of the outer Vec corresponds to every ContextRow in the
  /// `context`. The elements of the inner vec corresponds to the elements in
  /// `trans_table_views`. The `usize` indexes into an element in the corresponding
  /// `Vec<TableView>` inside the `trans_table_views`.
  pub new_trans_table_context: Vec<Vec<usize>>,

  /// The QueryId of this `GRQueryES`.
  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::GRQuery,
  pub query_plan: QueryPlan,

  // The dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub trans_table_views: Vec<(TransTableName, Vec<TableView>)>,
  pub state: GRExecutionS,

  /// This holds the path to the parent ES.
  pub orig_p: OrigP,
}

/// Ideally, we should create a create an auxiliary struct to cache `schema` and `instances`
/// so that they don't constantly have to be looked up. However, we would need to access `prefix`
/// here at the Server level, so we avoid doing this for now.
impl TransTableSource for GRQueryES {
  fn get_instance(&self, trans_table_name: &TransTableName, idx: usize) -> &TableView {
    lookup(&self.trans_table_views, trans_table_name).unwrap().get(idx).unwrap()
  }

  fn get_schema(&self, trans_table_name: &TransTableName) -> &Vec<Option<ColName>> {
    lookup(&self.sql_query.trans_tables, trans_table_name).unwrap().schema()
  }
}

// -----------------------------------------------------------------------------------------------
//  GRQuery Constructor
// -----------------------------------------------------------------------------------------------
// This is a convenient View Container we use to help create GRQuerys.

/// This is a trait for getting all of the Subqueries in a SQL
/// query (e.g. SuperSimpleSelect, Update).
pub trait SubqueryComputableSql {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery>;
}

impl SubqueryComputableSql for proc::TableSelect {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    let mut top_level_cols_set = Vec::<proc::GRQuery>::new();
    QueryIterator::new_top_level()
      .iterate_table_select(&mut gr_query_collecting_cb(&mut top_level_cols_set), self);
    top_level_cols_set
  }
}

impl SubqueryComputableSql for proc::TransTableSelect {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    let mut top_level_cols_set = Vec::<proc::GRQuery>::new();
    QueryIterator::new_top_level()
      .iterate_trans_table_select(&mut gr_query_collecting_cb(&mut top_level_cols_set), self);
    top_level_cols_set
  }
}

impl SubqueryComputableSql for proc::Update {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    let mut top_level_cols_set = Vec::<proc::GRQuery>::new();
    QueryIterator::new_top_level()
      .iterate_update(&mut gr_query_collecting_cb(&mut top_level_cols_set), self);
    top_level_cols_set
  }
}

impl SubqueryComputableSql for proc::Delete {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    let mut top_level_cols_set = Vec::<proc::GRQuery>::new();
    QueryIterator::new_top_level()
      .iterate_delete(&mut gr_query_collecting_cb(&mut top_level_cols_set), self);
    top_level_cols_set
  }
}

pub struct GRQueryConstructorView<'a, SqlQueryT: SubqueryComputableSql> {
  pub root_query_path: &'a CQueryPath,
  pub timestamp: &'a Timestamp,
  /// SQL query containing by the parent ES.
  pub sql_query: &'a SqlQueryT,
  /// QueryPlan of the parent ES
  pub query_plan: &'a QueryPlan,
  /// QueryId of the parent ES
  pub query_id: &'a QueryId,
  /// The parent ES's Context
  pub context: &'a Context,
}

impl<'a, SqlQueryT: SubqueryComputableSql> GRQueryConstructorView<'a, SqlQueryT> {
  pub fn mk_gr_query_es(
    &self,
    gr_query_id: QueryId,
    context: Rc<Context>,
    subquery_idx: usize,
  ) -> GRQueryES {
    // Filter the TransTables in the QueryPlan based on the TransTables available for this subquery.
    let new_trans_table_context = (0..context.context_rows.len()).map(|_| Vec::new()).collect();
    // Finally, construct the GRQueryES.
    GRQueryES {
      root_query_path: self.root_query_path.clone(),
      timestamp: self.timestamp.clone(),
      context,
      new_trans_table_context,
      query_id: gr_query_id,
      sql_query: self.sql_query.collect_subqueries().remove(subquery_idx),
      query_plan: self.query_plan.clone(),
      new_rms: Default::default(),
      trans_table_views: vec![],
      state: GRExecutionS::Start,
      orig_p: OrigP::new(self.query_id.clone()),
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

pub enum TransTableIdx {
  External(usize),
  Local(usize),
}

impl GRQueryES {
  /// Starts the GRQueryES from its initial state.
  pub fn start<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) -> Option<GRQueryAction> {
    self.advance(ctx, io_ctx)
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    tm_qid: QueryId,
    new_rms: BTreeSet<TQueryPath>,
    pre_agg_table_views: Vec<TableView>,
  ) -> Option<GRQueryAction> {
    let read_stage = cast!(GRExecutionS::ReadStage, &mut self.state).unwrap();
    assert_eq!(&read_stage.stage_query_id, &tm_qid);
    let (trans_table_name, stage) = self.sql_query.trans_tables.get(read_stage.stage_idx).unwrap();

    // Combine the results into a single one
    let table_views = match match stage {
      proc::GRQueryStage::TableSelect(sql_query) => {
        perform_aggregation(sql_query, pre_agg_table_views)
      }
      proc::GRQueryStage::TransTableSelect(sql_query) => {
        perform_aggregation(sql_query, pre_agg_table_views)
      }
      proc::GRQueryStage::JoinSelect(_) => return unexpected_branch(),
    } {
      Ok(result) => result,
      Err(eval_error) => {
        return Some(GRQueryAction::QueryError(msg::QueryError::RuntimeError {
          msg: format!("Aggregation of GRQueryES failed with error {:?}", eval_error),
        }));
      }
    };

    // Amend the `new_trans_table_context`
    for i in 0..self.context.context_rows.len() {
      let idx = read_stage.parent_to_child_context_map.get(i).unwrap();
      self.new_trans_table_context.get_mut(i).unwrap().push(*idx);
    }

    // Accumulate in the `new_rms`.
    self.new_rms.extend(new_rms);

    // Add the `table_views` to the GRQueryES and advance it.
    self.trans_table_views.push((trans_table_name.clone(), table_views));
    self.advance(ctx, io_ctx)
  }

  /// This is called when the TMStatus has completed unsuccessfully.
  pub fn handle_tm_aborted<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    _: &mut Ctx,
    _: &mut IO,
    aborted_data: msg::AbortedData,
  ) -> Option<GRQueryAction> {
    match aborted_data {
      msg::AbortedData::QueryError(query_error) => {
        // In the case of a QueryError, we just propagate it up.
        self.state = GRExecutionS::Done;
        Some(GRQueryAction::QueryError(query_error))
      }
    }
  }

  /// This is called when the JoinReadES has completed successfully.
  pub fn handle_join_select_done<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    child_qid: QueryId,
    new_rms: Vec<TQueryPath>,
    table_views: Vec<TableView>,
  ) -> Option<GRQueryAction> {
    let read_stage = cast!(GRExecutionS::ReadStage, &self.state)?;
    check!(&read_stage.stage_query_id == &child_qid);
    let (trans_table_name, _) = self.sql_query.trans_tables.get(read_stage.stage_idx).unwrap();

    // Amend the `new_trans_table_context`
    for i in 0..self.context.context_rows.len() {
      let idx = read_stage.parent_to_child_context_map.get(i).unwrap();
      self.new_trans_table_context.get_mut(i).unwrap().push(*idx);
    }

    // Accumulate in the `new_rms`.
    self.new_rms.extend(new_rms);

    // Add the `table_views` to the GRQueryES and advance it.
    self.trans_table_views.push((trans_table_name.clone(), table_views));
    self.advance(ctx, io_ctx)
  }

  /// This is called when the JoinReadES has aborted.
  pub fn handle_join_select_aborted<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    aborted_data: msg::AbortedData,
  ) -> Option<GRQueryAction> {
    self.handle_tm_aborted(ctx, io_ctx, aborted_data)
  }

  /// This is called when one of the remote node's Leadership changes beyond the
  /// LeadershipId that we had sent a PerformQuery to.
  pub fn handle_tm_remote_leadership_changed<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) -> Option<GRQueryAction> {
    let read_stage = cast!(GRExecutionS::ReadStage, &self.state)?;
    self.process_gr_query_stage(ctx, io_ctx, read_stage.stage_idx)
  }

  /// This Exits and Cleans up this GRQueryES.
  pub fn exit_and_clean_up<Ctx: CTServerContext>(&mut self, _: &mut Ctx) {
    self.state = GRExecutionS::Done;
  }

  /// This advanced the Stage of the GRQueryES. If there is no next Stage, then we
  /// return Done, containing the result and signaling that the GRQueryES is complete.
  fn advance<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) -> Option<GRQueryAction> {
    // Compute the next stage
    let next_stage_idx = match &self.state {
      GRExecutionS::Start => 0,
      GRExecutionS::ReadStage(read_stage) => read_stage.stage_idx + 1,
      _ => return unexpected_branch(),
    };

    if next_stage_idx < self.sql_query.trans_tables.len() {
      // This means that we have still have stages to evaluate, so we move on.
      self.process_gr_query_stage(ctx, io_ctx, next_stage_idx)
    } else {
      // This means the GRQueryES is done, so we send the desired result
      // back to the originator.
      let return_trans_table_pos =
        lookup_pos(&self.trans_table_views, &self.sql_query.returning).unwrap();
      let (_, table_views) = self.trans_table_views.get(return_trans_table_pos).unwrap();

      // To compute the result, recall that we need to associate the Context to each TableView.
      let mut result = Vec::<TableView>::new();
      for extended_trans_tables_row in &self.new_trans_table_context {
        let idx = extended_trans_tables_row.get(return_trans_table_pos).unwrap();
        result.push(table_views.get(*idx).unwrap().clone());
      }

      // Finally, we signal that the GRQueryES is done and send back the results.
      self.state = GRExecutionS::Done;
      Some(GRQueryAction::Success(GRQueryResult { new_rms: self.new_rms.clone(), result }))
    }
  }

  /// This function moves the GRQueryES to the Stage indicated by `stage_idx`.
  /// (Note the index must be valid (i.e. be an actual stage)).
  fn process_gr_query_stage<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    stage_idx: usize,
  ) -> Option<GRQueryAction> {
    let (_, stage) = self.sql_query.trans_tables.get(stage_idx).unwrap();

    let context_computer = ChildContextComputer {
      query_id: &self.query_id,
      new_trans_table_context: &self.new_trans_table_context,
      trans_table_views: &self.trans_table_views,
    };

    let (context, parent_to_child_context_map) = context_computer.compute_child_context(
      ctx,
      io_ctx,
      &self.context,
      QueryElement::GRQueryStage(stage),
    );

    // Construct the QueryPlan. We amend this Slave to the `query_leader_map`.
    let mut query_leader_map = self.query_plan.query_leader_map.clone();
    query_leader_map
      .insert(ctx.this_sid().clone(), ctx.leader_map().get(ctx.this_gid()).unwrap().clone());
    let query_plan = QueryPlan {
      tier_map: self.query_plan.tier_map.clone(),
      query_leader_map: query_leader_map.clone(),
      table_location_map: self.query_plan.table_location_map.clone(),
      col_presence_req: self.query_plan.col_presence_req.clone(),
    };

    // Construct the TMStatus
    let mut tm_status =
      TMStatus::new(io_ctx, self.root_query_path.clone(), OrigP::new(self.query_id.clone()));

    // Send out the PerformQuery and populate TMStatus accordingly.
    match stage {
      proc::GRQueryStage::TableSelect(select) => {
        // Here, we must do a TableSelectQuery.
        let general_query = msg::GeneralQuery::TableSelectQuery(msg::TableSelectQuery {
          timestamp: self.timestamp.clone(),
          context,
          sql_query: select.clone(),
          query_plan,
        });
        let full_gen = self.query_plan.table_location_map.get(&select.from.table_path).unwrap();
        let tids = ctx.get_min_tablets(&select.from, full_gen, &select.selection);

        let helper = SendHelper::TableQuery(general_query, tids);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx);
          Some(GRQueryAction::QueryError(msg::QueryError::InvalidLeadershipId))
        } else {
          // Move the GRQueryES to the next Stage.
          self.state = GRExecutionS::ReadStage(ReadStage {
            stage_idx,
            parent_to_child_context_map,
            stage_query_id: tm_status.query_id().clone(),
          });

          // Return the TMStatus for the parent Server to execute.
          Some(GRQueryAction::ExecuteTMStatus(tm_status))
        }
      }
      proc::GRQueryStage::TransTableSelect(select) => {
        // Here, we must do a TransTableSelectQuery. Recall there is only one RM.
        let location_prefix = context
          .context_schema
          .trans_table_context_schema
          .iter()
          .find(|prefix| &prefix.trans_table_name == &select.from.trans_table_name)
          .unwrap()
          .clone();
        let general_query = msg::GeneralQuery::TransTableSelectQuery(msg::TransTableSelectQuery {
          location_prefix: location_prefix.clone(),
          context,
          sql_query: select.clone(),
          query_plan,
        });

        let helper = SendHelper::TransTableQuery(general_query, location_prefix);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx);
          Some(GRQueryAction::QueryError(msg::QueryError::InvalidLeadershipId))
        } else {
          // Move the GRQueryES to the next Stage.
          self.state = GRExecutionS::ReadStage(ReadStage {
            stage_idx,
            parent_to_child_context_map,
            stage_query_id: tm_status.query_id().clone(),
          });

          // Return the TMStatus for the parent Server to execute.
          Some(GRQueryAction::ExecuteTMStatus(tm_status))
        }
      }
      proc::GRQueryStage::JoinSelect(select) => {
        let context = Rc::new(context);
        let child_es = JoinReadES::create(
          io_ctx,
          self.root_query_path.clone(),
          self.timestamp.clone(),
          context,
          select.clone(),
          self.query_plan.clone(),
          OrigP::new(self.query_id.clone()),
        );

        // Move the GRQueryES to the next Stage.
        self.state = GRExecutionS::ReadStage(ReadStage {
          stage_idx,
          parent_to_child_context_map,
          stage_query_id: child_es.query_id().clone(),
        });

        // Return the subqueries for the parent server to execute.
        Some(GRQueryAction::ExecuteJoinReadES(child_es))
      }
    }
  }
}

// -----------------------------------------------------------------------------------------------
//  ChildContextComputer
// -----------------------------------------------------------------------------------------------

struct ChildContextComputer<'a> {
  pub query_id: &'a QueryId,
  pub new_trans_table_context: &'a Vec<Vec<usize>>,
  pub trans_table_views: &'a Vec<(TransTableName, Vec<TableView>)>,
}

impl<'a> ChildContextComputer<'a> {
  /// This takes an arbitrary `Context`, which we consider as the parent context, and then
  /// produces a child context for an arbitrary `elem`.
  fn compute_child_context<IO: CoreIOCtx, Ctx: CTServerContext>(
    &self,
    ctx: &mut Ctx,
    _: &mut IO,
    parent_context: &Context,
    elem: QueryElement,
  ) -> (Context, Vec<usize>) {
    let (col_names, trans_table_names) = compute_children_general(elem);

    // We first compute the Context
    let mut new_context_schema = ContextSchema::default();

    // Elements here correspond to `col_names`, where the `usize` points to the corresponding
    // ColName in the parent ColumnContextSchema.
    let mut col_indices = Vec::<usize>::new();
    for col_name in col_names {
      col_indices.push(
        parent_context
          .context_schema
          .column_context_schema
          .iter()
          .position(|name| &col_name == name)
          .unwrap(),
      );
    }

    // Similarly, elements here correspond to `trans_table_names`, except the `usize` depends on
    // if the TransTableName is an external TransTable (in TransTableContextSchema) or a local one.
    let mut trans_table_name_indicies = Vec::<TransTableIdx>::new();
    for trans_table_name in trans_table_names {
      if let Some(idx) = parent_context
        .context_schema
        .trans_table_context_schema
        .iter()
        .position(|prefix| prefix.trans_table_name == trans_table_name)
      {
        trans_table_name_indicies.push(TransTableIdx::External(idx));
      } else {
        trans_table_name_indicies.push(TransTableIdx::Local(
          self.trans_table_views.iter().position(|(name, _)| name == &trans_table_name).unwrap(),
        ));
      }
    }

    // Compute the child ContextSchema
    for idx in &col_indices {
      let col_name = parent_context.context_schema.column_context_schema.get(*idx).unwrap();
      new_context_schema.column_context_schema.push(col_name.clone());
    }

    for trans_table_idx in &trans_table_name_indicies {
      match trans_table_idx {
        TransTableIdx::External(idx) => {
          let prefix = parent_context.context_schema.trans_table_context_schema.get(*idx).unwrap();
          new_context_schema.trans_table_context_schema.push(prefix.clone());
        }
        TransTableIdx::Local(idx) => {
          let (trans_table_name, _) = self.trans_table_views.get(*idx).unwrap();
          new_context_schema.trans_table_context_schema.push(TransTableLocationPrefix {
            source: ctx.mk_this_query_path(self.query_id.clone()),
            trans_table_name: trans_table_name.clone(),
          });
        }
      }
    }

    // This contains the ContextRows of the child Context we're creating.
    let mut new_context_rows = Vec::<ContextRow>::new();
    // This maps the above ContextRows back to the index in which they appear.
    let mut reverse_map = BTreeMap::<ContextRow, usize>::new();
    // Elements here correspond to the parent ContextRows that have been processed, which
    // index that the corresponding child ContextRow takes on above.
    let mut parent_to_child_context_map = Vec::<usize>::new();

    // Iterate through the parent ContextRows and construct the child ContextRows.
    for (row_idx, context_row) in parent_context.context_rows.iter().enumerate() {
      let mut new_context_row = ContextRow::default();

      // Populate the ColumnContextRow.
      for idx in &col_indices {
        let col_val = context_row.column_context_row.get(*idx).unwrap();
        new_context_row.column_context_row.push(col_val.clone());
      }

      // Populate the TransTableContextRow
      for trans_table_idx in &trans_table_name_indicies {
        match trans_table_idx {
          TransTableIdx::External(idx) => {
            let trans_val = context_row.trans_table_context_row.get(*idx).unwrap();
            new_context_row.trans_table_context_row.push(trans_val.clone());
          }
          TransTableIdx::Local(idx) => {
            let trans_val = self.new_trans_table_context.get(row_idx).unwrap().get(*idx).unwrap();
            new_context_row.trans_table_context_row.push(trans_val.clone());
          }
        }
      }

      // Populate the next Context and associated metadata containers.
      if !reverse_map.contains_key(&new_context_row) {
        reverse_map.insert(new_context_row.clone(), new_context_rows.len());
        new_context_rows.push(new_context_row.clone());
      }
      parent_to_child_context_map.push(reverse_map.get(&new_context_row).unwrap().clone());
    }

    // Compute the context.
    let context = Context { context_schema: new_context_schema, context_rows: new_context_rows };

    (context, parent_to_child_context_map)
  }
}
