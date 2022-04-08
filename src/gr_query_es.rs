use crate::col_usage::{
  collect_delete_subqueries, collect_select_subqueries, collect_update_subqueries,
  node_external_trans_tables, ColUsageNode,
};
use crate::common::{
  lookup, lookup_pos, merge_table_views, mk_qid, CoreIOCtx, OrigP, QueryPlan, Timestamp,
};
use crate::model::common::{
  proc, CQueryPath, ColName, Context, ContextRow, ContextSchema, Gen, LeadershipId,
  PaxosGroupIdTrait, QueryId, SlaveGroupId, TQueryPath, TablePath, TableView, TierMap,
  TransTableLocationPrefix, TransTableName,
};
use crate::model::message as msg;
use crate::server::ServerContextBase;
use crate::server::{CTServerContext, CommonQuery};
use crate::table_read_es::{is_agg, perform_aggregation};
use crate::tm_status::{SendHelper, TMStatus};
use crate::trans_table_read_es::TransTableSource;
use std::collections::{BTreeMap, BTreeSet};
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
  pub schema: Vec<Option<ColName>>,
  pub result: Vec<TableView>,
}

pub enum GRQueryAction {
  /// This tells the parent Server to execute the given TMStatus.
  ExecuteTMStatus(TMStatus),
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
  pub stage_idx: usize,
  /// This fields maps the indices of the GRQueryES Context to that of the Context
  /// in this SubqueryStatus. We cache this here since it's computed when the child
  /// context is computed.
  pub parent_context_map: Vec<usize>,
  pub stage_query_id: QueryId,
}

#[derive(Debug)]
pub enum GRExecutionS {
  Start,
  ReadStage(ReadStage),
  Done,
}

// Recall that the elements don't need to preserve the order of the TransTables, since the
// sql_query does that for us (thus, we can use BTreeMaps).
#[derive(Debug)]
pub struct GRQueryPlan {
  pub tier_map: TierMap,
  pub query_leader_map: BTreeMap<SlaveGroupId, LeadershipId>,
  pub table_location_map: BTreeMap<TablePath, Gen>,
  pub extra_req_cols: BTreeMap<TablePath, Vec<ColName>>,
  pub col_usage_nodes: Vec<(TransTableName, ColUsageNode)>,
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
  pub query_plan: GRQueryPlan,

  // The dynamically evolving fields.
  pub new_rms: BTreeSet<TQueryPath>,
  pub trans_table_views: Vec<(TransTableName, (Vec<Option<ColName>>, Vec<TableView>))>,
  pub state: GRExecutionS,

  /// This holds the path to the parent ES.
  pub orig_p: OrigP,
}

/// Ideally, we should create a create an auxiliary struct to cache `schema` and `instances`
/// so that they don't constantly have to be looked up. However, we would need to access `prefix`
/// here at the Server level, so we avoid doing this for now.
impl TransTableSource for GRQueryES {
  fn get_instance(&self, trans_table_name: &TransTableName, idx: usize) -> &TableView {
    let (_, instances) = lookup(&self.trans_table_views, trans_table_name).unwrap();
    instances.get(idx).unwrap()
  }

  fn get_schema(&self, trans_table_name: &TransTableName) -> Vec<Option<ColName>> {
    let (schema, _) = lookup(&self.trans_table_views, trans_table_name).unwrap();
    schema.clone()
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

impl SubqueryComputableSql for proc::SuperSimpleSelect {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    collect_select_subqueries(self)
  }
}

impl SubqueryComputableSql for proc::Update {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    collect_update_subqueries(self)
  }
}

impl SubqueryComputableSql for proc::Delete {
  fn collect_subqueries(&self) -> Vec<proc::GRQuery> {
    collect_delete_subqueries(self)
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
    let col_usage_nodes = self.query_plan.col_usage_node.children.get(subquery_idx).unwrap();
    let new_trans_table_context = (0..context.context_rows.len()).map(|_| Vec::new()).collect();
    // Finally, construct the GRQueryES.
    GRQueryES {
      root_query_path: self.root_query_path.clone(),
      timestamp: self.timestamp.clone(),
      context,
      new_trans_table_context,
      query_id: gr_query_id,
      sql_query: self.sql_query.collect_subqueries().remove(subquery_idx),
      query_plan: GRQueryPlan {
        tier_map: self.query_plan.tier_map.clone(),
        query_leader_map: self.query_plan.query_leader_map.clone(),
        table_location_map: self.query_plan.table_location_map.clone(),
        extra_req_cols: self.query_plan.extra_req_cols.clone(),
        col_usage_nodes: col_usage_nodes.clone(),
      },
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
  ) -> GRQueryAction {
    self.advance(ctx, io_ctx)
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    tm_qid: QueryId,
    new_rms: BTreeSet<TQueryPath>,
    results: Vec<(Vec<Option<ColName>>, Vec<TableView>)>,
  ) -> GRQueryAction {
    let read_stage = cast!(GRExecutionS::ReadStage, &mut self.state).unwrap();
    let stage_query_id = &read_stage.stage_query_id;
    assert_eq!(stage_query_id, &tm_qid);

    // Combine the results into a single one
    let (_, proc::GRQueryStage::SuperSimpleSelect(sql_query)) =
      self.sql_query.trans_tables.get(read_stage.stage_idx).unwrap();
    let (_, pre_agg_table_views) = merge_table_views(results);
    let (schema, table_views) = match perform_aggregation(sql_query, pre_agg_table_views) {
      Ok(result) => result,
      Err(eval_error) => {
        return GRQueryAction::QueryError(msg::QueryError::RuntimeError {
          msg: format!("Aggregation of GRQueryES failed with error {:?}", eval_error),
        });
      }
    };

    // For now, just assert that the schema that we get corresponds to that in the QueryPlan.
    let (trans_table_name, node) =
      self.query_plan.col_usage_nodes.get(read_stage.stage_idx).unwrap();
    assert_eq!(&schema, &node.schema);

    // Amend the `new_trans_table_context`
    for i in 0..self.context.context_rows.len() {
      let idx = read_stage.parent_context_map.get(i).unwrap();
      self.new_trans_table_context.get_mut(i).unwrap().push(*idx);
    }

    // Accumulate in the `new_rms`.
    self.new_rms.extend(new_rms);

    // Add the `table_views` to the GRQueryES and advance it.
    self.trans_table_views.push((trans_table_name.clone(), (schema, table_views)));
    self.advance(ctx, io_ctx)
  }

  /// This is called when the TMStatus has completed unsuccessfully.
  pub fn handle_tm_aborted<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    _: &mut Ctx,
    _: &mut IO,
    aborted_data: msg::AbortedData,
  ) -> GRQueryAction {
    match aborted_data {
      msg::AbortedData::QueryError(query_error) => {
        // In the case of a QueryError, we just propagate it up.
        self.state = GRExecutionS::Done;
        GRQueryAction::QueryError(query_error)
      }
    }
  }

  /// This is called when one of the remote node's Leadership changes beyond the
  /// LeadershipId that we had sent a PerformQuery to.
  pub fn handle_tm_remote_leadership_changed<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) -> GRQueryAction {
    let read_stage = cast!(GRExecutionS::ReadStage, &self.state).unwrap();
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
  ) -> GRQueryAction {
    // Compute the next stage
    let next_stage_idx = match &self.state {
      GRExecutionS::Start => 0,
      GRExecutionS::ReadStage(read_stage) => read_stage.stage_idx + 1,
      _ => panic!(),
    };

    if next_stage_idx < self.query_plan.col_usage_nodes.len() {
      // This means that we have still have stages to evaluate, so we move on.
      self.process_gr_query_stage(ctx, io_ctx, next_stage_idx)
    } else {
      // This means the GRQueryES is done, so we send the desired result
      // back to the originator.
      let return_trans_table_pos =
        lookup_pos(&self.trans_table_views, &self.sql_query.returning).unwrap();
      let (_, (schema, table_views)) = self.trans_table_views.get(return_trans_table_pos).unwrap();

      // To compute the result, recall that we need to associate the Context to each TableView.
      let mut result = Vec::<TableView>::new();
      for extended_trans_tables_row in &self.new_trans_table_context {
        let idx = extended_trans_tables_row.get(return_trans_table_pos).unwrap();
        result.push(table_views.get(*idx).unwrap().clone());
      }

      // Finally, we signal that the GRQueryES is done and send back the results.
      self.state = GRExecutionS::Done;
      GRQueryAction::Success(GRQueryResult {
        new_rms: self.new_rms.clone(),
        schema: schema.clone(),
        result,
      })
    }
  }

  /// This function moves the GRQueryES to the Stage indicated by `stage_idx`.
  /// (Note the index must be valid (i.e. be an actual stage)).
  fn process_gr_query_stage<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    stage_idx: usize,
  ) -> GRQueryAction {
    assert!(stage_idx < self.query_plan.col_usage_nodes.len());
    let (_, child) = self.query_plan.col_usage_nodes.get(stage_idx).unwrap();

    // Compute the `ColName`s and `TransTableNames` that we want in the child Query,
    // and process that.
    let col_names = &child.external_cols;
    let trans_table_names = &node_external_trans_tables(child);

    // We first compute the Context
    let mut new_context_schema = ContextSchema::default();

    // Elements here correspond to `col_names`, where the `usize` points to the corresponding
    // ColName in the parent ColumnContextSchema.
    let mut col_indices = Vec::<usize>::new();
    for col_name in col_names {
      col_indices.push(
        self
          .context
          .context_schema
          .column_context_schema
          .iter()
          .position(|name| col_name == name)
          .unwrap(),
      );
    }

    // Similarly, elements here correspond to `trans_table_names`, except the `usize` depends on
    // if the TransTableName is an external TransTable (in TransTableContextSchema) or a local one.
    let mut trans_table_name_indicies = Vec::<TransTableIdx>::new();
    for trans_table_name in trans_table_names {
      if let Some(idx) = self
        .context
        .context_schema
        .trans_table_context_schema
        .iter()
        .position(|prefix| &prefix.trans_table_name == trans_table_name)
      {
        trans_table_name_indicies.push(TransTableIdx::External(idx));
      } else {
        trans_table_name_indicies.push(TransTableIdx::Local(
          self
            .trans_table_views
            .iter()
            .position(|(name, (_, _))| name == trans_table_name)
            .unwrap(),
        ));
      }
    }

    // Compute the child ContextSchema
    for idx in &col_indices {
      let col_name = self.context.context_schema.column_context_schema.get(*idx).unwrap();
      new_context_schema.column_context_schema.push(col_name.clone());
    }

    for trans_table_idx in &trans_table_name_indicies {
      match trans_table_idx {
        TransTableIdx::External(idx) => {
          let prefix = self.context.context_schema.trans_table_context_schema.get(*idx).unwrap();
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
    let mut parent_context_map = Vec::<usize>::new();

    // Iterate through the parent ContextRows and construct the child ContextRows.
    for (row_idx, context_row) in self.context.context_rows.iter().enumerate() {
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
      parent_context_map.push(reverse_map.get(&new_context_row).unwrap().clone());
    }

    // Compute the context.
    let context = Context { context_schema: new_context_schema, context_rows: new_context_rows };

    // Construct the QueryPlan. We amend this Slave to the `query_leader_map`.
    let (_, col_usage_node) = self.query_plan.col_usage_nodes.get(stage_idx).unwrap();
    let mut query_leader_map = self.query_plan.query_leader_map.clone();
    query_leader_map
      .insert(ctx.this_sid().clone(), ctx.leader_map().get(ctx.this_gid()).unwrap().clone());
    let query_plan = QueryPlan {
      tier_map: self.query_plan.tier_map.clone(),
      query_leader_map: query_leader_map.clone(),
      table_location_map: self.query_plan.table_location_map.clone(),
      extra_req_cols: self.query_plan.extra_req_cols.clone(),
      col_usage_node: col_usage_node.clone(),
    };

    // Construct the TMStatus
    let mut tm_status =
      TMStatus::new(io_ctx, self.root_query_path.clone(), OrigP::new(self.query_id.clone()));

    // Send out the PerformQuery and populate TMStatus accordingly.
    let (_, stage) = self.sql_query.trans_tables.get(stage_idx).unwrap();
    let child_sql_query = cast!(proc::GRQueryStage::SuperSimpleSelect, stage).unwrap();
    let helper = match &child_sql_query.from.source_ref {
      proc::GeneralSourceRef::TablePath(table_path) => {
        // Here, we must do a SuperSimpleTableSelectQuery.
        let general_query =
          msg::GeneralQuery::SuperSimpleTableSelectQuery(msg::SuperSimpleTableSelectQuery {
            timestamp: self.timestamp.clone(),
            context: context.clone(),
            sql_query: child_sql_query.clone(),
            query_plan,
          });
        let gen = self.query_plan.table_location_map.get(table_path).unwrap();
        let tids =
          ctx.get_min_tablets(table_path, gen, &child_sql_query.from, &child_sql_query.selection);
        SendHelper::TableQuery(general_query, tids)
      }
      proc::GeneralSourceRef::TransTableName(trans_table_name) => {
        // Here, we must do a SuperSimpleTransTableSelectQuery. Recall there is only one RM.
        let location_prefix = context
          .context_schema
          .trans_table_context_schema
          .iter()
          .find(|prefix| &prefix.trans_table_name == trans_table_name)
          .unwrap()
          .clone();
        let general_query = msg::GeneralQuery::SuperSimpleTransTableSelectQuery(
          msg::SuperSimpleTransTableSelectQuery {
            location_prefix: location_prefix.clone(),
            context: context.clone(),
            sql_query: child_sql_query.clone(),
            query_plan,
          },
        );
        SendHelper::TransTableQuery(general_query, location_prefix)
      }
    };

    if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
      self.exit_and_clean_up(ctx);
      return GRQueryAction::QueryError(msg::QueryError::InvalidLeadershipId);
    }

    // Create a SubqueryStatus and move the GRQueryES to the next Stage.
    self.state = GRExecutionS::ReadStage(ReadStage {
      stage_idx,
      parent_context_map,
      stage_query_id: tm_status.query_id().clone(),
    });

    // Return the TMStatus for the parent Server to execute.
    GRQueryAction::ExecuteTMStatus(tm_status)
  }
}
