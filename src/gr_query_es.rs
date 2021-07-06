use crate::col_usage::{
  collect_select_subqueries, collect_update_subqueries, node_external_trans_tables,
  nodes_external_trans_tables, FrozenColUsageNode,
};
use crate::common::{
  lookup, lookup_pos, mk_qid, IOTypes, NetworkOut, OrigP, QueryPlan, TMStatus, TMWaitValue,
};
use crate::model::common::{
  proc, ColName, Context, ContextRow, ContextSchema, Gen, NodeGroupId, QueryId, QueryPath,
  TableView, TabletGroupId, TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::message as msg;
use crate::server::{CommonQuery, ServerContext};
use crate::tablet::{SingleSubqueryStatus, SubqueryPending};
use crate::trans_read_es::TransTableSource;
use std::collections::{HashMap, HashSet};
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
  pub new_rms: HashSet<QueryPath>,
  pub schema: Vec<ColName>,
  pub result: Vec<TableView>,
}

pub enum GRQueryAction {
  /// This tells the parent Server to execute the given TMStatus.
  ExecuteTMStatus(TMStatus),
  /// This tells the parent Server that this GRQueryES has completed
  /// successfully (having already responded, etc).
  Done(GRQueryResult),
  /// This indicates that the GRQueryES failed, where the Context was insufficient.
  /// Here, the GRQueryES can just be trivially erased from the parent.
  InternalColumnsDNE(Vec<ColName>),
  /// This indicates that the GRQueryES failed, where a child query responded with a QueryError.
  /// Here, the GRQueryES can just be trivially erased from the parent.
  QueryError(msg::QueryError),
  /// This tells the parent Server that this GRQueryES has completed
  /// unsuccessfully, and that the given `QueryId` should be
  /// Exit and Cleaned Up, along with this one.
  ExitAndCleanUp(Vec<QueryId>),
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
  pub pending_status: SubqueryPending,
}

#[derive(Debug)]
pub struct MasterQueryReplanning {
  pub master_query_id: QueryId,
}

#[derive(Debug)]
pub enum GRExecutionS {
  Start,
  ReadStage(ReadStage),
  MasterQueryReplanning(MasterQueryReplanning),
}

// Recall that the elements don't need to preserve the order of the TransTables, since the
// sql_query does that for us (thus, we can use HashMaps).
#[derive(Debug)]
pub struct GRQueryPlan {
  pub gossip_gen: Gen,
  pub trans_table_schemas: HashMap<TransTableName, Vec<ColName>>,
  pub col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
}

#[derive(Debug)]
pub struct GRQueryES {
  pub root_query_path: QueryPath,
  pub tier_map: TierMap,
  pub timestamp: Timestamp,
  pub context: Rc<Context>,

  /// The elements of the outer Vec corresponds to every ContextRow in the
  /// `context`. The elements of the inner vec corresponds to the elements in
  /// `trans_table_views`. The `usize` indexes into an element in the corresponding
  /// Vec<TableView> inside the `trans_table_views`.
  pub new_trans_table_context: Vec<Vec<usize>>,

  // Fields needed for responding.
  pub query_id: QueryId,

  // Query-related fields.
  pub sql_query: proc::GRQuery,
  pub query_plan: GRQueryPlan,

  // The dynamically evolving fields.
  pub new_rms: HashSet<QueryPath>,
  pub trans_table_views: Vec<(TransTableName, (Vec<ColName>, Vec<TableView>))>,
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

  fn get_schema(&self, trans_table_name: &TransTableName) -> Vec<ColName> {
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

pub struct GRQueryConstructorView<'a, SqlQueryT: SubqueryComputableSql> {
  pub root_query_path: &'a QueryPath,
  pub tier_map: &'a TierMap,
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
    let mut trans_table_schemas = HashMap::<TransTableName, Vec<ColName>>::new();
    for trans_table_name in nodes_external_trans_tables(col_usage_nodes) {
      let schema = self.query_plan.trans_table_schemas.get(&trans_table_name).unwrap().clone();
      trans_table_schemas.insert(trans_table_name, schema);
    }
    // Finally, construct the GRQueryES.
    GRQueryES {
      root_query_path: self.root_query_path.clone(),
      tier_map: self.tier_map.clone(),
      timestamp: self.timestamp.clone(),
      context,
      new_trans_table_context: vec![],
      query_id: gr_query_id,
      sql_query: self.sql_query.collect_subqueries().remove(subquery_idx),
      query_plan: GRQueryPlan {
        gossip_gen: self.query_plan.gossip_gen.clone(),
        trans_table_schemas,
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

impl GRQueryES {
  /// Starts the GRQueryES from its initial state.
  pub fn start<T: IOTypes>(&mut self, ctx: &mut ServerContext<T>) -> GRQueryAction {
    self.advance(ctx)
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    tm_query_id: QueryId,
    new_rms: HashSet<QueryPath>,
    (schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) -> GRQueryAction {
    let read_stage = cast!(GRExecutionS::ReadStage, &mut self.state).unwrap();

    // Extract the `context` from the SubqueryPending status and verify
    // that it corresponds to `table_views`.
    let pending_status = &read_stage.pending_status;
    assert_eq!(pending_status.query_id, tm_query_id);
    assert_eq!(table_views.len(), pending_status.context.context_rows.len());

    // For now, just assert assert that the schema that we get corresponds
    // to that in the QueryPlan.
    let (trans_table_name, (cur_schema, _)) =
      self.query_plan.col_usage_nodes.get(read_stage.stage_idx).unwrap();
    assert_eq!(&schema, cur_schema);

    // Amend the `new_trans_table_context`
    for i in 0..self.context.context_rows.len() {
      let idx = read_stage.parent_context_map.get(i).unwrap();
      self.new_trans_table_context.get_mut(i).unwrap().push(*idx);
    }

    // Accumulate in the `new_rms`.
    self.new_rms.extend(new_rms);

    // Add the `table_views` to the GRQueryES and advance it.
    self.trans_table_views.push((trans_table_name.clone(), (schema, table_views)));
    self.advance(ctx)
  }

  /// This is called when the TMStatus has completed unsuccessfully.
  pub fn handle_tm_aborted<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    aborted_data: msg::AbortedData,
  ) -> GRQueryAction {
    let read_stage = cast!(GRExecutionS::ReadStage, &self.state).unwrap();
    let pending_status = &read_stage.pending_status;
    match aborted_data {
      msg::AbortedData::ColumnsDNE { missing_cols } => {
        // First, assert that the missing columns are indeed not already a part of the Context.
        for col in &missing_cols {
          assert!(!pending_status.context.context_schema.column_context_schema.contains(col));
        }

        // Next, we compute the subset of `missing_cols` that are not in the Context here.
        let mut rem_cols = Vec::<ColName>::new();
        for col in &missing_cols {
          if !self.context.context_schema.column_context_schema.contains(col) {
            rem_cols.push(col.clone());
          }
        }

        if !rem_cols.is_empty() {
          // If the Context has the missing columns propagate the error upward. Note that
          // we don't have to call `exit_and_clean_up`, since the only TMStatus is finished.
          GRQueryAction::InternalColumnsDNE(rem_cols)
        } else {
          // If the GRQueryES Context is sufficient, we simply amend the new columns
          // to the Context and reprocess the ReadStage.
          let context_schema = &pending_status.context.context_schema;
          let mut context_cols = context_schema.column_context_schema.clone();
          context_cols.extend(missing_cols);
          let context_trans_tables = context_schema.trans_table_names();
          let stage_idx = read_stage.stage_idx.clone();
          self.process_gr_query_stage_simple(ctx, stage_idx, &context_cols, context_trans_tables)
        }
      }
      msg::AbortedData::QueryError(query_error) => {
        // In the case of a QueryError, we just propagate it up. Note that we don't
        // have to call `exit_and_clean_up`, since the only TMStatus is finished.
        GRQueryAction::QueryError(query_error)
      }
    }
  }

  /// This is called to Exit and Clean Up this GRQueryES.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut ServerContext<T>) -> GRQueryAction {
    match &self.state {
      GRExecutionS::Start => GRQueryAction::ExitAndCleanUp(vec![]),
      GRExecutionS::ReadStage(read_stage) => {
        GRQueryAction::ExitAndCleanUp(vec![read_stage.pending_status.query_id.clone()])
      }
      GRExecutionS::MasterQueryReplanning(planning) => {
        // Remove if present
        if ctx.master_query_map.remove(&planning.master_query_id).is_some() {
          // If the removal was successful, we should also send a Cancellation
          // message to the Master.
          ctx.network_output.send(
            &ctx.master_eid,
            msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
              msg::CancelMasterFrozenColUsage { query_id: planning.master_query_id.clone() },
            )),
          );
        }
        GRQueryAction::ExitAndCleanUp(vec![])
      }
    }
  }

  /// This advanced the Stage of the GRQueryES. If there is no next Stage, then we
  /// return Done, containing the result and signaling that the GRQueryES is complete.
  fn advance<T: IOTypes>(&mut self, ctx: &mut ServerContext<T>) -> GRQueryAction {
    // Compute the next stage
    let next_stage_idx = match &self.state {
      GRExecutionS::Start => 0,
      GRExecutionS::ReadStage(read_stage) => read_stage.stage_idx + 1,
      _ => panic!(),
    };

    if next_stage_idx < self.query_plan.col_usage_nodes.len() {
      // This means that we have still have stages to evaluate, so we move on.
      self.process_gr_query_stage(ctx, next_stage_idx)
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
      GRQueryAction::Done(GRQueryResult {
        new_rms: self.new_rms.clone(),
        schema: schema.clone(),
        result,
      })
    }
  }

  /// This function moves the GRQueryES to the Stage indicated by `stage_idx`.
  /// This index must be valid (an actual stage).
  fn process_gr_query_stage<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    stage_idx: usize,
  ) -> GRQueryAction {
    assert!(stage_idx < self.query_plan.col_usage_nodes.len());
    let (_, (_, child)) = self.query_plan.col_usage_nodes.get(stage_idx).unwrap();

    // Compute the `ColName`s and `TransTableNames` that we want in the child Query,
    // and process that.
    let context_cols = child.external_cols.clone();
    let context_trans_tables = node_external_trans_tables(child);
    self.process_gr_query_stage_simple(ctx, stage_idx, &context_cols, context_trans_tables)
  }

  /// This is a common function used for sending processing a GRQueryES stage that
  /// doesn't use the child QueryPlan to process the child ContextSchema, but rather
  /// uses the `context_cols` and `context_trans_table`.
  fn process_gr_query_stage_simple<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    stage_idx: usize,
    context_cols: &Vec<ColName>,
    context_trans_tables: Vec<TransTableName>,
  ) -> GRQueryAction {
    assert!(stage_idx < self.query_plan.col_usage_nodes.len());

    // We first compute the Context
    let mut context_schema = ContextSchema::default();

    // Point all `context_cols` columns to their index in main Query's ContextSchema.
    let mut context_col_index = HashMap::<ColName, usize>::new();
    let col_schema = &self.context.context_schema.column_context_schema;
    for (index, col) in col_schema.iter().enumerate() {
      if context_cols.contains(col) {
        context_col_index.insert(col.clone(), index);
      }
    }

    // Compute the ColumnContextSchema
    context_schema.column_context_schema.extend(context_cols.clone());

    // Split the `context_trans_tables` according to which of them are defined
    // in this GRQueryES, and which come from the outside.
    let mut local_trans_table_split = Vec::<TransTableName>::new();
    let mut external_trans_table_split = Vec::<TransTableName>::new();
    let completed_local_trans_tables: HashSet<TransTableName> =
      self.trans_table_views.iter().map(|(name, _)| name).cloned().collect();
    for trans_table_name in context_trans_tables {
      if completed_local_trans_tables.contains(&trans_table_name) {
        local_trans_table_split.push(trans_table_name);
      } else {
        assert!(self.query_plan.trans_table_schemas.contains_key(&trans_table_name));
        external_trans_table_split.push(trans_table_name);
      }
    }

    // Point all `external_trans_table_split` to their index in main Query's ContextSchema.
    let trans_table_context_schema = &self.context.context_schema.trans_table_context_schema;
    let mut context_external_trans_table_index = HashMap::<TransTableName, usize>::new();
    for (index, prefix) in trans_table_context_schema.iter().enumerate() {
      if external_trans_table_split.contains(&prefix.trans_table_name) {
        context_external_trans_table_index.insert(prefix.trans_table_name.clone(), index);
      }
    }

    // Compute the TransTableContextSchema
    for trans_table_name in &local_trans_table_split {
      context_schema.trans_table_context_schema.push(TransTableLocationPrefix {
        source: ctx.node_group_id(),
        query_id: self.query_id.clone(),
        trans_table_name: trans_table_name.clone(),
      });
    }
    for trans_table_name in &external_trans_table_split {
      let index = context_external_trans_table_index.get(trans_table_name).unwrap();
      let trans_table_prefix = trans_table_context_schema.get(*index).unwrap().clone();
      context_schema.trans_table_context_schema.push(trans_table_prefix);
    }

    // Construct the `ContextRow`s. To do this, we iterate over main Query's
    // `ContextRow`s and then the corresponding `ContextRow`s for the subquery.
    // We hold the child `ContextRow`s in Vec, and we use a HashSet to avoid duplicates.
    let mut new_context_rows = Vec::<ContextRow>::new();
    let mut new_row_map = HashMap::<ContextRow, usize>::new();
    // We also map the indices of the GRQueryES Context to that of the SubqueryStatus.
    let mut parent_context_map = Vec::<usize>::new();
    for (row_idx, context_row) in self.context.context_rows.iter().enumerate() {
      let mut new_context_row = ContextRow::default();

      // Compute the `ColumnContextRow`
      for col in context_cols {
        let index = context_col_index.get(col).unwrap();
        let col_val = context_row.column_context_row.get(*index).unwrap().clone();
        new_context_row.column_context_row.push(col_val);
      }

      // Compute the `TransTableContextRow`
      for local_trans_table_idx in 0..local_trans_table_split.len() {
        let trans_table_context_row = self.new_trans_table_context.get(row_idx).unwrap();
        let row_elem = trans_table_context_row.get(local_trans_table_idx).unwrap();
        new_context_row.trans_table_context_row.push(*row_elem);
      }
      for trans_table_name in &external_trans_table_split {
        let index = context_external_trans_table_index.get(trans_table_name).unwrap();
        let trans_index = context_row.trans_table_context_row.get(*index).unwrap();
        new_context_row.trans_table_context_row.push(*trans_index);
      }

      if !new_row_map.contains_key(&new_context_row) {
        new_row_map.insert(new_context_row.clone(), new_context_rows.len());
        new_context_rows.push(new_context_row.clone());
      }

      parent_context_map.push(new_row_map.get(&new_context_row).unwrap().clone());
    }

    // Finally, compute the context.
    let context = Rc::new(Context { context_schema, context_rows: new_context_rows });

    // Next, we compute the QueryPlan we should send out.

    // Compute the TransTable schemas. We use the true TransTables for all prior
    // locally defined TransTables.
    let trans_table_schemas = &self.query_plan.trans_table_schemas;
    let mut child_trans_table_schemas = HashMap::<TransTableName, Vec<ColName>>::new();
    for trans_table_name in &local_trans_table_split {
      let (_, (schema, _)) =
        self.trans_table_views.iter().find(|(name, _)| trans_table_name == name).unwrap();
      child_trans_table_schemas.insert(trans_table_name.clone(), schema.clone());
    }
    for trans_table_name in &external_trans_table_split {
      let schema = trans_table_schemas.get(trans_table_name).unwrap();
      child_trans_table_schemas.insert(trans_table_name.clone(), schema.clone());
    }

    let (_, (_, child)) = self.query_plan.col_usage_nodes.get(stage_idx).unwrap();
    let query_plan = QueryPlan {
      gossip_gen: self.query_plan.gossip_gen.clone(),
      trans_table_schemas: child_trans_table_schemas,
      col_usage_node: child.clone(),
    };

    // Construct the TMStatus
    let tm_query_id = mk_qid(ctx.rand);
    let mut tm_status = TMStatus {
      node_group_ids: Default::default(),
      query_id: tm_query_id.clone(),
      new_rms: Default::default(),
      responded_count: 0,
      tm_state: Default::default(),
      orig_p: OrigP::new(self.query_id.clone()),
    };

    let sender_path = QueryPath {
      slave_group_id: ctx.this_slave_group_id.clone(),
      maybe_tablet_group_id: ctx.maybe_this_tablet_group_id.map(|id| id.deref().clone()),
      query_id: tm_query_id.clone(),
    };

    // Send out the PerformQuery and populate TMStatus accordingly.
    let (_, stage) = self.sql_query.trans_tables.get(stage_idx).unwrap();
    let child_sql_query = cast!(proc::GRQueryStage::SuperSimpleSelect, stage).unwrap();
    match &child_sql_query.from {
      proc::TableRef::TablePath(table_path) => {
        // Here, we must do a SuperSimpleTableSelectQuery.
        let child_query = msg::SuperSimpleTableSelectQuery {
          timestamp: self.timestamp.clone(),
          context: context.deref().clone(),
          sql_query: child_sql_query.clone(),
          query_plan,
        };
        let general_query = msg::GeneralQuery::SuperSimpleTableSelectQuery(child_query);

        // Compute the TabletGroups involved.
        for tablet_group_id in ctx.get_min_tablets(table_path, &child_sql_query.selection) {
          let child_query_id = mk_qid(ctx.rand);
          let sid = ctx.tablet_address_config.get(&tablet_group_id).unwrap();
          let eid = ctx.slave_address_config.get(&sid).unwrap();

          // Send out PerformQuery.
          ctx.network_output.send(
            eid,
            msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
              tablet_group_id.clone(),
              msg::TabletMessage::PerformQuery(msg::PerformQuery {
                root_query_path: self.root_query_path.clone(),
                sender_path: sender_path.clone(),
                query_id: child_query_id.clone(),
                tier_map: self.tier_map.clone(),
                query: general_query.clone(),
              }),
            )),
          );

          // Add the TabletGroup into the TMStatus.
          let node_group_id = NodeGroupId::Tablet(tablet_group_id);
          tm_status.node_group_ids.insert(node_group_id, child_query_id.clone());
          tm_status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
        }
      }
      proc::TableRef::TransTableName(trans_table_name) => {
        // Here, we must do a SuperSimpleTransTableSelectQuery. Recall there is only one RM.
        let location_prefix = context
          .context_schema
          .trans_table_context_schema
          .iter()
          .find(|prefix| &prefix.trans_table_name == trans_table_name)
          .unwrap()
          .clone();
        let child_query = msg::SuperSimpleTransTableSelectQuery {
          location_prefix: location_prefix.clone(),
          context: context.deref().clone(),
          sql_query: child_sql_query.clone(),
          query_plan,
        };

        // Construct PerformQuery
        let general_query = msg::GeneralQuery::SuperSimpleTransTableSelectQuery(child_query);
        let child_query_id = mk_qid(ctx.rand);
        let perform_query = msg::PerformQuery {
          root_query_path: self.root_query_path.clone(),
          sender_path: sender_path.clone(),
          query_id: child_query_id.clone(),
          tier_map: self.tier_map.clone(),
          query: general_query.clone(),
        };

        // Send out PerformQuery, wrapping it according to if the
        // TransTable is on a Slave or a Tablet
        let node_group_id = location_prefix.source.clone();
        ctx.send_to_node(node_group_id.clone(), CommonQuery::PerformQuery(perform_query));

        // Add the TabletGroup into the TMStatus.
        tm_status.node_group_ids.insert(node_group_id, child_query_id.clone());
        tm_status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
      }
    };

    // Create a SubqueryStatus and move the GRQueryES to the next Stage.
    self.state = GRExecutionS::ReadStage(ReadStage {
      stage_idx,
      parent_context_map,
      pending_status: SubqueryPending { context: context.clone(), query_id: tm_query_id.clone() },
    });

    // Return the TMStatus for the parent Server to execute.
    GRQueryAction::ExecuteTMStatus(tm_status)
  }
}
