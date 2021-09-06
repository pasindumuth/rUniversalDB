use crate::col_usage::{
  collect_select_subqueries, collect_update_subqueries, node_external_trans_tables,
  nodes_external_trans_tables, FrozenColUsageNode,
};
use crate::common::{
  lookup, lookup_pos, mk_qid, IOTypes, NetworkOut, OrigP, QueryPlan, QueryPlan2, TMStatus,
};
use crate::model::common::{
  proc, ColName, Context, ContextRow, ContextSchema, Gen, NodeGroupId, NodePath, QueryId,
  QueryPath, TableView, TabletGroupId, TierMap, Timestamp, TransTableLocationPrefix,
  TransTableName,
};
use crate::model::message as msg;
use crate::server::{CommonQuery, ServerContext};
use crate::tablet::{SingleSubqueryStatus, SubqueryPending};
use crate::trans_table_read_es::TransTableSource;
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
  Success(GRQueryResult),
  /// This indicates that the GRQueryES failed, where the Context was insufficient.
  /// Here, the GRQueryES can just be trivially erased from the parent.
  InternalColumnsDNE(Vec<ColName>),
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
  Done,
}

// Recall that the elements don't need to preserve the order of the TransTables, since the
// sql_query does that for us (thus, we can use HashMaps).
#[derive(Debug)]
pub struct GRQueryPlan {
  pub gossip_gen: Gen,
  /// This consists of a superset of the external `TransTableName`s used within `col_usage_nodes`.
  pub trans_table_schemas: HashMap<TransTableName, Vec<ColName>>,
  pub col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
}

#[derive(Debug)]
pub struct GRQueryES {
  /// This is only here so it can be forwarded to child queries.
  pub root_query_path: QueryPath,
  /// This is only here so it can be forwarded to child queries.
  pub tier_map: TierMap,
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

pub enum TransTableIdx {
  External(usize),
  Local(usize),
}

impl GRQueryES {
  /// Starts the GRQueryES from its initial state.
  pub fn start<T: IOTypes>(&mut self, ctx: &mut ServerContext<T>) -> GRQueryAction {
    self.advance(ctx)
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    tm_qid: QueryId,
    new_rms: HashSet<QueryPath>,
    (schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) -> GRQueryAction {
    let read_stage = cast!(GRExecutionS::ReadStage, &mut self.state).unwrap();

    // Extract the `context` from the SubqueryPending status and verify
    // that it corresponds to `table_views`.
    let pending_status = &read_stage.pending_status;
    assert_eq!(pending_status.query_id, tm_qid);
    assert_eq!(table_views.len(), pending_status.context.context_rows.len());

    // For now, just assert that the schema that we get corresponds to that in the QueryPlan.
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
          // If the Context has the missing columns, propagate the error up.
          self.state = GRExecutionS::Done;
          GRQueryAction::InternalColumnsDNE(rem_cols)
        } else {
          // If the GRQueryES Context is sufficient, we simply amend the new columns
          // to the Context and reprocess the ReadStage.
          let context_schema = &pending_status.context.context_schema;
          let mut context_cols = context_schema.column_context_schema.clone();
          context_cols.extend(missing_cols);
          let context_trans_tables = context_schema.trans_table_names();
          let stage_idx = read_stage.stage_idx.clone();
          self.process_gr_query_stage_simple(ctx, stage_idx, &context_cols, &context_trans_tables)
        }
      }
      msg::AbortedData::QueryError(query_error) => {
        // In the case of a QueryError, we just propagate it up.
        self.state = GRExecutionS::Done;
        GRQueryAction::QueryError(query_error)
      }
    }
  }

  /// This Exits and Cleans up this GRQueryES.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut ServerContext<T>) {
    match &self.state {
      GRExecutionS::Start => {}
      GRExecutionS::ReadStage(_) => {}
      GRExecutionS::MasterQueryReplanning(planning) => {
        ctx.network_output.send(
          &ctx.master_eid,
          msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
            msg::CancelMasterFrozenColUsage { query_id: planning.master_query_id.clone() },
          )),
        );
      }
      GRExecutionS::Done => {}
    };
    self.state = GRExecutionS::Done;
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
      self.state = GRExecutionS::Done;
      GRQueryAction::Success(GRQueryResult {
        new_rms: self.new_rms.clone(),
        schema: schema.clone(),
        result,
      })
    }
  }

  /// This function moves the GRQueryES to the Stage indicated by `stage_idx`.
  /// This index must be valid (i.e. be an actual stage).
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
    self.process_gr_query_stage_simple(ctx, stage_idx, &context_cols, &context_trans_tables)
  }

  /// This is a common function used for sending processing a GRQueryES stage that
  /// doesn't use the child QueryPlan to process the child ContextSchema, but rather
  /// uses the `context_cols` and `context_trans_table`. Note that by this point, both
  /// of these should be present in the parent `Context` and `new_trans_table_context`.
  fn process_gr_query_stage_simple<T: IOTypes>(
    &mut self,
    ctx: &mut ServerContext<T>,
    stage_idx: usize,
    col_names: &Vec<ColName>,
    trans_table_names: &Vec<TransTableName>,
  ) -> GRQueryAction {
    assert!(stage_idx < self.query_plan.col_usage_nodes.len());

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
            source: ctx.node_group_id(),
            query_id: self.query_id.clone(),
            trans_table_name: trans_table_name.clone(),
          });
        }
      }
    }

    // This contains the ContextRows of the child Context we're creating.
    let mut new_context_rows = Vec::<ContextRow>::new();
    // This maps the above ContextRows back to the index in which they appear.
    let mut reverse_map = HashMap::<ContextRow, usize>::new();
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

    // Construct the QueryPlan
    let mut child_trans_table_schemas = HashMap::<TransTableName, Vec<ColName>>::new();
    for trans_table_idx in &trans_table_name_indicies {
      match trans_table_idx {
        TransTableIdx::External(idx) => {
          let prefix = self.context.context_schema.trans_table_context_schema.get(*idx).unwrap();
          let schema = self.query_plan.trans_table_schemas.get(&prefix.trans_table_name).unwrap();
          child_trans_table_schemas.insert(prefix.trans_table_name.clone(), schema.clone());
        }
        TransTableIdx::Local(idx) => {
          let (trans_table_name, (schema, _)) = self.trans_table_views.get(*idx).unwrap();
          child_trans_table_schemas.insert(trans_table_name.clone(), schema.clone());
        }
      }
    }
    let (_, (_, child)) = self.query_plan.col_usage_nodes.get(stage_idx).unwrap();
    let query_plan = QueryPlan {
      gossip_gen: self.query_plan.gossip_gen.clone(),
      trans_table_schemas: child_trans_table_schemas,
      col_usage_node: child.clone(),
    };

    // Construct the TMStatus
    let tm_qid = mk_qid(ctx.rand);
    let child_qid = mk_qid(&mut ctx.rand);
    let mut tm_status = TMStatus {
      query_id: tm_qid.clone(),
      child_query_id: child_qid.clone(),
      new_rms: Default::default(),
      responded_count: 0,
      tm_state: Default::default(),
      orig_p: OrigP::new(self.query_id.clone()),
    };
    let sender_path = ctx.mk_query_path(tm_qid.clone());

    // Send out the PerformQuery and populate TMStatus accordingly.
    let (_, stage) = self.sql_query.trans_tables.get(stage_idx).unwrap();
    let child_sql_query = cast!(proc::GRQueryStage::SuperSimpleSelect, stage).unwrap();
    match &child_sql_query.from {
      proc::TableRef::TablePath(table_path) => {
        // Here, we must do a SuperSimpleTableSelectQuery.
        let child_query = msg::SuperSimpleTableSelectQuery {
          timestamp: self.timestamp.clone(),
          context: context.clone(),
          sql_query: child_sql_query.clone(),
          query_plan,
          query_plan2: QueryPlan2::new(),
        };

        let tids = ctx.get_min_tablets(table_path, &child_sql_query.selection);
        // Having non-empty `tids` solves the TMStatus deadlock and determining the child schema.
        assert!(tids.len() > 0);
        for tid in tids {
          // Construct PerformQuery
          let general_query = msg::GeneralQuery::SuperSimpleTableSelectQuery(child_query.clone());
          let perform_query = msg::PerformQuery {
            root_query_path: self.root_query_path.clone(),
            sender_path: sender_path.clone(),
            query_id: child_qid.clone(),
            tier_map: self.tier_map.clone(),
            query: general_query,
          };

          // Send out PerformQuery. Recall that this could only be a Tablet.
          let nid = NodeGroupId::Tablet(tid);
          ctx.send_to_node(nid.clone(), CommonQuery::PerformQuery(perform_query));

          // Add the TabletGroup into the TMStatus.
          tm_status.tm_state.insert(ctx.core_ctx().mk_node_path(nid), None);
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
          context: context.clone(),
          sql_query: child_sql_query.clone(),
          query_plan,
          query_plan2: QueryPlan2::new(),
        };

        // Construct PerformQuery
        let general_query = msg::GeneralQuery::SuperSimpleTransTableSelectQuery(child_query);
        let perform_query = msg::PerformQuery {
          root_query_path: self.root_query_path.clone(),
          sender_path: sender_path.clone(),
          query_id: child_qid.clone(),
          tier_map: self.tier_map.clone(),
          query: general_query,
        };

        // Send out PerformQuery. Recall that this could be a Slave or a Tablet.
        let nid = location_prefix.source.clone();
        ctx.send_to_node(nid.clone(), CommonQuery::PerformQuery(perform_query));

        // Add the TabletGroup into the TMStatus.
        tm_status.tm_state.insert(ctx.core_ctx().mk_node_path(nid), None);
      }
    };

    // Create a SubqueryStatus and move the GRQueryES to the next Stage.
    self.state = GRExecutionS::ReadStage(ReadStage {
      stage_idx,
      parent_context_map,
      pending_status: SubqueryPending { context: Rc::new(context), query_id: tm_qid.clone() },
    });

    // Return the TMStatus for the parent Server to execute.
    GRQueryAction::ExecuteTMStatus(tm_status)
  }
}
