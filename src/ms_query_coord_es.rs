use crate::col_usage::{
  external_trans_table_collecting_cb, trans_table_collecting_cb, QueryElement, QueryIterator,
};
use crate::common::{
  lookup, merge_table_views, mk_qid, unexpected_branch, FullGen, OrigP, QueryPlan, QueryResult,
  Timestamp,
};
use crate::common::{
  ColName, Context, ContextRow, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, QueryId,
  SlaveGroupId, TQueryPath, TablePath, TableView, TabletGroupId, TierMap, TransTableLocationPrefix,
  TransTableName,
};
use crate::common::{CoreIOCtx, RemoteLeaderChangedPLm};
use crate::coord::CoordContext;
use crate::expression::EvalError;
use crate::join_read_es::JoinReadES;
use crate::master_query_planning_es::{master_query_planning, ColPresenceReq, StaticDBSchemaView};
use crate::message as msg;
use crate::server::{CTServerContext, CommonQuery, ServerContextBase};
use crate::sql_ast::iast;
use crate::sql_ast::proc;
use crate::table_read_es::perform_aggregation;
use crate::tm_status::{SendHelper, TMStatus};
use crate::trans_table_read_es::TransTableSource;
use sqlparser::test_utils::table;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  MSCoordES
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CoordQueryPlan {
  all_tier_maps: BTreeMap<TransTableName, TierMap>,
  query_leader_map: BTreeMap<SlaveGroupId, LeadershipId>,
  table_location_map: BTreeMap<TablePath, FullGen>,
  col_presence_req: BTreeMap<TablePath, ColPresenceReq>,
}

#[derive(Debug)]
pub struct Stage {
  stage_idx: usize,
  /// Here, `stage_query_id` is the QueryId of the TMStatus
  stage_query_id: QueryId,
}

#[derive(Debug)]
pub enum CoordState {
  Start,
  Stage(Stage),
  Done,
}

#[derive(Debug)]
pub struct MSCoordES {
  // Metadata copied from outside.
  pub timestamp: Timestamp,

  pub query_id: QueryId,
  pub sql_query: iast::Query,
  pub ms_query: proc::MSQuery,

  // Results of the query planning.
  pub query_plan: CoordQueryPlan,

  // The dynamically evolving fields.
  pub all_rms: BTreeSet<TQueryPath>,
  pub trans_table_views: Vec<(TransTableName, TableView)>,
  pub state: CoordState,

  /// Recall that since we remove a `TQueryPath` when its Leadership changes, that means that
  /// the LeadershipId of the PaxosGroup of a `TQueryPath`s here is the same as the one
  /// when this `TQueryPath` came in.
  pub registered_queries: BTreeSet<TQueryPath>,
}

impl TransTableSource for MSCoordES {
  fn get_instance(&self, trans_table_name: &TransTableName, idx: usize) -> &TableView {
    assert_eq!(idx, 0);
    lookup(&self.trans_table_views, trans_table_name).unwrap()
  }

  fn get_schema(&self, trans_table_name: &TransTableName) -> &Vec<Option<ColName>> {
    lookup(&self.ms_query.trans_tables, trans_table_name).unwrap().schema()
  }
}

#[derive(Debug)]
pub enum FullMSCoordES {
  QueryPlanning(QueryPlanningES),
  Executing(MSCoordES),
}

pub enum MSQueryCoordAction {
  /// This tells the parent Server to execute the given TMStatus.
  ExecuteTMStatus(TMStatus),
  /// This tells the parent Server to execute the given JoinReadES.
  ExecuteJoinReadES(JoinReadES),
  /// Indicates that a valid MSCoordES was successful, and was ECU.
  Success(Vec<TQueryPath>, iast::Query, QueryResult, Timestamp),
  /// Indicates that a valid MSCoordES was unsuccessful and there is no
  /// chance of success, and was ECU.
  FatalFailure(msg::ExternalAbortedData),
  /// Indicates that a valid MSCoordES was unsuccessful, but that there is a chance
  /// of success if it were repeated at a higher timestamp. If the `bool` is `true`,
  /// then on the next try, we should forcefully do a `MasterQueryPlanning` (even
  /// if the local `GossipData` appears to be sufficient).
  NonFatalFailure(bool),
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl FullMSCoordES {
  /// Start the FullMSCoordES
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    start_with_master_query_planning: bool,
  ) -> Option<MSQueryCoordAction> {
    let plan_es = cast!(FullMSCoordES::QueryPlanning, self)?;
    let action = if start_with_master_query_planning {
      plan_es.perform_master_query_planning(ctx, io_ctx)
    } else {
      plan_es.start(ctx, io_ctx)
    };
    self.handle_planning_action(ctx, io_ctx, action)
  }

  /// Handle Master Response, routing it to the QueryPlanning.
  pub fn handle_master_response<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    master_qid: QueryId,
    result: msg::MasteryQueryPlanningResult,
  ) -> Option<MSQueryCoordAction> {
    let plan_es = cast!(FullMSCoordES::QueryPlanning, self)?;
    let action = plan_es.handle_master_query_plan(ctx, io_ctx, master_qid, result);
    self.handle_planning_action(ctx, io_ctx, action)
  }

  /// Handle the `action` sent back by the `MSQueryCoordPlanningES`.
  fn handle_planning_action<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    action: Option<QueryPlanningAction>,
  ) -> Option<MSQueryCoordAction> {
    let plan_es = cast!(FullMSCoordES::QueryPlanning, self)?;
    match action {
      None => None,
      Some(QueryPlanningAction::Success(ms_query, query_plan)) => {
        *self = FullMSCoordES::Executing(MSCoordES {
          timestamp: plan_es.timestamp.clone(),
          query_id: plan_es.query_id.clone(),
          sql_query: plan_es.sql_query.clone(),
          ms_query,
          query_plan: query_plan.clone(),
          all_rms: Default::default(),
          trans_table_views: vec![],
          state: CoordState::Start,
          registered_queries: Default::default(),
        });

        // Move the ES onto the next stage.
        self.advance(ctx, io_ctx)
      }
      Some(QueryPlanningAction::Failed(error)) => {
        // Here, the QueryPlanning had failed. We do not need to ECU because
        // QueryPlanning will be in Done.
        Some(MSQueryCoordAction::FatalFailure(error))
      }
    }
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    tm_qid: QueryId,
    new_rms: BTreeSet<TQueryPath>,
    pre_agg_table_views: Vec<TableView>,
  ) -> Option<MSQueryCoordAction> {
    let es = cast!(FullMSCoordES::Executing, self)?;
    let coord_stage = cast!(CoordState::Stage, &es.state)?;
    check!(coord_stage.stage_query_id == tm_qid);

    // Combine the results into a single one
    let (trans_table_name, stage) = es.ms_query.trans_tables.get(coord_stage.stage_idx).unwrap();
    let table_views = match match stage {
      proc::MSQueryStage::TableSelect(sql_query) => {
        perform_aggregation(sql_query, pre_agg_table_views)
      }
      proc::MSQueryStage::TransTableSelect(sql_query) => {
        perform_aggregation(sql_query, pre_agg_table_views)
      }
      proc::MSQueryStage::JoinSelect(_) => {
        // TODO: do properly
        unimplemented!()
      }
      proc::MSQueryStage::Update(_) => Ok(pre_agg_table_views),
      proc::MSQueryStage::Insert(_) => Ok(pre_agg_table_views),
      proc::MSQueryStage::Delete(_) => Ok(pre_agg_table_views),
    } {
      Ok(result) => result,
      Err(eval_error) => {
        self.exit_and_clean_up(ctx, io_ctx);
        return Some(MSQueryCoordAction::FatalFailure(
          msg::ExternalAbortedData::QueryExecutionError(msg::ExternalQueryError::RuntimeError {
            msg: format!("Aggregation of MSQueryES failed with error {:?}", eval_error),
          }),
        ));
      }
    };

    // Recall that since we only send out one ContextRow, there should only be one TableView.
    debug_assert_eq!(table_views.len(), 1);

    // Then, the results to the `trans_table_views`
    let table_view = table_views.into_iter().next().unwrap();
    es.trans_table_views.push((trans_table_name.clone(), table_view));
    es.all_rms.extend(new_rms);
    self.advance(ctx, io_ctx)
  }

  /// This is called when the TMStatus has aborted.
  pub fn handle_tm_aborted<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    aborted_data: msg::AbortedData,
  ) -> Option<MSQueryCoordAction> {
    // Interpret the `aborted_data`.
    match aborted_data {
      // `TypeError` and `RuntimeError` both imply an unrecoverable error, since trying again at
      // a higher timestamp will not generally fix the issue. Thus we ECU and return accordingly.
      msg::AbortedData::QueryError(msg::QueryError::TypeError { msg: err_msg }) => {
        self.exit_and_clean_up(ctx, io_ctx);
        Some(MSQueryCoordAction::FatalFailure(msg::ExternalAbortedData::QueryExecutionError(
          msg::ExternalQueryError::TypeError { msg: err_msg },
        )))
      }
      // TODO: do not return the query error unless this run of MSCoordES had used
      //  the MasterQueryPlan. (Later on, we don't need to rely on the Master for checking
      //  metadata matches.
      msg::AbortedData::QueryError(msg::QueryError::RuntimeError { msg: err_msg }) => {
        self.exit_and_clean_up(ctx, io_ctx);
        Some(MSQueryCoordAction::FatalFailure(msg::ExternalAbortedData::QueryExecutionError(
          msg::ExternalQueryError::RuntimeError { msg: err_msg },
        )))
      }
      msg::AbortedData::QueryError(msg::QueryError::WriteRegionConflictWithSubsequentRead)
      | msg::AbortedData::QueryError(msg::QueryError::DeadlockSafetyAbortion)
      | msg::AbortedData::QueryError(msg::QueryError::TimestampConflict)
      // TODO: Verify this code in the below case.
      | msg::AbortedData::QueryError(msg::QueryError::InvalidLeadershipId)=> {
        // This implies a recoverable failure, so we ECU and return accordingly.
        self.exit_and_clean_up(ctx, io_ctx);
        Some(MSQueryCoordAction::NonFatalFailure(false))
      }
      | msg::AbortedData::QueryError(msg::QueryError::InvalidQueryPlan) => {
        // Unlike the above, we want to forcefully do a MasterQueryPlanning
        self.exit_and_clean_up(ctx, io_ctx);
        Some(MSQueryCoordAction::NonFatalFailure(true))
      }
      // Recall that LateralErrors should never make it back to the MSCoordES.
      msg::AbortedData::QueryError(msg::QueryError::LateralError) => unexpected_branch(),
    }
  }

  /// This is called when one of the remote node's Leadership changes beyond the
  /// LeadershipId that we had sent a PerformQuery to.
  pub fn handle_tm_remote_leadership_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> Option<MSQueryCoordAction> {
    let es = cast_safe!(FullMSCoordES::Executing, self)?;
    let stage = cast_safe!(CoordState::Stage, &es.state)?;
    let stage_idx = stage.stage_idx.clone();
    self.process_ms_query_stage(ctx, io_ctx, stage_idx)
  }

  // Handle a RegisterQuery sent by an MSQuery to an MSCoordES.
  pub fn handle_register_query(&mut self, register: msg::RegisterQuery) {
    match self {
      FullMSCoordES::Executing(es) => {
        es.registered_queries.insert(register.query_path);
      }
      _ => {}
    }
  }

  /// Handle a RemoteLeaderChanged
  pub fn remote_leader_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> Option<MSQueryCoordAction> {
    match self {
      FullMSCoordES::QueryPlanning(es) => {
        if remote_leader_changed.gid == PaxosGroupId::Master {
          let action = es.master_leader_changed(ctx, io_ctx);
          self.handle_planning_action(ctx, io_ctx, action)
        } else {
          None
        }
      }
      FullMSCoordES::Executing(es) => {
        // Compute the set of RegisteredQuery's that can be removed due to the Leadership change.
        let mut to_remove = Vec::<TQueryPath>::new();
        for registered_query in &es.registered_queries {
          if &remote_leader_changed.gid == &registered_query.node_path.sid.to_gid() {
            to_remove.push(registered_query.clone())
          }
        }
        // Remove them from the ES.
        for registered_query in to_remove {
          es.registered_queries.remove(&registered_query);
        }
        None
      }
    }
  }

  /// Handles the GossipData changing.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> Option<MSQueryCoordAction> {
    let es = cast_safe!(FullMSCoordES::QueryPlanning, self)?;
    let action = es.gossip_data_changed(ctx, io_ctx);
    self.handle_planning_action(ctx, io_ctx, action)
  }

  /// This function accepts the results for the subquery, and then decides either
  /// to move onto the next stage, or start 2PC to commit the change.
  fn advance<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> Option<MSQueryCoordAction> {
    // Compute the next stage
    let es = cast!(FullMSCoordES::Executing, self)?;
    let next_stage_idx = match &es.state {
      CoordState::Start => 0,
      CoordState::Stage(stage) => stage.stage_idx + 1,
      _ => return unexpected_branch(),
    };

    if next_stage_idx < es.ms_query.trans_tables.len() {
      self.process_ms_query_stage(ctx, io_ctx, next_stage_idx)
    } else {
      // Check that none of the Leaderships in `all_rms` have changed.
      for rm in &es.all_rms {
        let orig_lid = es.query_plan.query_leader_map.get(&rm.node_path.sid).unwrap();
        let cur_lid = ctx.leader_map.get(&rm.node_path.sid.to_gid()).unwrap();
        if orig_lid != cur_lid {
          // If a Leadership has changed, we abort and retry this MSCoordES.
          self.exit_and_clean_up(ctx, io_ctx);
          return Some(MSQueryCoordAction::NonFatalFailure(false));
        }
      }

      // Cancel all RegisteredQueries that are not also an RM in the upcoming Paxos2PC.
      //
      // Note: For all the Registered Queries we do not cancel here, we can think of them as
      // being handed off to FinishQueryTMES to finish off. If that is implemented correctly,
      // then this ES will have done its duty of making sure all RegisteredQueries get cleaned up.
      for registered_query in &es.registered_queries {
        if !es.all_rms.contains(registered_query) {
          ctx.send_to_ct(
            io_ctx,
            registered_query.clone().into_ct().node_path,
            CommonQuery::CancelQuery(msg::CancelQuery {
              query_id: registered_query.query_id.clone(),
            }),
          )
        }
      }

      // Finally, we go to Done and return the appropriate TableView.
      let schema = es.get_schema(&es.ms_query.returning).clone();
      let (_, data) = es
        .trans_table_views
        .iter()
        .find(|(trans_table_name, _)| trans_table_name == &es.ms_query.returning)
        .unwrap()
        .clone();
      es.state = CoordState::Done;
      Some(MSQueryCoordAction::Success(
        es.all_rms.iter().cloned().collect(),
        es.sql_query.clone(),
        QueryResult { schema, data },
        es.timestamp.clone(),
      ))
    }
  }

  /// This function advances the given MSCoordES at `query_id` to the next
  /// `Stage` with index `stage_idx`.
  fn process_ms_query_stage<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    stage_idx: usize,
  ) -> Option<MSQueryCoordAction> {
    let es = cast!(FullMSCoordES::Executing, self)?;

    // Get the corresponding MSQueryStage and ColUsageNode.
    let (trans_table_name, stage) = es.ms_query.trans_tables.get(stage_idx).unwrap();

    // Compute the Context for this stage. Recall there must be exactly one row.
    let mut trans_table_names = Vec::<TransTableName>::new();
    {
      let it = QueryIterator::new();
      let mut trans_table_container = BTreeSet::<TransTableName>::new();
      it.iterate_ms_query_stage(&mut trans_table_collecting_cb(&mut trans_table_container), &stage);
      let mut external_trans_table = BTreeSet::<TransTableName>::new();
      it.iterate_ms_query_stage(
        &mut external_trans_table_collecting_cb(&trans_table_container, &mut external_trans_table),
        &stage,
      );
      trans_table_names.extend(external_trans_table.into_iter());
    }

    let mut context = Context::default();
    let mut context_row = ContextRow::default();
    for trans_table_name in &trans_table_names {
      context.context_schema.trans_table_context_schema.push(TransTableLocationPrefix {
        source: ctx.mk_this_query_path(es.query_id.clone()),
        trans_table_name: trans_table_name.clone(),
      });
      context_row.trans_table_context_row.push(0);
    }
    context.context_rows.push(context_row);

    // Construct the QueryPlan
    let mut query_leader_map = es.query_plan.query_leader_map.clone();
    query_leader_map
      .insert(ctx.this_sid.clone(), ctx.leader_map.get(&ctx.this_gid).unwrap().clone());
    let query_plan = QueryPlan {
      tier_map: es.query_plan.all_tier_maps.get(trans_table_name).unwrap().clone(),
      query_leader_map: query_leader_map.clone(),
      table_location_map: es.query_plan.table_location_map.clone(),
      col_presence_req: es.query_plan.col_presence_req.clone(),
    };

    // Construct the TMStatus that is going to be used to coordinate this stage
    let root_query_path = ctx.mk_query_path(es.query_id.clone());
    let mut tm_status =
      TMStatus::new(io_ctx, root_query_path.clone(), OrigP::new(es.query_id.clone()));

    // Send out the PerformQuery.
    match stage {
      proc::MSQueryStage::TableSelect(select) => {
        // Here, we must do a TableSelectQuery.
        let general_query = msg::GeneralQuery::TableSelectQuery(msg::TableSelectQuery {
          timestamp: es.timestamp.clone(),
          context: context.clone(),
          sql_query: select.clone(),
          query_plan,
        });
        let full_gen = es.query_plan.table_location_map.get(&select.from.table_path).unwrap();
        let tids = ctx.get_min_tablets(&select.from, full_gen, &select.selection);
        let helper = SendHelper::TableQuery(general_query, tids);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx, io_ctx);
          Some(MSQueryCoordAction::NonFatalFailure(false))
        } else {
          // Populate the TMStatus accordingly.
          es.state =
            CoordState::Stage(Stage { stage_idx, stage_query_id: tm_status.query_id().clone() });
          Some(MSQueryCoordAction::ExecuteTMStatus(tm_status))
        }
      }
      proc::MSQueryStage::TransTableSelect(select) => {
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
          context: context.clone(),
          sql_query: select.clone(),
          query_plan,
        });
        let helper = SendHelper::TransTableQuery(general_query, location_prefix);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx, io_ctx);
          Some(MSQueryCoordAction::NonFatalFailure(false))
        } else {
          // Populate the TMStatus accordingly.
          es.state =
            CoordState::Stage(Stage { stage_idx, stage_query_id: tm_status.query_id().clone() });
          Some(MSQueryCoordAction::ExecuteTMStatus(tm_status))
        }
      }
      proc::MSQueryStage::JoinSelect(select) => {
        let child_es = JoinReadES::create(
          io_ctx,
          root_query_path,
          es.timestamp.clone(),
          Rc::new(context),
          select.clone(),
          query_plan.clone(),
          OrigP::new(es.query_id.clone()),
        );

        // Move the GRQueryES to the next Stage.
        es.state =
          CoordState::Stage(Stage { stage_idx, stage_query_id: child_es.query_id().clone() });

        // Return the subqueries for the parent server to execute.
        Some(MSQueryCoordAction::ExecuteJoinReadES(child_es))
      }
      proc::MSQueryStage::Update(update_query) => {
        let general_query = msg::GeneralQuery::UpdateQuery(msg::UpdateQuery {
          timestamp: es.timestamp.clone(),
          context: context.clone(),
          sql_query: update_query.clone(),
          query_plan,
        });
        let table_source = &update_query.table;
        let gen = es.query_plan.table_location_map.get(&table_source.table_path).unwrap();
        let tids = ctx.get_min_tablets(table_source, gen, &update_query.selection);
        let helper = SendHelper::TableQuery(general_query, tids);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx, io_ctx);
          Some(MSQueryCoordAction::NonFatalFailure(false))
        } else {
          // Populate the TMStatus accordingly.
          es.state =
            CoordState::Stage(Stage { stage_idx, stage_query_id: tm_status.query_id().clone() });
          Some(MSQueryCoordAction::ExecuteTMStatus(tm_status))
        }
      }
      proc::MSQueryStage::Insert(insert_query) => {
        let general_query = msg::GeneralQuery::InsertQuery(msg::InsertQuery {
          timestamp: es.timestamp.clone(),
          context: context.clone(),
          sql_query: insert_query.clone(),
          query_plan,
        });
        // As an optimization, for inserts, we can evaluate the VALUES and select only the
        // Tablets that are written to. For now, we just consider all.
        let table_source = &insert_query.table;
        let gen = es.query_plan.table_location_map.get(&table_source.table_path).unwrap();
        let tids = ctx.get_all_tablets(table_source, gen);
        let helper = SendHelper::TableQuery(general_query, tids);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx, io_ctx);
          Some(MSQueryCoordAction::NonFatalFailure(false))
        } else {
          // Populate the TMStatus accordingly.
          es.state =
            CoordState::Stage(Stage { stage_idx, stage_query_id: tm_status.query_id().clone() });
          Some(MSQueryCoordAction::ExecuteTMStatus(tm_status))
        }
      }
      proc::MSQueryStage::Delete(delete_query) => {
        let general_query = msg::GeneralQuery::DeleteQuery(msg::DeleteQuery {
          timestamp: es.timestamp.clone(),
          context: context.clone(),
          sql_query: delete_query.clone(),
          query_plan,
        });
        let table_source = &delete_query.table;
        let gen = es.query_plan.table_location_map.get(&table_source.table_path).unwrap();
        let tids = ctx.get_min_tablets(table_source, gen, &delete_query.selection);
        let helper = SendHelper::TableQuery(general_query, tids);
        if !tm_status.send_general(ctx, io_ctx, &query_leader_map, helper) {
          self.exit_and_clean_up(ctx, io_ctx);
          Some(MSQueryCoordAction::NonFatalFailure(false))
        } else {
          // Populate the TMStatus accordingly.
          es.state =
            CoordState::Stage(Stage { stage_idx, stage_query_id: tm_status.query_id().clone() });
          Some(MSQueryCoordAction::ExecuteTMStatus(tm_status))
        }
      }
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, ctx: &mut CoordContext, io_ctx: &mut IO) {
    match self {
      FullMSCoordES::QueryPlanning(plan_es) => plan_es.exit_and_clean_up(ctx, io_ctx),
      FullMSCoordES::Executing(es) => {
        match &es.state {
          CoordState::Start => {}
          CoordState::Stage(_) => {
            // Clean up any Registered Queries in the MSCoordES. The `registered_queries` docs
            // describe why `send_to_ct` sends the message to the right PaxosNode.
            for registered_query in &es.registered_queries {
              ctx.send_to_ct(
                io_ctx,
                registered_query.clone().into_ct().node_path,
                CommonQuery::CancelQuery(msg::CancelQuery {
                  query_id: registered_query.query_id.clone(),
                }),
              )
            }
          }
          CoordState::Done => {}
        }
        es.state = CoordState::Done
      }
    }
  }

  /// Case the FullMSCoordES to the Executing state.
  pub fn to_exec(&self) -> &MSCoordES {
    cast!(FullMSCoordES::Executing, self).unwrap()
  }
}

// -----------------------------------------------------------------------------------------------
//  QueryPlanning
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct MasterQueryPlanning {
  master_query_id: QueryId,
}

#[derive(Debug)]
pub struct GossipDataWaiting {
  master_query_plan: msg::MasterQueryPlan,
}

#[derive(Debug)]
pub enum QueryPlanningS {
  Start,
  MasterQueryPlanning(MasterQueryPlanning),
  GossipDataWaiting(GossipDataWaiting),
  Done,
}

#[derive(Debug)]
pub struct QueryPlanningES {
  pub timestamp: Timestamp,
  /// The query to do the planning with.
  pub sql_query: iast::Query,
  /// The OrigP of the Task holding this MSQueryCoordPlanningES
  pub query_id: QueryId,
  /// Used for managing MasterQueryPlanning
  pub state: QueryPlanningS,
}

pub enum QueryPlanningAction {
  /// Indicates the that QueryPlanningES has computed a valid query, and it is stored
  /// in the `query_plan` field.
  Success(proc::MSQuery, CoordQueryPlan),
  /// Indicates that a valid QueryPlan couldn't be computed. The ES will have
  /// also been cleaned up.
  Failed(msg::ExternalAbortedData),
}

impl QueryPlanningES {
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> Option<QueryPlanningAction> {
    check!(matches!(&self.state, QueryPlanningS::Start));
    let gossip = ctx.gossip.get();
    let mut view = StaticDBSchemaView {
      db_schema: gossip.db_schema,
      table_generation: gossip.table_generation,
      timestamp: self.timestamp.clone(),
      col_presence_req: Default::default(),
    };

    match master_query_planning(view, &self.sql_query) {
      Ok(master_query_plan) => self.finish_master_query_plan(ctx, master_query_plan),
      Err(_) => return self.perform_master_query_planning(ctx, io_ctx),
    }
  }

  /// Send a `PerformMasterQueryPlanning` and go to the `MasterQueryPlanning` state.
  fn perform_master_query_planning<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> Option<QueryPlanningAction> {
    let master_query_id = mk_qid(io_ctx.rand());
    let sender_path = ctx.mk_query_path(self.query_id.clone());
    ctx.send_to_master(
      io_ctx,
      msg::MasterRemotePayload::MasterQueryPlanning(msg::MasterQueryPlanningRequest::Perform(
        msg::PerformMasterQueryPlanning {
          sender_path,
          query_id: master_query_id.clone(),
          timestamp: self.timestamp.clone(),
          sql_query: self.sql_query.clone(),
        },
      )),
    );

    // Advance Planning State.
    self.state = QueryPlanningS::MasterQueryPlanning(MasterQueryPlanning { master_query_id });
    None
  }

  /// Here, we have verified all `TablePath`s are present in the GossipData.
  fn finish_master_query_plan(
    &mut self,
    ctx: &mut CoordContext,
    master_query_plan: msg::MasterQueryPlan,
  ) -> Option<QueryPlanningAction> {
    self.state = QueryPlanningS::Done;
    Some(QueryPlanningAction::Success(
      master_query_plan.ms_query,
      CoordQueryPlan {
        all_tier_maps: master_query_plan.all_tier_maps,
        query_leader_map: self.compute_query_leader_map(ctx, &master_query_plan.table_location_map),
        table_location_map: master_query_plan.table_location_map,
        col_presence_req: master_query_plan.col_presence_req,
      },
    ))
  }

  /// Computes the Leaderships of `SlaveGroupId`s whose LeadershipChanges would require us to
  /// retry the whole MSCoordES. Generally, this only needs to contain `SlaveGroupId`s
  /// that would contain a write (since an MSQueryES should be reused many times).
  ///
  /// Note: For simplicity, we just take the set of `SlaveGroupId`s for every `TablePath`
  /// in the MSQuery.
  ///
  /// Preconditions:
  ///   1. The `Gen`s must be present in the local `GossipData` (which might not be the case if
  ///      `table_location_map` was sent from the Master).
  fn compute_query_leader_map(
    &mut self,
    ctx: &mut CoordContext,
    table_location_map: &BTreeMap<TablePath, FullGen>,
  ) -> BTreeMap<SlaveGroupId, LeadershipId> {
    let gossip = ctx.gossip.get();
    let mut query_leader_map = BTreeMap::<SlaveGroupId, LeadershipId>::new();
    for (table_path, gen) in table_location_map {
      let shards = gossip.sharding_config.get(&(table_path.clone(), gen.clone())).unwrap();
      for (_, tid) in shards.clone() {
        let sid = gossip.tablet_address_config.get(&tid).unwrap().clone();
        let lid = ctx.leader_map.get(&sid.to_gid()).unwrap();
        query_leader_map.insert(sid.clone(), lid.clone());
      }
    }
    query_leader_map
  }

  /// Check if the local `GossipData` is sufficiently up-to-date to handle the
  /// `MasterQueryPlan`. In particular, we check that the local `GossipData` knows has all the
  /// desired (TablePath, Gen)s so that we can communicate with the Tablets that contain them.
  fn check_local_gossip(
    &self,
    ctx: &CoordContext,
    master_query_plan: &msg::MasterQueryPlan,
  ) -> bool {
    let gossip = ctx.gossip.get();
    for (table_path, gen) in &master_query_plan.table_location_map {
      if let Some(cur_gen) = gossip.table_generation.static_read(&table_path, &self.timestamp) {
        if cur_gen < gen {
          return false;
        }
      } else {
        return false;
      }
    }
    true
  }

  /// Handles the QueryPlan constructed by the Master.
  pub fn handle_master_query_plan<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    master_qid: QueryId,
    result: msg::MasteryQueryPlanningResult,
  ) -> Option<QueryPlanningAction> {
    let state = cast!(QueryPlanningS::MasterQueryPlanning, &self.state)?;
    check!(state.master_query_id == master_qid);
    match result {
      msg::MasteryQueryPlanningResult::MasterQueryPlan(master_query_plan) => {
        // By now, we know the QueryPlanning can succeed. However, we need to make sure
        // the local GossipData is recent enough so we can communicate with the shards.
        if self.check_local_gossip(ctx, &master_query_plan) {
          self.finish_master_query_plan(ctx, master_query_plan)
        } else {
          // Otherwise, this node's GossipData is too far out-of-date. We send
          // a MasterGossipRequest and go to GossipDataWaiting.
          let sender_path = ctx.this_sid.clone();
          ctx.send_to_master(
            io_ctx,
            msg::MasterRemotePayload::MasterGossipRequest(msg::MasterGossipRequest { sender_path }),
          );

          self.state = QueryPlanningS::GossipDataWaiting(GossipDataWaiting { master_query_plan });
          None
        }
      }
      msg::MasteryQueryPlanningResult::QueryPlanningError(error) => {
        self.state = QueryPlanningS::Done;
        Some(QueryPlanningAction::Failed(msg::ExternalAbortedData::QueryPlanningError(error)))
      }
    }
  }

  /// Handles the GossipData changing.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    _: &mut IO,
  ) -> Option<QueryPlanningAction> {
    let last_state = cast_safe!(QueryPlanningS::GossipDataWaiting, &self.state)?;
    // We must check again whether the GossipData is new enough, since this is called
    // for any GossipData update whatsoever (not just the one resulting from the
    // MasterGossipRequest we sent out above).
    if self.check_local_gossip(ctx, &last_state.master_query_plan) {
      self.finish_master_query_plan(ctx, last_state.master_query_plan.clone())
    } else {
      // Otherwise, are still out-of-date, so we stay in GossipDataWaiting.
      None
    }
  }

  /// This is called when there is a Leadership change in the Master PaxosGroup.
  pub fn master_leader_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> Option<QueryPlanningAction> {
    if let QueryPlanningS::MasterQueryPlanning(_) = &self.state {
      // This means we have to resend the PerformMasterQueryPlanning to the new Leader.
      self.perform_master_query_planning(ctx, io_ctx)
    } else {
      None
    }
  }

  /// This Exits and Cleans up this QueryPlanningES
  pub fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, ctx: &mut CoordContext, io_ctx: &mut IO) {
    match &self.state {
      QueryPlanningS::Start => {}
      QueryPlanningS::MasterQueryPlanning(MasterQueryPlanning { master_query_id }) => {
        ctx.send_to_master(
          io_ctx,
          msg::MasterRemotePayload::MasterQueryPlanning(msg::MasterQueryPlanningRequest::Cancel(
            msg::CancelMasterQueryPlanning { query_id: master_query_id.clone() },
          )),
        );
      }
      QueryPlanningS::GossipDataWaiting(_) => {}
      QueryPlanningS::Done => {}
    }
    self.state = QueryPlanningS::Done;
  }
}
