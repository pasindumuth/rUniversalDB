use crate::col_usage::{
  collect_table_paths, compute_all_tier_maps, compute_query_plan_data, iterate_stage_ms_query,
  node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode, GeneralStage,
};
use crate::common::{lookup, mk_qid, OrigP, QueryPlan, TMStatus};
use crate::common::{CoreIOCtx, RemoteLeaderChangedPLm};
use crate::coord::CoordContext;
use crate::model::common::{
  proc, CTQueryPath, ColName, Context, ContextRow, EndpointId, Gen, LeadershipId, NodeGroupId,
  PaxosGroupId, QueryId, RequestId, SlaveGroupId, TQueryPath, TablePath, TableView, TabletGroupId,
  TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::message as msg;
use crate::model::message::MasteryQueryPlanningResult;
use crate::server::{weak_contains_col, CommonQuery, ServerContextBase};
use crate::trans_table_read_es::TransTableSource;
use std::collections::{HashMap, HashSet};

// -----------------------------------------------------------------------------------------------
//  MSCoordES
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CoordQueryPlan {
  all_tier_maps: HashMap<TransTableName, TierMap>,
  query_leader_map: HashMap<SlaveGroupId, LeadershipId>,
  table_location_map: HashMap<TablePath, Gen>,
  extra_req_cols: HashMap<TablePath, Vec<ColName>>,
  col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
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

impl CoordState {
  fn stage_idx(&self) -> Option<usize> {
    match self {
      CoordState::Start => Some(0),
      CoordState::Stage(stage) => Some(stage.stage_idx),
      _ => None,
    }
  }

  fn stage_query_id(&self) -> Option<QueryId> {
    match self {
      CoordState::Stage(stage) => Some(stage.stage_query_id.clone()),
      _ => None,
    }
  }
}

#[derive(Debug)]
pub struct MSCoordES {
  // Metadata copied from outside.
  pub timestamp: Timestamp,

  pub query_id: QueryId,
  pub sql_query: proc::MSQuery,

  // Results of the query planning.
  pub query_plan: CoordQueryPlan,

  // The dynamically evolving fields.
  pub all_rms: HashSet<TQueryPath>,
  pub trans_table_views: Vec<(TransTableName, (Vec<ColName>, TableView))>,
  pub state: CoordState,

  /// Recall that since we remove a `TQueryPath` when its Leadership changes, that means that
  /// the LeadershipId of the PaxosGroup of a `TQueryPath`s here is the same as the one
  /// when this `TQueryPath` came in.
  pub registered_queries: HashSet<TQueryPath>,
}

impl TransTableSource for MSCoordES {
  fn get_instance(&self, trans_table_name: &TransTableName, idx: usize) -> &TableView {
    assert_eq!(idx, 0);
    let (_, instance) = lookup(&self.trans_table_views, trans_table_name).unwrap();
    instance
  }

  fn get_schema(&self, trans_table_name: &TransTableName) -> Vec<ColName> {
    let (schema, _) = lookup(&self.trans_table_views, trans_table_name).unwrap();
    schema.clone()
  }
}

#[derive(Debug)]
pub enum FullMSCoordES {
  QueryPlanning(QueryPlanningES),
  Executing(MSCoordES),
}

pub enum MSQueryCoordAction {
  /// This tells the parent Server to wait.
  Wait,
  /// This tells the parent Server to execute the given TMStatus.
  ExecuteTMStatus(TMStatus),
  /// Indicates that a valid MSCoordES was successful, and was ECU.
  Success(Vec<TQueryPath>, proc::MSQuery, TableView, Timestamp),
  /// Indicates that a valid MSCoordES was unsuccessful and there is no
  /// chance of success, and was ECU.
  FatalFailure(msg::ExternalAbortedData),
  /// Indicates that a valid MSCoordES was unsuccessful, but that there is a chance
  /// of success if it were repeated at a higher timestamp.
  NonFatalFailure,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

pub enum SendHelper {
  TableQuery(msg::PerformQuery, Vec<TabletGroupId>),
  TransTableQuery(msg::PerformQuery, TransTableLocationPrefix),
}

impl FullMSCoordES {
  /// Start the FullMSCoordES
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> MSQueryCoordAction {
    let plan_es = cast!(Self::QueryPlanning, self).unwrap();
    let action = plan_es.start(ctx, io_ctx);
    self.handle_planning_action(ctx, io_ctx, action)
  }

  /// Handle Master Response, routing it to the QueryReplanning.
  pub fn handle_master_response<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    master_qid: QueryId,
    result: msg::MasteryQueryPlanningResult,
  ) -> MSQueryCoordAction {
    let plan_es = cast!(FullMSCoordES::QueryPlanning, self).unwrap();
    let action = plan_es.handle_master_query_plan(ctx, io_ctx, master_qid, result);
    self.handle_planning_action(ctx, io_ctx, action)
  }

  /// Handle the `action` sent back by the `MSQueryCoordReplanningES`.
  fn handle_planning_action<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    action: QueryPlanningAction,
  ) -> MSQueryCoordAction {
    match action {
      QueryPlanningAction::Wait => MSQueryCoordAction::Wait,
      QueryPlanningAction::Success(query_plan) => {
        let plan_es = cast!(FullMSCoordES::QueryPlanning, self).unwrap();
        *self = FullMSCoordES::Executing(MSCoordES {
          timestamp: plan_es.timestamp.clone(),
          query_id: plan_es.query_id.clone(),
          sql_query: plan_es.sql_query.clone(),
          query_plan: query_plan.clone(),
          all_rms: Default::default(),
          trans_table_views: vec![],
          state: CoordState::Start,
          registered_queries: Default::default(),
        });

        // Move the ES onto the next stage.
        self.advance(ctx, io_ctx)
      }
      QueryPlanningAction::Failed(error) => {
        // Here, the QueryReplanning had failed. We do not need to ECU because
        // QueryReplanning will be in Done.
        MSQueryCoordAction::FatalFailure(error)
      }
    }
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    tm_qid: QueryId,
    new_rms: HashSet<TQueryPath>,
    (schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) -> MSQueryCoordAction {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();

    // We do some santity check on the result. We verify that the
    // TMStatus that just finished had the right QueryId.
    assert_eq!(tm_qid, es.state.stage_query_id().unwrap());
    // Look up the schema for the stage in the QueryPlan, and assert it's the same as the result.
    let stage_idx = es.state.stage_idx().unwrap();
    let (trans_table_name, _) = es.sql_query.trans_tables.get(stage_idx).unwrap();
    let (plan_schema, _) = lookup(&es.query_plan.col_usage_nodes, trans_table_name).unwrap();
    assert_eq!(plan_schema, &schema);
    // Recall that since we only send out one ContextRow, there should only be one TableView.
    assert_eq!(table_views.len(), 1);

    // Then, the results to the `trans_table_views`
    let table_view = table_views.into_iter().next().unwrap();
    es.trans_table_views.push((trans_table_name.clone(), (schema, table_view)));
    es.all_rms.extend(new_rms);
    self.advance(ctx, io_ctx)
  }

  /// This is called when the TMStatus has aborted.
  pub fn handle_tm_aborted<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    aborted_data: msg::AbortedData,
  ) -> MSQueryCoordAction {
    // Interpret the `aborted_data`.
    match aborted_data {
      msg::AbortedData::QueryError(msg::QueryError::TypeError { .. })
      | msg::AbortedData::QueryError(msg::QueryError::RuntimeError { .. }) => {
        // This implies an unrecoverable error, since trying again at a higher timestamp won't
        // generally fix the issue. Thus we ECU and return accordingly.
        self.exit_and_clean_up(ctx, io_ctx);
        MSQueryCoordAction::FatalFailure(msg::ExternalAbortedData::QueryExecutionError)
      }
      msg::AbortedData::QueryError(msg::QueryError::WriteRegionConflictWithSubsequentRead)
      | msg::AbortedData::QueryError(msg::QueryError::DeadlockSafetyAbortion)
      | msg::AbortedData::QueryError(msg::QueryError::TimestampConflict) => {
        // This implies a recoverable failure, so we ECU and return accordingly.
        self.exit_and_clean_up(ctx, io_ctx);
        MSQueryCoordAction::NonFatalFailure
      }
      // Recall that LateralErrors should never make it back to the MSCoordES.
      msg::AbortedData::QueryError(msg::QueryError::LateralError) => panic!(),
      // TODO: do these
      msg::AbortedData::QueryError(msg::QueryError::InvalidQueryPlan) => panic!(),
      msg::AbortedData::QueryError(msg::QueryError::InvalidLeadershipId) => panic!(),
    }
  }

  /// This is called when one of the remote node's Leadership changes beyond the
  /// LeadershipId that we had sent a PerformQuery to.
  pub fn handle_tm_remote_leadership_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> MSQueryCoordAction {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();
    let stage = cast!(CoordState::Stage, &es.state).unwrap();
    let stage_idx = stage.stage_idx.clone();
    self.process_ms_query_stage(ctx, io_ctx, stage_idx)
  }

  // Handle a RegisterQuery sent by an MSQuery to an MSCoordES.
  pub fn handle_register_query(&mut self, register: msg::RegisterQuery) {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();
    es.registered_queries.insert(register.query_path);
  }

  /// Handle a RemoteLeaderChanged
  pub fn remote_leader_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> MSQueryCoordAction {
    match self {
      FullMSCoordES::QueryPlanning(es) => {
        if remote_leader_changed.gid == PaxosGroupId::Master {
          let action = es.master_leader_changed(ctx, io_ctx);
          self.handle_planning_action(ctx, io_ctx, action)
        } else {
          MSQueryCoordAction::Wait
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
        MSQueryCoordAction::Wait
      }
    }
  }

  /// Handles the GossipData changing.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> MSQueryCoordAction {
    match self {
      FullMSCoordES::QueryPlanning(es) => {
        let action = es.gossip_data_changed(ctx, io_ctx);
        self.handle_planning_action(ctx, io_ctx, action)
      }
      FullMSCoordES::Executing(_) => MSQueryCoordAction::Wait,
    }
  }

  /// This function accepts the results for the subquery, and then decides either
  /// to move onto the next stage, or start 2PC to commit the change.
  fn advance<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> MSQueryCoordAction {
    // Compute the next stage
    let es = cast!(FullMSCoordES::Executing, self).unwrap();
    let next_stage_idx = es.state.stage_idx().unwrap() + 1;

    if next_stage_idx < es.sql_query.trans_tables.len() {
      self.process_ms_query_stage(ctx, io_ctx, next_stage_idx)
    } else {
      // Check that none of the Leaderships in `all_rms` have changed.
      for rm in &es.all_rms {
        let orig_lid = es.query_plan.query_leader_map.get(&rm.node_path.sid).unwrap();
        let cur_lid = ctx.leader_map.get(&rm.node_path.sid.to_gid()).unwrap();
        if orig_lid != cur_lid {
          // If a Leadership has changed, we abort and retry this MSCoordES.
          self.exit_and_clean_up(ctx, io_ctx);
          return MSQueryCoordAction::NonFatalFailure;
        }
      }

      // Cancel all RegisteredQueries that are not also an RM in the upcoming Paxos2PC.
      for registered_query in &es.registered_queries {
        if !es.all_rms.contains(registered_query) {
          ctx.ctx(io_ctx).send_to_ct(
            registered_query.clone().into_ct().node_path,
            CommonQuery::CancelQuery(msg::CancelQuery {
              query_id: registered_query.query_id.clone(),
            }),
          )
        }
      }

      // Finally, we go to Done and return the appropriate TableView.
      let (_, (_, table_view)) = es
        .trans_table_views
        .iter()
        .find(|(trans_table_name, _)| trans_table_name == &es.sql_query.returning)
        .unwrap();
      es.state = CoordState::Done;
      MSQueryCoordAction::Success(
        es.all_rms.iter().cloned().collect(),
        es.sql_query.clone(),
        table_view.clone(),
        es.timestamp,
      )
    }
  }

  /// This function advances the given MSCoordES at `query_id` to the next
  /// `Stage` with index `stage_idx`.
  fn process_ms_query_stage<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    stage_idx: usize,
  ) -> MSQueryCoordAction {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();

    // Get the corresponding MSQueryStage and FrozenColUsageNode.
    let (trans_table_name, ms_query_stage) = es.sql_query.trans_tables.get(stage_idx).unwrap();
    let (_, col_usage_node) = lookup(&es.query_plan.col_usage_nodes, trans_table_name).unwrap();

    // Compute the Context for this stage. Recall there must be exactly one row.
    let trans_table_names = node_external_trans_tables(col_usage_node);
    let mut context = Context::default();
    let mut context_row = ContextRow::default();
    for trans_table_name in &trans_table_names {
      context.context_schema.trans_table_context_schema.push(TransTableLocationPrefix {
        source: ctx.ctx(io_ctx).mk_this_query_path(es.query_id.clone()),
        trans_table_name: trans_table_name.clone(),
      });
      context_row.trans_table_context_row.push(0);
    }
    context.context_rows.push(context_row);

    // Construct the QueryPlan
    let mut query_leader_map = es.query_plan.query_leader_map.clone();
    query_leader_map
      .insert(ctx.this_sid.clone(), ctx.leader_map.get(&ctx.this_sid.to_gid()).unwrap().clone());
    let query_plan = QueryPlan {
      tier_map: es.query_plan.all_tier_maps.get(trans_table_name).unwrap().clone(),
      query_leader_map: query_leader_map.clone(),
      table_location_map: es.query_plan.table_location_map.clone(),
      extra_req_cols: es.query_plan.extra_req_cols.clone(),
      col_usage_node: col_usage_node.clone(),
    };

    // Create Construct the TMStatus that's going to be used to coordinate this stage.
    let tm_qid = mk_qid(io_ctx.rand());
    let child_qid = mk_qid(io_ctx.rand());
    let mut tm_status = TMStatus {
      query_id: tm_qid.clone(),
      child_query_id: child_qid.clone(),
      new_rms: Default::default(),
      leaderships: Default::default(),
      responded_count: 0,
      tm_state: Default::default(),
      orig_p: OrigP::new(es.query_id.clone()),
    };

    // The `sender_path` for the TMStatus above.
    let sender_path = ctx.mk_query_path(tm_qid.clone());
    // The `root_query_path` pointing to this MSCoordES.
    let root_query_path = ctx.mk_query_path(es.query_id.clone());

    // Send out the PerformQuery.
    let helper = match ms_query_stage {
      proc::MSQueryStage::SuperSimpleSelect(select_query) => {
        match &select_query.from {
          proc::TableRef::TablePath(table_path) => {
            let perform_query = msg::PerformQuery {
              root_query_path: root_query_path.clone(),
              sender_path: sender_path.clone().into_ct(),
              query_id: child_qid.clone(),
              query: msg::GeneralQuery::SuperSimpleTableSelectQuery(
                msg::SuperSimpleTableSelectQuery {
                  timestamp: es.timestamp.clone(),
                  context: context.clone(),
                  sql_query: select_query.clone(),
                  query_plan,
                },
              ),
            };
            let tids = ctx.ctx(io_ctx).get_min_tablets(&table_path, &select_query.selection);
            SendHelper::TableQuery(perform_query, tids)
          }
          proc::TableRef::TransTableName(sub_trans_table_name) => {
            // Here, we must do a SuperSimpleTransTableSelectQuery. Recall there is only one RM.
            let location_prefix = context
              .context_schema
              .trans_table_context_schema
              .iter()
              .find(|prefix| &prefix.trans_table_name == sub_trans_table_name)
              .unwrap()
              .clone();
            let perform_query = msg::PerformQuery {
              root_query_path,
              sender_path: sender_path.into_ct(),
              query_id: child_qid.clone(),
              query: msg::GeneralQuery::SuperSimpleTransTableSelectQuery(
                msg::SuperSimpleTransTableSelectQuery {
                  location_prefix: location_prefix.clone(),
                  context: context.clone(),
                  sql_query: select_query.clone(),
                  query_plan,
                },
              ),
            };
            SendHelper::TransTableQuery(perform_query, location_prefix)
          }
        }
      }
      proc::MSQueryStage::Update(update_query) => {
        let perform_query = msg::PerformQuery {
          root_query_path: root_query_path.clone(),
          sender_path: sender_path.clone().into_ct(),
          query_id: child_qid.clone(),
          query: msg::GeneralQuery::UpdateQuery(msg::UpdateQuery {
            timestamp: es.timestamp.clone(),
            context: context.clone(),
            sql_query: update_query.clone(),
            query_plan,
          }),
        };
        let tids = ctx.ctx(io_ctx).get_min_tablets(&update_query.table, &update_query.selection);
        SendHelper::TableQuery(perform_query, tids)
      }
    };

    match helper {
      SendHelper::TableQuery(perform_query, tids) => {
        // Validate the LeadershipId of PaxosGroups that the PerformQuery will be sent to.
        // We do this before sending any messages, in case it fails. Recall that the local
        // `leader_map` is allowed to get ahead of the `query_leader_map` which we computed
        // earlier, so this check is necessary.
        for tid in &tids {
          let sid = ctx.gossip.tablet_address_config.get(&tid).unwrap();
          if let Some(lid) = query_leader_map.get(sid) {
            if lid.gen < ctx.leader_map.get(&sid.to_gid()).unwrap().gen {
              // The `lid` has since changed, so we cannot finish this MSQueryES.
              self.exit_and_clean_up(ctx, io_ctx);
              return MSQueryCoordAction::NonFatalFailure;
            }
            // Recall that since > is not possible, these Leadership must be equals.
            assert_eq!(lid.gen, ctx.leader_map.get(&sid.to_gid()).unwrap().gen);
          }
        }

        // Having non-empty `tids` solves the TMStatus deadlock and determining the child schema.
        assert!(tids.len() > 0);
        for tid in tids {
          // Send out PerformQuery. Recall that this could only be a Tablet. Also recall
          // from the prior leader_map check, the local `leader_map` and the `query_leader_map`
          // for the given `tids` align, so using `send_to_t` sends to Leaderships
          // in `query_leader_map`.
          let tablet_msg = msg::TabletMessage::PerformQuery(perform_query.clone());
          let node_path = ctx.ctx(io_ctx).mk_node_path_from_tablet(tid);
          ctx.ctx(io_ctx).send_to_t(node_path.clone(), tablet_msg);

          // Add the TabletGroup into the TMStatus.
          tm_status.tm_state.insert(node_path.clone().into_ct(), None);
          let sid = node_path.sid.clone();
          let lid = ctx.leader_map.get(&sid.to_gid()).unwrap();
          tm_status.leaderships.insert(sid, lid.clone());
        }
      }
      SendHelper::TransTableQuery(perform_query, location_prefix) => {
        // Send out PerformQuery. Recall that this TransTable is held in this very
        // MSCoordES. Thus, there is no need to verify Leadership of `location_prefix` is
        // still alive, since it obviously is if we get here.
        let node_path = location_prefix.source.node_path.clone();
        ctx.ctx(io_ctx).send_to_ct(node_path.clone(), CommonQuery::PerformQuery(perform_query));

        // Add the TabletGroup into the TMStatus.
        tm_status.tm_state.insert(node_path.clone(), None);
        let sid = node_path.sid.clone();
        let lid = ctx.leader_map.get(&sid.to_gid()).unwrap();
        tm_status.leaderships.insert(sid, lid.clone());
      }
    }

    // Populate the TMStatus accordingly.
    es.state = CoordState::Stage(Stage { stage_idx, stage_query_id: tm_qid.clone() });
    MSQueryCoordAction::ExecuteTMStatus(tm_status)
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
              ctx.ctx(io_ctx).send_to_ct(
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
  /// The query to do the replanning with.
  pub sql_query: proc::MSQuery,
  /// The OrigP of the Task holding this MSQueryCoordReplanningES
  pub query_id: QueryId,
  /// Used for managing MasterQueryReplanning
  pub state: QueryPlanningS,
}

pub enum QueryPlanningAction {
  /// Indicates the parent needs to wait, making sure to fowards MasterQueryReplanning responses.
  Wait,
  /// Indicates the that MSQueryCoordReplanningES has computed a valid query, and it's stored
  /// in the `query_plan` field.
  Success(CoordQueryPlan),
  /// Indicates that a valid QueryPlan couldn't be computed. The ES will have
  /// also been cleaned up.
  Failed(msg::ExternalAbortedData),
}

impl QueryPlanningES {
  pub fn start<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> QueryPlanningAction {
    debug_assert!(matches!(&self.state, QueryPlanningS::Start));

    // First, we see if all TablePaths are in the GossipData
    for table_path in collect_table_paths(&self.sql_query) {
      if ctx.gossip.table_generation.static_read(&table_path, self.timestamp).is_none() {
        // We must go to MasterQueryPlanning.
        return self.perform_master_query_planning(ctx, io_ctx);
      }
    }

    // Next, we check that we are not trying to modify a Key Column in an Update.
    for (_, stage) in &self.sql_query.trans_tables {
      match stage {
        proc::MSQueryStage::SuperSimpleSelect(_) => {}
        proc::MSQueryStage::Update(query) => {
          // The TablePath exists, from the above.
          let gen = ctx.gossip.table_generation.static_read(&query.table, self.timestamp).unwrap();
          let schema = ctx.gossip.db_schema.get(&(query.table.clone(), gen.clone())).unwrap();
          for (col_name, _) in &query.assignment {
            if lookup(&schema.key_cols, col_name).is_some() {
              // If so, we return an InvalidUpdate to the External.
              return QueryPlanningAction::Failed(msg::ExternalAbortedData::InvalidUpdate);
            }
          }
        }
      }
    }

    // Next, we see if all required columns in Select and Update queries are present.
    let mut required_cols_exist = true;
    iterate_stage_ms_query(
      &mut |stage: GeneralStage| match stage {
        GeneralStage::SuperSimpleSelect(query) => {
          if let proc::TableRef::TablePath(table_path) = &query.from {
            // The TablePath exists, from the above.
            let gen = ctx.gossip.table_generation.static_read(&table_path, self.timestamp).unwrap();
            let schema = ctx.gossip.db_schema.get(&(table_path.clone(), gen.clone())).unwrap();
            for col_name in &query.projection {
              if !weak_contains_col(schema, col_name, &self.timestamp) {
                required_cols_exist = false;
              }
            }
          }
        }
        GeneralStage::Update(query) => {
          // The TablePath exists, from the above.
          let gen = ctx.gossip.table_generation.static_read(&query.table, self.timestamp).unwrap();
          let schema = ctx.gossip.db_schema.get(&(query.table.clone(), gen.clone())).unwrap();
          for (col_name, _) in &query.assignment {
            if !weak_contains_col(schema, col_name, &self.timestamp) {
              required_cols_exist = false;
            }
          }
        }
      },
      &self.sql_query,
    );

    if !required_cols_exist {
      // We must go to MasterQueryPlanning.
      return self.perform_master_query_planning(ctx, io_ctx);
    }

    // Next, we run the FrozenColUsageAlgorithm
    let mut planner = ColUsagePlanner {
      db_schema: &ctx.gossip.db_schema,
      table_generation: &ctx.gossip.table_generation,
      timestamp: self.timestamp.clone(),
    };
    let col_usage_nodes = planner.plan_ms_query(&self.sql_query);

    // If there is an External Column at the top-level, we must go to MasterQueryPlanning.
    for (_, (_, child)) in &col_usage_nodes {
      if !child.external_cols.is_empty() {
        return self.perform_master_query_planning(ctx, io_ctx);
      }
    }

    // If we get here, the QueryPlan is valid, so we return it and go to Done.
    self.finish_planning(ctx, io_ctx, col_usage_nodes)
  }

  /// Send a `PerformMasterQueryPlanning` and go to the `MasterQueryReplanning` state.
  fn perform_master_query_planning<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> QueryPlanningAction {
    let master_query_id = mk_qid(io_ctx.rand());
    let sender_path = ctx.mk_query_path(self.query_id.clone());
    ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::PerformMasterQueryPlanning(
      msg::PerformMasterQueryPlanning {
        sender_path,
        query_id: master_query_id.clone(),
        timestamp: self.timestamp.clone(),
        ms_query: self.sql_query.clone(),
      },
    ));

    // Advance Replanning State.
    self.state = QueryPlanningS::MasterQueryPlanning(MasterQueryPlanning { master_query_id });
    QueryPlanningAction::Wait
  }

  /// All verifications of `gossip` have been complete and we construct a
  /// `CoordQueryPlan` before going to the `Done` state.
  fn finish_planning<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
  ) -> QueryPlanningAction {
    let all_tier_maps = compute_all_tier_maps(&self.sql_query);
    let (table_location_map, extra_req_cols) =
      compute_query_plan_data(&self.sql_query, &ctx.gossip.table_generation, self.timestamp);
    self.state = QueryPlanningS::Done;
    QueryPlanningAction::Success(CoordQueryPlan {
      all_tier_maps,
      query_leader_map: self.compute_query_leader_map(ctx, io_ctx, &table_location_map),
      table_location_map,
      extra_req_cols,
      col_usage_nodes,
    })
  }

  /// Compute a query_leader_map using the `TablePath`s in `table_location_map`.
  fn compute_query_leader_map<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    table_location_map: &HashMap<TablePath, Gen>,
  ) -> HashMap<SlaveGroupId, LeadershipId> {
    let mut query_leader_map = HashMap::<SlaveGroupId, LeadershipId>::new();
    for (table_path, gen) in table_location_map {
      let shards = ctx.gossip.sharding_config.get(&(table_path.clone(), gen.clone())).unwrap();
      for (_, tid) in shards.clone() {
        let sid = ctx.ctx(io_ctx).gossip.tablet_address_config.get(&tid).unwrap().clone();
        let lid = ctx.ctx(io_ctx).leader_map.get(&sid.to_gid()).unwrap();
        query_leader_map.insert(sid.clone(), lid.clone());
      }
    }
    query_leader_map
  }

  /// Handles the QueryPlan constructed by the Master.
  pub fn handle_master_query_plan<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    master_qid: QueryId,
    result: msg::MasteryQueryPlanningResult,
  ) -> QueryPlanningAction {
    let last_state = cast!(QueryPlanningS::MasterQueryPlanning, &self.state).unwrap();
    assert_eq!(last_state.master_query_id, master_qid);
    match result {
      MasteryQueryPlanningResult::MasterQueryPlan(master_query_plan) => {
        // We must still check that there are no top-level External Columns.
        for (_, (_, child)) in &master_query_plan.col_usage_nodes {
          if !child.external_cols.is_empty() {
            // If so, we can definitively return an failure.
            self.state = QueryPlanningS::Done;
            return QueryPlanningAction::Failed(msg::ExternalAbortedData::QueryExecutionError);
          }
        }

        // By now, we know the QueryPlanning can succeed. However, recall that the MasterQueryPlan
        // might have used a db_schema that is beyond what this node has in its GossipData. Thus,
        // we check if all TablePaths are in the GossipData, waiting if not. This is only needed
        // so that we can actually contact those nodes.
        for table_path in collect_table_paths(&self.sql_query) {
          if ctx.gossip.table_generation.static_read(&table_path, self.timestamp).is_none() {
            // We send a MasterGossipRequest and go to GossipDataWaiting.
            let sender_path = ctx.ctx(io_ctx).mk_this_query_path(self.query_id.clone());
            ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::MasterGossipRequest(
              msg::MasterGossipRequest { sender_path },
            ));

            self.state = QueryPlanningS::GossipDataWaiting(GossipDataWaiting { master_query_plan });
            return QueryPlanningAction::Wait;
          }
        }

        // Otherwise, we can finish QueryPlanning and return a Success.
        self.finish_master_query_plan(ctx, io_ctx, master_query_plan)
      }
      MasteryQueryPlanningResult::TablePathDNE(_)
      | MasteryQueryPlanningResult::InvalidUpdate
      | MasteryQueryPlanningResult::RequiredColumnDNE(_) => {
        // We just return a generic error to the External
        self.state = QueryPlanningS::Done;
        QueryPlanningAction::Failed(msg::ExternalAbortedData::QueryExecutionError)
      }
    }
  }

  /// Handles the GossipData changing.
  pub fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> QueryPlanningAction {
    if let QueryPlanningS::GossipDataWaiting(last_state) = &self.state {
      // We must check again whether the GossipData is new enough, since this is called
      // for any GossipData update whatsoever (not just the one resulting from the
      // MasterGossipRequest we sent out).
      for table_path in collect_table_paths(&self.sql_query) {
        if ctx.gossip.table_generation.static_read(&table_path, self.timestamp).is_none() {
          // We stay in GossipDataWaiting.
          return QueryPlanningAction::Wait;
        }
      }

      // We have a recent enough GossipData, so we finish QueryPlanning and return Success.
      self.finish_master_query_plan(ctx, io_ctx, last_state.master_query_plan.clone())
    } else {
      QueryPlanningAction::Wait
    }
  }

  /// Here, we have verified all `TablePath`s are present in the GossipData.
  fn finish_master_query_plan<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
    master_query_plan: msg::MasterQueryPlan,
  ) -> QueryPlanningAction {
    self.state = QueryPlanningS::Done;
    QueryPlanningAction::Success(CoordQueryPlan {
      all_tier_maps: master_query_plan.all_tier_maps,
      query_leader_map: self.compute_query_leader_map(
        ctx,
        io_ctx,
        &master_query_plan.table_location_map,
      ),
      table_location_map: master_query_plan.table_location_map,
      extra_req_cols: master_query_plan.extra_req_cols,
      col_usage_nodes: master_query_plan.col_usage_nodes,
    })
  }

  /// This is called when there is a Leadership change in the Master PaxosGroup.
  pub fn master_leader_changed<IO: CoreIOCtx>(
    &mut self,
    ctx: &mut CoordContext,
    io_ctx: &mut IO,
  ) -> QueryPlanningAction {
    if let QueryPlanningS::MasterQueryPlanning(_) = &self.state {
      // This means we have to resend the PerformMasterQueryPlanning to the new Leader.
      self.perform_master_query_planning(ctx, io_ctx)
    } else {
      QueryPlanningAction::Wait
    }
  }

  /// This Exits and Cleans up this QueryReplanningES
  fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, ctx: &mut CoordContext, io_ctx: &mut IO) {
    match &self.state {
      QueryPlanningS::Start => {}
      QueryPlanningS::MasterQueryPlanning(MasterQueryPlanning { master_query_id }) => {
        ctx.ctx(io_ctx).send_to_master(msg::MasterRemotePayload::CancelMasterQueryPlanning(
          msg::CancelMasterQueryPlanning { query_id: master_query_id.clone() },
        ));
      }
      QueryPlanningS::GossipDataWaiting(_) => {}
      QueryPlanningS::Done => {}
    }
    self.state = QueryPlanningS::Done;
  }
}
