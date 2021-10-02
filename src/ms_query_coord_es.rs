use crate::col_usage::{
  collect_table_paths, iterate_stage_ms_query, node_external_trans_tables, ColUsagePlanner,
  FrozenColUsageNode, GeneralStage,
};
use crate::common::{lookup, mk_qid, IOTypes, NetworkOut, OrigP, QueryPlan, TMStatus};
use crate::coord::CoordContext;
use crate::model::common::proc::MSQueryStage;
use crate::model::common::{
  proc, CTQueryPath, ColName, Context, ContextRow, EndpointId, Gen, LeadershipId, NodeGroupId,
  PaxosGroupId, QueryId, RequestId, SlaveGroupId, TQueryPath, TablePath, TableView, TabletGroupId,
  TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::message as msg;
use crate::model::message::MasteryQueryPlanningResult;
use crate::server::{weak_contains_col, CommonQuery};
use crate::slave::RemoteLeaderChangedPLm;
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
pub struct PreparingState {
  prepared_rms: HashSet<TQueryPath>,
}

#[derive(Debug)]
pub struct Stage {
  stage_idx: usize,
  stage_query_id: QueryId,
}

#[derive(Debug)]
pub enum CoordState {
  Start,
  /// Here, `stage_query_id` is the QueryId of the TMStatus
  Stage(Stage),
  Preparing(PreparingState),
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
  Success(TableView),
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
  pub fn start<T: IOTypes>(&mut self, ctx: &mut CoordContext<T>) -> MSQueryCoordAction {
    let plan_es = cast!(Self::QueryPlanning, self).unwrap();
    let action = plan_es.start(ctx);
    self.handle_planning_action(ctx, action)
  }

  /// Handle Master Response, routing it to the QueryReplanning.
  pub fn handle_master_response<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
    master_qid: QueryId,
    result: msg::MasteryQueryPlanningResult,
  ) -> MSQueryCoordAction {
    let plan_es = cast!(FullMSCoordES::QueryPlanning, self).unwrap();
    let action = plan_es.handle_master_query_plan(ctx, master_qid, result);
    self.handle_planning_action(ctx, action)
  }

  /// Handle the `action` sent back by the `MSQueryCoordReplanningES`.
  fn handle_planning_action<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
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
        self.advance(ctx)
      }
      QueryPlanningAction::Failed(error) => {
        // Here, the QueryReplanning had failed. We do not need to ECU because
        // QueryReplanning will be in Done.
        MSQueryCoordAction::FatalFailure(error)
      }
    }
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
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
    self.advance(ctx)
  }

  /// This is called when the TMStatus has aborted.
  pub fn handle_tm_aborted<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
    aborted_data: msg::AbortedData,
  ) -> MSQueryCoordAction {
    // Interpret the `aborted_data`.
    match aborted_data {
      msg::AbortedData::QueryError(msg::QueryError::TypeError { .. })
      | msg::AbortedData::QueryError(msg::QueryError::RuntimeError { .. }) => {
        // This implies an unrecoverable error, since trying again at a higher timestamp won't
        // generally fix the issue. Thus we ECU and return accordingly.
        self.exit_and_clean_up(ctx);
        MSQueryCoordAction::FatalFailure(msg::ExternalAbortedData::QueryExecutionError)
      }
      msg::AbortedData::QueryError(msg::QueryError::WriteRegionConflictWithSubsequentRead)
      | msg::AbortedData::QueryError(msg::QueryError::DeadlockSafetyAbortion)
      | msg::AbortedData::QueryError(msg::QueryError::TimestampConflict) => {
        // This implies a recoverable failure, so we ECU and return accordingly.
        self.exit_and_clean_up(ctx);
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
  pub fn handle_tm_remote_leadership_changed<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
  ) -> MSQueryCoordAction {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();
    let stage = cast!(CoordState::Stage, &es.state).unwrap();
    let stage_idx = stage.stage_idx.clone();
    self.process_ms_query_stage(ctx, stage_idx)
  }

  // Handle a RegisterQuery sent by an MSQuery to an MSCoordES.
  pub fn handle_register_query(&mut self, register: msg::RegisterQuery) {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();
    es.registered_queries.insert(register.query_path);
  }

  /// Handle a Prepared message sent to an MSCoordES.
  pub fn handle_prepared<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
    prepared: msg::FinishQueryPrepared,
  ) -> MSQueryCoordAction {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();
    let preparing = cast!(CoordState::Preparing, &mut es.state).unwrap();

    // First, insert the QueryPath of the RM into the prepare state.
    assert!(!preparing.prepared_rms.contains(&prepared.rm_path));
    preparing.prepared_rms.insert(prepared.rm_path);

    // Next see if the prepare state is done.
    if preparing.prepared_rms.len() < es.all_rms.len() {
      MSQueryCoordAction::Wait
    } else {
      self.finish_ms_coord(ctx)
    }
  }

  /// Handle a Aborted message sent to an MSCoordES.
  pub fn handle_aborted<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
    aborted: msg::FinishQueryAborted,
  ) -> MSQueryCoordAction {
    // Recall that although ECU will send Abort to the RM that just responded,
    // this is not incorrect. We ignore this for simplicity.
    self.exit_and_clean_up(ctx);
    match aborted.reason {
      msg::FinishQueryAbortReason::DeadlockSafetyAbortion => {
        // Since this error is an anomaly that usually does not happen, we may try again the
        // transaction again at a higher timestamp. Thus, we signal that the failure is non-fatal.
        MSQueryCoordAction::NonFatalFailure
      }
    }
  }

  /// Handle a RemoteLeaderChanged
  pub fn remote_leader_changed<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> MSQueryCoordAction {
    match self {
      FullMSCoordES::QueryPlanning(es) => {
        if remote_leader_changed.gid == PaxosGroupId::Master {
          let action = es.master_leader_changed(ctx);
          self.handle_planning_action(ctx, action)
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
  pub fn gossip_data_change<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
  ) -> MSQueryCoordAction {
    match self {
      FullMSCoordES::QueryPlanning(es) => {
        let action = es.gossip_data_change(ctx);
        self.handle_planning_action(ctx, action)
      }
      FullMSCoordES::Executing(_) => MSQueryCoordAction::Wait,
    }
  }

  /// By now, all Prepared must have come on. Thus, we send out Commit, abort the
  /// RegisteredQueries, and return the results.
  fn finish_ms_coord<T: IOTypes>(&mut self, ctx: &mut CoordContext<T>) -> MSQueryCoordAction {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();

    // First, we commit all RMs.
    for query_path in &es.all_rms {
      ctx.ctx().send_to_t(
        query_path.node_path.clone(),
        msg::TabletMessage::FinishQueryCommit(msg::FinishQueryCommit {
          ms_query_id: query_path.query_id.clone(),
        }),
      );
    }

    // Next, we inform all `registered_query`s that are not an RM that they should exit,
    // since they were falsely added to this transaction.
    for query_path in &es.registered_queries {
      if !es.all_rms.contains(&query_path) {
        ctx.ctx().send_to_t(
          query_path.node_path.clone(),
          msg::TabletMessage::CancelQuery(msg::CancelQuery {
            query_id: query_path.query_id.clone(),
          }),
        );
      }
    }

    // Finally, we go to Done and return the appropriate TableView.
    let (_, (_, table_view)) = es
      .trans_table_views
      .iter()
      .find(|(trans_table_name, _)| trans_table_name == &es.sql_query.returning)
      .unwrap();
    es.state = CoordState::Done;
    MSQueryCoordAction::Success(table_view.clone())
  }

  /// This function accepts the results for the subquery, and then decides either
  /// to move onto the next stage, or start 2PC to commit the change.
  fn advance<T: IOTypes>(&mut self, ctx: &mut CoordContext<T>) -> MSQueryCoordAction {
    // Compute the next stage
    let es = cast!(FullMSCoordES::Executing, self).unwrap();
    let next_stage_idx = es.state.stage_idx().unwrap() + 1;

    if next_stage_idx < es.sql_query.trans_tables.len() {
      self.process_ms_query_stage(ctx, next_stage_idx)
    } else {
      if es.all_rms.is_empty() {
        // If there are no RMs, that means this was purely a read, so we can finish up.
        self.finish_ms_coord(ctx)
      } else {
        // Check that none of the Leaderships in `all_rms` have changed.
        for rm in &es.all_rms {
          let orig_lid = es.query_plan.query_leader_map.get(&rm.node_path.sid).unwrap();
          let cur_lid = ctx.leader_map.get(&rm.node_path.sid.to_gid()).unwrap();
          if orig_lid != cur_lid {
            // If a Leadership has changed, we abort and retry this MSCoordES.
            self.exit_and_clean_up(ctx);
            return MSQueryCoordAction::NonFatalFailure;
          }
        }

        // Cancel all RegisteredQueries that are not also an RM in the upcoming Paxos2PC.
        for registered_query in &es.registered_queries {
          if !es.all_rms.contains(registered_query) {
            ctx.ctx().send_to_ct(
              registered_query.clone().into_ct().node_path,
              CommonQuery::CancelQuery(msg::CancelQuery {
                query_id: registered_query.query_id.clone(),
              }),
            )
          }
        }

        // TODO: remove

        // Otherwise, this transaction had writes, so we start 2PC by sending out Prepared.
        let sender_path = ctx.mk_query_path(es.query_id.clone());
        for query_path in &es.all_rms {
          ctx.ctx().send_to_t(
            query_path.node_path.clone(),
            msg::TabletMessage::FinishQueryPrepare(msg::FinishQueryPrepare {
              sender_path: sender_path.clone(),
              ms_query_id: query_path.query_id.clone(),
            }),
          );
        }

        es.state = CoordState::Preparing(PreparingState { prepared_rms: HashSet::new() });
        MSQueryCoordAction::Wait
      }
    }
  }

  /// This function advances the given MSCoordES at `query_id` to the next
  /// `Stage` with index `stage_idx`.
  fn process_ms_query_stage<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
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
        source: ctx.ctx().mk_this_query_path(es.query_id.clone()),
        trans_table_name: trans_table_name.clone(),
      });
      context_row.trans_table_context_row.push(0);
    }
    context.context_rows.push(context_row);

    // Construct the QueryPlan
    let mut query_leader_map = es.query_plan.query_leader_map.clone();
    query_leader_map.insert(
      ctx.this_slave_group_id.clone(),
      ctx.leader_map.get(&ctx.this_slave_group_id.to_gid()).unwrap().clone(),
    );
    let query_plan = QueryPlan {
      tier_map: es.query_plan.all_tier_maps.get(trans_table_name).unwrap().clone(),
      query_leader_map: query_leader_map.clone(),
      table_location_map: es.query_plan.table_location_map.clone(),
      extra_req_cols: es.query_plan.extra_req_cols.clone(),
      col_usage_node: col_usage_node.clone(),
    };

    // Create Construct the TMStatus that's going to be used to coordinate this stage.
    let tm_qid = mk_qid(&mut ctx.rand);
    let child_qid = mk_qid(&mut ctx.rand);
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
            let tids = ctx.ctx().get_min_tablets(&table_path, &select_query.selection);
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
        let tids = ctx.ctx().get_min_tablets(&update_query.table, &update_query.selection);
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
              self.exit_and_clean_up(ctx);
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
          let node_path = ctx.ctx().mk_node_path_from_tablet(tid);
          ctx.ctx().send_to_t(node_path.clone(), tablet_msg);

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
        ctx.ctx().send_to_ct(node_path.clone(), CommonQuery::PerformQuery(perform_query));

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
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut CoordContext<T>) {
    match self {
      FullMSCoordES::QueryPlanning(plan_es) => plan_es.exit_and_clean_up(ctx),
      FullMSCoordES::Executing(es) => {
        match &es.state {
          CoordState::Start => {}
          CoordState::Stage(_) => {
            // Clean up any Registered Queries in the MSCoordES. The `registered_queries` docs
            // describe why `send_to_ct` sends the message to the right PaxosNode.
            for registered_query in &es.registered_queries {
              ctx.ctx().send_to_ct(
                registered_query.clone().into_ct().node_path,
                CommonQuery::CancelQuery(msg::CancelQuery {
                  query_id: registered_query.query_id.clone(),
                }),
              )
            }
          }
          CoordState::Preparing(_) => {
            // Recall that by now, Prepare should have been sent to all RMs. They need to be
            // cancelled with FinishQueryAbort (they don't react to CancelQuery). The remaining
            // `registered_queries` can be cancelled with CancelQuery.
            for query_path in &es.all_rms {
              ctx.ctx().send_to_t(
                query_path.node_path.clone(),
                msg::TabletMessage::FinishQueryAbort(msg::FinishQueryAbort {
                  ms_query_id: query_path.query_id.clone(),
                }),
              );
            }
            for query_path in &es.registered_queries {
              if !es.all_rms.contains(query_path) {
                ctx.ctx().send_to_t(
                  query_path.node_path.clone(),
                  msg::TabletMessage::CancelQuery(msg::CancelQuery {
                    query_id: query_path.query_id.clone(),
                  }),
                );
              }
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
  pub fn start<T: IOTypes>(&mut self, ctx: &mut CoordContext<T>) -> QueryPlanningAction {
    assert!(matches!(&self.state, QueryPlanningS::Start));

    // First, we see if all TablePaths are in the GossipData
    for table_path in collect_table_paths(&self.sql_query) {
      if !ctx.gossip.table_generation.contains_key(&table_path) {
        // We must go to MasterQueryPlanning.
        return self.perform_master_query_planning(ctx);
      }
    }

    // Next, we check that we are not trying to modify a Key Column in an Update.
    let mut attempted_key_col_update = false;
    'outer: for (_, stage) in &self.sql_query.trans_tables {
      match stage {
        MSQueryStage::SuperSimpleSelect(_) => {}
        MSQueryStage::Update(query) => {
          // The TablePath exists, from the above.
          let gen = ctx.gossip.table_generation.get(&query.table).unwrap();
          let schema = ctx.gossip.db_schema.get(&(query.table.clone(), gen.clone())).unwrap();
          for (col_name, _) in &query.assignment {
            if lookup(&schema.key_cols, col_name).is_some() {
              attempted_key_col_update = true;
              break 'outer;
            }
          }
        }
      }
    }

    if attempted_key_col_update {
      // If so, we return an InvalidUpdate to the External.
      return QueryPlanningAction::Failed(msg::ExternalAbortedData::InvalidUpdate);
    }

    // Next, we see if all required columns in Select and Update queries are present.
    let mut required_cols_exist = true;
    iterate_stage_ms_query(
      &mut |stage: GeneralStage| match stage {
        GeneralStage::SuperSimpleSelect(query) => {
          if let proc::TableRef::TablePath(table_path) = &query.from {
            // The TablePath exists, from the above.
            let gen = ctx.gossip.table_generation.get(&table_path).unwrap();
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
          let gen = ctx.gossip.table_generation.get(&query.table).unwrap();
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
      return self.perform_master_query_planning(ctx);
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
        return self.perform_master_query_planning(ctx);
      }
    }

    // If we get here, the QueryPlan is valid, so we return it and go to Done.
    self.finish_planning(ctx, col_usage_nodes)
  }

  /// Send a `PerformMasterQueryPlanning` and go to the `MasterQueryReplanning` state.
  fn perform_master_query_planning<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
  ) -> QueryPlanningAction {
    let master_query_id = mk_qid(&mut ctx.rand);
    let sender_path = ctx.mk_query_path(self.query_id.clone());
    ctx.ctx().send_to_master(msg::MasterRemotePayload::PerformMasterQueryPlanning(
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
  fn finish_planning<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
    col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
  ) -> QueryPlanningAction {
    // Compute `all_tier_maps

    // The Tier should be where every Read query should be reading from, except
    // if the current stage is an Update, which should be one Tier ahead (i.e.
    // lower) for that TablePath.
    let mut all_tier_maps = HashMap::<TransTableName, TierMap>::new();
    let mut cur_tier_map = HashMap::<TablePath, u32>::new();
    for (_, stage) in &self.sql_query.trans_tables {
      match stage {
        proc::MSQueryStage::SuperSimpleSelect(_) => {}
        proc::MSQueryStage::Update(update) => {
          cur_tier_map.insert(update.table.clone(), 0);
        }
      }
    }
    for (trans_table_name, stage) in self.sql_query.trans_tables.iter().rev() {
      all_tier_maps.insert(trans_table_name.clone(), TierMap { map: cur_tier_map.clone() });
      match stage {
        proc::MSQueryStage::SuperSimpleSelect(_) => {}
        proc::MSQueryStage::Update(update) => {
          *cur_tier_map.get_mut(&update.table).unwrap() += 1;
        }
      }
    }

    // Compute `table_location_map` and `extra_req_cols`.
    let mut table_location_map = HashMap::<TablePath, Gen>::new();
    let mut extra_req_cols = HashMap::<TablePath, Vec<ColName>>::new();
    iterate_stage_ms_query(
      &mut |stage: GeneralStage| match stage {
        GeneralStage::SuperSimpleSelect(query) => {
          if let proc::TableRef::TablePath(table_path) = &query.from {
            let gen = ctx.gossip.table_generation.get(&table_path).unwrap();
            table_location_map.insert(table_path.clone(), gen.clone());

            // Recall there might already be required columns for this TablePath.
            if !extra_req_cols.contains_key(&table_path) {
              extra_req_cols.insert(table_path.clone(), Vec::new());
            }
            let req_cols = extra_req_cols.get_mut(&table_path).unwrap();
            for col_name in &query.projection {
              if !req_cols.contains(col_name) {
                req_cols.push(col_name.clone());
              }
            }
          }
        }
        GeneralStage::Update(query) => {
          let gen = ctx.gossip.table_generation.get(&query.table).unwrap();
          table_location_map.insert(query.table.clone(), gen.clone());

          // Recall there might already be required columns for this TablePath.
          if !extra_req_cols.contains_key(&query.table) {
            extra_req_cols.insert(query.table.clone(), Vec::new());
          }
          let req_cols = extra_req_cols.get_mut(&query.table).unwrap();
          for (col_name, _) in &query.assignment {
            if !req_cols.contains(col_name) {
              req_cols.push(col_name.clone());
            }
          }
        }
      },
      &self.sql_query,
    );

    // Finish
    self.state = QueryPlanningS::Done;
    QueryPlanningAction::Success(CoordQueryPlan {
      all_tier_maps,
      query_leader_map: self.compute_query_leader_map(ctx, &table_location_map),
      table_location_map,
      extra_req_cols,
      col_usage_nodes,
    })
  }

  /// Compute a query_leader_map using the `TablePath`s in `table_location_map`.
  fn compute_query_leader_map<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
    table_location_map: &HashMap<TablePath, Gen>,
  ) -> HashMap<SlaveGroupId, LeadershipId> {
    let mut query_leader_map = HashMap::<SlaveGroupId, LeadershipId>::new();
    for (table_path, gen) in table_location_map {
      let shards = ctx.gossip.sharding_config.get(&(table_path.clone(), gen.clone())).unwrap();
      for (_, tid) in shards.clone() {
        let sid = ctx.ctx().gossip.tablet_address_config.get(&tid).unwrap().clone();
        let lid = ctx.ctx().leader_map.get(&sid.to_gid()).unwrap();
        query_leader_map.insert(sid.clone(), lid.clone());
      }
    }
    query_leader_map
  }

  /// Handles the QueryPlan constructed by the Master.
  pub fn handle_master_query_plan<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
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
          if !ctx.gossip.table_generation.contains_key(&table_path) {
            // We send a MasterGossipRequest and go to GossipDataWaiting.
            let sender_path = ctx.ctx().mk_this_query_path(self.query_id.clone());
            ctx.ctx().send_to_master(msg::MasterRemotePayload::MasterGossipRequest(
              msg::MasterGossipRequest { sender_path },
            ));

            self.state = QueryPlanningS::GossipDataWaiting(GossipDataWaiting { master_query_plan });
            return QueryPlanningAction::Wait;
          }
        }

        // Otherwise, we can finish QueryPlanning and return a Success.
        self.finish_master_query_plan(ctx, master_query_plan)
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
  pub fn gossip_data_change<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
  ) -> QueryPlanningAction {
    if let QueryPlanningS::GossipDataWaiting(last_state) = &self.state {
      // We must check again whether the GossipData is new enough, since this is called
      // for any GossipData update whatsoever (not just the one resulting from the
      // MasterGossipRequest we sent out).
      for table_path in collect_table_paths(&self.sql_query) {
        if !ctx.gossip.table_generation.contains_key(&table_path) {
          // We stay in GossipDataWaiting.
          return QueryPlanningAction::Wait;
        }
      }

      // We have a recent enough GossipData, so we finish QueryPlanning and return Success.
      self.finish_master_query_plan(ctx, last_state.master_query_plan.clone())
    } else {
      QueryPlanningAction::Wait
    }
  }

  /// Here, we have verified all `TablePath`s are present in the GossipData.
  fn finish_master_query_plan<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
    master_query_plan: msg::MasterQueryPlan,
  ) -> QueryPlanningAction {
    self.state = QueryPlanningS::Done;
    QueryPlanningAction::Success(CoordQueryPlan {
      all_tier_maps: master_query_plan.all_tier_maps,
      query_leader_map: self.compute_query_leader_map(ctx, &master_query_plan.table_location_map),
      table_location_map: master_query_plan.table_location_map,
      extra_req_cols: master_query_plan.extra_req_cols,
      col_usage_nodes: master_query_plan.col_usage_nodes,
    })
  }

  /// This is called when there is a Leadership change in the Master PaxosGroup.
  pub fn master_leader_changed<T: IOTypes>(
    &mut self,
    ctx: &mut CoordContext<T>,
  ) -> QueryPlanningAction {
    if let QueryPlanningS::MasterQueryPlanning(_) = &self.state {
      // This means we have to resend the PerformMasterQueryPlanning to the new Leader.
      self.perform_master_query_planning(ctx)
    } else {
      QueryPlanningAction::Wait
    }
  }

  /// This Exits and Cleans up this QueryReplanningES
  fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut CoordContext<T>) {
    match &self.state {
      QueryPlanningS::Start => {}
      QueryPlanningS::MasterQueryPlanning(MasterQueryPlanning { master_query_id }) => {
        ctx.ctx().send_to_master(msg::MasterRemotePayload::CancelMasterQueryPlanning(
          msg::CancelMasterQueryPlanning { query_id: master_query_id.clone() },
        ));
      }
      QueryPlanningS::GossipDataWaiting(_) => {}
      QueryPlanningS::Done => {}
    }
    self.state = QueryPlanningS::Done;
  }
}
