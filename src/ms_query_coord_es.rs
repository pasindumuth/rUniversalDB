use crate::col_usage::{node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode};
use crate::common::{lookup, mk_qid, IOTypes, NetworkOut, OrigP, QueryPlan, TMStatus};
use crate::model::common::{
  proc, ColName, Context, ContextRow, EndpointId, Gen, NodeGroupId, QueryId, QueryPath, RequestId,
  TablePath, TableView, TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::message as msg;
use crate::model::message::{AbortedData, Query2PCAbortReason, Query2PCAborted};
use crate::server::CommonQuery;
use crate::slave::SlaveContext;
use crate::trans_table_read_es::TransTableSource;
use std::collections::{HashMap, HashSet};

// -----------------------------------------------------------------------------------------------
//  MSCoordES
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CoordQueryPlan {
  gossip_gen: Gen,
  col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
}

#[derive(Debug)]
pub struct PreparingState {
  prepared_rms: HashSet<QueryPath>,
}

#[derive(Debug)]
pub enum CoordState {
  Start,
  /// Here, `stage_query_id` is the QueryId of the TMStatus
  Stage {
    stage_idx: usize,
    stage_query_id: QueryId,
  },
  Preparing(PreparingState),
  Done,
}

impl CoordState {
  fn stage_idx(&self) -> Option<usize> {
    match self {
      CoordState::Start => Some(0),
      CoordState::Stage { stage_idx, .. } => Some(*stage_idx),
      _ => None,
    }
  }

  fn stage_query_id(&self) -> Option<QueryId> {
    match self {
      CoordState::Stage { stage_query_id, .. } => Some(stage_query_id.clone()),
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

  /// For every TierMap, a TablePath should appear here iff that Table is written
  /// to in the MSQuery. The Tier for every such TablePath here is the Tier that
  /// MSTableRead should be using. If the TransTable is corresponds to an Update,
  /// the Tier for TablePath being updated should be one ahead (i.e. lower).
  pub all_tier_maps: HashMap<TransTableName, TierMap>,

  // The dynamically evolving fields.
  pub all_rms: HashSet<QueryPath>,
  pub trans_table_views: Vec<(TransTableName, (Vec<ColName>, TableView))>,
  pub state: CoordState,

  // Memory management fields
  pub registered_queries: HashSet<QueryPath>,
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
  QueryReplanning(MSQueryCoordReplanningES),
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

impl FullMSCoordES {
  /// Start the FullMSCoordES
  pub fn start<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> MSQueryCoordAction {
    let plan_es = cast!(Self::QueryReplanning, self).unwrap();
    let action = plan_es.start(ctx);
    self.handle_replanning_action(ctx, action)
  }

  /// Handle Master Response, routing it to the QueryReplanning.
  pub fn handle_master_response<T: IOTypes>(
    &mut self,
    ctx: &mut SlaveContext<T>,
    gossip_gen: Gen,
    tree: msg::FrozenColUsageTree,
  ) -> MSQueryCoordAction {
    let plan_es = cast!(FullMSCoordES::QueryReplanning, self).unwrap();
    let action = plan_es.handle_master_response(gossip_gen, tree);
    self.handle_replanning_action(ctx, action)
  }

  /// Handle the `action` sent back by the `MSQueryCoordReplanningES`.
  fn handle_replanning_action<T: IOTypes>(
    &mut self,
    ctx: &mut SlaveContext<T>,
    action: MSQueryCoordReplanningAction,
  ) -> MSQueryCoordAction {
    match action {
      MSQueryCoordReplanningAction::Wait => MSQueryCoordAction::Wait,
      MSQueryCoordReplanningAction::Success(query_plan) => {
        let plan_es = cast!(FullMSCoordES::QueryReplanning, self).unwrap();

        // We compute the TierMap here.
        let mut tier_map = HashMap::<TablePath, u32>::new();
        for (_, stage) in &plan_es.sql_query.trans_tables {
          match stage {
            proc::MSQueryStage::SuperSimpleSelect(_) => {}
            proc::MSQueryStage::Update(update) => {
              tier_map.insert(update.table.clone(), 0);
            }
          }
        }

        // The Tier should be where every Read query should be reading from, except
        // if the current stage is an Update, which should be one Tier ahead (i.e.
        // lower) for that TablePath.
        let mut all_tier_maps = HashMap::<TransTableName, TierMap>::new();
        for (trans_table_name, stage) in plan_es.sql_query.trans_tables.iter().rev() {
          all_tier_maps.insert(trans_table_name.clone(), TierMap { map: tier_map.clone() });
          match stage {
            proc::MSQueryStage::SuperSimpleSelect(_) => {}
            proc::MSQueryStage::Update(update) => {
              *tier_map.get_mut(&update.table).unwrap() += 1;
            }
          }
        }

        *self = FullMSCoordES::Executing(MSCoordES {
          timestamp: plan_es.timestamp.clone(),
          query_id: plan_es.query_id.clone(),
          sql_query: plan_es.sql_query.clone(),
          query_plan: query_plan.clone(),
          all_tier_maps,
          all_rms: Default::default(),
          trans_table_views: vec![],
          state: CoordState::Start,
          registered_queries: Default::default(),
        });

        // Move the ES onto the next stage.
        self.advance(ctx)
      }
      MSQueryCoordReplanningAction::Failed(error) => {
        // Here, the QueryReplanning had failed. We do not need to ECU because
        // QueryReplanning will be in Done.
        MSQueryCoordAction::FatalFailure(error)
      }
    }
  }

  /// This is called when the TMStatus has completed successfully.
  pub fn handle_tm_success<T: IOTypes>(
    &mut self,
    ctx: &mut SlaveContext<T>,
    tm_qid: QueryId,
    new_rms: HashSet<QueryPath>,
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
    ctx: &mut SlaveContext<T>,
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

  // Handle a RegisterQuery sent by an MSQuery to an MSCoordES.
  pub fn handle_register_query(&mut self, register: msg::RegisterQuery) {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();
    es.registered_queries.insert(register.query_path);
  }

  /// Handle a Prepared message sent to an MSCoordES.
  pub fn handle_prepared<T: IOTypes>(
    &mut self,
    ctx: &mut SlaveContext<T>,
    prepared: msg::Query2PCPrepared,
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
    ctx: &mut SlaveContext<T>,
    aborted: msg::Query2PCAborted,
  ) -> MSQueryCoordAction {
    // Recall that although ECU will send Abort to the RM that just responded,
    // this is not incorrect. We ignore this for simplicity.
    self.exit_and_clean_up(ctx);
    match aborted.reason {
      Query2PCAbortReason::DeadlockSafetyAbortion => {
        // Since this error is an anomaly that usually does not happen, we may try again the
        // transaction again at a higher timestamp. Thus, we signal that the failure is non-fatal.
        MSQueryCoordAction::NonFatalFailure
      }
    }
  }

  /// By now, all Prepared must have come on. Thus, we send out Commit, abort the
  /// RegisteredQueries, and return the results.
  fn finish_ms_coord<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> MSQueryCoordAction {
    let es = cast!(FullMSCoordES::Executing, self).unwrap();

    // First, we commit all RMs.
    for query_path in &es.all_rms {
      ctx.ctx().core_ctx().send_to_tablet(
        query_path.node_path.maybe_tablet_group_id.clone().unwrap(),
        msg::TabletMessage::Query2PCCommit(msg::Query2PCCommit {
          ms_query_id: query_path.query_id.clone(),
        }),
      );
    }

    // Next, we inform all `registered_query`s that are not an RM that they should exit,
    // since they were falsely added to this transaction.
    for query_path in &es.registered_queries {
      if !es.all_rms.contains(query_path) {
        ctx.ctx().core_ctx().send_to_tablet(
          query_path.node_path.maybe_tablet_group_id.clone().unwrap(),
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
  fn advance<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> MSQueryCoordAction {
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
        // Otherwise, this transaction had writes, so we start 2PC by sending out Prepared.
        let sender_path = ctx.mk_query_path(es.query_id.clone());
        for query_path in &es.all_rms {
          ctx.ctx().core_ctx().send_to_tablet(
            query_path.node_path.maybe_tablet_group_id.clone().unwrap(),
            msg::TabletMessage::Query2PCPrepare(msg::Query2PCPrepare {
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
    ctx: &mut SlaveContext<T>,
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
        source: NodeGroupId::Slave(ctx.this_slave_group_id.clone()),
        query_id: es.query_id.clone(),
        trans_table_name: trans_table_name.clone(),
      });
      context_row.trans_table_context_row.push(0);
    }
    context.context_rows.push(context_row);

    // Construct the QueryPlan
    // TODO: do this properly
    let query_plan = QueryPlan {
      tier_map: TierMap { map: Default::default() },
      query_leader_map: Default::default(),
      table_location_map: Default::default(),
      col_usage_node: col_usage_node.clone(),
      extra_req_cols: Default::default(),
    };

    // Create Construct the TMStatus that's going to be used to coordinate this stage.
    let tm_qid = mk_qid(&mut ctx.rand);
    let child_qid = mk_qid(&mut ctx.rand);
    let mut tm_status = TMStatus {
      query_id: tm_qid.clone(),
      child_query_id: child_qid.clone(),
      new_rms: Default::default(),
      responded_count: 0,
      tm_state: Default::default(),
      orig_p: OrigP::new(es.query_id.clone()),
    };

    // The `sender_path` for the TMStatus above.
    let sender_path = ctx.mk_query_path(tm_qid.clone());
    // The `root_query_path` pointing to this MSCoordES.
    let root_query_path = ctx.mk_query_path(es.query_id.clone());

    // Send out the PerformQuery.
    match ms_query_stage {
      proc::MSQueryStage::SuperSimpleSelect(select_query) => {
        match &select_query.from {
          proc::TableRef::TablePath(table_path) => {
            // Here, we must do a SuperSimpleTableSelectQuery.
            let child_query = msg::SuperSimpleTableSelectQuery {
              timestamp: es.timestamp.clone(),
              context: context.clone(),
              sql_query: select_query.clone(),
              query_plan,
            };

            // Compute the TabletGroups involved.
            let tids = ctx.ctx().get_min_tablets(table_path, &select_query.selection);
            // Having non-empty `tids` solves the TMStatus deadlock and determining the child schema.
            assert!(tids.len() > 0);
            for tid in tids {
              let perform_query = msg::PerformQuery {
                root_query_path: root_query_path.clone(),
                sender_path: sender_path.clone(),
                query_id: child_qid.clone(),
                tier_map: es.all_tier_maps.get(trans_table_name).unwrap().clone(),
                query: msg::GeneralQuery::SuperSimpleTableSelectQuery(child_query.clone()),
              };

              // Send out PerformQuery. Recall that this could only be a a Tablet.
              let nid = NodeGroupId::Tablet(tid);
              ctx.ctx().send_to_node(nid.clone(), CommonQuery::PerformQuery(perform_query));

              // Add the TabletGroup into the TMStatus.
              tm_status.tm_state.insert(ctx.ctx().core_ctx().mk_node_path(nid), None);
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

            // Add in the Slave to `tm_state`, and send out the PerformQuery. Recall that
            // if we are doing a TransTableRead here, then the TransTable must be located here.
            let perform_query = msg::PerformQuery {
              root_query_path,
              sender_path,
              query_id: child_qid.clone(),
              tier_map: es.all_tier_maps.get(trans_table_name).unwrap().clone(),
              query: msg::GeneralQuery::SuperSimpleTransTableSelectQuery(
                msg::SuperSimpleTransTableSelectQuery {
                  location_prefix: location_prefix.clone(),
                  context: context.clone(),
                  sql_query: select_query.clone(),
                  query_plan,
                },
              ),
            };

            // Send out PerformQuery. Recall that this could be a Slave or a Tablet.
            let nid = location_prefix.source.clone();
            ctx.ctx().send_to_node(nid.clone(), CommonQuery::PerformQuery(perform_query));

            // Add the TabletGroup into the TMStatus.
            tm_status.tm_state.insert(ctx.ctx().core_ctx().mk_node_path(nid), None);
          }
        }
      }
      proc::MSQueryStage::Update(update_query) => {
        // Here, we must do a Update.
        let child_query = msg::UpdateQuery {
          timestamp: es.timestamp.clone(),
          context: context.clone(),
          sql_query: update_query.clone(),
          query_plan,
        };

        // Compute the TabletGroups involved.
        let tids = ctx.ctx().get_min_tablets(&update_query.table, &update_query.selection);
        // Having non-empty `tids` solves the TMStatus deadlock and determining the child schema.
        assert!(tids.len() > 0);
        for tid in tids {
          let perform_query = msg::PerformQuery {
            root_query_path: root_query_path.clone(),
            sender_path: sender_path.clone(),
            query_id: child_qid.clone(),
            tier_map: es.all_tier_maps.get(trans_table_name).unwrap().clone(),
            query: msg::GeneralQuery::UpdateQuery(child_query.clone()),
          };

          // Send out PerformQuery. Recall that this could only be a a Tablet.
          let nid = NodeGroupId::Tablet(tid);
          ctx.ctx().send_to_node(nid.clone(), CommonQuery::PerformQuery(perform_query));

          // Add the TabletGroup into the TMStatus.
          tm_status.tm_state.insert(ctx.ctx().core_ctx().mk_node_path(nid), None);
        }
      }
    };

    // Populate the TMStatus accordingly.
    es.state = CoordState::Stage { stage_idx, stage_query_id: tm_qid.clone() };
    MSQueryCoordAction::ExecuteTMStatus(tm_status)
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) {
    match self {
      FullMSCoordES::QueryReplanning(plan_es) => plan_es.exit_and_clean_up(ctx),
      FullMSCoordES::Executing(es) => {
        match &es.state {
          CoordState::Start => {}
          CoordState::Stage { .. } => {
            // Clean up any Registered Queries in the MSCoordES. Recall that in the
            // absence of Commit/Abort, this is the only way that MSQueryESs can get cleaned up.
            for registered_query in &es.registered_queries {
              ctx.ctx().send_to_path(
                registered_query.clone(),
                CommonQuery::CancelQuery(msg::CancelQuery {
                  query_id: registered_query.query_id.clone(),
                }),
              )
            }
          }
          CoordState::Preparing(_) => {
            // Recall that by now, Prepare should have been sent to all RMs. They need to be
            // cancelled with Query2PCAbort (they don't react to CancelQuery). The remaining
            // `registered_queries` can be cancelled with CancelQuery.
            for query_path in &es.all_rms {
              ctx.ctx().core_ctx().send_to_tablet(
                query_path.node_path.maybe_tablet_group_id.clone().unwrap(),
                msg::TabletMessage::Query2PCAbort(msg::Query2PCAbort {
                  ms_query_id: query_path.query_id.clone(),
                }),
              );
            }
            for query_path in &es.registered_queries {
              if !es.all_rms.contains(query_path) {
                ctx.ctx().core_ctx().send_to_tablet(
                  query_path.node_path.maybe_tablet_group_id.clone().unwrap(),
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
//  QueryReplanning
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum MSQueryCoordReplanningS {
  Start,
  MasterQueryReplanning { master_query_id: QueryId },
  Done,
}

#[derive(Debug)]
pub struct MSQueryCoordReplanningES {
  pub timestamp: Timestamp,
  /// The query to do the replanning with.
  pub sql_query: proc::MSQuery,
  /// The OrigP of the Task holding this MSQueryCoordReplanningES
  pub query_id: QueryId,
  /// Used for managing MasterQueryReplanning
  pub state: MSQueryCoordReplanningS,
}

enum MSQueryCoordReplanningAction {
  /// Indicates the parent needs to wait, making sure to fowards MasterQueryReplanning responses.
  Wait,
  /// Indicates the that MSQueryCoordReplanningES has computed a valid query, and it's stored
  /// in the `query_plan` field.
  Success(CoordQueryPlan),
  /// Indicates that a valid QueryPlan couldn't be computed. The ES will have
  /// also been cleaned up.
  Failed(msg::ExternalAbortedData),
}

impl MSQueryCoordReplanningES {
  fn start<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) -> MSQueryCoordReplanningAction {
    // First, we compute the ColUsageNode.
    let mut planner =
      ColUsagePlanner { db_schema: &ctx.gossip.db_schema, timestamp: self.timestamp.clone() };
    let col_usage_nodes = planner.plan_ms_query(&self.sql_query);

    for (_, (_, child)) in &col_usage_nodes {
      if !child.external_cols.is_empty() {
        // If there are External Columns in any of the stages, then we need to consult the Master.
        let master_query_id = mk_qid(&mut ctx.rand);
        ctx.network_output.send(
          &ctx.master_eid,
          msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
            msg::PerformMasterFrozenColUsage {
              sender_path: ctx.mk_query_path(self.query_id.clone()),
              query_id: master_query_id.clone(),
              timestamp: self.timestamp.clone(),
              trans_table_schemas: HashMap::new(),
              col_usage_tree: msg::ColUsageTree::MSQuery(self.sql_query.clone()),
            },
          )),
        );

        // Advance Replanning State.
        self.state = MSQueryCoordReplanningS::MasterQueryReplanning { master_query_id };
        return MSQueryCoordReplanningAction::Wait;
      }
    }

    // If we get here, the QueryPlan is valid, so we return it and go to Done.
    self.state = MSQueryCoordReplanningS::Done;
    MSQueryCoordReplanningAction::Success(CoordQueryPlan {
      gossip_gen: ctx.gossip.gen,
      col_usage_nodes,
    })
  }

  /// Handles the Query Plan constructed by the Master.
  fn handle_master_response(
    &mut self,
    gossip_gen: Gen,
    tree: msg::FrozenColUsageTree,
  ) -> MSQueryCoordReplanningAction {
    // Here, we expect the `tree` to be multiple `nodes`.
    let col_usage_nodes = cast!(msg::FrozenColUsageTree::ColUsageNodes, tree).unwrap();
    for (_, (_, child)) in &col_usage_nodes {
      if !child.external_cols.is_empty() {
        // This means that the Master has confirmed that this Query doesn't have a
        // valid Query Plan (i.e. one of the ColumnRefs don't exist). Thus, we return
        // a fatal error and go to Done.
        self.state = MSQueryCoordReplanningS::Done;
        return MSQueryCoordReplanningAction::Failed(msg::ExternalAbortedData::QueryExecutionError);
      }
    }

    // This means the QueryReplanning was a success, so we update the QueryPlan and go to Done.
    self.state = MSQueryCoordReplanningS::Done;
    MSQueryCoordReplanningAction::Success(CoordQueryPlan { gossip_gen, col_usage_nodes })
  }

  /// This Exits and Cleans up this QueryReplanningES
  fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) {
    match &self.state {
      MSQueryCoordReplanningS::Start => {}
      MSQueryCoordReplanningS::MasterQueryReplanning { master_query_id } => {
        // If the removal was successful, we should also send a Cancellation
        // message to the Master.
        ctx.network_output.send(
          &ctx.master_eid,
          msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
            msg::CancelMasterFrozenColUsage { query_id: master_query_id.clone() },
          )),
        );
      }
      MSQueryCoordReplanningS::Done => {}
    }
    self.state = MSQueryCoordReplanningS::Done;
  }
}
