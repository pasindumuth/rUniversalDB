use crate::col_usage::{node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  merge_table_views, mk_qid, Clock, GossipData, IOTypes, NetworkOut, OrigP, QueryPlan, TMStatus,
  TMWaitValue, TableSchema, TabletForwardOut,
};
use crate::lang;
use crate::model::common::proc::MSQuery;
use crate::model::common::{
  iast, proc, ColName, ColType, Context, ContextRow, Gen, NodeGroupId, QueryPath, SlaveGroupId,
  TablePath, TableView, TabletGroupId, TabletKeyRange, TierMap, Timestamp,
  TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message as msg;
use crate::model::message::ExternalAbortedData;
use crate::multiversion_map::MVM;
use crate::query_converter::convert_to_msquery;
use crate::slave::CoordState::ReadStage;
use rand::RngCore;
use sqlparser::ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::{HashMap, HashSet};
use std::ops::Add;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  MSQueryCoordES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
struct CoordQueryPlan {
  gossip_gen: Gen,
  col_usage_nodes: HashMap<TransTableName, (Vec<ColName>, FrozenColUsageNode)>,
}

#[derive(Debug)]
enum CoordState {
  Start,
  ReadStage { stage_idx: usize, stage_query_id: QueryId },
  WriteStage { stage_idx: usize, stage_query_id: QueryId },
  Preparing { tablet_group_ids: HashSet<QueryPath> },
  Committing,
  Aborting,
}

#[derive(Debug)]
struct MSQueryCoordES {
  // Metadata copied from outside.
  request_id: RequestId,
  sender_path: EndpointId,
  timestamp: Timestamp,

  query_id: QueryId,
  query: proc::MSQuery,

  // Results of the query planning.
  query_plan: CoordQueryPlan,

  // Precomputed data for execute stages faster.
  all_tier_maps: HashMap<TransTableName, TierMap>,

  // The dynamically evolving fields.
  all_rms: HashSet<QueryPath>,
  trans_table_views: Vec<(TransTableName, TableView)>,
  state: CoordState,

  // Memory management fields
  registered_queries: HashSet<QueryId>,
}

#[derive(Debug)]
enum MSQueryCoordReplanningS {
  Start,
  MasterQueryReplanning { master_query_id: QueryId },
  Done(Option<CoordQueryPlan>),
}

#[derive(Debug)]
struct MSQueryCoordReplanningES {
  timestamp: Timestamp,
  query: proc::MSQuery,
  orig_query: msg::PerformExternalQuery,
  /// The OrigP of the Task holding this MSQueryCoordReplanningES
  // TODO: change this to `QueryId` here, and in tablet.
  orig_p: OrigP,
  /// Used for managing MasterQueryReplanning
  state: MSQueryCoordReplanningS,
}

#[derive(Debug)]
enum FullMSQueryCoordES {
  QueryReplanning(MSQueryCoordReplanningES),
  Executing(MSQueryCoordES),
}

// -----------------------------------------------------------------------------------------------
//  Slave Statuses
// -----------------------------------------------------------------------------------------------

/// This contains every TabletStatus. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  ms_coord_ess: HashMap<QueryId, FullMSQueryCoordES>,
  tm_statuses: HashMap<QueryId, TMStatus>,
}

// -----------------------------------------------------------------------------------------------
//  Slave State
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct SlaveState<T: IOTypes> {
  slave_context: SlaveContext<T>,
  statuses: Statuses,
}

/// The SlaveState that holds all the state of the Slave
#[derive(Debug)]
pub struct SlaveContext<T: IOTypes> {
  /// IO Objects.
  rand: T::RngCoreT,
  clock: T::ClockT,
  network_output: T::NetworkOutT,
  tablet_forward_output: T::TabletForwardOutT,

  /// Metadata
  this_slave_group_id: SlaveGroupId,
  master_eid: EndpointId,

  /// Gossip
  gossip: Arc<GossipData>,

  /// Distribution
  sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  /// External Query Management
  external_request_id_map: HashMap<RequestId, QueryId>,

  /// Child Queries
  master_query_map: HashMap<QueryId, OrigP>,
}

impl<T: IOTypes> SlaveState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    tablet_forward_output: T::TabletForwardOutT,
    gossip: Arc<GossipData>,
    sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, EndpointId>,
    this_slave_group_id: SlaveGroupId,
    master_eid: EndpointId,
  ) -> SlaveState<T> {
    SlaveState {
      slave_context: SlaveContext {
        rand,
        clock,
        network_output,
        tablet_forward_output,
        this_slave_group_id,
        master_eid,
        gossip,
        sharding_config,
        tablet_address_config,
        slave_address_config,
        external_request_id_map: Default::default(),
        master_query_map: Default::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::SlaveMessage) {
    self.slave_context.handle_incoming_message(&mut self.statuses, message);
  }
}

impl<T: IOTypes> SlaveContext<T> {
  pub fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::SlaveMessage) {
    match message {
      msg::SlaveMessage::PerformExternalQuery(external_query) => {
        match self.init_request(&external_query) {
          Ok(ms_query) => {
            let query_id = mk_qid(&mut self.rand);
            self
              .external_request_id_map
              .insert(external_query.request_id.clone(), query_id.clone());
            let mut plan_es = MSQueryCoordReplanningES {
              timestamp: self.clock.now(),
              query: ms_query,
              orig_query: external_query.clone(),
              orig_p: OrigP { query_id: query_id.clone() },
              state: MSQueryCoordReplanningS::Start,
            };
            plan_es.start(self);
            match plan_es.state {
              MSQueryCoordReplanningS::Done(maybe_plan) => {
                if let Some(query_plan) = maybe_plan {
                  // We compute the TierMap here.
                  let mut tier_map = HashMap::<TablePath, u32>::new();
                  for (_, stage) in &plan_es.query.trans_tables {
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
                  for (trans_table_name, stage) in plan_es.query.trans_tables.iter().rev() {
                    all_tier_maps
                      .insert(trans_table_name.clone(), TierMap { map: tier_map.clone() });
                    match stage {
                      proc::MSQueryStage::SuperSimpleSelect(_) => {}
                      proc::MSQueryStage::Update(update) => {
                        *tier_map.get_mut(&update.table).unwrap() += 1;
                      }
                    }
                  }

                  let mut coord_es = MSQueryCoordES {
                    request_id: plan_es.orig_query.request_id.clone(),
                    sender_path: plan_es.orig_query.sender_path.clone(),
                    timestamp: plan_es.timestamp.clone(),
                    query_id: query_id.clone(),
                    query: plan_es.query.clone(),
                    query_plan,
                    all_tier_maps,
                    all_rms: Default::default(),
                    trans_table_views: vec![],
                    state: CoordState::Start,
                    registered_queries: Default::default(),
                  };

                  // Move onto the next stage.
                  ms_coord_es_advance::<T>(
                    &mut coord_es,
                    &query_id,
                    0,
                    // Slave references
                    &mut self.rand,
                    &mut self.network_output,
                    &mut statuses.tm_statuses,
                    &self.this_slave_group_id,
                    &self.gossip,
                    &self.sharding_config,
                    &self.tablet_address_config,
                    &self.slave_address_config,
                  );
                }
              }
              _ => {}
            }
          }
          Err(payload) => self.network_output.send(
            &external_query.sender_path,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAbort(
              msg::ExternalQueryAbort { request_id: external_query.request_id, payload },
            )),
          ),
        }
      }
      msg::SlaveMessage::CancelExternalQuery(_) => unimplemented!(),
      msg::SlaveMessage::TabletMessage(tablet_group_id, tablet_msg) => {
        self.tablet_forward_output.forward(&tablet_group_id, tablet_msg);
      }
      msg::SlaveMessage::PerformQuery(_) => unimplemented!(),
      msg::SlaveMessage::CancelQuery(_) => unimplemented!(),
      msg::SlaveMessage::QuerySuccess(query_success) => {
        self.handle_query_success(statuses, query_success);
      }
      msg::SlaveMessage::QueryAborted(_) => unimplemented!(),
      msg::SlaveMessage::Query2PCPrepared(prepared) => {
        // Okay, let's get the thing prepared.
      }
      msg::SlaveMessage::Query2PCAborted(_) => unimplemented!(),
      msg::SlaveMessage::MasterFrozenColUsageAborted(_) => unimplemented!(),
      msg::SlaveMessage::MasterFrozenColUsageSuccess(_) => unimplemented!(),
      msg::SlaveMessage::RegisterQuery(_) => {}
    }
  }

  /// Does some initial validations and MSQuery processing before we start
  /// servicing the request.
  fn init_request(
    &mut self,
    external_query: &msg::PerformExternalQuery,
  ) -> Result<proc::MSQuery, msg::ExternalAbortedData> {
    if self.external_request_id_map.contains_key(&external_query.request_id) {
      // Duplicate RequestId; respond with an abort.
      Err(msg::ExternalAbortedData::NonUniqueRequestId)
    } else {
      // Parse the SQL
      match Parser::parse_sql(&GenericDialect {}, &external_query.query) {
        Ok(parsed_ast) => {
          let internal_ast = convert_ast(&parsed_ast);
          // Convert to MSQuery
          match convert_to_msquery(&self.gossip.gossiped_db_schema, internal_ast) {
            Ok(ms_query) => Ok(ms_query),
            Err(payload) => Err(payload),
          }
        }
        Err(parse_error) => {
          // Extract error string
          Err(msg::ExternalAbortedData::ParseError(match parse_error {
            TokenizerError(err_msg) => err_msg,
            ParserError(err_msg) => err_msg,
          }))
        }
      }
    }
  }

  /// This function accepts the results for the subquery, and then decides either
  /// to move onto the next stage, or start 2PC to commit the change.
  fn advance_ms_coord_es(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    let full_coord_es = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
    let coord_es = cast!(FullMSQueryCoordES::Executing, full_coord_es).unwrap();

    let next_stage_idx = match &coord_es.state {
      CoordState::Start => 1,
      CoordState::ReadStage { stage_idx, .. } => stage_idx + 1,
      CoordState::WriteStage { stage_idx, .. } => stage_idx + 1,
      _ => panic!(),
    };

    if next_stage_idx < coord_es.query.trans_tables.len() {
      self.process_ms_coord_es_stage(statuses, query_id, next_stage_idx);
    } else {
      // Finish the ES by sending out Prepared.
      let sender_path = QueryPath {
        slave_group_id: self.this_slave_group_id.clone(),
        maybe_tablet_group_id: None,
        query_id: coord_es.query_id.clone(),
      };
      for query_path in &coord_es.all_rms {
        let eid = self.slave_address_config.get(&query_path.slave_group_id).unwrap();
        self.network_output.send(
          eid,
          msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
            query_path.maybe_tablet_group_id.clone().unwrap(),
            msg::TabletMessage::Query2PCPrepare(msg::Query2PCPrepare {
              sender_path: sender_path.clone(),
              ms_query_id: coord_es.query_id.clone(),
            }),
          )),
        );
      }

      coord_es.state = CoordState::Preparing { tablet_group_ids: coord_es.all_rms.clone() };
    }
  }

  /// This function advances the given MSQueryCoordES at `query_id` to the next stage, `stage_idx`.
  /// This stage is gauranteed to be another `ReadStage` or `WriteStage`.
  fn process_ms_coord_es_stage(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    stage_idx: usize,
  ) {
    let full_coord_es = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
    let coord_es = cast!(FullMSQueryCoordES::Executing, full_coord_es).unwrap();

    // Get the corresponding MSQueryStage and FrozenColUsageNode.
    let (trans_table_name, ms_query_stage) = coord_es.query.trans_tables.get(stage_idx).unwrap();
    let (_, col_usage_node) = coord_es.query_plan.col_usage_nodes.get(trans_table_name).unwrap();

    // Compute the context of this `col_usage_node`. Recall there must be exactly one row.
    let external_trans_tables = node_external_trans_tables(col_usage_node);
    let mut context = Context::default();
    let mut context_row = ContextRow::default();
    for external_trans_table in &external_trans_tables {
      context.context_schema.trans_table_context_schema.push(TransTableLocationPrefix {
        source: NodeGroupId::Slave(self.this_slave_group_id.clone()),
        query_id: query_id.clone(),
        trans_table_name: external_trans_table.clone(),
      });
      context_row.trans_table_context_row.push(0);
    }
    context.context_rows.push(context_row);

    // Compute the `trans_table_schemas` using the `col_usage_nodes`.
    let mut trans_table_schemas = HashMap::<TransTableName, Vec<ColName>>::new();
    for external_trans_table in external_trans_tables {
      let (cols, _) = coord_es.query_plan.col_usage_nodes.get(&external_trans_table).unwrap();
      trans_table_schemas.insert(external_trans_table, cols.clone());
    }

    // Compute this MSQueryCoordESs QueryPath
    let root_query_path = QueryPath {
      slave_group_id: self.this_slave_group_id.clone(),
      maybe_tablet_group_id: None,
      query_id: query_id.clone(),
    };

    // Handle accordingly
    coord_es.state = match ms_query_stage {
      proc::MSQueryStage::SuperSimpleSelect(select_query) => {
        // Create ReadTMState
        let tm_query_id = mk_qid(&mut self.rand);
        let mut status = TMStatus {
          node_group_ids: Default::default(),
          new_rms: Default::default(),
          responded_count: 0,
          tm_state: Default::default(),
          orig_p: OrigP::new(query_id.clone()),
        };

        // Path of this TMStatus to respond to.
        let sender_path = QueryPath {
          slave_group_id: self.this_slave_group_id.clone(),
          maybe_tablet_group_id: None,
          query_id: tm_query_id.clone(),
        };

        match &select_query.from {
          proc::TableRef::TablePath(table_path) => {
            // Add in the Tablets that manage this TablePath to `tm_state`,
            // and send out the PerformQuery
            for (_, tablet_group_id) in self.sharding_config.get(table_path).unwrap() {
              let child_query_id = mk_qid(&mut self.rand);
              let sid = self.tablet_address_config.get(&tablet_group_id).unwrap();
              let eid = self.slave_address_config.get(&sid).unwrap();
              self.network_output.send(
                eid,
                msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
                  tablet_group_id.clone(),
                  msg::TabletMessage::PerformQuery(msg::PerformQuery {
                    root_query_path: root_query_path.clone(),
                    sender_path: sender_path.clone(),
                    query_id: child_query_id.clone(),
                    tier_map: coord_es.all_tier_maps.get(trans_table_name).unwrap().clone(),
                    query: msg::GeneralQuery::SuperSimpleTableSelectQuery(
                      msg::SuperSimpleTableSelectQuery {
                        timestamp: coord_es.timestamp.clone(),
                        context: context.clone(),
                        sql_query: select_query.clone(),
                        query_plan: QueryPlan {
                          gossip_gen: self.gossip.gossip_gen,
                          trans_table_schemas: trans_table_schemas.clone(),
                          col_usage_node: col_usage_node.clone(),
                        },
                      },
                    ),
                  }),
                )),
              );

              let node_group_id = NodeGroupId::Tablet(tablet_group_id.clone());
              status.node_group_ids.insert(node_group_id, child_query_id.clone());
              status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
            }
          }
          proc::TableRef::TransTableName(trans_table_name) => {
            let location = lookup_location(&context, trans_table_name).unwrap();

            // Add in the Slave to `tm_state`, and send out the PerformQuery
            let child_query_id = mk_qid(&mut self.rand);
            let eid = self.slave_address_config.get(&self.this_slave_group_id).unwrap();
            self.network_output.send(
              eid,
              msg::NetworkMessage::Slave(msg::SlaveMessage::PerformQuery(msg::PerformQuery {
                root_query_path,
                sender_path,
                query_id: child_query_id.clone(),
                tier_map: coord_es.all_tier_maps.get(trans_table_name).unwrap().clone(),
                query: msg::GeneralQuery::SuperSimpleTransTableSelectQuery(
                  msg::SuperSimpleTransTableSelectQuery {
                    location_prefix: location.clone(),
                    context: context.clone(),
                    sql_query: select_query.clone(),
                    query_plan: QueryPlan {
                      gossip_gen: self.gossip.gossip_gen,
                      trans_table_schemas,
                      col_usage_node: col_usage_node.clone(),
                    },
                  },
                ),
              })),
            );

            status.node_group_ids.insert(location.source, child_query_id.clone());
            status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
          }
        }

        statuses.tm_statuses.insert(tm_query_id.clone(), status);
        CoordState::ReadStage { stage_idx, stage_query_id: tm_query_id }
      }
      proc::MSQueryStage::Update(update_query) => {
        // Create WriteTMState
        // TODO: Should we move the below to a common location, including the
        // `tm_statuses.insert(tm_query_id.clone(), status);`?
        let tm_query_id = mk_qid(&mut self.rand);
        let mut status = TMStatus {
          node_group_ids: Default::default(),
          new_rms: Default::default(),
          responded_count: 0,
          tm_state: Default::default(),
          orig_p: OrigP::new(query_id.clone()),
        };

        // Path of this WriteTMStatus to respond to.
        let sender_path = QueryPath {
          slave_group_id: self.this_slave_group_id.clone(),
          maybe_tablet_group_id: None,
          query_id: tm_query_id.clone(),
        };

        // Add in the Tablets that manage this TablePath to `write_tm_state`,
        // and send out the PerformQuery
        for (_, tablet_group_id) in self.sharding_config.get(&update_query.table).unwrap() {
          let child_query_id = mk_qid(&mut self.rand);
          let sid = self.tablet_address_config.get(&tablet_group_id).unwrap();
          let eid = self.slave_address_config.get(&sid).unwrap();
          self.network_output.send(
            eid,
            msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
              tablet_group_id.clone(),
              msg::TabletMessage::PerformQuery(msg::PerformQuery {
                root_query_path: root_query_path.clone(),
                sender_path: sender_path.clone(),
                query_id: child_query_id.clone(),
                tier_map: coord_es.all_tier_maps.get(trans_table_name).unwrap().clone(),
                query: msg::GeneralQuery::UpdateQuery(msg::UpdateQuery {
                  timestamp: coord_es.timestamp.clone(),
                  context: context.clone(),
                  sql_query: update_query.clone(),
                  query_plan: QueryPlan {
                    gossip_gen: self.gossip.gossip_gen,
                    trans_table_schemas: trans_table_schemas.clone(),
                    col_usage_node: col_usage_node.clone(),
                  },
                }),
              }),
            )),
          );

          let node_group_id = NodeGroupId::Tablet(tablet_group_id.clone());
          status.node_group_ids.insert(node_group_id, child_query_id.clone());
          status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
        }

        statuses.tm_statuses.insert(tm_query_id.clone(), status);
        CoordState::WriteStage { stage_idx, stage_query_id: tm_query_id }
      }
    };
  }

  fn handle_query_success(&mut self, statuses: &mut Statuses, query_success: msg::QuerySuccess) {
    if let Some(tm_status) = statuses.tm_statuses.get_mut(&query_success.query_id) {
      // We just add the result of the `query_success` here.
      let tm_wait_value = tm_status.tm_state.get_mut(&query_success.return_path).unwrap();
      *tm_wait_value = TMWaitValue::Result(query_success.result.clone());
      tm_status.new_rms.extend(query_success.new_rms.into_iter());
      tm_status.responded_count += 1;
      if tm_status.responded_count == tm_status.tm_state.len() {
        // Remove the `TMStatus` and take ownership
        let tm_status = statuses.tm_statuses.remove(&query_success.query_id).unwrap();
        // Merge there TableViews together
        let mut results = Vec::<(Vec<ColName>, Vec<TableView>)>::new();
        for (_, tm_wait_value) in tm_status.tm_state {
          results.push(cast!(TMWaitValue::Result, tm_wait_value).unwrap());
        }
        let merged_result = merge_table_views(results);
        self.handle_tm_done(
          statuses,
          query_success.query_id,
          tm_status.orig_p.query_id,
          tm_status.new_rms,
          merged_result,
        );
      }
    }
  }

  fn handle_tm_done(
    &mut self,
    statuses: &mut Statuses,
    ret_query_id: QueryId,
    orig_path: QueryId,
    new_rms: HashSet<QueryPath>,
    result: (Vec<ColName>, Vec<TableView>),
  ) {
    // For now, each `query_id` here should point back to an MSQueryCoordES.
    // It must exist. Recall that if it were cancelled, then it would have deleted the all
    // child ReadTMStatuses.
    let full_coord = statuses.ms_coord_ess.get_mut(&orig_path).unwrap();
    let coord_es = cast!(FullMSQueryCoordES::Executing, full_coord).unwrap();

    // TODO: the below should be handled here.
    // let (res_schema, mut res_views) = result;
    // assert_eq!(ret_query_id, stage_query_id);
    //
    // // Look up the corresponding schema and assert that the incomding schema matches.
    // let (trans_table_name, _) = coord_es.query.trans_tables.get(*stage_idx).unwrap();
    // let (schema, _) = coord_es.query_plan.col_usage_nodes.get(trans_table_name).unwrap();
    // assert_eq!(schema, &res_schema);
    //
    // // There was only one ContextRow, so the result should have 1 TableView as well.
    // assert_eq!(res_views.len(), 1);
    // let res_view = res_views.into_iter().next().unwrap();
    //
    // // Add the results to the `trans_table_views`
    // coord_es.trans_table_views.push((trans_table_name.clone(), res_view));
    // coord_es.all_rms.extend(new_rms);

    match &coord_es.state {
      CoordState::Start => panic!(),
      CoordState::ReadStage { stage_idx, stage_query_id } => {
        let stage_idx = stage_idx.clone();
        let stage_query_id = stage_query_id.clone();
        ms_coord_process_stage_result::<T>(
          coord_es,
          &ret_query_id,
          &orig_path,
          new_rms,
          result,
          &stage_idx,
          &stage_query_id,
          // Slave references
          &mut self.rand,
          &mut self.network_output,
          &mut statuses.tm_statuses,
          &self.this_slave_group_id,
          &self.gossip,
          &self.sharding_config,
          &self.tablet_address_config,
          &self.slave_address_config,
        );
      }
      CoordState::WriteStage { stage_idx, stage_query_id } => {
        let stage_idx = stage_idx.clone();
        let stage_query_id = stage_query_id.clone();
        ms_coord_process_stage_result::<T>(
          coord_es,
          &ret_query_id,
          &orig_path,
          new_rms,
          result,
          &stage_idx,
          &stage_query_id,
          // Slave references
          &mut self.rand,
          &mut self.network_output,
          &mut statuses.tm_statuses,
          &self.this_slave_group_id,
          &self.gossip,
          &self.sharding_config,
          &self.tablet_address_config,
          &self.slave_address_config,
        );
      }
      CoordState::Preparing { .. } => panic!(),
      CoordState::Committing => panic!(),
      CoordState::Aborting => panic!(),
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  Utils
  // -----------------------------------------------------------------------------------------------

  /// We assume that `tablet_group_id` is a valid tablet.
  fn get_tablet_eid(&self, tablet_group_id: &TabletGroupId) -> &EndpointId {
    let sid = self.tablet_address_config.get(&tablet_group_id).unwrap();
    self.slave_address_config.get(&sid).unwrap()
  }
}

fn lookup_location(
  context: &Context,
  trans_table_name: &TransTableName,
) -> Option<TransTableLocationPrefix> {
  for location in &context.context_schema.trans_table_context_schema {
    if &location.trans_table_name == trans_table_name {
      return Some(location.clone());
    }
  }
  return None;
}

/// This function accepts the results for the subquery, and then decides either
/// to move onto the next stage, or start 2PC to commit the change.
fn ms_coord_process_stage_result<T: IOTypes>(
  coord_es: &mut MSQueryCoordES,
  ret_query_id: &QueryId,
  orig_path: &QueryId,
  new_rms: HashSet<QueryPath>,
  result: (Vec<ColName>, Vec<TableView>),
  stage_idx: &usize,
  stage_query_id: &QueryId,

  // Slave references
  rand: &mut T::RngCoreT,
  network_output: &mut T::NetworkOutT,
  tm_statuses: &mut HashMap<QueryId, TMStatus>,
  this_slave_group_id: &SlaveGroupId,
  gossip: &GossipData,
  sharding_config: &HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: &HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: &HashMap<SlaveGroupId, EndpointId>,
) {
  let (res_schema, mut res_views) = result;
  assert_eq!(ret_query_id, stage_query_id);

  // Look up the corresponding schema and assert that the incomding schema matches.
  let (trans_table_name, _) = coord_es.query.trans_tables.get(*stage_idx).unwrap();
  let (schema, _) = coord_es.query_plan.col_usage_nodes.get(trans_table_name).unwrap();
  assert_eq!(schema, &res_schema);

  // There was only one ContextRow, so the result should have 1 TableView as well.
  assert_eq!(res_views.len(), 1);
  let res_view = res_views.into_iter().next().unwrap();

  // Add the results to the `trans_table_views`
  coord_es.trans_table_views.push((trans_table_name.clone(), res_view));
  coord_es.all_rms.extend(new_rms);

  if *stage_idx < coord_es.query.trans_tables.len() {
    // Move onto the next stage.
    ms_coord_es_advance::<T>(
      coord_es,
      &orig_path,
      stage_idx + 1,
      // Slave references
      rand,
      network_output,
      tm_statuses,
      this_slave_group_id,
      gossip,
      sharding_config,
      tablet_address_config,
      slave_address_config,
    );
  } else {
    // Finish the ES by sending out Prepared.
    let sender_path = QueryPath {
      slave_group_id: this_slave_group_id.clone(),
      maybe_tablet_group_id: None,
      query_id: orig_path.clone(),
    };
    for query_path in &coord_es.all_rms {
      let eid = slave_address_config.get(&query_path.slave_group_id).unwrap();
      network_output.send(
        eid,
        msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
          query_path.maybe_tablet_group_id.clone().unwrap(),
          msg::TabletMessage::Query2PCPrepare(msg::Query2PCPrepare {
            sender_path: sender_path.clone(),
            ms_query_id: orig_path.clone(),
          }),
        )),
      );
    }

    coord_es.state = CoordState::Preparing { tablet_group_ids: coord_es.all_rms.clone() };
  }
}

/// This function advances the given `coord_es` to the next `stage_idx`. This stage
/// is gauranteed to be another `ReadStage` or `WriteStage`.
fn ms_coord_es_advance<T: IOTypes>(
  coord_es: &mut MSQueryCoordES,
  root_query_id: &QueryId,
  stage_idx: usize,

  // Slave references
  rand: &mut T::RngCoreT,
  network_output: &mut T::NetworkOutT,
  tm_statuses: &mut HashMap<QueryId, TMStatus>,
  this_slave_group_id: &SlaveGroupId,
  gossip: &GossipData,
  sharding_config: &HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: &HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: &HashMap<SlaveGroupId, EndpointId>,
) {
  // Get the corresponding MSQueryStage and FrozenColUsageNode.
  let (trans_table_name, ms_query_stage) = coord_es.query.trans_tables.get(stage_idx).unwrap();
  let (_, col_usage_node) = coord_es.query_plan.col_usage_nodes.get(trans_table_name).unwrap();

  // Compute the context of this `col_usage_node`. Recall there must be exactly one row.
  let external_trans_tables = node_external_trans_tables(col_usage_node);
  let mut context = Context::default();
  let mut context_row = ContextRow::default();
  for external_trans_table in &external_trans_tables {
    context.context_schema.trans_table_context_schema.push(TransTableLocationPrefix {
      source: NodeGroupId::Slave(this_slave_group_id.clone()),
      query_id: root_query_id.clone(),
      trans_table_name: external_trans_table.clone(),
    });
    context_row.trans_table_context_row.push(0);
  }
  context.context_rows.push(context_row);

  // Compute the `trans_table_schemas` using the `col_usage_nodes`.
  let mut trans_table_schemas = HashMap::<TransTableName, Vec<ColName>>::new();
  for external_trans_table in external_trans_tables {
    let (cols, _) = coord_es.query_plan.col_usage_nodes.get(&external_trans_table).unwrap();
    trans_table_schemas.insert(external_trans_table, cols.clone());
  }

  // Compute this MSQueryCoordESs QueryPath
  let root_query_path = QueryPath {
    slave_group_id: this_slave_group_id.clone(),
    maybe_tablet_group_id: None,
    query_id: root_query_id.clone(),
  };

  // Handle accordingly
  coord_es.state = match ms_query_stage {
    proc::MSQueryStage::SuperSimpleSelect(select_query) => {
      // Create ReadTMState
      let tm_query_id = mk_qid(rand);
      let mut status = TMStatus {
        node_group_ids: Default::default(),
        new_rms: Default::default(),
        responded_count: 0,
        tm_state: Default::default(),
        orig_p: OrigP::new(root_query_id.clone()),
      };

      // Path of this TMStatus to respond to.
      let sender_path = QueryPath {
        slave_group_id: this_slave_group_id.clone(),
        maybe_tablet_group_id: None,
        query_id: tm_query_id.clone(),
      };

      match &select_query.from {
        proc::TableRef::TablePath(table_path) => {
          // Add in the Tablets that manage this TablePath to `tm_state`,
          // and send out the PerformQuery
          for (_, tablet_group_id) in sharding_config.get(table_path).unwrap() {
            let child_query_id = mk_qid(rand);
            let sid = tablet_address_config.get(&tablet_group_id).unwrap();
            let eid = slave_address_config.get(&sid).unwrap();
            network_output.send(
              eid,
              msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
                tablet_group_id.clone(),
                msg::TabletMessage::PerformQuery(msg::PerformQuery {
                  root_query_path: root_query_path.clone(),
                  sender_path: sender_path.clone(),
                  query_id: child_query_id.clone(),
                  tier_map: coord_es.all_tier_maps.get(trans_table_name).unwrap().clone(),
                  query: msg::GeneralQuery::SuperSimpleTableSelectQuery(
                    msg::SuperSimpleTableSelectQuery {
                      timestamp: coord_es.timestamp.clone(),
                      context: context.clone(),
                      sql_query: select_query.clone(),
                      query_plan: QueryPlan {
                        gossip_gen: gossip.gossip_gen,
                        trans_table_schemas: trans_table_schemas.clone(),
                        col_usage_node: col_usage_node.clone(),
                      },
                    },
                  ),
                }),
              )),
            );

            let node_group_id = NodeGroupId::Tablet(tablet_group_id.clone());
            status.node_group_ids.insert(node_group_id, child_query_id.clone());
            status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
          }
        }
        proc::TableRef::TransTableName(trans_table_name) => {
          let location = lookup_location(&context, trans_table_name).unwrap();

          // Add in the Slave to `tm_state`, and send out the PerformQuery
          let child_query_id = mk_qid(rand);
          let eid = slave_address_config.get(this_slave_group_id).unwrap();
          network_output.send(
            eid,
            msg::NetworkMessage::Slave(msg::SlaveMessage::PerformQuery(msg::PerformQuery {
              root_query_path,
              sender_path,
              query_id: child_query_id.clone(),
              tier_map: coord_es.all_tier_maps.get(trans_table_name).unwrap().clone(),
              query: msg::GeneralQuery::SuperSimpleTransTableSelectQuery(
                msg::SuperSimpleTransTableSelectQuery {
                  location_prefix: location.clone(),
                  context: context.clone(),
                  sql_query: select_query.clone(),
                  query_plan: QueryPlan {
                    gossip_gen: gossip.gossip_gen,
                    trans_table_schemas,
                    col_usage_node: col_usage_node.clone(),
                  },
                },
              ),
            })),
          );

          status.node_group_ids.insert(location.source, child_query_id.clone());
          status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
        }
      }

      tm_statuses.insert(tm_query_id.clone(), status);
      CoordState::ReadStage { stage_idx, stage_query_id: tm_query_id }
    }
    proc::MSQueryStage::Update(update_query) => {
      // Create WriteTMState
      let tm_query_id = mk_qid(rand);
      let mut status = TMStatus {
        node_group_ids: Default::default(),
        new_rms: Default::default(),
        responded_count: 0,
        tm_state: Default::default(),
        orig_p: OrigP::new(root_query_id.clone()),
      };

      // Path of this WriteTMStatus to respond to.
      let sender_path = QueryPath {
        slave_group_id: this_slave_group_id.clone(),
        maybe_tablet_group_id: None,
        query_id: tm_query_id.clone(),
      };

      // Add in the Tablets that manage this TablePath to `write_tm_state`,
      // and send out the PerformQuery
      for (_, tablet_group_id) in sharding_config.get(&update_query.table).unwrap() {
        let child_query_id = mk_qid(rand);
        let sid = tablet_address_config.get(&tablet_group_id).unwrap();
        let eid = slave_address_config.get(&sid).unwrap();
        network_output.send(
          eid,
          msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
            tablet_group_id.clone(),
            msg::TabletMessage::PerformQuery(msg::PerformQuery {
              root_query_path: root_query_path.clone(),
              sender_path: sender_path.clone(),
              query_id: child_query_id.clone(),
              tier_map: coord_es.all_tier_maps.get(trans_table_name).unwrap().clone(),
              query: msg::GeneralQuery::UpdateQuery(msg::UpdateQuery {
                timestamp: coord_es.timestamp.clone(),
                context: context.clone(),
                sql_query: update_query.clone(),
                query_plan: QueryPlan {
                  gossip_gen: gossip.gossip_gen,
                  trans_table_schemas: trans_table_schemas.clone(),
                  col_usage_node: col_usage_node.clone(),
                },
              }),
            }),
          )),
        );

        let node_group_id = NodeGroupId::Tablet(tablet_group_id.clone());
        status.node_group_ids.insert(node_group_id, child_query_id.clone());
        status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
      }

      tm_statuses.insert(tm_query_id.clone(), status);
      CoordState::WriteStage { stage_idx, stage_query_id: tm_query_id }
    }
  };
}

// -----------------------------------------------------------------------------------------------
//  Convert from sqlparser AST to internal AST
// -----------------------------------------------------------------------------------------------

/// This function converts the sqlparser AST into our own internal
/// AST, `Query`. Recall that we can transform all DML and DQL transactions
/// together into a single Query, which is what we do here.
fn convert_ast(raw_query: &Vec<ast::Statement>) -> iast::Query {
  assert_eq!(raw_query.len(), 1, "Only one SQL statement support atm.");
  let stmt = &raw_query[0];
  match stmt {
    ast::Statement::Query(query) => return convert_query(query),
    _ => panic!("Unsupported ast::Statement {:?}", stmt),
  }
}

fn convert_query(query: &ast::Query) -> iast::Query {
  let mut ictes = Vec::<(String, iast::Query)>::new();
  if let Some(with) = &query.with {
    for cte in &with.cte_tables {
      ictes.push((cte.alias.name.value.clone(), convert_query(&cte.query)));
    }
  }
  let body = match &query.body {
    ast::SetExpr::Query(child_query) => {
      iast::QueryBody::Query(Box::new(convert_query(child_query)))
    }
    ast::SetExpr::Select(select) => {
      let from_clause = &select.from;
      assert_eq!(from_clause.len(), 1, "Joins with ',' not supported");
      assert!(from_clause[0].joins.is_empty(), "Joins not supported");
      if let ast::TableFactor::Table { name, .. } = &from_clause[0].relation {
        let ast::ObjectName(idents) = name;
        assert_eq!(idents.len(), 1, "Multi-part table references not supported");
        iast::QueryBody::SuperSimpleSelect(iast::SuperSimpleSelect {
          projection: convert_select_clause(&select.projection),
          from: idents[0].value.clone(),
          selection: if let Some(selection) = &select.selection {
            convert_expr(selection)
          } else {
            iast::ValExpr::Value { val: iast::Value::Boolean(true) }
          },
        })
      } else {
        panic!("TableFactor {:?} not supported", &from_clause[0].relation);
      }
    }
    ast::SetExpr::Insert(stmt) => {
      if let ast::Statement::Update { table_name, assignments, selection } = stmt {
        let ast::ObjectName(idents) = table_name;
        assert_eq!(idents.len(), 1, "Multi-part table references not supported");
        iast::QueryBody::Update(iast::Update {
          table: idents[0].value.clone(),
          assignments: assignments
            .iter()
            .map(|a| (a.id.value.clone(), convert_expr(&a.value)))
            .collect(),
          selection: if let Some(selection) = selection {
            convert_expr(selection)
          } else {
            iast::ValExpr::Value { val: iast::Value::Boolean(true) }
          },
        })
      } else {
        panic!("Statement type {:?} not supported", stmt)
      }
    }
    _ => panic!("Other stuff not supported"),
  };
  iast::Query { ctes: ictes, body }
}

fn convert_select_clause(select_clause: &Vec<ast::SelectItem>) -> Vec<String> {
  let mut select_list = Vec::<String>::new();
  for item in select_clause {
    match &item {
      ast::SelectItem::UnnamedExpr(expr) => {
        if let ast::Expr::Identifier(ident) = expr {
          select_list.push(ident.value.clone());
        } else {
          panic!("{:?} is not supported in SelectItem", item);
        }
      }
      _ => {
        panic!("{:?} is not supported in SelectItem", item);
      }
    }
  }
  select_list
}

fn convert_value(value: &ast::Value) -> iast::Value {
  match &value {
    ast::Value::Number(num, _) => iast::Value::Number(num.clone()),
    ast::Value::SingleQuotedString(string) => iast::Value::QuotedString(string.clone()),
    ast::Value::DoubleQuotedString(string) => iast::Value::QuotedString(string.clone()),
    ast::Value::Boolean(bool) => iast::Value::Boolean(bool.clone()),
    ast::Value::Null => iast::Value::Null,
    _ => panic!("Value type {:?} not supported.", value),
  }
}

fn convert_expr(expr: &ast::Expr) -> iast::ValExpr {
  match expr {
    ast::Expr::Identifier(ident) => iast::ValExpr::ColumnRef { col_ref: ident.value.clone() },
    ast::Expr::CompoundIdentifier(idents) => {
      assert_eq!(idents.len(), 2, "The only prefix fix for a column should be the table.");
      iast::ValExpr::ColumnRef { col_ref: idents[1].value.clone() }
    }
    ast::Expr::UnaryOp { op, expr } => {
      let iop = match op {
        ast::UnaryOperator::Minus => iast::UnaryOp::Minus,
        ast::UnaryOperator::Plus => iast::UnaryOp::Plus,
        ast::UnaryOperator::Not => iast::UnaryOp::Not,
        _ => panic!("UnaryOperator {:?} not supported", op),
      };
      iast::ValExpr::UnaryExpr { op: iop, expr: Box::new(convert_expr(expr)) }
    }
    ast::Expr::IsNull(expr) => {
      iast::ValExpr::UnaryExpr { op: iast::UnaryOp::IsNull, expr: Box::new(convert_expr(expr)) }
    }
    ast::Expr::IsNotNull(expr) => {
      iast::ValExpr::UnaryExpr { op: iast::UnaryOp::IsNotNull, expr: Box::new(convert_expr(expr)) }
    }
    ast::Expr::BinaryOp { op, left, right } => {
      let iop = match op {
        ast::BinaryOperator::Plus => iast::BinaryOp::Plus,
        ast::BinaryOperator::Minus => iast::BinaryOp::Minus,
        ast::BinaryOperator::Multiply => iast::BinaryOp::Multiply,
        ast::BinaryOperator::Divide => iast::BinaryOp::Divide,
        ast::BinaryOperator::Modulus => iast::BinaryOp::Modulus,
        ast::BinaryOperator::StringConcat => iast::BinaryOp::StringConcat,
        ast::BinaryOperator::Gt => iast::BinaryOp::Gt,
        ast::BinaryOperator::Lt => iast::BinaryOp::Lt,
        ast::BinaryOperator::GtEq => iast::BinaryOp::GtEq,
        ast::BinaryOperator::LtEq => iast::BinaryOp::LtEq,
        ast::BinaryOperator::Spaceship => iast::BinaryOp::Spaceship,
        ast::BinaryOperator::Eq => iast::BinaryOp::Eq,
        ast::BinaryOperator::NotEq => iast::BinaryOp::NotEq,
        ast::BinaryOperator::And => iast::BinaryOp::And,
        ast::BinaryOperator::Or => iast::BinaryOp::Or,
        _ => panic!("BinaryOperator {:?} not supported", op),
      };
      iast::ValExpr::BinaryExpr {
        op: iop,
        left: Box::new(convert_expr(left)),
        right: Box::new(convert_expr(right)),
      }
    }
    ast::Expr::Value(value) => iast::ValExpr::Value { val: convert_value(value) },
    ast::Expr::Subquery(query) => iast::ValExpr::Subquery { query: Box::new(convert_query(query)) },
    _ => panic!("Expr {:?} not supported", expr),
  }
}

impl MSQueryCoordReplanningES {
  fn start<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) {
    // First, we compute the ColUsageNode.
    let mut planner = ColUsagePlanner {
      gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
      timestamp: self.timestamp.clone(),
    };
    let col_usage_nodes = planner.plan_ms_query(&self.query);

    for (_, (_, child)) in &col_usage_nodes {
      if !child.external_cols.is_empty() {
        // If there are External Columns in any of the stages, then we need to consult the Master.
        let master_query_id = mk_qid(&mut ctx.rand);

        ctx.network_output.send(
          &ctx.master_eid,
          msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
            msg::PerformMasterFrozenColUsage {
              query_id: master_query_id.clone(),
              timestamp: self.timestamp,
              trans_table_schemas: HashMap::new(),
              col_usage_tree: msg::ColUsageTree::MSQuery(self.query.clone()),
            },
          )),
        );
        ctx.master_query_map.insert(master_query_id.clone(), self.orig_p.clone());

        // Advance Replanning State.
        self.state = MSQueryCoordReplanningS::MasterQueryReplanning { master_query_id };
        return;
      }
    }

    self.state = MSQueryCoordReplanningS::Done(Some(CoordQueryPlan {
      gossip_gen: ctx.gossip.gossip_gen,
      col_usage_nodes,
    }));
  }
}
