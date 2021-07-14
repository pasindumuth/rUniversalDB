use crate::col_usage::{node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, lookup_pos, map_insert, merge_table_views, mk_qid, Clock, GossipData, IOTypes,
  NetworkOut, OrigP, QueryPlan, TMStatus, TMWaitValue, TabletForwardOut,
};
use crate::gr_query_es::{GRQueryAction, GRQueryES};
use crate::model::common::{
  iast, proc, ColName, ColType, Context, ContextRow, Gen, NodeGroupId, QueryPath, SlaveGroupId,
  TablePath, TableView, TabletGroupId, TabletKeyRange, TierMap, Timestamp,
  TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message as msg;
use crate::model::message::{GeneralQuery, QueryError};
use crate::query_converter::convert_to_msquery;
use crate::server::{CommonQuery, ServerContext};
use crate::sql_parser::convert_ast;
use crate::trans_table_read_es::{
  FullTransTableReadES, TransQueryReplanningES, TransQueryReplanningS, TransTableAction,
  TransTableSource,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  MSQueryCoordES
// -----------------------------------------------------------------------------------------------
#[derive(Debug, Clone)]
struct CoordQueryPlan {
  gossip_gen: Gen,
  col_usage_nodes: Vec<(TransTableName, (Vec<ColName>, FrozenColUsageNode))>,
}

#[derive(Debug)]
struct PreparingState {
  prepared_rms: HashSet<QueryPath>,
}

#[derive(Debug)]
enum CoordState {
  Start,
  /// Here, `stage_query_id` is the QueryId of the TMStatus
  ReadStage {
    stage_idx: usize,
    stage_query_id: QueryId,
  },
  /// Here, `stage_query_id` is the QueryId of the TMStatus
  WriteStage {
    stage_idx: usize,
    stage_query_id: QueryId,
  },
  Preparing(PreparingState),
  Committing,
  Aborting,
}

impl CoordState {
  fn stage_idx(&self) -> Option<usize> {
    match self {
      CoordState::Start => Some(0),
      CoordState::ReadStage { stage_idx, .. } => Some(*stage_idx),
      CoordState::WriteStage { stage_idx, .. } => Some(*stage_idx),
      _ => None,
    }
  }

  fn stage_query_id(&self) -> Option<QueryId> {
    match self {
      CoordState::ReadStage { stage_query_id, .. } => Some(stage_query_id.clone()),
      CoordState::WriteStage { stage_query_id, .. } => Some(stage_query_id.clone()),
      _ => None,
    }
  }
}

#[derive(Debug)]
struct MSQueryCoordES {
  // Metadata copied from outside.
  request_id: RequestId,
  sender_eid: EndpointId,
  timestamp: Timestamp,

  query_id: QueryId,
  sql_query: proc::MSQuery,

  // Results of the query planning.
  query_plan: CoordQueryPlan,

  // Precomputed data for execute stages faster.
  all_tier_maps: HashMap<TransTableName, TierMap>,

  // The dynamically evolving fields.
  all_rms: HashSet<QueryPath>,
  trans_table_views: Vec<(TransTableName, (Vec<ColName>, TableView))>,
  state: CoordState,

  // Memory management fields
  registered_queries: HashSet<QueryPath>,
}

impl TransTableSource for MSQueryCoordES {
  fn get_instance(&self, trans_table_name: &TransTableName, idx: usize) -> &TableView {
    assert_eq!(idx, 1);
    let (_, instance) = lookup(&self.trans_table_views, trans_table_name).unwrap();
    instance
  }

  fn get_schema(&self, trans_table_name: &TransTableName) -> Vec<ColName> {
    let (schema, _) = lookup(&self.trans_table_views, trans_table_name).unwrap();
    schema.clone()
  }
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
  /// The following are needed for responding.
  sender_eid: EndpointId,
  request_id: RequestId,
  /// The query to do the replanning with.
  sql_query: proc::MSQuery,
  /// The OrigP of the Task holding this MSQueryCoordReplanningES
  query_id: QueryId,
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
  gr_query_ess: HashMap<QueryId, GRQueryES>,
  full_trans_table_read_ess: HashMap<QueryId, FullTransTableReadES>,
  tm_statuss: HashMap<QueryId, TMStatus>,
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
  fn ctx(&mut self) -> ServerContext<T> {
    ServerContext {
      rand: &mut self.rand,
      clock: &mut self.clock,
      network_output: &mut self.network_output,
      this_slave_group_id: &self.this_slave_group_id,
      maybe_this_tablet_group_id: None,
      master_eid: &self.master_eid,
      gossip: &mut self.gossip,
      sharding_config: &mut self.sharding_config,
      tablet_address_config: &mut self.tablet_address_config,
      slave_address_config: &mut self.slave_address_config,
      master_query_map: &mut self.master_query_map,
    }
  }

  pub fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::SlaveMessage) {
    match message {
      msg::SlaveMessage::PerformExternalQuery(external_query) => {
        match self.init_request(&external_query) {
          Ok(ms_query) => {
            let query_id = mk_qid(&mut self.rand);
            let request_id = &external_query.request_id;
            self.external_request_id_map.insert(request_id.clone(), query_id.clone());
            statuses.ms_coord_ess.insert(
              query_id.clone(),
              FullMSQueryCoordES::QueryReplanning(MSQueryCoordReplanningES {
                timestamp: self.clock.now(),
                sender_eid: external_query.sender_eid,
                request_id: external_query.request_id,
                sql_query: ms_query,
                query_id: query_id.clone(),
                state: MSQueryCoordReplanningS::Start,
              }),
            );
            self.drive_ms_coord(statuses, query_id);
          }
          Err(payload) => self.network_output.send(
            &external_query.sender_eid,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(
              msg::ExternalQueryAborted { request_id: external_query.request_id, payload },
            )),
          ),
        }
      }
      msg::SlaveMessage::CancelExternalQuery(_) => unimplemented!(),
      msg::SlaveMessage::TabletMessage(tablet_group_id, tablet_msg) => {
        self.tablet_forward_output.forward(&tablet_group_id, tablet_msg);
      }
      msg::SlaveMessage::PerformQuery(perform_query) => {
        match perform_query.query {
          msg::GeneralQuery::SuperSimpleTransTableSelectQuery(query) => {
            // First, we check if the GRQueryES still exists in the Statuses, continuing
            // if so and aborting if not.
            if let Some(ms_coord_es) = statuses.ms_coord_ess.get(&query.location_prefix.query_id) {
              let es = cast!(FullMSQueryCoordES::Executing, ms_coord_es).unwrap();
              // Construct and start the TransQueryReplanningES
              let full_trans_table_es = map_insert(
                &mut statuses.full_trans_table_read_ess,
                &perform_query.query_id,
                FullTransTableReadES::QueryReplanning(TransQueryReplanningES {
                  root_query_path: perform_query.root_query_path,
                  tier_map: perform_query.tier_map,
                  query_id: perform_query.query_id.clone(),
                  location_prefix: query.location_prefix,
                  context: Rc::new(query.context),
                  sql_query: query.sql_query,
                  query_plan: query.query_plan,
                  sender_path: perform_query.sender_path,
                  orig_p: OrigP::new(perform_query.query_id.clone()),
                  state: TransQueryReplanningS::Start,
                  timestamp: es.timestamp.clone(),
                }),
              );

              let action = full_trans_table_es.start(&mut self.ctx(), es);
              self.handle_trans_es_action(statuses, perform_query.query_id, action);
            } else if let Some(es) = statuses.gr_query_ess.get(&query.location_prefix.query_id) {
              // Construct and start the TransQueryReplanningES
              let full_trans_table_es = map_insert(
                &mut statuses.full_trans_table_read_ess,
                &perform_query.query_id,
                FullTransTableReadES::QueryReplanning(TransQueryReplanningES {
                  root_query_path: perform_query.root_query_path,
                  tier_map: perform_query.tier_map,
                  query_id: perform_query.query_id.clone(),
                  location_prefix: query.location_prefix,
                  context: Rc::new(query.context),
                  sql_query: query.sql_query,
                  query_plan: query.query_plan,
                  sender_path: perform_query.sender_path,
                  orig_p: OrigP::new(perform_query.query_id.clone()),
                  state: TransQueryReplanningS::Start,
                  timestamp: es.timestamp.clone(),
                }),
              );

              let action = full_trans_table_es.start(&mut self.ctx(), es);
              self.handle_trans_es_action(statuses, perform_query.query_id, action);
            } else {
              // This means that the target GRQueryES was deleted. We can send back an
              // Abort with LateralError. Exit and Clean Up will be done later.
              self.ctx().send_query_error(
                perform_query.sender_path,
                perform_query.query_id,
                msg::QueryError::LateralError,
              );
              return;
            }
          }
          GeneralQuery::SuperSimpleTableSelectQuery(_) => panic!(),
          GeneralQuery::UpdateQuery(_) => panic!(),
        }
      }
      msg::SlaveMessage::CancelQuery(cancel_query) => {
        self.exit_and_clean_up(statuses, cancel_query.query_id);
      }
      msg::SlaveMessage::QuerySuccess(query_success) => {
        self.handle_query_success(statuses, query_success);
      }
      msg::SlaveMessage::QueryAborted(_) => unimplemented!(),
      msg::SlaveMessage::Query2PCPrepared(prepared) => self.handle_prepared(statuses, prepared),
      msg::SlaveMessage::Query2PCAborted(_) => panic!(),
      msg::SlaveMessage::MasterFrozenColUsageAborted(_) => unimplemented!(),
      msg::SlaveMessage::MasterFrozenColUsageSuccess(_) => unimplemented!(),
      msg::SlaveMessage::RegisterQuery(register) => self.handle_register_query(statuses, register),
    }
  }

  /// A convenience function for sending messages to `query_path`s that are Tablets.
  /// The Slave most often communicates only with Tablets.
  fn send_to_tablet(&mut self, query_path: &QueryPath, tablet_message: msg::TabletMessage) {
    let eid = self.slave_address_config.get(&query_path.slave_group_id).unwrap();
    self.network_output.send(
      eid,
      msg::NetworkMessage::Slave(msg::SlaveMessage::TabletMessage(
        query_path.maybe_tablet_group_id.clone().unwrap(),
        tablet_message,
      )),
    );
  }

  /// Does some initial validations and MSQuery processing before we start
  /// servicing the request.
  fn init_request(
    &self,
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

  /// Here, we expect a FullMSQueryCoordES to exist for `query_id` in the QueryReplanning
  /// state. This is the first function that's called to drive its execution.
  fn drive_ms_coord(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    let full_es = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
    let plan_es = cast!(FullMSQueryCoordES::QueryReplanning, full_es).unwrap();
    plan_es.start(self);
    if let MSQueryCoordReplanningS::Done(maybe_plan) = &plan_es.state {
      if let Some(query_plan) = maybe_plan {
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

        *full_es = FullMSQueryCoordES::Executing(MSQueryCoordES {
          request_id: plan_es.request_id.clone(),
          sender_eid: plan_es.sender_eid.clone(),
          timestamp: plan_es.timestamp.clone(),
          query_id: query_id.clone(),
          sql_query: plan_es.sql_query.clone(),
          query_plan: query_plan.clone(),
          all_tier_maps,
          all_rms: Default::default(),
          trans_table_views: vec![],
          state: CoordState::Start,
          registered_queries: Default::default(),
        });

        // Move the ES onto the next stage.
        self.advance_ms_coord_es(statuses, query_id);
      } else {
        // Here, the QueryReplanning had failed. Recall that QueryReplanning will
        // have sent back the proper error message, so we just need to Exit and Clean Up.
        self.exit_and_clean_up(statuses, query_id);
      }
    }
  }

  /// This function accepts the results for the subquery, and then decides either
  /// to move onto the next stage, or start 2PC to commit the change.
  fn advance_ms_coord_es(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    let full_coord_es = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
    let coord_es = cast!(FullMSQueryCoordES::Executing, full_coord_es).unwrap();

    let next_stage_idx = coord_es.state.stage_idx().unwrap() + 1;
    if next_stage_idx < coord_es.sql_query.trans_tables.len() {
      self.process_ms_coord_es_stage(statuses, query_id, next_stage_idx);
    } else {
      // Finish the ES by sending out Prepared.
      let sender_path = QueryPath {
        slave_group_id: self.this_slave_group_id.clone(),
        maybe_tablet_group_id: None,
        query_id: coord_es.query_id.clone(),
      };
      for query_path in &coord_es.all_rms {
        self.send_to_tablet(
          &query_path,
          msg::TabletMessage::Query2PCPrepare(msg::Query2PCPrepare {
            sender_path: sender_path.clone(),
            ms_query_id: coord_es.query_id.clone(),
          }),
        );
      }

      coord_es.state = CoordState::Preparing(PreparingState { prepared_rms: HashSet::new() });
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
    let (trans_table_name, ms_query_stage) =
      coord_es.sql_query.trans_tables.get(stage_idx).unwrap();
    let (_, col_usage_node) =
      lookup(&coord_es.query_plan.col_usage_nodes, trans_table_name).unwrap();

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
      let (cols, _) = lookup(&coord_es.query_plan.col_usage_nodes, &external_trans_table).unwrap();
      trans_table_schemas.insert(external_trans_table, cols.clone());
    }

    // Compute this MSQueryCoordESs QueryPath
    let root_query_path = QueryPath {
      slave_group_id: self.this_slave_group_id.clone(),
      maybe_tablet_group_id: None,
      query_id: query_id.clone(),
    };

    // Create Construct the TMStatus that's going to be used to coordinate this stage.
    let tm_query_id = mk_qid(&mut self.rand);
    let mut tm_status = TMStatus {
      node_group_ids: Default::default(),
      query_id: tm_query_id.clone(),
      new_rms: Default::default(),
      responded_count: 0,
      tm_state: Default::default(),
      orig_p: OrigP::new(query_id.clone()),
    };

    // The `sender_path` for the TMStatus above.
    let sender_path = QueryPath {
      slave_group_id: self.this_slave_group_id.clone(),
      maybe_tablet_group_id: None,
      query_id: tm_query_id.clone(),
    };

    // Handle accordingly
    coord_es.state = match ms_query_stage {
      proc::MSQueryStage::SuperSimpleSelect(select_query) => {
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
              tm_status.node_group_ids.insert(node_group_id, child_query_id.clone());
              tm_status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
            }
          }
          proc::TableRef::TransTableName(trans_table_name) => {
            let location = lookup_location(&context, trans_table_name).unwrap();

            // Add in the Slave to `tm_state`, and send out the PerformQuery. Recall that
            // if we are doing a TransTableRead here, then the TransTable must be located here.
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

            tm_status.node_group_ids.insert(location.source, child_query_id.clone());
            tm_status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
          }
        }

        CoordState::ReadStage { stage_idx, stage_query_id: tm_query_id.clone() }
      }
      proc::MSQueryStage::Update(update_query) => {
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
          tm_status.node_group_ids.insert(node_group_id, child_query_id.clone());
          tm_status.tm_state.insert(child_query_id, TMWaitValue::Nothing);
        }

        CoordState::WriteStage { stage_idx, stage_query_id: tm_query_id.clone() }
      }
    };

    // Finally, add the TM Status to `statuses`.
    statuses.tm_statuss.insert(tm_query_id, tm_status);
  }

  /// Called when one of the child queries in the current Stages respond successfully.
  /// This accumulates the results and sends the result to the MSQueryCoordES when done.
  fn handle_query_success(&mut self, statuses: &mut Statuses, query_success: msg::QuerySuccess) {
    if let Some(tm_status) = statuses.tm_statuss.get_mut(&query_success.query_id) {
      // We just add the result of the `query_success` here.
      let tm_wait_value = tm_status.tm_state.get_mut(&query_success.return_qid).unwrap();
      *tm_wait_value = TMWaitValue::Result(query_success.result.clone());
      tm_status.new_rms.extend(query_success.new_rms.into_iter());
      tm_status.responded_count += 1;
      if tm_status.responded_count == tm_status.tm_state.len() {
        // Remove the `TMStatus` and take ownership
        let tm_status = statuses.tm_statuss.remove(&query_success.query_id).unwrap();
        // Merge there TableViews together
        let mut results = Vec::<(Vec<ColName>, Vec<TableView>)>::new();
        for (_, tm_wait_value) in tm_status.tm_state {
          results.push(cast!(TMWaitValue::Result, tm_wait_value).unwrap());
        }
        let merged_result = merge_table_views(results);
        self.handle_tm_done(
          statuses,
          tm_status.orig_p,
          tm_status.query_id,
          tm_status.new_rms,
          merged_result,
        );
      }
    }
  }

  // Routes the result of a TMStatus when it's done.
  fn handle_tm_done(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    tm_query_id: QueryId,
    new_rms: HashSet<QueryPath>,
    (schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    let query_id = orig_p.query_id;
    if let Some(full_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
      let coord_es = cast!(FullMSQueryCoordES::Executing, full_coord).unwrap();

      // We do some santity check on the result. We verify that the
      // TMStatus that just finished had the right QueryId.
      assert_eq!(tm_query_id, coord_es.state.stage_query_id().unwrap());
      // Look up the schema for the stage in the QueryPlan, and assert it's the same as the result.
      let stage_idx = coord_es.state.stage_idx().unwrap();
      let (trans_table_name, _) = coord_es.sql_query.trans_tables.get(stage_idx).unwrap();
      let (plan_schema, _) =
        lookup(&coord_es.query_plan.col_usage_nodes, trans_table_name).unwrap();
      assert_eq!(plan_schema, &schema);
      // Recall that since we only send out one ContextRow, there should only be one TableView.
      assert_eq!(table_views.len(), 1);

      // Then, the results to the `trans_table_views`
      let table_view = table_views.into_iter().next().unwrap();
      coord_es.trans_table_views.push((trans_table_name.clone(), (schema, table_view)));
      coord_es.all_rms.extend(new_rms);
      self.advance_ms_coord_es(statuses, query_id);
    } else if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
      let action =
        es.handle_tm_success(&mut self.ctx(), tm_query_id, new_rms, (schema, table_views));
      self.handle_gr_query_es_action(statuses, query_id, action);
    }
  }

  fn handle_query_aborted(&mut self, statuses: &mut Statuses, query_aborted: msg::QueryAborted) {
    if let Some(tm_status) = statuses.tm_statuss.remove(&query_aborted.return_qid) {
      // We Exit and Clean up this TMStatus (sending CancelQuery to all
      // remaining participants) and send the QueryAborted back to the orig_p
      for (node_group_id, child_query_id) in tm_status.node_group_ids {
        if tm_status.tm_state.get(&child_query_id).unwrap() == &TMWaitValue::Nothing
          && child_query_id != query_aborted.query_id
        {
          // If the child Query hasn't responded yet, and isn't also the Query that
          // just aborted, then we send it a CancelQuery
          self.ctx().send_to_node(
            node_group_id,
            CommonQuery::CancelQuery(msg::CancelQuery { query_id: child_query_id }),
          );
        }
      }

      // Finally, we propagate up the AbortData to the ES that owns this TMStatus
      self.handle_tm_aborted(statuses, tm_status.orig_p, query_aborted.payload);
    }
  }

  /// Handles a TMStatus aborting. If the originator was a GRQueryES, we can simply forward the
  /// `aborted_data` to that. Otherwise, if it was the MSQueryCoordES, then either the MSQuery
  /// has failed, either requiring an response to be sent or for the MSQuery to be retried.
  fn handle_tm_aborted(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    aborted_data: msg::AbortedData,
  ) {
    // Finally, we propagate up the AbortData to the GRQueryES that owns this TMStatus
    let query_id = orig_p.query_id;
    if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
      let action = es.handle_tm_aborted(&mut self.ctx(), aborted_data);
      self.handle_gr_query_es_action(statuses, query_id, action);
    } else if let Some(ms_coord_es) = statuses.ms_coord_ess.get_mut(&query_id) {
      let es = cast!(FullMSQueryCoordES::Executing, ms_coord_es).unwrap();
      match aborted_data {
        msg::AbortedData::ColumnsDNE { .. }
        | msg::AbortedData::QueryError(QueryError::TypeError { .. })
        | msg::AbortedData::QueryError(QueryError::RuntimeError { .. })
        | msg::AbortedData::QueryError(QueryError::ProjectedColumnsDNE { .. }) => {
          // Here, we simply respond to the External with a `QueryExecutionError`,
          // and Exit and Clean Up.
          self.network_output.send(
            &es.sender_eid,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(
              msg::ExternalQueryAborted {
                request_id: es.request_id.clone(),
                payload: msg::ExternalAbortedData::QueryExecutionError,
              },
            )),
          );
          self.exit_and_clean_up(statuses, query_id);
        }
        msg::AbortedData::QueryError(QueryError::WriteRegionConflictWithSubsequentRead)
        | msg::AbortedData::QueryError(QueryError::DeadlockSafetyAbortion)
        | msg::AbortedData::QueryError(QueryError::TimestampConflict) => {
          // Here, we need to Exit and Clean Up the MSQueryCoordES, but we need to try
          // everything again at a higher timestamp (as opposed to respond to the External).

          // First, we create a new MSQueryCoordES back in the initial state, using a Timestamp
          // that's strictly greater than the prior one.
          let query_id = mk_qid(&mut self.rand);
          let request_id = es.request_id.clone();
          let new_timestamp = self.clock.now();
          assert!(new_timestamp > es.timestamp);
          let new_ms_coord_es = FullMSQueryCoordES::QueryReplanning(MSQueryCoordReplanningES {
            timestamp: new_timestamp,
            sender_eid: es.sender_eid.clone(),
            request_id: es.request_id.clone(),
            sql_query: es.sql_query.clone(),
            query_id: query_id.clone(),
            state: MSQueryCoordReplanningS::Start,
          });

          // Next, we Exit and Clean Up the old MSQueryCoordES.
          self.exit_and_clean_up(statuses, query_id.clone());

          // Finally, we add the new MSQueryCoordES into the Slave and then execute it.
          self.external_request_id_map.insert(request_id.clone(), query_id.clone());
          statuses.ms_coord_ess.insert(query_id.clone(), new_ms_coord_es);
          self.drive_ms_coord(statuses, query_id);
        }
        // Recall that LateralErrors should never make it back to the MSQueryCoordES.
        msg::AbortedData::QueryError(QueryError::LateralError) => panic!(),
      }
    }
  }

  /// Handle a Prepared message sent to an MSQueryCoordES.
  fn handle_prepared(&mut self, statuses: &mut Statuses, prepared: msg::Query2PCPrepared) {
    let query_id = prepared.return_qid;
    if let Some(ms_coord_es) = statuses.ms_coord_ess.get_mut(&query_id) {
      let es = cast!(FullMSQueryCoordES::Executing, ms_coord_es).unwrap();
      let preparing = cast!(CoordState::Preparing, &mut es.state).unwrap();

      // First, insert the QueryPath of the RM into the prepare state.
      assert!(preparing.prepared_rms.contains(&prepared.rm_path));
      preparing.prepared_rms.insert(prepared.rm_path);

      // Next see if the prepare state is done.
      if preparing.prepared_rms.len() == es.all_rms.len() {
        // Here, we simply send out the commit message.
        for query_path in &es.all_rms {
          self.send_to_tablet(
            query_path,
            msg::TabletMessage::Query2PCCommit(msg::Query2PCCommit {
              ms_query_id: es.query_id.clone(),
            }),
          );
        }

        // Next, we need to inform all `registered_query`s that aren't an RM
        // that they should exit, since they were falsely added to this transaction.
        for reg_query_path in &es.registered_queries {
          if !es.all_rms.contains(reg_query_path) {
            self.send_to_tablet(
              reg_query_path,
              msg::TabletMessage::CancelQuery(msg::CancelQuery {
                query_id: reg_query_path.query_id.clone(),
              }),
            );
          }
        }

        // Finally, we send out an ExternalQuerySuccess with the appropriate TableView.
        let (_, (_, table_view)) = es
          .trans_table_views
          .iter()
          .find(|(trans_table_name, _)| trans_table_name == &es.sql_query.returning)
          .unwrap();
        self.network_output.send(
          &es.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(
            msg::ExternalQuerySuccess {
              request_id: es.request_id.clone(),
              timestamp: es.timestamp.clone(),
              result: table_view.clone(),
            },
          )),
        );

        // Exit and and Clean Up.
        self.exit_and_clean_up(statuses, query_id);
      }
    }
  }

  /// Routes the GRQueryES results the appropriate TransTableES.
  fn handle_gr_query_done(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    subquery_new_rms: HashSet<QueryPath>,
    (table_schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    let query_id = orig_p.query_id;
    let trans_read_es = statuses.full_trans_table_read_ess.get_mut(&query_id).unwrap();
    let prefix = trans_read_es.location_prefix();
    let action = if let Some(es) = statuses.gr_query_ess.get(&prefix.query_id) {
      trans_read_es.handle_subquery_done(
        &mut self.ctx(),
        es,
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      )
    } else if let Some(ms_coord_es) = statuses.ms_coord_ess.get(&prefix.query_id) {
      trans_read_es.handle_subquery_done(
        &mut self.ctx(),
        cast!(FullMSQueryCoordES::Executing, ms_coord_es).unwrap(),
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      )
    } else {
      trans_read_es.handle_internal_query_error(&mut self.ctx(), msg::QueryError::LateralError)
    };
    self.handle_trans_es_action(statuses, query_id, action);
  }

  // TODO: I feel like we should be able to pull out the TransTableSource lookup
  // into a function that takes in a generic lambda where the trait bound is defined
  // in the lambda itself. The function would assume the `query_id` points to a valid
  // TransTableRead, so it may take in the whole `statuses`.

  /// This function just routes the InternalColumnsDNE notification from the GRQueryES.
  /// Recall that the originator can only be a TransTableReadES and it must exist (since
  /// the GRQueryES had existed).
  fn handle_internal_columns_dne(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    rem_cols: Vec<ColName>,
  ) {
    let query_id = orig_p.query_id;
    let trans_read_es = statuses.full_trans_table_read_ess.get_mut(&query_id).unwrap();
    let prefix = trans_read_es.location_prefix();
    let action = if let Some(es) = statuses.gr_query_ess.get(&prefix.query_id) {
      trans_read_es.handle_internal_columns_dne(&mut self.ctx(), es, query_id.clone(), rem_cols)
    } else if let Some(ms_coord_es) = statuses.ms_coord_ess.get(&prefix.query_id) {
      let es = cast!(FullMSQueryCoordES::Executing, ms_coord_es).unwrap();
      trans_read_es.handle_internal_columns_dne(&mut self.ctx(), es, query_id.clone(), rem_cols)
    } else {
      trans_read_es.handle_internal_query_error(&mut self.ctx(), msg::QueryError::LateralError)
    };
    self.handle_trans_es_action(statuses, query_id, action);
  }

  /// Handles a `query_error` propagated up from a GRQueryES. Recall that the originator
  /// can only be a TransTableReadES and it must exist (since the GRQueryES had existed).
  fn handle_internal_query_error(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    query_error: msg::QueryError,
  ) {
    let query_id = orig_p.query_id;
    let trans_read_es = statuses.full_trans_table_read_ess.get_mut(&query_id).unwrap();
    let action = trans_read_es.handle_internal_query_error(&mut self.ctx(), query_error);
    self.handle_trans_es_action(statuses, query_id, action);
  }

  /// Handles the actions specified by a TransTableReadES.
  fn handle_trans_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: TransTableAction,
  ) {
    match action {
      TransTableAction::Wait => {}
      TransTableAction::SendSubqueries(gr_query_ess) => {
        /// Here, we have to add in the GRQueryESs and start them.
        let mut subquery_ids = Vec::<QueryId>::new();
        for gr_query_es in gr_query_ess {
          let subquery_id = gr_query_es.query_id.clone();
          statuses.gr_query_ess.insert(subquery_id.clone(), gr_query_es);
          subquery_ids.push(subquery_id);
        }

        // Drive GRQueries
        for query_id in subquery_ids {
          if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
            // Generally, we use an `if` guard in case one child Query aborts the parent and
            // thus all other children. (This won't happen for GRQueryESs, though)
            let action = es.start::<T>(&mut self.ctx());
            self.handle_gr_query_es_action(statuses, query_id, action);
          }
        }
      }
      TransTableAction::Done => {
        // Recall that all responses will have been sent. Here, the ES should not be
        // holding onto any other resources, so we can simply remove it.
        statuses.full_trans_table_read_ess.remove(&query_id);
      }
      TransTableAction::ExitAndCleanUp(subquery_ids) => {
        // Recall that all responses will have been sent. There only resources that the ES
        // has are subqueries, so we Exit and Clean Up them here.
        statuses.full_trans_table_read_ess.remove(&query_id);
        for subquery_id in subquery_ids {
          self.exit_and_clean_up(statuses, subquery_id);
        }
      }
    }
  }

  /// Handles the actions specified by a GRQueryES.
  fn handle_gr_query_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: GRQueryAction,
  ) {
    match action {
      GRQueryAction::ExecuteTMStatus(tm_status) => {
        statuses.tm_statuss.insert(tm_status.query_id.clone(), tm_status);
      }
      GRQueryAction::Done(res) => {
        let es = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_gr_query_done(
          statuses,
          es.orig_p,
          es.query_id,
          res.new_rms,
          (res.schema, res.result),
        );
      }
      GRQueryAction::InternalColumnsDNE(rem_cols) => {
        let es = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_internal_columns_dne(statuses, es.orig_p, rem_cols);
      }
      GRQueryAction::QueryError(query_error) => {
        let es = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_internal_query_error(statuses, es.orig_p, query_error);
      }
      GRQueryAction::ExitAndCleanUp(subquery_ids) => {
        // Recall that all responses will have been sent. There only resources that the ES
        // has are subqueries, so we Exit and Clean Up them here.
        statuses.gr_query_ess.remove(&query_id);
        for subquery_id in subquery_ids {
          self.exit_and_clean_up(statuses, subquery_id);
        }
      }
    }
  }

  // Handle a RegisterQuery sent by an MSQuery to an MSQueryCoordES.
  fn handle_register_query(&mut self, statuses: &mut Statuses, register: msg::RegisterQuery) {
    if let Some(ms_coord_es) = statuses.ms_coord_ess.get_mut(&register.root_query_id) {
      // Here, the MSQueryCoordES still exists and we register the incoming MSQueryES.
      let es = cast!(FullMSQueryCoordES::Executing, ms_coord_es).unwrap();
      es.registered_queries.insert(register.query_path);
    } else {
      // Otherwise, the MSQueryCoordES no longer exists, and we should
      // cancel MSQueryES immediately.
      self.send_to_tablet(
        &register.query_path,
        msg::TabletMessage::CancelQuery(msg::CancelQuery {
          query_id: register.query_path.query_id.clone(),
        }),
      );
    }
  }

  /// This function is used to initiate an Exit and Clean Up of ESs. This is needed to handle
  /// CancelQuery's, as well as when one on ES wants to Exit and Clean Up another ES. Note that
  /// We allow the ES at `query_id` to be in any state, and to not even exist.
  fn exit_and_clean_up(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    if let Some(ms_coord_es) = statuses.ms_coord_ess.remove(&query_id) {
      match ms_coord_es {
        FullMSQueryCoordES::QueryReplanning(plan_es) => {
          self.external_request_id_map.remove(&plan_es.request_id);
          match plan_es.state {
            MSQueryCoordReplanningS::Start => {}
            MSQueryCoordReplanningS::MasterQueryReplanning { master_query_id } => {
              // Remove if present
              if self.master_query_map.remove(&master_query_id).is_some() {
                // If the removal was successful, we should also send a Cancellation
                // message to the Master.
                self.network_output.send(
                  &self.master_eid,
                  msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
                    msg::CancelMasterFrozenColUsage { query_id: master_query_id },
                  )),
                );
              }
            }
            MSQueryCoordReplanningS::Done(_) => {}
          }
        }
        FullMSQueryCoordES::Executing(es) => {
          self.external_request_id_map.remove(&es.request_id);
          match es.state {
            CoordState::Start => {}
            CoordState::ReadStage { stage_query_id, .. }
            | CoordState::WriteStage { stage_query_id, .. } => {
              // Clean up any Registered Queries in the MSQueryCoordES. Recall that in the
              // absence of Commit/Abort, this is the only way that MSQueryESs can get cleaned up.
              for registered_query in es.registered_queries {
                self.ctx().send_to_path(
                  registered_query.clone(),
                  CommonQuery::CancelQuery(msg::CancelQuery {
                    query_id: registered_query.query_id,
                  }),
                )
              }
              // Clean up the child TMStatus that's currently executing.
              self.exit_and_clean_up(statuses, stage_query_id);
            }
            CoordState::Preparing(_) => {
              // Recall that here, all non-`all_rms` in `registered_queries` should have gotten
              // a CancelQuery, so we don't have to do that again.
              for query_path in es.all_rms {
                self.send_to_tablet(
                  &query_path,
                  msg::TabletMessage::Query2PCAbort(msg::Query2PCAbort {
                    ms_query_id: es.query_id.clone(),
                  }),
                )
              }
            }
            CoordState::Committing => {}
            CoordState::Aborting => {}
          }
        }
      }
    } else if let Some(es) = statuses.gr_query_ess.get_mut(&query_id) {
      // Here, we only take a `&mut` to the GRQueryES, since `handle_gr_query_es_action`
      // needs it to be present before deleting it.
      let action = es.exit_and_clean_up(&mut self.ctx());
      self.handle_gr_query_es_action(statuses, query_id, action);
    } else if let Some(trans_read_es) = statuses.full_trans_table_read_ess.get_mut(&query_id) {
      // Here, we only take a `&mut` to the TransTableRead, since `handle_trans_es_action`
      // needs it to be present before deleting it.
      let action = trans_read_es.exit_and_clean_up(&mut self.ctx());
      self.handle_trans_es_action(statuses, query_id, action);
    } else if let Some(tm_status) = statuses.tm_statuss.remove(&query_id) {
      // We Exit and Clean up this TMStatus (sending CancelQuery to all remaining participants)
      for (node_group_id, child_query_id) in tm_status.node_group_ids {
        if tm_status.tm_state.get(&child_query_id).unwrap() == &TMWaitValue::Nothing {
          // If the child Query hasn't responded, then sent it a CancelQuery
          self.ctx().send_to_node(
            node_group_id,
            CommonQuery::CancelQuery(msg::CancelQuery { query_id: child_query_id }),
          );
        }
      }
    }
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

// -----------------------------------------------------------------------------------------------
//  QueryReplanning
// -----------------------------------------------------------------------------------------------

impl MSQueryCoordReplanningES {
  fn start<T: IOTypes>(&mut self, ctx: &mut SlaveContext<T>) {
    // First, we compute the ColUsageNode.
    let mut planner = ColUsagePlanner {
      gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
      timestamp: self.timestamp.clone(),
    };
    let col_usage_nodes = planner.plan_ms_query(&self.sql_query);

    for (_, (_, child)) in &col_usage_nodes {
      if !child.external_cols.is_empty() {
        // If there are External Columns in any of the stages, then we need to consult the Master.
        let master_query_id = mk_qid(&mut ctx.rand);

        ctx.network_output.send(
          &ctx.master_eid,
          msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
            msg::PerformMasterFrozenColUsage {
              sender_path: QueryPath {
                slave_group_id: ctx.this_slave_group_id.clone(),
                maybe_tablet_group_id: None,
                query_id: self.query_id.clone(),
              },
              query_id: master_query_id.clone(),
              timestamp: self.timestamp,
              trans_table_schemas: HashMap::new(),
              col_usage_tree: msg::ColUsageTree::MSQuery(self.sql_query.clone()),
            },
          )),
        );
        ctx.master_query_map.insert(master_query_id.clone(), OrigP::new(self.query_id.clone()));

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
