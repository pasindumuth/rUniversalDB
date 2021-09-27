use crate::col_usage::{node_external_trans_tables, ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, lookup_pos, map_insert, merge_table_views, mk_qid, remove_item, Clock, GossipData,
  IOTypes, NetworkOut, OrigP, TMStatus, TableSchema, TabletForwardOut,
};
use crate::gr_query_es::{GRQueryAction, GRQueryES};
use crate::model::common::{
  iast, proc, ColName, ColType, Context, ContextRow, CoordGroupId, Gen, LeadershipId, NodeGroupId,
  NodePath, PaxosGroupId, QueryPath, SlaveGroupId, TablePath, TableView, TabletGroupId,
  TabletKeyRange, TierMap, Timestamp, TransTableLocationPrefix, TransTableName,
};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message as msg;
use crate::ms_query_coord_es::{
  FullMSCoordES, MSCoordES, MSQueryCoordAction, MSQueryCoordReplanningES, MSQueryCoordReplanningS,
};
use crate::paxos::LeaderChanged;
use crate::query_converter::convert_to_msquery;
use crate::server::{CommonQuery, ServerContext};
use crate::sql_parser::convert_ast;
use crate::tablet::{GRQueryESWrapper, TransTableReadESWrapper};
use crate::trans_table_read_es::{
  TransExecutionS, TransTableAction, TransTableReadES, TransTableSource,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

// -----------------------------------------------------------------------------------------------
//  CoordForwardMsg
// -----------------------------------------------------------------------------------------------

pub enum CoordForwardMsg {
  ExternalMessage(msg::SlaveExternalReq),
  CoordMessage(msg::CoordMessage),
  GossipData(Arc<GossipData>),
  RemoteLeaderChanged(msg::RemoteLeaderChanged),
  LeaderChanged(LeaderChanged),
}

// -----------------------------------------------------------------------------------------------
//  Coord Statuses
// -----------------------------------------------------------------------------------------------

/// A wrapper around MSCoordES that keeps track of the child queries it created. We
/// we use this for resource management
#[derive(Debug)]
struct MSCoordESWrapper {
  request_id: RequestId,
  sender_eid: EndpointId,
  child_queries: Vec<QueryId>,
  es: FullMSCoordES,
}

/// This contains every TabletStatus. Every QueryId here is unique across all
/// other members here.
#[derive(Debug, Default)]
pub struct Statuses {
  ms_coord_ess: HashMap<QueryId, MSCoordESWrapper>,
  gr_query_ess: HashMap<QueryId, GRQueryESWrapper>,
  trans_table_read_ess: HashMap<QueryId, TransTableReadESWrapper>,
  tm_statuss: HashMap<QueryId, TMStatus>,
}

// -----------------------------------------------------------------------------------------------
//  Coord State
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct CoordState<T: IOTypes> {
  coord_context: CoordContext<T>,
  statuses: Statuses,
}

/// The CoordState that holds all the state of the Coord
#[derive(Debug)]
pub struct CoordContext<T: IOTypes> {
  /// IO Objects.
  pub rand: T::RngCoreT,
  pub clock: T::ClockT,
  pub network_output: T::NetworkOutT,

  /// Metadata
  pub this_slave_group_id: SlaveGroupId,
  pub this_coord_group_id: CoordGroupId,
  pub master_eid: EndpointId,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// Paxos
  pub leader_map: HashMap<PaxosGroupId, LeadershipId>,

  /// External Query Management
  pub external_request_id_map: HashMap<RequestId, QueryId>,
}

impl<T: IOTypes> CoordState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    this_slave_group_id: SlaveGroupId,
    this_coord_group_id: CoordGroupId,
    master_eid: EndpointId,
    gossip: Arc<GossipData>,
    leader_map: HashMap<PaxosGroupId, LeadershipId>,
  ) -> CoordState<T> {
    CoordState {
      coord_context: CoordContext {
        rand,
        clock,
        network_output,
        this_slave_group_id,
        this_coord_group_id,
        master_eid,
        gossip,
        leader_map,
        external_request_id_map: Default::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_input(&mut self, coord_input: CoordForwardMsg) {
    self.coord_context.handle_input(&mut self.statuses, coord_input);
  }
}

impl<T: IOTypes> CoordContext<T> {
  pub fn ctx(&mut self) -> ServerContext<T> {
    ServerContext {
      rand: &mut self.rand,
      clock: &mut self.clock,
      network_output: &mut self.network_output,
      this_slave_group_id: &self.this_slave_group_id,
      maybe_this_tablet_group_id: None,
      master_eid: &self.master_eid,
      gossip: &mut self.gossip,
    }
  }

  pub fn handle_input(&mut self, statuses: &mut Statuses, coord_input: CoordForwardMsg) {
    match coord_input {
      CoordForwardMsg::ExternalMessage(message) => {
        match message {
          msg::SlaveExternalReq::PerformExternalQuery(external_query) => {
            match self.init_request(&external_query) {
              Ok(ms_query) => {
                let query_id = mk_qid(&mut self.rand);
                let request_id = &external_query.request_id;
                self.external_request_id_map.insert(request_id.clone(), query_id.clone());
                let ms_coord = map_insert(
                  &mut statuses.ms_coord_ess,
                  &query_id,
                  MSCoordESWrapper {
                    request_id: external_query.request_id,
                    sender_eid: external_query.sender_eid,
                    child_queries: vec![],
                    es: FullMSCoordES::QueryReplanning(MSQueryCoordReplanningES {
                      timestamp: self.clock.now(),
                      sql_query: ms_query,
                      query_id: query_id.clone(),
                      state: MSQueryCoordReplanningS::Start,
                    }),
                  },
                );
                let action = ms_coord.es.start(self);
                self.handle_ms_coord_es_action(statuses, query_id, action);
              }
              Err(payload) => self.network_output.send(
                &external_query.sender_eid,
                msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(
                  msg::ExternalQueryAborted { request_id: external_query.request_id, payload },
                )),
              ),
            }
          }
          msg::SlaveExternalReq::CancelExternalQuery(cancel) => {
            if let Some(query_id) = self.external_request_id_map.get(&cancel.request_id) {
              // ECU the transaction if it exists.
              self.exit_and_clean_up(statuses, query_id.clone());
            }

            // Recall that we need to respond with an ExternalQueryAborted
            // to confirm the cancellation.
            self.network_output.send(
              &cancel.sender_eid,
              msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(
                msg::ExternalQueryAborted {
                  request_id: cancel.request_id,
                  payload: msg::ExternalAbortedData::ConfirmCancel,
                },
              )),
            );
          }
        }
      }
      CoordForwardMsg::CoordMessage(message) => {
        match message {
          msg::CoordMessage::PerformQuery(perform_query) => {
            match perform_query.query {
              msg::GeneralQuery::SuperSimpleTransTableSelectQuery(query) => {
                // First, we check if the MSCoordES or GRCoordES still exists in the Statuses,
                // continuing if so and aborting if not.
                if let Some(ms_coord) = statuses.ms_coord_ess.get(&query.location_prefix.query_id) {
                  let es = ms_coord.es.to_exec();
                  // Construct and start the TransQueryReplanningES
                  let trans_table = map_insert(
                    &mut statuses.trans_table_read_ess,
                    &perform_query.query_id,
                    TransTableReadESWrapper {
                      sender_path: perform_query.sender_path.clone(),
                      child_queries: vec![],
                      es: TransTableReadES {
                        root_query_path: perform_query.root_query_path,
                        location_prefix: query.location_prefix,
                        context: Rc::new(query.context),
                        sender_path: perform_query.sender_path,
                        query_id: perform_query.query_id.clone(),
                        sql_query: query.sql_query,
                        query_plan: query.query_plan,
                        new_rms: Default::default(),
                        state: TransExecutionS::Start,
                        timestamp: es.timestamp.clone(),
                      },
                    },
                  );

                  let action = trans_table.es.start(&mut self.ctx(), es);
                  self.handle_trans_read_es_action(statuses, perform_query.query_id, action);
                } else if let Some(gr_query) =
                  statuses.gr_query_ess.get(&query.location_prefix.query_id)
                {
                  // Construct and start the TransQueryReplanningES
                  let trans_table = map_insert(
                    &mut statuses.trans_table_read_ess,
                    &perform_query.query_id,
                    TransTableReadESWrapper {
                      sender_path: perform_query.sender_path.clone(),
                      child_queries: vec![],
                      es: TransTableReadES {
                        root_query_path: perform_query.root_query_path,
                        location_prefix: query.location_prefix,
                        context: Rc::new(query.context),
                        sender_path: perform_query.sender_path,
                        query_id: perform_query.query_id.clone(),
                        sql_query: query.sql_query,
                        query_plan: query.query_plan,
                        new_rms: Default::default(),
                        state: TransExecutionS::Start,
                        timestamp: gr_query.es.timestamp.clone(),
                      },
                    },
                  );

                  let action = trans_table.es.start(&mut self.ctx(), &gr_query.es);
                  self.handle_trans_read_es_action(statuses, perform_query.query_id, action);
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
              msg::GeneralQuery::SuperSimpleTableSelectQuery(_) => panic!(),
              msg::GeneralQuery::UpdateQuery(_) => panic!(),
            }
          }
          msg::CoordMessage::CancelQuery(cancel_query) => {
            self.exit_and_clean_up(statuses, cancel_query.query_id);
          }
          msg::CoordMessage::QuerySuccess(query_success) => {
            self.handle_query_success(statuses, query_success);
          }
          msg::CoordMessage::QueryAborted(query_aborted) => {
            self.handle_query_aborted(statuses, query_aborted);
          }
          msg::CoordMessage::FinishQueryPrepared(prepared) => {
            let query_id = prepared.return_qid.clone();
            if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
              let action = ms_coord.es.handle_prepared(self, prepared);
              self.handle_ms_coord_es_action(statuses, query_id, action);
            }
          }
          msg::CoordMessage::FinishQueryAborted(aborted) => {
            let query_id = aborted.return_qid.clone();
            if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
              let action = ms_coord.es.handle_aborted(self, aborted);
              self.handle_ms_coord_es_action(statuses, query_id, action);
            }
          }
          msg::CoordMessage::MasterFrozenColUsageAborted(_) => {
            panic!(); // In practice, this is never received.
          }
          msg::CoordMessage::MasterFrozenColUsageSuccess(success) => {
            // Update the Gossip with incoming Gossip data.
            // let gossip_data = success.gossip.clone().to_gossip();
            // if self.gossip.gossip_gen < gossip_data.gossip_gen {
            //   self.gossip = Arc::new(gossip_data);
            // }

            // Route the response to the appropriate ES.
            let query_id = success.return_qid;
            if let Some(trans_read) = statuses.trans_table_read_ess.get_mut(&query_id) {
              // TODO: remove this section.
              // let prefix = trans_read.es.location_prefix();
              // let action = if let Some(gr_query) = statuses.gr_query_ess.get(&prefix.query_id) {
              //   trans_read.es.handle_master_response(
              //     &mut self.ctx(),
              //     &gr_query.es,
              //     success.gossip.gossip_gen,
              //     success.frozen_col_usage_tree,
              //   )
              // } else if let Some(ms_coord) = statuses.ms_coord_ess.get(&prefix.query_id) {
              //   trans_read.es.handle_master_response(
              //     &mut self.ctx(),
              //     ms_coord.es.to_exec(),
              //     success.gossip.gossip_gen,
              //     success.frozen_col_usage_tree,
              //   )
              // } else {
              //   // TODO: I'm not fully convinced it's a good practice to use
              //   // handle_internal_query_error for this.
              //   trans_read
              //     .es
              //     .handle_internal_query_error(&mut self.ctx(), msg::QueryError::LateralError)
              // };
              // self.handle_trans_read_es_action(statuses, query_id, action);
            } else if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
              let action = ms_coord.es.handle_master_response(
                self,
                success.gossip.gen,
                success.frozen_col_usage_tree,
              );
              self.handle_ms_coord_es_action(statuses, query_id, action);
            }
          }
          msg::CoordMessage::RegisterQuery(register) => {
            self.handle_register_query(statuses, register)
          }
          msg::CoordMessage::FinishQueryInformPrepared(_) => panic!(),
          msg::CoordMessage::FinishQueryWait(_) => panic!(),
        }
      }
      CoordForwardMsg::GossipData(_) => panic!(),
      CoordForwardMsg::RemoteLeaderChanged(_) => panic!(),
      CoordForwardMsg::LeaderChanged(_) => panic!(),
    }
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
          // Convert to MSQuery
          let internal_ast = convert_ast(&parsed_ast);
          convert_to_msquery(&self.gossip.db_schema, internal_ast)
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

  /// Called when one of the child queries in the current Stages respond successfully.
  /// This accumulates the results and sends the result to the MSCoordES when done.
  fn handle_query_success(&mut self, statuses: &mut Statuses, query_success: msg::QuerySuccess) {
    let tm_query_id = &query_success.return_qid;
    if let Some(tm_status) = statuses.tm_statuss.get_mut(tm_query_id) {
      // We just add the result of the `query_success` here.
      let node_path = query_success.responder_path.node_path;
      tm_status.tm_state.insert(node_path, Some(query_success.result.clone()));
      tm_status.new_rms.extend(query_success.new_rms);
      tm_status.responded_count += 1;
      if tm_status.responded_count == tm_status.tm_state.len() {
        // Remove the `TMStatus` and take ownership
        let tm_status = statuses.tm_statuss.remove(tm_query_id).unwrap();
        // Merge there TableViews together
        let mut results = Vec::<(Vec<ColName>, Vec<TableView>)>::new();
        for (_, rm_result) in tm_status.tm_state {
          results.push(rm_result.unwrap());
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
    tm_qid: QueryId,
    new_rms: HashSet<QueryPath>,
    (schema, table_views): (Vec<ColName>, Vec<TableView>),
  ) {
    let query_id = orig_p.query_id;
    if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
      // Route TM results to MSQueryES
      remove_item(&mut ms_coord.child_queries, &tm_qid);
      let action = ms_coord.es.handle_tm_success(self, tm_qid, new_rms, (schema, table_views));
      self.handle_ms_coord_es_action(statuses, query_id, action);
    } else if let Some(gr_query) = statuses.gr_query_ess.get_mut(&query_id) {
      // Route TM results to GRQueryES
      remove_item(&mut gr_query.child_queries, &tm_qid);
      let action =
        gr_query.es.handle_tm_success(&mut self.ctx(), tm_qid, new_rms, (schema, table_views));
      self.handle_gr_query_es_action(statuses, query_id, action);
    }
  }

  fn handle_query_aborted(&mut self, statuses: &mut Statuses, query_aborted: msg::QueryAborted) {
    if let Some(tm_status) = statuses.tm_statuss.remove(&query_aborted.return_qid) {
      // We ECU this TMStatus by sending CancelQuery to all remaining participants.
      // Then, we send the QueryAborted back to the orig_p.
      for (rm_path, rm_result) in tm_status.tm_state {
        if rm_result.is_none() && rm_path != query_aborted.responder_path.node_path {
          // We avoid sending CancelQuery for the RM that just responded.
          self.ctx().send_to_node(
            rm_path.to_node_id(),
            CommonQuery::CancelQuery(msg::CancelQuery {
              query_id: tm_status.child_query_id.clone(),
            }),
          );
        }
      }

      // Finally, we propagate up the AbortData to the ES that owns this TMStatus
      self.handle_tm_aborted(statuses, tm_status.orig_p, query_aborted.payload);
    }
  }

  /// Handles a TMStatus aborting. If the originator was a GRQueryES, we can simply forward the
  /// `aborted_data` to that. Otherwise, if it was the MSCoordES, then either the MSQuery
  /// has failed, either requiring an response to be sent or for the MSQuery to be retried.
  fn handle_tm_aborted(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    aborted_data: msg::AbortedData,
  ) {
    // Finally, we propagate up the AbortData to the GRQueryES that owns this TMStatus
    let query_id = orig_p.query_id;
    if let Some(gr_query) = statuses.gr_query_ess.get_mut(&query_id) {
      let action = gr_query.es.handle_tm_aborted(&mut self.ctx(), aborted_data);
      self.handle_gr_query_es_action(statuses, query_id, action);
    } else if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
      let action = ms_coord.es.handle_tm_aborted(self, aborted_data);
      self.handle_ms_coord_es_action(statuses, query_id, action);
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
    let trans_read = statuses.trans_table_read_ess.get_mut(&query_id).unwrap();
    remove_item(&mut trans_read.child_queries, &subquery_id);
    let prefix = trans_read.es.location_prefix();
    let action = if let Some(gr_query) = statuses.gr_query_ess.get(&prefix.query_id) {
      trans_read.es.handle_subquery_done(
        &mut self.ctx(),
        &gr_query.es,
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      )
    } else if let Some(ms_coord) = statuses.ms_coord_ess.get(&prefix.query_id) {
      trans_read.es.handle_subquery_done(
        &mut self.ctx(),
        ms_coord.es.to_exec(),
        subquery_id,
        subquery_new_rms,
        (table_schema, table_views),
      )
    } else {
      trans_read.es.handle_internal_query_error(&mut self.ctx(), msg::QueryError::LateralError)
    };
    self.handle_trans_read_es_action(statuses, query_id, action);
  }

  // TODO: I feel like we should be able to pull out the TransTableSource lookup
  // into a function that takes in a generic lambda where the trait bound is defined
  // in the lambda itself. The function would assume the `query_id` points to a valid
  // TransTableRead, so it may take in the whole `statuses`.

  /// Handles a `query_error` propagated up from a GRQueryES. Recall that the originator
  /// can only be a TransTableReadES and it must exist (since the GRQueryES had existed).
  fn handle_internal_query_error(
    &mut self,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    query_error: msg::QueryError,
  ) {
    let query_id = orig_p.query_id;
    let trans_read = statuses.trans_table_read_ess.get_mut(&query_id).unwrap();
    remove_item(&mut trans_read.child_queries, &subquery_id);
    let action = trans_read.es.handle_internal_query_error(&mut self.ctx(), query_error);
    self.handle_trans_read_es_action(statuses, query_id, action);
  }

  /// Handles the actions specified by a GRQueryES.
  fn handle_ms_coord_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: MSQueryCoordAction,
  ) {
    match action {
      MSQueryCoordAction::Wait => {}
      MSQueryCoordAction::ExecuteTMStatus(tm_status) => {
        let ms_coord = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
        ms_coord.child_queries.push(tm_status.query_id.clone());
        statuses.tm_statuss.insert(tm_status.query_id.clone(), tm_status);
      }
      MSQueryCoordAction::Success(result) => {
        // Send back a success to the External, and ECU the MSCoordES.
        let ms_coord = statuses.ms_coord_ess.get(&query_id).unwrap();
        let timestamp = match &ms_coord.es {
          FullMSCoordES::QueryReplanning(es) => es.timestamp.clone(),
          FullMSCoordES::Executing(es) => es.timestamp.clone(),
        };
        self.network_output.send(
          &ms_coord.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(
            msg::ExternalQuerySuccess {
              request_id: ms_coord.request_id.clone(),
              timestamp,
              result,
            },
          )),
        );
        self.exit_and_clean_up(statuses, query_id);
      }
      MSQueryCoordAction::FatalFailure(payload) => {
        let ms_coord = statuses.ms_coord_ess.get(&query_id).unwrap();
        self.network_output.send(
          &ms_coord.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(
            msg::ExternalQueryAborted { request_id: ms_coord.request_id.clone(), payload },
          )),
        );
        self.exit_and_clean_up(statuses, query_id);
      }
      MSQueryCoordAction::NonFatalFailure => {
        // First ECU the MSCoordES without removing it from `statuses`.
        let ms_coord = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
        ms_coord.es.exit_and_clean_up(self);
        let child_queries = ms_coord.child_queries.clone();
        self.exit_all(statuses, child_queries);

        // Construct a new MSCoordES using a Timestamp that's strictly greater than before.
        let ms_coord = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
        let query_id = mk_qid(&mut self.rand);
        let new_timestamp = self.clock.now();
        ms_coord.es = FullMSCoordES::QueryReplanning(MSQueryCoordReplanningES {
          timestamp: new_timestamp,
          sql_query: ms_coord.es.to_exec().sql_query.clone(),
          query_id: query_id.clone(),
          state: MSQueryCoordReplanningS::Start,
        });

        // Update the QueryId that's stored in the request map.
        *self.external_request_id_map.get_mut(&ms_coord.request_id).unwrap() = query_id.clone();

        // Start executing the new MSCoordES.
        let action = ms_coord.es.start(self);
        self.handle_ms_coord_es_action(statuses, query_id, action);
      }
    }
  }

  /// Handles the actions specified by a TransTableReadES.
  fn handle_trans_read_es_action(
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
          let gr_query = GRQueryESWrapper { child_queries: vec![], es: gr_query_es };
          statuses.gr_query_ess.insert(subquery_id.clone(), gr_query);
          subquery_ids.push(subquery_id);
        }

        // Drive GRQueries
        for query_id in subquery_ids {
          if let Some(gr_query) = statuses.gr_query_ess.get_mut(&query_id) {
            // Generally, we use an `if` guard in case one child Query aborts the parent and
            // thus all other children. (This won't happen for GRQueryESs, though)
            let action = gr_query.es.start::<T>(&mut self.ctx());
            self.handle_gr_query_es_action(statuses, query_id, action);
          }
        }
      }
      TransTableAction::Success(success) => {
        // Remove the TableReadESWrapper and respond.
        let trans_read = statuses.trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = trans_read.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QuerySuccess(msg::QuerySuccess {
            return_qid: sender_path.query_id,
            responder_path,
            result: success.result,
            new_rms: success.new_rms,
          }),
        )
      }
      TransTableAction::QueryError(query_error) => {
        // Remove the TableReadESWrapper, abort subqueries, and respond.
        let trans_read = statuses.trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = trans_read.sender_path;
        let responder_path = self.mk_query_path(query_id);
        self.ctx().send_to_path(
          sender_path.clone(),
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        );
        self.exit_all(statuses, trans_read.child_queries)
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
        let gr_query = statuses.gr_query_ess.get_mut(&query_id).unwrap();
        gr_query.child_queries.push(tm_status.query_id.clone());
        statuses.tm_statuss.insert(tm_status.query_id.clone(), tm_status);
      }
      GRQueryAction::Success(res) => {
        let gr_query = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_gr_query_done(
          statuses,
          gr_query.es.orig_p,
          gr_query.es.query_id,
          res.new_rms,
          (res.schema, res.result),
        );
      }
      GRQueryAction::QueryError(query_error) => {
        let gr_query = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_internal_query_error(
          statuses,
          gr_query.es.orig_p,
          gr_query.es.query_id,
          query_error,
        );
      }
    }
  }

  // Handle a RegisterQuery sent by an MSQuery to an MSCoordES.
  fn handle_register_query(&mut self, statuses: &mut Statuses, register: msg::RegisterQuery) {
    if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&register.root_query_id) {
      ms_coord.es.handle_register_query(register);
    } else {
      // Otherwise, the MSCoordES no longer exists, and we should
      // cancel the MSQueryES immediately.
      let query_path = register.query_path;
      self.ctx().core_ctx().send_to_tablet(
        query_path.node_path.maybe_tablet_group_id.clone().unwrap(),
        msg::TabletMessage::CancelQuery(msg::CancelQuery { query_id: query_path.query_id.clone() }),
      );
    }
  }

  /// Run `exit_and_clean_up` for all QueryIds in `query_ids`.
  fn exit_all(&mut self, statuses: &mut Statuses, query_ids: Vec<QueryId>) {
    for query_id in query_ids {
      self.exit_and_clean_up(statuses, query_id);
    }
  }

  /// This function is used to initiate an Exit and Clean Up of ESs. This is needed to handle
  /// CancelQuery's, as well as when one ES wants to Exit and Clean Up another ES. Note that
  /// we allow the ES at `query_id` to be in any state, and to not even exist.
  fn exit_and_clean_up(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    if let Some(mut ms_coord) = statuses.ms_coord_ess.remove(&query_id) {
      // MSCoordES
      self.external_request_id_map.remove(&ms_coord.request_id);
      ms_coord.es.exit_and_clean_up(self);
      self.exit_all(statuses, ms_coord.child_queries);
    } else if let Some(mut gr_query) = statuses.gr_query_ess.remove(&query_id) {
      // GRQueryES
      gr_query.es.exit_and_clean_up(&mut self.ctx());
      self.exit_all(statuses, gr_query.child_queries);
    } else if let Some(mut trans_read) = statuses.trans_table_read_ess.remove(&query_id) {
      // TransTableReadES
      trans_read.es.exit_and_clean_up(&mut self.ctx());
      self.exit_all(statuses, trans_read.child_queries);
    } else if let Some(tm_status) = statuses.tm_statuss.remove(&query_id) {
      // We ECU this TMStatus by sending CancelQuery to all remaining participants.
      for (rm_path, rm_result) in tm_status.tm_state {
        if rm_result.is_none() {
          self.ctx().send_to_node(
            rm_path.to_node_id(),
            CommonQuery::CancelQuery(msg::CancelQuery {
              query_id: tm_status.child_query_id.clone(),
            }),
          );
        }
      }
    }
  }

  /// Construct QueryPath for a given `query_id` that belongs to this Slave.
  pub fn mk_query_path(&self, query_id: QueryId) -> QueryPath {
    QueryPath {
      node_path: NodePath {
        slave_group_id: self.this_slave_group_id.clone(),
        maybe_tablet_group_id: None,
      },
      query_id,
    }
  }
}
