use crate::common::{
  cur_timestamp, map_insert, mk_qid, mk_t, remove_item, update_leader_map,
  update_leader_map_unversioned, BasicIOCtx, GeneralTraceMessage, GossipData, LeaderMap, OrigP,
  Timestamp, VersionedValue,
};
use crate::common::{
  CNodePath, CQueryPath, CSubNodePath, CTSubNodePath, ColName, CoordGroupId, Gen, LeadershipId,
  PaxosGroupId, PaxosGroupIdTrait, SlaveGroupId, TNodePath, TQueryPath, TableView,
};
use crate::common::{CoreIOCtx, RemoteLeaderChangedPLm};
use crate::common::{EndpointId, QueryId, RequestId};
use crate::finish_query_tm_es::{
  FinishQueryPayloadTypes, FinishQueryPrepare, FinishQueryTMES, FinishQueryTMInner, ResponseData,
};
use crate::gr_query_es::{GRQueryAction, GRQueryES};
use crate::master_query_planning_es::StaticDBSchemaView;
use crate::message as msg;
use crate::ms_query_coord_es::{
  FullMSCoordES, MSQueryCoordAction, QueryPlanningES, QueryPlanningS,
};
use crate::paxos2pc_tm as paxos2pc;
use crate::paxos2pc_tm::{Paxos2PCTMAction, TMMessage};
use crate::server::{CTServerContext, CommonQuery, ServerContextBase};
use crate::sql_ast::iast;
use crate::sql_ast::proc;
use crate::sql_parser::convert_ast;
use crate::tablet::TPESAction;
use crate::tablet::{GRQueryESWrapper, TransTableReadESWrapper};
use crate::tm_status::TMStatus;
use crate::trans_table_read_es::{TransExecutionS, TransTableReadES};
use rand::RngCore;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::rc::Rc;
use std::sync::Arc;

#[path = "test/coord_test.rs"]
pub mod coord_test;

// -----------------------------------------------------------------------------------------------
//  CoordForwardMsg
// -----------------------------------------------------------------------------------------------

pub enum CoordForwardMsg {
  ExternalMessage(msg::SlaveExternalReq),
  CoordMessage(msg::CoordMessage),
  GossipData(Arc<GossipData>, LeaderMap),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
  LeaderChanged(msg::LeaderChanged),
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
  // Paxos2PC
  finish_query_tm_ess: BTreeMap<QueryId, FinishQueryTMES>,

  // TP
  ms_coord_ess: BTreeMap<QueryId, MSCoordESWrapper>,
  gr_query_ess: BTreeMap<QueryId, GRQueryESWrapper>,
  trans_table_read_ess: BTreeMap<QueryId, TransTableReadESWrapper>,
  tm_statuss: BTreeMap<QueryId, TMStatus>,
}

// -----------------------------------------------------------------------------------------------
//  TMServerContext
// -----------------------------------------------------------------------------------------------

impl paxos2pc::TMServerContext<FinishQueryPayloadTypes> for CoordContext {
  fn send_to_rm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    rm: &TNodePath,
    msg: msg::TabletMessage,
  ) {
    self.send_to_t(io_ctx, rm.clone(), msg);
  }

  fn mk_node_path(&self) -> CNodePath {
    self.mk_this_node_path()
  }

  fn is_leader(&self) -> bool {
    CoordContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  CoordConfig
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CoordConfig {
  /// This is used for generate the `suffix` of a Timestamp, where we just generate
  /// a random `u64` and take the remainder after dividing by `timestamp_suffix_divisor`.
  /// This cannot be 0; the default value is 1, making the suffix always be 0.
  pub timestamp_suffix_divisor: u64,
}

// -----------------------------------------------------------------------------------------------
//  Server Context
// -----------------------------------------------------------------------------------------------

impl ServerContextBase for CoordContext {
  fn leader_map(&self) -> &LeaderMap {
    &self.leader_map
  }

  fn this_gid(&self) -> &PaxosGroupId {
    &self.this_gid
  }

  fn this_eid(&self) -> &EndpointId {
    &self.this_eid
  }
}

impl CTServerContext for CoordContext {
  fn this_sid(&self) -> &SlaveGroupId {
    &self.this_sid
  }

  fn sub_node_path(&self) -> &CTSubNodePath {
    &self.sub_node_path
  }

  fn gossip(&self) -> &Arc<GossipData> {
    &self.gossip
  }
}

// -----------------------------------------------------------------------------------------------
//  Coord State
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct CoordState {
  pub ctx: CoordContext,
  statuses: Statuses,
}

/// The CoordState that holds all the state of the Coord
#[derive(Debug)]
pub struct CoordContext {
  /// Metadata
  pub coord_config: CoordConfig, // Config
  pub this_sid: SlaveGroupId,
  pub this_gid: PaxosGroupId,
  pub this_cid: CoordGroupId,
  pub sub_node_path: CTSubNodePath, // Wraps `this_cid` for expedience
  pub this_eid: EndpointId,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// LeaderMap. See `LeaderMap` docs.
  pub leader_map: LeaderMap,

  /// This is used to allow the user to cancel requests. There is an element (`RequestId`,
  /// `QueryId`) here exactly when there is an `MSCoordES` or `FinishQueryES` stored at that
  /// `QueryId` in `statuses` (recall that the former becomes the latter). In addition, ES
  /// will hold the `RequestId` in its `request_id` field.
  pub external_request_id_map: BTreeMap<RequestId, QueryId>,
}

impl CoordState {
  pub fn new(ctx: CoordContext) -> CoordState {
    CoordState { ctx, statuses: Default::default() }
  }

  pub fn handle_input<IO: CoreIOCtx>(&mut self, io_ctx: &mut IO, coord_input: CoordForwardMsg) {
    self.ctx.handle_input(io_ctx, &mut self.statuses, coord_input);
  }
}

impl CoordContext {
  /// This is used to first create a Slave node. The `gossip` and `leader_map` are
  /// exactly what would come in a `CreateSlaveGroup`; i.e. they do not contain
  /// `paxos_nodes`.
  pub fn new(
    this_sid: SlaveGroupId,
    this_cid: CoordGroupId,
    gossip: Arc<GossipData>,
    mut leader_map: LeaderMap,
    paxos_nodes: Vec<EndpointId>,
    this_eid: EndpointId,
    coord_config: CoordConfig,
  ) -> CoordContext {
    // Amend the LeaderMap to include this new Slave.
    let lid = LeadershipId::mk_first(paxos_nodes.get(0).unwrap().clone());
    leader_map.insert(this_sid.to_gid(), lid);

    // Recall that `gossip` does not contain `paxos_nodes`.
    let mut all_eids = BTreeSet::<EndpointId>::new();
    for (_, eids) in gossip.get().slave_address_config {
      all_eids.extend(eids.clone());
    }
    all_eids.extend(gossip.get().master_address_config.clone());
    all_eids.extend(paxos_nodes.clone());

    CoordContext {
      coord_config,
      this_sid: this_sid.clone(),
      this_gid: this_sid.to_gid(),
      this_cid: this_cid.clone(),
      sub_node_path: CTSubNodePath::Coord(this_cid),
      this_eid,
      gossip,
      leader_map,
      external_request_id_map: Default::default(),
    }
  }

  pub fn handle_input<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    coord_input: CoordForwardMsg,
  ) {
    match coord_input {
      CoordForwardMsg::ExternalMessage(message) => {
        match message {
          msg::SlaveExternalReq::PerformExternalQuery(external_query) => {
            match self.init_request(&external_query) {
              Ok(query) => {
                let query_id = mk_qid(io_ctx.rand());
                let request_id = &external_query.request_id;

                // Update the `external_request_id_map` and trace it.
                self.external_request_id_map.insert(request_id.clone(), query_id.clone());
                io_ctx.general_trace(GeneralTraceMessage::RequestIdQueryId(
                  request_id.clone(),
                  query_id.clone(),
                ));

                let ms_coord = map_insert(
                  &mut statuses.ms_coord_ess,
                  &query_id,
                  MSCoordESWrapper {
                    request_id: external_query.request_id,
                    sender_eid: external_query.sender_eid,
                    child_queries: vec![],
                    es: FullMSCoordES::QueryPlanning(QueryPlanningES {
                      timestamp: cur_timestamp(io_ctx, self.coord_config.timestamp_suffix_divisor),
                      sql_query: query,
                      query_id: query_id.clone(),
                      state: QueryPlanningS::Start,
                    }),
                  },
                );
                let action = ms_coord.es.start(self, io_ctx, false);
                self.handle_ms_coord_es_action(io_ctx, statuses, query_id, action);
              }
              Err(payload) => io_ctx.send(
                &external_query.sender_eid,
                msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(
                  msg::ExternalQueryAborted { request_id: external_query.request_id, payload },
                )),
              ),
            }
          }
          msg::SlaveExternalReq::CancelExternalQuery(cancel) => {
            // Recall that Cancellation is merely a passive hint on how the execution of the
            // original query should go. The Coord have no obligation to respond to it.
            if let Some(query_id) = self.external_request_id_map.get(&cancel.request_id) {
              if statuses.ms_coord_ess.contains_key(query_id) {
                // Recall that a request is only cancellable while it is an MSCoordES.
                self.exit_and_clean_up(io_ctx, statuses, query_id.clone());

                // Send the payload back to the client.
                io_ctx.send(
                  &cancel.sender_eid,
                  msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(
                    msg::ExternalQueryAborted {
                      request_id: cancel.request_id,
                      payload: msg::ExternalAbortedData::CancelConfirmed,
                    },
                  )),
                );
              }
            }
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
                if let Some(ms_coord) =
                  statuses.ms_coord_ess.get(&query.location_prefix.source.query_id)
                {
                  let es = ms_coord.es.to_exec();
                  // Construct and start the TransQueryPlanningES
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
                        child_queries: vec![],
                        timestamp: es.timestamp.clone(),
                      },
                    },
                  );

                  let action = trans_table.es.start(self, io_ctx, es);
                  self.handle_trans_read_es_action(
                    io_ctx,
                    statuses,
                    perform_query.query_id,
                    action,
                  );
                } else if let Some(gr_query) =
                  statuses.gr_query_ess.get(&query.location_prefix.source.query_id)
                {
                  // Construct and start the TransQueryPlanningES
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
                        child_queries: vec![],
                        timestamp: gr_query.es.timestamp.clone(),
                      },
                    },
                  );

                  let action = trans_table.es.start(self, io_ctx, &gr_query.es);
                  self.handle_trans_read_es_action(
                    io_ctx,
                    statuses,
                    perform_query.query_id,
                    action,
                  );
                } else {
                  // This means that the target GRQueryES was deleted. We can send back an
                  // Abort with LateralError. Exit and Clean Up will be done later.
                  self.send_query_error(
                    io_ctx,
                    perform_query.sender_path,
                    perform_query.query_id,
                    msg::QueryError::LateralError,
                  );
                  return;
                }
              }
              msg::GeneralQuery::SuperSimpleTableSelectQuery(_) => panic!(),
              msg::GeneralQuery::UpdateQuery(_) => panic!(),
              msg::GeneralQuery::InsertQuery(_) => panic!(),
              msg::GeneralQuery::DeleteQuery(_) => panic!(),
            }
          }
          msg::CoordMessage::CancelQuery(cancel_query) => {
            self.exit_and_clean_up(io_ctx, statuses, cancel_query.query_id);
          }
          msg::CoordMessage::QuerySuccess(query_success) => {
            self.handle_query_success(io_ctx, statuses, query_success);
          }
          msg::CoordMessage::QueryAborted(query_aborted) => {
            self.handle_query_aborted(io_ctx, statuses, query_aborted);
          }
          msg::CoordMessage::FinishQuery(message) => {
            let (query_id, action) =
              paxos2pc::handle_tm_msg(self, io_ctx, &mut statuses.finish_query_tm_ess, message);
            self.handle_finish_query_es_action(io_ctx, statuses, query_id, action);
          }
          msg::CoordMessage::MasterQueryPlanningSuccess(success) => {
            // Route the response to the appropriate MSCoordES.
            let query_id = success.return_qid;
            if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
              let action =
                ms_coord.es.handle_master_response(self, io_ctx, success.query_id, success.result);
              self.handle_ms_coord_es_action(io_ctx, statuses, query_id, action);
            }
          }
          msg::CoordMessage::RegisterQuery(register) => {
            self.handle_register_query(io_ctx, statuses, register)
          }
        }
      }
      CoordForwardMsg::GossipData(gossip, some_leader_map) => {
        // We only accept new Gossips where the generation increases.
        if self.gossip.get_gen() < gossip.get_gen() {
          // Amend the local LeaderMap to refect the new GossipData.
          update_leader_map_unversioned(
            &mut self.leader_map,
            self.gossip.as_ref(),
            &some_leader_map,
            gossip.as_ref(),
          );

          // Update Gossip
          self.gossip = gossip;

          // Inform Top-Level ESs.

          // TableReadES
          let query_ids: Vec<QueryId> = statuses.ms_coord_ess.keys().cloned().collect();
          for query_id in query_ids {
            let ms_coord = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
            let action = ms_coord.es.gossip_data_changed(self, io_ctx);
            self.handle_ms_coord_es_action(io_ctx, statuses, query_id, action);
          }

          // TransTableReadES
          let query_ids: Vec<QueryId> = statuses.trans_table_read_ess.keys().cloned().collect();
          for query_id in query_ids {
            let trans_read = statuses.trans_table_read_ess.get_mut(&query_id).unwrap();
            let prefix = trans_read.es.location_prefix();
            let action = if let Some(gr_query) = statuses.gr_query_ess.get(&prefix.source.query_id)
            {
              trans_read.es.gossip_data_changed(self, io_ctx, &gr_query.es)
            } else if let Some(ms_coord) = statuses.ms_coord_ess.get(&prefix.source.query_id) {
              trans_read.es.gossip_data_changed(self, io_ctx, ms_coord.es.to_exec())
            } else {
              trans_read.es.handle_internal_query_error(self, io_ctx, msg::QueryError::LateralError)
            };
            self.handle_trans_read_es_action(io_ctx, statuses, query_id, action);
          }
        }
      }
      CoordForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();

        // Only accept a RemoteLeaderChanged if the PaxosGroupId is already known.
        if let Some(cur_lid) = self.leader_map.get(&gid) {
          // Only update the LeadershipId if the new one increases the old one.
          if lid.gen > cur_lid.gen {
            self.leader_map.insert(gid.clone(), lid.clone());

            // For Top-Level ESs, if the sending PaxosGroup's Leadership changed, we ECU (no response).
            let query_ids: Vec<QueryId> = statuses.trans_table_read_ess.keys().cloned().collect();
            for query_id in query_ids {
              let trans_read = statuses.trans_table_read_ess.get_mut(&query_id).unwrap();
              if trans_read.sender_gid() == gid {
                self.exit_and_clean_up(io_ctx, statuses, query_id);
              }
            }

            let query_ids: Vec<QueryId> = statuses.ms_coord_ess.keys().cloned().collect();
            for query_id in query_ids {
              let ms_coord = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
              let action =
                ms_coord.es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
              self.handle_ms_coord_es_action(io_ctx, statuses, query_id, action);
            }

            let query_ids: Vec<QueryId> = statuses.finish_query_tm_ess.keys().cloned().collect();
            for query_id in query_ids {
              let finish_query_es = statuses.finish_query_tm_ess.get_mut(&query_id).unwrap();
              let action =
                finish_query_es.remote_leader_changed(self, io_ctx, remote_leader_changed.clone());
              self.handle_finish_query_es_action(io_ctx, statuses, query_id, action);
            }

            // Inform TMStatus
            if let PaxosGroupId::Slave(sid) = gid {
              let query_ids: Vec<QueryId> = statuses.tm_statuss.keys().cloned().collect();
              for query_id in query_ids {
                let tm_status = statuses.tm_statuss.get_mut(&query_id).unwrap();
                if let Some(cur_lid) = tm_status.leaderships.get(&sid) {
                  if cur_lid < &lid {
                    // The new Leadership of a remote slave has changed beyond what the TMStatus
                    // had contacted, so that RM will surely not respond. Thus we abort this whole
                    // TMStatus and inform the MSCoordES/GRQueryES so that it can retry the stage.
                    let orig_qid = tm_status.orig_p.query_id.clone();
                    self.exit_and_clean_up(io_ctx, statuses, query_id.clone());

                    // Inform the GRQueryES
                    if let Some(gr_query) = statuses.gr_query_ess.get_mut(&orig_qid) {
                      remove_item(&mut gr_query.child_queries, &query_id);
                      let action = gr_query.es.handle_tm_remote_leadership_changed(self, io_ctx);
                      self.handle_gr_query_es_action(io_ctx, statuses, orig_qid, action);
                    }
                    // Inform the MSCoordES
                    else if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&orig_qid) {
                      remove_item(&mut ms_coord.child_queries, &query_id);
                      let action = ms_coord.es.handle_tm_remote_leadership_changed(self, io_ctx);
                      self.handle_ms_coord_es_action(io_ctx, statuses, orig_qid, action);
                    }
                  }
                }
              }
            }
          }
        }
      }
      CoordForwardMsg::LeaderChanged(leader_changed) => {
        let this_gid = self.this_sid.to_gid();
        self.leader_map.insert(this_gid, leader_changed.lid);

        // Check if this node just lost Leadership
        if !self.is_leader() {
          // Wink away all RequestIds
          self.external_request_id_map.clear();

          // Wink away all TM ESs.
          statuses.ms_coord_ess.clear();
          statuses.finish_query_tm_ess.clear();

          // Wink away all Coord TP ESs.
          statuses.gr_query_ess.clear();
          statuses.trans_table_read_ess.clear();
          statuses.tm_statuss.clear();
        }
      }
    }
  }

  /// Does some initial validations and MSQuery processing before we start
  /// servicing the request.
  fn init_request(
    &self,
    external_query: &msg::PerformExternalQuery,
  ) -> Result<iast::Query, msg::ExternalAbortedData> {
    if self.external_request_id_map.contains_key(&external_query.request_id) {
      // Duplicate RequestId; respond with an abort.
      Err(msg::ExternalAbortedData::NonUniqueRequestId)
    } else {
      // Parse the SQL
      match Parser::parse_sql(&GenericDialect {}, &external_query.query) {
        Ok(parsed_ast) => {
          // Convert to MSQuery
          match convert_ast(parsed_ast) {
            Ok(internal_ast) => Ok(internal_ast),
            Err(parse_error) => Err(msg::ExternalAbortedData::ParseError(parse_error)),
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

  /// Called when one of the child queries in the current Stages respond successfully.
  /// This accumulates the results and sends the result to the MSCoordES when done.
  fn handle_query_success<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_success: msg::QuerySuccess,
  ) {
    let tm_query_id = query_success.return_qid.clone();
    if let Some(tm_status) = statuses.tm_statuss.get_mut(&tm_query_id) {
      tm_status.handle_query_success(query_success);
      if tm_status.is_complete() {
        // Remove the TMStatus and take ownership before forwarding the results back.
        let tm_status = statuses.tm_statuss.remove(&tm_query_id).unwrap();
        let (orig_p, results, new_rms) = tm_status.get_results();
        self.handle_tm_done(io_ctx, statuses, orig_p, tm_query_id, new_rms, results);
      }
    }
  }

  // Routes the result of a TMStatus when it's done.
  fn handle_tm_done<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
    tm_qid: QueryId,
    new_rms: BTreeSet<TQueryPath>,
    results: Vec<(Vec<Option<ColName>>, Vec<TableView>)>,
  ) {
    let query_id = orig_p.query_id;
    // Route TM results to MSQueryES
    if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
      remove_item(&mut ms_coord.child_queries, &tm_qid);
      let action = ms_coord.es.handle_tm_success(self, io_ctx, tm_qid, new_rms, results);
      self.handle_ms_coord_es_action(io_ctx, statuses, query_id, action);
    }
    // Route TM results to GRQueryES
    else if let Some(gr_query) = statuses.gr_query_ess.get_mut(&query_id) {
      remove_item(&mut gr_query.child_queries, &tm_qid);
      let action = gr_query.es.handle_tm_success(self, io_ctx, tm_qid, new_rms, results);
      self.handle_gr_query_es_action(io_ctx, statuses, query_id, action);
    }
  }

  fn handle_query_aborted<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_aborted: msg::QueryAborted,
  ) {
    let tm_query_id = &query_aborted.return_qid;
    if let Some(tm_status) = statuses.tm_statuss.get(tm_query_id) {
      // We ECU this TMStatus by sending CancelQuery to all remaining participants.
      // Then, we propagate the QueryAborted back to the orig_p.
      let orig_p = tm_status.orig_p.clone();
      self.exit_and_clean_up(io_ctx, statuses, tm_query_id.clone());

      // Finally, we propagate up the AbortData to the ES that owns this TMStatus
      self.handle_tm_aborted(io_ctx, statuses, orig_p, query_aborted.payload);
    }
  }

  /// Handles a TMStatus aborting. If the originator was a GRQueryES, we can simply forward the
  /// `aborted_data` to that. Otherwise, if it was the MSCoordES, then either the MSQuery
  /// has failed, either requiring an response to be sent or for the MSQuery to be retried.
  fn handle_tm_aborted<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
    aborted_data: msg::AbortedData,
  ) {
    let query_id = orig_p.query_id;
    // Route TM results to MSQueryES
    if let Some(gr_query) = statuses.gr_query_ess.get_mut(&query_id) {
      let action = gr_query.es.handle_tm_aborted(self, io_ctx, aborted_data);
      self.handle_gr_query_es_action(io_ctx, statuses, query_id, action);
    }
    // Route TM results to GRQueryES
    else if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&query_id) {
      let action = ms_coord.es.handle_tm_aborted(self, io_ctx, aborted_data);
      self.handle_ms_coord_es_action(io_ctx, statuses, query_id, action);
    }
  }

  /// Routes the GRQueryES results the appropriate TransTableES.
  fn handle_gr_query_done<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    result: (Vec<Option<ColName>>, Vec<TableView>),
  ) {
    let query_id = orig_p.query_id;
    let trans_read = statuses.trans_table_read_ess.get_mut(&query_id).unwrap();
    remove_item(&mut trans_read.child_queries, &subquery_id);
    let prefix = trans_read.es.location_prefix();
    let action = if let Some(gr_query) = statuses.gr_query_ess.get(&prefix.source.query_id) {
      trans_read.es.handle_subquery_done(
        self,
        io_ctx,
        &gr_query.es,
        subquery_id,
        subquery_new_rms,
        result,
      )
    } else if let Some(ms_coord) = statuses.ms_coord_ess.get(&prefix.source.query_id) {
      trans_read.es.handle_subquery_done(
        self,
        io_ctx,
        ms_coord.es.to_exec(),
        subquery_id,
        subquery_new_rms,
        result,
      )
    } else {
      trans_read.es.handle_internal_query_error(self, io_ctx, msg::QueryError::LateralError)
    };
    self.handle_trans_read_es_action(io_ctx, statuses, query_id, action);
  }

  /// Handles a `query_error` propagated up from a GRQueryES. Recall that the originator
  /// can only be a TransTableReadES and it must exist (since the GRQueryES had existed).
  fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    query_error: msg::QueryError,
  ) {
    let query_id = orig_p.query_id;
    let trans_read = statuses.trans_table_read_ess.get_mut(&query_id).unwrap();
    remove_item(&mut trans_read.child_queries, &subquery_id);
    let action = trans_read.es.handle_internal_query_error(self, io_ctx, query_error);
    self.handle_trans_read_es_action(io_ctx, statuses, query_id, action);
  }

  /// Adds the given `gr_query_ess` to `statuses`, executing them one at a time.
  fn launch_subqueries<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    gr_query_ess: Vec<GRQueryES>,
  ) {
    // Here, we have to add in the GRQueryESs and start them.
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
        let action = gr_query.es.start(self, io_ctx);
        self.handle_gr_query_es_action(io_ctx, statuses, query_id, action);
      }
    }
  }

  /// Handles the actions specified by a GRQueryES.
  fn handle_ms_coord_es_action<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
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
      MSQueryCoordAction::Success(all_rms, sql_query, result, timestamp) => {
        let ms_coord = statuses.ms_coord_ess.remove(&query_id).unwrap();

        if all_rms.is_empty() {
          // If there are no RMs, respond immediately.
          self.external_request_id_map.remove(&ms_coord.request_id);
          io_ctx.send(
            &ms_coord.sender_eid,
            msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(
              msg::ExternalQuerySuccess { request_id: ms_coord.request_id, timestamp, result },
            )),
          );
        } else {
          // Construct the Prepare messages
          let mut prepare_payloads = BTreeMap::<TNodePath, FinishQueryPrepare>::new();
          for rm in all_rms {
            prepare_payloads.insert(rm.node_path, FinishQueryPrepare { query_id: rm.query_id });
          }

          // Add in the FinishQueryES and start it
          let outer = FinishQueryTMES::start_orig(
            self,
            io_ctx,
            query_id.clone(),
            FinishQueryTMInner {
              response_data: Some(ResponseData {
                request_id: ms_coord.request_id,
                sender_eid: ms_coord.sender_eid,
                sql_query,
                result,
                timestamp,
              }),
              committed: false,
            },
            prepare_payloads,
          );
          statuses.finish_query_tm_ess.insert(query_id, outer);
        }
      }
      MSQueryCoordAction::FatalFailure(payload) => {
        let ms_coord = statuses.ms_coord_ess.get(&query_id).unwrap();
        io_ctx.send(
          &ms_coord.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalQueryAborted(
            msg::ExternalQueryAborted { request_id: ms_coord.request_id.clone(), payload },
          )),
        );
        self.exit_and_clean_up(io_ctx, statuses, query_id);
      }
      MSQueryCoordAction::NonFatalFailure(start_with_master_query_planning) => {
        // First ECU the MSCoordES without removing it from `statuses`.
        let ms_coord = statuses.ms_coord_ess.get_mut(&query_id).unwrap();
        let child_queries = ms_coord.child_queries.clone();
        self.exit_all(io_ctx, statuses, child_queries);

        // Construct a new MSCoordES using a Timestamp that is strictly greater than before.
        let mut ms_coord = statuses.ms_coord_ess.remove(&query_id).unwrap();
        let exec = ms_coord.es.to_exec();
        let query_id = mk_qid(io_ctx.rand());
        ms_coord.es = FullMSCoordES::QueryPlanning(QueryPlanningES {
          timestamp: max(
            cur_timestamp(io_ctx, self.coord_config.timestamp_suffix_divisor),
            exec.timestamp.add(mk_t(1)),
          ),
          sql_query: exec.orig_query.clone(),
          query_id: query_id.clone(),
          state: QueryPlanningS::Start,
        });
        let ms_coord = map_insert(&mut statuses.ms_coord_ess, &query_id, ms_coord);

        // Update the QueryId that's stored in the `external_request_id_map` and trace it.
        *self.external_request_id_map.get_mut(&ms_coord.request_id).unwrap() = query_id.clone();
        io_ctx.general_trace(GeneralTraceMessage::RequestIdQueryId(
          ms_coord.request_id.clone(),
          query_id.clone(),
        ));

        // Start executing the new MSCoordES.
        let action = ms_coord.es.start(self, io_ctx, start_with_master_query_planning);
        self.handle_ms_coord_es_action(io_ctx, statuses, query_id, action);
      }
    }
  }

  /// Handles the actions specified by a FinishQueryES.
  fn handle_finish_query_es_action<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: Paxos2PCTMAction,
  ) {
    match action {
      Paxos2PCTMAction::Wait => {}
      Paxos2PCTMAction::Exit => {
        let es = statuses.finish_query_tm_ess.remove(&query_id).unwrap();
        if es.inner.committed {
          // If the ES was successful, send back a success to the External.
          if let Some(response_data) = es.inner.response_data {
            self.external_request_id_map.remove(&response_data.request_id);
            io_ctx.send(
              &response_data.sender_eid,
              msg::NetworkMessage::External(msg::ExternalMessage::ExternalQuerySuccess(
                msg::ExternalQuerySuccess {
                  request_id: response_data.request_id,
                  timestamp: response_data.timestamp,
                  result: response_data.result,
                },
              )),
            );
          }
        } else {
          if let Some(response_data) = es.inner.response_data {
            // Otherwise, we should retry the request. We reconstruct a MSCoordESWrapper.
            let query_id = mk_qid(io_ctx.rand());
            let ms_coord = map_insert(
              &mut statuses.ms_coord_ess,
              &query_id,
              MSCoordESWrapper {
                request_id: response_data.request_id,
                sender_eid: response_data.sender_eid,
                child_queries: vec![],
                es: FullMSCoordES::QueryPlanning(QueryPlanningES {
                  timestamp: max(
                    cur_timestamp(io_ctx, self.coord_config.timestamp_suffix_divisor),
                    response_data.timestamp.add(mk_t(1)),
                  ),
                  sql_query: response_data.sql_query,
                  query_id: query_id.clone(),
                  state: QueryPlanningS::Start,
                }),
              },
            );

            // Update the QueryId that's stored in the `external_request_id_map` and trace it.
            *self.external_request_id_map.get_mut(&ms_coord.request_id).unwrap() = query_id.clone();
            io_ctx.general_trace(GeneralTraceMessage::RequestIdQueryId(
              ms_coord.request_id.clone(),
              query_id.clone(),
            ));

            // Start executing the new MSCoordES.
            let action = ms_coord.es.start(self, io_ctx, false);
            self.handle_ms_coord_es_action(io_ctx, statuses, query_id, action);
          }
        }
      }
    }
  }

  /// Handles the actions specified by a TransTableReadES.
  fn handle_trans_read_es_action<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: TPESAction,
  ) {
    match action {
      TPESAction::Wait => {}
      TPESAction::SendSubqueries(gr_query_ess) => {
        self.launch_subqueries(io_ctx, statuses, gr_query_ess);
      }
      TPESAction::Success(success) => {
        // Remove the TableReadESWrapper and respond.
        let trans_read = statuses.trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = trans_read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.send_to_ct(
          io_ctx,
          sender_path.node_path,
          CommonQuery::QuerySuccess(msg::QuerySuccess {
            return_qid: sender_path.query_id,
            responder_path,
            result: success.result,
            new_rms: success.new_rms,
          }),
        )
      }
      TPESAction::QueryError(query_error) => {
        // Remove the TableReadESWrapper, abort subqueries, and respond.
        let trans_read = statuses.trans_table_read_ess.remove(&query_id).unwrap();
        let sender_path = trans_read.sender_path;
        let responder_path = self.mk_query_path(query_id).into_ct();
        // This is the originating Leadership (see Scenario 4,"SenderPath LeaderMap Consistency").
        self.send_to_ct(
          io_ctx,
          sender_path.node_path,
          CommonQuery::QueryAborted(msg::QueryAborted {
            return_qid: sender_path.query_id,
            responder_path,
            payload: msg::AbortedData::QueryError(query_error.clone()),
          }),
        );
        self.exit_all(io_ctx, statuses, trans_read.child_queries)
      }
    }
  }

  /// Handles the actions specified by a GRQueryES.
  fn handle_gr_query_es_action<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
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
          io_ctx,
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
          io_ctx,
          statuses,
          gr_query.es.orig_p,
          gr_query.es.query_id,
          query_error,
        );
      }
    }
  }

  // Handle a RegisterQuery sent by an MSQuery to an MSCoordES.
  fn handle_register_query<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    register: msg::RegisterQuery,
  ) {
    if let Some(ms_coord) = statuses.ms_coord_ess.get_mut(&register.root_query_id) {
      ms_coord.es.handle_register_query(register);
    } else {
      // Otherwise, to guarantee that MSQueryESs always get cleaned up, we send a `CancelQuery`.
      //
      // Note that we do not need to check for the case that this MSQueryES will be cleaned up
      // up by a FinishQueryTMES. Even if the `CancelQuery` we send out deletes the MSQueryES,
      // Paxos2PC will gracefully abort. (But this will never happen, since we already sent out
      // the Prepare and network queues are FIFO.)
      let query_path = register.query_path;
      self.send_to_t(
        io_ctx,
        query_path.node_path,
        msg::TabletMessage::CancelQuery(msg::CancelQuery { query_id: query_path.query_id.clone() }),
      );
    }
  }

  /// Run `exit_and_clean_up` for all QueryIds in `query_ids`.
  fn exit_all<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_ids: Vec<QueryId>,
  ) {
    for query_id in query_ids {
      self.exit_and_clean_up(io_ctx, statuses, query_id);
    }
  }

  /// This function is used to initiate an Exit and Clean Up of ESs. This is needed to handle
  /// CancelQuery's, as well as when one ES wants to Exit and Clean Up another ES. Note that
  /// we allow the ES at `query_id` to be in any state, and to not even exist.
  fn exit_and_clean_up<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
  ) {
    // MSCoordES
    if let Some(mut ms_coord) = statuses.ms_coord_ess.remove(&query_id) {
      self.external_request_id_map.remove(&ms_coord.request_id);
      ms_coord.es.exit_and_clean_up(self, io_ctx);
      self.exit_all(io_ctx, statuses, ms_coord.child_queries);
    }
    // GRQueryES
    else if let Some(mut gr_query) = statuses.gr_query_ess.remove(&query_id) {
      gr_query.es.exit_and_clean_up(self);
      self.exit_all(io_ctx, statuses, gr_query.child_queries);
    }
    // TransTableReadES
    else if let Some(mut trans_read) = statuses.trans_table_read_ess.remove(&query_id) {
      trans_read.es.exit_and_clean_up(self, io_ctx);
      self.exit_all(io_ctx, statuses, trans_read.child_queries);
    }
    // TMStatus
    else if let Some(mut tm_status) = statuses.tm_statuss.remove(&query_id) {
      tm_status.exit_and_clean_up(self, io_ctx);
    }
  }

  /// Construct QueryPath for a given `query_id` that belongs to this Coord.
  pub fn mk_query_path(&self, query_id: QueryId) -> CQueryPath {
    CQueryPath { node_path: self.mk_this_node_path(), query_id }
  }

  pub fn mk_this_node_path(&self) -> CNodePath {
    CNodePath { sid: self.this_sid.clone(), sub: CSubNodePath::Coord(self.this_cid.clone()) }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&self.this_sid.to_gid()).unwrap();
    lid.eid == self.this_eid
  }
}
