use crate::common::{mk_qid, IOTypes, NetworkOut};
use crate::model::common::{EndpointId, QueryId, RequestId};
use crate::model::message::NetworkMessage::External;
use crate::model::message::{
  ExternalMessage, ExternalQueryAbort, NetworkMessage, QueryError, SlaveMessage,
};
use rand::RngCore;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use std::collections::HashMap;

/// A struct to track an ExternalQuery so that it can be replied to.
#[derive(Debug)]
struct ExternalQueryMetadata {
  request_id: RequestId,
  eid: EndpointId,
}

/// Handles creation and cancellation of External Queries
#[derive(Debug)]
struct ExternalQueryManager {
  /// External Request management
  external_query_map: HashMap<QueryId, ExternalQueryMetadata>,
  external_query_index: HashMap<RequestId, QueryId>, // This is used to cancel an External Request
}

impl ExternalQueryManager {
  fn new() -> ExternalQueryManager {
    ExternalQueryManager {
      external_query_map: HashMap::new(),
      external_query_index: HashMap::new(),
    }
  }

  fn add_request(&mut self, query_id: QueryId, from_eid: EndpointId, request_id: RequestId) {
    self.external_query_map.insert(
      query_id.clone(),
      ExternalQueryMetadata {
        request_id: request_id.clone(),
        eid: from_eid,
      },
    );
    self.external_query_index.insert(request_id, query_id);
  }

  fn remove_with_request_id(&mut self, request_id: &RequestId) -> Option<ExternalQueryMetadata> {
    if let Some(query_id) = self.external_query_index.remove(request_id) {
      Some(self.external_query_map.remove(&query_id).unwrap())
    } else {
      None
    }
  }

  fn remove_with_query_id(&mut self, query_id: &QueryId) -> Option<ExternalQueryMetadata> {
    if let Some(metadata) = self.external_query_map.remove(query_id) {
      self
        .external_query_index
        .remove(&metadata.request_id)
        .unwrap();
      Some(metadata)
    } else {
      None
    }
  }
}

/// The SlaveState that holds all the state of the Slave
#[derive(Debug)]
pub struct SlaveState<T: IOTypes> {
  /// IO Objects.
  rand: T::RngCoreT,
  network_output: T::NetworkOutT,
  tablet_forward_output: T::TabletForwardOutT,

  /// External Request management
  external_query_manager: ExternalQueryManager,
}

impl<T: IOTypes> SlaveState<T> {
  pub fn new(
    rand: T::RngCoreT,
    network_output: T::NetworkOutT,
    tablet_forward_output: T::TabletForwardOutT,
  ) -> SlaveState<T> {
    SlaveState {
      rand,
      network_output,
      tablet_forward_output,
      external_query_manager: ExternalQueryManager::new(),
    }
  }

  /// Normally, the sender's metadata would be buried in the message itself.
  /// However, we also have to handle client messages here, we need the EndpointId
  /// passed in explicitly.
  pub fn handle_incoming_message(&mut self, from_eid: EndpointId, msg: SlaveMessage) {
    match &msg {
      SlaveMessage::PerformExternalQuery(exteral_query) => {
        // Create and store a ExternalQueryMetadata to keep track of the new request
        let query_id = mk_qid(&mut self.rand);
        self.external_query_manager.add_request(
          query_id.clone(),
          from_eid.clone(),
          exteral_query.request_id.clone(),
        );

        // Parse
        let sql = &exteral_query.query;
        let dialect = GenericDialect {};
        match Parser::parse_sql(&dialect, sql) {
          Ok(ast) => {
            println!("AST: {:#?}", ast);
          }
          Err(e) => {
            // Extract error string
            let err_string = match e {
              TokenizerError(s) => s,
              ParserError(s) => s,
            };

            // Reply to the client with the error
            if let Some(metadata) = self.external_query_manager.remove_with_query_id(&query_id) {
              let response = ExternalQueryAbort {
                request_id: metadata.request_id.clone(),
                error: QueryError::ParseError(err_string),
              };
              self.network_output.send(
                &metadata.eid,
                NetworkMessage::External(ExternalMessage::ExternalQueryAbort(response)),
              );
            }
          }
        }
      }
      SlaveMessage::CancelExternalQuery(_) => panic!("support"),
    }
  }
}
