use crate::col_usage::{ColUsagePlanner, FrozenColUsageNode};
use crate::common::{
  lookup, mk_qid, GossipData, GossipDataSer, IOTypes, NetworkOut, TableSchema, TableSchemaSer,
};
use crate::model::common::proc::{AlterTable, TableRef};
use crate::model::common::{
  proc, ColName, EndpointId, Gen, QueryId, RequestId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::model::message::{
  ColUsageTree, ExternalDDLQueryAbortData, FrozenColUsageTree, MasterMessage,
};
use crate::server::{CommonQuery, CoreServerContext};
use crate::sql_parser::convert_ddl_ast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::{ParserError, TokenizerError};
use sqlparser::test_utils::table;
use std::cmp::max;
use std::collections::{HashMap, HashSet};

// -----------------------------------------------------------------------------------------------
//  AlterTableES
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct Executing {
  responded_count: usize,
  tm_state: HashMap<TabletGroupId, Option<Timestamp>>,
}

#[derive(Debug)]
pub enum AlterTableS {
  Start,
  Executing(Executing),
}

#[derive(Debug)]
pub struct AlterTableES {
  // Metadata copied from outside.
  request_id: RequestId,
  sender_eid: EndpointId,

  // Core ES data
  query_id: QueryId,
  table_path: TablePath,
  alter_op: proc::AlterOp,

  // State
  state: AlterTableS,
}

#[derive(Debug, Default)]
pub struct Statuses {
  alter_table_ess: HashMap<QueryId, AlterTableES>,
}

// -----------------------------------------------------------------------------------------------
//  Master State
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct MasterState<T: IOTypes> {
  context: MasterContext<T>,
  statuses: Statuses,
}

impl<T: IOTypes> MasterState<T> {
  pub fn new(
    rand: T::RngCoreT,
    clock: T::ClockT,
    network_output: T::NetworkOutT,
    tablet_forward_output: T::TabletForwardOutT,
    db_schema: HashMap<TablePath, TableSchema>,
    sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
    tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
    slave_address_config: HashMap<SlaveGroupId, EndpointId>,
  ) -> MasterState<T> {
    MasterState {
      context: MasterContext {
        rand,
        clock,
        network_output,
        tablet_forward_output,
        gen: Gen(0),
        db_schema,
        sharding_config,
        tablet_address_config,
        slave_address_config,
        external_request_id_map: Default::default(),
      },
      statuses: Default::default(),
    }
  }

  pub fn handle_incoming_message(&mut self, message: msg::MasterMessage) {
    self.context.handle_incoming_message(&mut self.statuses, message);
  }
}

#[derive(Debug)]
pub struct MasterContext<T: IOTypes> {
  /// IO Objects.
  rand: T::RngCoreT,
  clock: T::ClockT,
  network_output: T::NetworkOutT,
  tablet_forward_output: T::TabletForwardOutT,

  /// Database Schema
  gen: Gen,
  db_schema: HashMap<TablePath, TableSchema>,

  /// Distribution
  sharding_config: HashMap<TablePath, Vec<(TabletKeyRange, TabletGroupId)>>,
  tablet_address_config: HashMap<TabletGroupId, SlaveGroupId>,
  slave_address_config: HashMap<SlaveGroupId, EndpointId>,

  /// Request Management
  external_request_id_map: HashMap<RequestId, QueryId>,
}

impl<T: IOTypes> MasterContext<T> {
  pub fn ctx(&mut self) -> CoreServerContext<T> {
    CoreServerContext {
      rand: &mut self.rand,
      clock: &mut self.clock,
      network_output: &mut self.network_output,
      sharding_config: &mut self.sharding_config,
      tablet_address_config: &mut self.tablet_address_config,
      slave_address_config: &mut self.slave_address_config,
    }
  }

  pub fn handle_incoming_message(&mut self, statuses: &mut Statuses, message: msg::MasterMessage) {
    match message {
      MasterMessage::PerformExternalDDLQuery(external_alter) => {
        match self.validate_ddl_query(&external_alter) {
          Ok(alter_table) => {
            let query_id = mk_qid(&mut self.rand);
            let request_id = external_alter.request_id;
            self.external_request_id_map.insert(request_id.clone(), query_id.clone());
            statuses.alter_table_ess.insert(
              query_id.clone(),
              AlterTableES {
                request_id,
                sender_eid: external_alter.sender_eid,
                query_id,
                table_path: alter_table.table_path,
                alter_op: alter_table.alter_op,
                state: AlterTableS::Start,
              },
            );
          }
          Err(payload) => {
            // We return an error because the RequestId is not unique.
            self.network_output.send(
              &external_alter.sender_eid,
              msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
                msg::ExternalDDLQueryAborted { request_id: external_alter.request_id, payload },
              )),
            );
          }
        }
      }
      MasterMessage::CancelExternalDDLQuery(cancel) => {
        if let Some(query_id) = self.external_request_id_map.get(&cancel.request_id) {
          self.exit_and_clean_up(statuses, query_id.clone())
        }
      }
      MasterMessage::AlterTablePrepared(prepared) => {
        self.handle_alter_table_prepared(statuses, prepared);
      }
      MasterMessage::AlterTableAborted(_) => {
        // The Tablets never send this.
        panic!()
      }
      MasterMessage::PerformMasterFrozenColUsage(request) => {
        // Construct the FrozenColUsageTree with the current database schema in the Master.
        let timestamp = request.timestamp.clone();
        let mut planner = ColUsagePlanner { gossiped_db_schema: &self.db_schema, timestamp };
        let frozen_col_usage_tree = match request.col_usage_tree {
          ColUsageTree::MSQuery(ms_query) => {
            msg::FrozenColUsageTree::ColUsageNodes(planner.plan_ms_query(&ms_query))
          }
          ColUsageTree::GRQuery(gr_query) => {
            let mut trans_table_schemas = request.trans_table_schemas;
            msg::FrozenColUsageTree::ColUsageNodes(
              planner.plan_gr_query(&mut trans_table_schemas, &gr_query),
            )
          }
          ColUsageTree::MSQueryStage(stage_query) => {
            let mut trans_table_schemas = request.trans_table_schemas;
            msg::FrozenColUsageTree::ColUsageNode(
              planner.plan_ms_query_stage(&mut trans_table_schemas, &stage_query),
            )
          }
        };

        // Freeze the `safe_present_cols` and `external_cols` used for every node in the
        // FrozenColUsageTree computed above.
        self.freeze_schema(&frozen_col_usage_tree, timestamp);

        // Send the response to the originator.
        let response = CommonQuery::MasterFrozenColUsageSuccess(msg::MasterFrozenColUsageSuccess {
          return_qid: request.sender_path.query_id.clone(),
          frozen_col_usage_tree,
          gossip: GossipDataSer::from_gossip(GossipData {
            gossip_gen: self.gen.clone(),
            gossiped_db_schema: self.db_schema.clone(),
          }),
        });
        self.ctx().send_to_path(request.sender_path, response);
      }
      MasterMessage::CancelMasterFrozenColUsage(_) => {}
    }

    self.run_main_loop(statuses);
  }

  /// Validate the uniqueness of `RequestId`, parse the SQL, and do minor
  /// validations on it before returning the parsed output.
  fn validate_ddl_query(
    &self,
    external_query: &msg::PerformExternalDDLQuery,
  ) -> Result<proc::AlterTable, msg::ExternalDDLQueryAbortData> {
    if self.external_request_id_map.contains_key(&external_query.request_id) {
      // Duplicate RequestId; respond with an abort.
      Err(msg::ExternalDDLQueryAbortData::NonUniqueRequestId)
    } else {
      // Parse the SQL
      match Parser::parse_sql(&GenericDialect {}, &external_query.query) {
        Ok(parsed_ast) => {
          let alter_table = convert_ddl_ast(&parsed_ast);
          // Do several more checks on `alter_table` before returning.
          if let Some(table_schema) = self.db_schema.get(&alter_table.table_path) {
            if lookup(&table_schema.key_cols, &alter_table.alter_op.col_name).is_none() {
              return Ok(alter_table);
            }
          }
          Err(msg::ExternalDDLQueryAbortData::InvalidAlterOp)
        }
        Err(parse_error) => {
          // Extract error string
          Err(msg::ExternalDDLQueryAbortData::ParseError(match parse_error {
            TokenizerError(err_msg) => err_msg,
            ParserError(err_msg) => err_msg,
          }))
        }
      }
    }
  }

  /// Runs the `run_main_loop_once` until it finally results in no states changes.
  fn run_main_loop(&mut self, statuses: &mut Statuses) {
    while self.run_main_loop_once(statuses) {}
  }

  /// Thus runs one iteration of the Main Loop, returning `false` exactly when nothing changes.
  fn run_main_loop_once(&mut self, statuses: &mut Statuses) -> bool {
    // First, figure out which (TablePath, ColName)s are used by `alter_table_ess`.
    let mut used_col_names = HashSet::<(&TablePath, &ColName)>::new();
    for (_, es) in &statuses.alter_table_ess {
      used_col_names.insert((&es.table_path, &es.alter_op.col_name));
    }

    // Then, see if there is a `pending_alter_table_requests` that doesn't use the above.
    for (query_id, es) in &statuses.alter_table_ess {
      if !used_col_names.contains(&(&es.table_path, &es.alter_op.col_name)) {
        // Remove the element from `pending_*`, construct an AlterTableES, and start it.
        self.start_alter_table(statuses, query_id.clone());
        return true;
      }
    }

    return false;
  }

  /// Here, we start the AlterTable, sending out the Prepare messages.
  fn start_alter_table(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    let es = statuses.alter_table_ess.get_mut(&query_id).unwrap();

    // First, we compute if the `col_name` is currently present in the TableSchema of `table_path`.
    let table_schema = self.db_schema.get(&es.table_path).unwrap();
    let maybe_col_type = table_schema.val_cols.get_last_version(&es.alter_op.col_name);

    // Next, we check if the current `alter_op` is Column Valid.
    if (maybe_col_type.is_none() && es.alter_op.maybe_col_type.is_none())
      || (maybe_col_type.is_some() && es.alter_op.maybe_col_type.is_some())
    {
      // This means the `alter_op` is not Column Valid. Thus, we can't service this request
      // so we Exit and Clean Up, responding to the External.
      self.network_output.send(
        &es.sender_eid,
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
          msg::ExternalDDLQueryAborted {
            request_id: es.request_id.clone(),
            payload: ExternalDDLQueryAbortData::InvalidAlterOp,
          },
        )),
      );
      self.exit_and_clean_up(statuses, query_id);
    } else {
      // Otherwise, we start the 2PC.

      // Okay, so what now? move `state`, construct tm_state, send off altertableprepred.
      let mut tm_state = HashMap::<TabletGroupId, Option<Timestamp>>::new();
      for (_, tid) in self.sharding_config.get(&es.table_path).unwrap() {
        tm_state.insert(tid.clone(), None);
      }

      // Send off AlterTablePrepare to the Tablets and move the `state` to Executing.
      for tid in tm_state.keys() {
        self.ctx().send_to_tablet(
          tid.clone(),
          msg::TabletMessage::AlterTablePrepare(msg::AlterTablePrepare {
            query_id: query_id.clone(),
            alter_op: es.alter_op.clone(),
          }),
        )
      }

      es.state = AlterTableS::Executing(Executing { responded_count: 0, tm_state });
    }
  }

  /// Handle `AlterTablePrepared`
  fn handle_alter_table_prepared(
    &mut self,
    statuses: &mut Statuses,
    prepared: msg::AlterTablePrepared,
  ) {
    if let Some(es) = statuses.alter_table_ess.get_mut(&prepared.query_id) {
      let executing = cast!(AlterTableS::Executing, &mut es.state).unwrap();
      let rm_state = executing.tm_state.get_mut(&prepared.tablet_group_id).unwrap();
      assert_eq!(rm_state, &None);
      *rm_state = Some(prepared.timestamp.clone());
      executing.responded_count += 1;

      // Check if all RMs have responded and finish AlterTableES if so.
      if executing.responded_count == executing.tm_state.len() {
        // Compute the final timestamp to apply the `alter_op` at.
        let table_schema = self.db_schema.get_mut(&es.table_path).unwrap();
        let mut new_timestamp = table_schema.val_cols.get_lat(&es.alter_op.col_name);
        for (_, rm_state) in &executing.tm_state {
          new_timestamp = max(new_timestamp, rm_state.unwrap());
        }
        new_timestamp.0 += 1; // We add 1, since we can't actually modify a value at a `lat`.

        // Apply the `alter_op`.
        self.gen.0 += 1;
        table_schema.val_cols.write(
          &es.alter_op.col_name,
          es.alter_op.maybe_col_type.clone(),
          new_timestamp,
        );

        // Send off AlterTableCommit to the Tablets.
        let gossip_data = GossipDataSer::from_gossip(GossipData {
          gossip_gen: self.gen.clone(),
          gossiped_db_schema: self.db_schema.clone(),
        });
        for tid in executing.tm_state.keys() {
          self.ctx().send_to_tablet(
            tid.clone(),
            msg::TabletMessage::AlterTableCommit(msg::AlterTableCommit {
              query_id: es.query_id.clone(),
              timestamp: new_timestamp,
              gossip_data: gossip_data.clone(),
            }),
          );
        }

        // Send off a success message to the External and ECU this ES.
        self.network_output.send(
          &es.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(
            msg::ExternalDDLQuerySuccess {
              request_id: es.request_id.clone(),
              timestamp: new_timestamp,
            },
          )),
        );
        let query_id = es.query_id.clone();
        self.exit_and_clean_up(statuses, query_id);
      }
    }
  }

  /// For each node in the `frozen_col_usage_tree`, we take the union of `safe_present_cols`
  /// and `external_col`, and then increase their lat to the `timestamp`.
  fn freeze_schema(&mut self, frozen_col_usage_tree: &FrozenColUsageTree, timestamp: Timestamp) {
    fn freeze_schema_r(
      db_schema: &mut HashMap<TablePath, TableSchema>,
      node: &FrozenColUsageNode,
      timestamp: Timestamp,
    ) {
      match &node.table_ref {
        TableRef::TablePath(table_path) => {
          let table_schema = db_schema.get_mut(table_path).unwrap();
          for col in &node.safe_present_cols {
            // Update the LAT for non-Key Columns.
            if lookup(&table_schema.key_cols, col).is_none() {
              table_schema.val_cols.update_lat(col, timestamp);
            }
          }
          for col in &node.external_cols {
            // Recall that no External Column should be a Key Column.
            assert!(lookup(&table_schema.key_cols, col).is_none());
            table_schema.val_cols.update_lat(col, timestamp);
          }
        }
        TableRef::TransTableName(_) => {}
      }

      // Recurse through the child nodes.
      for child in &node.children {
        for (_, (_, node)) in child {
          freeze_schema_r(db_schema, node, timestamp);
        }
      }
    }

    match frozen_col_usage_tree {
      FrozenColUsageTree::ColUsageNodes(nodes) => {
        for (_, (_, node)) in nodes {
          freeze_schema_r(&mut self.db_schema, node, timestamp);
        }
      }
      FrozenColUsageTree::ColUsageNode((_, node)) => {
        freeze_schema_r(&mut self.db_schema, node, timestamp);
      }
    }
  }

  /// Removes the `query_id` from the Master, cleaning up any remaining resources as well.
  fn exit_and_clean_up(&mut self, statuses: &mut Statuses, query_id: QueryId) {
    if let Some(es) = statuses.alter_table_ess.remove(&query_id) {
      self.external_request_id_map.remove(&es.request_id);
      match es.state {
        AlterTableS::Start => {}
        AlterTableS::Executing(executing) => {
          for (tid, _) in executing.tm_state {
            self.ctx().send_to_tablet(
              tid.clone(),
              msg::TabletMessage::AlterTableAbort(msg::AlterTableAbort {
                query_id: es.query_id.clone(),
              }),
            );
          }
        }
      }
    }
  }
}
