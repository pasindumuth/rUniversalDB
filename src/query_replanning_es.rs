use crate::col_usage::ColUsagePlanner;
use crate::common::{lookup_pos, mk_qid, IOTypes, NetworkOut, OrigP, QueryPlan};
use crate::model::common::{proc, ColName, Context, Gen, QueryId, QueryPath, TablePath, Timestamp};
use crate::model::message as msg;
use crate::server::{are_cols_locked, contains_col};
use crate::tablet::TabletContext;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  CommonQueryReplanningES
// -----------------------------------------------------------------------------------------------

pub trait QueryReplanningSqlView {
  /// Get the ColNames that must exist in the TableSchema (where it's not sufficient
  /// for the Context to have these columns instead).
  fn required_local_cols(&self) -> Vec<ColName>;
  /// Get the TablePath that the query reads.
  fn table(&self) -> &TablePath;
  /// Get all expressions in the Query (in some deterministic order)
  fn exprs(&self) -> Vec<proc::ValExpr>;
  /// This converts the underlying SQL into an MSQueryStage, useful
  /// for sending it out over the network.
  fn ms_query_stage(&self) -> proc::MSQueryStage;
}

impl QueryReplanningSqlView for proc::SuperSimpleSelect {
  fn required_local_cols(&self) -> Vec<ColName> {
    return self.projection.clone();
  }

  fn table(&self) -> &TablePath {
    cast!(proc::TableRef::TablePath, &self.from).unwrap()
  }

  fn exprs(&self) -> Vec<proc::ValExpr> {
    vec![self.selection.clone()]
  }

  fn ms_query_stage(&self) -> proc::MSQueryStage {
    proc::MSQueryStage::SuperSimpleSelect(self.clone())
  }
}

impl QueryReplanningSqlView for proc::Update {
  fn required_local_cols(&self) -> Vec<ColName> {
    self.assignment.iter().map(|(col, _)| col.clone()).collect()
  }

  fn table(&self) -> &TablePath {
    &self.table
  }

  fn exprs(&self) -> Vec<proc::ValExpr> {
    let mut exprs = Vec::new();
    exprs.push(self.selection.clone());
    for (_, expr) in &self.assignment {
      exprs.push(expr.clone());
    }
    exprs
  }

  fn ms_query_stage(&self) -> proc::MSQueryStage {
    proc::MSQueryStage::Update(self.clone())
  }
}

#[derive(Debug)]
pub enum CommonQueryReplanningS {
  Start,
  /// Used to lock the columsn in the SELECT clause or SET clause.
  RequiredLocalColumnLocking {
    locked_columns_query_id: QueryId,
  },
  /// Used to lock the query plan's columns
  ColumnLocking {
    locked_columns_query_id: QueryId,
  },
  /// Used to lock the query plan's columns after recomputation.
  RecomputeQueryPlan {
    locked_columns_query_id: QueryId,
  },
  /// Used to wait on the master
  MasterQueryReplanning {
    master_query_id: QueryId,
  },
  Done(bool),
}

#[derive(Debug)]
pub struct CommonQueryReplanningES<T: QueryReplanningSqlView> {
  // These members are parallel to the messages in `msg::GeneralQuery`.
  pub timestamp: Timestamp,
  pub context: Rc<Context>,
  pub sql_view: T,
  pub query_plan: QueryPlan,

  /// Path of the original sender (needed for responding with errors).
  pub sender_path: QueryPath,
  /// The OrigP of the Task holding this CommonQueryReplanningES
  pub query_id: QueryId,
  /// The state of the CommonQueryReplanningES
  pub state: CommonQueryReplanningS,
}

// -----------------------------------------------------------------------------------------------
//  Query Replanning
// -----------------------------------------------------------------------------------------------

impl<R: QueryReplanningSqlView> CommonQueryReplanningES<R> {
  /// This is the entrypoint of the ES.
  pub fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    matches!(self.state, CommonQueryReplanningS::Start);
    // See if all Required Column are locked at this Timestamp.
    if !are_cols_locked(&ctx.table_schema, &self.sql_view.required_local_cols(), &self.timestamp) {
      // Not all ColNames in Required Column are locked, so lock them.
      let locked_columns_query_id = ctx.add_requested_locked_columns(
        OrigP::new(self.query_id.clone()),
        self.timestamp,
        self.sql_view.required_local_cols(),
      );
      self.state = CommonQueryReplanningS::RequiredLocalColumnLocking { locked_columns_query_id };
      return;
    } else {
      self.check_required_cols_present(ctx);
    }
  }

  fn check_required_cols_present<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    // By this point, the Required Columns are locked, but we still need to verify
    // that the they are present in the TableSchema.
    for col in &self.sql_view.required_local_cols() {
      if !contains_col(&ctx.table_schema, col, &self.timestamp) {
        // This means a Required Column is not present. Thus, we exit, send back an error.
        ctx.ctx().send_query_error(
          self.sender_path.clone(),
          self.query_id.clone(),
          msg::QueryError::RequiredColumnsDNE { msg: String::new() },
        );
        self.state = CommonQueryReplanningS::Done(false);
        return;
      }
    }

    // Next, we check if the `gen` in the QueryPlan is >= the Tablet's `gen`. If so,
    // we need to try using this QueryPlan. We shouldn't just ignore it, since for liveness, we
    // want the `gen` of QueryPlans used during a Transaction to always increase.
    if ctx.gossip.gossip_gen <= self.query_plan.gossip_gen {
      // Check if all `safe_present_cols` and `external_cols` in this QueryPlan are locked.
      let node = &self.query_plan.col_usage_node;
      let mut all_cols = node.external_cols.clone();
      all_cols.extend(node.safe_present_cols.iter().cloned());

      if !are_cols_locked(&ctx.table_schema, &all_cols, &self.timestamp) {
        // If they aren't all locked, then lock them.
        let locked_columns_query_id = ctx.add_requested_locked_columns(
          OrigP::new(self.query_id.clone()),
          self.timestamp.clone(),
          all_cols,
        );
        self.state = CommonQueryReplanningS::ColumnLocking { locked_columns_query_id };
      } else {
        // Note that here, the `safe_present_cols` and `external_cols` of `node` must align
        // with the TableSchema. See the else clause in `check_cols_aligned`.
        self.state = CommonQueryReplanningS::Done(true);
      }
    } else {
      // Here, we compute a new QueryPlan using the local GossipGen.
      self.replan_query(ctx);
    }
  }

  fn check_cols_aligned<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    // Now, we need to verify that the `external_cols` and `safe_present_cols` (in the
    // QueryPlan sent to us) are all absent and present in the TableSchema respectively.
    let does_query_plan_align = (|| {
      // First, check that `external_cols are absent.
      for col in &self.query_plan.col_usage_node.external_cols {
        // Since the `key_cols` are static, no query plan should have one of
        // these as an External Column.
        assert!(lookup_pos(&ctx.table_schema.key_cols, col).is_none());
        if ctx.table_schema.val_cols.strong_static_read(&col, self.timestamp).is_some() {
          return false;
        }
      }
      // Next, check that `safe_present_cols` are present.
      for col in &self.query_plan.col_usage_node.safe_present_cols {
        if !contains_col(&ctx.table_schema, col, &self.timestamp) {
          return false;
        }
      }
      return true;
    })();

    if does_query_plan_align {
      // Here, QueryReplanning is finished and we may start executing the query.
      self.state = CommonQueryReplanningS::Done(true);
    } else {
      // Here, we compute a new QueryPlan using the local GossipGen.
      assert!(ctx.gossip.gossip_gen > self.query_plan.gossip_gen);
      self.replan_query(ctx);
    }
  }

  fn replan_query<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    // First, we compute the union of `safe_present_cols` and `external_cols` that we would
    // have if we were to create a new QueryPlan right now.
    let mut planner = ColUsagePlanner {
      gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
      timestamp: self.timestamp,
    };

    let all_cols = planner
      .get_all_cols(&mut self.query_plan.trans_table_schemas.clone(), &self.sql_view.exprs());

    if !are_cols_locked(&ctx.table_schema, &all_cols, &self.timestamp) {
      // If they aren't all locked, then lock them.
      let locked_columns_query_id = ctx.add_requested_locked_columns(
        OrigP::new(self.query_id.clone()),
        self.timestamp.clone(),
        all_cols.clone(),
      );
      self.state = CommonQueryReplanningS::RecomputeQueryPlan { locked_columns_query_id };
    } else {
      // Otherwise, we compute the QueryPlan. Recall that we needed `all_cols` above to be
      // locked in the TableSchema so we can figure out which are External and which are not.
      let mut schema_cols = Vec::<ColName>::new();
      for col in all_cols {
        if contains_col(&ctx.table_schema, &col, &self.timestamp) {
          schema_cols.push(col.clone());
        }
      }

      let (_, col_usage_node) = planner.plan_stage_query_with_schema(
        &mut self.query_plan.trans_table_schemas.clone(),
        &self.sql_view.required_local_cols(), // TODO: we shouldn't have to pass this in.
        &proc::TableRef::TablePath(self.sql_view.table().clone()),
        schema_cols,
        &self.sql_view.exprs(),
      );

      // Next, we check to see if all ColNames in `external_cols` is contaiend
      // in the context. If not, we have to consult the Master.
      for col in &col_usage_node.external_cols {
        if !self.context.context_schema.column_context_schema.contains(&col) {
          // This means we need to consult the Master.
          let master_query_id = mk_qid(&mut ctx.rand);
          ctx.network_output.send(
            &ctx.master_eid,
            msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
              msg::PerformMasterFrozenColUsage {
                sender_path: QueryPath {
                  slave_group_id: ctx.this_slave_group_id.clone(),
                  maybe_tablet_group_id: Some(ctx.this_tablet_group_id.clone()),
                  query_id: self.query_id.clone(),
                },
                query_id: master_query_id.clone(),
                timestamp: self.timestamp,
                trans_table_schemas: self.query_plan.trans_table_schemas.clone(),
                col_usage_tree: msg::ColUsageTree::MSQueryStage(self.sql_view.ms_query_stage()),
              },
            )),
          );

          // Advance Read Status
          self.state = CommonQueryReplanningS::MasterQueryReplanning { master_query_id };
          return;
        }
      }

      // If we make it here, we have a valid QueryPlan and we are done.
      self.query_plan.gossip_gen = ctx.gossip.gossip_gen;
      self.query_plan.col_usage_node = col_usage_node;
      self.state = CommonQueryReplanningS::Done(true);
    }
  }

  /// This is called when Columns have been locked, and the originator was this ES.
  pub fn columns_locked<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
    match &self.state {
      CommonQueryReplanningS::RequiredLocalColumnLocking { .. } => {
        self.check_required_cols_present(ctx);
      }
      CommonQueryReplanningS::ColumnLocking { .. } => {
        self.check_cols_aligned(ctx);
      }
      CommonQueryReplanningS::RecomputeQueryPlan { .. } => {
        self.replan_query(ctx);
      }
      _ => panic!(),
    }
  }

  /// Handles the Query Plan constructed by the Master.
  pub fn handle_master_response<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
    gossip_gen: Gen,
    tree: msg::FrozenColUsageTree,
  ) {
    // Recall that since we only send single nodes, we expect the `tree` to just be a `node`.
    let (_, col_usage_node) = cast!(msg::FrozenColUsageTree::ColUsageNode, tree).unwrap();

    // Compute the set of External Columns that still aren't in the Context.
    let mut missing_cols = Vec::<ColName>::new();
    for col in &col_usage_node.external_cols {
      if !self.context.context_schema.column_context_schema.contains(&col) {
        missing_cols.push(col.clone());
      }
    }

    if !missing_cols.is_empty() {
      // If the above set is non-empty, that means the QueryReplanning has conclusively
      // detected an insufficient Context, and so we respond to the sender and move to Done.
      ctx.ctx().send_abort_data(
        self.sender_path.clone(),
        self.query_id.clone(),
        msg::AbortedData::ColumnsDNE { missing_cols },
      );
      self.state = CommonQueryReplanningS::Done(false);
      return;
    } else {
      // This means the QueryReplanning was a success, so we update the QueryPlan and go to Done.
      self.query_plan.gossip_gen = gossip_gen;
      self.query_plan.col_usage_node = col_usage_node;
      self.state = CommonQueryReplanningS::Done(true);
    }
  }

  /// This Exits and Cleans up this QueryReplanningES
  pub fn exit_and_clean_up<T: IOTypes>(&self, ctx: &mut TabletContext<T>) {
    match &self.state {
      CommonQueryReplanningS::Start => {}
      CommonQueryReplanningS::RequiredLocalColumnLocking { locked_columns_query_id }
      | CommonQueryReplanningS::ColumnLocking { locked_columns_query_id }
      | CommonQueryReplanningS::RecomputeQueryPlan { locked_columns_query_id, .. } => {
        ctx.remove_col_locking_request(locked_columns_query_id.clone());
      }
      CommonQueryReplanningS::MasterQueryReplanning { master_query_id } => {
        // If the removal was successful, we should also send a Cancellation
        // message to the Master.
        ctx.network_output.send(
          &ctx.master_eid,
          msg::NetworkMessage::Master(msg::MasterMessage::CancelMasterFrozenColUsage(
            msg::CancelMasterFrozenColUsage { query_id: master_query_id.clone() },
          )),
        );
      }
      CommonQueryReplanningS::Done(_) => {}
    }
  }
}
