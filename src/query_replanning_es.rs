use crate::col_usage::ColUsagePlanner;
use crate::common::{lookup, lookup_pos, mk_qid, IOTypes, NetworkOut, OrigP, QueryPlan};
use crate::model::common::{proc, ColName, Context, Gen, QueryId, QueryPath, TablePath, Timestamp};
use crate::model::message as msg;
use crate::server::{are_cols_locked, contains_col};
use crate::tablet::TabletContext;
use std::rc::Rc;

// -----------------------------------------------------------------------------------------------
//  QueryReplanningSqlView
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

/// Implementation for `SuperSimpleSelect`.
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

/// Implementation for `Update`.
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

// -----------------------------------------------------------------------------------------------
//  CommonQueryReplanningES
// -----------------------------------------------------------------------------------------------

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
  Done,
}

#[derive(Debug)]
pub struct CommonQueryReplanningES<T: QueryReplanningSqlView> {
  // These members are parallel to the messages in `msg::GeneralQuery`.
  pub timestamp: Timestamp,
  pub context: Rc<Context>,
  pub sql_view: T,
  pub query_plan: QueryPlan,

  /// The OrigP of the Task holding this CommonQueryReplanningES
  pub query_id: QueryId,
  /// The state of the CommonQueryReplanningES
  pub state: CommonQueryReplanningS,
}

pub enum QueryReplanningAction {
  /// Indicates the parent needs to wait, making sure to fowards Column Locking and
  /// MasterQueryReplanning responses.
  Wait,
  /// Indicates the that CommonQueryReplanningES has computed a valid query where the Context
  /// is sufficient, and it's stored in the `query_plan` field.
  Success,
  /// Indicates a valid QueryPlan couldn't be computed and resulted in a QueryError.
  QueryError(msg::QueryError),
  /// Indicates a valid QueryPlan couldn't be computed because there were insufficient columns.
  ColumnsDNE(Vec<ColName>),
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl<R: QueryReplanningSqlView> CommonQueryReplanningES<R> {
  /// This is the entrypoint of the ES.
  pub fn start<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> QueryReplanningAction {
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
      QueryReplanningAction::Wait
    } else {
      self.check_required_cols_present(ctx)
    }
  }

  fn check_required_cols_present<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
  ) -> QueryReplanningAction {
    // By this point, the Required Columns are locked, but we still need to verify
    // that the they are present in the TableSchema.
    for col in &self.sql_view.required_local_cols() {
      if !contains_col(&ctx.table_schema, col, &self.timestamp) {
        // This means a Required Column is not present. Thus, we go to Done and return the error.
        self.state = CommonQueryReplanningS::Done;
        return QueryReplanningAction::QueryError(msg::QueryError::RequiredColumnsDNE {
          msg: String::new(),
        });
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
        QueryReplanningAction::Wait
      } else {
        // Since `ctx.gossip.gossip_gen <= self.query_plan.gossip_gen`, with a little proof by
        // contradiction, we see that the QueryPlan must align with the TableSchema.
        assert!(self.does_query_plan_align(ctx));
        self.state = CommonQueryReplanningS::Done;
        QueryReplanningAction::Success
      }
    } else {
      // Here, we compute a new QueryPlan using the local GossipGen.
      self.replan_query(ctx)
    }
  }

  /// This requires all columns in `query_plan` to be locked in the TableSchema. This checks
  /// that `external_cols` aren't present, and `safe_present_cols` are present.
  fn does_query_plan_align<T: IOTypes>(&self, ctx: &TabletContext<T>) -> bool {
    // First, check that `external_cols are absent.
    for col in &self.query_plan.col_usage_node.external_cols {
      // Since the `key_cols` are static, no query plan should have one of
      // these as an External Column.
      assert!(lookup(&ctx.table_schema.key_cols, col).is_none());
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
  }

  fn check_cols_aligned<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
  ) -> QueryReplanningAction {
    if self.does_query_plan_align(ctx) {
      // Here, QueryReplanning is finished and we may start executing the query.
      self.state = CommonQueryReplanningS::Done;
      QueryReplanningAction::Success
    } else {
      // Here, we compute a new QueryPlan using the local GossipGen.
      assert!(ctx.gossip.gossip_gen > self.query_plan.gossip_gen);
      self.replan_query(ctx)
    }
  }

  fn replan_query<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) -> QueryReplanningAction {
    // Here, we compute a new QueryPlan. Recall that `gossiped_db_schema` will contain
    // the most up-to-date TableSchema of this Tablet.
    let mut planner = ColUsagePlanner {
      gossiped_db_schema: &ctx.gossip.gossiped_db_schema,
      timestamp: self.timestamp,
    };

    let col_usage_node = planner.compute_frozen_col_usage_node(
      &mut self.query_plan.trans_table_schemas.clone(),
      &proc::TableRef::TablePath(self.sql_view.table().clone()),
      &self.sql_view.exprs(),
    );

    // Make sure that all `external_cols` and `safe_present_cols` are locked in the TabletSchema.
    let mut all_cols = col_usage_node.safe_present_cols.clone();
    all_cols.extend(col_usage_node.external_cols.clone());

    if !are_cols_locked(&ctx.table_schema, &all_cols, &self.timestamp) {
      // If they aren't all locked, then lock them.
      let locked_columns_query_id = ctx.add_requested_locked_columns(
        OrigP::new(self.query_id.clone()),
        self.timestamp.clone(),
        all_cols.clone(),
      );
      self.state = CommonQueryReplanningS::RecomputeQueryPlan { locked_columns_query_id };
      QueryReplanningAction::Wait
    } else {
      // If they are locked, we check to see if all ColNames in `external_cols` is contaiend
      // in the context. If not, we have to consult the Master.
      for col in &col_usage_node.external_cols {
        if !self.context.context_schema.column_context_schema.contains(&col) {
          // This means we need to consult the Master.
          let master_query_id = mk_qid(&mut ctx.rand);
          ctx.network_output.send(
            &ctx.master_eid,
            msg::NetworkMessage::Master(msg::MasterMessage::PerformMasterFrozenColUsage(
              msg::PerformMasterFrozenColUsage {
                sender_path: ctx.mk_query_path(self.query_id.clone()),
                query_id: master_query_id.clone(),
                timestamp: self.timestamp,
                trans_table_schemas: self.query_plan.trans_table_schemas.clone(),
                col_usage_tree: msg::ColUsageTree::MSQueryStage(self.sql_view.ms_query_stage()),
              },
            )),
          );

          // Advance Read Status
          self.state = CommonQueryReplanningS::MasterQueryReplanning { master_query_id };
          return QueryReplanningAction::Wait;
        }
      }

      // If we make it here, we have a valid QueryPlan and we are done.
      self.query_plan.gossip_gen = ctx.gossip.gossip_gen;
      self.query_plan.col_usage_node = col_usage_node;
      self.state = CommonQueryReplanningS::Done;
      QueryReplanningAction::Success
    }
  }

  /// This is called when Columns have been locked, and the originator was this ES.
  pub fn columns_locked<T: IOTypes>(
    &mut self,
    ctx: &mut TabletContext<T>,
  ) -> QueryReplanningAction {
    match &self.state {
      CommonQueryReplanningS::RequiredLocalColumnLocking { .. } => {
        self.check_required_cols_present(ctx)
      }
      CommonQueryReplanningS::ColumnLocking { .. } => self.check_cols_aligned(ctx),
      CommonQueryReplanningS::RecomputeQueryPlan { .. } => self.replan_query(ctx),
      _ => panic!(),
    }
  }

  /// Handles the Query Plan constructed by the Master.
  pub fn handle_master_response<T: IOTypes>(
    &mut self,
    _: &mut TabletContext<T>,
    gossip_gen: Gen,
    tree: msg::FrozenColUsageTree,
  ) -> QueryReplanningAction {
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
      // detected an insufficient Context, and move to Done and return the missing columns.
      self.state = CommonQueryReplanningS::Done;
      QueryReplanningAction::ColumnsDNE(missing_cols)
    } else {
      // This means the QueryReplanning was a success, so we update the QueryPlan and go to Done.
      self.query_plan.gossip_gen = gossip_gen;
      self.query_plan.col_usage_node = col_usage_node;
      self.state = CommonQueryReplanningS::Done;
      QueryReplanningAction::Success
    }
  }

  /// This Exits and Cleans up this QueryReplanningES.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut TabletContext<T>) {
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
      CommonQueryReplanningS::Done => {}
    }
    self.state = CommonQueryReplanningS::Done;
  }
}
