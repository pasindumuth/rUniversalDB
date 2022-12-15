use crate::alter_table_rm_es::{
  AlterTableRMAction, AlterTableRMES, AlterTableRMInner, AlterTableRMPayloadTypes,
};
use crate::alter_table_tm_es::AlterTableTMPayloadTypes;
use crate::col_usage::{
  alias_collecting_cb, external_col_collecting_cb, external_trans_table_collecting_cb,
  trans_table_collecting_cb, QueryElement, QueryIterator,
};
use crate::common::{
  btree_multimap_insert, lookup, map_insert, mk_qid, mk_t, remove_item, update_leader_map,
  update_leader_map_unversioned, BasicIOCtx, BoundType, CoreIOCtx, GossipData, KeyBound, LeaderMap,
  OrigP, QueryESResult, QueryPlan, ReadRegion, RemoteLeaderChangedPLm, ShardingGen, TableSchema,
  Timestamp, VersionedValue, WriteRegion,
};
use crate::common::{
  CNodePath, CQueryPath, CTQueryPath, CTSubNodePath, ColType, ColVal, ColValN, Context, ContextRow,
  ContextSchema, Gen, LeadershipId, PaxosGroupId, PaxosGroupIdTrait, PrimaryKey, TNodePath,
  TQueryPath, TSubNodePath, TableView, TransTableName,
};
use crate::common::{
  ColName, EndpointId, QueryId, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange,
};
use crate::drop_table_rm_es::{
  DropTableRMAction, DropTableRMES, DropTableRMInner, DropTableRMPayloadTypes,
};
use crate::drop_table_tm_es::DropTableTMPayloadTypes;
use crate::expression::{
  compute_key_region, is_surely_isolated_multiread, is_surely_isolated_multiwrite,
  range_row_region_intersection, EvalError,
};
use crate::finish_query_rm_es::{FinishQueryRMES, FinishQueryRMInner};
use crate::finish_query_tm_es::FinishQueryPayloadTypes;
use crate::gr_query_es::{GRQueryAction, GRQueryConstructorView, GRQueryES, SubqueryComputableSql};
use crate::join_util::compute_children_general;
use crate::message as msg;
use crate::ms_table_delete_es::{DeleteInner, MSTableDeleteES};
use crate::ms_table_es::{GeneralQueryES, MSTableES, MSTableExecutionS, SqlQueryInner};
use crate::ms_table_insert_es::{InsertInner, MSTableInsertES};
use crate::ms_table_read_es::{MSTableReadES, SelectInner};
use crate::ms_table_write_es::{MSTableWriteES, UpdateInner};
use crate::paxos2pc_rm;
use crate::paxos2pc_tm;
use crate::server::{
  CTServerContext, CommonQuery, ContextConstructor, LocalColumnRef, LocalTable, ServerContextBase,
};
use crate::shard_snapshot_es::{ShardingConfirmedPLm, ShardingSnapshotAction, ShardingSnapshotES};
use crate::shard_split_tablet_rm_es::{
  ShardSplitTabletRMAction, ShardSplitTabletRMES, ShardSplitTabletRMPayloadTypes,
};
use crate::slave::{SlaveBackMessage, TabletBundleInsertion};
use crate::sql_ast::proc;
use crate::stmpaxos2pc_rm;
use crate::storage::{GenericMVTable, GenericTable, StorageView};
use crate::table_read_es::{ExecutionS, TableReadES};
use crate::tm_status::TMStatus;
use crate::trans_table_read_es::{TransExecutionS, TransTableReadES};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sqlparser::test_utils::table;
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::rc::Rc;
use std::sync::Arc;

#[path = "test/tablet_test.rs"]
pub mod tablet_test;

// -----------------------------------------------------------------------------------------------
//  Before Subquery
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct ColumnsLocking {
  pub locked_cols_qid: QueryId,
}

#[derive(Debug)]
pub struct Pending {
  pub query_id: QueryId,
}

// -----------------------------------------------------------------------------------------------
//  Subquery Execution
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct SubqueryPending {
  context: Rc<Context>,
  query_id: QueryId,
}

#[derive(Debug)]
struct SubqueryFinished {
  context: Rc<Context>,
  result: Vec<TableView>,
}

#[derive(Debug, Default)]
pub struct Executing {
  /// Since Subquery's have an order, we need to remember that.
  order: Vec<QueryId>,
  pending: BTreeMap<QueryId, SubqueryPending>,
  finished: BTreeMap<QueryId, SubqueryFinished>,
}

impl Executing {
  pub fn create(gr_query_ess: &Vec<GRQueryES>) -> Executing {
    let mut exec = Executing::default();
    for es in gr_query_ess {
      exec.order.push(es.query_id.clone());
      exec.pending.insert(
        es.query_id.clone(),
        SubqueryPending { context: es.context.clone(), query_id: es.query_id.clone() },
      );
    }
    exec
  }

  /// Add in the results of a subquery. The given `query_id` must still be be pending.
  pub fn add_subquery_result(&mut self, query_id: QueryId, table_views: Vec<TableView>) {
    let pending_subquery = self.pending.remove(&query_id).unwrap();
    self.finished.insert(
      query_id,
      SubqueryFinished { context: pending_subquery.context, result: table_views },
    );
  }

  /// This should only be called when `is_complete` evalautes to true.
  pub fn get_results(
    self,
  ) -> (Vec<(Vec<proc::ColumnRef>, Vec<TransTableName>)>, Vec<Vec<TableView>>) {
    let mut children = Vec::<(Vec<proc::ColumnRef>, Vec<TransTableName>)>::new();
    let mut subquery_results = Vec::<Vec<TableView>>::new();
    for query_id in self.order {
      let result = self.finished.get(&query_id).unwrap();
      let context_schema = &result.context.context_schema;
      children
        .push((context_schema.column_context_schema.clone(), context_schema.trans_table_names()));
      subquery_results.push(result.result.clone());
    }
    (children, subquery_results)
  }

  /// Return true iff all subqueries have returne dtheir result.
  pub fn is_complete(&self) -> bool {
    self.pending.is_empty()
  }
}

// -----------------------------------------------------------------------------------------------
//  MSQueryES
// -----------------------------------------------------------------------------------------------

/// When this exists, there will be a corresponding `VerifyingReadWriteRegion`.
#[derive(Debug)]
pub struct MSQueryES {
  pub root_query_path: CQueryPath,
  pub sharding_gen: ShardingGen,
  // The LeadershipId of the root PaxosNode.
  pub root_lid: LeadershipId,
  pub query_id: QueryId,
  pub timestamp: Timestamp,
  pub update_views: BTreeMap<u32, GenericTable>,
  /// This holds all `MSTable*ES`s that belong to this MSQueryES. We make sure
  /// that every ES reference here exist.
  pub pending_queries: BTreeSet<QueryId>,
}

// -----------------------------------------------------------------------------------------------
//  Wrappers
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct TransTableReadESWrapper {
  pub sender_path: CTQueryPath,
  pub child_queries: Vec<QueryId>,
  pub es: TransTableReadES,
}

impl TransTableReadESWrapper {
  pub fn sender_gid(&self) -> PaxosGroupId {
    self.sender_path.node_path.sid.to_gid()
  }
}

#[derive(Debug)]
pub struct GRQueryESWrapper {
  pub child_queries: Vec<QueryId>,
  pub es: GRQueryES,
}

// -----------------------------------------------------------------------------------------------
//  TabletSnapshot
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshot {
  /// Metadata
  pub this_sid: SlaveGroupId,
  pub this_tid: TabletGroupId,
  pub sub_node_path: CTSubNodePath, // Wraps `this_tablet_group_id` for expedience
  pub leader_map: LeaderMap,
  pub this_table_path: TablePath,

  // Sharding
  pub this_sharding_gen: ShardingGen,
  pub this_table_key_range: TabletKeyRange,
  pub sharding_done: bool,

  // Storage
  pub storage: GenericMVTable,

  // Schema
  pub table_schema: TableSchema,
  pub presence_timestamp: Timestamp,

  // Region Isolation Algorithm
  pub prepared_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub committed_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub read_protected: BTreeMap<Timestamp, BTreeSet<ReadRegion>>,

  // Statuses
  /// If this is a Follower, we copy over the ESs in `Statuses` to the below. If this
  /// is the Leader, we compute the ESs that would result as a result of a Leadership
  /// change and populate the below.
  pub finish_query_ess: BTreeMap<QueryId, FinishQueryRMES>,
  pub ddl_es: DDLES,
  pub sharding_state: ShardingState,
}

// -----------------------------------------------------------------------------------------------
//  ShardingSnapshot
// -----------------------------------------------------------------------------------------------

/// When data from this `Tablet` needs to be sent to another one for the purpose
/// of Sharding, this is what is used.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ShardingSnapshot {
  /// Metadata
  pub this_tid: TabletGroupId, // The `TabletGroupId` of the new Tablet.
  pub this_table_path: TablePath,

  /// Sharding. Here, `this_table_key_range` is that of the target Tablet.  
  pub this_sharding_gen: ShardingGen,
  pub this_table_key_range: TabletKeyRange,

  /// Storage. This only contains the data that is being
  /// split off from the sending Tablet.
  pub storage: GenericMVTable,

  // Schema
  pub table_schema: TableSchema,
  pub presence_timestamp: Timestamp,

  // Region Isolation Algorithm
  pub committed_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub read_protected: BTreeMap<Timestamp, BTreeSet<ReadRegion>>,
}

// -----------------------------------------------------------------------------------------------
//  TPESBase
// -----------------------------------------------------------------------------------------------

pub trait TPESBase {
  type ESContext;

  fn sender_sid(&self) -> &SlaveGroupId;
  fn query_id(&self) -> &QueryId;
  fn ctx_query_id(&self) -> Option<&QueryId>;

  fn start<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _es_ctx: &mut Self::ESContext,
  ) -> Option<TPESAction> {
    None
  }

  fn local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _locked_cols_qid: QueryId,
  ) -> Option<TPESAction> {
    None
  }

  fn global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _locked_cols_qid: QueryId,
  ) -> Option<TPESAction> {
    None
  }

  fn table_dropped(&mut self, _ctx: &mut TabletContext) -> Option<TPESAction> {
    None
  }

  fn gossip_data_changed<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _es_ctx: &mut Self::ESContext,
  ) -> Option<TPESAction> {
    None
  }

  fn m_local_read_protected<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _es_ctx: &mut Self::ESContext,
    _protect_qid: QueryId,
  ) -> Option<TPESAction> {
    None
  }

  fn local_read_protected<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _es_ctx: &mut Self::ESContext,
    _protect_qid: QueryId,
  ) -> Option<TPESAction> {
    None
  }

  fn global_read_protected<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _protect_qid: QueryId,
  ) -> Option<TPESAction> {
    None
  }

  fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _query_error: msg::QueryError,
  ) -> Option<TPESAction> {
    None
  }

  fn handle_subquery_done<IO: CoreIOCtx>(
    &mut self,
    _ctx: &mut TabletContext,
    _io_ctx: &mut IO,
    _es_ctx: &mut Self::ESContext,
    _subquery_id: QueryId,
    _subquery_new_rms: BTreeSet<TQueryPath>,
    _results: Vec<TableView>,
  ) -> Option<TPESAction> {
    None
  }

  fn exit_and_clean_up<IO: CoreIOCtx>(&mut self, _ctx: &mut TabletContext, _io_ctx: &mut IO) {}

  // TODO: see if we can merge this with exit_and_clean_up
  fn deregister(self, _es_ctx: &mut Self::ESContext) -> (QueryId, CTQueryPath, Vec<QueryId>);

  fn remove_subquery(&mut self, _subquery_id: &QueryId);
}

pub enum TPESAction {
  /// This tells the parent Server to perform subqueries.
  SendSubqueries(Vec<GRQueryES>),
  /// Indicates the ES succeeded with the given result.
  Success(QueryESResult),
  /// Indicates the ES failed with a QueryError.
  QueryError(msg::QueryError),
}

// -----------------------------------------------------------------------------------------------
//  MSCallable
// -----------------------------------------------------------------------------------------------

trait Callback<ExtraDataT> {
  fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    es: &mut TPEST,
    extra_data: &ExtraDataT,
  ) -> TabletAction;
}

trait CallbackWithContext<ExtraDataT> {
  fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    es: &mut TPEST,
    es_ctx: &mut TPEST::ESContext,
    extra_data: &ExtraDataT,
  ) -> TabletAction;
}

trait CallbackOnce<ExtraDataT> {
  fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    es: &mut TPEST,
    extra_data: ExtraDataT,
  ) -> TabletAction;
}

trait CallbackWithContextOnce<ExtraDataT> {
  fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    es: &mut TPEST,
    es_ctx: &mut TPEST::ESContext,
    extra_data: ExtraDataT,
  ) -> TabletAction;
}

trait CallbackWithContextRemove<ExtraDataT> {
  fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    es: TPEST,
    es_ctx: &mut TPEST::ESContext,
    extra_data: ExtraDataT,
  ) -> TabletAction;
}

/// A more general action that the above callbacks can return so that the
/// `TabletContext` can do more than just `TPESAction`s.
pub enum TabletAction {
  Wait,
  TPESAction(Option<TPESAction>),
  ExitAll(Vec<QueryId>),
  ExitAndCleanUp(QueryId),
}

// -----------------------------------------------------------------------------------------------
//  Status
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Default)]
struct TopLevelStatuses {
  table_read_ess: BTreeMap<QueryId, TableReadES>,
  trans_table_read_ess: BTreeMap<QueryId, TransTableReadES>,
  ms_table_read_ess: BTreeMap<QueryId, MSTableReadES>,
  ms_table_write_ess: BTreeMap<QueryId, MSTableWriteES>,
  ms_table_insert_ess: BTreeMap<QueryId, MSTableInsertES>,
  ms_table_delete_ess: BTreeMap<QueryId, MSTableDeleteES>,
}

/// This contains every TabletStatus. Every QueryId here is unique across all
/// other members here.
/// NOTE: When adding a new element here, amend the `TabletSnapshot` accordingly.
#[derive(Debug, Default)]
pub struct Statuses {
  // Paxos2PC
  finish_query_ess: BTreeMap<QueryId, FinishQueryRMES>,

  /// TP

  /// This contains `PerformQuery`s with a `root_query_path`
  perform_query_buffer: BTreeMap<QueryId, msg::PerformQuery>,
  gr_query_ess: BTreeMap<QueryId, GRQueryESWrapper>,
  tm_statuss: BTreeMap<QueryId, TMStatus>,
  ms_query_ess: BTreeMap<QueryId, MSQueryES>,
  top: TopLevelStatuses,

  // DDL
  ddl_es: DDLES,

  // Sharding
  sharding_state: ShardingState,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum DDLES {
  /// DDL ESs
  None,
  Alter(AlterTableRMES),
  Drop(DropTableRMES),
  Dropped(Timestamp),

  /// Shard ESs
  ShardSplit(ShardSplitTabletRMES),
}

impl Default for DDLES {
  fn default() -> Self {
    DDLES::None
  }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ShardingState {
  None,
  ShardingSnapshotES(ShardingSnapshotES),
}

impl Default for ShardingState {
  fn default() -> Self {
    ShardingState::None
  }
}

// -----------------------------------------------------------------------------------------------
//  ESContGetter
// -----------------------------------------------------------------------------------------------
// This is used to provide a uniform interface access the `TPESBase`
// containers located in `Statuses`

trait ESContGetter: Sized + TPESBase {
  fn mut_ess(top: &mut TopLevelStatuses) -> &mut BTreeMap<QueryId, Self>;
  fn mut_ctx<'a>(
    query_id: Option<&QueryId>,
    unit: &'a mut (),
    gr_query_ess: &'a mut BTreeMap<QueryId, GRQueryESWrapper>,
    ms_query_ess: &'a mut BTreeMap<QueryId, MSQueryES>,
  ) -> Option<&'a mut Self::ESContext>;
}

impl ESContGetter for TableReadES {
  fn mut_ess(top: &mut TopLevelStatuses) -> &mut BTreeMap<QueryId, Self> {
    &mut top.table_read_ess
  }

  /// Recall that the ESContext of `TableReadES` is just `()`, which always exists.
  fn mut_ctx<'a>(
    _: Option<&QueryId>,
    unit: &'a mut (),
    _: &'a mut BTreeMap<QueryId, GRQueryESWrapper>,
    _: &'a mut BTreeMap<QueryId, MSQueryES>,
  ) -> Option<&'a mut Self::ESContext> {
    Some(unit)
  }
}

impl ESContGetter for TransTableReadES {
  fn mut_ess(top: &mut TopLevelStatuses) -> &mut BTreeMap<QueryId, Self> {
    &mut top.trans_table_read_ess
  }

  fn mut_ctx<'a>(
    query_id: Option<&QueryId>,
    _: &'a mut (),
    gr_query_ess: &'a mut BTreeMap<QueryId, GRQueryESWrapper>,
    _: &'a mut BTreeMap<QueryId, MSQueryES>,
  ) -> Option<&'a mut Self::ESContext> {
    gr_query_ess.get_mut(query_id.unwrap()).map(|wrapper| &mut wrapper.es)
  }
}

impl ESContGetter for MSTableReadES {
  fn mut_ess(top: &mut TopLevelStatuses) -> &mut BTreeMap<QueryId, Self> {
    &mut top.ms_table_read_ess
  }

  fn mut_ctx<'a>(
    query_id: Option<&QueryId>,
    _: &'a mut (),
    _: &'a mut BTreeMap<QueryId, GRQueryESWrapper>,
    ms_query_ess: &'a mut BTreeMap<QueryId, MSQueryES>,
  ) -> Option<&'a mut Self::ESContext> {
    ms_query_ess.get_mut(query_id.unwrap())
  }
}

impl ESContGetter for MSTableWriteES {
  fn mut_ess(top: &mut TopLevelStatuses) -> &mut BTreeMap<QueryId, Self> {
    &mut top.ms_table_write_ess
  }

  fn mut_ctx<'a>(
    query_id: Option<&QueryId>,
    _: &'a mut (),
    _: &'a mut BTreeMap<QueryId, GRQueryESWrapper>,
    ms_query_ess: &'a mut BTreeMap<QueryId, MSQueryES>,
  ) -> Option<&'a mut Self::ESContext> {
    ms_query_ess.get_mut(query_id.unwrap())
  }
}

impl ESContGetter for MSTableInsertES {
  fn mut_ess(top: &mut TopLevelStatuses) -> &mut BTreeMap<QueryId, Self> {
    &mut top.ms_table_insert_ess
  }

  fn mut_ctx<'a>(
    query_id: Option<&QueryId>,
    _: &'a mut (),
    _: &'a mut BTreeMap<QueryId, GRQueryESWrapper>,
    ms_query_ess: &'a mut BTreeMap<QueryId, MSQueryES>,
  ) -> Option<&'a mut Self::ESContext> {
    ms_query_ess.get_mut(query_id.unwrap())
  }
}

impl ESContGetter for MSTableDeleteES {
  fn mut_ess(top: &mut TopLevelStatuses) -> &mut BTreeMap<QueryId, Self> {
    &mut top.ms_table_delete_ess
  }

  fn mut_ctx<'a>(
    query_id: Option<&QueryId>,
    _: &'a mut (),
    _: &'a mut BTreeMap<QueryId, GRQueryESWrapper>,
    ms_query_ess: &'a mut BTreeMap<QueryId, MSQueryES>,
  ) -> Option<&'a mut Self::ESContext> {
    ms_query_ess.get_mut(query_id.unwrap())
  }
}

impl Statuses {
  // -----------------------------------------------------------------------------------------------
  //  do_it
  // -----------------------------------------------------------------------------------------------

  fn do_it_once<IOCtx: CoreIOCtx, ExtraDataT, Cb: CallbackOnce<ExtraDataT>, TPEST: TPESBase>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    query_id: QueryId,
    extra_data: ExtraDataT,
  ) -> Option<(QueryId, ExtraDataT)>
  where
    TPEST: ESContGetter,
  {
    if let Some(es) = TPEST::mut_ess(&mut self.top).get_mut(&query_id) {
      let action = Cb::call(ctx, io_ctx, es, extra_data);
      ctx.handle_tablet_action(io_ctx, self, query_id, action);
      None
    } else {
      Some((query_id, extra_data))
    }
  }

  fn do_it<IOCtx: CoreIOCtx, ExtraDataT, Cb: Callback<ExtraDataT>, TPEST: TPESBase>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    query_id: QueryId,
    extra_data: &ExtraDataT,
  ) where
    TPEST: ESContGetter,
  {
    if let Some(es) = TPEST::mut_ess(&mut self.top).get_mut(&query_id) {
      let action = Cb::call(ctx, io_ctx, es, extra_data);
      ctx.handle_tablet_action(io_ctx, self, query_id, action);
    }
  }

  fn do_it_all<IOCtx: CoreIOCtx, ExtraDataT, Cb: Callback<ExtraDataT>, TPEST: TPESBase>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    extra_data: &ExtraDataT,
  ) where
    TPEST: ESContGetter,
  {
    let query_ids: Vec<QueryId> = TPEST::mut_ess(&mut self.top).keys().cloned().collect();
    for query_id in query_ids {
      self.do_it::<_, _, Cb, TPEST>(ctx, io_ctx, query_id, extra_data);
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  do_it_ctx
  // -----------------------------------------------------------------------------------------------

  fn do_it_once_ctx<
    IOCtx: CoreIOCtx,
    ExtraDataT,
    Cb: CallbackWithContextOnce<ExtraDataT>,
    TPEST: TPESBase,
  >(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    query_id: QueryId,
    extra_data: ExtraDataT,
  ) -> Option<(QueryId, ExtraDataT)>
  where
    TPEST: ESContGetter,
  {
    if let Some(es) = TPEST::mut_ess(&mut self.top).get_mut(&query_id) {
      let action = if let Some(ctx_es) =
        TPEST::mut_ctx(es.ctx_query_id(), &mut (), &mut self.gr_query_ess, &mut self.ms_query_ess)
      {
        Cb::call(ctx, io_ctx, es, ctx_es, extra_data)
      } else {
        TabletAction::TPESAction(es.handle_internal_query_error(
          ctx,
          io_ctx,
          msg::QueryError::LateralError,
        ))
      };
      ctx.handle_tablet_action(io_ctx, self, query_id, action);
      None
    } else {
      Some((query_id, extra_data))
    }
  }

  fn do_it_ctx<IOCtx: CoreIOCtx, ExtraDataT, Cb: CallbackWithContext<ExtraDataT>, TPEST: TPESBase>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    query_id: QueryId,
    extra_data: &ExtraDataT,
  ) where
    TPEST: ESContGetter,
  {
    if let Some(es) = TPEST::mut_ess(&mut self.top).get_mut(&query_id) {
      let action = if let Some(ctx_es) =
        TPEST::mut_ctx(es.ctx_query_id(), &mut (), &mut self.gr_query_ess, &mut self.ms_query_ess)
      {
        Cb::call(ctx, io_ctx, es, ctx_es, extra_data)
      } else {
        TabletAction::TPESAction(es.handle_internal_query_error(
          ctx,
          io_ctx,
          msg::QueryError::LateralError,
        ))
      };
      ctx.handle_tablet_action(io_ctx, self, query_id, action);
    }
  }

  fn do_it_all_ctx<
    IOCtx: CoreIOCtx,
    ExtraDataT,
    Cb: CallbackWithContext<ExtraDataT>,
    TPEST: TPESBase,
  >(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    extra_data: &ExtraDataT,
  ) where
    TPEST: ESContGetter,
  {
    let query_ids: Vec<QueryId> = TPEST::mut_ess(&mut self.top).keys().cloned().collect();
    for query_id in query_ids {
      self.do_it_ctx::<_, _, Cb, TPEST>(ctx, io_ctx, query_id, extra_data);
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  do_it_remove_ctx
  // -----------------------------------------------------------------------------------------------

  fn do_it_remove_ctx<
    IOCtx: CoreIOCtx,
    ExtraDataT,
    Cb: CallbackWithContextRemove<ExtraDataT>,
    TPEST: TPESBase,
  >(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    query_id: QueryId,
    extra_data: ExtraDataT,
  ) -> Option<(QueryId, ExtraDataT)>
  where
    TPEST: ESContGetter,
  {
    if let Some(mut es) = TPEST::mut_ess(&mut self.top).remove(&query_id) {
      let action = if let Some(ctx_es) =
        TPEST::mut_ctx(es.ctx_query_id(), &mut (), &mut self.gr_query_ess, &mut self.ms_query_ess)
      {
        Cb::call(ctx, io_ctx, es, ctx_es, extra_data)
      } else {
        TabletAction::TPESAction(es.handle_internal_query_error(
          ctx,
          io_ctx,
          msg::QueryError::LateralError,
        ))
      };
      ctx.handle_tablet_action(io_ctx, self, query_id, action);
      None
    } else {
      Some((query_id, extra_data))
    }
  }

  // -----------------------------------------------------------------------------------------------
  //  execute_*
  // -----------------------------------------------------------------------------------------------

  fn execute_all<IOCtx: CoreIOCtx, ExtraDataT, Cb: Callback<ExtraDataT>>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    extra_data: &ExtraDataT,
  ) {
    self.do_it_all::<_, _, Cb, TableReadES>(ctx, io_ctx, extra_data);
    self.do_it_all::<_, _, Cb, TransTableReadES>(ctx, io_ctx, extra_data);
    self.do_it_all::<_, _, Cb, MSTableReadES>(ctx, io_ctx, extra_data);
    self.do_it_all::<_, _, Cb, MSTableWriteES>(ctx, io_ctx, extra_data);
    self.do_it_all::<_, _, Cb, MSTableInsertES>(ctx, io_ctx, extra_data);
    self.do_it_all::<_, _, Cb, MSTableDeleteES>(ctx, io_ctx, extra_data);
  }

  fn execute_all_ctx<IOCtx: CoreIOCtx, ExtraDataT, Cb: CallbackWithContext<ExtraDataT>>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    extra_data: &ExtraDataT,
  ) {
    self.do_it_all_ctx::<_, _, Cb, TableReadES>(ctx, io_ctx, extra_data);
    self.do_it_all_ctx::<_, _, Cb, TransTableReadES>(ctx, io_ctx, extra_data);
    self.do_it_all_ctx::<_, _, Cb, MSTableReadES>(ctx, io_ctx, extra_data);
    self.do_it_all_ctx::<_, _, Cb, MSTableWriteES>(ctx, io_ctx, extra_data);
    self.do_it_all_ctx::<_, _, Cb, MSTableInsertES>(ctx, io_ctx, extra_data);
    self.do_it_all_ctx::<_, _, Cb, MSTableDeleteES>(ctx, io_ctx, extra_data);
  }

  fn execute_once<IOCtx: CoreIOCtx, ExtraDataT, Cb: CallbackOnce<ExtraDataT>>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    query_id: QueryId,
    extra_data: ExtraDataT,
  ) -> Option<(QueryId, ExtraDataT)> {
    let (query_id, extra_data) =
      self.do_it_once::<_, _, Cb, TableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once::<_, _, Cb, TransTableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once::<_, _, Cb, MSTableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once::<_, _, Cb, MSTableWriteES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once::<_, _, Cb, MSTableInsertES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once::<_, _, Cb, MSTableDeleteES>(ctx, io_ctx, query_id, extra_data)?;
    Some((query_id, extra_data))
  }

  fn execute_once_ctx<IOCtx: CoreIOCtx, ExtraDataT, Cb: CallbackWithContextOnce<ExtraDataT>>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    query_id: QueryId,
    extra_data: ExtraDataT,
  ) -> Option<(QueryId, ExtraDataT)> {
    let (query_id, extra_data) =
      self.do_it_once_ctx::<_, _, Cb, TableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once_ctx::<_, _, Cb, TransTableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once_ctx::<_, _, Cb, MSTableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once_ctx::<_, _, Cb, MSTableWriteES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once_ctx::<_, _, Cb, MSTableInsertES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_once_ctx::<_, _, Cb, MSTableDeleteES>(ctx, io_ctx, query_id, extra_data)?;
    Some((query_id, extra_data))
  }

  fn execute_remove_ctx<IOCtx: CoreIOCtx, ExtraDataT, Cb: CallbackWithContextRemove<ExtraDataT>>(
    &mut self,
    ctx: &mut TabletContext,
    io_ctx: &mut IOCtx,
    query_id: QueryId,
    extra_data: ExtraDataT,
  ) -> Option<(QueryId, ExtraDataT)> {
    let (query_id, extra_data) =
      self.do_it_remove_ctx::<_, _, Cb, TableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_remove_ctx::<_, _, Cb, TransTableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_remove_ctx::<_, _, Cb, MSTableReadES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_remove_ctx::<_, _, Cb, MSTableWriteES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_remove_ctx::<_, _, Cb, MSTableInsertES>(ctx, io_ctx, query_id, extra_data)?;
    let (query_id, extra_data) =
      self.do_it_remove_ctx::<_, _, Cb, MSTableDeleteES>(ctx, io_ctx, query_id, extra_data)?;
    Some((query_id, extra_data))
  }
}

// -----------------------------------------------------------------------------------------------
//  DDL Aggregate Container
// -----------------------------------------------------------------------------------------------
/// Implementation for DDLES

impl paxos2pc_tm::Paxos2PCContainer<AlterTableRMES> for DDLES {
  fn get_mut(&mut self, query_id: &QueryId) -> Option<&mut AlterTableRMES> {
    if let DDLES::Alter(es) = self {
      // Recall that our DDL and Sharding Coordination scheme requires the previous
      // STMPaxos2PC to be totally done before the next, so we should never get
      // mismatching QueryId's here.
      debug_assert_eq!(&es.query_id, query_id);
      Some(es)
    } else {
      // Similarly, if there is no running ShardSplit, no other ESs should be here.
      match self {
        DDLES::None => (),
        _ => debug_assert!(false),
      }
      None
    }
  }

  fn insert(&mut self, _: QueryId, es: AlterTableRMES) {
    *self = DDLES::Alter(es);
  }
}

impl paxos2pc_tm::Paxos2PCContainer<DropTableRMES> for DDLES {
  fn get_mut(&mut self, query_id: &QueryId) -> Option<&mut DropTableRMES> {
    if let DDLES::Drop(es) = self {
      // Recall that our DDL and Sharding Coordination scheme requires the previous
      // STMPaxos2PC to be totally done before the next, so we should never get
      // mismatching QueryId's here.
      debug_assert_eq!(&es.query_id, query_id);
      Some(es)
    } else {
      // Similarly, if there is no running DropTable, no other ESs should be here. Note
      // that this Tablet can be `Dropped` by now (as a consequence of `DropTable` having
      // already committed).
      match self {
        DDLES::None | DDLES::Dropped(_) => (),
        _ => debug_assert!(false),
      }
      None
    }
  }

  fn insert(&mut self, _: QueryId, es: DropTableRMES) {
    *self = DDLES::Drop(es);
  }
}

impl paxos2pc_tm::Paxos2PCContainer<ShardSplitTabletRMES> for DDLES {
  fn get_mut(&mut self, query_id: &QueryId) -> Option<&mut ShardSplitTabletRMES> {
    if let DDLES::ShardSplit(es) = self {
      // Recall that our DDL and Sharding Coordination scheme requires the previous
      // STMPaxos2PC to be totally done before the next, so we should never get
      // mismatching QueryId's here.
      debug_assert_eq!(&es.query_id, query_id);
      Some(es)
    } else {
      // Similarly, if there is no running ShardSplit, no other ESs should be here.
      match self {
        DDLES::None => (),
        _ => debug_assert!(false),
      }
      None
    }
  }

  fn insert(&mut self, _: QueryId, es: ShardSplitTabletRMES) {
    *self = DDLES::ShardSplit(es);
  }
}

// -----------------------------------------------------------------------------------------------
//  Region Isolation Algorithm
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestedReadProtected {
  pub query_id: QueryId,
  pub read_region: ReadRegion,
  pub orig_p: OrigP,
}

#[derive(Debug, Clone)]
pub struct VerifyingReadWriteRegion {
  pub orig_p: OrigP,
  pub m_waiting_read_protected: BTreeSet<RequestedReadProtected>,
  pub m_read_protected: BTreeSet<ReadRegion>,
  pub m_write_protected: BTreeSet<WriteRegion>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ReadWriteRegion {
  pub orig_p: OrigP,
  pub m_read_protected: BTreeSet<ReadRegion>,
  pub m_write_protected: BTreeSet<WriteRegion>,
}

// -----------------------------------------------------------------------------------------------
//  Schema Change and Locking
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ColSet {
  Cols(Vec<ColName>),
  All,
}

impl ColSet {
  pub fn contains(&self, col: &ColName) -> bool {
    match &self {
      ColSet::Cols(cols) => cols.contains(col),
      ColSet::All => true,
    }
  }
}

#[derive(Debug, Clone)]
pub struct RequestedLockedCols {
  pub query_id: QueryId,
  pub timestamp: Timestamp,
  pub cols: ColSet,
  pub orig_p: OrigP,
}

// -----------------------------------------------------------------------------------------------
//  StorageLocalTable
// -----------------------------------------------------------------------------------------------

pub struct StorageLocalTable<'a, StorageViewT: StorageView> {
  table_schema: &'a TableSchema,
  /// The Timestamp which we are reading data at.
  timestamp: &'a Timestamp,
  /// The `GeneralSource` in the Data Source of the Query.
  source: &'a proc::TableSource,
  /// The `TabletKeyRange` of the Tablet. Recall that the underlying storage might temporarily
  /// have keys outside of this range during the Sharding process.
  tablet_key_range: &'a TabletKeyRange,
  /// The row-filtering expression (i.e. WHERE clause) to compute subtables with.
  selection: &'a proc::ValExpr,
  /// This is used to compute the KeyBound
  storage: StorageViewT,
  /// A flattened view of the Table schema
  schema: Vec<Option<ColName>>,
}

impl<'a, StorageViewT: StorageView> StorageLocalTable<'a, StorageViewT> {
  pub fn new(
    table_schema: &'a TableSchema,
    timestamp: &'a Timestamp,
    source: &'a proc::TableSource,
    tablet_key_range: &'a TabletKeyRange,
    selection: &'a proc::ValExpr,
    storage: StorageViewT,
  ) -> StorageLocalTable<'a, StorageViewT> {
    let schema = table_schema.get_schema_static(timestamp).into_iter().map(|c| Some(c)).collect();
    StorageLocalTable {
      table_schema,
      source,
      timestamp,
      selection,
      tablet_key_range,
      storage,
      schema,
    }
  }
}

impl<'a, StorageViewT: StorageView> LocalTable for StorageLocalTable<'a, StorageViewT> {
  fn source_name(&self) -> &String {
    &self.source.alias
  }

  fn schema(&self) -> &Vec<Option<ColName>> {
    &self.schema
  }

  fn get_rows(
    &self,
    parent_context_schema: &ContextSchema,
    parent_context_row: &ContextRow,
    local_col_refs: &Vec<LocalColumnRef>,
  ) -> Vec<(Vec<ColValN>, u64)> {
    // Evaluate the `local_col_refs` to the `ColName`s in the Table.
    let mut col_names = Vec::<ColName>::new();
    for col_ref in local_col_refs {
      match col_ref {
        LocalColumnRef::Named(col_name) => col_names.push(col_name.clone()),
        LocalColumnRef::Unnamed(index) => {
          let col_name = self.schema.get(*index).unwrap().as_ref().unwrap();
          col_names.push(col_name.clone());
        }
      }
    }

    // Recall that since `contains_col_ref` is false for the `parent_context_schema`, this
    // `col_map` passes the precondition of `compute_key_region`.
    let col_map = compute_col_map(parent_context_schema, parent_context_row);
    let key_bounds = range_row_region_intersection(
      &self.table_schema.key_cols,
      &self.tablet_key_range,
      compute_key_region(&self.selection, col_map, &self.source.alias, &self.table_schema.key_cols),
    );
    self.storage.compute_subtable(&key_bounds, &col_names, self.timestamp)
  }
}

/// Constructs a map from the `column_context_schema` to the `column_context_row`.
pub fn compute_col_map(
  parent_context_schema: &ContextSchema,
  parent_context_row: &ContextRow,
) -> BTreeMap<proc::ColumnRef, ColValN> {
  debug_assert_eq!(
    parent_context_schema.column_context_schema.len(),
    parent_context_row.column_context_row.len()
  );
  let mut col_map = BTreeMap::<proc::ColumnRef, ColValN>::new();
  let context_col_names = &parent_context_schema.column_context_schema;
  let context_col_vals = &parent_context_row.column_context_row;
  for i in 0..context_col_names.len() {
    let context_col_name = context_col_names.get(i).unwrap().clone();
    let context_col_val = context_col_vals.get(i).unwrap().clone();
    col_map.insert(context_col_name, context_col_val);
  }
  col_map
}

// -----------------------------------------------------------------------------------------------
//  TabletBundle
// -----------------------------------------------------------------------------------------------

pub mod plm {
  use crate::common::{CQueryPath, TQueryPath};
  use crate::common::{ColName, QueryId};
  use crate::common::{ReadRegion, Timestamp};
  use crate::storage::GenericTable;
  use crate::tablet::{ColSet, ReadWriteRegion};
  use serde::{Deserialize, Serialize};

  // LockedCols

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct LockedCols {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
    pub cols: ColSet,
  }

  // ReadProtected

  #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
  pub struct ReadProtected {
    pub query_id: QueryId,
    pub timestamp: Timestamp,
    pub region: ReadRegion,
  }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TabletPLm {
  LockedCols(plm::LockedCols),
  ReadProtected(plm::ReadProtected),
  FinishQuery(paxos2pc_tm::RMPLm<FinishQueryPayloadTypes>),
  AlterTable(stmpaxos2pc_rm::RMPLm<AlterTableRMPayloadTypes>),
  DropTable(stmpaxos2pc_rm::RMPLm<DropTableRMPayloadTypes>),
  ShardSplit(stmpaxos2pc_rm::RMPLm<ShardSplitTabletRMPayloadTypes>),
  ShardingConfirmedPLm(ShardingConfirmedPLm),
}

// -----------------------------------------------------------------------------------------------
//  TabletForwardMsg
// -----------------------------------------------------------------------------------------------

pub type TabletBundle = Vec<TabletPLm>;

#[derive(Debug)]
pub enum TabletForwardMsg {
  TabletBundle(TabletBundle),
  TabletMessage(msg::TabletMessage),
  GossipData(Arc<GossipData>, LeaderMap),
  RemoteLeaderChanged(RemoteLeaderChangedPLm),
  LeaderChanged(msg::LeaderChanged),
  ConstructTabletSnapshot,
}

// -----------------------------------------------------------------------------------------------
//  RMServerContext FinishQuery
// -----------------------------------------------------------------------------------------------

impl paxos2pc_tm::RMServerContext<FinishQueryPayloadTypes> for TabletContext {
  fn push_plm(&mut self, plm: TabletPLm) {
    self.tablet_bundle.push(plm);
  }

  fn send_to_tm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    tm: &CNodePath,
    msg: msg::CoordMessage,
  ) {
    self.send_to_c(io_ctx, tm.clone(), msg);
  }

  fn mk_node_path(&self) -> TNodePath {
    TabletContext::mk_node_path(self)
  }

  fn is_leader(&self) -> bool {
    TabletContext::is_leader(self)
  }

  fn leader_map(&self) -> &BTreeMap<PaxosGroupId, LeadershipId> {
    &self.leader_map
  }
}

// -----------------------------------------------------------------------------------------------
//  TabletConfig
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TabletConfig {
  /// This is used for generate the `suffix` of a Timestamp, where we just generate
  /// a random `u64` and take the remainder after dividing by `timestamp_suffix_divisor`.
  /// This cannot be 0; the default value is 1, making the suffix always be 0.
  pub timestamp_suffix_divisor: u64,
}

// -----------------------------------------------------------------------------------------------
//  Server Context
// -----------------------------------------------------------------------------------------------

impl ServerContextBase for TabletContext {
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

impl CTServerContext for TabletContext {
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
//  Tablet State
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct TabletState {
  pub ctx: TabletContext,
  pub statuses: Statuses,
}

#[derive(Debug)]
pub struct TabletContext {
  /// Metadata
  pub tablet_config: TabletConfig,
  pub this_sid: SlaveGroupId,
  pub this_gid: PaxosGroupId,
  pub this_tid: TabletGroupId,
  pub sub_node_path: CTSubNodePath, // Wraps `this_tablet_group_id` for expedience
  pub this_eid: EndpointId,
  pub this_table_path: TablePath,

  /// Gossip
  pub gossip: Arc<GossipData>,

  /// LeaderMap
  pub leader_map: LeaderMap,

  // Sharding.
  /// These are Purely Derived form `ShardSplitTabletRMES`
  pub this_sharding_gen: ShardingGen,
  pub this_tablet_key_range: TabletKeyRange,
  /// This is `true` iff `sharding_state` is `None`. Notice that this is Purely Derived State.
  /// When this is `true`, all `row_regions` across all non-commit `*_writes` and
  /// `*_read_protected` will be within the `this_tablet_key_range`.
  pub sharding_done: bool,

  // Storage
  pub storage: GenericMVTable,

  // Schema
  pub table_schema: TableSchema,
  pub presence_timestamp: Timestamp,

  // Region Isolation Algorithm
  pub verifying_writes: BTreeMap<Timestamp, VerifyingReadWriteRegion>,
  pub inserting_prepared_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub prepared_writes: BTreeMap<Timestamp, ReadWriteRegion>,
  pub committed_writes: BTreeMap<Timestamp, ReadWriteRegion>,

  pub waiting_read_protected: BTreeMap<Timestamp, BTreeSet<RequestedReadProtected>>,
  pub inserting_read_protected: BTreeMap<Timestamp, BTreeSet<RequestedReadProtected>>,
  pub read_protected: BTreeMap<Timestamp, BTreeSet<ReadRegion>>,

  // Schema Change and Locking
  pub waiting_locked_cols: BTreeMap<QueryId, RequestedLockedCols>,
  pub inserting_locked_cols: BTreeMap<QueryId, RequestedLockedCols>,

  /// For every `MSQueryES`, this maps its corresponding `root_query_id` to its `query_id`.
  /// This is for use when a MSTable*ES is constructed. This needs to be updated whenever an
  /// `MSQueryES` is added or removed.
  pub ms_root_query_map: BTreeMap<QueryId, QueryId>,

  // Paxos
  pub tablet_bundle: TabletBundle,
}

impl TabletState {
  pub fn new(ctx: TabletContext) -> TabletState {
    TabletState { ctx, statuses: Default::default() }
  }

  pub fn create_reconfig(
    gossip: Arc<GossipData>,
    snapshot: TabletSnapshot,
    this_eid: EndpointId,
    tablet_config: TabletConfig,
  ) -> TabletState {
    // Create Statuses
    let mut statuses = Statuses::default();
    statuses.finish_query_ess = snapshot.finish_query_ess;
    statuses.ddl_es = snapshot.ddl_es;
    statuses.sharding_state = snapshot.sharding_state;

    // Create the TabletCtx
    let ctx = TabletContext {
      tablet_config,
      this_sid: snapshot.this_sid.clone(),
      this_gid: snapshot.this_sid.to_gid(),
      this_tid: snapshot.this_tid,
      sub_node_path: snapshot.sub_node_path,
      this_eid,
      gossip,
      leader_map: snapshot.leader_map,
      storage: snapshot.storage,
      this_table_path: snapshot.this_table_path,
      this_sharding_gen: snapshot.this_sharding_gen,
      sharding_done: snapshot.sharding_done,
      this_tablet_key_range: snapshot.this_table_key_range,
      table_schema: snapshot.table_schema,
      presence_timestamp: snapshot.presence_timestamp,
      verifying_writes: Default::default(),
      inserting_prepared_writes: Default::default(),
      prepared_writes: snapshot.prepared_writes,
      committed_writes: snapshot.committed_writes,
      waiting_read_protected: Default::default(),
      inserting_read_protected: Default::default(),
      read_protected: snapshot.read_protected,
      waiting_locked_cols: Default::default(),
      inserting_locked_cols: Default::default(),
      ms_root_query_map: Default::default(),
      tablet_bundle: Default::default(),
    };

    TabletState { ctx, statuses }
  }

  pub fn handle_input<IO: CoreIOCtx>(&mut self, io_ctx: &mut IO, coord_input: TabletForwardMsg) {
    self.ctx.handle_input(io_ctx, &mut self.statuses, coord_input);
  }
}

impl TabletContext {
  fn handle_input<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    tablet_input: TabletForwardMsg,
  ) {
    match tablet_input {
      TabletForwardMsg::TabletBundle(bundle) => {
        for paxos_log_msg in bundle {
          match paxos_log_msg {
            TabletPLm::LockedCols(locked_cols) => {
              // Increase TableSchema LATs
              match &locked_cols.cols {
                ColSet::Cols(cols) => {
                  for col_name in cols {
                    self.table_schema.val_cols.update_lat(col_name, locked_cols.timestamp.clone());
                  }
                }
                ColSet::All => {
                  self.table_schema.val_cols.update_all_lats(locked_cols.timestamp.clone());
                }
              }

              self.presence_timestamp = max(self.presence_timestamp.clone(), locked_cols.timestamp);

              if self.is_leader() {
                // Remove RequestedLockedCols and grant GlobalLockedCols
                let req = self.inserting_locked_cols.remove(&locked_cols.query_id).unwrap();
                self.grant_global_locked_cols(io_ctx, statuses, req.orig_p, req.query_id);
              }
            }
            TabletPLm::ReadProtected(read_protected) => {
              btree_multimap_insert(
                &mut self.read_protected,
                &read_protected.timestamp,
                read_protected.region,
              );

              if self.is_leader() {
                // Remove the RequestedReadProtected and grang GlobalReadProtected
                let req = self
                  .remove_inserting_read_protected_request(
                    &read_protected.timestamp,
                    &read_protected.query_id,
                  )
                  .unwrap();
                self.grant_global_read_protected(io_ctx, statuses, req);
              }
            }
            // FinishQuery
            TabletPLm::FinishQuery(plm) => {
              let (query_id, action) =
                paxos2pc_rm::handle_rm_plm(self, io_ctx, &mut statuses.finish_query_ess, plm);
              self.handle_finish_query_es_action(statuses, query_id, action);
            }
            // AlterTable
            TabletPLm::AlterTable(plm) => {
              let (query_id, action) =
                stmpaxos2pc_rm::handle_rm_plm(self, io_ctx, &mut statuses.ddl_es, plm);
              self.handle_alter_table_es_action(statuses, query_id, action);
            }
            // DropTable
            TabletPLm::DropTable(plm) => {
              let (query_id, action) =
                stmpaxos2pc_rm::handle_rm_plm(self, io_ctx, &mut statuses.ddl_es, plm);
              self.handle_drop_table_es_action(statuses, query_id, action);
            }
            // ShardSplit
            TabletPLm::ShardSplit(plm) => {
              let (query_id, action) =
                stmpaxos2pc_rm::handle_rm_plm(self, io_ctx, &mut statuses.ddl_es, plm);
              self.handle_shard_split_es_action(io_ctx, statuses, query_id, action);
            }
            // ShardingSnapshotES
            TabletPLm::ShardingConfirmedPLm(plm) => match &mut statuses.sharding_state {
              ShardingState::None => {}
              ShardingState::ShardingSnapshotES(es) => {
                let action = es.handle_plm(self, plm);
                let query_id = es.query_id.clone();
                self.handle_shard_send_es_action(statuses, query_id, action);
              }
            },
          }
        }

        if self.is_leader() {
          // Run the Main Loop
          self.run_main_loop(io_ctx, statuses);

          // Inform all ESs in WaitingInserting and start inserting a PLm.
          for (_, es) in &mut statuses.finish_query_ess {
            es.start_inserting(self, io_ctx);
          }
          match &mut statuses.ddl_es {
            DDLES::None => {}
            DDLES::Alter(es) => {
              es.start_inserting(self, io_ctx);
            }
            DDLES::Drop(es) => {
              es.start_inserting(self, io_ctx);
            }
            DDLES::Dropped(_) => {}
            DDLES::ShardSplit(es) => {
              es.start_inserting(self, io_ctx);
            }
          }

          // Inform the ShardingSnapshotES that it might be able to advance.
          match &mut statuses.sharding_state {
            ShardingState::None => {}
            ShardingState::ShardingSnapshotES(es) => {
              let action = es.handle_bundle_processed(self, io_ctx, &statuses.finish_query_ess);
              let query_id = es.query_id.clone();
              self.handle_shard_send_es_action(statuses, query_id, action);
            }
          }

          // Dispatch the TabletBundle for insertion and start a new one.
          io_ctx.slave_forward(SlaveBackMessage::TabletBundleInsertion(TabletBundleInsertion {
            tid: self.this_tid.clone(),
            lid: self.leader_map.get(&self.this_sid.to_gid()).unwrap().clone(),
            bundle: std::mem::replace(&mut self.tablet_bundle, Vec::default()),
          }));
        }
      }
      TabletForwardMsg::TabletMessage(message) => {
        match message {
          msg::TabletMessage::PerformQuery(perform_query) => {
            // If the `root_sid` is in the GossipData, then we move forward. Otherwise, we wait.
            // This is needed for the `RegisterQuery` that we do with MSTable*ESs.
            let root_sid = &perform_query.root_query_path.node_path.sid;
            if self.gossip.get().slave_address_config.contains_key(root_sid) {
              self.handle_perform_query(io_ctx, statuses, perform_query);
            } else {
              // Request a GossipData from the Master to help stimulate progress.
              let sender_path = self.this_sid.clone();
              self.send_to_master(
                io_ctx,
                msg::MasterRemotePayload::MasterGossipRequest(msg::MasterGossipRequest {
                  sender_path,
                }),
              );

              // Buffer the PerformQuery
              let query_id = perform_query.query_id.clone();
              statuses.perform_query_buffer.insert(query_id, perform_query);
            }
          }
          msg::TabletMessage::CancelQuery(cancel_query) => {
            self.exit_and_clean_up(io_ctx, statuses, cancel_query.query_id);
          }
          msg::TabletMessage::QueryAborted(query_aborted) => {
            self.handle_query_aborted(io_ctx, statuses, query_aborted);
          }
          msg::TabletMessage::QuerySuccess(query_success) => {
            self.handle_query_success(io_ctx, statuses, query_success);
          }
          msg::TabletMessage::FinishQuery(message) => {
            let (query_id, action) = paxos2pc_rm::handle_rm_msg(
              self,
              io_ctx,
              &mut statuses.finish_query_ess,
              &mut statuses.ms_query_ess,
              message,
            );
            self.handle_finish_query_es_action(statuses, query_id, action);
          }
          msg::TabletMessage::AlterTable(message) => {
            let (query_id, action) =
              stmpaxos2pc_rm::handle_rm_msg(self, io_ctx, &mut statuses.ddl_es, message);
            self.handle_alter_table_es_action(statuses, query_id, action);
          }
          msg::TabletMessage::DropTable(message) => {
            let (query_id, action) =
              stmpaxos2pc_rm::handle_rm_msg(self, io_ctx, &mut statuses.ddl_es, message);
            self.handle_drop_table_es_action(statuses, query_id, action);
          }
          msg::TabletMessage::ShardSplit(message) => {
            let (query_id, action) =
              stmpaxos2pc_rm::handle_rm_msg(self, io_ctx, &mut statuses.ddl_es, message);
            self.handle_shard_split_es_action(io_ctx, statuses, query_id, action);
          }
          msg::TabletMessage::ShardingConfirmed(message) => match &mut statuses.sharding_state {
            ShardingState::None => {}
            ShardingState::ShardingSnapshotES(es) => {
              let action = es.handle_msg(self, message);
              let query_id = es.query_id.clone();
              self.handle_shard_send_es_action(statuses, query_id, action);
            }
          },
        }

        // Run Main Loop
        self.run_main_loop(io_ctx, statuses);
      }
      TabletForwardMsg::GossipData(gossip, some_leader_map) => {
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

          // Process Bufferred PerformQuerys
          let query_ids: Vec<QueryId> = statuses.perform_query_buffer.keys().cloned().collect();
          for query_id in query_ids {
            // Check if we can move the PerformQuery forward.
            let perform_query = statuses.perform_query_buffer.get(&query_id).unwrap();
            let root_sid = &perform_query.root_query_path.node_path.sid;
            if self.gossip.get().slave_address_config.contains_key(root_sid) {
              // If so, then move it forward.
              let perform_query = statuses.perform_query_buffer.remove(&query_id).unwrap();
              self.handle_perform_query(io_ctx, statuses, perform_query);
            }
          }

          // Inform Top-Level ESs.
          struct Cb;
          impl CallbackWithContext<()> for Cb {
            fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
              ctx: &mut TabletContext,
              io_ctx: &mut IOCtx,
              es: &mut TPEST,
              es_ctx: &mut TPEST::ESContext,
              _: &(),
            ) -> TabletAction {
              TabletAction::TPESAction(es.gossip_data_changed(ctx, io_ctx, es_ctx))
            }
          }

          statuses.execute_all_ctx::<_, _, Cb>(self, io_ctx, &());

          // Run Main Loop
          self.run_main_loop(io_ctx, statuses);
        }
      }
      TabletForwardMsg::RemoteLeaderChanged(remote_leader_changed) => {
        let gid = remote_leader_changed.gid.clone();
        let lid = remote_leader_changed.lid.clone();

        // Only accept a RemoteLeaderChanged if the PaxosGroupId is already known.
        if let Some(cur_lid) = self.leader_map.get(&gid) {
          // Only update the LeadershipId if the new one increases the old one.
          if lid.gen > cur_lid.gen {
            self.leader_map.insert(gid.clone(), lid.clone());
            if let PaxosGroupId::Slave(sid) = gid {
              // For Top-Level ESs, if the sending PaxosGroup's Leadership changed, we ECU (no
              // response). Note that although it is not critical for avoiding resource leaks, it
              // means we only end up responding to the PaxosNode that sent the request (not a
              // random subsequent one).

              let query_ids: Vec<QueryId> = statuses.perform_query_buffer.keys().cloned().collect();
              for query_id in query_ids {
                let perform_query = statuses.perform_query_buffer.get(&query_id).unwrap();
                if perform_query.sender_path.node_path.sid == sid {
                  self.exit_and_clean_up(io_ctx, statuses, query_id);
                }
              }

              // Inform Top-Level ESs.
              struct Cb;
              impl Callback<SlaveGroupId> for Cb {
                fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
                  _: &mut TabletContext,
                  _: &mut IOCtx,
                  es: &mut TPEST,
                  sid: &SlaveGroupId,
                ) -> TabletAction {
                  if es.sender_sid() == sid {
                    TabletAction::ExitAndCleanUp(es.query_id().clone())
                  } else {
                    TabletAction::Wait
                  }
                }
              }

              statuses.execute_all::<_, _, Cb>(self, io_ctx, &sid);

              // Inform TMStatus
              let query_ids: Vec<QueryId> = statuses.tm_statuss.keys().cloned().collect();
              for query_id in query_ids {
                let tm_status = statuses.tm_statuss.get_mut(&query_id).unwrap();
                if let Some(cur_lid) = tm_status.leaderships.get(&sid) {
                  if cur_lid < &lid {
                    // The new Leadership of a remote slave has changed beyond what the TMStatus
                    // had contacted, so that RM will surely not respond. Thus we abort this
                    // whole TMStatus and inform the GRQueryES so that it can retry the stage.
                    let gr_query_id = tm_status.orig_p.query_id.clone();
                    self.exit_and_clean_up(io_ctx, statuses, query_id.clone());

                    // Inform the GRQueryES
                    let gr_query = statuses.gr_query_ess.get_mut(&gr_query_id).unwrap();
                    remove_item(&mut gr_query.child_queries, &query_id);
                    let action = gr_query.es.handle_tm_remote_leadership_changed(self, io_ctx);
                    self.handle_gr_query_es_action(io_ctx, statuses, gr_query_id, action);
                  }
                }
              }

              // Inform MSQueryES
              let query_ids: Vec<QueryId> = statuses.ms_query_ess.keys().cloned().collect();
              for query_id in query_ids {
                let ms_query_es = statuses.ms_query_ess.get_mut(&query_id).unwrap();
                let root_sid = &ms_query_es.root_query_path.node_path.sid;
                if root_sid == &sid && ms_query_es.root_lid < lid {
                  // Here, the root PaxosNode is dead, so we simply ECU the MSQueryES.
                  self.exit_and_clean_up(io_ctx, statuses, query_id);
                }
              }

              // Inform FinishQueryRMES
              let query_ids: Vec<QueryId> = statuses.finish_query_ess.keys().cloned().collect();
              for query_id in query_ids {
                let finish_query_es = statuses.finish_query_ess.get_mut(&query_id).unwrap();
                let action = finish_query_es.remote_leader_changed(
                  self,
                  io_ctx,
                  remote_leader_changed.clone(),
                );
                self.handle_finish_query_es_action(statuses, query_id.clone(), action);
              }

              // Inform ShardingState
              match &mut statuses.sharding_state {
                ShardingState::None => {}
                ShardingState::ShardingSnapshotES(es) => {
                  let action = es.handle_rlc(self, io_ctx, remote_leader_changed.clone());
                  let query_id = es.query_id.clone();
                  self.handle_shard_send_es_action(statuses, query_id, action);
                }
              }

              // Run Main Loop
              self.run_main_loop(io_ctx, statuses);
            }
          }
        }
      }
      TabletForwardMsg::LeaderChanged(leader_changed) => {
        let this_gid = self.this_sid.to_gid();
        self.leader_map.insert(this_gid, leader_changed.lid); // Update the LeadershipId

        if self.is_leader() {
          // By the SharedPaxosInserter, this must be empty at the start of Leadership.
          self.tablet_bundle = TabletBundle::default();
        }

        // Inform FinishQueryRMES
        let query_ids: Vec<QueryId> = statuses.finish_query_ess.keys().cloned().collect();
        for query_id in query_ids {
          let finish_query_es = statuses.finish_query_ess.get_mut(&query_id).unwrap();
          let action = finish_query_es.leader_changed(self, io_ctx);
          self.handle_finish_query_es_action(statuses, query_id.clone(), action);
        }

        // Inform DDLESs
        match &mut statuses.ddl_es {
          DDLES::None => {}
          DDLES::Alter(es) => {
            es.leader_changed(self);
          }
          DDLES::Drop(es) => {
            es.leader_changed(self);
          }
          DDLES::Dropped(_) => {}
          DDLES::ShardSplit(es) => {
            es.leader_changed(self);
          }
        }

        // Inform ShardingState
        match &mut statuses.sharding_state {
          ShardingState::None => {}
          ShardingState::ShardingSnapshotES(es) => {
            let action = es.handle_lc(self, io_ctx, &statuses.finish_query_ess);
            let query_id = es.query_id.clone();
            self.handle_shard_send_es_action(statuses, query_id, action);
          }
        }

        // Check if this node just lost Leadership
        if !self.is_leader() {
          // Wink away all TM ESs.
          statuses.perform_query_buffer.clear();
          statuses.gr_query_ess.clear();
          statuses.tm_statuss.clear();
          statuses.ms_query_ess.clear();
          statuses.top.table_read_ess.clear();
          statuses.top.trans_table_read_ess.clear();
          statuses.top.ms_table_read_ess.clear();
          statuses.top.ms_table_write_ess.clear();
          statuses.top.ms_table_insert_ess.clear();
          statuses.top.ms_table_delete_ess.clear();

          // Wink away all unpersisted Region Isolation Algorithm data
          self.verifying_writes.clear();
          self.inserting_prepared_writes.clear();
          self.waiting_read_protected.clear();
          self.inserting_read_protected.clear();

          // Wink away all unpersisted Column Locking Algorithm data
          self.waiting_locked_cols.clear();
          self.inserting_locked_cols.clear();

          // Wink away root query map
          self.ms_root_query_map.clear();
        } else {
          // TODO: should we be running the main loop here?
          // Run Main Loop
          self.run_main_loop(io_ctx, statuses);

          // If this node becomes the Leader, then we continue the insert cycle.
          io_ctx.slave_forward(SlaveBackMessage::TabletBundleInsertion(TabletBundleInsertion {
            tid: self.this_tid.clone(),
            lid: self.leader_map.get(&self.this_sid.to_gid()).unwrap().clone(),
            bundle: std::mem::replace(&mut self.tablet_bundle, TabletBundle::default()),
          }));
        }
      }
      TabletForwardMsg::ConstructTabletSnapshot => {
        let mut snapshot = TabletSnapshot {
          this_sid: self.this_sid.clone(),
          this_tid: self.this_tid.clone(),
          sub_node_path: self.sub_node_path.clone(),
          leader_map: self.leader_map.clone(),
          storage: self.storage.clone(),
          this_table_path: self.this_table_path.clone(),
          table_schema: self.table_schema.clone(),
          presence_timestamp: self.presence_timestamp.clone(),
          this_sharding_gen: self.this_sharding_gen.clone(),
          this_table_key_range: self.this_tablet_key_range.clone(),
          sharding_done: self.sharding_done.clone(),
          prepared_writes: self.prepared_writes.clone(),
          committed_writes: self.committed_writes.clone(),
          read_protected: self.read_protected.clone(),
          finish_query_ess: Default::default(),
          ddl_es: DDLES::None,
          sharding_state: ShardingState::None,
        };

        // Add in the FinishQueryRMES that have at least been Prepared.
        for (qid, es) in &statuses.finish_query_ess {
          if let Some(es) = es.reconfig_snapshot() {
            snapshot.finish_query_ess.insert(qid.clone(), es);
          }
        }

        // Only use a DDLES if it has been Prepared.
        snapshot.ddl_es = match &statuses.ddl_es {
          DDLES::None => DDLES::None,
          DDLES::Alter(es) => {
            if let Some(es) = es.reconfig_snapshot() {
              DDLES::Alter(es)
            } else {
              DDLES::None
            }
          }
          DDLES::Drop(es) => {
            if let Some(es) = es.reconfig_snapshot() {
              DDLES::Drop(es)
            } else {
              DDLES::None
            }
          }
          DDLES::Dropped(timestamp) => DDLES::Dropped(timestamp.clone()),
          DDLES::ShardSplit(es) => {
            if let Some(es) = es.reconfig_snapshot() {
              DDLES::ShardSplit(es)
            } else {
              DDLES::None
            }
          }
        };

        // Created ShardingState Reconfig Snapshot
        snapshot.sharding_state = match &statuses.sharding_state {
          ShardingState::None => ShardingState::None,
          ShardingState::ShardingSnapshotES(es) => {
            ShardingState::ShardingSnapshotES(es.reconfig_snapshot())
          }
        };

        io_ctx.slave_forward(SlaveBackMessage::TabletSnapshot(snapshot));
      }
    }
  }

  /// Handle `PerformQuery` accordingly. Note that the `root_query_path`'s PaxosGroupId
  /// must be in the `gossip` by this point.
  fn handle_perform_query<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    perform_query: msg::PerformQuery,
  ) {
    // If the `ShardingGen` is behind, then we abort.
    let query_plan = perform_query.get_query_plan();
    let (_, sharding_gen) = query_plan.table_location_map.get(&self.this_table_path).unwrap();
    if sharding_gen < &self.this_sharding_gen {
      self.send_query_error(
        io_ctx,
        perform_query.sender_path,
        perform_query.query_id,
        msg::QueryError::InvalidQueryPlan,
      );
      return;
    }

    // Otherwise, we may process the PerformQuery
    match perform_query.query {
      msg::GeneralQuery::TransTableSelectQuery(query) => {
        // First, we check if the GRQueryES still exists in the Statuses, continuing
        // if so and aborting if not.
        if let Some(gr_query) = statuses.gr_query_ess.get(&query.location_prefix.source.query_id) {
          // Construct and start the TransQueryPlanningES
          let trans_table = map_insert(
            &mut statuses.top.trans_table_read_ess,
            &perform_query.query_id,
            TransTableReadES {
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
          );

          let action = trans_table.start(self, io_ctx, &gr_query.es);
          self.handle_tp_es_action(io_ctx, statuses, perform_query.query_id, action);
        } else {
          // This means that the target GRQueryES was deleted, so we send back
          // an Abort with LateralError.
          self.send_query_error(
            io_ctx,
            perform_query.sender_path,
            perform_query.query_id,
            msg::QueryError::LateralError,
          );
          return;
        }
      }
      msg::GeneralQuery::TableSelectQuery(query) => {
        // We inspect the TierMap to see what kind of ES to create
        let table_path = &query.sql_query.from.table_path;
        if query.query_plan.tier_map.map.contains_key(table_path) {
          self.start_ms_table_es(
            io_ctx,
            statuses,
            perform_query.root_query_path,
            perform_query.sender_path,
            perform_query.query_id,
            query.timestamp,
            query.context,
            query.query_plan,
            SelectInner::new(query.sql_query),
          );
        } else {
          // Here, we create a standard TableReadES.
          let read = map_insert(
            &mut statuses.top.table_read_ess,
            &perform_query.query_id,
            TableReadES {
              root_query_path: perform_query.root_query_path,
              timestamp: query.timestamp,
              context: Rc::new(query.context),
              sender_path: perform_query.sender_path.clone(),
              query_id: perform_query.query_id.clone(),
              sql_query: query.sql_query,
              query_plan: query.query_plan,
              new_rms: Default::default(),
              waiting_global_locks: Default::default(),
              state: ExecutionS::Start,
              child_queries: vec![],
            },
          );
          let action = read.start(self, io_ctx, &mut ());
          self.handle_tp_es_action(io_ctx, statuses, perform_query.query_id, action);
        }
      }
      msg::GeneralQuery::UpdateQuery(query) => {
        self.start_ms_table_es(
          io_ctx,
          statuses,
          perform_query.root_query_path,
          perform_query.sender_path,
          perform_query.query_id,
          query.timestamp,
          query.context,
          query.query_plan,
          UpdateInner::new(query.sql_query),
        );
      }
      msg::GeneralQuery::InsertQuery(query) => {
        self.start_ms_table_es(
          io_ctx,
          statuses,
          perform_query.root_query_path,
          perform_query.sender_path,
          perform_query.query_id,
          query.timestamp,
          query.context,
          query.query_plan,
          InsertInner::new(query.sql_query),
        );
      }
      msg::GeneralQuery::DeleteQuery(query) => {
        self.start_ms_table_es(
          io_ctx,
          statuses,
          perform_query.root_query_path,
          perform_query.sender_path,
          perform_query.query_id,
          query.timestamp,
          query.context,
          query.query_plan,
          DeleteInner::new(query.sql_query),
        );
      }
    }
  }

  fn start_ms_table_es<IO: CoreIOCtx, InnerT: SqlQueryInner>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    root_query_path: CQueryPath,
    sender_path: CTQueryPath,
    query_id: QueryId,
    timestamp: Timestamp,
    context: Context,
    query_plan: QueryPlan,
    inner: InnerT,
  ) where
    MSTableES<InnerT>: ESContGetter,
    MSTableES<InnerT>: TPESBase<ESContext = MSQueryES>,
  {
    match self.get_msquery_id(
      io_ctx,
      statuses,
      root_query_path.clone(),
      timestamp.clone(),
      &query_plan,
    ) {
      Ok(ms_query_id) => {
        // Lookup the MSQueryES and add the new Query into `pending_queries`.
        let ms_query_es = statuses.ms_query_ess.get_mut(&ms_query_id).unwrap();
        ms_query_es.pending_queries.insert(query_id.clone());
        let ms_query_path =
          TQueryPath { node_path: self.mk_node_path(), query_id: ms_query_id.clone() };

        // Lookup the Tier
        let tier = query_plan.tier_map.map.get(inner.table_path()).unwrap().clone();

        // Create an MSTableES in the `Start` state and add it into the container.
        let ms_es = map_insert(
          MSTableES::<InnerT>::mut_ess(&mut statuses.top),
          &query_id,
          MSTableES::<InnerT> {
            sender_path: sender_path.clone(),
            child_queries: vec![],
            general: GeneralQueryES {
              root_query_path,
              timestamp,
              tier,
              context: Rc::new(context),
              query_id: query_id.clone(),
              query_plan,
              ms_query_id,
              new_rms: vec![ms_query_path].into_iter().collect(),
            },
            state: MSTableExecutionS::Start,
            inner,
          },
        );
        let action = ms_es.start(self, io_ctx, ms_query_es);
        self.handle_tp_es_action(io_ctx, statuses, query_id, action);
      }
      Err(query_error) => {
        // The MSTableES could not be constructed.
        self.send_query_error(io_ctx, sender_path, query_id, query_error);
      }
    }
  }

  /// Checks if the MSQueryES for `root_query_id` already exists, returning its
  /// `QueryId` if so. If not, we create an MSQueryES (populating `verifying_writes`,
  /// sending `RegisterQuery`, etc).
  fn get_msquery_id<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    root_query_path: CQueryPath,
    timestamp: Timestamp,
    query_plan: &QueryPlan,
  ) -> Result<QueryId, msg::QueryError> {
    let root_query_id = root_query_path.query_id.clone();
    if let Some(ms_query_id) = self.ms_root_query_map.get(&root_query_id) {
      // Here, the MSQueryES already exists, so we just return it's QueryId.
      return Ok(ms_query_id.clone());
    }

    // Otherwise, we need to create one. First check whether the Timestamp is available or not.
    if self.verifying_writes.contains_key(&timestamp)
      || self.inserting_prepared_writes.contains_key(&timestamp)
      || self.prepared_writes.contains_key(&timestamp)
      || self.committed_writes.contains_key(&timestamp)
    {
      // This means the Timestamp is already in use, so we return an error.
      return Err(msg::QueryError::TimestampConflict);
    }

    let ms_query_id = mk_qid(io_ctx.rand());

    // We check that the original Leadership of root_query_path is not dead,
    // returning a QueryError if so.
    let root_sid = root_query_path.node_path.sid.clone();
    let root_lid = query_plan.query_leader_map.get(&root_sid).unwrap().clone();
    if self.leader_map.get(&root_sid.to_gid()).unwrap() > &root_lid {
      return Err(msg::QueryError::InvalidLeadershipId);
    }

    // Otherwise, send a register message back to the root.
    let ms_query_path = self.mk_query_path(ms_query_id.clone());
    self.send_to_c_lid(
      io_ctx,
      root_query_path.node_path.clone(),
      msg::CoordMessage::RegisterQuery(msg::RegisterQuery {
        root_query_id: root_query_id.clone(),
        query_path: ms_query_path,
      }),
      root_lid.clone(),
    );

    // This means that we can add an MSQueryES at the Timestamp
    let full_gen = query_plan.table_location_map.get(&self.this_table_path).unwrap();
    let (_, sharding_gen) = full_gen.clone();
    statuses.ms_query_ess.insert(
      ms_query_id.clone(),
      MSQueryES {
        root_query_path,
        sharding_gen,
        root_lid,
        query_id: ms_query_id.clone(),
        timestamp: timestamp.clone(),
        update_views: Default::default(),
        pending_queries: Default::default(),
      },
    );

    // We also amend the `ms_root_query_map` to associate the root query.
    self.ms_root_query_map.insert(root_query_id.clone(), ms_query_id.clone());

    // Finally, add an empty VerifyingReadWriteRegion
    self.verifying_writes.insert(
      timestamp,
      VerifyingReadWriteRegion {
        orig_p: OrigP::new(ms_query_id.clone()),
        m_waiting_read_protected: BTreeSet::new(),
        m_read_protected: BTreeSet::new(),
        m_write_protected: BTreeSet::new(),
      },
    );

    Ok(ms_query_id)
  }

  /// Adds the following triple into `waiting_locked_cols`. Here, `orig_p` is the origiator
  /// who should get the locking result.
  pub fn add_requested_locked_columns<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    orig_p: OrigP,
    timestamp: Timestamp,
    cols: ColSet,
  ) -> QueryId {
    let locked_cols_qid = mk_qid(io_ctx.rand());
    self.waiting_locked_cols.insert(
      locked_cols_qid.clone(),
      RequestedLockedCols { query_id: locked_cols_qid.clone(), timestamp, cols, orig_p },
    );
    locked_cols_qid
  }

  /// This removes the Read Protection request from `waiting_read_protected` with the given
  /// `query_id` at the given `timestamp`, if it exists, and returns it.
  pub fn remove_read_protected_request(
    &mut self,
    timestamp: &Timestamp,
    query_id: &QueryId,
  ) -> Option<RequestedReadProtected> {
    if let Some(waiting) = self.waiting_read_protected.get_mut(&timestamp) {
      for protect_request in waiting.iter() {
        if &protect_request.query_id == query_id {
          // Here, we found a request with matching QueryId, so we remove it.
          let protect_request = protect_request.clone();
          waiting.remove(&protect_request);
          if waiting.is_empty() {
            self.waiting_read_protected.remove(&timestamp);
          }
          return Some(protect_request);
        }
      }
    }
    return None;
  }

  /// This removes the Read Protection request from `inserting_read_protected` with the given
  /// `query_id` at the given `timestamp`, if it exists, and returns it.
  pub fn remove_inserting_read_protected_request(
    &mut self,
    timestamp: &Timestamp,
    query_id: &QueryId,
  ) -> Option<RequestedReadProtected> {
    if let Some(inserting) = self.inserting_read_protected.get_mut(&timestamp) {
      for protect_request in inserting.iter() {
        if &protect_request.query_id == query_id {
          // Here, we found a request with matching QueryId, so we remove it.
          let protect_request = protect_request.clone();
          inserting.remove(&protect_request);
          if inserting.is_empty() {
            self.inserting_read_protected.remove(&timestamp);
          }
          return Some(protect_request);
        }
      }
    }
    return None;
  }

  /// This removes the Read Protection request from `waiting_read_protected` with the given
  /// `query_id` at the given `timestamp`, if it exists, and returns it.
  pub fn remove_m_read_protected_request(
    &mut self,
    timestamp: &Timestamp,
    query_id: &QueryId,
  ) -> Option<RequestedReadProtected> {
    if let Some(verifying_write) = self.verifying_writes.get_mut(timestamp) {
      for protect_request in verifying_write.m_waiting_read_protected.iter() {
        if &protect_request.query_id == query_id {
          // Here, we found a request with matching QueryId, so we remove it.
          let protect_request = protect_request.clone();
          verifying_write.m_waiting_read_protected.remove(&protect_request);
          return Some(protect_request);
        }
      }
    }
    return None;
  }

  /// Checks if the give `write_region` has a Region Isolation with subsequent reads.
  pub fn check_write_region_isolation(
    &self,
    write_region: &WriteRegion,
    timestamp: &Timestamp,
  ) -> bool {
    // We iterate through every subsequent Reads that this `write_region` can conflict
    // with, and check if there is indeed a conflict.

    // First, verify Region Isolation with ReadRegions of subsequent *_writes.
    let bound = (Bound::Excluded(timestamp), Bound::Unbounded);
    for (_, verifying_write) in self.verifying_writes.range(bound) {
      if !is_surely_isolated_multiread(write_region, &verifying_write.m_read_protected) {
        return false;
      }
    }
    for (_, prepared_write) in self.prepared_writes.range(bound) {
      if !is_surely_isolated_multiread(write_region, &prepared_write.m_read_protected) {
        return false;
      }
    }
    for (_, inserting_prepared_write) in self.inserting_prepared_writes.range(bound) {
      if !is_surely_isolated_multiread(write_region, &inserting_prepared_write.m_read_protected) {
        return false;
      }
    }
    for (_, committed_write) in self.committed_writes.range(bound) {
      if !is_surely_isolated_multiread(write_region, &committed_write.m_read_protected) {
        return false;
      }
    }

    // Then, verify Region Isolation with ReadRegions of subsequent Reads.
    let bound = (Bound::Included(timestamp), Bound::Unbounded);
    for (_, read_regions) in self.read_protected.range(bound) {
      if !is_surely_isolated_multiread(write_region, read_regions) {
        return false;
      }
    }
    for (_, inserting_read_regions) in self.inserting_read_protected.range(bound) {
      let mut read_regions = BTreeSet::<ReadRegion>::new();
      for req in inserting_read_regions {
        read_regions.insert(req.read_region.clone());
      }
      if !is_surely_isolated_multiread(write_region, &read_regions) {
        return false;
      }
    }

    // If we get here, it means we have Region Isolation.
    return true;
  }

  // The Main Loop
  fn run_main_loop<IO: CoreIOCtx>(&mut self, io_ctx: &mut IO, statuses: &mut Statuses) {
    while self.run_main_loop_iteration(io_ctx, statuses) {}
  }

  /// Thus runs one iteration of the Main Loop, returning `false` exactly when nothing changes.
  fn run_main_loop_iteration<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
  ) -> bool {
    // First, we see if we can satisfy Column Locking

    // Process `waiting_locked_cols`. This is Tablets a part of the Tablet DDL Algorithm.
    for (_, req) in &self.waiting_locked_cols {
      // First, we see if we can grant GlobalLockedCols immediately.
      let mut global_locked = req.timestamp <= self.presence_timestamp;
      match &req.cols {
        ColSet::Cols(cols) => {
          for col_name in cols {
            if req.timestamp > self.table_schema.val_cols.get_lat(col_name) {
              global_locked = false;
              break;
            }
          }
        }
        ColSet::All => {
          if req.timestamp > self.table_schema.val_cols.get_min_lat() {
            global_locked = false;
          }
        }
      }

      if global_locked {
        let orig_p = req.orig_p.clone();
        let query_id = req.query_id.clone();
        self.waiting_locked_cols.remove(&query_id);
        self.grant_global_locked_cols(io_ctx, statuses, orig_p, query_id);
        return true;
      }

      // Next, we see if we can grant LocalLockedCols. When there is no DDL ES, we can always
      // grant LocalLockedCols. Otherwise, we must verify the `req` does not conflict.
      match &statuses.ddl_es {
        DDLES::None | DDLES::ShardSplit(_) => {
          // Grant LocalLockedCols
          let query_id = req.query_id.clone();
          self.grant_local_locked_cols(io_ctx, statuses, query_id);
          return true;
        }
        DDLES::Alter(es) => {
          if !(req.cols.contains(&es.inner.alter_op.col_name)
            && req.timestamp >= es.inner.prepared_timestamp)
          {
            // Grant LocalLockedCols
            let query_id = req.query_id.clone();
            self.grant_local_locked_cols(io_ctx, statuses, query_id);
            return true;
          }
        }
        DDLES::Drop(es) => {
          if req.timestamp < es.inner.prepared_timestamp {
            // Grant LocalLockedCols
            let query_id = req.query_id.clone();
            self.grant_local_locked_cols(io_ctx, statuses, query_id);
            return true;
          }
        }
        DDLES::Dropped(dropped_timestamp) => {
          if &req.timestamp < dropped_timestamp {
            // Grant LocalLockedCols
            let query_id = req.query_id.clone();
            self.grant_local_locked_cols(io_ctx, statuses, query_id);
          } else {
            // Grant TableDropped
            let orig_p = req.orig_p.clone();
            let query_id = req.query_id.clone();
            self.waiting_locked_cols.remove(&query_id);
            self.grant_table_dropped(io_ctx, statuses, orig_p);
          }
          return true;
        }
      }
    }

    // Next, we see if we can provide Region Protection

    // To account for both `verifying_writes` and `prepared_writes`, we merge them into a
    // single container similar to `verifying_writes`. This should be optimized later.
    let mut all_cur_writes = BTreeMap::<Timestamp, VerifyingReadWriteRegion>::new();
    for (cur_timestamp, verifying_write) in &self.verifying_writes {
      all_cur_writes.insert(cur_timestamp.clone(), verifying_write.clone());
    }
    let write_it = self.inserting_prepared_writes.iter().chain(self.prepared_writes.iter());
    for (cur_timestamp, prepared_write) in write_it {
      assert!(all_cur_writes
        .insert(
          cur_timestamp.clone(),
          VerifyingReadWriteRegion {
            orig_p: prepared_write.orig_p.clone(),
            m_waiting_read_protected: Default::default(),
            m_read_protected: prepared_write.m_read_protected.clone(),
            m_write_protected: prepared_write.m_write_protected.clone(),
          },
        )
        .is_none());
    }

    // First, we see if any `(m_)waiting_read_protected`s can be moved to `(m_)read_protected`.
    if !all_cur_writes.is_empty() {
      let (first_write_timestamp, verifying_write) = all_cur_writes.first_key_value().unwrap();

      // First, process all `waiting_read_protected`s before the `first_write_timestamp`
      let bound = (Bound::Unbounded, Bound::Excluded(first_write_timestamp));
      for (timestamp, set) in self.waiting_read_protected.range(bound) {
        let protect_request = set.first().unwrap().clone();
        self.grant_local_read_protected(io_ctx, statuses, timestamp.clone(), protect_request);
        return true;
      }

      // Next, process all `m_read_protected`s for the first `verifying_write`
      for protect_request in &verifying_write.m_waiting_read_protected {
        self.grant_m_local_read_protected(
          io_ctx,
          statuses,
          first_write_timestamp.clone(),
          protect_request.clone(),
        );
        return true;
      }

      // Next, accumulate the WriteRegions, and then search for region protection with
      // all subsequent `(m_)waiting_read_protected`s.
      let mut cum_write_regions = verifying_write.m_write_protected.clone();
      let mut prev_write_timestamp = first_write_timestamp;
      let bound = (Bound::Excluded(first_write_timestamp), Bound::Unbounded);
      for (cur_timestamp, verifying_write) in all_cur_writes.range(bound) {
        // The loop state is that `cum_write_regions` contains all WriteRegions <=
        // `prev_write_timestamp`, all `m_waiting_read_protected` <= `prev_write_timestamp`
        // have been processed, and all `waiting_read_protected`s < `prev_write_timestamp`
        // have been processed.

        // Process `m_waiting_read_protected`
        for protect_request in &verifying_write.m_waiting_read_protected {
          if is_surely_isolated_multiwrite(&cum_write_regions, &protect_request.read_region) {
            self.grant_m_local_read_protected(
              io_ctx,
              statuses,
              cur_timestamp.clone(),
              protect_request.clone(),
            );
            return true;
          }
        }

        // Process `waiting_read_protected`
        let bound = (Bound::Included(prev_write_timestamp), Bound::Excluded(cur_timestamp));
        for (timestamp, set) in self.waiting_read_protected.range(bound) {
          for protect_request in set {
            if is_surely_isolated_multiwrite(&cum_write_regions, &protect_request.read_region) {
              self.grant_local_read_protected(
                io_ctx,
                statuses,
                timestamp.clone(),
                protect_request.clone(),
              );
              return true;
            }
          }
        }

        // Add the WriteRegions into `cur_write_regions`.
        for write_region in &verifying_write.m_write_protected {
          cum_write_regions.insert(write_region.clone());
        }
        prev_write_timestamp = cur_timestamp;
      }

      // Finally, finish processing any remaining `read_protected`s
      let bound = (Bound::Included(prev_write_timestamp), Bound::Unbounded);
      for (timestamp, set) in self.waiting_read_protected.range(bound) {
        for protect_request in set {
          if is_surely_isolated_multiwrite(&cum_write_regions, &protect_request.read_region) {
            self.grant_local_read_protected(
              io_ctx,
              statuses,
              timestamp.clone(),
              protect_request.clone(),
            );
            return true;
          }
        }
      }
    } else {
      for (timestamp, set) in &self.waiting_read_protected {
        for protect_request in set {
          self.grant_local_read_protected(
            io_ctx,
            statuses,
            timestamp.clone(),
            protect_request.clone(),
          );
          return true;
        }
      }
    }

    // Next, we search for any DeadlockSafetyWriteAbort.
    for (timestamp, set) in &self.waiting_read_protected {
      if let Some(verifying_write) = self.verifying_writes.get(timestamp) {
        for protect_request in set {
          if !is_surely_isolated_multiwrite(
            &verifying_write.m_write_protected,
            &protect_request.read_region,
          ) {
            self.deadlock_safety_write_abort(
              io_ctx,
              statuses,
              verifying_write.orig_p.clone(),
              timestamp.clone(),
            );
            return true;
          }
        }
      }
    }

    return false;
  }

  /// Route the column locking to the appropriate ES. Here, `query_id` is that of
  /// the `waiting_locked_cols` that can be moved forward.
  fn grant_local_locked_cols<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    locked_cols_qid: QueryId,
  ) {
    // Move the RequestedLockedCols to `inserting_*`
    let req = self.waiting_locked_cols.remove(&locked_cols_qid).unwrap();
    self.inserting_locked_cols.insert(locked_cols_qid.clone(), req.clone());
    self.tablet_bundle.push(TabletPLm::LockedCols(plm::LockedCols {
      query_id: req.query_id.clone(),
      timestamp: req.timestamp,
      cols: req.cols,
    }));

    // Inform the ES.
    struct Cb;
    impl CallbackOnce<QueryId> for Cb {
      fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
        ctx: &mut TabletContext,
        io_ctx: &mut IOCtx,
        es: &mut TPEST,
        locked_cols_qid: QueryId,
      ) -> TabletAction {
        TabletAction::TPESAction(es.local_locked_cols(ctx, io_ctx, locked_cols_qid))
      }
    }

    statuses.execute_once::<_, _, Cb>(self, io_ctx, req.orig_p.query_id, locked_cols_qid);
  }

  /// Route the column locking to the appropriate ES.
  fn grant_global_locked_cols<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
    locked_cols_qid: QueryId,
  ) {
    struct Cb;
    impl CallbackOnce<QueryId> for Cb {
      fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
        ctx: &mut TabletContext,
        io_ctx: &mut IOCtx,
        es: &mut TPEST,
        locked_cols_qid: QueryId,
      ) -> TabletAction {
        TabletAction::TPESAction(es.global_locked_cols(ctx, io_ctx, locked_cols_qid))
      }
    }

    statuses.execute_once::<_, _, Cb>(self, io_ctx, orig_p.query_id, locked_cols_qid);
  }

  /// Route the column locking to the appropriate ES.
  fn grant_table_dropped<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
  ) {
    struct Cb;
    impl CallbackOnce<()> for Cb {
      fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
        ctx: &mut TabletContext,
        _: &mut IOCtx,
        es: &mut TPEST,
        _: (),
      ) -> TabletAction {
        TabletAction::TPESAction(es.table_dropped(ctx))
      }
    }

    statuses.execute_once::<_, _, Cb>(self, io_ctx, orig_p.query_id, ());
  }

  /// Move the ProtectRequest in `waiting_read_protected` forward.
  fn grant_local_read_protected<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    timestamp: Timestamp,
    protect_request: RequestedReadProtected,
  ) {
    self.remove_read_protected_request(&timestamp, &protect_request.query_id).unwrap();
    btree_multimap_insert(&mut self.inserting_read_protected, &timestamp, protect_request.clone());
    self.tablet_bundle.push(TabletPLm::ReadProtected(plm::ReadProtected {
      query_id: protect_request.query_id.clone(),
      timestamp,
      region: protect_request.read_region,
    }));

    // Inform the ES.
    struct Cb;
    impl CallbackWithContextOnce<QueryId> for Cb {
      fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
        ctx: &mut TabletContext,
        io_ctx: &mut IOCtx,
        es: &mut TPEST,
        es_ctx: &mut TPEST::ESContext,
        protect_qid: QueryId,
      ) -> TabletAction {
        TabletAction::TPESAction(es.local_read_protected(ctx, io_ctx, es_ctx, protect_qid))
      }
    }

    statuses.execute_once_ctx::<_, _, Cb>(
      self,
      io_ctx,
      protect_request.orig_p.query_id,
      protect_request.query_id,
    );
  }

  /// By here, the `ReadRegion` has been persisted.
  fn grant_global_read_protected<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    req: RequestedReadProtected,
  ) {
    struct Cb;
    impl CallbackOnce<QueryId> for Cb {
      fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
        ctx: &mut TabletContext,
        io_ctx: &mut IOCtx,
        es: &mut TPEST,
        protect_qid: QueryId,
      ) -> TabletAction {
        TabletAction::TPESAction(es.global_read_protected(ctx, io_ctx, protect_qid))
      }
    }

    statuses.execute_once::<_, _, Cb>(self, io_ctx, req.orig_p.query_id, req.query_id);
  }

  /// Move the ProtectRequest in `m_waiting_read_protected` forward.
  fn grant_m_local_read_protected<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    timestamp: Timestamp,
    protect_request: RequestedReadProtected,
  ) {
    let verifying_write = self.verifying_writes.get_mut(&timestamp).unwrap();
    verifying_write.m_waiting_read_protected.remove(&protect_request);
    verifying_write.m_read_protected.insert(protect_request.read_region);

    // Inform the ES.
    struct Cb;
    impl CallbackWithContextOnce<QueryId> for Cb {
      fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
        ctx: &mut TabletContext,
        io_ctx: &mut IOCtx,
        es: &mut TPEST,
        es_ctx: &mut TPEST::ESContext,
        protect_qid: QueryId,
      ) -> TabletAction {
        TabletAction::TPESAction(es.m_local_read_protected(ctx, io_ctx, es_ctx, protect_qid))
      }
    }

    statuses.execute_once_ctx::<_, _, Cb>(
      self,
      io_ctx,
      protect_request.orig_p.query_id,
      protect_request.query_id,
    );
  }

  /// Simply aborts the MSQueryES, which will clean up everything to do with it.
  fn deadlock_safety_write_abort<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
    _: Timestamp,
  ) {
    self.exit_ms_query_es(
      io_ctx,
      statuses,
      orig_p.query_id,
      msg::QueryError::DeadlockSafetyAbortion,
    );
  }

  /// Handles an incoming QuerySuccess message.
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
        let gr_query_id = orig_p.query_id;

        // Then, inform the GRQueryES
        let gr_query = statuses.gr_query_ess.get_mut(&gr_query_id).unwrap();
        remove_item(&mut gr_query.child_queries, &tm_query_id);
        let action = gr_query.es.handle_tm_success(self, io_ctx, tm_query_id, new_rms, results);
        self.handle_gr_query_es_action(io_ctx, statuses, gr_query_id, action);
      }
    }
  }

  /// Handles an incoming QueryAborted message.
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
      let gr_query_id = tm_status.orig_p.query_id.clone();
      self.exit_and_clean_up(io_ctx, statuses, tm_query_id.clone());

      // Then, inform the GRQueryES
      let gr_query = statuses.gr_query_ess.get_mut(&gr_query_id).unwrap();
      remove_item(&mut gr_query.child_queries, tm_query_id);
      let action = gr_query.es.handle_tm_aborted(self, io_ctx, query_aborted.payload);
      self.handle_gr_query_es_action(io_ctx, statuses, gr_query_id, action);
    }
  }

  /// This function processes the result of a GRQueryES. Generally, it finds the ESWrapper
  /// to route the results to, cleans up its `child_queries`, and forwards the result.
  fn handle_gr_query_done<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    subquery_new_rms: BTreeSet<TQueryPath>,
    result: Vec<TableView>,
  ) {
    struct Cb;
    impl CallbackWithContextOnce<(QueryId, BTreeSet<TQueryPath>, Vec<TableView>)> for Cb {
      fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
        ctx: &mut TabletContext,
        io_ctx: &mut IOCtx,
        es: &mut TPEST,
        es_ctx: &mut TPEST::ESContext,
        (subquery_id, subquery_new_rms, result): (QueryId, BTreeSet<TQueryPath>, Vec<TableView>),
      ) -> TabletAction {
        es.remove_subquery(&subquery_id);
        TabletAction::TPESAction(es.handle_subquery_done(
          ctx,
          io_ctx,
          es_ctx,
          subquery_id,
          subquery_new_rms,
          result,
        ))
      }
    }

    statuses.execute_once_ctx::<_, _, Cb>(
      self,
      io_ctx,
      orig_p.query_id,
      (subquery_id, subquery_new_rms, result),
    );
  }

  /// This routes the QueryError propagated by a GRQueryES up to the appropriate top-level ES.
  fn handle_internal_query_error<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    orig_p: OrigP,
    subquery_id: QueryId,
    query_error: msg::QueryError,
  ) {
    struct Cb;
    impl CallbackOnce<(QueryId, msg::QueryError)> for Cb {
      fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
        ctx: &mut TabletContext,
        io_ctx: &mut IOCtx,
        es: &mut TPEST,
        (subquery_id, query_error): (QueryId, msg::QueryError),
      ) -> TabletAction {
        es.remove_subquery(&subquery_id);
        TabletAction::TPESAction(es.handle_internal_query_error(ctx, io_ctx, query_error))
      }
    }

    statuses.execute_once::<_, _, Cb>(self, io_ctx, orig_p.query_id, (subquery_id, query_error));
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

  /// Handles the actions produced by a DropTableES.
  fn handle_drop_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    _: QueryId,
    action: DropTableRMAction,
  ) {
    match action {
      DropTableRMAction::Wait => {}
      DropTableRMAction::Exit(maybe_commit_action) => {
        if let Some(committed_timestamp) = maybe_commit_action {
          // The ES Committed, and so we should mark this Tablet as dropped.
          statuses.ddl_es = DDLES::Dropped(committed_timestamp.clone());
        } else {
          // The ES Aborted, so we just reset it to `None`.
          statuses.ddl_es = DDLES::None;
        }
      }
    }
  }

  /// Handles the actions produced by a AlterTableES.
  fn handle_alter_table_es_action(
    &mut self,
    statuses: &mut Statuses,
    _: QueryId,
    action: AlterTableRMAction,
  ) {
    match action {
      AlterTableRMAction::Wait => {}
      AlterTableRMAction::Exit(_) => {
        statuses.ddl_es = DDLES::None;
      }
    }
  }

  /// Handles the actions produced by a ShardSplitTabletES.
  fn handle_shard_split_es_action<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: ShardSplitTabletRMAction,
  ) {
    match action {
      ShardSplitTabletRMAction::Wait => {}
      ShardSplitTabletRMAction::Exit(maybe_commit_action) => {
        // In both the case of Commit and Abort, ddl_es should be cleared
        statuses.ddl_es = DDLES::None;

        // The ES Committed, and so we should create a `ShardingSnapshotES`
        // and finish the ShardSplit.
        if let Some(target_new) = maybe_commit_action {
          // If this is the Leader, abort all non-Prepared TPESs, including any `PerformQuerys`
          // that are buffered. These will all inevitably have a `ShardingGen` that is too old
          // anyways. (In constract, we will let the Prepared ESs finish, since it's too
          // late to abort them.)
          if self.is_leader() {
            // For PerformQuery, we simply remove them and respond; there are no child queries.
            for (_, perform_query) in std::mem::take(&mut statuses.perform_query_buffer) {
              self.send_query_error(
                io_ctx,
                perform_query.sender_path,
                perform_query.query_id,
                msg::QueryError::InvalidQueryPlan,
              );
            }

            // Otherwise, we use the standard exiting functions.
            let qids: Vec<_> = statuses.ms_query_ess.keys().cloned().collect();
            for qid in qids {
              self.exit_ms_query_es(io_ctx, statuses, qid, msg::QueryError::InvalidQueryPlan);
            }
            let mut qids: Vec<_> = statuses.top.table_read_ess.keys().cloned().collect();
            qids.extend(statuses.top.trans_table_read_ess.keys().cloned());
            for qid in qids {
              self.handle_tp_es_action(
                io_ctx,
                statuses,
                qid,
                Some(TPESAction::QueryError(msg::QueryError::InvalidQueryPlan)),
              );
            }
          }

          // Construct a ShardingStateES and mark `sharding_done` as not done.
          self.sharding_done = false;
          statuses.sharding_state =
            ShardingState::ShardingSnapshotES(ShardingSnapshotES::create_split(
              self,
              io_ctx,
              &statuses.finish_query_ess,
              query_id,
              target_new,
            ));
        }
      }
    }
  }

  /// Handles the actions produced by a ShardSplitTabletES.
  fn handle_shard_send_es_action(
    &mut self,
    statuses: &mut Statuses,
    _: QueryId,
    action: ShardingSnapshotAction,
  ) {
    match action {
      ShardingSnapshotAction::Wait => {}
      ShardingSnapshotAction::Exit => {
        // Mark the sharding as finished.
        self.sharding_done = true;
        statuses.sharding_state = ShardingState::None
      }
    }
  }

  /// Handles the actions produced by a FinishQueryRMES.
  fn handle_finish_query_es_action(
    &mut self,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: paxos2pc_rm::Paxos2PCRMAction,
  ) {
    match action {
      paxos2pc_rm::Paxos2PCRMAction::Wait => {}
      paxos2pc_rm::Paxos2PCRMAction::Exit => {
        statuses.finish_query_ess.remove(&query_id);
      }
    }
  }

  /// Cleans up the MSQueryES with QueryId of `query_id`. This can only be called
  /// if the MSQueryES hasn't Prepared yet. I believe this only ultimately called when
  /// when a `CancelQuery` comes in for this `MSQueryES` (via `exit_and_clean_up`), when
  /// a `RemoteLeaderChanged` occurs for the root node (via `exit_and_clean_up`), and when
  /// a `DeadlockSafetyAbortion` occurs. It's not called when an MSTable*ES experiences a fatal
  /// error (like a `QueryPlanning` error), even though doing so would not be wrong (and in
  /// fact be more efficient).
  fn exit_ms_query_es<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    query_error: msg::QueryError,
  ) {
    // Exit all ESs in `pending_queries` with `query_error` first.
    let pending_queries = statuses.ms_query_ess.get(&query_id).unwrap().pending_queries.clone();
    for query_id in pending_queries {
      self.handle_tp_es_action(
        io_ctx,
        statuses,
        query_id,
        Some(TPESAction::QueryError(query_error.clone())),
      );
    }

    // Remove the MSQueryES.
    let ms_query_es = statuses.ms_query_ess.remove(&query_id).unwrap();

    // Cleanup the TableContext's
    self.ms_root_query_map.remove(&ms_query_es.root_query_path.query_id);
    self.verifying_writes.remove(&ms_query_es.timestamp);
  }

  /// Generate Table Action Handler
  fn handle_tablet_action<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: TabletAction,
  ) {
    match action {
      TabletAction::Wait => {}
      TabletAction::TPESAction(action) => {
        self.handle_tp_es_action(io_ctx, statuses, query_id, action);
      }
      TabletAction::ExitAll(child_queries) => {
        self.exit_all(io_ctx, statuses, child_queries);
      }
      TabletAction::ExitAndCleanUp(query_id) => {
        self.exit_and_clean_up(io_ctx, statuses, query_id);
      }
    }
  }

  /// Handles the actions for all TPESs
  fn handle_tp_es_action<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: Option<TPESAction>,
  ) {
    match action {
      None => {}
      Some(TPESAction::SendSubqueries(gr_query_ess)) => {
        self.launch_subqueries(io_ctx, statuses, gr_query_ess);
      }
      Some(TPESAction::Success(success)) => {
        struct Cb;
        impl CallbackWithContextRemove<QueryESResult> for Cb {
          fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
            ctx: &mut TabletContext,
            io_ctx: &mut IOCtx,
            es: TPEST,
            es_ctx: &mut TPEST::ESContext,
            success: QueryESResult,
          ) -> TabletAction {
            let (query_id, sender_path, child_queries) = es.deregister(es_ctx);
            let responder_path = ctx.mk_query_path(query_id).into_ct();
            // This is the originating Leadership.
            ctx.send_to_ct(
              io_ctx,
              sender_path.node_path,
              CommonQuery::QuerySuccess(msg::QuerySuccess {
                return_qid: sender_path.query_id,
                responder_path,
                result: success.result,
                new_rms: success.new_rms,
              }),
            );
            TabletAction::ExitAll(child_queries)
          }
        }

        statuses.execute_remove_ctx::<_, _, Cb>(self, io_ctx, query_id, success);
      }
      Some(TPESAction::QueryError(query_error)) => {
        struct Cb;
        impl CallbackWithContextRemove<msg::QueryError> for Cb {
          fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
            ctx: &mut TabletContext,
            io_ctx: &mut IOCtx,
            es: TPEST,
            es_ctx: &mut TPEST::ESContext,
            query_error: msg::QueryError,
          ) -> TabletAction {
            let (query_id, sender_path, child_queries) = es.deregister(es_ctx);
            let responder_path = ctx.mk_query_path(query_id).into_ct();
            // This is the originating Leadership.
            ctx.send_to_ct(
              io_ctx,
              sender_path.node_path,
              CommonQuery::QueryAborted(msg::QueryAborted {
                return_qid: sender_path.query_id,
                responder_path,
                payload: msg::AbortedData::QueryError(query_error.clone()),
              }),
            );
            TabletAction::ExitAll(child_queries)
          }
        }

        statuses.execute_remove_ctx::<_, _, Cb>(self, io_ctx, query_id, query_error);
      }
    }
  }

  /// Handles the actions produced by a GRQueryES.
  fn handle_gr_query_es_action<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
    action: GRQueryAction,
  ) {
    match action {
      GRQueryAction::Wait => {}
      GRQueryAction::ExecuteTMStatus(tm_status) => {
        let gr_query = statuses.gr_query_ess.get_mut(&query_id).unwrap();
        gr_query.child_queries.push(tm_status.query_id.clone());
        statuses.tm_statuss.insert(tm_status.query_id.clone(), tm_status);
      }
      GRQueryAction::ExecuteJoinReadES(join_es) => {
        // TODO: do
        unimplemented!()
      }
      GRQueryAction::Success(res) => {
        let gr_query = statuses.gr_query_ess.remove(&query_id).unwrap();
        self.handle_gr_query_done(
          io_ctx,
          statuses,
          gr_query.es.orig_p,
          gr_query.es.query_id,
          res.new_rms,
          res.result,
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
  /// CancelQuery's, as well as when one on ES wants to Exit and Clean Up another ES. Note that
  /// We allow the ES at `query_id` to be in any state, or to not even exist.
  /// TODO: I don't like the funneling behavior of this function. It makes it more cumbersome to
  ///  verify with confidence that a particular ES exists at a certain time.
  /// TODO: this is currently used for Cancel, RemoteLeaderChanged, exit_all, and a msg::QueryAborted
  ///  to inform the TMStatus. Note that it's mostly action handlers that hand exit_all.
  fn exit_and_clean_up<IO: CoreIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    statuses: &mut Statuses,
    query_id: QueryId,
  ) {
    // Buffered PerformQuery
    if let Some(_) = statuses.perform_query_buffer.remove(&query_id) {
      // No-op
    }
    // GRQueryES
    else if let Some(mut gr_query) = statuses.gr_query_ess.remove(&query_id) {
      gr_query.es.exit_and_clean_up(self);
      self.exit_all(io_ctx, statuses, gr_query.child_queries);
    }
    // TMStatus
    else if let Some(mut tm_status) = statuses.tm_statuss.remove(&query_id) {
      tm_status.exit_and_clean_up(self, io_ctx);
    }
    // MSQueryES
    else if let Some(ms_query_es) = statuses.ms_query_ess.get(&query_id) {
      // We should only run this code when a CancelQuery comes in (from the Coord) for
      // the MSQueryES. We shouldn't run this code in any other circumstance (e.g.
      // DeadlockSafetyAborted), since this only sends back `LateralError`s to the origiators
      // of the MSTable*ESs, which we should only do if an ancestor is known already have exit
      // (i.e. the MSCoordES, in this case).
      //
      // This is also called by RemoteLeaderChanged, when the Coord ceases to exist.
      // Here, a LateralError is fine too.
      //
      // TODO: In the spirit of getting local safety, we shouldn't have the above expection.
      // Instead, we should give MSQueryES a state variable and have it react to CancelQuery
      // accordingly (ignore it if we have prepared).
      self.exit_ms_query_es(
        io_ctx,
        statuses,
        ms_query_es.query_id.clone(),
        msg::QueryError::LateralError,
      );
    }
    // Top Level ESs
    else {
      struct Cb;
      impl CallbackWithContextRemove<()> for Cb {
        fn call<IOCtx: CoreIOCtx, TPEST: TPESBase>(
          _: &mut TabletContext,
          _: &mut IOCtx,
          es: TPEST,
          es_ctx: &mut TPEST::ESContext,
          _: (),
        ) -> TabletAction {
          let (_, _, child_queries) = es.deregister(es_ctx);
          TabletAction::ExitAll(child_queries)
        }
      }

      statuses.execute_remove_ctx::<_, _, Cb>(self, io_ctx, query_id, ());
    }
  }

  /// Construct NodePath of this Tablet.
  pub fn mk_node_path(&self) -> TNodePath {
    TNodePath { sid: self.this_sid.clone(), sub: TSubNodePath::Tablet(self.this_tid.clone()) }
  }

  /// Construct QueryPath for a given `query_id` that belongs to this Tablet.
  pub fn mk_query_path(&self, query_id: QueryId) -> TQueryPath {
    TQueryPath { node_path: self.mk_node_path(), query_id }
  }

  /// Returns true iff this is the Leader.
  pub fn is_leader(&self) -> bool {
    let lid = self.leader_map.get(&self.this_sid.to_gid()).unwrap();
    lid.eid == self.this_eid
  }

  /// Check whether the `pkey` falls in the range of this Tablet's `TabletKeyRange`. The `pkey`
  /// must conform the tablets KeyCol schema (which the `TabletKeyRange` also does).
  pub fn check_range_inclusion(&self, pkey: &PrimaryKey) -> bool {
    self.this_tablet_key_range.contains_pkey(pkey)
  }

  /// If any Sharding operations are still in the process of finishing, we avoid
  /// advancing any other DDLES beyond `Working` until it is finished.
  pub fn pause_ddl(&self) -> bool {
    !self.sharding_done
  }
}

// -----------------------------------------------------------------------------------------------
//  New Table Subquery Construction
// -----------------------------------------------------------------------------------------------

/// This runs the `ContextConstructor` with the given inputs and simply accumulates the
/// `ContextRow` to produce a `Context` for each element in `children`.
pub fn compute_contexts<LocalTableT: LocalTable>(
  parent_context: &Context,
  local_table: LocalTableT,
  children: Vec<(Vec<proc::ColumnRef>, Vec<TransTableName>)>,
) -> Vec<Context> {
  // Create the ContextConstruct.
  let context_constructor =
    ContextConstructor::new(parent_context.context_schema.clone(), local_table, children);

  // Initialize the child Contexts
  let mut child_contexts = Vec::<Context>::new();
  for schema in context_constructor.get_schemas() {
    child_contexts.push(Context::new(schema));
  }

  // Create the child Contexts.
  let callback = &mut |_context_row_idx: usize,
                       _top_level_col_vals: Vec<ColValN>,
                       contexts: Vec<(ContextRow, usize)>,
                       _count: u64| {
    for (subquery_idx, (context_row, idx)) in contexts.into_iter().enumerate() {
      let child_context = child_contexts.get_mut(subquery_idx).unwrap();
      if idx == child_context.context_rows.len() {
        // This is a new ContextRow, so add it in.
        child_context.context_rows.push(context_row);
      }
    }

    Ok(())
  };

  // Run the Constructor. Recall that errors are only returned from the callback,
  // which in this case does not return any errors.
  context_constructor.run(&parent_context.context_rows, Vec::new(), callback).unwrap();
  child_contexts
}

/// Compute children for the given query
pub fn compute_children(
  subqueries: &Vec<proc::GRQuery>,
) -> Vec<(Vec<proc::ColumnRef>, Vec<TransTableName>)> {
  let mut children = Vec::<(Vec<proc::ColumnRef>, Vec<TransTableName>)>::new();
  for subquery in subqueries {
    children.push(compute_children_general(QueryElement::GRQuery(subquery)))
  }
  children
}

/// This computes GRQueryESs corresponding to every element in `subqueries`.
pub fn compute_subqueries<
  RngCoreT: RngCore,
  LocalTableT: LocalTable,
  SqlQueryT: SubqueryComputableSql,
>(
  subquery_view: GRQueryConstructorView<SqlQueryT>,
  rand: &mut RngCoreT,
  local_table: LocalTableT,
) -> Vec<GRQueryES> {
  // Here, we construct first construct all of the subquery Contexts using the
  // ContextConstructor, and then we construct GRQueryESs.

  // Compute children.
  let children = compute_children(&subquery_view.sql_query.collect_subqueries());

  // Create the child context.
  let child_contexts = compute_contexts(subquery_view.context, local_table, children);

  // We compute all GRQueryESs.
  let mut gr_query_ess = Vec::<GRQueryES>::new();
  for (subquery_idx, child_context) in child_contexts.into_iter().enumerate() {
    gr_query_ess.push(subquery_view.mk_gr_query_es(
      mk_qid(rand),
      Rc::new(child_context),
      subquery_idx,
    ));
  }

  gr_query_ess
}
