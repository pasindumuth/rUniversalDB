use crate::common::{merge_table_views, mk_qid, CoreIOCtx, OrigP};
use crate::common::{
  CQueryPath, CTNodePath, ColName, LeadershipId, PaxosGroupIdTrait, QueryId, SlaveGroupId,
  TQueryPath, TableView, TabletGroupId, TransTableLocationPrefix,
};
use crate::message as msg;
use crate::server::{CTServerContext, CommonQuery};
use std::collections::{BTreeMap, BTreeSet};

// -----------------------------------------------------------------------------------------------
//  TMStatus
// -----------------------------------------------------------------------------------------------

pub enum SendHelper {
  TableQuery(msg::GeneralQuery, Vec<TabletGroupId>),
  TransTableQuery(msg::GeneralQuery, TransTableLocationPrefix),
}

// These are used to perform PCSA over the network for reads and writes.
#[derive(Debug)]
pub struct TMStatus {
  root_query_path: CQueryPath,
  /// The QueryId of the TMStatus.
  pub query_id: QueryId,
  /// This is the QueryId of the PerformQuery. We keep this distinct from the TMStatus'
  /// QueryId, since one of the RMs might be this node.
  child_query_id: QueryId,
  /// Accumulates all transitively accessed Tablets where an `MSQueryES` was used.
  new_rms: BTreeSet<TQueryPath>,
  /// The current set of Leaderships that this TMStatus is waiting on. Thus, in order to
  /// contact an RM, we just use the `LeadershipId` found here.
  pub leaderships: BTreeMap<SlaveGroupId, LeadershipId>,
  /// Holds the number of nodes that responded (used to decide when this TM is done).
  responded_count: usize,
  /// Holds all child Querys, initially mapping to `None`. As results come in, we hold them here.
  tm_state: BTreeMap<CTNodePath, Option<Vec<TableView>>>,
  pub orig_p: OrigP,
}

impl TMStatus {
  pub fn new<IO: CoreIOCtx>(
    io_ctx: &mut IO,
    root_query_path: CQueryPath,
    orig_p: OrigP,
  ) -> TMStatus {
    TMStatus {
      root_query_path,
      query_id: mk_qid(io_ctx.rand()),
      child_query_id: mk_qid(io_ctx.rand()),
      new_rms: Default::default(),
      leaderships: Default::default(),
      responded_count: 0,
      tm_state: Default::default(),
      orig_p,
    }
  }

  pub fn query_id(&self) -> &QueryId {
    &self.query_id
  }

  /// Perform the sending indicated by `helper`.
  /// TODO: when sharding occurs, this query_leader_map.get might be invalid.
  pub fn send_general<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    query_leader_map: &BTreeMap<SlaveGroupId, LeadershipId>,
    helper: SendHelper,
  ) -> bool {
    match helper {
      SendHelper::TableQuery(general_query, tids) => {
        // Validate the LeadershipId of PaxosGroups that the PerformQuery will be sent to.
        // We do this before sending any messages, in case it fails. Recall that the local
        // `leader_map` is allowed to get ahead of the `query_leader_map` which we computed
        // earlier, so this check is necessary.
        for tid in &tids {
          let sid = ctx.gossip().get().tablet_address_config.get(&tid).unwrap();
          if let Some(lid) = query_leader_map.get(sid) {
            if lid.gen < ctx.leader_map().get(&sid.to_gid()).unwrap().gen {
              // The `lid` has since changed, so we cannot finish this MSQueryES.
              return false;
            }
          }
        }

        // Having non-empty `tids` solves the TMStatus deadlock and determining the child schema.
        assert!(tids.len() > 0);
        for tid in tids {
          // Recall we already validated that `lid` in `query_leader_map` is no lower than
          // the one at this node's LeaderMap, so it safe to use.
          let to_node_path = ctx.mk_tablet_node_path(tid).into_ct();
          let sid = &to_node_path.sid;
          let to_lid = query_leader_map.get(sid).or(ctx.leader_map().get(&sid.to_gid())).unwrap();
          self.send_perform(ctx, io_ctx, general_query.clone(), to_node_path, to_lid.clone());
        }
      }
      SendHelper::TransTableQuery(general_query, location_prefix) => {
        // Validate the LeadershipId of PaxosGroups that the PerformQuery will be sent to.
        // We do this before sending any messages, in case it fails.
        let sid = &location_prefix.source.node_path.sid;
        if let Some(lid) = query_leader_map.get(sid) {
          if lid.gen < ctx.leader_map().get(&sid.to_gid()).unwrap().gen {
            // The `lid` is too old, so we cannot finish this GRQueryES.
            return false;
          }
        }

        // Recall we already validated that `lid` in `query_leader_map` is no lower than
        // the one at this node's LeaderMap, so it safe to use.
        let to_lid = query_leader_map.get(&sid).or(ctx.leader_map().get(&sid.to_gid())).unwrap();
        let to_node_path = location_prefix.source.node_path.clone();
        self.send_perform(ctx, io_ctx, general_query, to_node_path, to_lid.clone());
      }
    }

    true
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn send_perform<IO: CoreIOCtx, Ctx: CTServerContext>(
    &mut self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
    general_query: msg::GeneralQuery,
    to_node_path: CTNodePath,
    to_lid: LeadershipId,
  ) {
    let sender_path = ctx.mk_this_query_path(self.query_id.clone());
    // Construct PerformQuery
    let perform_query = msg::PerformQuery {
      root_query_path: self.root_query_path.clone(),
      sender_path,
      query_id: self.child_query_id.clone(),
      query: general_query,
    };

    // Send out PerformQuery. Recall that this could only be a Tablet.
    let common_query = CommonQuery::PerformQuery(perform_query);
    ctx.send_to_ct_lid(io_ctx, to_node_path.clone(), common_query, to_lid.clone());

    // Add the TabletGroup into the TMStatus.
    self.leaderships.insert(to_node_path.sid.clone(), to_lid);
    self.tm_state.insert(to_node_path, None);
  }

  /// We accumulate the results of the `query_success` here.
  pub fn handle_query_success(&mut self, query_success: msg::QuerySuccess) {
    let node_path = query_success.responder_path.node_path;
    self.tm_state.insert(node_path, Some(query_success.result.clone()));
    self.new_rms.extend(query_success.new_rms);
    self.responded_count += 1;
  }

  /// Merge there `TableView`s together. Note that this should be only called when
  /// all child queries have responded.
  pub fn get_results(self) -> (OrigP, Vec<TableView>, BTreeSet<TQueryPath>) {
    debug_assert!(self.is_complete());
    let mut results = Vec::<Vec<TableView>>::new();
    for (_, rm_result) in self.tm_state {
      results.push(rm_result.unwrap());
    }
    (self.orig_p, merge_table_views(results), self.new_rms)
  }

  pub fn is_complete(&self) -> bool {
    self.responded_count == self.tm_state.len()
  }

  /// We ECU this `TMStatus` by sending `CancelQuery` to all remaining RMs.
  pub fn exit_and_clean_up<IO: CoreIOCtx, Ctx: CTServerContext>(
    self,
    ctx: &mut Ctx,
    io_ctx: &mut IO,
  ) {
    for (rm_path, rm_result) in self.tm_state {
      if rm_result.is_none() {
        let orig_sid = &rm_path.sid;
        let orig_lid = self.leaderships.get(&orig_sid).unwrap().clone();
        ctx.send_to_ct_lid(
          io_ctx,
          rm_path,
          CommonQuery::CancelQuery(msg::CancelQuery { query_id: self.child_query_id.clone() }),
          orig_lid,
        );
      }
    }
  }
}
