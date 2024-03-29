use crate::common::{
  cur_timestamp, mk_t, BasicIOCtx, GeneralTraceMessage, GossipData, GossipDataView, Timestamp,
};
use crate::common::{EndpointId, RequestId, TNodePath, TSubNodePath, TablePath};
use crate::master::{MasterContext, MasterPLm};
use crate::message as msg;
use crate::server::ServerContextBase;
use crate::sql_ast::proc;
use crate::stmpaxos2pc_tm::{
  RMMessage, STMPaxos2PCTMInner, STMPaxos2PCTMOuter, TMClosedPLm, TMCommittedPLm, TMMessage, TMPLm,
  TMPayloadTypes, TMServerContext,
};
use crate::tablet::{TabletContext, TabletPLm};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableTMPayloadTypes {}

impl TMPayloadTypes for AlterTableTMPayloadTypes {
  // Master
  type RMPath = TNodePath;
  type TMPath = ();
  type NetworkMessageT = msg::NetworkMessage;
  type TMContext = MasterContext;

  // TM PLm
  type TMPreparedPLm = AlterTableTMPrepared;
  type TMCommittedPLm = AlterTableTMCommitted;
  type TMAbortedPLm = AlterTableTMAborted;
  type TMClosedPLm = AlterTableTMClosed;

  // TM-to-RM Messages
  type Prepare = AlterTablePrepare;
  type Abort = AlterTableAbort;
  type Commit = AlterTableCommit;

  // RM-to-TM Messages
  type Prepared = AlterTablePrepared;
  type Aborted = AlterTableAborted;
  type Closed = AlterTableClosed;
}

// TM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableTMPrepared {
  pub table_path: TablePath,
  pub alter_op: proc::AlterOp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableTMCommitted {
  pub timestamp_hint: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableTMAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableTMClosed {}

// TM-to-RM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePrepare {
  pub alter_op: proc::AlterOp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAbort {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableCommit {
  pub timestamp: Timestamp,
}

// RM-to-TM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePrepared {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableClosed {}

// -----------------------------------------------------------------------------------------------
//  TMServerContext AlterTable
// -----------------------------------------------------------------------------------------------

impl TMServerContext<AlterTableTMPayloadTypes> for MasterContext {
  fn push_plm(&mut self, plm: TMPLm<AlterTableTMPayloadTypes>) {
    self.master_bundle.plms.push(MasterPLm::AlterTable(plm));
  }

  fn send_to_rm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    rm: &TNodePath,
    msg: RMMessage<AlterTableTMPayloadTypes>,
  ) {
    self.send_to_t(io_ctx, rm.clone(), msg::TabletMessage::AlterTable(msg));
  }

  fn mk_node_path(&self) -> () {
    ()
  }

  fn is_leader(&self) -> bool {
    MasterContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  General STMPaxos2PC TM Types
// -----------------------------------------------------------------------------------------------
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ResponseData {
  pub request_id: RequestId,
  pub sender_eid: EndpointId,
}

// -----------------------------------------------------------------------------------------------
//  AlterTable Implementation
// -----------------------------------------------------------------------------------------------

pub type AlterTableTMES = STMPaxos2PCTMOuter<AlterTableTMPayloadTypes, AlterTableTMInner>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableTMInner {
  // Response data
  pub response_data: Option<ResponseData>,

  // AlterTable Query data
  pub table_path: TablePath,
  pub alter_op: proc::AlterOp,
}

impl STMPaxos2PCTMInner<AlterTableTMPayloadTypes> for AlterTableTMInner {
  fn new_follower<IO: BasicIOCtx>(
    _: &mut MasterContext,
    _: &mut IO,
    payload: AlterTableTMPrepared,
  ) -> AlterTableTMInner {
    AlterTableTMInner {
      response_data: None,
      table_path: payload.table_path,
      alter_op: payload.alter_op,
    }
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> AlterTableTMPrepared {
    AlterTableTMPrepared { table_path: self.table_path.clone(), alter_op: self.alter_op.clone() }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> BTreeMap<TNodePath, AlterTablePrepare> {
    let mut prepares = BTreeMap::<TNodePath, AlterTablePrepare>::new();
    for rm in get_rms::<IO>(&ctx.gossip.get(), &self.table_path) {
      prepares.insert(rm.clone(), AlterTablePrepare { alter_op: self.alter_op.clone() });
    }
    prepares
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: &BTreeMap<TNodePath, AlterTablePrepared>,
  ) -> AlterTableTMCommitted {
    let mut timestamp_hint = cur_timestamp(io_ctx, ctx.master_config.timestamp_suffix_divisor);
    for (_, prepared) in prepared {
      timestamp_hint = max(timestamp_hint, prepared.timestamp.clone());
    }
    AlterTableTMCommitted { timestamp_hint }
  }

  /// Apply this `alter_op` to the system and construct Commit messages with the
  /// commit timestamp (which is resolved form the resolved from `timestamp_hint`).
  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    committed_plm: &TMCommittedPLm<AlterTableTMPayloadTypes>,
  ) -> BTreeMap<TNodePath, AlterTableCommit> {
    let timestamp = ctx.gossip.update(|gossip| {
      let (gen, _) = gossip.table_generation.get_last_version(&self.table_path).unwrap();
      let table_schema = gossip.db_schema.get_mut(&(self.table_path.clone(), gen.clone())).unwrap();

      // Compute the timestamp to commit at
      let mut timestamp = committed_plm.payload.timestamp_hint.clone();
      timestamp = max(timestamp, gossip.table_generation.get_lat(&self.table_path).add(mk_t(1)));
      timestamp =
        max(timestamp, table_schema.val_cols.get_lat(&self.alter_op.col_name).add(mk_t(1)));

      // Apply the AlterOp
      gossip.table_generation.update_lat(&self.table_path, timestamp.clone());
      table_schema.val_cols.write(
        &self.alter_op.col_name,
        self.alter_op.maybe_col_type.clone(),
        timestamp.clone(),
      );

      timestamp
    });

    // Potentially respond to the External if we are the leader.
    if ctx.is_leader() {
      if let Some(response_data) = &self.response_data {
        // This means this is the original Leader that got the query.
        ctx.external_request_id_map.remove(&response_data.request_id);
        io_ctx.send(
          &response_data.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(
            msg::ExternalDDLQuerySuccess {
              request_id: response_data.request_id.clone(),
              timestamp: timestamp.clone(),
            },
          )),
        );
        self.response_data = None;
      }
    }

    // Trace this commit.
    io_ctx.general_trace(GeneralTraceMessage::CommittedQueryId(
      committed_plm.query_id.clone(),
      timestamp.clone(),
    ));

    // Send out GossipData to all Slaves.
    // TODO: should this and the other DDL TM Statuses only be doing this if this is the elader?
    ctx.broadcast_gossip(io_ctx);

    // Return Commit messages
    let mut commits = BTreeMap::<TNodePath, AlterTableCommit>::new();
    for rm in get_rms::<IO>(&ctx.gossip.get(), &self.table_path) {
      commits.insert(rm.clone(), AlterTableCommit { timestamp: timestamp.clone() });
    }
    commits
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> AlterTableTMAborted {
    AlterTableTMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> BTreeMap<TNodePath, AlterTableAbort> {
    // Potentially respond to the External if we are the leader.
    if ctx.is_leader() {
      if let Some(response_data) = &self.response_data {
        ctx.external_request_id_map.remove(&response_data.request_id);
        io_ctx.send(
          &response_data.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
            msg::ExternalDDLQueryAborted {
              request_id: response_data.request_id.clone(),
              payload: msg::ExternalDDLQueryAbortData::Unknown,
            },
          )),
        );
        self.response_data = None;
      }
    }

    let mut aborts = BTreeMap::<TNodePath, AlterTableAbort>::new();
    for rm in get_rms::<IO>(&ctx.gossip.get(), &self.table_path) {
      aborts.insert(rm.clone(), AlterTableAbort {});
    }
    aborts
  }

  fn mk_closed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> AlterTableTMClosed {
    AlterTableTMClosed {}
  }

  fn closed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
    _: &TMClosedPLm<AlterTableTMPayloadTypes>,
  ) {
  }

  fn leader_changed<IO: BasicIOCtx>(&mut self, _: &mut MasterContext, _: &mut IO) {
    self.response_data = None;
  }

  fn reconfig_snapshot(&self) -> AlterTableTMInner {
    AlterTableTMInner {
      response_data: None,
      table_path: self.table_path.clone(),
      alter_op: self.alter_op.clone(),
    }
  }
}

/// This returns the current set of RMs associated with the given `TablePath`. Recall that while
/// the ES is alive, we ensure that this is idempotent.
pub fn get_rms<IO: BasicIOCtx>(gossip: &GossipDataView, table_path: &TablePath) -> Vec<TNodePath> {
  let full_gen = gossip.table_generation.get_last_version(table_path).unwrap();
  let mut rms = Vec::<TNodePath>::new();
  for (_, tid) in gossip.sharding_config.get(&(table_path.clone(), full_gen.clone())).unwrap() {
    let sid = gossip.tablet_address_config.get(&tid).unwrap().clone();
    rms.push(TNodePath { sid, sub: TSubNodePath::Tablet(tid.clone()) });
  }
  rms
}
