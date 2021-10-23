use crate::common::BasicIOCtx;
use crate::master::{MasterContext, MasterPLm};
use crate::model::common::{
  proc, EndpointId, QueryId, RequestId, TNodePath, TSubNodePath, TablePath, Timestamp,
};
use crate::model::message as msg;
use crate::stmpaxos2pc_tm::{
  Abort, Aborted, Closed, Commit, PayloadTypes, Prepare, Prepared, RMAbortedPLm, RMCommittedPLm,
  RMPreparedPLm, STMPaxos2PCTMInner, STMPaxos2PCTMOuter, TMAbortedPLm, TMClosedPLm, TMCommittedPLm,
  TMPreparedPLm,
};
use crate::tablet::{TabletContext, TabletPLm};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

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

// RM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRMPrepared {
  pub alter_op: proc::AlterOp,
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRMCommitted {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTableRMAborted {}

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

// AlterTablePayloadTypes

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AlterTablePayloadTypes {}

impl PayloadTypes for AlterTablePayloadTypes {
  // Master
  type RMPLm = TabletPLm;
  type TMPLm = MasterPLm;
  type RMPath = TNodePath;
  type TMPath = ();
  type RMMessage = msg::TabletMessage;
  type TMMessage = msg::MasterRemotePayload;
  type NetworkMessageT = msg::NetworkMessage;
  type RMContext = TabletContext;
  type TMContext = MasterContext;

  // TM PLm
  type TMPreparedPLm = AlterTableTMPrepared;
  type TMCommittedPLm = AlterTableTMCommitted;
  type TMAbortedPLm = AlterTableTMAborted;
  type TMClosedPLm = AlterTableTMClosed;

  fn tm_prepared_plm(prepared_plm: TMPreparedPLm<Self>) -> MasterPLm {
    MasterPLm::AlterTableTMPrepared(prepared_plm)
  }

  fn tm_committed_plm(committed_plm: TMCommittedPLm<Self>) -> MasterPLm {
    MasterPLm::AlterTableTMCommitted(committed_plm)
  }

  fn tm_aborted_plm(aborted_plm: TMAbortedPLm<Self>) -> MasterPLm {
    MasterPLm::AlterTableTMAborted(aborted_plm)
  }

  fn tm_closed_plm(closed_plm: TMClosedPLm<Self>) -> MasterPLm {
    MasterPLm::AlterTableTMClosed(closed_plm)
  }

  // RM PLm
  type RMPreparedPLm = AlterTableRMPrepared;
  type RMCommittedPLm = AlterTableRMCommitted;
  type RMAbortedPLm = AlterTableRMAborted;

  fn rm_prepared_plm(prepared_plm: RMPreparedPLm<Self>) -> TabletPLm {
    TabletPLm::AlterTablePrepared(prepared_plm)
  }

  fn rm_committed_plm(committed_plm: RMCommittedPLm<Self>) -> TabletPLm {
    TabletPLm::AlterTableCommitted(committed_plm)
  }

  fn rm_aborted_plm(aborted_plm: RMAbortedPLm<Self>) -> TabletPLm {
    TabletPLm::AlterTableAborted(aborted_plm)
  }

  // TM-to-RM Messages
  type Prepare = AlterTablePrepare;
  type Abort = AlterTableAbort;
  type Commit = AlterTableCommit;

  fn rm_prepare(prepare: Prepare<Self>) -> msg::TabletMessage {
    msg::TabletMessage::AlterTablePrepare(prepare)
  }

  fn rm_commit(commit: Commit<Self>) -> msg::TabletMessage {
    msg::TabletMessage::AlterTableCommit(commit)
  }

  fn rm_abort(abort: Abort<Self>) -> msg::TabletMessage {
    msg::TabletMessage::AlterTableAbort(abort)
  }

  // RM-to-TM Messages
  type Prepared = AlterTablePrepared;
  type Aborted = AlterTableAborted;
  type Closed = AlterTableClosed;

  fn tm_prepared(prepared: Prepared<Self>) -> msg::MasterRemotePayload {
    msg::MasterRemotePayload::AlterTablePrepared(prepared)
  }

  fn tm_aborted(aborted: Aborted<Self>) -> msg::MasterRemotePayload {
    msg::MasterRemotePayload::AlterTableAborted(aborted)
  }

  fn tm_closed(closed: Closed<Self>) -> msg::MasterRemotePayload {
    msg::MasterRemotePayload::AlterTableClosed(closed)
  }
}

// -----------------------------------------------------------------------------------------------
//  General STMPaxos2PC TM Types
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct ResponseData {
  pub request_id: RequestId,
  pub sender_eid: EndpointId,
}

// -----------------------------------------------------------------------------------------------
//  AlterTable Implementation
// -----------------------------------------------------------------------------------------------

pub type AlterTableTMES = STMPaxos2PCTMOuter<AlterTablePayloadTypes, AlterTableTMInner>;

#[derive(Debug)]
pub struct AlterTableTMInner {
  // Response data
  pub response_data: Option<ResponseData>,

  // AlterTable Query data
  pub table_path: TablePath,
  pub alter_op: proc::AlterOp,
}

impl STMPaxos2PCTMInner<AlterTablePayloadTypes> for AlterTableTMInner {
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
    for rm in get_rms::<IO>(ctx, &self.table_path) {
      prepares.insert(rm.clone(), AlterTablePrepare { alter_op: self.alter_op.clone() });
    }
    prepares
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: &BTreeMap<TNodePath, AlterTablePrepared>,
  ) -> AlterTableTMCommitted {
    let mut timestamp_hint = io_ctx.now();
    for (_, prepared) in prepared {
      timestamp_hint = max(timestamp_hint, prepared.timestamp);
    }
    AlterTableTMCommitted { timestamp_hint }
  }

  /// Apply this `alter_op` to the system and construct Commit messages with the
  /// commit timestamp (which is resolved form the resolved from `timestamp_hint`).
  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    committed_plm: &TMCommittedPLm<AlterTablePayloadTypes>,
  ) -> BTreeMap<TNodePath, AlterTableCommit> {
    let gen = ctx.table_generation.get_last_version(&self.table_path).unwrap();
    let table_schema = ctx.db_schema.get_mut(&(self.table_path.clone(), gen.clone())).unwrap();

    // Compute the resolved timestamp
    let mut commit_timestamp = committed_plm.payload.timestamp_hint;
    commit_timestamp = max(commit_timestamp, ctx.table_generation.get_lat(&self.table_path) + 1);
    commit_timestamp =
      max(commit_timestamp, table_schema.val_cols.get_lat(&self.alter_op.col_name) + 1);

    // Apply the AlterOp
    ctx.gen.0 += 1;
    ctx.table_generation.update_lat(&self.table_path, commit_timestamp);
    table_schema.val_cols.write(
      &self.alter_op.col_name,
      self.alter_op.maybe_col_type.clone(),
      commit_timestamp,
    );

    // Potentially respond to the External if we are the leader.
    if ctx.is_leader() {
      if let Some(response_data) = &self.response_data {
        ctx.external_request_id_map.remove(&response_data.request_id);
        io_ctx.send(
          &response_data.sender_eid,
          msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQuerySuccess(
            msg::ExternalDDLQuerySuccess {
              request_id: response_data.request_id.clone(),
              timestamp: commit_timestamp,
            },
          )),
        );
        self.response_data = None;
      }
    }

    // Send out GossipData to all Slaves.
    ctx.broadcast_gossip(io_ctx);

    // Return Commit messages
    let mut commits = BTreeMap::<TNodePath, AlterTableCommit>::new();
    for rm in get_rms::<IO>(ctx, &self.table_path) {
      commits.insert(rm.clone(), AlterTableCommit { timestamp: commit_timestamp });
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
    for rm in get_rms::<IO>(ctx, &self.table_path) {
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
    _: &TMClosedPLm<AlterTablePayloadTypes>,
  ) {
  }

  fn node_died<IO: BasicIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
  }
}

/// This returns the current set of RMs associated with the given `TablePath`. Recall that while
/// the ES is alive, we ensure that this is idempotent.
pub fn get_rms<IO: BasicIOCtx>(ctx: &mut MasterContext, table_path: &TablePath) -> Vec<TNodePath> {
  let gen = ctx.table_generation.get_last_version(table_path).unwrap();
  let mut rms = Vec::<TNodePath>::new();
  for (_, tid) in ctx.sharding_config.get(&(table_path.clone(), gen.clone())).unwrap() {
    let sid = ctx.tablet_address_config.get(&tid).unwrap().clone();
    rms.push(TNodePath { sid, sub: TSubNodePath::Tablet(tid.clone()) });
  }
  rms
}

/// Send a response back to the External, informing them that the current Master Leader died.
pub fn maybe_respond_dead<IO: BasicIOCtx>(
  response_data: &mut Option<ResponseData>,
  ctx: &mut MasterContext,
  io_ctx: &mut IO,
) {
  if let Some(data) = response_data {
    ctx.external_request_id_map.remove(&data.request_id);
    io_ctx.send(
      &data.sender_eid,
      msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
        msg::ExternalDDLQueryAborted {
          request_id: data.request_id.clone(),
          payload: msg::ExternalDDLQueryAbortData::NodeDied,
        },
      )),
    );
    *response_data = None;
  }
}
