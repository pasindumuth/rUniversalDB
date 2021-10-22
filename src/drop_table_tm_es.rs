use crate::alter_table_tm_es::{get_rms, maybe_respond_dead, ResponseData};
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
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

// TM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableTMPrepared {
  pub table_path: TablePath,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableTMCommitted {
  pub timestamp_hint: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableTMAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableTMClosed {}

// RM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableRMPrepared {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableRMCommitted {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableRMAborted {}

// TM-to-RM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTablePrepare {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableAbort {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableCommit {
  pub timestamp: Timestamp,
}

// RM-to-TM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTablePrepared {
  pub timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableClosed {}

// DropTablePayloadTypes

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTablePayloadTypes {}

impl PayloadTypes for DropTablePayloadTypes {
  // Master
  type TMPLm = MasterPLm;
  type RMPLm = TabletPLm;
  type RMPath = TNodePath;
  type TMPath = ();
  type RMMessage = msg::TabletMessage;
  type TMMessage = msg::MasterRemotePayload;
  type RMContext = TabletContext;
  type TMContext = MasterContext;

  // TM PLm
  type TMPreparedPLm = DropTableTMPrepared;
  type TMCommittedPLm = DropTableTMCommitted;
  type TMAbortedPLm = DropTableTMAborted;
  type TMClosedPLm = DropTableTMClosed;

  fn tm_prepared_plm(prepared_plm: TMPreparedPLm<Self>) -> MasterPLm {
    MasterPLm::DropTableTMPrepared(prepared_plm)
  }

  fn tm_committed_plm(committed_plm: TMCommittedPLm<Self>) -> MasterPLm {
    MasterPLm::DropTableTMCommitted(committed_plm)
  }

  fn tm_aborted_plm(aborted_plm: TMAbortedPLm<Self>) -> MasterPLm {
    MasterPLm::DropTableTMAborted(aborted_plm)
  }

  fn tm_closed_plm(closed_plm: TMClosedPLm<Self>) -> MasterPLm {
    MasterPLm::DropTableTMClosed(closed_plm)
  }

  // RM PLm
  type RMPreparedPLm = DropTableRMPrepared;
  type RMCommittedPLm = DropTableRMCommitted;
  type RMAbortedPLm = DropTableRMAborted;

  fn rm_prepared_plm(prepared_plm: RMPreparedPLm<Self>) -> TabletPLm {
    TabletPLm::DropTablePrepared(prepared_plm)
  }

  fn rm_committed_plm(committed_plm: RMCommittedPLm<Self>) -> TabletPLm {
    TabletPLm::DropTableCommitted(committed_plm)
  }

  fn rm_aborted_plm(aborted_plm: RMAbortedPLm<Self>) -> TabletPLm {
    TabletPLm::DropTableAborted(aborted_plm)
  }

  // TM-to-RM Messages
  type Prepare = DropTablePrepare;
  type Abort = DropTableAbort;
  type Commit = DropTableCommit;

  fn rm_prepare(prepare: Prepare<Self>) -> msg::TabletMessage {
    msg::TabletMessage::DropTablePrepare(prepare)
  }

  fn rm_commit(commit: Commit<Self>) -> msg::TabletMessage {
    msg::TabletMessage::DropTableCommit(commit)
  }

  fn rm_abort(abort: Abort<Self>) -> msg::TabletMessage {
    msg::TabletMessage::DropTableAbort(abort)
  }

  // RM-to-TM Messages
  type Prepared = DropTablePrepared;
  type Aborted = DropTableAborted;
  type Closed = DropTableClosed;

  fn tm_prepared(prepared: Prepared<Self>) -> msg::MasterRemotePayload {
    msg::MasterRemotePayload::DropTablePrepared(prepared)
  }

  fn tm_aborted(aborted: Aborted<Self>) -> msg::MasterRemotePayload {
    msg::MasterRemotePayload::DropTableAborted(aborted)
  }

  fn tm_closed(closed: Closed<Self>) -> msg::MasterRemotePayload {
    msg::MasterRemotePayload::DropTableClosed(closed)
  }
}

// -----------------------------------------------------------------------------------------------
//  DropTable Implementation
// -----------------------------------------------------------------------------------------------

pub type DropTableTMES = STMPaxos2PCTMOuter<DropTablePayloadTypes, DropTableTMInner>;

#[derive(Debug)]
pub struct DropTableTMInner {
  // Response data
  pub response_data: Option<ResponseData>,

  // DropTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,
}

impl STMPaxos2PCTMInner<DropTablePayloadTypes> for DropTableTMInner {
  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> DropTableTMPrepared {
    DropTableTMPrepared { table_path: self.table_path.clone() }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> HashMap<TNodePath, DropTablePrepare> {
    let mut prepares = HashMap::<TNodePath, DropTablePrepare>::new();
    for rm in get_rms::<IO>(ctx, &self.table_path) {
      prepares.insert(rm.clone(), DropTablePrepare {});
    }
    prepares
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: &HashMap<TNodePath, DropTablePrepared>,
  ) -> DropTableTMCommitted {
    let mut timestamp_hint = io_ctx.now();
    for (_, prepared) in prepared {
      timestamp_hint = max(timestamp_hint, prepared.timestamp);
    }
    DropTableTMCommitted { timestamp_hint }
  }

  /// Apply this `alter_op` to the system and construct Commit messages with the
  /// commit timestamp (which is resolved form the resolved from `timestamp_hint`).
  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    committed_plm: &TMCommittedPLm<DropTablePayloadTypes>,
  ) -> HashMap<TNodePath, DropTableCommit> {
    // Compute the resolved timestamp
    let mut commit_timestamp = committed_plm.payload.timestamp_hint;
    commit_timestamp = max(commit_timestamp, ctx.table_generation.get_lat(&self.table_path) + 1);

    // Apply the Drop
    ctx.gen.0 += 1;
    ctx.table_generation.write(&self.table_path, None, commit_timestamp);

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
    let mut commits = HashMap::<TNodePath, DropTableCommit>::new();
    for rm in get_rms::<IO>(ctx, &self.table_path) {
      commits.insert(rm.clone(), DropTableCommit { timestamp: commit_timestamp });
    }
    commits
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> DropTableTMAborted {
    DropTableTMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> HashMap<TNodePath, DropTableAbort> {
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

    let mut aborts = HashMap::<TNodePath, DropTableAbort>::new();
    for rm in get_rms::<IO>(ctx, &self.table_path) {
      aborts.insert(rm.clone(), DropTableAbort {});
    }
    aborts
  }

  fn mk_closed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> DropTableTMClosed {
    DropTableTMClosed {}
  }

  fn closed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
    _: &TMClosedPLm<DropTablePayloadTypes>,
  ) {
  }

  fn node_died<IO: BasicIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
  }
}
