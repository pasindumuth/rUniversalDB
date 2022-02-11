use crate::alter_table_tm_es::{get_rms, maybe_respond_dead, ResponseData};
use crate::common::{cur_timestamp, mk_t, BasicIOCtx, GeneralTraceMessage, Timestamp};
use crate::master::{MasterContext, MasterPLm};
use crate::model::common::{TNodePath, TablePath};
use crate::model::message as msg;
use crate::stmpaxos2pc_tm::{
  PayloadTypes, RMMessage, RMPLm, STMPaxos2PCTMInner, STMPaxos2PCTMOuter, TMClosedPLm,
  TMCommittedPLm, TMMessage, TMPLm,
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
  type RMPLm = TabletPLm;
  type TMPLm = MasterPLm;
  type RMPath = TNodePath;
  type TMPath = ();
  type RMMessage = msg::TabletMessage;
  type TMMessage = msg::MasterRemotePayload;
  type NetworkMessageT = msg::NetworkMessage;
  type RMContext = TabletContext;
  type TMContext = MasterContext;

  // Actions
  type RMCommitActionData = Timestamp;

  // TM PLm
  type TMPreparedPLm = DropTableTMPrepared;
  type TMCommittedPLm = DropTableTMCommitted;
  type TMAbortedPLm = DropTableTMAborted;
  type TMClosedPLm = DropTableTMClosed;

  fn tm_plm(plm: TMPLm<Self>) -> Self::TMPLm {
    MasterPLm::DropTable(plm)
  }

  // RM PLm
  type RMPreparedPLm = DropTableRMPrepared;
  type RMCommittedPLm = DropTableRMCommitted;
  type RMAbortedPLm = DropTableRMAborted;

  fn rm_plm(plm: RMPLm<Self>) -> Self::RMPLm {
    TabletPLm::DropTable(plm)
  }

  // TM-to-RM Messages
  type Prepare = DropTablePrepare;
  type Abort = DropTableAbort;
  type Commit = DropTableCommit;

  fn rm_msg(msg: RMMessage<Self>) -> Self::RMMessage {
    msg::TabletMessage::DropTable(msg)
  }

  // RM-to-TM Messages
  type Prepared = DropTablePrepared;
  type Aborted = DropTableAborted;
  type Closed = DropTableClosed;

  fn tm_msg(msg: TMMessage<Self>) -> Self::TMMessage {
    msg::MasterRemotePayload::DropTable(msg)
  }
}

// -----------------------------------------------------------------------------------------------
//  DropTable Implementation
// -----------------------------------------------------------------------------------------------

pub type DropTableTMES = STMPaxos2PCTMOuter<DropTablePayloadTypes, DropTableTMInner>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableTMInner {
  // Response data
  pub response_data: Option<ResponseData>,

  // DropTable Query data
  pub table_path: TablePath,
}

impl STMPaxos2PCTMInner<DropTablePayloadTypes> for DropTableTMInner {
  fn new_follower<IO: BasicIOCtx>(
    _: &mut MasterContext,
    _: &mut IO,
    payload: DropTableTMPrepared,
  ) -> DropTableTMInner {
    DropTableTMInner { response_data: None, table_path: payload.table_path }
  }

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
  ) -> BTreeMap<TNodePath, DropTablePrepare> {
    let mut prepares = BTreeMap::<TNodePath, DropTablePrepare>::new();
    for rm in get_rms::<IO>(&ctx.gossip.get(), &self.table_path) {
      prepares.insert(rm.clone(), DropTablePrepare {});
    }
    prepares
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    prepared: &BTreeMap<TNodePath, DropTablePrepared>,
  ) -> DropTableTMCommitted {
    let mut timestamp_hint = cur_timestamp(io_ctx, ctx.master_config.timestamp_suffix_divisor);
    for (_, prepared) in prepared {
      timestamp_hint = max(timestamp_hint, prepared.timestamp.clone());
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
  ) -> BTreeMap<TNodePath, DropTableCommit> {
    let (timestamp, rms) = ctx.gossip.update(|gossip| {
      // Compute the resolved timestamp
      let mut timestamp = committed_plm.payload.timestamp_hint.clone();
      timestamp = max(timestamp, gossip.table_generation.get_lat(&self.table_path).add(mk_t(1)));

      // The the RMs before dropping
      let rms = get_rms::<IO>(&gossip.get(), &self.table_path);

      // Apply the Drop
      gossip.table_generation.write(&self.table_path, None, timestamp.clone());

      (timestamp, rms)
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
      } else {
        // This means the query succeeded but this is a backup. Thus, we
        // record the success in a trace message.
        io_ctx.general_trace(GeneralTraceMessage::CommittedQueryId(
          committed_plm.query_id.clone(),
          timestamp.clone(),
        ));
      }
    }

    // Send out GossipData to all Slaves.
    ctx.broadcast_gossip(io_ctx);

    // Return Commit messages
    let mut commits = BTreeMap::<TNodePath, DropTableCommit>::new();
    for rm in rms {
      commits.insert(rm.clone(), DropTableCommit { timestamp: timestamp.clone() });
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
  ) -> BTreeMap<TNodePath, DropTableAbort> {
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

    let mut aborts = BTreeMap::<TNodePath, DropTableAbort>::new();
    for rm in get_rms::<IO>(&ctx.gossip.get(), &self.table_path) {
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

  fn reconfig_snapshot(&self) -> DropTableTMInner {
    DropTableTMInner { response_data: None, table_path: self.table_path.clone() }
  }
}
