use crate::alter_table_tm_es::{get_rms, ResponseData};
use crate::common::{cur_timestamp, mk_t, BasicIOCtx, GeneralTraceMessage, Timestamp};
use crate::common::{TNodePath, TablePath};
use crate::master::{MasterContext, MasterPLm};
use crate::message as msg;
use crate::server::ServerContextBase;
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
pub struct DropTableTMPayloadTypes {}

impl TMPayloadTypes for DropTableTMPayloadTypes {
  // Master
  type RMPath = TNodePath;
  type TMPath = ();
  type NetworkMessageT = msg::NetworkMessage;
  type TMContext = MasterContext;

  // TM PLm
  type TMPreparedPLm = DropTableTMPrepared;
  type TMCommittedPLm = DropTableTMCommitted;
  type TMAbortedPLm = DropTableTMAborted;
  type TMClosedPLm = DropTableTMClosed;

  // TM-to-RM Messages
  type Prepare = DropTablePrepare;
  type Abort = DropTableAbort;
  type Commit = DropTableCommit;

  // RM-to-TM Messages
  type Prepared = DropTablePrepared;
  type Aborted = DropTableAborted;
  type Closed = DropTableClosed;
}

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

// -----------------------------------------------------------------------------------------------
//  TMServerContext DropTable
// -----------------------------------------------------------------------------------------------

impl TMServerContext<DropTableTMPayloadTypes> for MasterContext {
  fn push_plm(&mut self, plm: TMPLm<DropTableTMPayloadTypes>) {
    self.master_bundle.plms.push(MasterPLm::DropTable(plm));
  }

  fn send_to_rm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    rm: &TNodePath,
    msg: RMMessage<DropTableTMPayloadTypes>,
  ) {
    self.send_to_t(io_ctx, rm.clone(), msg::TabletMessage::DropTable(msg));
  }

  fn mk_node_path(&self) -> () {
    ()
  }

  fn is_leader(&self) -> bool {
    MasterContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  DropTable Implementation
// -----------------------------------------------------------------------------------------------

pub type DropTableTMES = STMPaxos2PCTMOuter<DropTableTMPayloadTypes, DropTableTMInner>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DropTableTMInner {
  // Response data
  pub response_data: Option<ResponseData>,

  // DropTable Query data
  pub table_path: TablePath,
}

impl STMPaxos2PCTMInner<DropTableTMPayloadTypes> for DropTableTMInner {
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
    committed_plm: &TMCommittedPLm<DropTableTMPayloadTypes>,
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
      }
    }

    // Trace this commit.
    io_ctx.general_trace(GeneralTraceMessage::CommittedQueryId(
      committed_plm.query_id.clone(),
      timestamp.clone(),
    ));

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
    _: &TMClosedPLm<DropTableTMPayloadTypes>,
  ) {
  }

  fn leader_changed<IO: BasicIOCtx>(&mut self, _: &mut MasterContext, _: &mut IO) {
    self.response_data = None;
  }

  fn reconfig_snapshot(&self) -> DropTableTMInner {
    DropTableTMInner { response_data: None, table_path: self.table_path.clone() }
  }
}
