use crate::common::{BasicIOCtx, TableSchema};
use crate::common::{
  ColName, ColType, Gen, SlaveGroupId, TablePath, TabletGroupId, TabletKeyRange,
};
use crate::create_table_tm_es::{
  CreateTableClosed, CreateTableCommit, CreateTablePrepare, CreateTablePrepared,
  CreateTableTMPayloadTypes,
};
use crate::message as msg;
use crate::multiversion_map::MVM;
use crate::server::ServerContextBase;
use crate::slave::{SlaveContext, SlavePLm};
use crate::stmpaxos2pc_rm::{
  RMCommittedPLm, RMPLm, RMPayloadTypes, RMServerContext, STMPaxos2PCRMAction, STMPaxos2PCRMInner,
  STMPaxos2PCRMOuter,
};
use crate::stmpaxos2pc_tm::TMMessage;
use crate::tablet::{TabletConfig, TabletCreateHelper};
use rand::RngCore;
use serde::{Deserialize, Serialize};

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableRMPayloadTypes {}

impl RMPayloadTypes for CreateTableRMPayloadTypes {
  type TM = CreateTableTMPayloadTypes;
  type RMContext = SlaveContext;

  // Actions
  type RMCommitActionData = TabletCreateHelper;

  // RM PLm
  type RMPreparedPLm = CreateTableRMPrepared;
  type RMCommittedPLm = CreateTableRMCommitted;
  type RMAbortedPLm = CreateTableRMAborted;
}

// RM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableRMPrepared {
  pub tablet_group_id: TabletGroupId,
  pub table_path: TablePath,
  pub gen: Gen,

  pub key_range: TabletKeyRange,
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableRMCommitted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableRMAborted {}

// -----------------------------------------------------------------------------------------------
//  RMServerContext
// -----------------------------------------------------------------------------------------------

impl RMServerContext<CreateTableRMPayloadTypes> for SlaveContext {
  fn push_plm(&mut self, plm: RMPLm<CreateTableRMPayloadTypes>) {
    self.slave_bundle.plms.push(SlavePLm::CreateTable(plm));
  }

  fn send_to_tm<IO: BasicIOCtx>(
    &mut self,
    io_ctx: &mut IO,
    _: &(),
    msg: TMMessage<CreateTableTMPayloadTypes>,
  ) {
    self.send_to_master(io_ctx, msg::MasterRemotePayload::CreateTable(msg));
  }

  fn mk_node_path(&self) -> SlaveGroupId {
    self.this_sid.clone()
  }

  fn is_leader(&self) -> bool {
    SlaveContext::is_leader(self)
  }
}

// -----------------------------------------------------------------------------------------------
//  CreateTableES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableRMInner {
  pub tablet_group_id: TabletGroupId,
  pub table_path: TablePath,
  pub gen: Gen,

  pub key_range: TabletKeyRange,
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,
}

pub type CreateTableRMES = STMPaxos2PCRMOuter<CreateTableRMPayloadTypes, CreateTableRMInner>;
pub type CreateTableRMAction = STMPaxos2PCRMAction<CreateTableRMPayloadTypes>;

impl STMPaxos2PCRMInner<CreateTableRMPayloadTypes> for CreateTableRMInner {
  fn new<IO: BasicIOCtx>(
    _: &mut SlaveContext,
    _: &mut IO,
    payload: CreateTablePrepare,
  ) -> CreateTableRMInner {
    CreateTableRMInner {
      tablet_group_id: payload.tablet_group_id,
      table_path: payload.table_path,
      gen: payload.gen,
      key_range: payload.key_range,
      key_cols: payload.key_cols,
      val_cols: payload.val_cols,
    }
  }

  fn new_follower<IO: BasicIOCtx>(
    _: &mut SlaveContext,
    _: &mut IO,
    payload: CreateTableRMPrepared,
  ) -> CreateTableRMInner {
    CreateTableRMInner {
      tablet_group_id: payload.tablet_group_id,
      table_path: payload.table_path,
      gen: payload.gen,
      key_range: payload.key_range,
      key_cols: payload.key_cols,
      val_cols: payload.val_cols,
    }
  }

  fn mk_closed() -> CreateTableClosed {
    CreateTableClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> Option<CreateTableRMPrepared> {
    Some(CreateTableRMPrepared {
      tablet_group_id: self.tablet_group_id.clone(),
      table_path: self.table_path.clone(),
      gen: self.gen.clone(),
      key_range: self.key_range.clone(),
      key_cols: self.key_cols.clone(),
      val_cols: self.val_cols.clone(),
    })
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> CreateTablePrepared {
    CreateTablePrepared {}
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &CreateTableCommit,
  ) -> CreateTableRMCommitted {
    CreateTableRMCommitted {}
  }

  /// Construct `TabletCreateHelper` so an appropriate Tablet can be constructed.
  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut SlaveContext,
    io_ctx: &mut IO,
    _: &RMCommittedPLm<CreateTableRMPayloadTypes>,
  ) -> TabletCreateHelper {
    let mut rand_seed = [0; 16];
    io_ctx.rand().fill_bytes(&mut rand_seed);
    TabletCreateHelper {
      tablet_config: TabletConfig {
        timestamp_suffix_divisor: ctx.slave_config.timestamp_suffix_divisor,
      },
      this_sid: ctx.this_sid.clone(),
      this_tid: self.tablet_group_id.clone(),
      this_eid: ctx.this_eid.clone(),
      gossip: ctx.gossip.clone(),
      leader_map: ctx.leader_map.value().clone(),
      this_table_path: self.table_path.clone(),
      this_table_key_range: self.key_range.clone(),
      table_schema: TableSchema {
        key_cols: self.key_cols.clone(),
        val_cols: MVM::init(self.val_cols.clone().into_iter().collect()),
      },
    }
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> CreateTableRMAborted {
    CreateTableRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(&mut self, _: &mut SlaveContext, _: &mut IO) {}

  fn reconfig_snapshot(&self) -> CreateTableRMInner {
    self.clone()
  }
}
