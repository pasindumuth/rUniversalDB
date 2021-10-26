use crate::common::{BasicIOCtx, TableSchema};
use crate::create_table_tm_es::{
  CreateTableClosed, CreateTableCommit, CreateTablePayloadTypes, CreateTablePrepare,
  CreateTablePrepared, CreateTableRMAborted, CreateTableRMCommitted, CreateTableRMPrepared,
};
use crate::model::common::{
  proc, ColName, ColType, Gen, QueryId, TablePath, TabletGroupId, TabletKeyRange, Timestamp,
};
use crate::multiversion_map::MVM;
use crate::server::ServerContextBase;
use crate::slave::SlaveContext;
use crate::stmpaxos2pc_rm::{STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use crate::stmpaxos2pc_tm::{PayloadTypes, RMCommittedPLm};
use crate::tablet::{plm, TabletContext, TabletCreateHelper, TabletPLm};
use rand::RngCore;

// -----------------------------------------------------------------------------------------------
//  CreateTableES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct CreateTableRMInner {
  pub tablet_group_id: TabletGroupId,
  pub table_path: TablePath,
  pub gen: Gen,

  pub key_range: TabletKeyRange,
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,

  /// This is populated if this ES completes with a Success. We cannot construct the
  /// new Tablet in here (since the IO type is BasicIOCtx), so we do this in the Slave node.
  pub committed_helper: Option<TabletCreateHelper>,
}

pub type CreateTableRMES = STMPaxos2PCRMOuter<CreateTablePayloadTypes, CreateTableRMInner>;

impl STMPaxos2PCRMInner<CreateTablePayloadTypes> for CreateTableRMInner {
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
      committed_helper: None,
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
      committed_helper: None,
    }
  }

  fn mk_closed() -> CreateTableClosed {
    CreateTableClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> CreateTableRMPrepared {
    CreateTableRMPrepared {
      tablet_group_id: self.tablet_group_id.clone(),
      table_path: self.table_path.clone(),
      gen: self.gen.clone(),
      key_range: self.key_range.clone(),
      key_cols: self.key_cols.clone(),
      val_cols: self.val_cols.clone(),
    }
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
    _: &RMCommittedPLm<CreateTablePayloadTypes>,
  ) {
    let mut rand_seed = [0; 16];
    io_ctx.rand().fill_bytes(&mut rand_seed);
    self.committed_helper = Some(TabletCreateHelper {
      rand_seed,
      this_slave_group_id: ctx.this_sid.clone(),
      this_tablet_group_id: self.tablet_group_id.clone(),
      this_eid: ctx.this_eid.clone(),
      gossip: ctx.gossip.clone(),
      leader_map: ctx.leader_map.clone(),
      this_table_path: self.table_path.clone(),
      this_table_key_range: self.key_range.clone(),
      table_schema: TableSchema {
        key_cols: self.key_cols.clone(),
        val_cols: MVM::init(self.val_cols.clone().into_iter().collect()),
      },
    });
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> CreateTableRMAborted {
    CreateTableRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(&mut self, _: &mut SlaveContext, _: &mut IO) {}
}
