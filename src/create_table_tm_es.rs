use crate::alter_table_tm_es::{maybe_respond_dead, ResponseData};
use crate::common::{BasicIOCtx, TableSchema};
use crate::master::{MasterContext, MasterPLm};
use crate::model::common::{
  proc, ColName, ColType, Gen, QueryId, SlaveGroupId, TSubNodePath, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::multiversion_map::MVM;
use crate::slave::{SlaveContext, SlavePLm};
use crate::stmpaxos2pc_tm::{
  Abort, Aborted, Closed, Commit, PayloadTypes, Prepare, Prepared, RMAbortedPLm, RMCommittedPLm,
  RMMessage, RMPLm, RMPreparedPLm, STMPaxos2PCTMInner, STMPaxos2PCTMOuter, TMAbortedPLm,
  TMClosedPLm, TMCommittedPLm, TMMessage, TMPLm, TMPreparedPLm,
};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Payloads
// -----------------------------------------------------------------------------------------------

// TM PLm

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableTMPrepared {
  pub table_path: TablePath,
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,
  pub shards: Vec<(TabletKeyRange, TabletGroupId, SlaveGroupId)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableTMCommitted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableTMAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableTMClosed {
  pub timestamp_hint: Option<Timestamp>,
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

// TM-to-RM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePrepare {
  /// Randomly generated by the Master for use by the new Tablet.
  pub tablet_group_id: TabletGroupId,
  /// The `TablePath` of the new Tablet
  pub table_path: TablePath,
  /// The `Gen` of the new Table.
  pub gen: Gen,
  /// The KeyRange that the Tablet created by this should be servicing.
  pub key_range: TabletKeyRange,

  /// The initial schema of the Table.
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAbort {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableCommit {}

// RM-to-TM

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePrepared {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableAborted {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTableClosed {}

// CreateTablePayloadTypes

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePayloadTypes {}

impl PayloadTypes for CreateTablePayloadTypes {
  // Master
  type RMPLm = SlavePLm;
  type TMPLm = MasterPLm;
  type RMPath = SlaveGroupId;
  type TMPath = ();
  type RMMessage = msg::SlaveRemotePayload;
  type TMMessage = msg::MasterRemotePayload;
  type NetworkMessageT = msg::NetworkMessage;
  type RMContext = SlaveContext;
  type TMContext = MasterContext;

  // TM PLm
  type TMPreparedPLm = CreateTableTMPrepared;
  type TMCommittedPLm = CreateTableTMCommitted;
  type TMAbortedPLm = CreateTableTMAborted;
  type TMClosedPLm = CreateTableTMClosed;

  fn tm_plm(plm: TMPLm<Self>) -> Self::TMPLm {
    MasterPLm::CreateTable(plm)
  }

  // RM PLm
  type RMPreparedPLm = CreateTableRMPrepared;
  type RMCommittedPLm = CreateTableRMCommitted;
  type RMAbortedPLm = CreateTableRMAborted;

  fn rm_plm(plm: RMPLm<Self>) -> Self::RMPLm {
    SlavePLm::CreateTable(plm)
  }

  // TM-to-RM Messages
  type Prepare = CreateTablePrepare;
  type Abort = CreateTableAbort;
  type Commit = CreateTableCommit;

  fn rm_msg(msg: RMMessage<Self>) -> Self::RMMessage {
    msg::SlaveRemotePayload::CreateTable(msg)
  }

  // RM-to-TM Messages
  type Prepared = CreateTablePrepared;
  type Aborted = CreateTableAborted;
  type Closed = CreateTableClosed;

  fn tm_msg(msg: TMMessage<Self>) -> Self::TMMessage {
    msg::MasterRemotePayload::CreateTable(msg)
  }
}

// -----------------------------------------------------------------------------------------------
//  CreateTable Implementation
// -----------------------------------------------------------------------------------------------

pub type CreateTableTMES = STMPaxos2PCTMOuter<CreateTablePayloadTypes, CreateTableTMInner>;

#[derive(Debug)]
pub struct CreateTableTMInner {
  // Response data
  pub response_data: Option<ResponseData>,

  // CreateTable Query data
  pub table_path: TablePath,
  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,
  pub shards: Vec<(TabletKeyRange, TabletGroupId, SlaveGroupId)>,

  /// This is set when `Committed` or `Aborted` gets inserted
  /// for use when constructing `Closed`.
  pub timestamp_hint: Option<Timestamp>,
}

impl CreateTableTMInner {
  /// Recompute the Gen of the Table that we are trying to create.
  fn compute_gen(&self, ctx: &mut MasterContext) -> Gen {
    if let Some(gen) = ctx.table_generation.get_last_present_version(&self.table_path) {
      Gen(gen.0 + 1)
    } else {
      Gen(0)
    }
  }

  /// Create the Table and return the `Timestamp` at which the Table has been created
  /// (based on the `timestamp_hint` and from GossipData).
  fn apply_create<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    timestamp_hint: Timestamp,
  ) -> Timestamp {
    let commit_timestamp = max(timestamp_hint, ctx.table_generation.get_lat(&self.table_path) + 1);
    let gen = self.compute_gen(ctx);

    // Update `table_generation`
    ctx.table_generation.write(&self.table_path, Some(gen.clone()), commit_timestamp);

    // Update `db_schema`
    let table_path_gen = (self.table_path.clone(), gen.clone());
    debug_assert!(!ctx.db_schema.contains_key(&table_path_gen));
    let mut val_cols = MVM::new();
    for (col_name, col_type) in &self.val_cols {
      val_cols.write(col_name, Some(col_type.clone()), commit_timestamp);
    }
    let table_schema = TableSchema { key_cols: self.key_cols.clone(), val_cols };
    ctx.db_schema.insert(table_path_gen.clone(), table_schema);

    // Update `sharding_config`.
    let mut stripped_shards = Vec::<(TabletKeyRange, TabletGroupId)>::new();
    for (key_range, tid, _) in &self.shards {
      stripped_shards.push((key_range.clone(), tid.clone()));
    }
    ctx.sharding_config.insert(table_path_gen.clone(), stripped_shards);

    // Update `tablet_address_config`.
    for (_, tid, sid) in &self.shards {
      ctx.tablet_address_config.insert(tid.clone(), sid.clone());
    }

    // Update Gossip Gen
    ctx.gen.0 += 1;

    commit_timestamp
  }
}

impl STMPaxos2PCTMInner<CreateTablePayloadTypes> for CreateTableTMInner {
  fn new_follower<IO: BasicIOCtx>(
    _: &mut MasterContext,
    _: &mut IO,
    payload: CreateTableTMPrepared,
  ) -> CreateTableTMInner {
    CreateTableTMInner {
      response_data: None,
      table_path: payload.table_path,
      key_cols: payload.key_cols,
      val_cols: payload.val_cols,
      shards: payload.shards,
      timestamp_hint: None,
    }
  }

  fn mk_prepared_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> CreateTableTMPrepared {
    CreateTableTMPrepared {
      table_path: self.table_path.clone(),
      key_cols: self.key_cols.clone(),
      val_cols: self.val_cols.clone(),
      shards: self.shards.clone(),
    }
  }

  fn prepared_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> BTreeMap<SlaveGroupId, CreateTablePrepare> {
    // The RMs are just the shards. Each shard should be in its own Slave.
    let mut prepares = BTreeMap::<SlaveGroupId, CreateTablePrepare>::new();
    let gen = self.compute_gen(ctx);
    for (key_range, tid, sid) in &self.shards {
      prepares.insert(
        sid.clone(),
        CreateTablePrepare {
          tablet_group_id: tid.clone(),
          table_path: self.table_path.clone(),
          gen: gen.clone(),
          key_range: key_range.clone(),
          key_cols: self.key_cols.clone(),
          val_cols: self.val_cols.clone(),
        },
      );
    }
    debug_assert_eq!(prepares.len(), self.shards.len());
    prepares
  }

  fn mk_committed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
    _: &BTreeMap<SlaveGroupId, CreateTablePrepared>,
  ) -> CreateTableTMCommitted {
    CreateTableTMCommitted {}
  }

  /// Apply this `alter_op` to the system and construct Commit messages with the
  /// commit timestamp (which is resolved form the resolved from `timestamp_hint`).
  fn committed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
    _: &TMCommittedPLm<CreateTablePayloadTypes>,
  ) -> BTreeMap<SlaveGroupId, CreateTableCommit> {
    self.timestamp_hint = None;

    // The RMs are just the shards. Each shard should be in its own Slave.
    let mut commits = BTreeMap::<SlaveGroupId, CreateTableCommit>::new();
    for (_, _, sid) in &self.shards {
      commits.insert(sid.clone(), CreateTableCommit {});
    }
    debug_assert_eq!(commits.len(), self.shards.len());
    commits
  }

  fn mk_aborted_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> CreateTableTMAborted {
    CreateTableTMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> BTreeMap<SlaveGroupId, CreateTableAbort> {
    self.timestamp_hint = Some(io_ctx.now());

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

    let mut aborts = BTreeMap::<SlaveGroupId, CreateTableAbort>::new();
    for (_, _, sid) in &self.shards {
      aborts.insert(sid.clone(), CreateTableAbort {});
    }
    aborts
  }

  fn mk_closed_plm<IO: BasicIOCtx>(
    &mut self,
    _: &mut MasterContext,
    _: &mut IO,
  ) -> CreateTableTMClosed {
    CreateTableTMClosed { timestamp_hint: self.timestamp_hint }
  }

  fn closed_plm_inserted<IO: BasicIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    closed_plm: &TMClosedPLm<CreateTablePayloadTypes>,
  ) {
    if let Some(timestamp_hint) = closed_plm.payload.timestamp_hint {
      // This means we closed_plm as a result of a committing the CreateTable.
      let commit_timestamp = self.apply_create(ctx, io_ctx, timestamp_hint);

      if ctx.is_leader() {
        // Respond to the External.
        // Note: Recall we will already have responded if the CreateTable had failed.
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
    }
  }

  fn node_died<IO: BasicIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    maybe_respond_dead(&mut self.response_data, ctx, io_ctx);
  }
}
