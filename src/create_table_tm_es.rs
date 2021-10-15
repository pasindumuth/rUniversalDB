use crate::common::{MasterIOCtx, RemoteLeaderChangedPLm, TableSchema};
use crate::master::MasterPLm::CreateTableTMPrepared;
use crate::master::{plm, MasterContext, MasterPLm};
use crate::model::common::{
  ColName, ColType, EndpointId, Gen, QueryId, RequestId, SlaveGroupId, TablePath, TabletGroupId,
  TabletKeyRange, Timestamp,
};
use crate::model::message as msg;
use crate::multiversion_map::MVM;
use crate::server::ServerContextBase;
use sqlparser::ast::WindowFrameBound::Following;
use std::cmp::max;
use std::collections::{HashMap, HashSet};

// -----------------------------------------------------------------------------------------------
//  General STMPaxos2PC TM Types
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct ResponseData {
  pub request_id: RequestId,
  pub sender_eid: EndpointId,
}

// -----------------------------------------------------------------------------------------------
//  CreateTableTMES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum Follower {
  Preparing,
  Committed,
  Aborted,
}

#[derive(Debug)]
pub struct Preparing {
  /// The set of RMs that still have not prepared.
  rms_remaining: HashSet<SlaveGroupId>,
}

#[derive(Debug)]
pub struct Committed {
  /// The set of RMs that still have not committed.
  rms_remaining: HashSet<SlaveGroupId>,
}

#[derive(Debug)]
pub struct Aborted {
  /// The set of RMs that still have not aborted.
  rms_remaining: HashSet<SlaveGroupId>,
}

#[derive(Debug)]
pub enum InsertingTMClosed {
  Committed,
  Aborted,
}

#[derive(Debug)]
pub enum CreateTableTMS {
  Start,
  Follower(Follower),
  WaitingInsertTMPrepared,
  InsertTMPreparing,
  Preparing(Preparing),
  InsertingTMCommitted,
  Committed(Committed),
  InsertingTMAborted,
  Aborted(Aborted),
  InsertingTMClosed(InsertingTMClosed),
}

#[derive(Debug)]
pub struct CreateTableTMES {
  // Response data
  pub response_data: Option<ResponseData>,

  // CreateTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,

  pub key_cols: Vec<(ColName, ColType)>,
  pub val_cols: Vec<(ColName, ColType)>,

  pub shards: Vec<(TabletKeyRange, TabletGroupId, SlaveGroupId)>,

  // STMPaxos2PCTM state
  pub state: CreateTableTMS,
}

#[derive(Debug)]
pub enum CreateTableTMAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl CreateTableTMES {
  // STMPaxos2PC messages

  pub fn handle_prepared<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
    prepared: msg::CreateTablePrepared,
  ) -> CreateTableTMAction {
    match &mut self.state {
      CreateTableTMS::Preparing(preparing) => {
        if preparing.rms_remaining.remove(&prepared.sid) {
          if preparing.rms_remaining.is_empty() {
            // All RMs have prepared
            ctx.master_bundle.plms.push(MasterPLm::CreateTableTMCommitted(
              plm::CreateTableTMCommitted { query_id: self.query_id.clone() },
            ));
            self.state = CreateTableTMS::InsertingTMCommitted;
          }
        }
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }

  pub fn handle_aborted<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> CreateTableTMAction {
    match &self.state {
      CreateTableTMS::Preparing(_) => {
        ctx.master_bundle.plms.push(MasterPLm::CreateTableTMAborted(plm::CreateTableTMAborted {
          query_id: self.query_id.clone(),
        }));
        self.state = CreateTableTMS::InsertingTMAborted;
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }

  pub fn handle_close_confirmed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    closed: msg::CreateTableCloseConfirm,
  ) -> CreateTableTMAction {
    match &mut self.state {
      CreateTableTMS::Committed(committed) => {
        if committed.rms_remaining.remove(&closed.sid) {
          if committed.rms_remaining.is_empty() {
            // All RMs have closed
            let timestamp_hint = io_ctx.now();
            ctx.master_bundle.plms.push(MasterPLm::CreateTableTMClosed(plm::CreateTableTMClosed {
              query_id: self.query_id.clone(),
              timestamp_hint: Some(timestamp_hint),
            }));
            self.state = CreateTableTMS::InsertingTMClosed(InsertingTMClosed::Committed);
          }
        }
      }
      CreateTableTMS::Aborted(aborted) => {
        if aborted.rms_remaining.remove(&closed.sid) {
          if aborted.rms_remaining.is_empty() {
            // All RMs have closed
            ctx.master_bundle.plms.push(MasterPLm::CreateTableTMClosed(plm::CreateTableTMClosed {
              query_id: self.query_id.clone(),
              timestamp_hint: None,
            }));
            self.state = CreateTableTMS::InsertingTMClosed(InsertingTMClosed::Committed);
          }
        }
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }

  // STMPaxos2PC PLm Insertions

  /// Recompute the Gen of the Table that we are trying to create.
  fn compute_gen<IO: MasterIOCtx>(&self, ctx: &mut MasterContext, _: &mut IO) -> Gen {
    if let Some(gen) = ctx.table_generation.get_last_present_version(&self.table_path) {
      Gen(gen.0 + 1)
    } else {
      Gen(0)
    }
  }

  /// Change state to `Preparing` and broadcast `CreateTablePrepare` to the RMs.
  fn advance_to_prepared<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    // The RMs are just the shards. Each shard should be in its own Slave.
    let mut rms_remaining = HashSet::<SlaveGroupId>::new();
    let gen = self.compute_gen(ctx, io_ctx);
    for (key_range, tid, sid) in &self.shards {
      rms_remaining.insert(sid.clone());
      ctx.ctx(io_ctx).send_to_slave_common(
        sid.clone(),
        msg::SlaveRemotePayload::CreateTablePrepare(msg::CreateTablePrepare {
          query_id: self.query_id.clone(),
          tablet_group_id: tid.clone(),
          table_path: self.table_path.clone(),
          gen: gen.clone(),
          key_range: key_range.clone(),
          key_cols: self.key_cols.clone(),
          val_cols: self.val_cols.clone(),
        }),
      );
    }
    debug_assert_eq!(rms_remaining.len(), self.shards.len());
    self.state = CreateTableTMS::Preparing(Preparing { rms_remaining });
  }

  pub fn handle_prepared_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> CreateTableTMAction {
    match &self.state {
      CreateTableTMS::InsertTMPreparing => {
        self.advance_to_prepared(ctx, io_ctx);
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }

  /// Change state to `Aborted` and broadcast `CreateTableAbort` to the RMs.
  fn advance_to_aborted<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    let mut rms_remaining = HashSet::<SlaveGroupId>::new();
    for (_, _, sid) in &self.shards {
      rms_remaining.insert(sid.clone());
      ctx.ctx(io_ctx).send_to_slave_common(
        sid.clone(),
        msg::SlaveRemotePayload::CreateTableAbort(msg::CreateTableAbort {
          query_id: self.query_id.clone(),
        }),
      );
    }

    self.state = CreateTableTMS::Aborted(Aborted { rms_remaining });
  }

  pub fn handle_aborted_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> CreateTableTMAction {
    match &self.state {
      CreateTableTMS::Start => {}
      CreateTableTMS::Follower(_) => {
        self.state = CreateTableTMS::Follower(Follower::Aborted);
      }
      CreateTableTMS::InsertingTMAborted => {
        // Respond to the External
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
          )
        }

        self.advance_to_aborted(ctx, io_ctx);
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }

  /// Change state to `Committed` and broadcast `CreateTableCommit` to the RMs.
  fn advance_to_committed<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    // The RMs are just the shards. Each shard should be in its own Slave.
    let mut rms_remaining = HashSet::<SlaveGroupId>::new();
    for (_, _, sid) in &self.shards {
      rms_remaining.insert(sid.clone());
      ctx.ctx(io_ctx).send_to_slave_common(
        sid.clone(),
        msg::SlaveRemotePayload::CreateTableCommit(msg::CreateTableCommit {
          query_id: self.query_id.clone(),
        }),
      );
    }
    debug_assert_eq!(rms_remaining.len(), self.shards.len());
    self.state = CreateTableTMS::Committed(Committed { rms_remaining });
  }

  pub fn handle_committed_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> CreateTableTMAction {
    match &mut self.state {
      CreateTableTMS::Follower(_) => {
        self.state = CreateTableTMS::Follower(Follower::Committed);
      }
      CreateTableTMS::InsertingTMCommitted => {
        self.advance_to_committed(ctx, io_ctx);
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }

  /// Create the Table and return the `Timestamp` at which the Table has been created
  /// (based on the `timestamp_hint` and from GossipData).
  fn apply_create<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    timestamp_hint: Timestamp,
  ) -> Timestamp {
    let commit_timestamp = max(timestamp_hint, ctx.table_generation.get_lat(&self.table_path) + 1);
    let gen = self.compute_gen(ctx, io_ctx);

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

    commit_timestamp
  }

  pub fn handle_closed_plm<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    closed: plm::CreateTableTMClosed,
  ) -> CreateTableTMAction {
    match &self.state {
      CreateTableTMS::Follower(_) => {
        if let Some(timestamp_hint) = closed.timestamp_hint {
          self.apply_create(ctx, io_ctx, timestamp_hint);
        }
        CreateTableTMAction::Exit
      }
      CreateTableTMS::InsertingTMClosed(_) => {
        if let Some(timestamp_hint) = closed.timestamp_hint {
          let commit_timestamp = self.apply_create(ctx, io_ctx, timestamp_hint);

          // Respond to the External
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
            )
          }

          // Broadcast a GossipData
          ctx.broadcast_gossip(io_ctx);
        }
        CreateTableTMAction::Exit
      }
      _ => CreateTableTMAction::Wait,
    }
  }

  // Other

  pub fn start_inserting<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    _: &mut IO,
  ) -> CreateTableTMAction {
    match self.state {
      CreateTableTMS::WaitingInsertTMPrepared => {
        ctx.master_bundle.plms.push(MasterPLm::CreateTableTMPrepared(plm::CreateTableTMPrepared {
          query_id: self.query_id.clone(),
          table_path: self.table_path.clone(),
          key_cols: self.key_cols.clone(),
          val_cols: self.val_cols.clone(),
          shards: self.shards.clone(),
        }));
        self.state = CreateTableTMS::InsertTMPreparing;
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }

  pub fn leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
  ) -> CreateTableTMAction {
    match &self.state {
      CreateTableTMS::Start => CreateTableTMAction::Wait,
      CreateTableTMS::Follower(follower) => {
        if ctx.is_leader() {
          match follower {
            Follower::Preparing => {
              self.advance_to_prepared(ctx, io_ctx);
            }
            Follower::Committed => {
              self.advance_to_committed(ctx, io_ctx);
            }
            Follower::Aborted => {
              self.advance_to_aborted(ctx, io_ctx);
            }
          }
        }
        CreateTableTMAction::Wait
      }
      CreateTableTMS::WaitingInsertTMPrepared => {
        self.maybe_respond_dead(ctx, io_ctx);
        CreateTableTMAction::Exit
      }
      CreateTableTMS::InsertTMPreparing => {
        self.maybe_respond_dead(ctx, io_ctx);
        CreateTableTMAction::Exit
      }
      CreateTableTMS::Preparing(_)
      | CreateTableTMS::InsertingTMCommitted
      | CreateTableTMS::InsertingTMAborted => {
        self.state = CreateTableTMS::Follower(Follower::Preparing);
        self.maybe_respond_dead(ctx, io_ctx);
        CreateTableTMAction::Wait
      }
      CreateTableTMS::Committed(_) => {
        self.state = CreateTableTMS::Follower(Follower::Committed);
        self.maybe_respond_dead(ctx, io_ctx);
        CreateTableTMAction::Wait
      }
      CreateTableTMS::Aborted(_) => {
        self.state = CreateTableTMS::Follower(Follower::Aborted);
        self.maybe_respond_dead(ctx, io_ctx);
        CreateTableTMAction::Wait
      }
      CreateTableTMS::InsertingTMClosed(closed) => {
        self.state = CreateTableTMS::Follower(match closed {
          InsertingTMClosed::Committed => Follower::Committed,
          InsertingTMClosed::Aborted => Follower::Aborted,
        });
        self.maybe_respond_dead(ctx, io_ctx);
        CreateTableTMAction::Wait
      }
    }
  }

  pub fn remote_leader_changed<IO: MasterIOCtx>(
    &mut self,
    ctx: &mut MasterContext,
    io_ctx: &mut IO,
    remote_leader_changed: RemoteLeaderChangedPLm,
  ) -> CreateTableTMAction {
    match &self.state {
      CreateTableTMS::Preparing(preparing) => {
        let gen = self.compute_gen(ctx, io_ctx);
        for (key_range, tid, sid) in &self.shards {
          if preparing.rms_remaining.contains(sid) && sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_slave_common(
              sid.clone(),
              msg::SlaveRemotePayload::CreateTablePrepare(msg::CreateTablePrepare {
                query_id: self.query_id.clone(),
                tablet_group_id: tid.clone(),
                table_path: self.table_path.clone(),
                gen: gen.clone(),
                key_range: key_range.clone(),
                key_cols: self.key_cols.clone(),
                val_cols: self.val_cols.clone(),
              }),
            );
          }
        }
      }
      CreateTableTMS::Committed(committed) => {
        for (_, _, sid) in &self.shards {
          if committed.rms_remaining.contains(sid) && sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_slave_common(
              sid.clone(),
              msg::SlaveRemotePayload::CreateTableCommit(msg::CreateTableCommit {
                query_id: self.query_id.clone(),
              }),
            );
          }
        }
      }
      CreateTableTMS::Aborted(aborted) => {
        for (_, _, sid) in &self.shards {
          if aborted.rms_remaining.contains(sid) && sid.to_gid() == remote_leader_changed.gid {
            ctx.ctx(io_ctx).send_to_slave_common(
              sid.clone(),
              msg::SlaveRemotePayload::CreateTableAbort(msg::CreateTableAbort {
                query_id: self.query_id.clone(),
              }),
            );
          }
        }
      }
      _ => {}
    }
    CreateTableTMAction::Wait
  }

  fn maybe_respond_dead<IO: MasterIOCtx>(&mut self, ctx: &mut MasterContext, io_ctx: &mut IO) {
    if let Some(response_data) = &self.response_data {
      ctx.external_request_id_map.remove(&response_data.request_id);
      io_ctx.send(
        &response_data.sender_eid,
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
          msg::ExternalDDLQueryAborted {
            request_id: response_data.request_id.clone(),
            payload: msg::ExternalDDLQueryAbortData::NodeDied,
          },
        )),
      );
      self.response_data = None;
    }
  }
}
