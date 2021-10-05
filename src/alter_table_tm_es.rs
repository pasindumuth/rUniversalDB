use crate::common::{GossipData, IOTypes, NetworkOut};
use crate::create_table_tm_es::{ResponseData, TMClosedState};
use crate::master::{plm, MasterContext, MasterPLm};
use crate::model::common::{
  proc, EndpointId, Gen, QueryId, RequestId, TablePath, TabletGroupId, Timestamp,
};
use crate::model::message as msg;
use std::cmp::max;
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  AlterTableES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub enum Follower {
  Preparing,
  Committed(Timestamp),
  Closed,
}

#[derive(Debug)]
pub enum AlterTableTMS {
  Start,
  Follower(Follower),
  WaitingInsertTMPrepared,
  InsertTMPreparing,
  Preparing(HashMap<TabletGroupId, Option<Timestamp>>),
  InsertingTMCommitted,
  Committed(Timestamp, HashMap<TabletGroupId, Option<()>>),
  InsertingTMAborted,
  Aborted(HashMap<TabletGroupId, Option<()>>),
  InsertingTMClosed(TMClosedState),
}

#[derive(Debug)]
pub struct AlterTableTMES {
  // Response data
  pub response_data: Option<ResponseData>,

  // AlterTable Query data
  pub query_id: QueryId,
  pub table_path: TablePath,
  pub alter_op: proc::AlterOp,

  // STMPaxos2PCTM state
  pub state: AlterTableTMS,
}

#[derive(Debug)]
pub enum AlterTableTMAction {
  Wait,
  Exit,
}

// -----------------------------------------------------------------------------------------------
//  Implementatiion
// -----------------------------------------------------------------------------------------------

impl AlterTableTMES {
  pub fn handle_prepared_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> AlterTableTMAction {
    match self.state {
      AlterTableTMS::Start => {}
      AlterTableTMS::Follower(_) => {}
      AlterTableTMS::WaitingInsertTMPrepared => {}
      AlterTableTMS::InsertTMPreparing => {
        let mut state = HashMap::<TabletGroupId, Option<Timestamp>>::new();
        for tid in get_rms(ctx, &self.table_path) {
          state.insert(tid, None);
        }
        self.state = AlterTableTMS::Preparing(state);
      }
      AlterTableTMS::Preparing(_) => {}
      AlterTableTMS::InsertingTMCommitted => {}
      AlterTableTMS::Committed(_, _) => {}
      AlterTableTMS::InsertingTMAborted => {}
      AlterTableTMS::Aborted(_) => {}
      AlterTableTMS::InsertingTMClosed(_) => {}
    }
    AlterTableTMAction::Wait
  }

  pub fn handle_aborted_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> AlterTableTMAction {
    AlterTableTMAction::Wait
  }

  pub fn handle_committed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> AlterTableTMAction {
    AlterTableTMAction::Wait
  }

  pub fn handle_closed_plm<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> AlterTableTMAction {
    AlterTableTMAction::Wait
  }

  pub fn starting_insert<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> AlterTableTMAction {
    match self.state {
      AlterTableTMS::WaitingInsertTMPrepared => {
        ctx.master_bundle.push(MasterPLm::AlterTableTMPrepared(plm::AlterTableTMPrepared {
          query_id: self.query_id.clone(),
          table_path: self.table_path.clone(),
          alter_op: self.alter_op.clone(),
        }));
        self.state = AlterTableTMS::InsertTMPreparing;
      }
      _ => {}
    }
    AlterTableTMAction::Wait
  }

  pub fn leader_changed<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> AlterTableTMAction {
    match self.state {
      AlterTableTMS::Start => AlterTableTMAction::Wait,
      AlterTableTMS::Follower(_) => AlterTableTMAction::Wait,
      AlterTableTMS::WaitingInsertTMPrepared => self.maybe_respond_dead(ctx),
      AlterTableTMS::InsertTMPreparing => self.maybe_respond_dead(ctx),
      AlterTableTMS::Preparing(_) => AlterTableTMAction::Wait,
      AlterTableTMS::InsertingTMCommitted => AlterTableTMAction::Wait,
      AlterTableTMS::Committed(_, _) => AlterTableTMAction::Wait,
      AlterTableTMS::InsertingTMAborted => AlterTableTMAction::Wait,
      AlterTableTMS::Aborted(_) => AlterTableTMAction::Wait,
      AlterTableTMS::InsertingTMClosed(_) => AlterTableTMAction::Wait,
    }
  }

  pub fn maybe_respond_dead<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
  ) -> AlterTableTMAction {
    if let Some(response_data) = &self.response_data {
      ctx.network_output.send(
        &response_data.sender_eid,
        msg::NetworkMessage::External(msg::ExternalMessage::ExternalDDLQueryAborted(
          msg::ExternalDDLQueryAborted {
            request_id: response_data.request_id.clone(),
            payload: msg::ExternalDDLQueryAbortData::NodeDied,
          },
        )),
      )
    }
    AlterTableTMAction::Exit
  }
}

/// This returns the current set of RMs used by the `AlterTableTMES`. Recall that while
/// the ES is alive, we ensure that this is idempotent.
fn get_rms<T: IOTypes>(ctx: &mut MasterContext<T>, table_path: &TablePath) -> Vec<TabletGroupId> {
  let gen = ctx.table_generation.get_last_version(table_path).unwrap();
  let mut rms = Vec::<TabletGroupId>::new();
  for (_, tid) in ctx.sharding_config.get(&(table_path.clone(), gen.clone())).unwrap() {
    rms.push(tid.clone());
  }
  rms
}
