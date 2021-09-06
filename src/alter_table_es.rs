use crate::common::{GossipData, GossipDataSer, IOTypes, NetworkOut};
use crate::master::MasterContext;
use crate::model::common::{
  proc, EndpointId, QueryId, RequestId, TablePath, TabletGroupId, Timestamp,
};
use crate::model::message as msg;
use std::cmp::max;
use std::collections::HashMap;

// -----------------------------------------------------------------------------------------------
//  AlterTableES
// -----------------------------------------------------------------------------------------------
#[derive(Debug)]
pub struct Executing {
  responded_count: usize,
  tm_state: HashMap<TabletGroupId, Option<Timestamp>>,
}

#[derive(Debug)]
pub enum AlterTableS {
  Start,
  Executing(Executing),
  Done,
}

#[derive(Debug)]
pub struct AlterTableES {
  // Metadata copied from outside.
  pub request_id: RequestId,
  pub sender_eid: EndpointId,

  // Core ES data
  pub query_id: QueryId,
  pub table_path: TablePath,
  pub alter_op: proc::AlterOp,

  // State
  pub state: AlterTableS,
}

pub enum AlterTableAction {
  /// This tells the parent Server to wait.
  Wait,
  /// Indicates the ES succeeded. This returns the Timestamp which the AlterOp was Committed.
  Success(Timestamp),
  /// Indicates the AlterOp was Column Invalid.
  ColumnInvalid,
}

// -----------------------------------------------------------------------------------------------
//  Implementation
// -----------------------------------------------------------------------------------------------

impl AlterTableES {
  pub fn start<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) -> AlterTableAction {
    // First, we see if the AlterOp is Column Valid.
    let table_schema = ctx.db_schema.get(&self.table_path).unwrap();
    let maybe_col_type = table_schema.val_cols.get_last_version(&self.alter_op.col_name);
    if (maybe_col_type.is_none() && self.alter_op.maybe_col_type.is_none())
      || (maybe_col_type.is_some() && self.alter_op.maybe_col_type.is_some())
    {
      // This means the `alter_op` is not Column Valid. Thus, we go to Done and signal the parent.
      self.state = AlterTableS::Done;
      AlterTableAction::ColumnInvalid
    } else {
      // Otherwise, we start the 2PC.
      let mut tm_state = HashMap::<TabletGroupId, Option<Timestamp>>::new();
      for (_, tid) in ctx.sharding_config.get(&self.table_path).unwrap() {
        tm_state.insert(tid.clone(), None);
      }

      // Send off AlterTablePrepare to the Tablets and move the `state` to Executing.
      for tid in tm_state.keys() {
        ctx.ctx().send_to_tablet(
          tid.clone(),
          msg::TabletMessage::AlterTablePrepare(msg::AlterTablePrepare {
            query_id: self.query_id.clone(),
            alter_op: self.alter_op.clone(),
          }),
        )
      }

      // Note that since there is at least one `tid` for a Table, we do not have to short-circuit
      // for the case that we vacuously have all the Prepareds by this point.
      self.state = AlterTableS::Executing(Executing { responded_count: 0, tm_state });
      AlterTableAction::Wait
    }
  }

  /// Handle `AlterTablePrepared`
  pub fn handle_prepared<T: IOTypes>(
    &mut self,
    ctx: &mut MasterContext<T>,
    prepared: msg::AlterTablePrepared,
  ) -> AlterTableAction {
    let executing = cast!(AlterTableS::Executing, &mut self.state).unwrap();
    let rm_state = executing.tm_state.get_mut(&prepared.tablet_group_id).unwrap();
    assert!(rm_state.is_none());
    *rm_state = Some(prepared.timestamp.clone());
    executing.responded_count += 1;

    // Check if all RMs have responded and finish AlterTableES if so, otherwise Wait.
    if executing.responded_count < executing.tm_state.len() {
      AlterTableAction::Wait
    } else {
      // Compute the final timestamp to apply the `alter_op` at.
      let table_schema = ctx.db_schema.get_mut(&self.table_path).unwrap();
      let mut new_timestamp = table_schema.val_cols.get_lat(&self.alter_op.col_name);
      for (_, rm_state) in &executing.tm_state {
        new_timestamp = max(new_timestamp, rm_state.unwrap());
      }
      new_timestamp.0 += 1; // We add 1, since we cannot actually modify a value at a `lat`.

      // Apply the `alter_op`.
      ctx.gen.0 += 1;
      table_schema.val_cols.write(
        &self.alter_op.col_name,
        self.alter_op.maybe_col_type.clone(),
        new_timestamp,
      );

      // Send off AlterTableCommit to the Tablets and return Success.
      let gossip_data = GossipDataSer::from_gossip(GossipData {
        gen: ctx.gen.clone(),
        db_schema: ctx.db_schema.clone(),
        table_generation: ctx.table_generation.clone(),
        sharding_config: ctx.sharding_config.clone(),
        tablet_address_config: ctx.tablet_address_config.clone(),
        slave_address_config: ctx.slave_address_config.clone(),
      });
      for tid in executing.tm_state.keys() {
        ctx.ctx().send_to_tablet(
          tid.clone(),
          msg::TabletMessage::AlterTableCommit(msg::AlterTableCommit {
            query_id: self.query_id.clone(),
            timestamp: new_timestamp,
            gossip_data: gossip_data.clone(),
          }),
        );
      }
      self.state = AlterTableS::Done;
      AlterTableAction::Success(new_timestamp)
    }
  }

  /// Cleans up all currently owned resources, and goes to Done.
  pub fn exit_and_clean_up<T: IOTypes>(&mut self, ctx: &mut MasterContext<T>) {
    match &self.state {
      AlterTableS::Start => {}
      AlterTableS::Executing(executing) => {
        for tid in executing.tm_state.keys() {
          ctx.ctx().send_to_tablet(
            tid.clone(),
            msg::TabletMessage::AlterTableAbort(msg::AlterTableAbort {
              query_id: self.query_id.clone(),
            }),
          );
        }
      }
      AlterTableS::Done => {}
    }
  }
}
