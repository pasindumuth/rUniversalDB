use crate::common::rand::RandGen;
use crate::model::common::{
  EndpointId, Schema, TabletKeyRange, TabletPath, TabletShape, Timestamp, TransactionId,
};
use crate::model::message::{
  AdminMessage, AdminRequest, AdminResponse, NetworkMessage, SelectPrepare, SlaveAction,
  SlaveMessage, TabletMessage,
};
use crate::model::sqlast::{SelectStmt, SqlStmt};
use crate::slave::network_task::{NetworkTask, RequestMeta, SelectTask};
use std::collections::HashMap;

#[derive(Debug)]
pub struct SlaveSideEffects {
  pub actions: Vec<SlaveAction>,
}

impl SlaveSideEffects {
  pub fn new() -> SlaveSideEffects {
    SlaveSideEffects {
      actions: Vec::new(),
    }
  }

  pub fn add(&mut self, action: SlaveAction) {
    self.actions.push(action);
  }
}

#[derive(Debug)]
pub struct SlaveState {
  pub rand_gen: RandGen,
  pub this_eid: EndpointId,
  tasks: HashMap<TransactionId, NetworkTask>,
  tablet_config: HashMap<EndpointId, Vec<TabletShape>>,
}

impl SlaveState {
  pub fn new(
    rand_gen: RandGen,
    this_eid: EndpointId,
    tablet_config: HashMap<EndpointId, Vec<TabletShape>>,
  ) -> SlaveState {
    SlaveState {
      rand_gen,
      this_eid,
      tasks: HashMap::new(),
      tablet_config,
    }
  }

  /// Top-level network message handling function. It muttates
  /// the SlaveState and Populates `side_effects` with any IO
  /// operations that need to be done as a consequence.
  pub fn handle_incoming_message(
    &mut self,
    side_effects: &mut SlaveSideEffects,
    from_eid: &EndpointId,
    msg: SlaveMessage,
  ) {
    match &msg {
      SlaveMessage::AdminRequest { req } => match req {
        AdminRequest::Insert { path, .. } => {
          side_effects.add(SlaveAction::Forward {
            tablet: mk_shape(path),
            msg: TabletMessage::AdminRequest {
              eid: from_eid.clone(),
              req: req.clone(),
            },
          });
        }
        AdminRequest::Read { path, .. } => {
          side_effects.add(SlaveAction::Forward {
            tablet: mk_shape(path),
            msg: TabletMessage::AdminRequest {
              eid: from_eid.clone(),
              req: req.clone(),
            },
          });
        }
        AdminRequest::SqlQuery {
          rid,
          tid,
          sql,
          timestamp,
        } => match sql {
          SqlStmt::Select(select_stmt) => {
            let table = &select_stmt.table_name;
            // These are the tablets that will engage in MPC.
            let mut mpc_tablets: Vec<(EndpointId, TabletShape)> = Vec::new();
            for (eid, tablets) in &self.tablet_config {
              for tablet in tablets {
                if &tablet.path.path == table {
                  mpc_tablets.push((eid.clone(), tablet.clone()));
                }
              }
            }
            let pending_tablets = mpc_tablets.iter().map(|i| i.clone()).collect();
            for (eid, tablet) in &mpc_tablets {
              side_effects.add(SlaveAction::Send {
                eid: eid.clone(),
                msg: NetworkMessage::Slave(SlaveMessage::FW_SelectPrepare {
                  tablet: tablet.clone(),
                  msg: SelectPrepare {
                    tm_eid: self.this_eid.clone(),
                    tid: tid.clone(),
                    select_query: select_stmt.clone(),
                    timestamp: *timestamp,
                  },
                }),
              });
            }
            self.tasks.insert(
              tid.clone(),
              NetworkTask::Select(SelectTask {
                tablets: mpc_tablets,
                pending_tablets,
                view: vec![],
                req_meta: RequestMeta {
                  origin_eid: from_eid.clone(),
                  rid: rid.clone(),
                },
              }),
            );
          }
        },
      },
      SlaveMessage::ClientRequest { .. } => {
        panic!("Client messages not supported yet.");
      }
      SlaveMessage::SelectPrepared {
        tablet,
        tid,
        view_o,
      } => {
        if let Some(task) = self.tasks.get_mut(tid) {
          if let NetworkTask::Select(select_task) = task {
            // We assume that that there are no duplicate message here.
            let eid_tablet = (from_eid.clone(), tablet.clone());
            assert!(select_task.pending_tablets.contains(&eid_tablet));
            select_task.pending_tablets.remove(&eid_tablet);
            if let Some(view) = view_o {
              // This means that Partial Query was successful and we
              // should continue on with the SelectTask. If all pending
              // tablets have replied, then send a success message back to
              // the client.
              for row in view {
                select_task.view.push(row.clone());
              }
              if select_task.pending_tablets.is_empty() {
                side_effects.add(SlaveAction::Send {
                  eid: select_task.req_meta.origin_eid.clone(),
                  msg: NetworkMessage::Admin(AdminMessage::AdminResponse {
                    res: AdminResponse::SqlQuery {
                      rid: select_task.req_meta.rid.clone(),
                      result: Ok(select_task.view.clone()),
                    },
                  }),
                });
                self.tasks.remove(tid);
              }
            } else {
              // This means the Partial Query failed, meaning the whole
              // Select Query was a failure. We may respond to the client now
              // and deleted this SelectTask.
              side_effects.add(SlaveAction::Send {
                eid: select_task.req_meta.origin_eid.clone(),
                msg: NetworkMessage::Admin(AdminMessage::AdminResponse {
                  res: AdminResponse::SqlQuery {
                    rid: select_task.req_meta.rid.clone(),
                    result: Err("One of the Partial Transactions failed".to_string()),
                  },
                }),
              });
              self.tasks.remove(tid);
            }
          } else {
            panic!(
              "We shouldn't get a SelectPrepared message for task: {:?}",
              task
            );
          }
        }
      }
      SlaveMessage::FW_SelectPrepare { tablet, msg } => {
        side_effects.add(SlaveAction::Forward {
          tablet: tablet.clone(),
          msg: TabletMessage::SelectPrepare(msg.clone()),
        });
      }
    }
  }
}

fn mk_shape(path: &TabletPath) -> TabletShape {
  // For now, we just assume that if we get an AdminMessage
  // with some `path`, then this Slave has the Tablet for it
  // and that Tablet contains the whole key space.
  TabletShape {
    path: path.clone(),
    range: TabletKeyRange {
      start: None,
      end: None,
    },
  }
}
