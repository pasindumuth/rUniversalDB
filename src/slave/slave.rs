use crate::common::rand::RandGen;
use crate::model::common::{
  EndpointId, Schema, TabletKeyRange, TabletPath, TabletShape, Timestamp, TransactionId,
};
use crate::model::message::{
  AdminMessage, AdminRequest, AdminResponse, NetworkMessage, SelectPrepare, SlaveAction,
  SlaveMessage, TabletMessage, WriteAbort, WriteCommit, WritePrepare, WriteQuery,
};
use crate::model::sqlast::{SelectStmt, SqlStmt};
use crate::slave::network_task::{NetworkTask, RequestMeta, SelectTask, WritePhase, WriteTask};
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
      SlaveMessage::ClientRequest { .. } => {
        panic!("Client messages not supported yet.");
      }
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
            let mpc_tablets = get_tablets(&table, &self.tablet_config);
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
          SqlStmt::Update(update_stmt) => {
            let table = &update_stmt.table_name;
            let mpc_tablets = get_tablets(table, &self.tablet_config);
            let pending_tablets = mpc_tablets.iter().map(|i| i.clone()).collect();
            for (eid, tablet) in &mpc_tablets {
              side_effects.add(SlaveAction::Send {
                eid: eid.clone(),
                msg: NetworkMessage::Slave(SlaveMessage::FW_WritePrepare {
                  tablet: tablet.clone(),
                  msg: WritePrepare {
                    tm_eid: self.this_eid.clone(),
                    tid: tid.clone(),
                    write_query: WriteQuery::Update(update_stmt.clone()),
                    timestamp: *timestamp,
                  },
                }),
              });
            }
            self.tasks.insert(
              tid.clone(),
              NetworkTask::Write(WriteTask {
                tablets: mpc_tablets,
                pending_tablets,
                prepared_tablets: Default::default(),
                phase: WritePhase::Preparing,
                req_meta: RequestMeta {
                  origin_eid: from_eid.clone(),
                  rid: rid.clone(),
                },
              }),
            );
          }
        },
      },
      SlaveMessage::SelectPrepared {
        tablet,
        tid,
        view_o,
      } => {
        if let Some(task) = self.tasks.get_mut(tid) {
          let select_task = cast!(NetworkTask::Select, task)
            .expect("We shouldn't get a SelectPrepared message for task: {:?}");
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
        }
      }
      SlaveMessage::WritePrepared { tablet, tid } => {
        if let Some(task) = self.tasks.get_mut(tid) {
          let write_task = cast!(NetworkTask::Write, task)
            .expect("We shouldn't get a WritePrepared message for task: {:?}");
          // We assume that that there are no duplicate message here.
          let eid_tablet = (from_eid.clone(), tablet.clone());
          assert!(write_task.pending_tablets.contains(&eid_tablet));
          write_task.pending_tablets.remove(&eid_tablet);
          write_task.prepared_tablets.insert(eid_tablet.clone());
          if write_task.pending_tablets.is_empty() {
            // Here, we are finished waiting for the WritePrepare messages
            if write_task.prepared_tablets.len() < write_task.tablets.len() {
              // This means that one or more Tablets sent back an Aborted
              // message, so we start the Abort sequence. This involves
              // responding to the origin and sending Abort to all Tablets
              // that send back Prepared.
              for (eid, tablet) in &write_task.prepared_tablets {
                side_effects.add(SlaveAction::Send {
                  eid: eid.clone(),
                  msg: NetworkMessage::Slave(SlaveMessage::FW_WriteAbort {
                    tablet: tablet.clone(),
                    msg: WriteAbort {
                      tm_eid: self.this_eid.clone(),
                      tid: tid.clone(),
                    },
                  }),
                });
              }
              // We now have to wait for this new sent of tablets.
              write_task.phase = WritePhase::Aborting;
              write_task.pending_tablets = write_task.prepared_tablets.clone();
              // Send the origin a repsonse.
              side_effects.add(SlaveAction::Send {
                eid: write_task.req_meta.origin_eid.clone(),
                msg: NetworkMessage::Admin(AdminMessage::AdminResponse {
                  res: AdminResponse::SqlQuery {
                    rid: write_task.req_meta.rid.clone(),
                    result: Err("Update Failed".to_string()),
                  },
                }),
              });
            } else {
              // This means all Tablets responded with Prepared, and now
              // we may begin the Commit phase of 2PC.
              for (eid, tablet) in &write_task.prepared_tablets {
                side_effects.add(SlaveAction::Send {
                  eid: eid.clone(),
                  msg: NetworkMessage::Slave(SlaveMessage::FW_WriteCommit {
                    tablet: tablet.clone(),
                    msg: WriteCommit {
                      tm_eid: self.this_eid.clone(),
                      tid: tid.clone(),
                    },
                  }),
                });
              }
              // We now have to wait for this new sent of tablets.
              write_task.phase = WritePhase::Committing;
              write_task.pending_tablets = write_task.prepared_tablets.clone();
              // Send the origin a repsonse.
              side_effects.add(SlaveAction::Send {
                eid: write_task.req_meta.origin_eid.clone(),
                msg: NetworkMessage::Admin(AdminMessage::AdminResponse {
                  res: AdminResponse::SqlQuery {
                    rid: write_task.req_meta.rid.clone(),
                    result: Ok(vec![]),
                  },
                }),
              });
            }
          }
        }
      }
      SlaveMessage::WriteCommitted { tablet, tid } => {
        if let Some(task) = self.tasks.get_mut(tid) {
          let write_task = cast!(NetworkTask::Write, task)
            .expect("We shouldn't get a WriteCommitted message for task: {:?}");
          // We assume that that there are no duplicate message here.
          let eid_tablet = (from_eid.clone(), tablet.clone());
          assert!(write_task.pending_tablets.contains(&eid_tablet));
          write_task.pending_tablets.remove(&eid_tablet);
          if write_task.pending_tablets.is_empty() {
            // This means we are finished commit. The origin will already
            // have been responded to. Now, we just need to remove the
            // Network task.
            self.tasks.remove(tid);
          }
        }
      }
      SlaveMessage::WriteAborted { tablet, tid } => {
        if let Some(task) = self.tasks.get_mut(tid) {
          let write_task = cast!(NetworkTask::Write, task)
            .expect("We shouldn't get a WriteAborted message for task: {:?}");
          // We assume that that there are no duplicate message here.
          let eid_tablet = (from_eid.clone(), tablet.clone());
          assert!(write_task.pending_tablets.contains(&eid_tablet));
          write_task.pending_tablets.remove(&eid_tablet);
          if write_task.pending_tablets.is_empty() {
            // This means we are finished commit. The origin will already
            // have been responded to. Now, we just need to remove the
            // Network task.
            self.tasks.remove(tid);
          }
        }
      }
      SlaveMessage::FW_SelectPrepare { tablet, msg } => {
        side_effects.add(SlaveAction::Forward {
          tablet: tablet.clone(),
          msg: TabletMessage::SelectPrepare(msg.clone()),
        });
      }
      SlaveMessage::FW_WritePrepare { tablet, msg } => {
        side_effects.add(SlaveAction::Forward {
          tablet: tablet.clone(),
          msg: TabletMessage::WritePrepare(msg.clone()),
        });
      }
      SlaveMessage::FW_WriteCommit { tablet, msg } => {
        side_effects.add(SlaveAction::Forward {
          tablet: tablet.clone(),
          msg: TabletMessage::WriteCommit(msg.clone()),
        });
      }
      SlaveMessage::FW_WriteAbort { tablet, msg } => {
        side_effects.add(SlaveAction::Forward {
          tablet: tablet.clone(),
          msg: TabletMessage::WriteAbort(msg.clone()),
        });
      }
    }
  }
}

fn get_tablets(
  table: &String,
  tablet_config: &HashMap<EndpointId, Vec<TabletShape>>,
) -> Vec<(EndpointId, TabletShape)> {
  let mut mpc_tablets: Vec<(EndpointId, TabletShape)> = Vec::new();
  for (eid, tablets) in tablet_config {
    for tablet in tablets {
      if &tablet.path.path == table {
        mpc_tablets.push((eid.clone(), tablet.clone()));
      }
    }
  }
  return mpc_tablets;
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
