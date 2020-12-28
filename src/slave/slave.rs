use crate::common::rand::RandGen;
use crate::model::common::{
  EndpointId, RequestId, SelectQueryId, SelectView, TabletShape, Timestamp, TransactionId,
  WriteQueryId,
};
use crate::model::message::{
  AdminMessage, AdminRequest, AdminResponse, NetworkMessage, SelectPrepare, SlaveAction,
  SlaveMessage, SubqueryResponse, TabletMessage, WriteAbort, WriteCommit, WritePrepare, WriteQuery,
};
use crate::model::sqlast::SqlStmt;
use crate::slave::network_task::{
  SelectRequestMeta, SelectTask, WritePhase, WriteRequestMeta, WriteTask,
};
use std::collections::{BTreeMap, HashMap};

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
  select_tasks: HashMap<SelectQueryId, SelectTask>,
  write_tasks: HashMap<WriteQueryId, WriteTask>,
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
      select_tasks: HashMap::new(),
      write_tasks: HashMap::new(),
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
                    sid: SelectQueryId(tid.clone()),
                    select_query: select_stmt.clone(),
                    timestamp: *timestamp,
                  },
                }),
              });
            }
            self.select_tasks.insert(
              SelectQueryId(tid.clone()),
              SelectTask {
                tablets: mpc_tablets,
                pending_tablets,
                view: BTreeMap::new(),
                req_meta: SelectRequestMeta::Admin {
                  origin_eid: from_eid.clone(),
                  rid: rid.clone(),
                },
              },
            );
          }
          SqlStmt::Update(update_stmt) => self.start_write_task(
            side_effects,
            &update_stmt.table_name,
            rid,
            from_eid,
            tid,
            timestamp,
            &WriteQuery::Update(update_stmt.clone()),
          ),
          SqlStmt::Insert(insert_stmt) => self.start_write_task(
            side_effects,
            &insert_stmt.table_name,
            rid,
            from_eid,
            tid,
            timestamp,
            &WriteQuery::Insert(insert_stmt.clone()),
          ),
        },
      },
      SlaveMessage::SelectPrepared {
        tablet,
        sid,
        view_o,
      } => {
        if let Some(select_task) = self.select_tasks.get_mut(sid) {
          // We assume that that there are no duplicate message here.
          let eid_tablet = (from_eid.clone(), tablet.clone());
          assert!(select_task.pending_tablets.contains(&eid_tablet));
          select_task.pending_tablets.remove(&eid_tablet);
          if let Some(view) = view_o {
            // This means that Partial Query was successful and we
            // should continue on with the SelectTask.
            for (key, values) in view {
              select_task.view.insert(key.clone(), values.clone());
            }
            if select_task.pending_tablets.is_empty() {
              // If all pending tablets have replied, then send a success
              // message back to the client.
              send_select_response(
                side_effects,
                &select_task.req_meta,
                Ok(select_task.view.clone()),
              );
              self.select_tasks.remove(sid);
            }
          } else {
            // This means the Partial Query failed, meaning the whole
            // Select Query was a failure. We may respond to the client now
            // and deleted this SelectTask.
            send_select_response(
              side_effects,
              &select_task.req_meta,
              Err("One of the Partial Transactions failed".to_string()),
            );
            self.select_tasks.remove(sid);
          }
        }
      }
      SlaveMessage::WritePrepared { tablet, wid } => {
        if let Some(write_task) = self.write_tasks.get_mut(wid) {
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
                      wid: wid.clone(),
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
                      wid: wid.clone(),
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
                    result: Ok(BTreeMap::new()),
                  },
                }),
              });
            }
          }
        }
      }
      SlaveMessage::WriteCommitted { tablet, wid } => {
        if let Some(write_task) = self.write_tasks.get_mut(wid) {
          // We assume that that there are no duplicate message here.
          let eid_tablet = (from_eid.clone(), tablet.clone());
          assert!(write_task.pending_tablets.contains(&eid_tablet));
          write_task.pending_tablets.remove(&eid_tablet);
          if write_task.pending_tablets.is_empty() {
            // This means we are finished commit. The origin will already
            // have been responded to. Now, we just need to remove the
            // Network task.
            self.write_tasks.remove(wid);
          }
        }
      }
      SlaveMessage::WriteAborted { tablet, wid } => {
        if let Some(write_task) = self.write_tasks.get_mut(wid) {
          // We assume that that there are no duplicate message here.
          let eid_tablet = (from_eid.clone(), tablet.clone());
          assert!(write_task.pending_tablets.contains(&eid_tablet));
          write_task.pending_tablets.remove(&eid_tablet);
          if write_task.pending_tablets.is_empty() {
            // This means we are finished commit. The origin will already
            // have been responded to. Now, we just need to remove the
            // Network task.
            self.write_tasks.remove(wid);
          }
        }
      }
      SlaveMessage::SubqueryRequest {
        tablet,
        sid,
        subquery_path,
        select_stmt,
        timestamp,
      } => {
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
                sid: sid.clone(),
                select_query: select_stmt.clone(),
                timestamp: *timestamp,
              },
            }),
          });
        }
        self.select_tasks.insert(
          sid.clone(),
          SelectTask {
            tablets: mpc_tablets,
            pending_tablets,
            view: BTreeMap::new(),
            req_meta: SelectRequestMeta::Tablet {
              origin_eid: from_eid.clone(),
              tablet: tablet.clone(),
              subquery_path: subquery_path.clone(),
              sid: sid.clone(),
            },
          },
        );
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
      SlaveMessage::FW_SubqueryResponse { tablet, msg } => {
        side_effects.add(SlaveAction::Forward {
          tablet: tablet.clone(),
          msg: TabletMessage::SubqueryResponse(msg.clone()),
        });
      }
    }
  }

  /// This function finds all tablets that hold data for the given
  /// `table_name`, which is the table that the `write_query` touches,
  /// and then includes each of them in a 2PC by creating a `WriteTask`.
  fn start_write_task(
    &mut self,
    side_effects: &mut SlaveSideEffects,
    table_name: &String,
    rid: &RequestId,
    from_eid: &EndpointId,
    tid: &TransactionId,
    timestamp: &Timestamp,
    write_query: &WriteQuery,
  ) {
    let mpc_tablets = get_tablets(table_name, &self.tablet_config);
    let pending_tablets = mpc_tablets.iter().map(|i| i.clone()).collect();
    for (eid, tablet) in &mpc_tablets {
      side_effects.add(SlaveAction::Send {
        eid: eid.clone(),
        msg: NetworkMessage::Slave(SlaveMessage::FW_WritePrepare {
          tablet: tablet.clone(),
          msg: WritePrepare {
            tm_eid: self.this_eid.clone(),
            wid: WriteQueryId(tid.clone()),
            write_query: write_query.clone(),
            timestamp: *timestamp,
          },
        }),
      });
    }
    self.write_tasks.insert(
      WriteQueryId(tid.clone()),
      WriteTask {
        tablets: mpc_tablets,
        pending_tablets,
        prepared_tablets: Default::default(),
        phase: WritePhase::Preparing,
        req_meta: WriteRequestMeta {
          origin_eid: from_eid.clone(),
          rid: rid.clone(),
        },
      },
    );
  }
}

/// Pattern matches against `req_meta`, which describes
/// who the initiator of the Select Query was, and then send them
/// an appropriate response with the given `result`.
fn send_select_response(
  side_effects: &mut SlaveSideEffects,
  req_meta: &SelectRequestMeta,
  result: Result<SelectView, String>,
) {
  match req_meta {
    SelectRequestMeta::Admin { origin_eid, rid } => {
      side_effects.add(SlaveAction::Send {
        eid: origin_eid.clone(),
        msg: NetworkMessage::Admin(AdminMessage::AdminResponse {
          res: AdminResponse::SqlQuery {
            rid: rid.clone(),
            result,
          },
        }),
      });
    }
    SelectRequestMeta::Tablet {
      origin_eid,
      tablet,
      subquery_path,
      sid,
    } => {
      side_effects.add(SlaveAction::Send {
        eid: origin_eid.clone(),
        msg: NetworkMessage::Slave(SlaveMessage::FW_SubqueryResponse {
          tablet: tablet.clone(),
          msg: SubqueryResponse {
            sid: sid.clone(),
            subquery_path: subquery_path.clone(),
            result,
          },
        }),
      });
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
