use runiversal::model::message::{
  CoordMessage, ExternalMessage, MasterExternalReq, MasterMessage, MasterRemotePayload,
  NetworkMessage, PaxosDriverMessage, RemoteMessage, SlaveExternalReq, SlaveMessage,
  SlaveRemotePayload, TabletMessage,
};
use runiversal::slave::SharedPaxosBundle;

#[derive(Debug, Default)]
pub struct Stats {
  external_query_success: u64,
  external_query_aborted: u64,
  external_ddl_query_success: u64,
  external_ddl_query_aborted: u64,

  // Master
  perform_ddl_external_query: u64,
  cancel_ddl_external_query: u64,

  master_remote_leader_changed: u64,
  master_ddl: u64,
  master_master_gossip: u64,

  // Master Paxos
  master_multi_paxos_message: u64,
  master_is_leader: u64,
  master_log_sync_request: u64,
  master_log_sync_response: u64,
  master_next_index_request: u64,
  master_next_index_response: u64,

  // Slave
  perform_external_query: u64,
  cancel_external_query: u64,

  slave_remote_leader_changed: u64,
  slave_create_table: u64,
  slave_master_gossip: u64,

  // Slave Paxos
  slave_multi_paxos_message: u64,
  slave_is_leader: u64,
  slave_log_sync_request: u64,
  slave_log_sync_response: u64,
  slave_next_index_request: u64,
  slave_next_index_response: u64,

  // Tablet
  tablet_pcsa: u64,
  tablet_finish_query: u64,
  tablet_ddl: u64,

  // Coord
  coord_pcsa: u64,
  coord_finish_query: u64,
}

impl Stats {
  pub fn record(&mut self, m: &NetworkMessage) {
    match m {
      NetworkMessage::External(m) => match m {
        ExternalMessage::ExternalQuerySuccess(_) => self.external_query_success += 1,
        ExternalMessage::ExternalQueryAborted(_) => self.external_query_aborted += 1,
        ExternalMessage::ExternalDDLQuerySuccess(_) => self.external_ddl_query_success += 1,
        ExternalMessage::ExternalDDLQueryAborted(_) => self.external_ddl_query_aborted += 1,
      },
      NetworkMessage::Master(m) => match m {
        MasterMessage::MasterExternalReq(m) => match m {
          MasterExternalReq::PerformExternalDDLQuery(_) => self.perform_ddl_external_query += 1,
          MasterExternalReq::CancelExternalDDLQuery(_) => self.cancel_ddl_external_query += 1,
        },
        MasterMessage::RemoteMessage(m) => match m {
          RemoteMessage { payload: m, .. } => match m {
            MasterRemotePayload::PerformMasterQueryPlanning(_) => {}
            MasterRemotePayload::CancelMasterQueryPlanning(_) => {}
            MasterRemotePayload::CreateTable(_) => self.master_ddl += 1,
            MasterRemotePayload::AlterTable(_) => self.master_ddl += 1,
            MasterRemotePayload::DropTable(_) => self.master_ddl += 1,
            MasterRemotePayload::MasterGossipRequest(_) => {}
          },
        },
        MasterMessage::RemoteLeaderChangedGossip(_) => self.master_remote_leader_changed += 1,
        MasterMessage::PaxosDriverMessage(m) => match m {
          PaxosDriverMessage::MultiPaxosMessage(_) => self.master_multi_paxos_message += 1,
          PaxosDriverMessage::IsLeader(_) => self.master_is_leader += 1,
          PaxosDriverMessage::LogSyncRequest(_) => self.master_log_sync_request += 1,
          PaxosDriverMessage::LogSyncResponse(_) => self.master_log_sync_response += 1,
          PaxosDriverMessage::NextIndexRequest(_) => self.master_next_index_request += 1,
          PaxosDriverMessage::NextIndexResponse(_) => self.master_next_index_response += 1,
        },
      },
      NetworkMessage::Slave(m) => match m {
        SlaveMessage::SlaveExternalReq(m) => match m {
          SlaveExternalReq::PerformExternalQuery(_) => self.perform_external_query += 1,
          SlaveExternalReq::CancelExternalQuery(_) => self.cancel_external_query += 1,
        },
        SlaveMessage::RemoteMessage(m) => match m {
          RemoteMessage { payload: m, .. } => match m {
            SlaveRemotePayload::CreateTable(_) => self.slave_create_table += 1,
            SlaveRemotePayload::MasterGossip(_) => self.slave_master_gossip += 1,
            SlaveRemotePayload::TabletMessage(_, m) => match m {
              TabletMessage::PerformQuery(_) => self.tablet_pcsa += 1,
              TabletMessage::CancelQuery(_) => self.tablet_pcsa += 1,
              TabletMessage::QueryAborted(_) => self.tablet_pcsa += 1,
              TabletMessage::QuerySuccess(_) => self.tablet_pcsa += 1,
              TabletMessage::FinishQuery(_) => self.tablet_finish_query += 1,
              TabletMessage::AlterTable(_) => self.tablet_ddl += 1,
              TabletMessage::DropTable(_) => self.tablet_ddl += 1,
            },
            SlaveRemotePayload::CoordMessage(_, m) => match m {
              CoordMessage::MasterQueryPlanningAborted(_) => {}
              CoordMessage::MasterQueryPlanningSuccess(_) => {}
              CoordMessage::PerformQuery(_) => self.coord_pcsa += 1,
              CoordMessage::CancelQuery(_) => self.coord_pcsa += 1,
              CoordMessage::QueryAborted(_) => self.coord_pcsa += 1,
              CoordMessage::QuerySuccess(_) => self.coord_pcsa += 1,
              CoordMessage::FinishQuery(_) => self.coord_finish_query += 1,
              CoordMessage::RegisterQuery(_) => {}
            },
          },
        },
        SlaveMessage::RemoteLeaderChangedGossip(_) => self.slave_remote_leader_changed += 1,
        SlaveMessage::PaxosDriverMessage(m) => match m {
          PaxosDriverMessage::MultiPaxosMessage(_) => self.slave_multi_paxos_message += 1,
          PaxosDriverMessage::IsLeader(_) => self.slave_is_leader += 1,
          PaxosDriverMessage::LogSyncRequest(_) => self.slave_log_sync_request += 1,
          PaxosDriverMessage::LogSyncResponse(_) => self.slave_log_sync_response += 1,
          PaxosDriverMessage::NextIndexRequest(_) => self.slave_next_index_request += 1,
          PaxosDriverMessage::NextIndexResponse(_) => self.slave_next_index_response += 1,
        },
      },
    }
  }
}
