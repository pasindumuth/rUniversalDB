use runiversal::master::MasterBundle;
use runiversal::model::message::{
  CoordMessage, ExternalMessage, FreeNodeAssoc, FreeNodeMessage, MasterExternalReq, MasterMessage,
  MasterRemotePayload, NetworkMessage, PaxosDriverMessage, RemoteMessage, SlaveExternalReq,
  SlaveMessage, SlaveReconfig, SlaveRemotePayload, TabletMessage,
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
  master_inform_learned: u64,

  master_new_node_started: u64,
  master_start_new_node: u64,

  master_nodes_dead: u64,
  master_slave_group_reconfigured: u64,

  master_register_free_node: u64,
  master_free_node_heartbeat: u64,
  master_confirm_slave_creation: u64,

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
  slave_inform_learned: u64,
  slave_new_node_started: u64,
  slave_start_new_node: u64,

  // Tablet
  tablet_pcsa: u64,
  tablet_finish_query: u64,
  tablet_ddl: u64,

  // Coord
  coord_pcsa: u64,
  coord_finish_query: u64,

  // FreeNode
  free_node_master_leadership: u64,

  // Unnaccounted
  unnaccounted: u64,
}

impl Stats {
  pub fn record(&mut self, m: &NetworkMessage) {
    let count_ref = match m {
      NetworkMessage::External(m) => match m {
        ExternalMessage::ExternalQuerySuccess(_) => &mut self.external_query_success,
        ExternalMessage::ExternalQueryAborted(_) => &mut self.external_query_aborted,
        ExternalMessage::ExternalDDLQuerySuccess(_) => &mut self.external_ddl_query_success,
        ExternalMessage::ExternalDDLQueryAborted(_) => &mut self.external_ddl_query_aborted,
        ExternalMessage::ExternalDebugResponse(_) => &mut self.unnaccounted,
      },
      NetworkMessage::Master(m) => match m {
        MasterMessage::MasterExternalReq(m) => match m {
          MasterExternalReq::PerformExternalDDLQuery(_) => &mut self.perform_ddl_external_query,
          MasterExternalReq::CancelExternalDDLQuery(_) => &mut self.cancel_ddl_external_query,
          MasterExternalReq::ExternalDebugRequest(_) => &mut self.unnaccounted,
        },
        MasterMessage::RemoteMessage(m) => match m {
          RemoteMessage { payload: m, .. } => match m {
            MasterRemotePayload::MasterQueryPlanning(_) => &mut self.unnaccounted,
            MasterRemotePayload::CreateTable(_) => &mut self.master_ddl,
            MasterRemotePayload::AlterTable(_) => &mut self.master_ddl,
            MasterRemotePayload::DropTable(_) => &mut self.master_ddl,
            MasterRemotePayload::MasterGossipRequest(_) => &mut self.unnaccounted,
            MasterRemotePayload::SlaveReconfig(m) => match m {
              SlaveReconfig::NodesDead(_) => &mut self.master_nodes_dead,
              SlaveReconfig::SlaveGroupReconfigured(_) => &mut self.master_slave_group_reconfigured,
            },
          },
        },
        MasterMessage::RemoteLeaderChangedGossip(_) => &mut self.master_remote_leader_changed,
        MasterMessage::PaxosDriverMessage(m) => match m {
          PaxosDriverMessage::MultiPaxosMessage(_) => &mut self.master_multi_paxos_message,
          PaxosDriverMessage::IsLeader(_) => &mut self.master_is_leader,
          PaxosDriverMessage::LogSyncRequest(_) => &mut self.master_log_sync_request,
          PaxosDriverMessage::LogSyncResponse(_) => &mut self.master_log_sync_response,
          PaxosDriverMessage::NextIndexRequest(_) => &mut self.master_next_index_request,
          PaxosDriverMessage::NextIndexResponse(_) => &mut self.master_next_index_response,
          PaxosDriverMessage::InformLearned(_) => &mut self.master_inform_learned,
          PaxosDriverMessage::NewNodeStarted(_) => &mut self.master_new_node_started,
          PaxosDriverMessage::StartNewNode(_) => &mut self.master_start_new_node,
        },
        MasterMessage::FreeNodeAssoc(m) => match m {
          FreeNodeAssoc::RegisterFreeNode(_) => &mut self.master_register_free_node,
          FreeNodeAssoc::FreeNodeHeartbeat(_) => &mut self.master_free_node_heartbeat,
          FreeNodeAssoc::ConfirmSlaveCreation(_) => &mut self.master_confirm_slave_creation,
        },
      },
      NetworkMessage::Slave(m) => match m {
        SlaveMessage::SlaveExternalReq(m) => match m {
          SlaveExternalReq::PerformExternalQuery(_) => &mut self.perform_external_query,
          SlaveExternalReq::CancelExternalQuery(_) => &mut self.cancel_external_query,
        },
        SlaveMessage::RemoteMessage(m) => match m {
          RemoteMessage { payload: m, .. } => match m {
            SlaveRemotePayload::CreateTable(_) => &mut self.slave_create_table,
            SlaveRemotePayload::MasterGossip(_) => &mut self.slave_master_gossip,
            SlaveRemotePayload::TabletMessage(_, m) => match m {
              TabletMessage::PerformQuery(_) => &mut self.tablet_pcsa,
              TabletMessage::CancelQuery(_) => &mut self.tablet_pcsa,
              TabletMessage::QueryAborted(_) => &mut self.tablet_pcsa,
              TabletMessage::QuerySuccess(_) => &mut self.tablet_pcsa,
              TabletMessage::FinishQuery(_) => &mut self.tablet_finish_query,
              TabletMessage::AlterTable(_) => &mut self.tablet_ddl,
              TabletMessage::DropTable(_) => &mut self.tablet_ddl,
            },
            SlaveRemotePayload::CoordMessage(_, m) => match m {
              CoordMessage::MasterQueryPlanningSuccess(_) => &mut self.unnaccounted,
              CoordMessage::PerformQuery(_) => &mut self.coord_pcsa,
              CoordMessage::CancelQuery(_) => &mut self.coord_pcsa,
              CoordMessage::QueryAborted(_) => &mut self.coord_pcsa,
              CoordMessage::QuerySuccess(_) => &mut self.coord_pcsa,
              CoordMessage::FinishQuery(_) => &mut self.coord_finish_query,
              CoordMessage::RegisterQuery(_) => &mut self.unnaccounted,
            },
            SlaveRemotePayload::ReconfigSlaveGroup(_) => &mut self.unnaccounted,
          },
        },
        SlaveMessage::RemoteLeaderChangedGossip(_) => &mut self.slave_remote_leader_changed,
        SlaveMessage::PaxosDriverMessage(m) => match m {
          PaxosDriverMessage::MultiPaxosMessage(_) => &mut self.slave_multi_paxos_message,
          PaxosDriverMessage::IsLeader(_) => &mut self.slave_is_leader,
          PaxosDriverMessage::LogSyncRequest(_) => &mut self.slave_log_sync_request,
          PaxosDriverMessage::LogSyncResponse(_) => &mut self.slave_log_sync_response,
          PaxosDriverMessage::NextIndexRequest(_) => &mut self.slave_next_index_request,
          PaxosDriverMessage::NextIndexResponse(_) => &mut self.slave_next_index_response,
          PaxosDriverMessage::InformLearned(_) => &mut self.slave_inform_learned,
          PaxosDriverMessage::NewNodeStarted(_) => &mut self.slave_new_node_started,
          PaxosDriverMessage::StartNewNode(_) => &mut self.slave_start_new_node,
        },
      },
      NetworkMessage::FreeNode(m) => match m {
        FreeNodeMessage::StartMaster(_) => &mut self.unnaccounted,
        FreeNodeMessage::FreeNodeRegistered(_) => &mut self.unnaccounted,
        FreeNodeMessage::MasterLeadershipId(_) => &mut self.free_node_master_leadership,
        FreeNodeMessage::ShutdownNode => &mut self.unnaccounted,
        FreeNodeMessage::CreateSlaveGroup(_) => &mut self.unnaccounted,
        FreeNodeMessage::SlaveSnapshot(_) => &mut self.unnaccounted,
        FreeNodeMessage::MasterSnapshot(_) => &mut self.unnaccounted,
      },
    };

    *count_ref += 1;
  }
}
