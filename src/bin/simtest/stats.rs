use runiversal::master::MasterBundle;
use runiversal::model::message::{
  CoordMessage, ExternalMessage, FreeNodeAssoc, FreeNodeMessage, MasterExternalReq, MasterMessage,
  MasterRemotePayload, NetworkMessage, PaxosDriverMessage, RemoteMessage, SlaveExternalReq,
  SlaveMessage, SlaveReconfig, SlaveRemotePayload, TabletMessage,
};
use runiversal::slave::SharedPaxosBundle;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Message Statistics Keys
// -----------------------------------------------------------------------------------------------
// These strings are used to display the statistics.

const K_EXTERNAL_QUERY_SUCCESS: &str = "external_query_success";
const K_EXTERNAL_QUERY_ABORTED: &str = "external_query_aborted";
const K_EXTERNAL_DDL_QUERY_SUCCESS: &str = "external_ddl_query_success";
const K_EXTERNAL_DDL_QUERY_ABORTED: &str = "external_ddl_query_aborted";

// Master
const K_PERFORM_DDL_EXTERNAL_QUERY: &str = "perform_ddl_external_query";
const K_CANCEL_DDL_EXTERNAL_QUERY: &str = "cancel_ddl_external_query";

const K_MASTER_REMOTE_LEADER_CHANGED: &str = "master_remote_leader_changed";
const K_MASTER_DDL: &str = "master_ddl";
const K_MASTER_MASTER_GOSSIP: &str = "master_master_gossip";

// Master Paxos
const K_MASTER_MULTI_PAXOS_MESSAGE: &str = "master_multi_paxos_message";
const K_MASTER_IS_LEADER: &str = "master_is_leader";
const K_MASTER_LOG_SYNC_REQUEST: &str = "master_log_sync_request";
const K_MASTER_LOG_SYNC_RESPONSE: &str = "master_log_sync_response";
const K_MASTER_NEXT_INDEX_REQUEST: &str = "master_next_index_request";
const K_MASTER_NEXT_INDEX_RESPONSE: &str = "master_next_index_response";
const K_MASTER_INFORM_LEARNED: &str = "master_inform_learned";

const K_MASTER_NEW_NODE_STARTED: &str = "master_new_node_started";
const K_MASTER_START_NEW_NODE: &str = "master_start_new_node";

const K_MASTER_NODES_DEAD: &str = "master_nodes_dead";
const K_MASTER_SLAVE_GROUP_RECONFIGURED: &str = "master_slave_group_reconfigured";

const K_MASTER_REGISTER_FREE_NODE: &str = "master_register_free_node";
const K_MASTER_FREE_NODE_HEARTBEAT: &str = "master_free_node_heartbeat";
const K_MASTER_CONFIRM_SLAVE_CREATION: &str = "master_confirm_slave_creation";

// Slave
const K_PERFORM_EXTERNAL_QUERY: &str = "perform_external_query";
const K_CANCEL_EXTERNAL_QUERY: &str = "cancel_external_query";

const K_SLAVE_REMOTE_LEADER_CHANGED: &str = "slave_remote_leader_changed";
const K_SLAVE_CREATE_TABLE: &str = "slave_create_table";
const K_SLAVE_MASTER_GOSSIP: &str = "slave_master_gossip";

// Slave Paxos
const K_SLAVE_MULTI_PAXOS_MESSAGE: &str = "slave_multi_paxos_message";
const K_SLAVE_IS_LEADER: &str = "slave_is_leader";
const K_SLAVE_LOG_SYNC_REQUEST: &str = "slave_log_sync_request";
const K_SLAVE_LOG_SYNC_RESPONSE: &str = "slave_log_sync_response";
const K_SLAVE_NEXT_INDEX_REQUEST: &str = "slave_next_index_request";
const K_SLAVE_NEXT_INDEX_RESPONSE: &str = "slave_next_index_response";
const K_SLAVE_INFORM_LEARNED: &str = "slave_inform_learned";
const K_SLAVE_NEW_NODE_STARTED: &str = "slave_new_node_started";
const K_SLAVE_START_NEW_NODE: &str = "slave_start_new_node";

// Tablet
const K_TABLET_PCSA: &str = "tablet_pcsa";
const K_TABLET_FINISH_QUERY: &str = "tablet_finish_query";
const K_TABLET_DDL: &str = "tablet_ddl";

// Coord
const K_COORD_PCSA: &str = "coord_pcsa";
const K_COORD_FINISH_QUERY: &str = "coord_finish_query";

// FreeNode
const K_FREE_NODE_MASTER_LEADERSHIP: &str = "free_node_master_leadership";

// Unnaccounted
const K_UNNACCOUNTED: &str = "unnaccounted";

// -----------------------------------------------------------------------------------------------
//  Stats
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct Stats {
  /// Records the (system time) duration of the simulation.
  pub duration: u32,
  /// Records message frequency.
  message_stats: BTreeMap<&'static str, u32>,
}

impl Stats {
  pub fn get_message_stats(&self) -> &BTreeMap<&'static str, u32> {
    &self.message_stats
  }

  pub fn record(&mut self, m: &NetworkMessage) {
    let message_key = match m {
      NetworkMessage::External(m) => match m {
        ExternalMessage::ExternalQuerySuccess(_) => K_EXTERNAL_QUERY_SUCCESS,
        ExternalMessage::ExternalQueryAborted(_) => K_EXTERNAL_QUERY_ABORTED,
        ExternalMessage::ExternalDDLQuerySuccess(_) => K_EXTERNAL_DDL_QUERY_SUCCESS,
        ExternalMessage::ExternalDDLQueryAborted(_) => K_EXTERNAL_DDL_QUERY_ABORTED,
        ExternalMessage::ExternalDebugResponse(_) => K_UNNACCOUNTED,
      },
      NetworkMessage::Master(m) => match m {
        MasterMessage::MasterExternalReq(m) => match m {
          MasterExternalReq::PerformExternalDDLQuery(_) => K_PERFORM_DDL_EXTERNAL_QUERY,
          MasterExternalReq::CancelExternalDDLQuery(_) => K_CANCEL_DDL_EXTERNAL_QUERY,
          MasterExternalReq::ExternalDebugRequest(_) => K_UNNACCOUNTED,
        },
        MasterMessage::RemoteMessage(m) => match m {
          RemoteMessage { payload: m, .. } => match m {
            MasterRemotePayload::MasterQueryPlanning(_) => K_UNNACCOUNTED,
            MasterRemotePayload::CreateTable(_) => K_MASTER_DDL,
            MasterRemotePayload::AlterTable(_) => K_MASTER_DDL,
            MasterRemotePayload::DropTable(_) => K_MASTER_DDL,
            MasterRemotePayload::MasterGossipRequest(_) => K_UNNACCOUNTED,
            MasterRemotePayload::SlaveReconfig(m) => match m {
              SlaveReconfig::NodesDead(_) => K_MASTER_NODES_DEAD,
              SlaveReconfig::SlaveGroupReconfigured(_) => K_MASTER_SLAVE_GROUP_RECONFIGURED,
            },
          },
        },
        MasterMessage::RemoteLeaderChangedGossip(_) => K_MASTER_REMOTE_LEADER_CHANGED,
        MasterMessage::PaxosDriverMessage(m) => match m {
          PaxosDriverMessage::MultiPaxosMessage(_) => K_MASTER_MULTI_PAXOS_MESSAGE,
          PaxosDriverMessage::IsLeader(_) => K_MASTER_IS_LEADER,
          PaxosDriverMessage::LogSyncRequest(_) => K_MASTER_LOG_SYNC_REQUEST,
          PaxosDriverMessage::LogSyncResponse(_) => K_MASTER_LOG_SYNC_RESPONSE,
          PaxosDriverMessage::NextIndexRequest(_) => K_MASTER_NEXT_INDEX_REQUEST,
          PaxosDriverMessage::NextIndexResponse(_) => K_MASTER_NEXT_INDEX_RESPONSE,
          PaxosDriverMessage::InformLearned(_) => K_MASTER_INFORM_LEARNED,
          PaxosDriverMessage::NewNodeStarted(_) => K_MASTER_NEW_NODE_STARTED,
          PaxosDriverMessage::StartNewNode(_) => K_MASTER_START_NEW_NODE,
        },
        MasterMessage::FreeNodeAssoc(m) => match m {
          FreeNodeAssoc::RegisterFreeNode(_) => K_MASTER_REGISTER_FREE_NODE,
          FreeNodeAssoc::FreeNodeHeartbeat(_) => K_MASTER_FREE_NODE_HEARTBEAT,
          FreeNodeAssoc::ConfirmSlaveCreation(_) => K_MASTER_CONFIRM_SLAVE_CREATION,
        },
      },
      NetworkMessage::Slave(m) => match m {
        SlaveMessage::SlaveExternalReq(m) => match m {
          SlaveExternalReq::PerformExternalQuery(_) => K_PERFORM_EXTERNAL_QUERY,
          SlaveExternalReq::CancelExternalQuery(_) => K_CANCEL_EXTERNAL_QUERY,
        },
        SlaveMessage::RemoteMessage(m) => match m {
          RemoteMessage { payload: m, .. } => match m {
            SlaveRemotePayload::CreateTable(_) => K_SLAVE_CREATE_TABLE,
            SlaveRemotePayload::MasterGossip(_) => K_SLAVE_MASTER_GOSSIP,
            SlaveRemotePayload::TabletMessage(_, m) => match m {
              TabletMessage::PerformQuery(_) => K_TABLET_PCSA,
              TabletMessage::CancelQuery(_) => K_TABLET_PCSA,
              TabletMessage::QueryAborted(_) => K_TABLET_PCSA,
              TabletMessage::QuerySuccess(_) => K_TABLET_PCSA,
              TabletMessage::FinishQuery(_) => K_TABLET_FINISH_QUERY,
              TabletMessage::AlterTable(_) => K_TABLET_DDL,
              TabletMessage::DropTable(_) => K_TABLET_DDL,
            },
            SlaveRemotePayload::CoordMessage(_, m) => match m {
              CoordMessage::MasterQueryPlanningSuccess(_) => K_UNNACCOUNTED,
              CoordMessage::PerformQuery(_) => K_COORD_PCSA,
              CoordMessage::CancelQuery(_) => K_COORD_PCSA,
              CoordMessage::QueryAborted(_) => K_COORD_PCSA,
              CoordMessage::QuerySuccess(_) => K_COORD_PCSA,
              CoordMessage::FinishQuery(_) => K_COORD_FINISH_QUERY,
              CoordMessage::RegisterQuery(_) => K_UNNACCOUNTED,
            },
            SlaveRemotePayload::ReconfigSlaveGroup(_) => K_UNNACCOUNTED,
          },
        },
        SlaveMessage::RemoteLeaderChangedGossip(_) => K_SLAVE_REMOTE_LEADER_CHANGED,
        SlaveMessage::PaxosDriverMessage(m) => match m {
          PaxosDriverMessage::MultiPaxosMessage(_) => K_SLAVE_MULTI_PAXOS_MESSAGE,
          PaxosDriverMessage::IsLeader(_) => K_SLAVE_IS_LEADER,
          PaxosDriverMessage::LogSyncRequest(_) => K_SLAVE_LOG_SYNC_REQUEST,
          PaxosDriverMessage::LogSyncResponse(_) => K_SLAVE_LOG_SYNC_RESPONSE,
          PaxosDriverMessage::NextIndexRequest(_) => K_SLAVE_NEXT_INDEX_REQUEST,
          PaxosDriverMessage::NextIndexResponse(_) => K_SLAVE_NEXT_INDEX_RESPONSE,
          PaxosDriverMessage::InformLearned(_) => K_SLAVE_INFORM_LEARNED,
          PaxosDriverMessage::NewNodeStarted(_) => K_SLAVE_NEW_NODE_STARTED,
          PaxosDriverMessage::StartNewNode(_) => K_SLAVE_START_NEW_NODE,
        },
      },
      NetworkMessage::FreeNode(m) => match m {
        FreeNodeMessage::StartMaster(_) => K_UNNACCOUNTED,
        FreeNodeMessage::FreeNodeRegistered(_) => K_UNNACCOUNTED,
        FreeNodeMessage::MasterLeadershipId(_) => K_FREE_NODE_MASTER_LEADERSHIP,
        FreeNodeMessage::ShutdownNode => K_UNNACCOUNTED,
        FreeNodeMessage::CreateSlaveGroup(_) => K_UNNACCOUNTED,
        FreeNodeMessage::SlaveSnapshot(_) => K_UNNACCOUNTED,
        FreeNodeMessage::MasterSnapshot(_) => K_UNNACCOUNTED,
      },
    };

    if let Some(count) = self.message_stats.get_mut(message_key) {
      *count += 1;
    } else {
      self.message_stats.insert(message_key, 1);
    }
  }
}
