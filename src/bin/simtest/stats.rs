use runiversal::master::MasterBundle;
use runiversal::message::{
  CoordMessage, ExternalMessage, FreeNodeAssoc, FreeNodeMessage, MasterExternalReq, MasterMessage,
  MasterRemotePayload, NetworkMessage, PaxosDriverMessage, RemoteMessage, SlaveExternalReq,
  SlaveMessage, SlaveReconfig, SlaveRemotePayload, TabletMessage,
};
use runiversal::slave::SharedPaxosBundle;
use std::cmp::max;
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------------------------
//  Message Statistics Keys
// -----------------------------------------------------------------------------------------------
// These strings are used to display the statistics.

const K_EXTERNAL_QUERY_SUCCESS: &str = "external_query_success";
const K_EXTERNAL_QUERY_ABORTED: &str = "external_query_aborted";
const K_EXTERNAL_DDL_QUERY_SUCCESS: &str = "external_ddl_query_success";
const K_EXTERNAL_DDL_QUERY_ABORTED: &str = "external_ddl_query_aborted";
const K_EXTERNAL_SHARDING_SUCCESS: &str = "external_sharding_success";
const K_EXTERNAL_SHARDING_ABORTED: &str = "external_sharding_aborted";

// Master
const K_PERFORM_EXTERNAL_DDL_QUERY: &str = "perform_external_ddl_query";
const K_CANCEL_EXTERNAL_DDL_QUERY: &str = "cancel_external_ddl_query";

const K_PERFORM_EXTERNAL_SHARDING: &str = "perform_external_sharding";
const K_CANCEL_EXTERNAL_SHARDING: &str = "cancel_external_sharding";

const K_MASTER_REMOTE_LEADER_CHANGED: &str = "master_remote_leader_changed";
const K_MASTER_DDL: &str = "master_ddl";
const K_MASTER_SHARDING: &str = "master_sharding";
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
const K_SLAVE_SHARDING: &str = "slave_master_sharding";
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
const K_TABLET_PERFORM: &str = "tablet_perform";
const K_TABLET_CANCEL: &str = "tablet_cancel";
const K_TABLET_SUCCESS: &str = "tablet_success";
const K_TABLET_ABORT: &str = "tablet_abort";
const K_TABLET_FINISH_QUERY: &str = "tablet_finish_query";
const K_TABLET_DDL: &str = "tablet_ddl";
const K_TABLET_SHARDING: &str = "tablet_sharding";

// Coord
const K_COORD_PCSA: &str = "coord_pcsa";
const K_COORD_FINISH_QUERY: &str = "coord_finish_query";

// FreeNode
const K_FREE_NODE_MASTER_LEADERSHIP: &str = "free_node_master_leadership";

// Unnaccounted
const K_UNNACCOUNTED: &str = "unnaccounted";

/// This defines the order that the messages should be displayed.
const NUM_MESSAGES: usize = 54;
const DISPLAY_ORDER: [&str; NUM_MESSAGES] = [
  K_EXTERNAL_QUERY_SUCCESS,
  K_EXTERNAL_QUERY_ABORTED,
  K_EXTERNAL_DDL_QUERY_SUCCESS,
  K_EXTERNAL_DDL_QUERY_ABORTED,
  K_EXTERNAL_SHARDING_SUCCESS,
  K_EXTERNAL_SHARDING_ABORTED,
  K_PERFORM_EXTERNAL_DDL_QUERY,
  K_CANCEL_EXTERNAL_DDL_QUERY,
  K_PERFORM_EXTERNAL_SHARDING,
  K_CANCEL_EXTERNAL_SHARDING,
  K_MASTER_REMOTE_LEADER_CHANGED,
  K_MASTER_DDL,
  K_MASTER_SHARDING,
  K_MASTER_MASTER_GOSSIP,
  K_MASTER_MULTI_PAXOS_MESSAGE,
  K_MASTER_IS_LEADER,
  K_MASTER_LOG_SYNC_REQUEST,
  K_MASTER_LOG_SYNC_RESPONSE,
  K_MASTER_NEXT_INDEX_REQUEST,
  K_MASTER_NEXT_INDEX_RESPONSE,
  K_MASTER_INFORM_LEARNED,
  K_MASTER_NEW_NODE_STARTED,
  K_MASTER_START_NEW_NODE,
  K_MASTER_NODES_DEAD,
  K_MASTER_SLAVE_GROUP_RECONFIGURED,
  K_MASTER_REGISTER_FREE_NODE,
  K_MASTER_FREE_NODE_HEARTBEAT,
  K_MASTER_CONFIRM_SLAVE_CREATION,
  K_PERFORM_EXTERNAL_QUERY,
  K_CANCEL_EXTERNAL_QUERY,
  K_SLAVE_REMOTE_LEADER_CHANGED,
  K_SLAVE_CREATE_TABLE,
  K_SLAVE_SHARDING,
  K_SLAVE_MASTER_GOSSIP,
  K_SLAVE_MULTI_PAXOS_MESSAGE,
  K_SLAVE_IS_LEADER,
  K_SLAVE_LOG_SYNC_REQUEST,
  K_SLAVE_LOG_SYNC_RESPONSE,
  K_SLAVE_NEXT_INDEX_REQUEST,
  K_SLAVE_NEXT_INDEX_RESPONSE,
  K_SLAVE_INFORM_LEARNED,
  K_SLAVE_NEW_NODE_STARTED,
  K_SLAVE_START_NEW_NODE,
  K_TABLET_PERFORM,
  K_TABLET_CANCEL,
  K_TABLET_SUCCESS,
  K_TABLET_ABORT,
  K_TABLET_FINISH_QUERY,
  K_TABLET_DDL,
  K_TABLET_SHARDING,
  K_COORD_PCSA,
  K_COORD_FINISH_QUERY,
  K_FREE_NODE_MASTER_LEADERSHIP,
  K_UNNACCOUNTED,
];

// -----------------------------------------------------------------------------------------------
//  Stats
// -----------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Stats {
  /// Records the (system time) duration of the simulation.
  pub duration: u32,
  /// Records message frequency.
  reverse_map: BTreeMap<&'static str, usize>,
  message_stats: Vec<(&'static str, u32)>,
}

impl Stats {
  pub fn new() -> Stats {
    let mut reverse_map = BTreeMap::<&'static str, usize>::new();
    let mut message_stats = Vec::<(&'static str, u32)>::new();
    for (i, message_name) in DISPLAY_ORDER.iter().enumerate() {
      reverse_map.insert(message_name, i);
      message_stats.push((message_name, 0));
    }
    Stats { duration: 0, reverse_map, message_stats }
  }

  pub fn get_message_stats(&self) -> &Vec<(&'static str, u32)> {
    &self.message_stats
  }

  pub fn record(&mut self, m: &NetworkMessage) {
    let message_key = match m {
      NetworkMessage::External(m) => match m {
        ExternalMessage::ExternalQuerySuccess(_) => K_EXTERNAL_QUERY_SUCCESS,
        ExternalMessage::ExternalQueryAborted(_) => K_EXTERNAL_QUERY_ABORTED,
        ExternalMessage::ExternalDDLQuerySuccess(_) => K_EXTERNAL_DDL_QUERY_SUCCESS,
        ExternalMessage::ExternalDDLQueryAborted(_) => K_EXTERNAL_DDL_QUERY_ABORTED,
        ExternalMessage::ExternalShardingSuccess(_) => K_EXTERNAL_SHARDING_SUCCESS,
        ExternalMessage::ExternalShardingAborted(_) => K_EXTERNAL_SHARDING_ABORTED,
        ExternalMessage::ExternalDebugResponse(_) => K_UNNACCOUNTED,
      },
      NetworkMessage::Master(m) => match m {
        MasterMessage::MasterExternalReq(m) => match m {
          MasterExternalReq::PerformExternalDDLQuery(_) => K_PERFORM_EXTERNAL_DDL_QUERY,
          MasterExternalReq::CancelExternalDDLQuery(_) => K_CANCEL_EXTERNAL_DDL_QUERY,
          MasterExternalReq::ExternalDebugRequest(_) => K_UNNACCOUNTED,
          MasterExternalReq::PerformExternalSharding(_) => K_PERFORM_EXTERNAL_SHARDING,
          MasterExternalReq::CancelExternalSharding(_) => K_CANCEL_EXTERNAL_SHARDING,
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
            MasterRemotePayload::ShardSplit(_) => K_MASTER_SHARDING,
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
              TabletMessage::PerformQuery(_) => K_TABLET_PERFORM,
              TabletMessage::CancelQuery(_) => K_TABLET_CANCEL,
              TabletMessage::QueryAborted(_) => K_TABLET_SUCCESS,
              TabletMessage::QuerySuccess(_) => K_TABLET_ABORT,
              TabletMessage::FinishQuery(_) => K_TABLET_FINISH_QUERY,
              TabletMessage::AlterTable(_) => K_TABLET_DDL,
              TabletMessage::DropTable(_) => K_TABLET_DDL,
              TabletMessage::ShardSplit(_) => K_TABLET_SHARDING,
              TabletMessage::ShardingConfirmed(_) => K_TABLET_SHARDING,
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
            SlaveRemotePayload::ShardSplit(_) => K_SLAVE_SHARDING,
            SlaveRemotePayload::ShardingMessage(_) => K_SLAVE_SHARDING,
          },
        },
        SlaveMessage::MasterGossip(_) => K_SLAVE_MASTER_GOSSIP,
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

    let index = self.reverse_map.get_mut(&message_key).unwrap();
    let (_, count) = self.message_stats.get_mut(*index).unwrap();
    *count += 1;
  }
}

// -----------------------------------------------------------------------------------------------
//  Stat Utils
// -----------------------------------------------------------------------------------------------

/// Takes the average of all numbers in `Stats` across the whole vector `all_stats`. There
/// must be at least one `all_stats`.
pub fn process_stats(all_stats: Vec<Stats>) -> (f64, Vec<(&'static str, f64)>) {
  let num_stats = all_stats.len();

  let mut avg_duration: f64 = 0.0;
  let mut avg_message_stats = Vec::<(&'static str, f64)>::new();

  // Initialize using the first `Stats`.
  let mut it = all_stats.into_iter();
  for (message, count) in it.next().unwrap().get_message_stats() {
    avg_message_stats.push((message, *count as f64));
  }

  // First, take the sum of the desired stats.
  while let Some(stats) = it.next() {
    avg_duration += stats.duration as f64;
    for (i, (_, count)) in stats.get_message_stats().iter().enumerate() {
      let (_, accum) = avg_message_stats.get_mut(i).unwrap();
      *accum += *count as f64
    }
  }

  // Next, compute the average.
  avg_duration /= num_stats as f64;
  for (_, count) in &mut avg_message_stats {
    *count /= num_stats as f64;
  }

  (avg_duration, avg_message_stats)
}

/// Formats the `message_stats` into a string such that the colons in the map are aligned.
pub fn format_message_stats(message_stats: &Vec<(&'static str, f64)>) -> String {
  let mut max_key_len = 0;
  for (key, _) in message_stats {
    max_key_len = max(max_key_len, key.len());
  }

  let mut lines = Vec::<String>::new();
  lines.push("{".to_string());
  for (key, count) in message_stats {
    lines.push(format!(
      "{spaces}{key}: {count},",
      spaces = " ".repeat(max_key_len - key.len() + 4), // We use an indent of 4
      key = key,
      count = count.floor()
    ));
  }
  lines.push("}".to_string());

  lines.join("\n")
}
