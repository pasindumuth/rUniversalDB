use crate::model::common::EndpointId;
use std::collections::{BTreeMap, VecDeque};

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------
// Construct the PaxosNode EndpointIds of the paxos at the given index.
pub fn mk_paxos_eid(i: u32) -> EndpointId {
  EndpointId(format!("pe{}", i))
}

// Construct the Master EndpointId of the Master at the given index.
pub fn mk_master_eid(i: u32) -> EndpointId {
  EndpointId(format!("me{}", i))
}

// Construct the Slave EndpointId of the Slave at the given index.
pub fn mk_slave_eid(i: u32) -> EndpointId {
  EndpointId(format!("se{}", i))
}

// Construct the Client id of the slave at the given index.
pub fn mk_client_eid(i: u32) -> EndpointId {
  EndpointId(format!("ce{}", i))
}

/// Add a message between two nodes in the network.
pub fn add_msg<NetworkMessageT>(
  queues: &mut BTreeMap<EndpointId, BTreeMap<EndpointId, VecDeque<NetworkMessageT>>>,
  nonempty_queues: &mut Vec<(EndpointId, EndpointId)>,
  msg: NetworkMessageT,
  from_eid: &EndpointId,
  to_eid: &EndpointId,
) {
  let queue = queues.get_mut(from_eid).unwrap().get_mut(to_eid).unwrap();
  if queue.len() == 0 {
    let queue_id = (from_eid.clone(), to_eid.clone());
    nonempty_queues.push(queue_id);
  }
  queue.push_back(msg);
}
