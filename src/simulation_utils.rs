use crate::common::{EndpointId, InternalMode};
use std::collections::{BTreeMap, VecDeque};

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------
// Construct the PaxosNode EndpointIds of the paxos at the given index.
pub fn mk_paxos_eid(i: u32) -> EndpointId {
  EndpointId::new(format!("pe{}", i), InternalMode::Internal)
}

// Construct the Slave EndpointId of the Slave at the given index.
pub fn mk_slave_eid(i: u32) -> EndpointId {
  EndpointId::new(format!("se{}", i), InternalMode::Internal)
}

// Construct the EndpointId of a Node.
pub fn mk_node_eid(i: u32) -> EndpointId {
  EndpointId::new(format!("ne{}", i), InternalMode::Internal)
}

// Construct the Client id of the slave at the given index.
pub fn mk_client_eid(i: u32) -> EndpointId {
  EndpointId::new(format!("ce{}", i), InternalMode::External { salt: "".to_string() })
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
