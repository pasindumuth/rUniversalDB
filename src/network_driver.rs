use crate::common::RemoteLeaderChangedPLm;
use crate::model::common::{EndpointId, LeadershipId, PaxosGroupId};
use crate::model::message as msg;
use std::collections::HashMap;

pub struct NetworkDriverContext<'a> {
  pub this_gid: &'a PaxosGroupId,
  pub this_eid: &'a EndpointId,
  pub leader_map: &'a HashMap<PaxosGroupId, LeadershipId>,
  pub remote_leader_changes: &'a mut Vec<RemoteLeaderChangedPLm>,
}

#[derive(Debug)]
pub struct NetworkDriver<PayloadT> {
  network_buffer: HashMap<PaxosGroupId, Vec<msg::RemoteMessage<PayloadT>>>,
}

impl<PayloadT: Clone> NetworkDriver<PayloadT> {
  pub fn new(all_gids: Vec<PaxosGroupId>) -> NetworkDriver<PayloadT> {
    let mut network_buffer = HashMap::<PaxosGroupId, Vec<msg::RemoteMessage<PayloadT>>>::new();
    for gid in all_gids {
      network_buffer.insert(gid, Vec::new());
    }

    NetworkDriver { network_buffer }
  }

  pub fn receive(
    &mut self,
    ctx: NetworkDriverContext,
    remote_message: msg::RemoteMessage<PayloadT>,
  ) -> Option<PayloadT> {
    let this_gid = &ctx.this_gid;
    let this_lid = ctx.leader_map.get(&this_gid).unwrap();

    // A node only gets to this code if it is the Leader.
    debug_assert!(&this_lid.eid == ctx.this_eid);
    // Messages should not misrouted.
    debug_assert!(remote_message.to_lid.eid == this_lid.eid);
    // Messages should not be routed here ahead of this node knowing it is the Leader.
    debug_assert!(remote_message.to_lid.gen <= this_lid.gen);

    // If the RemoteMessage was destined to an older generation, dropping if so.
    if remote_message.to_lid.gen < this_lid.gen {
      return None;
    }

    // This assertion follows immediately from the above.
    debug_assert!(remote_message.to_lid.gen == this_lid.gen);

    let from_gid = remote_message.from_gid.clone();
    let from_lid = remote_message.from_lid.clone();
    let buffer = self.network_buffer.get_mut(&from_gid).unwrap();
    if !buffer.is_empty() {
      // This means there are already messages from a new remote Leader.
      let new_from_lid = &buffer.get(0).unwrap().from_lid;
      if from_lid.gen < new_from_lid.gen {
        // The Leadership of the new message is too old, so we drop it.
        None
      } else if from_lid.gen == new_from_lid.gen {
        // The Leadership of the new message is the same as the other new messages, so we push.
        buffer.push(remote_message);
        None
      } else {
        // The Leadership of the new message is even newer, so we replace.
        buffer.clear();
        buffer.push(remote_message);
        // We also add a new RemoteLeaderChanged PLm to be inserted.
        ctx.remote_leader_changes.push(RemoteLeaderChangedPLm { gid: from_gid, lid: from_lid });
        None
      }
    } else {
      let cur_from_lid = ctx.leader_map.get(&from_gid).unwrap();
      if from_lid.gen < cur_from_lid.gen {
        // The Leadership of the new message is old, so we drop it.
        None
      } else if from_lid.gen == cur_from_lid.gen {
        // The Leadership of the new message is current, so we Deliver the message
        Some(remote_message.payload)
      } else {
        // The Leadership of the new message is new, so we buffer it.
        buffer.push(remote_message);
        // We also add a new RemoteLeaderChanged PLm to be inserted.
        ctx.remote_leader_changes.push(RemoteLeaderChangedPLm { gid: from_gid, lid: from_lid });
        None
      }
    }
  }

  /// This is called a `RemoteLeaderChangedPLm` is inserted.
  pub fn deliver_blocked_messages(
    &mut self,
    from_gid: PaxosGroupId,
    from_lid: LeadershipId,
  ) -> Vec<PayloadT> {
    let buffer = self.network_buffer.get_mut(&from_gid).unwrap();
    // Recall that the `from_lid.gen` of all bufferred messages should be the same.
    let new_from_lid = &buffer.get(0).unwrap().from_lid;
    if new_from_lid.gen < from_lid.gen {
      // Here, the new RemoteLeaderChangedPLm is beyond all buffered messages, so we drop them.
      buffer.clear();
      Vec::new()
    } else if from_lid.gen == new_from_lid.gen {
      // Deliver all messages from the buffer.
      let remote_messages = buffer.clone();
      buffer.clear();
      remote_messages.into_iter().map(|m| m.payload).collect()
    } else {
      // Here, the newly inserted RemoteLeaderChangedPLm will have no affect. Note that from
      // `recieve`, an appropriate one is still scheduled for insertion.
      Vec::new()
    }
  }

  // Here, we just clear the NetworkBuffer.
  pub fn leader_changed(&mut self) {
    for (_, buffer) in &mut self.network_buffer {
      buffer.clear();
    }
  }
}
