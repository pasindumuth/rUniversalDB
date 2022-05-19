use crate::common::{EndpointId, Gen, LeadershipId, PaxosGroupId};
use crate::common::{LeaderMap, RemoteLeaderChangedPLm, VersionedValue};
use crate::message as msg;
use std::collections::BTreeMap;

pub struct NetworkDriverContext<'a> {
  pub this_gid: &'a PaxosGroupId,
  pub this_eid: &'a EndpointId,
  pub leader_map: &'a VersionedValue<LeaderMap>,
  pub remote_leader_changes: &'a mut Vec<RemoteLeaderChangedPLm>,
}

// TODO: amend the proof for when PaxosGroupIds get removed. It can go something like:
//  "Safety: a `remote_message` with PaxosGroupId outside of `network_buffer` will never
//  come in unless `leader_map` comes in containing the new PaxosGroupIds. Liveness: a
//  buffered messages, either `deliver_blocked_messages` is called with high enough `lid`,
//  or the `gid` is removed.

#[derive(Debug)]
pub struct NetworkDriver<PayloadT> {
  /// This buffers the NetworkMessages until the corresponding `LeadershipId` in the
  /// `LeaderMap` is sufficiently high enough. Some properties:
  ///   1. All `RemoteMessage`s for a given `PaxosGroupId` have the same `from_lid`.
  network_buffer: BTreeMap<PaxosGroupId, Vec<msg::RemoteMessage<PayloadT>>>,
  /// The `Gen` of the `ctx.leader_map` that `network_buffer` corresponds to.
  gen: Gen,
}

impl<PayloadT: Clone> NetworkDriver<PayloadT> {
  pub fn new(leader_map: &VersionedValue<LeaderMap>) -> NetworkDriver<PayloadT> {
    let mut network_buffer = BTreeMap::<PaxosGroupId, Vec<msg::RemoteMessage<PayloadT>>>::new();
    for (gid, _) in leader_map.value() {
      network_buffer.insert(gid.clone(), Vec::new());
    }

    NetworkDriver { network_buffer, gen: leader_map.gen().clone() }
  }

  /// The precondition is that the `remote_message` is always from a `PaxosGroupId` that
  /// is in the `leader_map` in `ctx`.
  pub fn receive(
    &mut self,
    ctx: NetworkDriverContext,
    remote_message: msg::RemoteMessage<PayloadT>,
  ) -> Option<PayloadT> {
    // Update `network_buffer` if the LeaderMap has since been updated.
    if &self.gen < ctx.leader_map.gen() {
      self.gen = ctx.leader_map.gen().clone();

      // Add new PaxosGroupIds
      for (gid, _) in ctx.leader_map.value() {
        if !self.network_buffer.contains_key(gid) {
          self.network_buffer.insert(gid.clone(), vec![]);
        }
      }

      // Remove old PaxosGroupIds
      let mut removed_gids = Vec::<PaxosGroupId>::new();
      for (gid, _) in &self.network_buffer {
        if !ctx.leader_map.value().contains_key(gid) {
          removed_gids.push(gid.clone());
        }
      }
      for gid in removed_gids {
        self.network_buffer.remove(&gid);
      }
    }

    let this_gid = &ctx.this_gid;
    let this_lid = ctx.leader_map.value().get(&this_gid).unwrap();

    // A node only gets to this code if it is the Leader.
    debug_assert!(&this_lid.eid == ctx.this_eid);
    // Messages should not misrouted.
    debug_assert!(remote_message.to_lid.eid == this_lid.eid);
    // Messages should not be routed here ahead of this node knowing it is the Leader.
    debug_assert!(remote_message.to_lid.gen <= this_lid.gen);

    // Drop the RemoteMessage if it was destined to an older generation.
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
      let cur_from_lid = ctx.leader_map.value().get(&from_gid).unwrap();
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
    if let Some(buffer) = self.network_buffer.get_mut(&from_gid) {
      if !buffer.is_empty() {
        // Recall that the `from_lid.gen` of all bufferred messages should be the same.
        let new_from_lid = &buffer.get(0).unwrap().from_lid;
        if from_lid.gen > new_from_lid.gen {
          // Here, the new RemoteLeaderChangedPLm is beyond all buffered messages, so we drop them.
          buffer.clear();
          Vec::new()
        } else if from_lid.gen == new_from_lid.gen {
          // Deliver all messages from the buffer.
          let remote_messages = std::mem::replace(buffer, Vec::new());
          remote_messages.into_iter().map(|m| m.payload).collect()
        } else {
          // Here, the newly inserted RemoteLeaderChangedPLm will have no affect. Note that from
          // `recieve`, an appropriate one is still scheduled for insertion.
          Vec::new()
        }
      } else {
        Vec::new()
      }
    } else {
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
