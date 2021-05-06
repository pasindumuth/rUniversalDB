use crate::model::common::{EndpointId, TabletGroupId};
use crate::model::message::{NetworkMessage, TabletMessage};
use rand::RngCore;

pub trait NetworkOut {
  fn send(self, eid: EndpointId, msg: NetworkMessage);
}

pub trait TabletForwardOut {
  fn forward(self, tablet_group_id: TabletGroupId, msg: TabletMessage);
}

pub trait IOTypes {
  type RngCoreT: RngCore;
  type NetworkOutT: NetworkOut;
  type TabletForwardOutT: TabletForwardOut;
}
