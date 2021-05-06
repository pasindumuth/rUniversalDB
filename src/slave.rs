use crate::common::IOTypes;
use crate::model::message::SlaveMessage;
use rand::RngCore;

#[derive(Debug)]
pub struct SlaveState<T: IOTypes> {
  rand: T::RngCoreT,
  network_output: T::NetworkOutT,
  tablet_forward_output: T::TabletForwardOutT,
}

impl<T: IOTypes> SlaveState<T> {
  pub fn new(
    rand: T::RngCoreT,
    network_output: T::NetworkOutT,
    tablet_forward_output: T::TabletForwardOutT,
  ) -> SlaveState<T> {
    SlaveState {
      rand,
      network_output,
      tablet_forward_output,
    }
  }

  pub fn handle_incoming_message(&mut self, msg: SlaveMessage) {
    unimplemented!()
  }

  pub fn mk_rand(&mut self) -> u32 {
    self.rand.next_u32()
  }
}
