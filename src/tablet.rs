use crate::common::IOTypes;
use crate::model::message::TabletMessage;
use rand::RngCore;

#[derive(Debug)]
pub struct TabletState<T: IOTypes> {
  rand: T::RngCoreT,
  network_output: T::NetworkOutT,
}

impl<T: IOTypes> TabletState<T> {
  pub fn new(rand: T::RngCoreT, network_output: T::NetworkOutT) -> TabletState<T> {
    TabletState {
      rand,
      network_output,
    }
  }

  pub fn handle_incoming_message(&mut self, msg: TabletMessage) {
    unimplemented!();
  }

  pub fn mk_rand(&mut self) -> u32 {
    self.rand.next_u32()
  }
}
