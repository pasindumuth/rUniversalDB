use crate::message as msg;
use crate::simple_tm_es::{
  SimplePayloadTypes, SimplePrepare, SimpleRMAborted, SimpleRMCommitted, SimpleRMPrepared,
};
use crate::slave::SlaveContext;
use rand::RngCore;
use runiversal::common::BasicIOCtx;
use runiversal::paxos2pc_rm::{Paxos2PCRMInner, Paxos2PCRMOuter};
use runiversal::paxos2pc_tm::{PayloadTypes, RMCommittedPLm};

// -----------------------------------------------------------------------------------------------
//  SimpleES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct SimpleRMInner {}

pub type SimpleRMES = Paxos2PCRMOuter<SimplePayloadTypes, SimpleRMInner>;

impl Paxos2PCRMInner<SimplePayloadTypes> for SimpleRMInner {
  fn new<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    io_ctx: &mut IO,
    _: SimplePrepare,
    _: &mut (),
  ) -> Option<SimpleRMInner> {
    // Here, we randomly decide whether to accept continue or Abort. // We abort with 5% chance.
    if io_ctx.rand().next_u32() % 100 < 5 {
      None
    } else {
      Some(SimpleRMInner {})
    }
  }

  fn new_follower<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    _: &mut IO,
    _: SimpleRMPrepared,
  ) -> SimpleRMInner {
    SimpleRMInner {}
  }

  fn early_aborted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut <SimplePayloadTypes as PayloadTypes>::RMContext,
    _: &mut IO,
  ) {
  }

  fn mk_prepared_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> SimpleRMPrepared {
    SimpleRMPrepared {}
  }

  fn prepared_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) {
  }

  fn mk_committed_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> SimpleRMCommitted {
    SimpleRMCommitted {}
  }

  fn committed_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) {
  }

  fn mk_aborted_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> SimpleRMAborted {
    SimpleRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) {
  }
}
