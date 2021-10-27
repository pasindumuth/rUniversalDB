use crate::message as msg;
use crate::simple_tm_es::{
  SimpleClosed, SimpleCommit, SimplePayloadTypes, SimplePrepare, SimplePrepared, SimpleRMAborted,
  SimpleRMCommitted, SimpleRMPrepared,
};
use crate::slave::SlaveContext;
use runiversal::common::BasicIOCtx;
use runiversal::stmpaxos2pc_rm::{STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use runiversal::stmpaxos2pc_tm::RMCommittedPLm;

// -----------------------------------------------------------------------------------------------
//  SimpleES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct SimpleRMInner {}

pub type SimpleRMES = STMPaxos2PCRMOuter<SimplePayloadTypes, SimpleRMInner>;

impl STMPaxos2PCRMInner<SimplePayloadTypes> for SimpleRMInner {
  fn new<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    _: &mut IO,
    _: SimplePrepare,
  ) -> SimpleRMInner {
    SimpleRMInner {}
  }

  fn new_follower<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    _: &mut IO,
    _: SimpleRMPrepared,
  ) -> SimpleRMInner {
    SimpleRMInner {}
  }

  fn mk_closed() -> SimpleClosed {
    SimpleClosed {}
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
  ) -> SimplePrepared {
    SimplePrepared {}
  }

  fn mk_committed_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &SimpleCommit,
  ) -> SimpleRMCommitted {
    SimpleRMCommitted {}
  }

  fn committed_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &RMCommittedPLm<SimplePayloadTypes>,
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
