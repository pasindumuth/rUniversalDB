use crate::message as msg;
use crate::slave::SlaveContext;
use crate::stm_simple_tm_es::{
  STMSimpleClosed, STMSimpleCommit, STMSimplePayloadTypes, STMSimplePrepare, STMSimplePrepared,
  STMSimpleRMAborted, STMSimpleRMCommitted, STMSimpleRMPrepared,
};
use runiversal::common::BasicIOCtx;
use runiversal::stmpaxos2pc_rm::{STMPaxos2PCRMAction, STMPaxos2PCRMInner, STMPaxos2PCRMOuter};
use runiversal::stmpaxos2pc_tm::RMCommittedPLm;

// -----------------------------------------------------------------------------------------------
//  SimpleES Implementation
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
pub struct STMSimpleRMInner {}

pub type STMSimpleRMES = STMPaxos2PCRMOuter<STMSimplePayloadTypes, STMSimpleRMInner>;
pub type STMSimpleRMAction = STMPaxos2PCRMAction<STMSimplePayloadTypes>;

impl STMPaxos2PCRMInner<STMSimplePayloadTypes> for STMSimpleRMInner {
  fn new<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    _: &mut IO,
    _: STMSimplePrepare,
  ) -> STMSimpleRMInner {
    STMSimpleRMInner {}
  }

  fn new_follower<IO: BasicIOCtx<msg::NetworkMessage>>(
    _: &mut SlaveContext,
    _: &mut IO,
    _: STMSimpleRMPrepared,
  ) -> STMSimpleRMInner {
    STMSimpleRMInner {}
  }

  fn mk_closed() -> STMSimpleClosed {
    STMSimpleClosed {}
  }

  fn mk_prepared_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> STMSimpleRMPrepared {
    STMSimpleRMPrepared {}
  }

  fn prepared_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> STMSimplePrepared {
    STMSimplePrepared {}
  }

  fn mk_committed_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &STMSimpleCommit,
  ) -> STMSimpleRMCommitted {
    STMSimpleRMCommitted {}
  }

  fn committed_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
    _: &RMCommittedPLm<STMSimplePayloadTypes>,
  ) {
  }

  fn mk_aborted_plm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) -> STMSimpleRMAborted {
    STMSimpleRMAborted {}
  }

  fn aborted_plm_inserted<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    _: &mut SlaveContext,
    _: &mut IO,
  ) {
  }
}
