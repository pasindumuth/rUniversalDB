use crate::message as msg;
use crate::slave::{SlaveContext, SlavePLm};
use crate::stm_simple_tm_es::{
  STMSimpleClosed, STMSimpleCommit, STMSimplePayloadTypes, STMSimplePrepare, STMSimplePrepared,
  STMSimpleRMAborted, STMSimpleRMCommitted, STMSimpleRMPrepared,
};
use runiversal::common::BasicIOCtx;
use runiversal::model::common::SlaveGroupId;
use runiversal::stmpaxos2pc_rm::{
  RMCommittedPLm, RMPLm, RMServerContext, STMPaxos2PCRMAction, STMPaxos2PCRMInner,
  STMPaxos2PCRMOuter,
};
use runiversal::stmpaxos2pc_tm::TMMessage;

// -----------------------------------------------------------------------------------------------
//  RMServerContext
// -----------------------------------------------------------------------------------------------

impl RMServerContext<STMSimplePayloadTypes> for SlaveContext {
  fn push_plm(&mut self, plm: RMPLm<STMSimplePayloadTypes>) {
    self.slave_bundle.plms.push(SlavePLm::SimpleSTMRM(plm));
  }

  fn send_to_tm<IO: BasicIOCtx<msg::NetworkMessage>>(
    &mut self,
    io_ctx: &mut IO,
    tm: &SlaveGroupId,
    msg: TMMessage<STMSimplePayloadTypes>,
  ) {
    self.send(io_ctx, tm, msg::SlaveRemotePayload::STMTMMessage(msg));
  }

  fn mk_node_path(&self) -> SlaveGroupId {
    self.this_sid.clone()
  }

  fn is_leader(&self) -> bool {
    SlaveContext::is_leader(self)
  }
}

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

  fn reconfig_snapshot(&self) -> Self {
    unimplemented!()
  }
}
