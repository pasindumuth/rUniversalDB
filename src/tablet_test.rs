use crate::finish_query_rm_es::FinishQueryRMES;
use crate::model::common::{PrimaryKey, QueryId, TabletKeyRange};
use crate::tablet::{check_range_inclusion, TabletState, DDLES};
use crate::test_utils::{cvb, cvi, cvs, CheckCtx};
use std::collections::BTreeMap;

#[test]
fn test_key_comparison() {
  assert_eq!(
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] },
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
  );

  assert!(
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
      < PrimaryKey { cols: vec![cvi(3), cvs("a"), cvb(false)] }
  );

  assert!(
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
      < PrimaryKey { cols: vec![cvi(2), cvs("b"), cvb(false)] }
  );

  assert!(
    PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(false)] }
      < PrimaryKey { cols: vec![cvi(2), cvs("a"), cvb(true)] }
  );
}

// -----------------------------------------------------------------------------------------------
//  Consistency Testing
// -----------------------------------------------------------------------------------------------

/// Asserts various consistency properties in the `TabletState`.
pub fn assert_tablet_consistency(tablet: &TabletState) {
  let statuses = &tablet.statuses;

  // Verify for every MSQueryES, every ES in `pending_queries` exist.
  for (query_id, ms_query_es) in &statuses.ms_query_ess {
    for child_qid in &ms_query_es.pending_queries {
      if let Some(wrapper) = statuses.ms_table_read_ess.get(child_qid) {
        assert_eq!(&wrapper.es.general.ms_query_id, query_id);
      } else if let Some(wrapper) = statuses.ms_table_write_ess.get(child_qid) {
        assert_eq!(&wrapper.es.general.ms_query_id, query_id);
      } else if let Some(wrapper) = statuses.ms_table_insert_ess.get(child_qid) {
        assert_eq!(&wrapper.es.general.ms_query_id, query_id);
      } else if let Some(wrapper) = statuses.ms_table_delete_ess.get(child_qid) {
        assert_eq!(&wrapper.es.general.ms_query_id, query_id);
      } else {
        panic!();
      }
    }
  }

  // Verify that for every MSTable*ES, a valid MSQueryES exists.
  for (query_id, wrapper) in &statuses.ms_table_read_ess {
    if let Some(ms_query_es) = statuses.ms_query_ess.get(&wrapper.es.general.ms_query_id) {
      assert!(ms_query_es.pending_queries.contains(query_id));
    } else {
      panic!()
    }
  }
  for (query_id, wrapper) in &statuses.ms_table_write_ess {
    if let Some(ms_query_es) = statuses.ms_query_ess.get(&wrapper.es.general.ms_query_id) {
      assert!(ms_query_es.pending_queries.contains(query_id));
    } else {
      panic!()
    }
  }
  for (query_id, wrapper) in &statuses.ms_table_insert_ess {
    if let Some(ms_query_es) = statuses.ms_query_ess.get(&wrapper.es.general.ms_query_id) {
      assert!(ms_query_es.pending_queries.contains(query_id));
    } else {
      panic!()
    }
  }
  for (query_id, wrapper) in &statuses.ms_table_delete_ess {
    if let Some(ms_query_es) = statuses.ms_query_ess.get(&wrapper.es.general.ms_query_id) {
      assert!(ms_query_es.pending_queries.contains(query_id));
    } else {
      panic!()
    }
  }
}

pub fn check_tablet_clean(tablet: &TabletState, check_ctx: &mut CheckCtx) {
  let statuses = &tablet.statuses;
  let ctx = &tablet.ctx;

  // Check `Tablet` clean

  check_ctx.check(ctx.verifying_writes.is_empty());
  check_ctx.check(ctx.inserting_prepared_writes.is_empty());
  check_ctx.check(ctx.prepared_writes.is_empty());

  check_ctx.check(ctx.waiting_read_protected.is_empty());
  check_ctx.check(ctx.inserting_read_protected.is_empty());

  check_ctx.check(ctx.waiting_locked_cols.is_empty());
  check_ctx.check(ctx.inserting_locked_cols.is_empty());

  check_ctx.check(ctx.ms_root_query_map.is_empty());

  // Check `Statuses` clean

  check_ctx.check(statuses.perform_query_buffer.is_empty());

  check_ctx.check(statuses.gr_query_ess.is_empty());
  check_ctx.check(statuses.table_read_ess.is_empty());
  check_ctx.check(statuses.trans_table_read_ess.is_empty());
  check_ctx.check(statuses.tm_statuss.is_empty());
  check_ctx.check(statuses.ms_query_ess.is_empty());
  check_ctx.check(statuses.ms_table_read_ess.is_empty());
  check_ctx.check(statuses.ms_table_write_ess.is_empty());
  check_ctx.check(statuses.ms_table_insert_ess.is_empty());
  check_ctx.check(statuses.ms_table_delete_ess.is_empty());
  for (_, es) in &statuses.finish_query_ess {
    if let FinishQueryRMES::Paxos2PCRMExecOuter(_) = es {
      check_ctx.check(false);
    }
  }

  check_ctx.check(match &statuses.ddl_es {
    DDLES::None => true,
    DDLES::Alter(_) => false,
    DDLES::Drop(_) => false,
    DDLES::Dropped(_) => true,
  });
}
