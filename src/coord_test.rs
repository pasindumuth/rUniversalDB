use crate::coord::CoordState;
use crate::test_utils::CheckCtx;

// -----------------------------------------------------------------------------------------------
//  Consistency Testing
// -----------------------------------------------------------------------------------------------

/// Asserts various consistency properties in the `CoordState`.
pub fn assert_coord_consistency(coord: &CoordState) {
  external_request_id_map_consistency(coord);
}

// Verify that every `MSCoordES` and every `FinishQueryTMES`.
fn external_request_id_map_consistency(coord: &CoordState) {
  let statuses = &coord.statuses;
  let ctx = &coord.ctx;

  if ctx.is_leader() {
    // If this is a Leader, we make sure all RequestIds in the ESs exist in
    // the `external_request_id_map`.
    for (qid, es) in &statuses.ms_coord_ess {
      if let Some(stored_qid) = ctx.external_request_id_map.get(&es.request_id) {
        assert_eq!(stored_qid, qid);
      } else {
        panic!();
      }
    }

    for (qid, es) in &statuses.finish_query_tm_ess {
      if let Some(response_data) = &es.inner.response_data {
        if let Some(stored_qid) = ctx.external_request_id_map.get(&response_data.request_id) {
          assert_eq!(stored_qid, qid);
        } else {
          panic!();
        }
      }
    }

    // Next, we see if all entries in `external_request_id_map` are in an ES.
    for (rid, qid) in &ctx.external_request_id_map {
      if let Some(es) = &statuses.ms_coord_ess.get(qid) {
        assert_eq!(&es.request_id, rid);
        assert!(!statuses.finish_query_tm_ess.contains_key(qid));
      } else if let Some(es) = &statuses.finish_query_tm_ess.get(qid) {
        if let Some(response_data) = &es.inner.response_data {
          assert_eq!(&response_data.request_id, rid);
        } else {
          panic!();
        }
      } else {
        panic!();
      }
    }
  } else {
    // If this is a Follower, we make sure it has `external_request_id_map` be empty.
    assert!(ctx.external_request_id_map.is_empty());
    assert!(statuses.ms_coord_ess.is_empty());
  }
}

pub fn check_coord_clean(coord: &CoordState, check_ctx: &mut CheckCtx) {
  let statuses = &coord.statuses;
  let ctx = &coord.ctx;

  // Check `Coord` clean
  check_ctx.check(ctx.external_request_id_map.is_empty());

  // Check `Status` clean
  check_ctx.check(statuses.finish_query_tm_ess.is_empty());
  check_ctx.check(statuses.ms_coord_ess.is_empty());
  check_ctx.check(statuses.gr_query_ess.is_empty());
  check_ctx.check(statuses.trans_table_read_ess.is_empty());
  check_ctx.check(statuses.tm_statuss.is_empty());
}
