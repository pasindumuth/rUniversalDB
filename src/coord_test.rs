use crate::coord::CoordState;

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
  let ctx = &coord.coord_context;

  // First, we make sure all RequestIds in the ESs exist in the `external_request_id_map`.
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
}
