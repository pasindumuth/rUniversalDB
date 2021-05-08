mod simulation;

use crate::simulation::{client_id, slave_id, Simulation};
use runiversal::model::common::{EndpointId, RequestId, TabletGroupId};
use runiversal::model::message::{ExternalMessage, NetworkMessage};
use runiversal::model::message::{PerformExternalQuery, SlaveMessage};
use std::collections::HashMap;

fn main() {
  // Fundamental seed used for all random number generation,
  // providing determinism.
  let mut seed = [0; 16];
  for i in 0..16 {
    seed[i] = i as u8;
  }

  let mut tablet_config = HashMap::<EndpointId, Vec<TabletGroupId>>::new();
  tablet_config.insert(EndpointId("s1".to_string()), vec![]);
  tablet_config.insert(EndpointId("s2".to_string()), vec![]);
  tablet_config.insert(EndpointId("s3".to_string()), vec![]);
  tablet_config.insert(EndpointId("s4".to_string()), vec![]);
  tablet_config.insert(EndpointId("s5".to_string()), vec![]);
  let mut sim = Simulation::new(seed, tablet_config, 5);

  let query = "\
    SELECT a, b, 123, myfunc(b) \
    FROM table_1 \
    WHERE a > b AND b < 100 \
    ORDER BY a DESC, b;
\
    SELECT a, b, 123, myfunc(b) \
    FROM table_1 \
    WHERE a > b AND b < 100 \
    ORDER BY a DESC, b";

  sim.add_msg(
    NetworkMessage::Slave(SlaveMessage::PerformExternalQuery(PerformExternalQuery {
      request_id: RequestId("rid".to_string()),
      query: query.to_string(),
    })),
    &client_id(&2),
    &slave_id(&2),
  );

  sim.simulate_all();
}
