use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::rand::RandGen;
use runiversal::model::common::{EndpointId, TabletShape};
use runiversal::model::message::{SlaveActions, SlaveMessage};
use runiversal::slave::slave::SlaveState;
use runiversal::tablet::tablet::TabletState;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug)]
pub struct Simulation {
    rng: XorShiftRng,
    slave_eids: Vec<EndpointId>,
    client_eids: Vec<EndpointId>,
    /// Message queues between nodes. This field contains contains 2 queues (in for
    /// each direction) for every pair of client EndpointIds and slave Endpoints.
    queues: Vec<Vec<VecDeque<SlaveMessage>>>,
    /// We use pairs of endpoints as identifiers of a queue.
    /// This field contain all queue IDs where the queue is non-empty
    nonempty_queues: VecDeque<(i32, i32)>,
    slave_states: HashMap<EndpointId, SlaveState>,
    tablet_states: HashMap<EndpointId, HashMap<TabletShape, TabletState>>,
    /// The following are everything related to asynchronous computation
    /// done at a node.
    slave_async_queue: HashMap<EndpointId, VecDeque<SlaveActions>>,
    tablet_async_queue: HashMap<EndpointId, HashMap<TabletShape, VecDeque<SlaveActions>>>,
    clocks: HashMap<EndpointId, i64>,
    /// Meta
    next_int: i32,
    true_timestamp: i64,
    /// Accumulated client responses
    client_msgs_received: HashSet<SlaveMessage>,
}

fn vec(i: i32, j: i32) -> Vec<i32> {
    (i..j).collect()
}

fn slave_id(i: &i32) -> EndpointId {
    EndpointId(format!("s{}", i))
}

fn client_id(i: &i32) -> EndpointId {
    EndpointId(format!("c{}", i))
}

impl Simulation {
    pub fn new(
        seed: [u8; 16],
        key_space_config: HashMap<EndpointId, Vec<TabletShape>>,
        num_clients: i32,
    ) -> Simulation {
        let mut rng = XorShiftRng::from_seed(seed);
        let num_slaves = key_space_config.len() as i32;
        let slave_eids: Vec<EndpointId> = key_space_config.keys().cloned().collect();
        let client_eids = vec(0, num_clients).iter().map(client_id).collect();
        let num_nodes = num_slaves + num_clients;
        let queues = vec(0, num_nodes)
            .iter()
            .map(|_| vec(0, num_nodes).iter().map(|_| VecDeque::new()).collect())
            .collect();
        let mut slave_states = HashMap::new();
        for eid in &slave_eids {
            let mut seed = [0; 16];
            rng.fill_bytes(&mut seed);
            // Create Tablet RNG.
            slave_states.insert(
                eid.clone(),
                SlaveState::new(RandGen {
                    rng: Box::new(XorShiftRng::from_seed(seed)),
                }),
            );
        }
        let mut tablet_states = HashMap::new();
        for eid in &slave_eids {
            for shape in &key_space_config[eid] {
                let mut slave_tablet_states = HashMap::new();
                let mut seed = [0; 16];
                rng.fill_bytes(&mut seed);
                // Create Tablet RNG.
                slave_tablet_states.insert(
                    shape.clone(),
                    TabletState::new(RandGen {
                        rng: Box::new(XorShiftRng::from_seed(seed)),
                    }),
                );
                tablet_states.insert(eid.clone(), slave_tablet_states);
            }
        }
        Simulation {
            rng,
            slave_eids,
            client_eids,
            queues,
            nonempty_queues: Default::default(),
            slave_states,
            tablet_states,
            slave_async_queue: Default::default(),
            tablet_async_queue: Default::default(),
            clocks: Default::default(),
            next_int: 0,
            true_timestamp: 0,
            client_msgs_received: Default::default(),
        }
    }
}
