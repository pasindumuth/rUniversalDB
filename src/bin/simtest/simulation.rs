use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::model::common::{EndpointId, TabletShape};
use runiversal::model::message::{SlaveActions, SlaveMessage};
use runiversal::slave::slave::ServerState;
use runiversal::tablet::tablet::TabletState;
use std::collections::{HashMap, HashSet, VecDeque};

struct SimState {}

#[derive(Debug)]
pub struct Simulation {
    rand: XorShiftRng,
    slave_eids: Vec<EndpointId>,
    client_eids: Vec<EndpointId>,
    /// Message queues between nodes. This field contains contains 2 queues (in for
    /// each direction) for every pair of client EndpointIds and slave Endpoints.
    queues: VecDeque<VecDeque<SlaveMessage>>,
    /// We use pairs of endpoints as identifiers of a queue.
    /// This field contain all queue IDs where the queue is non-empty
    nonempty_queues: VecDeque<(i32, i32)>,
    slave_states: HashMap<EndpointId, ServerState>,
    tablet_states: HashMap<EndpointId, HashMap<TabletShape, TabletState>>,
    /// The following are everything related to asynchronous computation
    /// done at a node.
    slave_async_queue: HashMap<EndpointId, VecDeque<SlaveActions>>,
    tablet_async_queue: HashMap<EndpointId, HashMap<TabletShape, VecDeque<SlaveActions>>>,
    clocks: HashMap<EndpointId, i64>,
    /// Meta
    next_int: i32,
    true_timestamp: i64,
    // Accumulated client responses
    client_msgs_received: HashSet<SlaveMessage>,
}

impl Simulation {
    pub fn new(seed: [u8; 16]) -> Simulation {
        // let slave_eids = [0..num_slaves].iter.map(|i| fmt!("s{}", i));
        Simulation {
            rand: XorShiftRng::from_seed(seed),
            slave_eids: vec![],
            client_eids: vec![],
            queues: Default::default(),
            nonempty_queues: Default::default(),
            slave_states: Default::default(),
            tablet_states: Default::default(),
            slave_async_queue: Default::default(),
            tablet_async_queue: Default::default(),
            clocks: Default::default(),
            next_int: 0,
            true_timestamp: 0,
            client_msgs_received: Default::default(),
        }
    }
}
