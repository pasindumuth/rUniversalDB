use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::common::rand::RandGen;
use runiversal::common::test_config::{endpoint, table_shape};
use runiversal::model::common::{EndpointId, TabletKeyRange, TabletPath, TabletShape};
use runiversal::net::network::{recv, send};
use runiversal::slave::thread::start_slave_thread;
use runiversal::tablet::thread::start_tablet_thread;
use std::collections::{HashMap, LinkedList};
use std::env;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// The threading architecture we use is as follows. Every network
/// connection has 2 threads, one for receiving data, called the
/// Receiving Thread (which most of its time blocking on reading
/// the socket), and one thread for sending data, called the Sending
/// Thread (which spends most of it's time blocking on an Output
/// Queue, which we create every time a new socket is created). Once a
/// Receiving Thread receives a packet, it puts it into a Multi-Producer-
/// Single-Consumer Queue. Here, each Receiving Thread is a Producer, and
/// the only Consumer is the Server Thread. We call this Queue the
/// "Event Queue". Once the Server Thread wants to send a packet out of a
/// socket, it places it in the socket's Output Queue. The Sending Thread
/// picks this up and sends it out of the socket. The Output Queue is a
/// Single-Producer-Single-Consumer queue.
///
/// For the Server Thread also needs to connect to itself. We don't use
/// a network socket for this, and we don't have two auxiliary threads.
/// Instead, we have one auxiliary thread, called the Self Connection
/// Thread, which takes packets that are sent out of Server Thread and
/// immediately feeds it back in.

const SERVER_PORT: u32 = 1610;

fn handle_conn(
    net_conn_map: &Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
    slave_sender: &Sender<(EndpointId, Vec<u8>)>,
    stream: TcpStream,
) -> EndpointId {
    let endpoint_id = EndpointId(stream.peer_addr().unwrap().ip().to_string());

    // Setup Receiving Thread
    {
        let slave_sender = slave_sender.clone();
        let endpoint_id = endpoint_id.clone();
        let stream = stream.try_clone().unwrap();
        thread::spawn(move || loop {
            let val_in = recv(&stream);
            slave_sender.send((endpoint_id.clone(), val_in)).unwrap();
        });
    }

    // Used like a Single-Producer-Single-Consumer queue, where Server Thread
    // is the producer, the Sending Thread is the consumer.
    let (slave_sender, receiver) = mpsc::channel();
    // Add slave_sender of the SPSC to the net_conn_map so the Server Thread can access it.
    let mut net_conn_map = net_conn_map.lock().unwrap();
    net_conn_map.insert(endpoint_id.clone(), slave_sender);

    // Setup Sending Thread
    thread::spawn(move || loop {
        let data_out = receiver.recv().unwrap();
        send(&data_out, &stream);
    });

    return endpoint_id;
}

fn handle_self_conn(
    endpoint_id: &EndpointId,
    net_conn_map: &Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
    slave_sender: &Sender<(EndpointId, Vec<u8>)>,
) {
    // Used like a Single-Producer-Single-Consumer queue, where Server Thread
    // is the producer, the Sending Thread is the consumer.
    let (sender, receiver) = mpsc::channel();
    // Add sender of the SPSC to the net_conn_map so the Server Thread can access it.
    let mut net_conn_map = net_conn_map.lock().unwrap();
    net_conn_map.insert(endpoint_id.clone(), sender);

    // Setup Sending Thread
    let slave_sender = slave_sender.clone();
    let endpoint_id = endpoint_id.clone();
    thread::spawn(move || loop {
        let data = receiver.recv().unwrap();
        slave_sender.send((endpoint_id.clone(), data)).unwrap();
    });
}

fn main() {
    let mut args: LinkedList<String> = env::args().collect();

    // Removes the program name argument.
    args.pop_front();
    // This remove the seed for now.
    let slave_index = args
        .pop_front()
        .expect("A slave index should be provided.")
        .parse::<u32>()
        .expect("The slave index couldn't be parsed as a string.");
    let cur_ip = args
        .pop_front()
        .expect("The endpoint_id of the current slave should be provided.");

    // The mpsc channel for sending data to the Server Thread
    let (slave_sender, slave_receiver) = mpsc::channel();
    // The map mapping the IP addresses to a mpsc Sender object, used to
    // communicate with the Sender Threads to send data out.
    let net_conn_map = Arc::new(Mutex::new(HashMap::new()));

    // Start the Accepting Thread
    {
        let sender = slave_sender.clone();
        let net_conn_map = net_conn_map.clone();
        let cur_ip = cur_ip.clone();
        thread::spawn(move || {
            let listener = TcpListener::bind(format!("{}:{}", &cur_ip, SERVER_PORT)).unwrap();
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let endpoint_id = handle_conn(&net_conn_map, &sender, stream);
                println!("Connected from: {:?}", endpoint_id);
            }
        });
    }

    // Connect to other IPs
    for ip in args {
        let stream = TcpStream::connect(format!("{}:{}", ip, SERVER_PORT));
        let endpoint_id = handle_conn(&net_conn_map, &slave_sender, stream.unwrap());
        println!("Connected to: {:?}", endpoint_id);
    }

    // Handle self-connection
    let endpoint_id = EndpointId(cur_ip);
    handle_self_conn(&endpoint_id, &net_conn_map, &slave_sender);

    // A pre-defined map of what tablets that each slave should be managing.
    // For now, we create all tablets for the current Slave during boot-time.
    let mut key_space_config = HashMap::new();
    key_space_config.insert(
        endpoint("172.19.0.3"),
        vec![table_shape("table1", None, None)],
    );
    key_space_config.insert(
        endpoint("172.19.0.4"),
        vec![table_shape("table2", None, Some("j"))],
    );
    key_space_config.insert(
        endpoint("172.19.0.5"),
        vec![
            table_shape("table2", Some("j"), None),
            table_shape("table3", None, Some("d")),
            table_shape("table4", None, Some("k")),
        ],
    );
    key_space_config.insert(
        endpoint("172.19.0.6"),
        vec![table_shape("table3", Some("d"), Some("p"))],
    );
    key_space_config.insert(
        endpoint("172.19.0.7"),
        vec![
            table_shape("table3", Some("p"), None),
            table_shape("table4", Some("k"), None),
        ],
    );
    let key_space_config = key_space_config;

    // Create the seed that this Slave uses for random number generation.
    // It's 16 bytes long, so we do (16 * slave_index + i) to make sure
    // every element of the seed is different across all slaves.
    let mut seed = [0; 16];
    for i in 0..16 {
        seed[i] = (16 * slave_index + i as u32) as u8;
    }
    // Create Slave RNG.
    let mut rng = Box::new(XorShiftRng::from_seed(seed));

    // Setup the Tablet.
    let mut tablet_map = HashMap::new();

    for tablet_shape in key_space_config.get(&endpoint_id).unwrap() {
        // Create the seed for the Tablet's RNG. We use the Slave's
        // RNG to create a random seed.
        let mut seed = [0; 16];
        rng.fill_bytes(&mut seed);
        // Create Tablet RNG.
        let rng = Box::new(XorShiftRng::from_seed(seed));

        // Create mpsc queue for Slave-Tablet communication.
        let (tablet_sender, tablet_receiver) = mpsc::channel();
        tablet_map.insert(tablet_shape.clone(), tablet_sender);

        // Start the Tablet Thread
        let net_conn_map = net_conn_map.clone();
        thread::spawn(move || {
            start_tablet_thread(RandGen { rng }, tablet_receiver, net_conn_map);
        });
    }

    start_slave_thread(
        endpoint_id,
        RandGen { rng },
        slave_receiver,
        net_conn_map,
        tablet_map,
    );
}
