use std::env;
use std::thread;
use std::sync::{mpsc, Mutex, Arc};
use std::net::{TcpListener, TcpStream};
use std::collections::{HashMap, LinkedList};
use std::sync::mpsc::Sender;
use runiversal::net::network::{recv, send};
use runiversal::model::message::Message;

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
    conn_map: &Arc<Mutex<HashMap<String, Sender<Vec<u8>>>>>,
    sender: &Sender<Vec<u8>>,
    receiving_stream: TcpStream
){
    let sending_stream = receiving_stream.try_clone().unwrap();
    // Setup Receiving Thread
    let sender = sender.clone();
    thread::spawn(move || {
        loop {
            let val_in = recv(&receiving_stream);
            sender.send(val_in).unwrap();
        }
    });

    let endpoint_id = sending_stream.peer_addr().unwrap().ip().to_string();
    // Used like a Single-Producer-Single-Consumer queue, where Server Thread
    // is the producer, the Sending Thread is the consumer.
    let (sender, receiver) = mpsc::channel();
    // Add sender of the SPSC to the conn_map so the Server Thread can access it.
    let mut conn_map = conn_map.lock().unwrap();
    conn_map.insert(endpoint_id, sender);

    // Setup Sending Thread
    thread::spawn(move || {
        loop {
            let data_out = receiver.recv().unwrap();
            send(&data_out, &sending_stream);
        }
    });
}

fn handle_self_conn(
    endpoint_id: &str,
    conn_map: &Arc<Mutex<HashMap<String, Sender<Vec<u8>>>>>,
    server_thread_sender: &Sender<Vec<u8>>
){
    // Used like a Single-Producer-Single-Consumer queue, where Server Thread
    // is the producer, the Sending Thread is the consumer.
    let (sender, receiver) = mpsc::channel();
    // Add sender of the SPSC to the conn_map so the Server Thread can access it.
    let mut conn_map = conn_map.lock().unwrap();
    conn_map.insert(String::from(endpoint_id), sender);

    // Setup Sending Thread
    let server_thread_sender = server_thread_sender.clone();
    thread::spawn(move || {
        loop {
            let data = receiver.recv().unwrap();
            server_thread_sender.send(data).unwrap();
        }
    });
}

fn main() {
    let mut args: LinkedList<String> = env::args().collect();
    args.pop_front(); // Removes the program name argument.
    let _seed = args.pop_front().expect( // This remove the seed for now.
        "A random seed should be provided.");
    let cur_ip = args.pop_front().expect(
        "The endpoint_id of the current server should be provided.");
    // The mpsc channel for sending data to the Server Thread
    let (sender, receiver) = mpsc::channel();
    // The map mapping the IP addresses to a mpsc Sender object, used to
    // communicate with the Sender Threads to send data out.
    let conn_map = Arc::new(Mutex::new(HashMap::new()));

    // Start the Accepting Thread
    {
        let sender = sender.clone();
        let conn_map = conn_map.clone();
        let cur_ip = cur_ip.clone();
        thread::spawn(move || {
            let listener = TcpListener::bind(format!("{}:{}", cur_ip, SERVER_PORT)).unwrap();
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                println!("Connected from: {}", stream.peer_addr().unwrap().ip().to_string());
                handle_conn(&conn_map, &sender, stream);
            }
        });
    }

    // Connect to other IPs
    for ip in args {
        let stream = TcpStream::connect(format!("{}:{}", ip, SERVER_PORT));
        handle_conn(&conn_map, &sender, stream.unwrap());
        println!("Connected to: {}", ip);
    }

    // Handle self-connection
    handle_self_conn(&cur_ip, &conn_map, &sender);

    // Start Server Thread
    println!("Starting Server {}", cur_ip);
    loop {
        let data = receiver.recv().unwrap();
        let msg: Message = rmp_serde::from_read_ref(&data).unwrap();
        println!("Recieved message: {:?}", msg);
    }
}
