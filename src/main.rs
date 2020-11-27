#![allow(non_snake_case)]

use std::env;
use std::thread;
use std::sync::{mpsc, Mutex, Arc};
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::str::from_utf8;
use std::collections::HashMap;
use std::sync::mpsc::Sender;

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

const SERVER_PORT: u32 = 1610;
const IPS: [&str;5] = [
    "172.19.0.3",
    "172.19.0.4",
    "172.19.0.5",
    "172.19.0.6",
    "172.19.0.7"
];

fn handle_conn(
    conn_map: &Arc<Mutex<HashMap<String, Sender<String>>>>,
    sender: &Sender<String>,
    mut receiving_stream: TcpStream
){
    let mut sending_stream = receiving_stream.try_clone().unwrap();
    // Setup Receiving Thread
    let sender = sender.clone();
    thread::spawn(move || {
        loop {
            let mut buf = [0; 128]; // We use 128 byte headers
            receiving_stream.read(&mut buf).unwrap();
            let str_in = from_utf8(&buf).unwrap();
            sender.send(String::from(str_in)).unwrap();
            println!("Received string: {}", str_in);
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
            let out_msg: String = receiver.recv().unwrap();
            sending_stream.write(out_msg.as_bytes()).unwrap();
        }
    });
}

fn handle_self_conn(
    endpoint_id: &str,
    conn_map: &Arc<Mutex<HashMap<String, Sender<String>>>>,
    server_thread_sender: &Sender<String>
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
            let msg: String = receiver.recv().unwrap();
            server_thread_sender.send(msg).unwrap();
        }
    });
}

fn main() {
    let mut args: Vec<String> = env::args().collect();
    let index = args.pop().expect(
        "An integer should be present to indicate which server this is.");
    // The mpsc channel for sending data to the Server Thread
    let (sender, receiver) = mpsc::channel();
    // The map mapping the IP addresses to a mpsc Sender object, used to
    // communicate with the Sender Threads to send data out.
    let conn_map = Arc::new(Mutex::new(HashMap::new()));

    // Start the Accepting Thread
    let sender_acceptor = sender.clone();
    let conn_map_acceptor = conn_map.clone();
    thread::spawn(move || {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", SERVER_PORT)).unwrap();
        for stream in listener.incoming() {
            handle_conn(&conn_map_acceptor, &sender_acceptor, stream.unwrap());
        }
    });

    // Connect to other IPs
    for ip in args {
        let stream = TcpStream::connect(format!("{}:{}", ip, SERVER_PORT));
        handle_conn(&conn_map, &sender, stream.unwrap());
    }

    // Handle self-connection
    let index = index.parse::<usize>().unwrap();
    handle_self_conn(IPS[index], &conn_map, &sender);

    // Start Server Thread
    println!("Index: {}", index);
    loop {
        let msg: String = receiver.recv().unwrap();
        println!("Recieved message: {}", msg);
    }
}
