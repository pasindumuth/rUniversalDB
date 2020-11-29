use rmp_serde;
use runiversal::model::message::SlaveMessage;
use runiversal::model::message::SlaveMessage::Client;
use runiversal::net::network::{recv, send};
use std::collections::LinkedList;
use std::env;
use std::io::stdin;
use std::net::TcpStream;

fn main() {
    let mut args: LinkedList<String> = env::args().collect();
    args.pop_front(); // Remove the program name arg.
    let ip = args.pop_front().unwrap();
    let port = args.pop_front().unwrap().parse::<i32>().unwrap();

    // Connect to the slave
    let stream = TcpStream::connect(format!("{}:{}", ip, port)).unwrap();

    loop {
        // Read in the message to send
        let mut val = String::new();
        stdin().read_line(&mut val).unwrap();
        let msg = Client { msg: val };

        // Send the message
        let buf = rmp_serde::to_vec(&msg).unwrap();
        send(buf.as_slice(), &stream);

        // Receive a message
        let buf = recv(&stream);
        let msg: SlaveMessage = rmp_serde::from_read_ref(&buf).unwrap();
        println!("{:?}", msg);
    }
}
