use rmp_serde;
use runiversal::net::network::{send, recv};
use runiversal::model::message::Message::Basic;
use std::collections::LinkedList;
use std::env;
use std::net::TcpStream;
use std::io::stdin;
use runiversal::model::message::Message;

fn main() {
    let mut args: LinkedList<String> = env::args().collect();
    args.pop_front(); // Remove the program name arg.
    let ip = args.pop_front().unwrap();
    let port = args.pop_front().unwrap().parse::<i32>().unwrap();

    // Connect to the server
    let stream = TcpStream::connect(format!("{}:{}", ip, port)).unwrap();

    loop {
        // Read in the message to send
        let mut val = String::new();
        stdin().read_line(&mut val).unwrap();
        let msg = Basic(val);

        // Send the message
        let buf = rmp_serde::to_vec(&msg).unwrap();
        send(buf.as_slice(), &stream);

        // Receive a message
        let buf = recv(&stream);
        let msg: Message = rmp_serde::from_read_ref(&buf).unwrap();
        println!("{:?}", msg);
    }
}
