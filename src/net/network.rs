use std::net::TcpStream;
use std::io::{Read, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// We use simple 4 byte header that holds the
/// length of the real message.

pub fn send(data: &[u8], mut stream: &TcpStream) {
    // Write out the fixed-size header
    stream.write_u32::<BigEndian>(data.len() as u32).unwrap();
    // Write out the full message
    stream.write(data).unwrap();
}

pub fn recv(mut stream: &TcpStream) -> Vec<u8> {
    // Read in the fixed-size header
    let size = stream.read_u32::<BigEndian>().unwrap();
    // Read in the full message
    let mut buf = vec![0; size as usize];
    let mut i = 0;
    while i < size {
        let num_read = stream.read(&mut buf[i as usize..]).unwrap();
        i += num_read as u32;
    }
    return buf;
}
