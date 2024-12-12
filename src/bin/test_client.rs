use std::io::{Read, Write};
use std::net::TcpStream;

/// Send requests to the project running locally, for testing purposes
fn main() {
    fn make_request() {
        let first_request = [0x00u8, 0x00, 0x00, 0x23, 0x00, 0x12, 0x00, 0x04, 0x5d, 0xa3,
            0x2c, 0x35, 0x00, 0x09, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, 0x00,
            0x0a, 0x6b, 0x61, 0x66, 0x6b, 0x61, 0x2d, 0x63, 0x6c, 0x69, 0x04, 0x30, 0x2e, 0x31, 0x00];

        println!("Sending Request");
        let mut stream = TcpStream::connect("127.0.0.1:9092").unwrap();
        stream.write_all(&first_request).unwrap();
        println!("Sent Request");

        println!("Reading Response");
        let mut response_bytes = [0; 100];
        stream.read(&mut response_bytes).unwrap();
        println!("Read response: {:?}", response_bytes);
    }

    println!("First Request");
    make_request();

    println!("Second Request");
    make_request();
}