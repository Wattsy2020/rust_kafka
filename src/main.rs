#![allow(unused_imports)]

use std::io::Write;
use std::net::TcpListener;
use codecrafters_kafka::api::KafkaResponse;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let response = KafkaResponse::new();
                let response_bytes: Vec<u8> = response.to_bytes().collect();
                stream
                    .write_all(&response_bytes)
                    .expect("returning a response shouldn't fail");
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
