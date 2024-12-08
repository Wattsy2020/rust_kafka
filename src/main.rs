use std::io::{Read, Write};
use std::net::TcpListener;
use codecrafters_kafka::api::request::KafkaRequest;
use codecrafters_kafka::api::response::KafkaResponse;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // todo: move this into an api::Server module
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut request_bytes: Vec<u8> = Vec::new();
                stream.read_to_end(&mut request_bytes).unwrap();
                let request = KafkaRequest::try_from(request_bytes.as_slice()).unwrap();

                let response = KafkaResponse::new(request);
                let response_bytes: Vec<u8> = response.to_bytes().collect();
                stream.write_all(&response_bytes).unwrap();
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
