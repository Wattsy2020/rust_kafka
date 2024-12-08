use std::io::{Read, Write};
use std::net::TcpListener;
use codecrafters_kafka::api::api_versions::ApiVersionsResponse;
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
                println!("Received new request");

                let mut request_bytes = [0; 36];
                stream.read(&mut request_bytes).unwrap();
                let request = KafkaRequest::try_from(request_bytes.as_slice()).unwrap();
                println!("Received Request: {request:?}");

                // assume request is an ApiVersions request
                let response = ApiVersionsResponse::process_request(&request);
                println!("Sending Response: {response:?}");
                let response_bytes: Vec<u8> = response.to_response_message().collect();
                stream.write_all(&response_bytes).unwrap();
                println!("Sent response bytes: {response_bytes:?}");

                // read all remaining bytes from the client,
                // so that it can finish writing and start reading our response
                let mut ignored_bytes = Vec::new();
                stream.read_to_end(&mut ignored_bytes).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
