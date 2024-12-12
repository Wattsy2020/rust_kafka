use std::io;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use crate::api::api_versions::ApiVersionsResponse;
use crate::api::request::KafkaRequest;
use crate::serialisation::to_response_message;

pub struct Server {
    listener: TcpListener
}

impl Server {
    pub fn new(address: &str) -> io::Result<Server> {
        TcpListener::bind(address)
            .map(|listener| Server { listener })
    }

    /// Serve incoming Kafka Protocol Requests
    pub fn serve(&self) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("Received new request");
                    self.handle_connection(stream);
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }

    /// Read a KafkaRequest and send response
    /// until the Kafka Request from the connection is invalid / missing
    fn handle_connection(&self, stream: TcpStream) {
        let mut buf_reader = BufReader::new(&stream);
        let mut buf_writer = BufWriter::new(&stream);

        loop {
            match KafkaRequest::try_from(&mut buf_reader) {
                Ok(request) => {
                    println!("Received Request: {request:?}");

                    // assume request is an ApiVersions request
                    let response = ApiVersionsResponse::process_request(&request);
                    println!("Sending Response: {response:?}");
                    let response_bytes: Box<[u8]> = to_response_message(response).collect();
                    buf_writer.write_all(&response_bytes).unwrap();
                    println!("Sent response bytes: {response_bytes:?}");
                }
                Err(err) => {
                    eprintln!("Received incorrect request: {err}");
                    return;
                }
            }
        }
    }
}