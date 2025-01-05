use std::io;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use crate::api::api_versions::ApiVersionsResponse;
use crate::api::request::KafkaRequest;
use crate::serialisation::to_response_message;

pub struct Server {
    listener: TcpListener
}

impl Server {
    pub async fn new(address: &str) -> io::Result<Server> {
        TcpListener::bind(address)
            .await
            .map(|listener| Server { listener })
    }

    /// Serve incoming Kafka Protocol Requests
    pub async fn serve(&self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    println!("Received new request");
                    tokio::spawn(Server::handle_connection(stream));
                    // note this doesn't have graceful shutdown.
                    // the server could be shutdown, and in progress requests might not be handled
                }
                Err(e) => {
                    eprintln!("error: {}", e);
                }
            }
        }
    }

    /// Read a KafkaRequest and send response
    /// until the Kafka Request from the connection is invalid / missing
    async fn handle_connection(mut stream: TcpStream) {
        let (stream_read, mut stream_writer) = stream.split();
        let mut stream_reader = BufReader::new(stream_read);
        
        loop {
            println!("Waiting to parse request");
            let request = match KafkaRequest::try_read_from(&mut stream_reader).await {
                Ok(request) => {
                    println!("Received Request: {request:?}");
                    request
                }
                Err(err) => {
                    eprintln!("Received incorrect request: {err}");
                    return;
                }
            };

            let response = ApiVersionsResponse::process_request(&request);
            println!("Sending Response: {response:?}");
            let response_bytes: Box<[u8]> = to_response_message(response).collect();
            stream_writer.write_all(&response_bytes).await.unwrap();
            println!("Sent response bytes: {response_bytes:?}");
        }
    }
}