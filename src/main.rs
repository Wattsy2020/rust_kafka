use codecrafters_kafka::api::server::Server;

#[tokio::main]
async fn main() {
    let server = Server::new("127.0.0.1:9092").await.unwrap();
    println!("Server created, starting to serve...");
    server.serve().await;
}
