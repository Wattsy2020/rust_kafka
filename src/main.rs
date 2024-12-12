use codecrafters_kafka::api::server::Server;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let server = Server::new("127.0.0.1:9092").unwrap();
    server.serve();
}
