pub struct KafkaResponse {
    message_size: i32,
    correlation_id: i32
}

impl KafkaResponse {
    pub fn new() -> KafkaResponse {
        KafkaResponse {
            message_size: 0,
            correlation_id: 7
        }
    }

    /// Convert the message to bytes that can be returned in the response
    pub fn to_bytes(&self) -> impl Iterator<Item=u8> {
        self.message_size
            .to_be_bytes()
            .into_iter()
            .chain(self.correlation_id.to_be_bytes())
    }
}