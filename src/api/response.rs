use super::request::KafkaRequest;

/// Represents a response to a Kafka API Request
pub trait KafkaResponse {
    /// Convert the message to bytes that can be returned in the response
    fn to_bytes(&self) -> impl Iterator<Item=u8>;

    /// Converts to bytes and adds the message size
    fn to_response_message(&self) -> impl Iterator<Item=u8> {
        let bytes: Vec<u8> = self.to_bytes().collect();
        let size = (bytes.len() + 4) as i32; // include the size of the size itself
        size
            .to_be_bytes()
            .into_iter()
            .chain(bytes)
    }
}

#[derive(Debug)]
pub struct BaseKafkaResponse {
    correlation_id: i32
}

impl BaseKafkaResponse {
    pub fn new(request: &KafkaRequest) -> BaseKafkaResponse {
        BaseKafkaResponse {
            correlation_id: request.correlation_id()
        }
    }
}

impl KafkaResponse for BaseKafkaResponse {
    /// Convert the message to bytes that can be returned in the response
    fn to_bytes(&self) -> impl Iterator<Item=u8> {
        self.correlation_id.to_be_bytes().into_iter()
    }
}
