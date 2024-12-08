use crate::serialisation::ToKafkaBytes;
use super::request::KafkaRequest;

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

impl ToKafkaBytes for BaseKafkaResponse {
    /// Convert the message to bytes that can be returned in the response
    fn to_kafka_bytes(self) -> impl IntoIterator<Item=u8> {
        self.correlation_id.to_kafka_bytes()
    }
}
