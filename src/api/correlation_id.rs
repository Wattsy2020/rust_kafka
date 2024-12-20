use crate::serialisation::ToKafkaBytes;

#[derive(Debug, Copy, Clone)]
pub struct CorrelationId(i32);

impl ToKafkaBytes for CorrelationId {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item=u8> {
        self.0.to_kafka_bytes()
    }
}

impl From<i32> for CorrelationId {
    fn from(value: i32) -> Self {
        CorrelationId(value)
    }
}
