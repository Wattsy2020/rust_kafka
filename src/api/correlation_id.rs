use tokio::io::AsyncRead;
use crate::api::request::KafkaRequestParseError;
use crate::serialisation::from_kafka_bytes::ReadKafkaBytes;
use crate::serialisation::ToKafkaBytes;

#[derive(Debug, Copy, Clone)]
pub struct CorrelationId(i32);

impl From<i32> for CorrelationId {
    fn from(value: i32) -> Self {
        CorrelationId(value)
    }
}

impl ReadKafkaBytes for CorrelationId {
    async fn read_kafka_bytes<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError> {
        i32::read_kafka_bytes(reader)
            .await
            .map(|int| int.into())
    }
}

impl ToKafkaBytes for CorrelationId {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item=u8> {
        self.0.to_kafka_bytes()
    }
}
