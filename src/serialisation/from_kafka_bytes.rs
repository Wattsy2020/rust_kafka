use tokio::io::{AsyncRead, AsyncReadExt};
use crate::api::request::KafkaRequestParseError;
use crate::api::request::KafkaRequestParseError::MissingData;

/// Trait that supports reading a type from the kafka protocol bytes that represent it
pub trait ReadKafkaBytes: Sized {
    #[allow(async_fn_in_trait)]
    async fn read_kafka_bytes<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError>;
}

impl ReadKafkaBytes for i16 {
    async fn read_kafka_bytes<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError> {
        reader.read_i16()
            .await
            .map_err(|_| MissingData(size_of::<i16>()))
    }
}

impl ReadKafkaBytes for i32 {
    async fn read_kafka_bytes<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError> {
        reader.read_i32()
            .await
            .map_err(|_| MissingData(size_of::<i32>()))
    }
}