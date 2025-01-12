use thiserror::Error;
use tokio::io::AsyncRead;
use crate::api::request::KafkaRequestParseError;
use crate::serialisation::from_kafka_bytes::ReadKafkaBytes;
use crate::serialisation::ToKafkaBytes;

#[derive(Debug)]
pub enum ApiKey {
    Produce,
    Fetch,
    ApiVersions,
    DescribeTopicPartitions
}

#[derive(Error, Debug)]
pub enum ParseApiKeyError {
    #[error("Invalid Api Key: {0}")]
    InvalidKey(i16),
}

impl TryFrom<i16> for ApiKey {
    type Error = ParseApiKeyError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ApiKey::Produce),
            1 => Ok(ApiKey::Fetch),
            18 => Ok(ApiKey::ApiVersions),
            _ => Err(ParseApiKeyError::InvalidKey(value)),
        }
    }
}

impl ReadKafkaBytes for ApiKey {
    async fn read_kafka_bytes<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError> {
        i16::read_kafka_bytes(reader)
            .await
            .and_then(|int| Ok(int.try_into()?))
    }
}

impl ToKafkaBytes for ApiKey {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8> {
        let int_repr: i16 = match self {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::ApiVersions => 18,
            ApiKey::DescribeTopicPartitions => 75
        };
        int_repr.to_kafka_bytes()
    }
}
