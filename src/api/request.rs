use std::string::FromUtf8Error;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use crate::api::api_key::{ApiKey, ParseApiKeyError};
use crate::api::api_versions::ApiVersionsRequest;
use crate::api::correlation_id::CorrelationId;
use crate::api::request::KafkaRequestParseError::MissingData;
use crate::serialisation::ReadKafkaBytes;
use crate::serialisation::nullable_string::NullableString;

#[derive(Debug)]
pub struct KafkaRequest {
    message_size: i32,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: CorrelationId,
    client_id: NullableString,
    api_request: ApiRequest
}

#[derive(Debug)]
pub enum ApiRequest {
    ApiVersions(ApiVersionsRequest)
}

impl KafkaRequest {
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    pub fn correlation_id(&self) -> CorrelationId { self.correlation_id }

    pub async fn try_read_from<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError> {
        let message_size = i32::read_kafka_bytes(reader).await?;
        let api_key = ApiKey::read_kafka_bytes(reader).await?;
        let api_version = i16::read_kafka_bytes(reader).await?;
        let correlation_id = CorrelationId::read_kafka_bytes(reader).await?;
        let client_id = NullableString::read_kafka_bytes(reader).await?;
        reader.read_u8().await.map_err(|_| MissingData(1))?; // ignore the tag buffer for now

        let api_request = match api_key {
            ApiKey::DescribeTopicPartitions => { todo!("parse things")},
            _ => ApiRequest::ApiVersions(ApiVersionsRequest{})
        };

        Ok(KafkaRequest {
            message_size,
            api_key,
            api_version,
            correlation_id,
            client_id,
            api_request
        })
    }
}

#[derive(Debug, Error)]
pub enum KafkaRequestParseError {
    #[error("Missing data, received insufficient bytes, fewer than: {0}")]
    MissingData(usize),
    #[error("Invalid Api Key requested: {0}")]
    InvalidApiKey(#[from] ParseApiKeyError),
    #[error("Invalid String Length: {0}")]
    InvalidStringLength(i32),
    #[error("Invalid String: {0}")]
    InvalidString(#[from] FromUtf8Error)
}
