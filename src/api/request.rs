use std::string::FromUtf8Error;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use crate::api::api_key::{ApiKey, ParseApiKeyError};
use crate::api::correlation_id::CorrelationId;
use crate::api::request::KafkaRequestParseError::MissingData;
use crate::serialisation::from_kafka_bytes::ReadKafkaBytes;
use crate::serialisation::nullable_string::NullableString;

#[derive(Debug)]
pub struct KafkaRequest {
    message_size: i32,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: CorrelationId,
    client_id: NullableString
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

        // eventually should read the body, for now ignore the remaining body
        let mut body_bytes = [0; 1000];
        let num_bytes_read = reader.read(&mut body_bytes)
            .await
            .map_err(|_| MissingData(message_size as usize))?;
        println!("Read body of {} bytes", num_bytes_read);

        Ok(KafkaRequest {
            message_size,
            api_key,
            api_version,
            correlation_id,
            client_id
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
