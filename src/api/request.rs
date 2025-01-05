use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use crate::api::api_key::{ApiKey, ParseApiKeyError};
use crate::api::correlation_id::CorrelationId;
use crate::api::request::KafkaRequestParseError::MissingData;

#[derive(Debug)]
pub struct KafkaRequest {
    message_size: i32,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: CorrelationId,
}

impl KafkaRequest {
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    pub fn correlation_id(&self) -> CorrelationId { self.correlation_id }

    pub async fn try_read_from<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError> {
        let message_size = reader.read_i32()
            .await
            .map_err(|_| MissingData(size_of::<i32>()))?;
        let api_key = reader.read_i16()
            .await
            .map_err(|_| MissingData(size_of::<i16>()))?
            .try_into()?;
        let api_version = reader.read_i16()
            .await
            .map_err(|_| MissingData(size_of::<i16>()))?;
        let correlation_id = reader.read_i32()
            .await
            .map_err(|_| MissingData(size_of::<i32>()))?
            .into();

        // eventually should read the body, for now ignore the remaining body
        // note `message_size` doesn't include itself, does include the 8 bytes of api_key, version, and correlation_id
        let expected_num_bytes = message_size as usize - 8;
        let mut body_bytes = vec![0; expected_num_bytes];
        reader.read_exact(&mut body_bytes)
            .await
            .map_err(|_| MissingData(expected_num_bytes))?;
        println!("Read body of {} bytes", body_bytes.len());

        Ok(KafkaRequest {
            message_size,
            api_key,
            api_version,
            correlation_id,
        })
    }
}

#[derive(Debug, Error)]
pub enum KafkaRequestParseError {
    #[error("Missing data, received insufficient bytes, fewer than: {0}")]
    MissingData(usize),
    #[error("Invalid Api Key requested: {0}")]
    InvalidApiKey(#[from] ParseApiKeyError),
}
