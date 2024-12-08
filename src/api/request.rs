use std::array::TryFromSliceError;
use thiserror::Error;

#[derive(Debug)]
pub struct KafkaRequest {
    message_size: i32,
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
}

impl KafkaRequest {
    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }
}

#[derive(Debug, Error)]
pub enum KafkaRequestParseError {
    #[error("Missing data, received insufficient bytes of length: {0}")]
    MissingData(usize),
    #[error("Failed to parse bytes")]
    ByteParseError(#[from] TryFromSliceError)
}

impl TryFrom<&[u8]> for KafkaRequest {
    type Error = KafkaRequestParseError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < 12 {
            return Err(KafkaRequestParseError::MissingData(value.len()));
        }

        let (int_bytes, rest) = value.split_at(size_of::<i32>());
        let message_size = i32::from_be_bytes(int_bytes.try_into()?);

        let (int_bytes, rest) = rest.split_at(size_of::<i16>());
        let request_api_key = i16::from_be_bytes(int_bytes.try_into()?);

        let (int_bytes, rest) = rest.split_at(size_of::<i16>());
        let request_api_version = i16::from_be_bytes(int_bytes.try_into()?);

        let (int_bytes, _) = rest.split_at(size_of::<i32>());
        let correlation_id = i32::from_be_bytes(int_bytes.try_into()?);
        
        Ok(KafkaRequest {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id
        })
    }
}