use crate::serialisation::ToKafkaBytes;
use std::array::TryFromSliceError;
use thiserror::Error;

#[derive(Debug)]
pub struct KafkaRequest {
    message_size: i32,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
}

impl KafkaRequest {
    pub fn api_version(&self) -> i16 {
        self.api_version
    }

    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }
}

#[derive(Debug, Error)]
pub enum KafkaRequestParseError {
    #[error("Missing data, received insufficient bytes of length: {0}")]
    MissingData(usize),
    #[error("Failed to parse bytes")]
    ByteParseError(#[from] TryFromSliceError),
    #[error("Invalid Api Key requested: {0}")]
    InvalidApiKey(#[from] ParseApiKeyError),
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
        let api_key = i16::from_be_bytes(int_bytes.try_into()?).try_into()?;

        let (int_bytes, rest) = rest.split_at(size_of::<i16>());
        let api_version = i16::from_be_bytes(int_bytes.try_into()?);

        let (int_bytes, _) = rest.split_at(size_of::<i32>());
        let correlation_id = i32::from_be_bytes(int_bytes.try_into()?);

        Ok(KafkaRequest {
            message_size,
            api_key,
            api_version,
            correlation_id,
        })
    }
}

#[derive(Debug)]
pub enum ApiKey {
    Produce,
    Fetch,
    ApiVersions,
}

#[derive(Error, Debug)]
pub enum ParseApiKeyError {
    #[error("Invalid Api Key: {0}")]
    InvalidKey(i16),
}

impl ToKafkaBytes for ApiKey {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8> {
        let int_repr: i16 = match self {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::ApiVersions => 18,
        };
        int_repr.to_kafka_bytes()
    }
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
