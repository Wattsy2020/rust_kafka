use crate::serialisation::ToKafkaBytes;
use std::io::{BufReader, Read};
use thiserror::Error;
use crate::api::request::KafkaRequestParseError::MissingData;

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

    pub fn try_from<T: Read>(buf_reader: &mut BufReader<T>) -> Result<Self, KafkaRequestParseError> {
        fn read_i32<T: Read>(buf_reader: &mut BufReader<T>) -> Result<i32, KafkaRequestParseError> {
            let mut bytes = [0; size_of::<i32>()];
            buf_reader.read(&mut bytes).map_err(|_| MissingData(size_of::<i32>()))?;
            Ok(i32::from_be_bytes(bytes))
        }

        fn read_i16<T: Read>(buf_reader: &mut BufReader<T>) -> Result<i16, KafkaRequestParseError> {
            let mut bytes = [0; size_of::<i16>()];
            buf_reader.read(&mut bytes).map_err(|_| MissingData(size_of::<i16>()))?;
            Ok(i16::from_be_bytes(bytes))
        }

        Ok(KafkaRequest {
            message_size: read_i32(buf_reader)?,
            api_key: read_i16(buf_reader)?.try_into()?,
            api_version: read_i16(buf_reader)?,
            correlation_id: read_i32(buf_reader)?,
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
