use std::io::{BufReader, Read};
use thiserror::Error;
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

    pub fn try_from<T: Read>(buf_reader: &mut BufReader<T>) -> Result<Self, KafkaRequestParseError> {
        fn read_i32<T: Read>(buf_reader: &mut BufReader<T>) -> Result<i32, KafkaRequestParseError> {
            let mut bytes = [0; size_of::<i32>()];
            buf_reader.read_exact(&mut bytes).map_err(|_| MissingData(size_of::<i32>()))?;
            Ok(i32::from_be_bytes(bytes))
        }

        fn read_i16<T: Read>(buf_reader: &mut BufReader<T>) -> Result<i16, KafkaRequestParseError> {
            let mut bytes = [0; size_of::<i16>()];
            buf_reader.read_exact(&mut bytes).map_err(|_| MissingData(size_of::<i16>()))?;
            Ok(i16::from_be_bytes(bytes))
        }

        let message_size = read_i32(buf_reader)?;
        let api_key = read_i16(buf_reader)?.try_into()?;
        let api_version = read_i16(buf_reader)?;
        let correlation_id = read_i32(buf_reader)?.into();

        // eventually should read the body, for now ignore the remaining body
        let expected_num_bytes = message_size as usize - 8;
        let mut body_bytes = vec![0; expected_num_bytes];
        buf_reader.read_exact(&mut body_bytes).map_err(|_| MissingData(expected_num_bytes))?;
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
