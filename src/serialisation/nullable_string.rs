use tokio::io::{AsyncRead, AsyncReadExt};
use crate::api::request::KafkaRequestParseError;
use crate::api::request::KafkaRequestParseError::{InvalidStringLength, MissingData};
use crate::serialisation::from_kafka_bytes::ReadKafkaBytes;

/// Represents a nullable string read from the Kafka Protocol
#[derive(Debug)]
pub struct NullableString(Option<Box<str>>);

impl ReadKafkaBytes for NullableString {
    async fn read_kafka_bytes<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError> {
        let length = i16::read_kafka_bytes(reader).await?;
        match length {
            -1 => Ok(NullableString(None)),
            ..-1 => Err(InvalidStringLength(length as i32)),
            _ => {
                let mut string_bytes = vec![0u8; length as usize];
                reader.read_exact(&mut string_bytes).await
                    .map_err(|_| MissingData(length as usize))?;
                let string = String::from_utf8(string_bytes)?;
                Ok(NullableString(Some(string.into())))
            }
        }
    }
}