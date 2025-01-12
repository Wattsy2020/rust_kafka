use crate::serialisation::varint::VarInt;

pub mod varint;
pub mod nullable_string;
pub mod from_kafka_bytes;

/// Types that can be serialised and used in the Kafka API
pub trait ToKafkaBytes {
    /// Convert the data to bytes that can be returned in a Kafka API Protocol Response
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8>;
}

impl ToKafkaBytes for u8 {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8> {
        self.to_be_bytes()
    }
}

impl ToKafkaBytes for i16 {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8> {
        self.to_be_bytes()
    }
}

impl ToKafkaBytes for i32 {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8> {
        self.to_be_bytes()
    }
}

impl<T: ToKafkaBytes> ToKafkaBytes for Vec<T> {
    // write the length of the array, then each item in the array
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8> {
        let length = VarInt::new((self.len() + 1) as u32);
        let vec_as_bytes = self.into_iter().flat_map(|item| item.to_kafka_bytes());
        length.to_kafka_bytes().into_iter().chain(vec_as_bytes)
    }
}

/// Converts to bytes and adds the message size
pub fn to_response_message<T: ToKafkaBytes>(response: T) -> impl Iterator<Item = u8> {
    let bytes: Box<[u8]> = response.to_kafka_bytes().into_iter().collect();
    let size = bytes.len() as i32;
    size.to_be_bytes().into_iter().chain(bytes)
}
