use tokio::io::AsyncRead;
use crate::api::request::KafkaRequestParseError;
use crate::serialisation::{ReadKafkaBytes, ToKafkaBytes};

pub struct VarInt(u32);

impl VarInt {
    pub fn new(x: u32) -> VarInt {
        VarInt(x)
    }

    /// Read a VarInt from bytes, assumes the bytes represent a valid VarInt
    pub fn from_bytes(bytes: &[u8]) -> VarInt {
        let result = bytes.iter()
            .enumerate()
            .fold(0, |result, (idx, byte)| {
                // strip continuation bit
                let new_num = (byte & 0b0111_1111) as u32;
                // each byte represents the next 7 bits of the number, multiply by 2^7 to shift the bits up to that value
                result + (new_num * 2u32.pow(7 * (idx as u32)))
            });
        VarInt(result)
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl ReadKafkaBytes for VarInt {
    async fn read_kafka_bytes<T: AsyncRead + Unpin>(reader: &mut T) -> Result<Self, KafkaRequestParseError> {
        // read all bytes in the varint
        let mut int_bytes = Vec::with_capacity(1);
        while int_bytes.last().map_or(true, |byte| has_continuation(*byte)) {
            int_bytes.push(u8::read_kafka_bytes(reader).await?);
        }

        Ok(VarInt::from_bytes(&int_bytes))
    }
}

/// Return true if the bytes continuation bit is set, and the integer continues in the next byte
fn has_continuation(byte: u8) -> bool {
    0b1000_0000 & byte != 0
}

fn mask_top_bits(n: u8) -> u8 {
    assert!(n <= 8);
    let mut result = 0u8;
    for _ in 0..n {
        result >>= 1;
        result = result | 0b1000_0000;
    }
    result
}

/// shift the bits in x up by the offset, returning any remainder in the tuple.1
fn shift_bits_up(x: u8, offset: u8) -> (u8, u8) {
    assert!(offset <= 8);
    if offset == 0 {
        (x, 0)
    } else {
        let mask = mask_top_bits(offset);
        let top_bits = x & mask;
        (x << offset, top_bits >> (8 - offset))
    }
}

// Follows Zig-Zag encoding described in https://protobuf.dev/programming-guides/encoding/#varints
impl ToKafkaBytes for VarInt {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8> {
        if self.0 == 0 {
            return vec![0];
        }

        // skip the empty bytes at the end, so that encode_bytes doesn't encode useless zeros
        let mut bytes: Vec<u8> = self.0
            .to_be_bytes()
            .into_iter()
            .skip_while(|byte| *byte == 0)
            .collect();
        bytes.reverse();

        let mut result = encode_bytes(&bytes, 0, 0, bytes.len());
        result.reverse();
        return result;

        // Use recursion to Zig-Zag encode bytes
        fn encode_bytes(bytes: &[u8], offset: u8, carry: u8, num_bytes: usize) -> Vec<u8> {
            match bytes {
                [] if carry != 0 => vec![carry],
                [] => Vec::new(),
                [byte, rest @ ..] => {
                    let (mut byte, mut new_carry) = shift_bits_up(*byte, offset);
                    byte |= carry; // above line shifted the bits up enough to accommodate the carry

                    // shift the new_carry up by one, since we're going to the next byte
                    new_carry <<= 1;

                    // if the top bit of this byte is set, then we need to carry it to the next byte
                    if has_continuation(byte) {
                        new_carry += 1
                    }
                    // otherwise, if we're not at the end then set the carry bit
                    else if num_bytes != 1 {
                        byte |= 0b1000_0000
                    }

                    // always loop through until the end of the byte array,
                    // since perhaps the last bit is set while some bytes in the middle are all zeros
                    let mut next_bytes = encode_bytes(rest, offset + 1, new_carry, num_bytes - 1);
                    next_bytes.push(byte);
                    next_bytes
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn convert_to_bytes(x: u32) -> Vec<u8> {
        VarInt::new(x).to_kafka_bytes().into_iter().collect()
    }

    #[test]
    fn test_mask_top_bits() {
        assert_eq!(mask_top_bits(0), 0);
        assert_eq!(mask_top_bits(1), 0b1000_0000);
        assert_eq!(mask_top_bits(2), 0b1100_0000);
        assert_eq!(mask_top_bits(3), 0b1110_0000);
        assert_eq!(mask_top_bits(8), 0b1111_1111);
    }

    #[test]
    fn test_shift_bits_up() {
        assert_eq!(shift_bits_up(0, 0), (0, 0));
        assert_eq!(shift_bits_up(1, 3), (1 << 3, 0));
        assert_eq!(shift_bits_up(0b1000_0000, 1), (0, 1));
        assert_eq!(shift_bits_up(0b1011_0110, 3), (0b1011_0000, 0b0000_0101));
    }

    #[test]
    fn test_single_byte() {
        assert_eq!(convert_to_bytes(0), vec![0]);
        assert_eq!(convert_to_bytes(127), vec![127]);
    }

    #[test]
    fn test_two_bytes() {
        assert_eq!(
            convert_to_bytes(0b1000_0000),
            vec![0b1000_0000, 0b0000_0001]
        );
        assert_eq!(
            convert_to_bytes(0b1000_0101),
            vec![0b1000_0101, 0b0000_0001]
        );
        assert_eq!(
            convert_to_bytes(0b0000_0010_0000_0000),
            vec![0b1000_0000, 0b0000_0100]
        );
    }

    #[test]
    fn test_three_bytes() {
        assert_eq!(
            convert_to_bytes(0b0100_0011_1000_0101),
            vec![0b1000_0101, 0b1000_0111, 0b0000_0001]
        );
    }

    #[test]
    fn test_varint_read() {
        fn roundtrip(x: u32) -> u32 {
            VarInt::from_bytes(&convert_to_bytes(x)).value()
        }

        assert_eq!(roundtrip(127), 127);
        assert_eq!(roundtrip(128), 128);
        assert_eq!(roundtrip(400), 400);
        assert_eq!(roundtrip(511), 511);
        assert_eq!(roundtrip(512), 512);
        assert_eq!(roundtrip(513), 513);
        assert_eq!(roundtrip(0b0100_0011_1000_0101), 0b0100_0011_1000_0101);
    }
}
