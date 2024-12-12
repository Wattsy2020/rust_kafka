use crate::serialisation::ToKafkaBytes;

pub struct VarInt(u32);

impl VarInt {
    pub fn new(x: u32) -> VarInt {
        VarInt(x)
    }
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

// Use recursion to Zig-Zag encode bytes
fn encode_bytes(bytes: &[u8], offset: u8, carry: u8) -> Vec<u8> {
    match bytes {
        [] => Vec::new(),
        [byte, rest @ ..] => {
            let (mut byte, new_carry) = shift_bits_up(*byte, offset);
            byte |= carry; // above line shifted the bits up enough to accommodate the carry

            // logic to check if we need a next byte
            // chick if the top bit in byte is set, or if carry isn't 0
            if 0b1000_0000 & byte != 0 || new_carry != 0 {
                let new_carry = (new_carry << 1) + 1;
                let mut next_bytes = encode_bytes(rest, offset + 1, new_carry);
                next_bytes.push(byte);
                next_bytes
            } else {
                vec![byte]
            }
        }
    }
}

// Follows Zig-Zag encoding described in https://protobuf.dev/programming-guides/encoding/#varints
impl ToKafkaBytes for VarInt {
    fn to_kafka_bytes(self) -> impl IntoIterator<Item = u8> {
        let bytes = Box::new(self.0.to_le_bytes());
        let mut result = encode_bytes(bytes.as_slice(), 0, 0);
        result.reverse();
        result
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
    }

    #[test]
    fn test_three_bytes() {
        assert_eq!(
            convert_to_bytes(0b0100_0011_1000_0101),
            vec![0b1000_0101, 0b1000_0111, 0b0000_0001]
        );
    }
}
