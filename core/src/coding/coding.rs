pub fn put_fixed16(dst: &mut Vec<u8>, value: u16) {
    dst.extend_from_slice(&value.to_le_bytes());
}

pub fn put_fixed32(dst: &mut Vec<u8>, value: u32) {
    dst.extend_from_slice(&value.to_le_bytes());
}

pub fn put_fixed64(dst: &mut Vec<u8>, value: u64) {
    dst.extend_from_slice(&value.to_le_bytes());
}

pub fn encode_fixed64(dst: &mut [u8], value: u64) {
    dst[..8].copy_from_slice(&value.to_le_bytes());
}

pub fn decode_fixed16(src: &[u8]) -> u16 {
    let bytes: [u8; 2] = src[..2].try_into().expect("fixed16 requires 2 bytes");
    u16::from_le_bytes(bytes)
}

pub fn decode_fixed32(src: &[u8]) -> u32 {
    let bytes: [u8; 4] = src[..4].try_into().expect("fixed32 requires 4 bytes");
    u32::from_le_bytes(bytes)
}

pub fn decode_fixed64(src: &[u8]) -> u64 {
    let bytes: [u8; 8] = src[..8].try_into().expect("fixed64 requires 8 bytes");
    u64::from_le_bytes(bytes)
}

/// Cursor-style fixed-width readers. Each advances `*input` by
/// the consumed bytes on success.
pub fn get_u8(input: &mut &[u8]) -> Option<u8> {
    let (&first, rest) = input.split_first()?;
    *input = rest;
    Some(first)
}

pub fn get_fixed16(input: &mut &[u8]) -> Option<u16> {
    if input.len() < 2 {
        return None;
    }
    let value = decode_fixed16(input);
    *input = &input[2..];
    Some(value)
}

pub fn get_fixed32(input: &mut &[u8]) -> Option<u32> {
    if input.len() < 4 {
        return None;
    }
    let value = decode_fixed32(input);
    *input = &input[4..];
    Some(value)
}

pub fn get_fixed64(input: &mut &[u8]) -> Option<u64> {
    if input.len() < 8 {
        return None;
    }
    let value = decode_fixed64(input);
    *input = &input[8..];
    Some(value)
}

pub fn put_varint32(dst: &mut Vec<u8>, value: u32) {
    put_varint64(dst, u64::from(value));
}

pub fn put_varint64(dst: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        dst.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    dst.push(value as u8);
}

pub fn encode_varint32(dst: &mut [u8], value: u32) -> usize {
    encode_varint64(dst, u64::from(value))
}

pub fn encode_varint64(dst: &mut [u8], mut value: u64) -> usize {
    let mut offset = 0usize;
    while value >= 0x80 {
        dst[offset] = ((value & 0x7f) as u8) | 0x80;
        value >>= 7;
        offset += 1;
    }
    dst[offset] = value as u8;
    offset + 1
}

pub fn get_varint32(input: &mut &[u8]) -> Option<u32> {
    let mut cursor = *input;
    let mut result = 0u32;
    for shift in (0..=28).step_by(7) {
        let (&byte, rest) = cursor.split_first()?;
        cursor = rest;
        if byte & 0x80 != 0 {
            result |= u32::from(byte & 0x7f) << shift;
        } else {
            result |= u32::from(byte) << shift;
            *input = cursor;
            return Some(result);
        }
    }
    None
}

pub fn get_varint64(input: &mut &[u8]) -> Option<u64> {
    let mut cursor = *input;
    let mut result = 0u64;
    for shift in (0..=63).step_by(7) {
        let (&byte, rest) = cursor.split_first()?;
        cursor = rest;
        if byte & 0x80 != 0 {
            result |= u64::from(byte & 0x7f) << shift;
        } else {
            result |= u64::from(byte) << shift;
            *input = cursor;
            return Some(result);
        }
    }
    None
}

pub fn varint_length(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

pub fn put_length_prefixed_slice(dst: &mut Vec<u8>, value: &[u8]) {
    // Reserve the varint length prefix plus the payload up front so
    // the byte-at-a-time varint push and the slice copy never
    // trigger more than one reallocation.
    dst.reserve(varint_length(value.len() as u64) + value.len());
    put_varint32(dst, value.len() as u32);
    dst.extend_from_slice(value);
}

pub fn get_length_prefixed_slice<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = get_varint32(input)? as usize;
    if input.len() < len {
        return None;
    }
    let (value, rest) = input.split_at(len);
    *input = rest;
    Some(value)
}
