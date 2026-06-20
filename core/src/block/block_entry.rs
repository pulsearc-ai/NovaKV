use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockEntry {
    pub shared: u32,
    pub unshared: u32,
    pub value_len: u32,
    pub key_delta: Vec<u8>,
    pub value: Vec<u8>,
}

impl BlockEntry {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        crate::coding::put_varint32(dst, self.shared);
        crate::coding::put_varint32(dst, self.unshared);
        crate::coding::put_varint32(dst, self.value_len);
        dst.extend_from_slice(&self.key_delta);
        dst.extend_from_slice(&self.value);
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let shared = crate::coding::get_varint32(input)
            .ok_or_else(|| Status::corruption("BlockEntry: missing shared"))?;
        let unshared = crate::coding::get_varint32(input)
            .ok_or_else(|| Status::corruption("BlockEntry: missing unshared"))?;
        let value_len = crate::coding::get_varint32(input)
            .ok_or_else(|| Status::corruption("BlockEntry: missing value_len"))?;
        let key_delta = {
            let n = unshared as usize;
            if input.len() < n {
                return Err(Status::corruption("BlockEntry: missing key_delta"));
            }
            let bytes = input[..n].to_vec();
            *input = &input[n..];
            bytes
        };
        let value = {
            let n = value_len as usize;
            if input.len() < n {
                return Err(Status::corruption("BlockEntry: missing value"));
            }
            let bytes = input[..n].to_vec();
            *input = &input[n..];
            bytes
        };
        Ok(Self {
            shared,
            unshared,
            value_len,
            key_delta,
            value,
        })
    }

    pub fn encode_streaming(
        dst: &mut Vec<u8>,
        shared: u32,
        unshared: u32,
        value_len: u32,
        key_delta: &[u8],
        value: &[u8],
    ) {
        crate::coding::put_varint32(dst, shared);
        crate::coding::put_varint32(dst, unshared);
        crate::coding::put_varint32(dst, value_len);
        dst.extend_from_slice(key_delta);
        dst.extend_from_slice(value);
    }

    pub fn visit<F, R>(input: &mut &[u8], f: F) -> Result<R>
    where
        F: FnOnce(u32, u32, u32, &[u8], &[u8]) -> R,
    {
        let shared = crate::coding::get_varint32(input)
            .ok_or_else(|| Status::corruption("BlockEntry: missing shared"))?;
        let unshared = crate::coding::get_varint32(input)
            .ok_or_else(|| Status::corruption("BlockEntry: missing unshared"))?;
        let value_len = crate::coding::get_varint32(input)
            .ok_or_else(|| Status::corruption("BlockEntry: missing value_len"))?;
        let key_delta = {
            let n = unshared as usize;
            if input.len() < n {
                return Err(Status::corruption("BlockEntry: missing key_delta"));
            }
            let bytes: &[u8] = &input[..n];
            *input = &input[n..];
            bytes
        };
        let value = {
            let n = value_len as usize;
            if input.len() < n {
                return Err(Status::corruption("BlockEntry: missing value"));
            }
            let bytes: &[u8] = &input[..n];
            *input = &input[n..];
            bytes
        };
        Ok(f(shared, unshared, value_len, key_delta, value))
    }
}
