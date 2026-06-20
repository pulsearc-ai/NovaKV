use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        crate::coding::put_varint64(dst, self.offset);
        crate::coding::put_varint64(dst, self.size);
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let offset = crate::coding::get_varint64(input)
            .ok_or_else(|| Status::corruption("BlockHandle: missing offset"))?;
        let size = crate::coding::get_varint64(input)
            .ok_or_else(|| Status::corruption("BlockHandle: missing size"))?;
        Ok(Self { offset, size })
    }

    pub fn encode_streaming(dst: &mut Vec<u8>, offset: u64, size: u64) {
        crate::coding::put_varint64(dst, offset);
        crate::coding::put_varint64(dst, size);
    }

    pub fn visit<F, R>(input: &mut &[u8], f: F) -> Result<R>
    where
        F: FnOnce(u64, u64) -> R,
    {
        let offset = crate::coding::get_varint64(input)
            .ok_or_else(|| Status::corruption("BlockHandle: missing offset"))?;
        let size = crate::coding::get_varint64(input)
            .ok_or_else(|| Status::corruption("BlockHandle: missing size"))?;
        Ok(f(offset, size))
    }
}
