use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockTrailer {
    pub kind: u8,
    pub crc: u32,
}

impl BlockTrailer {
    pub const SIZE: usize = 5;
    pub const KIND_NO_COMPRESSION: u8 = 0;
    pub const KIND_SNAPPY: u8 = 1;

    pub fn encode(&self, dst: &mut Vec<u8>) {
        dst.push(self.kind);
        crate::coding::put_fixed32(dst, crate::crc32c::mask(self.crc));
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let kind = crate::coding::get_u8(input)
            .ok_or_else(|| Status::corruption("BlockTrailer: missing kind"))?;
        let crc_raw = crate::coding::get_fixed32(input)
            .ok_or_else(|| Status::corruption("BlockTrailer: missing crc"))?;
        let crc = crate::crc32c::unmask(crc_raw);
        Ok(Self { kind, crc })
    }
}
