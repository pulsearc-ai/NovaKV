use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogRecordHeader {
    pub crc: u32,
    pub length: u16,
    pub kind: u8,
}

impl LogRecordHeader {
    pub const SIZE: usize = 7;
    pub const KIND_ZERO: u8 = 0;
    pub const KIND_FULL: u8 = 1;
    pub const KIND_FIRST: u8 = 2;
    pub const KIND_MIDDLE: u8 = 3;
    pub const KIND_LAST: u8 = 4;

    pub fn encode(&self, dst: &mut Vec<u8>) {
        crate::coding::put_fixed32(dst, crate::crc32c::mask(self.crc));
        crate::coding::put_fixed16(dst, self.length);
        dst.push(self.kind);
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let crc_raw = crate::coding::get_fixed32(input)
            .ok_or_else(|| Status::corruption("LogRecordHeader: missing crc"))?;
        let crc = crate::crc32c::unmask(crc_raw);
        let length = crate::coding::get_fixed16(input)
            .ok_or_else(|| Status::corruption("LogRecordHeader: missing length"))?;
        let kind = crate::coding::get_u8(input)
            .ok_or_else(|| Status::corruption("LogRecordHeader: missing kind"))?;
        Ok(Self { crc, length, kind })
    }
}
