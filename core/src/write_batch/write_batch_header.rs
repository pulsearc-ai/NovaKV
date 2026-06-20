use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatchHeader {
    pub sequence: u64,
    pub count: u32,
}

impl WriteBatchHeader {
    pub const SIZE: usize = 12;

    pub fn encode(&self, dst: &mut Vec<u8>) {
        crate::coding::put_fixed64(dst, self.sequence);
        crate::coding::put_fixed32(dst, self.count);
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let sequence = crate::coding::get_fixed64(input)
            .ok_or_else(|| Status::corruption("WriteBatchHeader: missing sequence"))?;
        let count = crate::coding::get_fixed32(input)
            .ok_or_else(|| Status::corruption("WriteBatchHeader: missing count"))?;
        Ok(Self { sequence, count })
    }
}
