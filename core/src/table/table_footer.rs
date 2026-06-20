use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableFooter {
    pub metaindex: crate::block::BlockHandle,
    pub index: crate::block::BlockHandle,
}

impl TableFooter {
    pub const ENCODED_LENGTH: usize = 48;
    pub const MAGIC: u64 = 15800726617472432983;

    pub fn encode(&self, dst: &mut Vec<u8>) {
        let start = dst.len();
        self.metaindex.encode(dst);
        self.index.encode(dst);
        if dst.len() - start < 40 {
            dst.resize(start + 40, 0);
        }
        crate::coding::put_fixed64(dst, Self::MAGIC);
        debug_assert_eq!(dst.len() - start, Self::ENCODED_LENGTH);
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        if input.len() < Self::ENCODED_LENGTH {
            return Err(Status::corruption("TableFooter: truncated input"));
        }
        let magic_bytes = &input[Self::ENCODED_LENGTH - 8..Self::ENCODED_LENGTH];
        let magic = crate::coding::decode_fixed64(magic_bytes);
        if magic != Self::MAGIC {
            return Err(Status::corruption("TableFooter: bad magic number"));
        }
        let mut prefix_slice: &[u8] = &input[..Self::ENCODED_LENGTH - 8];
        let prefix = &mut prefix_slice;
        let metaindex = crate::block::BlockHandle::decode_from(prefix)?;
        let index = crate::block::BlockHandle::decode_from(prefix)?;
        *input = &input[Self::ENCODED_LENGTH..];
        Ok(Self { metaindex, index })
    }
}
