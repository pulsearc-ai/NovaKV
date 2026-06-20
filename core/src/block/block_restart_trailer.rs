use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlockRestartTrailer {
    pub items: Vec<u32>,
}

impl BlockRestartTrailer {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        for item in &self.items {
            crate::coding::put_fixed32(dst, *item);
        }
        crate::coding::put_fixed32(dst, self.items.len() as u32);
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        if input.len() < 4 {
            return Err(Status::corruption("BlockRestartTrailer: missing count"));
        }
        let count_offset = input.len() - 4;
        let count = crate::coding::decode_fixed32(&input[count_offset..]) as usize;
        if count_offset != count * 4 {
            return Err(Status::corruption(
                "BlockRestartTrailer: count/length mismatch",
            ));
        }
        let mut items = Vec::with_capacity(count);
        for i in 0..count {
            items.push(crate::coding::decode_fixed32(&input[i * 4..(i + 1) * 4]));
        }
        *input = &input[input.len()..];
        Ok(Self { items })
    }
}
