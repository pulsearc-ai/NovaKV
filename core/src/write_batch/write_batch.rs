use crate::status::Result;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    pub header: crate::write_batch::WriteBatchHeader,
    pub records: Vec<crate::write_batch::WriteBatchRecord>,
}

impl WriteBatch {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        self.header.encode(dst);
        for item in &self.records {
            item.encode(dst);
        }
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let header = crate::write_batch::WriteBatchHeader::decode_from(input)?;
        let records = {
            let n = (header.count) as usize;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(crate::write_batch::WriteBatchRecord::decode_from(input)?);
            }
            items
        };
        Ok(Self { header, records })
    }
}
