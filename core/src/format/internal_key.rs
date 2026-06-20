use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub tag: u64,
}

impl InternalKey {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        dst.extend_from_slice(&self.user_key);
        crate::coding::put_fixed64(dst, self.tag);
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        if input.len() < 8 {
            return Err(Status::corruption("InternalKey: too short"));
        }
        let user_key = {
            let n = input.len() - 8;
            let bytes = input[..n].to_vec();
            *input = &input[n..];
            bytes
        };
        let tag = crate::coding::get_fixed64(input)
            .ok_or_else(|| Status::corruption("InternalKey: missing tag"))?;
        Ok(Self { user_key, tag })
    }
}
