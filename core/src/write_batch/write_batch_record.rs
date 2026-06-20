use crate::status::{Result, Status};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteBatchRecord {
    Delete { key: Vec<u8> },
    Put { key: Vec<u8>, value: Vec<u8> },
}

impl WriteBatchRecord {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        match self {
            Self::Delete { key } => {
                dst.push(0u8);
                crate::coding::put_length_prefixed_slice(dst, key);
            }
            Self::Put { key, value } => {
                dst.push(1u8);
                crate::coding::put_length_prefixed_slice(dst, key);
                crate::coding::put_length_prefixed_slice(dst, value);
            }
        }
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let tag = crate::coding::get_u8(input)
            .ok_or_else(|| Status::corruption("WriteBatchRecord: missing tag"))?;
        match tag {
            0 => {
                let key = crate::coding::get_length_prefixed_slice(input)
                    .map(<[u8]>::to_vec)
                    .ok_or_else(|| Status::corruption("WriteBatchRecord: missing key"))?;
                Ok(Self::Delete { key })
            }
            1 => {
                let key = crate::coding::get_length_prefixed_slice(input)
                    .map(<[u8]>::to_vec)
                    .ok_or_else(|| Status::corruption("WriteBatchRecord: missing key"))?;
                let value = crate::coding::get_length_prefixed_slice(input)
                    .map(<[u8]>::to_vec)
                    .ok_or_else(|| Status::corruption("WriteBatchRecord: missing value"))?;
                Ok(Self::Put { key, value })
            }
            other => Err(Status::corruption(format!(
                "WriteBatchRecord: unknown tag {other}"
            ))),
        }
    }

    pub fn encode_delete(dst: &mut Vec<u8>, key: &[u8]) {
        dst.reserve(6 + key.len());
        dst.push(0u8);
        crate::coding::put_length_prefixed_slice(dst, key);
    }

    pub fn encode_put(dst: &mut Vec<u8>, key: &[u8], value: &[u8]) {
        dst.reserve(11 + key.len() + value.len());
        dst.push(1u8);
        crate::coding::put_length_prefixed_slice(dst, key);
        crate::coding::put_length_prefixed_slice(dst, value);
    }

    pub fn visit<V: WriteBatchRecordVisitor>(input: &mut &[u8], visitor: &mut V) -> Result<()> {
        let tag = crate::coding::get_u8(input)
            .ok_or_else(|| Status::corruption("WriteBatchRecord: missing tag"))?;
        match tag {
            0 => {
                let key = crate::coding::get_length_prefixed_slice(input)
                    .ok_or_else(|| Status::corruption("WriteBatchRecord: missing key"))?;
                visitor.delete(key);
            }
            1 => {
                let key = crate::coding::get_length_prefixed_slice(input)
                    .ok_or_else(|| Status::corruption("WriteBatchRecord: missing key"))?;
                let value = crate::coding::get_length_prefixed_slice(input)
                    .ok_or_else(|| Status::corruption("WriteBatchRecord: missing value"))?;
                visitor.put(key, value);
            }
            other => {
                return Err(Status::corruption(format!(
                    "WriteBatchRecord: unknown tag {other}"
                )))
            }
        }
        Ok(())
    }
}

pub trait WriteBatchRecordVisitor {
    fn delete(&mut self, key: &[u8]);
    fn put(&mut self, key: &[u8], value: &[u8]);
}
