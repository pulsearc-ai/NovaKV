use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileMetaData {
    pub number: u64,
    pub file_size: u64,
    pub smallest: Vec<u8>,
    pub largest: Vec<u8>,
}

impl FileMetaData {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        crate::coding::put_varint64(dst, self.number);
        crate::coding::put_varint64(dst, self.file_size);
        crate::coding::put_length_prefixed_slice(dst, &self.smallest);
        crate::coding::put_length_prefixed_slice(dst, &self.largest);
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let number = crate::coding::get_varint64(input)
            .ok_or_else(|| Status::corruption("FileMetaData: missing number"))?;
        let file_size = crate::coding::get_varint64(input)
            .ok_or_else(|| Status::corruption("FileMetaData: missing file_size"))?;
        let smallest = crate::coding::get_length_prefixed_slice(input)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| Status::corruption("FileMetaData: missing smallest"))?;
        let largest = crate::coding::get_length_prefixed_slice(input)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| Status::corruption("FileMetaData: missing largest"))?;
        Ok(Self {
            number,
            file_size,
            smallest,
            largest,
        })
    }

    pub fn encode_streaming(
        dst: &mut Vec<u8>,
        number: u64,
        file_size: u64,
        smallest: &[u8],
        largest: &[u8],
    ) {
        crate::coding::put_varint64(dst, number);
        crate::coding::put_varint64(dst, file_size);
        crate::coding::put_length_prefixed_slice(dst, smallest);
        crate::coding::put_length_prefixed_slice(dst, largest);
    }

    pub fn visit<F, R>(input: &mut &[u8], f: F) -> Result<R>
    where
        F: FnOnce(u64, u64, &[u8], &[u8]) -> R,
    {
        let number = crate::coding::get_varint64(input)
            .ok_or_else(|| Status::corruption("FileMetaData: missing number"))?;
        let file_size = crate::coding::get_varint64(input)
            .ok_or_else(|| Status::corruption("FileMetaData: missing file_size"))?;
        let smallest = crate::coding::get_length_prefixed_slice(input)
            .ok_or_else(|| Status::corruption("FileMetaData: missing smallest"))?;
        let largest = crate::coding::get_length_prefixed_slice(input)
            .ok_or_else(|| Status::corruption("FileMetaData: missing largest"))?;
        Ok(f(number, file_size, smallest, largest))
    }
}
