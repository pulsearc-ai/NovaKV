use crate::status::{Result, Status};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactPointer {
    pub level: u32,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeletedFile {
    pub level: u32,
    pub number: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NewFile {
    pub level: u32,
    pub meta: crate::version_set::FileMetaData,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    pub comparator: Option<Vec<u8>>,
    pub log_number: Option<u64>,
    pub prev_log_number: Option<u64>,
    pub next_file_number: Option<u64>,
    pub last_sequence: Option<u64>,
    pub compact_pointers: Vec<CompactPointer>,
    pub deleted_files: Vec<DeletedFile>,
    pub new_files: Vec<NewFile>,
}

impl VersionEdit {
    pub fn encode(&self, dst: &mut Vec<u8>) {
        if let Some(name) = &self.comparator {
            crate::coding::put_varint32(dst, 1u32);
            crate::coding::put_length_prefixed_slice(dst, name);
        }
        if let Some(number) = &self.log_number {
            crate::coding::put_varint32(dst, 2u32);
            crate::coding::put_varint64(dst, *number);
        }
        if let Some(number) = &self.prev_log_number {
            crate::coding::put_varint32(dst, 9u32);
            crate::coding::put_varint64(dst, *number);
        }
        if let Some(number) = &self.next_file_number {
            crate::coding::put_varint32(dst, 3u32);
            crate::coding::put_varint64(dst, *number);
        }
        if let Some(seq) = &self.last_sequence {
            crate::coding::put_varint32(dst, 4u32);
            crate::coding::put_varint64(dst, *seq);
        }
        for item in &self.compact_pointers {
            crate::coding::put_varint32(dst, 5u32);
            crate::coding::put_varint32(dst, item.level);
            crate::coding::put_length_prefixed_slice(dst, &item.key);
        }
        for item in &self.deleted_files {
            crate::coding::put_varint32(dst, 6u32);
            crate::coding::put_varint32(dst, item.level);
            crate::coding::put_varint64(dst, item.number);
        }
        for item in &self.new_files {
            crate::coding::put_varint32(dst, 7u32);
            crate::coding::put_varint32(dst, item.level);
            item.meta.encode(dst);
        }
    }

    pub fn decode_from(input: &mut &[u8]) -> Result<Self> {
        let mut comparator = None;
        let mut log_number = None;
        let mut prev_log_number = None;
        let mut next_file_number = None;
        let mut last_sequence = None;
        let mut compact_pointers = Vec::new();
        let mut deleted_files = Vec::new();
        let mut new_files = Vec::new();
        while !input.is_empty() {
            let tag = crate::coding::get_varint32(input)
                .ok_or_else(|| Status::corruption("VersionEdit: missing tag"))?;
            match tag {
                1 => {
                    let name = crate::coding::get_length_prefixed_slice(input)
                        .map(<[u8]>::to_vec)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing name"))?;
                    comparator = Some(name);
                }
                2 => {
                    let number = crate::coding::get_varint64(input)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing number"))?;
                    log_number = Some(number);
                }
                9 => {
                    let number = crate::coding::get_varint64(input)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing number"))?;
                    prev_log_number = Some(number);
                }
                3 => {
                    let number = crate::coding::get_varint64(input)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing number"))?;
                    next_file_number = Some(number);
                }
                4 => {
                    let seq = crate::coding::get_varint64(input)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing seq"))?;
                    last_sequence = Some(seq);
                }
                5 => {
                    let level = crate::coding::get_varint32(input)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing level"))?;
                    if level >= crate::format::NUM_LEVELS as u32 {
                        return Err(Status::corruption("VersionEdit: level out of range"));
                    }
                    let key = crate::coding::get_length_prefixed_slice(input)
                        .map(<[u8]>::to_vec)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing key"))?;
                    compact_pointers.push(CompactPointer { level, key });
                }
                6 => {
                    let level = crate::coding::get_varint32(input)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing level"))?;
                    if level >= crate::format::NUM_LEVELS as u32 {
                        return Err(Status::corruption("VersionEdit: level out of range"));
                    }
                    let number = crate::coding::get_varint64(input)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing number"))?;
                    deleted_files.push(DeletedFile { level, number });
                }
                7 => {
                    let level = crate::coding::get_varint32(input)
                        .ok_or_else(|| Status::corruption("VersionEdit: missing level"))?;
                    if level >= crate::format::NUM_LEVELS as u32 {
                        return Err(Status::corruption("VersionEdit: level out of range"));
                    }
                    let meta = crate::version_set::FileMetaData::decode_from(input)?;
                    new_files.push(NewFile { level, meta });
                }
                other => {
                    return Err(Status::corruption(format!(
                        "VersionEdit: unknown tag {other}"
                    )))
                }
            }
        }
        Ok(Self {
            comparator,
            log_number,
            prev_log_number,
            next_file_number,
            last_sequence,
            compact_pointers,
            deleted_files,
            new_files,
        })
    }
}
