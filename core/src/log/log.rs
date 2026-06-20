use crate::env::{SequentialFile, WritableFile};
use crate::status::{Result, Status};

pub const BLOCK_SIZE: usize = 32768;
pub const HEADER_SIZE: usize = crate::log::LogRecordHeader::SIZE;

/// Write side of the record log. Frames variable-length
/// records into BLOCK_SIZE-sized blocks; records that don't
/// fit get split into Full/First/Middle/Last fragments. The
/// trailing < HEADER_SIZE bytes of each block are zero-padded.
#[derive(Debug, Default)]
pub struct LogWriter {
    sink: Vec<u8>,
    block_offset: usize,
    /// Reused buffer for the per-record header encode on the
    /// `*_to` (streaming) path, so each record does not heap-
    /// allocate a fresh header `Vec`.
    scratch: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogWriterMark {
    sink_len: usize,
    block_offset: usize,
}

impl LogWriter {
    pub fn new() -> Self {
        Self {
            sink: Vec::new(),
            block_offset: 0,
            scratch: Vec::new(),
        }
    }

    /// Resume writing into an existing log. `tail_len` is the byte
    /// length already present in the log file; the writer's cursor is
    /// placed `tail_len % BLOCK_SIZE` into the active block so framing
    /// continues seamlessly from where the prior writer stopped. Used
    /// by `reuse_logs` recovery to append to an existing WAL instead of
    /// starting a fresh one. The `*_to` (streaming) write path uses only
    /// `block_offset`; `sink` stays empty.
    pub fn resume(tail_len: u64) -> Self {
        Self {
            sink: Vec::new(),
            block_offset: (tail_len % BLOCK_SIZE as u64) as usize,
            scratch: Vec::new(),
        }
    }

    pub fn mark(&self) -> LogWriterMark {
        LogWriterMark {
            sink_len: self.sink.len(),
            block_offset: self.block_offset,
        }
    }

    pub fn rewind_to(&mut self, mark: LogWriterMark) {
        assert!(
            mark.sink_len <= self.sink.len(),
            "LogWriter::rewind_to: future mark"
        );
        self.sink.truncate(mark.sink_len);
        self.block_offset = mark.block_offset;
    }

    pub fn add_record_and_get_fragment(&mut self, data: &[u8]) -> &[u8] {
        let start = self.sink.len();
        self.add_record(data);
        &self.sink[start..]
    }

    pub fn add_record(&mut self, data: &[u8]) {
        let mut left = data;
        let mut begin = true;
        loop {
            let leftover = BLOCK_SIZE - self.block_offset;
            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    // Zero-pad trailing < HEADER_SIZE bytes; readers skip.
                    self.sink.extend(std::iter::repeat_n(0u8, leftover));
                }
                self.block_offset = 0;
            }

            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_len = left.len().min(avail);
            let end = left.len() == fragment_len;
            let kind = if begin && end {
                crate::log::LogRecordHeader::KIND_FULL
            } else if begin {
                crate::log::LogRecordHeader::KIND_FIRST
            } else if end {
                crate::log::LogRecordHeader::KIND_LAST
            } else {
                crate::log::LogRecordHeader::KIND_MIDDLE
            };
            self.emit_physical_record(kind, &left[..fragment_len]);
            left = &left[fragment_len..];
            begin = false;
            if end {
                break;
            }
        }
    }

    pub fn add_record_to<W: WritableFile>(&mut self, dest: &mut W, data: &[u8]) -> Result<()> {
        self.add_record_pair_to(dest, data, &[])
    }

    pub fn add_record_pair_to<W: WritableFile>(
        &mut self,
        dest: &mut W,
        first: &[u8],
        second: &[u8],
    ) -> Result<()> {
        let total_len = first.len() + second.len();
        let mut offset = 0usize;
        let mut left = total_len;
        let mut begin = true;
        loop {
            let leftover = BLOCK_SIZE - self.block_offset;
            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    let padding = [0u8; HEADER_SIZE - 1];
                    dest.append(&padding[..leftover])?;
                }
                self.block_offset = 0;
            }

            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_len = left.min(avail);
            let end = left == fragment_len;
            let kind = if begin && end {
                crate::log::LogRecordHeader::KIND_FULL
            } else if begin {
                crate::log::LogRecordHeader::KIND_FIRST
            } else if end {
                crate::log::LogRecordHeader::KIND_LAST
            } else {
                crate::log::LogRecordHeader::KIND_MIDDLE
            };
            let (first_fragment, second_fragment) =
                Self::pair_fragment(first, second, offset, fragment_len);
            self.emit_physical_record_pair_to(dest, kind, first_fragment, second_fragment)?;
            offset += fragment_len;
            left -= fragment_len;
            begin = false;
            if end {
                break;
            }
        }
        Ok(())
    }

    fn emit_physical_record(&mut self, kind: u8, data: &[u8]) {
        // CRC covers the kind byte plus the data payload.
        let crc = crate::crc32c::value(&[kind]);
        let crc = crate::crc32c::extend(crc, data);
        let header = crate::log::LogRecordHeader {
            crc,
            length: data.len() as u16,
            kind,
        };
        header.encode(&mut self.sink);
        self.sink.extend_from_slice(data);
        self.block_offset += HEADER_SIZE + data.len();
    }

    fn pair_fragment<'a>(
        first: &'a [u8],
        second: &'a [u8],
        offset: usize,
        len: usize,
    ) -> (&'a [u8], &'a [u8]) {
        if offset < first.len() {
            let first_len = (first.len() - offset).min(len);
            let first_fragment = &first[offset..offset + first_len];
            let remaining = len - first_len;
            let second_fragment = &second[..remaining];
            (first_fragment, second_fragment)
        } else {
            let start = offset - first.len();
            (&[], &second[start..start + len])
        }
    }

    fn emit_physical_record_pair_to<W: WritableFile>(
        &mut self,
        dest: &mut W,
        kind: u8,
        first: &[u8],
        second: &[u8],
    ) -> Result<()> {
        let crc = crate::crc32c::value(&[kind]);
        let crc = crate::crc32c::extend(crc, first);
        let crc = crate::crc32c::extend(crc, second);
        let length = first.len() + second.len();
        let header = crate::log::LogRecordHeader {
            crc,
            length: length as u16,
            kind,
        };
        self.scratch.clear();
        header.encode(&mut self.scratch);
        dest.append(&self.scratch)?;
        if !first.is_empty() {
            dest.append(first)?;
        }
        if !second.is_empty() {
            dest.append(second)?;
        }
        dest.flush()?;
        self.block_offset += HEADER_SIZE + length;
        Ok(())
    }

    pub fn bytes(&self) -> &[u8] {
        &self.sink
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.sink
    }
}

/// Read side of the record log. Yields one user record
/// per call, reassembling Full or First+Middle*+Last
/// fragment sequences. Validates each header's masked CRC.
pub struct LogReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> LogReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut scratch: Vec<u8> = Vec::new();
        let mut in_fragmented = false;
        loop {
            match self.read_physical()? {
                None => {
                    if in_fragmented {
                        return Err(Status::corruption("LogReader: truncated fragmented record"));
                    }
                    return Ok(None);
                }
                Some((kind, fragment)) => {
                    if kind == crate::log::LogRecordHeader::KIND_FULL {
                        if in_fragmented {
                            return Err(Status::corruption(
                                "LogReader: full inside fragmented record",
                            ));
                        }
                        return Ok(Some(fragment));
                    } else if kind == crate::log::LogRecordHeader::KIND_FIRST {
                        if in_fragmented {
                            return Err(Status::corruption(
                                "LogReader: first inside fragmented record",
                            ));
                        }
                        scratch = fragment;
                        in_fragmented = true;
                    } else if kind == crate::log::LogRecordHeader::KIND_MIDDLE {
                        if !in_fragmented {
                            return Err(Status::corruption(
                                "LogReader: middle outside fragmented record",
                            ));
                        }
                        scratch.extend_from_slice(&fragment);
                    } else if kind == crate::log::LogRecordHeader::KIND_LAST {
                        if !in_fragmented {
                            return Err(Status::corruption(
                                "LogReader: last outside fragmented record",
                            ));
                        }
                        scratch.extend_from_slice(&fragment);
                        return Ok(Some(scratch));
                    } else {
                        return Err(Status::corruption(format!(
                            "LogReader: unknown record kind {kind}"
                        )));
                    }
                }
            }
        }
    }

    fn read_physical(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
        loop {
            let block_offset = self.cursor % BLOCK_SIZE;
            let leftover = BLOCK_SIZE - block_offset;
            if leftover < HEADER_SIZE {
                // Skip the zero-pad trailer of the current block.
                self.cursor += leftover;
                continue;
            }
            if self.cursor + HEADER_SIZE > self.bytes.len() {
                return Ok(None);
            }
            let mut header_input: &[u8] = &self.bytes[self.cursor..self.cursor + HEADER_SIZE];
            let header = crate::log::LogRecordHeader::decode_from(&mut header_input)?;
            let len = header.length as usize;
            let avail_in_block = BLOCK_SIZE - block_offset - HEADER_SIZE;
            if len > avail_in_block {
                return Err(Status::corruption(
                    "LogReader: record extends past block end",
                ));
            }
            if self.cursor + HEADER_SIZE + len > self.bytes.len() {
                return Err(Status::corruption("LogReader: truncated payload"));
            }
            let payload = &self.bytes[self.cursor + HEADER_SIZE..self.cursor + HEADER_SIZE + len];
            let actual_crc = crate::crc32c::value(&[header.kind]);
            let actual_crc = crate::crc32c::extend(actual_crc, payload);
            if actual_crc != header.crc {
                return Err(Status::corruption("LogReader: bad record CRC"));
            }
            let payload = payload.to_vec();
            self.cursor += HEADER_SIZE + len;
            if header.kind == crate::log::LogRecordHeader::KIND_ZERO && len == 0 {
                // Pre-allocated file padding; skip.
                continue;
            }
            return Ok(Some((header.kind, payload)));
        }
    }
}

/// SequentialFile-backed log reader used for WAL and MANIFEST replay.
/// It preserves the same record framing and CRC behavior as LogReader
/// without requiring the whole file to be loaded into memory first.
pub struct LogSequentialReader<F: SequentialFile> {
    file: F,
    block: Vec<u8>,
    block_offset: usize,
    eof: bool,
}

impl<F: SequentialFile> LogSequentialReader<F> {
    pub fn new(file: F) -> Self {
        Self {
            file,
            block: Vec::new(),
            block_offset: 0,
            eof: false,
        }
    }

    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut scratch: Vec<u8> = Vec::new();
        let mut in_fragmented = false;
        loop {
            match self.read_physical()? {
                None => {
                    if in_fragmented {
                        return Err(Status::corruption("LogReader: truncated fragmented record"));
                    }
                    return Ok(None);
                }
                Some((kind, fragment)) => {
                    if kind == crate::log::LogRecordHeader::KIND_FULL {
                        if in_fragmented {
                            return Err(Status::corruption(
                                "LogReader: full inside fragmented record",
                            ));
                        }
                        return Ok(Some(fragment));
                    } else if kind == crate::log::LogRecordHeader::KIND_FIRST {
                        if in_fragmented {
                            return Err(Status::corruption(
                                "LogReader: first inside fragmented record",
                            ));
                        }
                        scratch = fragment;
                        in_fragmented = true;
                    } else if kind == crate::log::LogRecordHeader::KIND_MIDDLE {
                        if !in_fragmented {
                            return Err(Status::corruption(
                                "LogReader: middle outside fragmented record",
                            ));
                        }
                        scratch.extend_from_slice(&fragment);
                    } else if kind == crate::log::LogRecordHeader::KIND_LAST {
                        if !in_fragmented {
                            return Err(Status::corruption(
                                "LogReader: last outside fragmented record",
                            ));
                        }
                        scratch.extend_from_slice(&fragment);
                        return Ok(Some(scratch));
                    } else {
                        return Err(Status::corruption(format!(
                            "LogReader: unknown record kind {kind}"
                        )));
                    }
                }
            }
        }
    }

    fn fill_block(&mut self) -> Result<()> {
        if self.eof {
            return Ok(());
        }
        self.block = self.file.read(BLOCK_SIZE)?;
        self.block_offset = 0;
        if self.block.is_empty() {
            self.eof = true;
        }
        Ok(())
    }

    fn read_physical(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
        match self.read_physical_span()? {
            Some((kind, start, end)) => Ok(Some((kind, self.block[start..end].to_vec()))),
            None => Ok(None),
        }
    }

    /// Like `read_physical` but returns the payload as a `(kind, start,
    /// end)` span into `self.block` instead of allocating a `Vec`. The
    /// span is valid only until the next read (which may refill the
    /// block), so the caller must consume it before reading again.
    /// Zero-padding records are skipped internally.
    fn read_physical_span(&mut self) -> Result<Option<(u8, usize, usize)>> {
        loop {
            if self.block_offset >= self.block.len() {
                self.fill_block()?;
                if self.eof {
                    return Ok(None);
                }
            }
            let leftover = self.block.len() - self.block_offset;
            if leftover < HEADER_SIZE {
                if self.block.len() < BLOCK_SIZE {
                    self.eof = true;
                    return Ok(None);
                }
                self.block_offset = self.block.len();
                continue;
            }
            let mut header_input: &[u8] =
                &self.block[self.block_offset..self.block_offset + HEADER_SIZE];
            let header = crate::log::LogRecordHeader::decode_from(&mut header_input)?;
            let len = header.length as usize;
            let avail_in_block = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            if len > avail_in_block {
                return Err(Status::corruption(
                    "LogReader: record extends past block end",
                ));
            }
            if self.block_offset + HEADER_SIZE + len > self.block.len() {
                return Err(Status::corruption("LogReader: truncated payload"));
            }
            let start = self.block_offset + HEADER_SIZE;
            let end = start + len;
            let actual_crc = crate::crc32c::value(&[header.kind]);
            let actual_crc = crate::crc32c::extend(actual_crc, &self.block[start..end]);
            if actual_crc != header.crc {
                return Err(Status::corruption("LogReader: bad record CRC"));
            }
            self.block_offset += HEADER_SIZE + len;
            if header.kind == crate::log::LogRecordHeader::KIND_ZERO && len == 0 {
                continue;
            }
            return Ok(Some((header.kind, start, end)));
        }
    }

    /// Read the next record into `buf` (cleared and reused), returning
    /// `Ok(true)` if a record was read or `Ok(false)` at EOF. Avoids the
    /// per-record `Vec` allocation `read_record` makes - the hot path for
    /// WAL replay, which reads many small records back to back.
    pub fn read_record_into(&mut self, buf: &mut Vec<u8>) -> Result<bool> {
        buf.clear();
        let mut in_fragmented = false;
        loop {
            let Some((kind, start, end)) = self.read_physical_span()? else {
                if in_fragmented {
                    return Err(Status::corruption("LogReader: truncated fragmented record"));
                }
                return Ok(false);
            };
            let fragment = &self.block[start..end];
            if kind == crate::log::LogRecordHeader::KIND_FULL {
                if in_fragmented {
                    return Err(Status::corruption(
                        "LogReader: full inside fragmented record",
                    ));
                }
                buf.extend_from_slice(fragment);
                return Ok(true);
            } else if kind == crate::log::LogRecordHeader::KIND_FIRST {
                if in_fragmented {
                    return Err(Status::corruption(
                        "LogReader: first inside fragmented record",
                    ));
                }
                buf.extend_from_slice(fragment);
                in_fragmented = true;
            } else if kind == crate::log::LogRecordHeader::KIND_MIDDLE {
                if !in_fragmented {
                    return Err(Status::corruption(
                        "LogReader: middle outside fragmented record",
                    ));
                }
                buf.extend_from_slice(fragment);
            } else if kind == crate::log::LogRecordHeader::KIND_LAST {
                if !in_fragmented {
                    return Err(Status::corruption(
                        "LogReader: last outside fragmented record",
                    ));
                }
                buf.extend_from_slice(fragment);
                return Ok(true);
            } else {
                return Err(Status::corruption(format!(
                    "LogReader: unknown record kind {kind}"
                )));
            }
        }
    }
}
