use std::sync::Arc;

use crate::comparator::Comparator;
use crate::status::{Result, Status};

pub const DEFAULT_RESTART_INTERVAL: usize = 16;

/// Owns the parsed block bytes plus the location/length
/// of the trailing restart array.
#[derive(Debug, Clone)]
pub struct Block {
    bytes: Vec<u8>,
    restart_offset: usize,
    num_restarts: u32,
}

impl Block {
    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(Status::corruption("Block: too small for restart count"));
        }
        let num_restarts = crate::coding::decode_fixed32(&bytes[bytes.len() - 4..]);
        let max_restarts = (bytes.len() - 4) / 4;
        if num_restarts as usize > max_restarts {
            return Err(Status::corruption("Block: bad restart count"));
        }
        let restart_offset = bytes.len() - 4 - num_restarts as usize * 4;
        Ok(Self {
            bytes,
            restart_offset,
            num_restarts,
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }
    pub fn restart_offset(&self) -> usize {
        self.restart_offset
    }
    pub fn size(&self) -> usize {
        self.bytes.len()
    }
}

/// Accumulates prefix-compressed entries with periodic
/// restart points.
#[derive(Debug, Clone)]
pub struct BlockBuilder<C: Comparator> {
    comparator: C,
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    finished: bool,
    last_key: Vec<u8>,
    block_restart_interval: usize,
}

impl<C: Comparator> BlockBuilder<C> {
    pub fn new(comparator: C, restart_interval: usize) -> Self {
        assert!(restart_interval >= 1);
        Self {
            comparator,
            buffer: Vec::new(),
            restarts: vec![0],
            counter: 0,
            finished: false,
            last_key: Vec::new(),
            block_restart_interval: restart_interval,
        }
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.finished = false;
        self.last_key.clear();
    }

    pub fn empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.finished);
        assert!(self.counter <= self.block_restart_interval);
        assert!(
            self.buffer.is_empty() || self.comparator.compare(key, &self.last_key).is_gt(),
            "BlockBuilder keys must be added in increasing order"
        );

        let shared = if self.counter < self.block_restart_interval {
            let min_len = self.last_key.len().min(key.len());
            let mut s = 0usize;
            while s < min_len && self.last_key[s] == key[s] {
                s += 1;
            }
            s
        } else {
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
            0
        };
        let unshared = key.len() - shared;

        crate::block::BlockEntry::encode_streaming(
            &mut self.buffer,
            shared as u32,
            unshared as u32,
            value.len() as u32,
            &key[shared..],
            value,
        );

        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(&key[shared..]);
        self.counter += 1;
    }

    pub fn finish(&mut self) -> &[u8] {
        for r in &self.restarts {
            crate::coding::put_fixed32(&mut self.buffer, *r);
        }
        crate::coding::put_fixed32(&mut self.buffer, self.restarts.len() as u32);
        self.finished = true;
        &self.buffer
    }
}

/// Streaming iterator over a Block. State machine over
/// compressed
/// entries with restart-array binary search for `seek` and
/// restart-walk for `prev`.
enum BlockSource<'a> {
    Borrowed(&'a Block),
    Owned(Arc<Block>),
}

impl<'a> BlockSource<'a> {
    fn block(&self) -> &Block {
        match self {
            BlockSource::Borrowed(block) => block,
            BlockSource::Owned(block) => block.as_ref(),
        }
    }
}

/// Running-key buffer for `BlockIter` with small-buffer
/// optimization: a key up to `KEY_INLINE_CAP` bytes lives inline
/// in the struct; a longer key spills to the heap. A `BlockIter`
/// that only ever sees short keys never allocates.
const KEY_INLINE_CAP: usize = 48;

#[derive(Debug)]
enum KeyBuf {
    Inline {
        data: [u8; KEY_INLINE_CAP],
        len: usize,
    },
    Heap(Vec<u8>),
}

impl KeyBuf {
    fn new() -> Self {
        KeyBuf::Inline {
            data: [0u8; KEY_INLINE_CAP],
            len: 0,
        }
    }

    /// Resets to empty. A heap buffer keeps its capacity so a
    /// long-key iterator does not re-allocate on every seek.
    fn clear(&mut self) {
        match self {
            KeyBuf::Inline { len, .. } => *len = 0,
            KeyBuf::Heap(v) => v.clear(),
        }
    }

    fn truncate(&mut self, n: usize) {
        match self {
            KeyBuf::Inline { len, .. } => {
                if n < *len {
                    *len = n;
                }
            }
            KeyBuf::Heap(v) => v.truncate(n),
        }
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) {
        match self {
            KeyBuf::Inline { data, len } => {
                let new_len = *len + bytes.len();
                if new_len <= KEY_INLINE_CAP {
                    data[*len..new_len].copy_from_slice(bytes);
                    *len = new_len;
                } else {
                    // Spill: move the inline bytes plus the new
                    // bytes into a heap vector and switch variant.
                    let mut v = Vec::with_capacity(new_len);
                    v.extend_from_slice(&data[..*len]);
                    v.extend_from_slice(bytes);
                    *self = KeyBuf::Heap(v);
                }
            }
            KeyBuf::Heap(v) => v.extend_from_slice(bytes),
        }
    }
}

impl std::ops::Deref for KeyBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            KeyBuf::Inline { data, len } => &data[..*len],
            KeyBuf::Heap(v) => v.as_slice(),
        }
    }
}

pub struct BlockIter<'a, C: Comparator> {
    block: BlockSource<'a>,
    comparator: C,
    /// Offset into block.bytes where the current entry starts.
    /// Equal to block.restart_offset when invalid (past-the-end).
    current_offset: usize,
    /// Offset where the next entry starts (or past-the-end).
    next_entry_offset: usize,
    /// Restart index of (or just before) the current entry.
    restart_index: u32,
    /// Reconstructed full key for the current entry.
    key: KeyBuf,
    value_offset: usize,
    value_len: usize,
    valid: bool,
    status: Result<()>,
}

impl<'a, C: Comparator> BlockIter<'a, C> {
    pub fn new(block: &'a Block, comparator: C) -> Self {
        Self::from_source(BlockSource::Borrowed(block), comparator)
    }

    pub fn from_owned(block: Block, comparator: C) -> BlockIter<'static, C> {
        BlockIter::from_source(BlockSource::Owned(Arc::new(block)), comparator)
    }

    pub fn from_shared(block: Arc<Block>, comparator: C) -> BlockIter<'static, C> {
        BlockIter::from_source(BlockSource::Owned(block), comparator)
    }

    fn from_source(block: BlockSource<'a>, comparator: C) -> Self {
        let restart_offset = block.block().restart_offset;
        let num_restarts = block.block().num_restarts;
        Self {
            block,
            comparator,
            current_offset: restart_offset,
            next_entry_offset: restart_offset,
            restart_index: num_restarts,
            key: KeyBuf::new(),
            value_offset: 0,
            value_len: 0,
            valid: false,
            status: Ok(()),
        }
    }

    fn block(&self) -> &Block {
        self.block.block()
    }

    fn read_restart(&self, idx: u32) -> u32 {
        let block = self.block();
        let offset = block.restart_offset + idx as usize * 4;
        crate::coding::decode_fixed32(&block.bytes[offset..offset + 4])
    }

    fn set_corruption(&mut self, msg: &str) {
        self.valid = false;
        let restart_offset = self.block().restart_offset;
        self.current_offset = restart_offset;
        self.next_entry_offset = restart_offset;
        self.status = Err(Status::corruption(format!("Block: {msg}")));
    }

    fn parse_next_entry(&mut self) -> bool {
        let restart_offset = self.block().restart_offset;
        if self.next_entry_offset >= restart_offset {
            self.valid = false;
            self.current_offset = restart_offset;
            self.next_entry_offset = restart_offset;
            return false;
        }
        let mut p: &[u8] = &self.block().bytes[self.next_entry_offset..restart_offset];
        let start_len = p.len();
        let shared = match crate::coding::get_varint32(&mut p) {
            Some(x) => x as usize,
            None => {
                self.set_corruption("bad shared");
                return false;
            }
        };
        let unshared = match crate::coding::get_varint32(&mut p) {
            Some(x) => x as usize,
            None => {
                self.set_corruption("bad unshared");
                return false;
            }
        };
        let value_len = match crate::coding::get_varint32(&mut p) {
            Some(x) => x as usize,
            None => {
                self.set_corruption("bad value_len");
                return false;
            }
        };
        if p.len() < unshared + value_len {
            self.set_corruption("truncated entry");
            return false;
        }
        if shared > self.key.len() {
            self.set_corruption("shared > prev key len");
            return false;
        }
        let header_consumed = start_len - p.len();
        let key_pos = self.next_entry_offset + header_consumed;
        let value_pos = key_pos + unshared;

        self.current_offset = self.next_entry_offset;
        self.key.truncate(shared);
        let key_delta = unsafe {
            let block = self.block();
            std::slice::from_raw_parts(block.bytes.as_ptr().add(key_pos), unshared)
        };
        self.key.extend_from_slice(key_delta);
        self.value_offset = value_pos;
        self.value_len = value_len;
        self.next_entry_offset = value_pos + value_len;
        self.valid = true;
        true
    }

    fn seek_to_restart_point(&mut self, idx: u32) {
        self.key.clear();
        self.restart_index = idx;
        let offset = self.read_restart(idx) as usize;
        self.next_entry_offset = offset;
        self.current_offset = offset;
        self.valid = false;
    }
}

impl<'a, C: Comparator> crate::db_iter::DbIterator for BlockIter<'a, C> {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        if self.block().num_restarts == 0 {
            self.valid = false;
            return;
        }
        self.seek_to_restart_point(0);
        self.parse_next_entry();
    }

    fn seek_to_last(&mut self) {
        if self.block().num_restarts == 0 {
            self.valid = false;
            return;
        }
        self.seek_to_restart_point(self.block().num_restarts - 1);
        while self.parse_next_entry() && self.next_entry_offset < self.block().restart_offset {}
    }

    fn seek(&mut self, target: &[u8]) {
        if self.block().num_restarts == 0 {
            self.valid = false;
            return;
        }
        let mut left = 0u32;
        let mut right = self.block().num_restarts - 1;
        while left < right {
            let mid = left + (right - left).div_ceil(2);
            let restart_offset = self.read_restart(mid) as usize;
            let mut p: &[u8] = &self.block().bytes[restart_offset..self.block().restart_offset];
            let _shared = crate::coding::get_varint32(&mut p);
            let unshared = match crate::coding::get_varint32(&mut p) {
                Some(x) => x as usize,
                None => {
                    self.set_corruption("bad seek unshared");
                    return;
                }
            };
            let _value_len = crate::coding::get_varint32(&mut p);
            if p.len() < unshared {
                self.set_corruption("truncated seek");
                return;
            }
            let mid_key = &p[..unshared];
            if self.comparator.compare(mid_key, target).is_lt() {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        self.seek_to_restart_point(left);
        while self.parse_next_entry() {
            if self.comparator.compare(&self.key, target).is_ge() {
                return;
            }
        }
    }

    fn next(&mut self) {
        self.parse_next_entry();
        while self.restart_index + 1 < self.block().num_restarts
            && self.read_restart(self.restart_index + 1) as usize <= self.current_offset
        {
            self.restart_index += 1;
        }
    }

    fn prev(&mut self) {
        let original_offset = self.current_offset;
        while self.read_restart(self.restart_index) as usize >= original_offset {
            if self.restart_index == 0 {
                self.valid = false;
                let restart_offset = self.block().restart_offset;
                self.current_offset = restart_offset;
                self.next_entry_offset = restart_offset;
                return;
            }
            self.restart_index -= 1;
        }
        self.seek_to_restart_point(self.restart_index);
        loop {
            if !self.parse_next_entry() {
                return;
            }
            if self.next_entry_offset >= original_offset {
                return;
            }
        }
    }

    fn key(&self) -> &[u8] {
        &self.key
    }
    fn value(&self) -> &[u8] {
        &self.block().bytes[self.value_offset..self.value_offset + self.value_len]
    }
    fn status(&self) -> Result<()> {
        self.status.clone()
    }
}
