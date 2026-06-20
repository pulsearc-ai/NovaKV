use crate::filter::FilterPolicy;

pub const FILTER_BASE_LG: usize = 11;
pub const FILTER_BASE: u64 = 1u64 << FILTER_BASE_LG;

/// Builds a filter block from per-data-block key sets.
/// Sequence: `(StartBlock AddKey*)* Finish`.
#[derive(Debug, Clone)]
pub struct FilterBlockBuilder<P: FilterPolicy> {
    policy: P,
    keys: Vec<u8>,
    start: Vec<usize>,
    result: Vec<u8>,
    filter_offsets: Vec<u32>,
}

impl<P: FilterPolicy> FilterBlockBuilder<P> {
    pub fn new(policy: P) -> Self {
        Self {
            policy,
            keys: Vec::new(),
            start: Vec::new(),
            result: Vec::new(),
            filter_offsets: Vec::new(),
        }
    }

    pub fn start_block(&mut self, block_offset: u64) {
        let filter_index = block_offset / FILTER_BASE;
        while filter_index > self.filter_offsets.len() as u64 {
            self.generate_filter();
        }
    }

    pub fn add_key(&mut self, key: &[u8]) {
        self.start.push(self.keys.len());
        self.keys.extend_from_slice(key);
    }

    pub fn finish(&mut self) -> &[u8] {
        if !self.start.is_empty() {
            self.generate_filter();
        }
        let array_offset = self.result.len() as u32;
        for offset in &self.filter_offsets {
            crate::coding::put_fixed32(&mut self.result, *offset);
        }
        crate::coding::put_fixed32(&mut self.result, array_offset);
        self.result.push(FILTER_BASE_LG as u8);
        &self.result
    }

    fn generate_filter(&mut self) {
        let num_keys = self.start.len();
        if num_keys == 0 {
            self.filter_offsets.push(self.result.len() as u32);
            return;
        }
        // Sentinel for the last key's end offset.
        self.start.push(self.keys.len());
        let key_slices: Vec<&[u8]> = (0..num_keys)
            .map(|i| &self.keys[self.start[i]..self.start[i + 1]])
            .collect();
        self.filter_offsets.push(self.result.len() as u32);
        self.policy.create_filter(&key_slices, &mut self.result);
        self.keys.clear();
        self.start.clear();
    }
}

/// Reads a filter block produced by `FilterBlockBuilder`.
/// Borrows the raw filter-block bytes; computes per-block
/// filter location from `block_offset >> base_lg`.
pub struct FilterBlockReader<'a, P: FilterPolicy> {
    policy: P,
    data: &'a [u8],
    offset_array_start: usize,
    num_filters: usize,
    base_lg: u8,
}

impl<'a, P: FilterPolicy> FilterBlockReader<'a, P> {
    pub fn new(policy: P, contents: &'a [u8]) -> Option<Self> {
        let n = contents.len();
        if n < 5 {
            return None;
        }
        let base_lg = contents[n - 1];
        let array_offset = crate::coding::decode_fixed32(&contents[n - 5..n - 1]) as usize;
        if array_offset > n - 5 {
            return None;
        }
        let num_filters = (n - 5 - array_offset) / 4;
        Some(Self {
            policy,
            data: contents,
            offset_array_start: array_offset,
            num_filters,
            base_lg,
        })
    }

    pub fn key_may_match(&self, block_offset: u64, key: &[u8]) -> bool {
        let index = (block_offset >> self.base_lg) as usize;
        if index >= self.num_filters {
            return true; // out of range; don't skip
        }
        let start_pos = self.offset_array_start + index * 4;
        let start = crate::coding::decode_fixed32(&self.data[start_pos..start_pos + 4]) as usize;
        let limit = if index + 1 < self.num_filters {
            let next_pos = self.offset_array_start + (index + 1) * 4;
            crate::coding::decode_fixed32(&self.data[next_pos..next_pos + 4]) as usize
        } else {
            self.offset_array_start
        };
        if start <= limit && limit <= self.offset_array_start {
            if start == limit {
                return false; // empty filter for this block
            }
            let filter = &self.data[start..limit];
            self.policy.key_may_match(key, filter)
        } else {
            true // corrupt offsets; be safe and don't skip
        }
    }
}
