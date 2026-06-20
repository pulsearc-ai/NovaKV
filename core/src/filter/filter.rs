/// Filter policy interface. A filter
/// policy turns a set of keys into a compact byte string
/// that can later answer probabilistic membership queries.
pub trait FilterPolicy {
    fn name(&self) -> &'static str;
    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>);
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

pub const DEFAULT_BITS_PER_KEY: usize = 10;
pub const HASH_SEED: u32 = 0xbc9f1d34;

/// Bloom filter policy.
/// Uses double-hashing (rotate-add) to derive K hash
/// positions from a single seeded hash. K is set to
/// `bits_per_key * ln(2) ~= 0.69 * bits_per_key`,
/// clamped to `[1, 30]`.
#[derive(Debug, Clone, Copy)]
pub struct BloomFilterPolicy {
    bits_per_key: usize,
    k: usize,
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: usize) -> Self {
        // K = bits_per_key * ln(2). Use the constant 0.69.
        let k = (((bits_per_key as f64) * 0.69) as usize).clamp(1, 30);
        Self { bits_per_key, k }
    }

    pub fn bits_per_key(&self) -> usize {
        self.bits_per_key
    }
    pub fn k(&self) -> usize {
        self.k
    }
}

impl Default for BloomFilterPolicy {
    fn default() -> Self {
        Self::new(DEFAULT_BITS_PER_KEY)
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &'static str {
        "novakv.BuiltinBloomFilter2"
    }

    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>) {
        let n = keys.len();
        // Compute bloom-filter bit array length, with a 64-bit floor.
        let mut bits = n * self.bits_per_key;
        if bits < 64 {
            bits = 64;
        }
        let bytes = bits.div_ceil(8);
        bits = bytes * 8;

        let init_size = dst.len();
        dst.resize(init_size + bytes, 0);
        // Trailing byte: number of probes K (so the reader can use it).
        dst.push(self.k as u8);

        for key in keys {
            let mut h = crate::hash::hash(key, HASH_SEED);
            let delta = h.rotate_left(15);
            for _ in 0..self.k {
                let bit_pos = (h as usize) % bits;
                dst[init_size + bit_pos / 8] |= 1u8 << (bit_pos % 8) as u8;
                h = h.wrapping_add(delta);
            }
        }
    }

    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        let len = filter.len();
        if len < 2 {
            return false;
        }
        let array = &filter[..len - 1];
        let k = filter[len - 1] as usize;
        // Reserve K > 30 for future filter encodings; treat as match.
        if k > 30 {
            return true;
        }
        let bits = array.len() * 8;

        let mut h = crate::hash::hash(key, HASH_SEED);
        let delta = h.rotate_left(15);
        for _ in 0..k {
            let bit_pos = (h as usize) % bits;
            if array[bit_pos / 8] & (1u8 << (bit_pos % 8) as u8) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
}

impl<P: FilterPolicy + ?Sized> FilterPolicy for std::sync::Arc<P> {
    fn name(&self) -> &'static str {
        (**self).name()
    }
    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>) {
        (**self).create_filter(keys, dst)
    }
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        (**self).key_may_match(key, filter)
    }
}

impl<P: FilterPolicy + ?Sized> FilterPolicy for Box<P> {
    fn name(&self) -> &'static str {
        (**self).name()
    }
    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>) {
        (**self).create_filter(keys, dst)
    }
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        (**self).key_may_match(key, filter)
    }
}

/// Filter policy adapter for internal keys.
/// Strips the trailing 8-byte tag from each internal key
/// before delegating to the wrapped user `FilterPolicy`.
#[derive(Debug, Clone)]
pub struct InternalFilterPolicy<P: FilterPolicy> {
    pub user: P,
}

impl<P: FilterPolicy> InternalFilterPolicy<P> {
    pub fn new(user: P) -> Self {
        Self { user }
    }
}

impl<P: FilterPolicy> FilterPolicy for InternalFilterPolicy<P> {
    fn name(&self) -> &'static str {
        self.user.name()
    }
    fn create_filter(&self, keys: &[&[u8]], dst: &mut Vec<u8>) {
        // Rewrite each key in place to its
        // user_key prefix (everything before the trailing tag).
        let stripped: Vec<&[u8]> = keys.iter().map(|k| user_key_of(k)).collect();
        self.user.create_filter(&stripped, dst);
    }
    fn key_may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        self.user.key_may_match(user_key_of(key), filter)
    }
}

fn user_key_of(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() >= 8 {
        &internal_key[..internal_key.len() - 8]
    } else {
        internal_key
    }
}
