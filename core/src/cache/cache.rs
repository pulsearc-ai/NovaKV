use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const NUM_SHARD_BITS: u32 = 4;
pub const NUM_SHARDS: usize = 1 << NUM_SHARD_BITS;

/// Polymorphic block/value cache. Returned `CacheHandle<V>`s
/// are refcounted via `Arc`; values stay alive as long as
/// any handle holds a reference, even after eviction from
/// the cache.
pub trait Cache<V> {
    fn insert(&self, key: &[u8], value: V, charge: usize) -> CacheHandle<V>;
    fn lookup(&self, key: &[u8]) -> Option<CacheHandle<V>>;
    fn erase(&self, key: &[u8]);
    fn new_id(&self) -> u64;
    fn total_charge(&self) -> usize;
    fn prune(&self);
}

/// Refcounted handle on a cache entry. Cloning creates
/// another reference to the same entry.
#[derive(Debug)]
pub struct CacheHandle<V> {
    entry: Arc<Entry<V>>,
}

impl<V> CacheHandle<V> {
    pub fn value(&self) -> &V {
        &self.entry.value
    }
    pub fn charge(&self) -> usize {
        self.entry.charge
    }

    /// Construct a handle from raw value + charge. Custom
    /// `Cache<V>` implementations use this to wrap their own
    /// values in a refcounted handle the rest of the engine
    /// already understands.
    pub fn new(value: V, charge: usize) -> Self {
        Self {
            entry: Arc::new(Entry { value, charge }),
        }
    }
}

impl<V> Clone for CacheHandle<V> {
    fn clone(&self) -> Self {
        Self {
            entry: self.entry.clone(),
        }
    }
}

#[derive(Debug)]
struct Entry<V> {
    value: V,
    charge: usize,
}

#[derive(Default, Clone, Copy)]
struct FastBuildHasher;

impl std::hash::BuildHasher for FastBuildHasher {
    type Hasher = FastHasher;
    fn build_hasher(&self) -> FastHasher {
        FastHasher { hash: 0 }
    }
}

struct FastHasher {
    hash: u64,
}

impl FastHasher {
    const K: u64 = 0x517c_c1b7_2722_0a95;
    #[inline]
    fn mix(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(Self::K);
    }
}

impl std::hash::Hasher for FastHasher {
    fn finish(&self) -> u64 {
        self.hash
    }
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.mix(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        let rem = chunks.remainder();
        if !rem.is_empty() {
            let mut buf = [0u8; 8];
            buf[..rem.len()].copy_from_slice(rem);
            self.mix(u64::from_le_bytes(buf));
        }
    }
}

#[derive(Debug)]
struct ShardEntry<V> {
    arc: Arc<Entry<V>>,
    last_access: u64,
}

#[derive(Debug)]
struct LruShard<V> {
    entries: HashMap<Vec<u8>, ShardEntry<V>, FastBuildHasher>,
    usage: usize,
    capacity: usize,
    counter: u64,
}

impl<V> LruShard<V> {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::default(),
            usage: 0,
            capacity,
            counter: 0,
        }
    }

    fn insert(&mut self, key: &[u8], value: V, charge: usize) -> Arc<Entry<V>> {
        // If the key already exists, evict it first to keep usage accurate.
        if let Some(old) = self.entries.remove(key) {
            self.usage = self.usage.saturating_sub(old.arc.charge);
        }
        // Evict until we have room.
        while self.usage + charge > self.capacity && !self.entries.is_empty() {
            self.evict_oldest();
        }
        let arc = Arc::new(Entry { value, charge });
        self.counter += 1;
        self.entries.insert(
            key.to_vec(),
            ShardEntry {
                arc: arc.clone(),
                last_access: self.counter,
            },
        );
        self.usage += charge;
        arc
    }

    fn lookup(&mut self, key: &[u8]) -> Option<Arc<Entry<V>>> {
        self.counter += 1;
        let counter = self.counter;
        let entry = self.entries.get_mut(key)?;
        entry.last_access = counter;
        Some(entry.arc.clone())
    }

    fn erase(&mut self, key: &[u8]) {
        if let Some(old) = self.entries.remove(key) {
            self.usage = self.usage.saturating_sub(old.arc.charge);
        }
    }

    fn evict_oldest(&mut self) {
        // Find the entry with the smallest last_access.
        let oldest_key = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.last_access)
            .map(|(k, _)| k.clone());
        if let Some(k) = oldest_key {
            if let Some(old) = self.entries.remove(&k) {
                self.usage = self.usage.saturating_sub(old.arc.charge);
            }
        }
    }

    fn prune(&mut self) {
        // Drop all entries whose Arc has only one strong reference (us).
        self.entries.retain(|_, e| Arc::strong_count(&e.arc) > 1);
        self.usage = self.entries.values().map(|e| e.arc.charge).sum();
    }
}

/// Sharded LRU cache.
#[derive(Debug)]
pub struct ShardedLRUCache<V> {
    shards: Vec<Mutex<LruShard<V>>>,
    next_id: AtomicU64,
}

impl<V> ShardedLRUCache<V> {
    pub fn new(capacity: usize) -> Self {
        // Per-shard capacity rounds up to ensure aggregate >= capacity.
        let per_shard = capacity.div_ceil(NUM_SHARDS);
        let shards = (0..NUM_SHARDS)
            .map(|_| Mutex::new(LruShard::new(per_shard)))
            .collect();
        Self {
            shards,
            next_id: AtomicU64::new(1),
        }
    }

    fn shard_for(&self, key: &[u8]) -> &Mutex<LruShard<V>> {
        let h = crate::hash::hash(key, 0);
        let idx = (h >> (32 - NUM_SHARD_BITS)) as usize;
        &self.shards[idx]
    }
}

impl<V> Cache<V> for ShardedLRUCache<V> {
    fn insert(&self, key: &[u8], value: V, charge: usize) -> CacheHandle<V> {
        let arc = self
            .shard_for(key)
            .lock()
            .unwrap()
            .insert(key, value, charge);
        CacheHandle { entry: arc }
    }

    fn lookup(&self, key: &[u8]) -> Option<CacheHandle<V>> {
        self.shard_for(key)
            .lock()
            .unwrap()
            .lookup(key)
            .map(|arc| CacheHandle { entry: arc })
    }

    fn erase(&self, key: &[u8]) {
        self.shard_for(key).lock().unwrap().erase(key);
    }

    fn new_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn total_charge(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().usage).sum()
    }

    fn prune(&self) {
        for shard in &self.shards {
            shard.lock().unwrap().prune();
        }
    }
}
