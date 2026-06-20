//! Generic refcounted LRU cache.
//!
//! [`Cache<V>`] is a trait with a compact surface
//! (`insert`, `lookup`, `erase`, `new_id`, `total_charge`, `prune`).
//! [`ShardedLRUCache<V>`] is the built-in implementation: 16 shards
//! keyed by [`crate::hash::hash`], each shard a vanilla LRU.
//!
//! # Two consumption patterns
//!
//! 1. **DB block cache** — the canonical use. Pass an
//!    `Arc<dyn Cache<Arc<Block>> + Send + Sync>` via
//!    [`crate::db_impl::Options::block_cache`] to share a cache
//!    across multiple `DBImpl` instances. If `None`, the engine
//!    builds a default `ShardedLRUCache(block_cache_size)`
//!    internally.
//!
//!    ```
//!    use std::sync::Arc;
//!    use novakv::block::Block;
//!    use novakv::prelude::*;
//!
//!    let shared: Arc<ShardedLRUCache<Arc<Block>>> =
//!        Arc::new(ShardedLRUCache::new(64 * 1024 * 1024));
//!
//!    let mut opts_a = Options::default();
//!    opts_a.block_cache = Some(shared.clone());
//!    let db_a = DBImpl::open("/db-a", MemEnv::new(), BytewiseComparator, opts_a).unwrap();
//!
//!    let mut opts_b = Options::default();
//!    opts_b.block_cache = Some(shared.clone());
//!    let db_b = DBImpl::open("/db-b", MemEnv::new(), BytewiseComparator, opts_b).unwrap();
//!
//!    db_a.put(b"a", b"1").unwrap();
//!    db_b.put(b"b", b"2").unwrap();
//!    let _ = (db_a, db_b);
//!    ```
//!
//! 2. **Custom cache impl** — implement [`Cache<V>`] yourself and
//!    return [`CacheHandle::new(value, charge)`] from `insert` /
//!    `lookup`. Useful for scan-resistant caches, custom eviction,
//!    or telemetry wrappers.
//!
//! # Handles are refcounted
//!
//! `CacheHandle<V>` holds an `Arc` of the cached value. The cache
//! can evict the entry from its index, but the value lives until
//! every outstanding handle drops.
//!
//! # `new_id`
//!
//! Returns a unique `u64` per cache. The DB's table cache prepends
//! it to block-cache keys so per-table block IDs don't collide
//! across multiple Tables sharing one cache.

mod cache;
pub use cache::*;
