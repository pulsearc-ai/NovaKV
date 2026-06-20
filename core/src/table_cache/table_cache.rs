use std::path::Path;

use crate::cache::{Cache, CacheHandle, ShardedLRUCache};
use crate::comparator::Comparator;
use crate::env::Env;
use crate::filename::{sst_table_file_name, table_file_name};
use crate::filter::FilterPolicy;
use crate::status::Result;
use crate::table::{Compressor, Table, TableIterator};

/// Caches open SSTables by
/// file_number; on miss, opens a random-access file from
/// `env` and reads only the footer/index eagerly.
/// Type alias for the polymorphic block cache shared across
/// every Table opened through a TableCache. Custom Cache
/// impls plug in here via `Options::block_cache`.
pub type BlockCache = std::sync::Arc<dyn Cache<std::sync::Arc<crate::block::Block>> + Send + Sync>;

pub struct TableCache<C: Comparator + Clone + 'static, E: Env> {
    dbname: String,
    env: E,
    comparator: C,
    cache: std::sync::Arc<ShardedLRUCache<Table<C, E::RandomAccess>>>,
    /// Phase 72: shared per-block cache passed to every
    /// Table opened through this TableCache. Phase C: now a
    /// `dyn Cache<Arc<Block>>` so callers can plug in a custom
    /// implementation via `Options::block_cache`.
    block_cache: BlockCache,
    /// Phase B: filter policy passed to every Table opened
    /// through this TableCache. Plumbed in from
    /// `Options::filter_policy` when the Table is opened.
    filter_policy: Option<std::sync::Arc<dyn FilterPolicy + Send + Sync>>,
    /// Phase E: block compressor passed to every Table opened
    /// through this TableCache. Reflects the
    /// `Options::compression` decision plumbed through to the
    /// reader via the trailer kind byte.
    compressor: Option<std::sync::Arc<dyn Compressor>>,
}

impl<C: Comparator + Clone + 'static, E: Env + Clone> Clone for TableCache<C, E> {
    fn clone(&self) -> Self {
        Self {
            dbname: self.dbname.clone(),
            env: self.env.clone(),
            comparator: self.comparator.clone(),
            cache: self.cache.clone(),
            block_cache: self.block_cache.clone(),
            filter_policy: self.filter_policy.clone(),
            compressor: self.compressor.clone(),
        }
    }
}

impl<C: Comparator + Clone + 'static, E: Env> TableCache<C, E> {
    /// `table_capacity` bounds the open-Table cache; `block_cache_size`
    /// bounds the per-block cache shared across all Tables.
    pub fn new(
        dbname: &str,
        env: E,
        comparator: C,
        table_capacity: usize,
        block_cache_size: usize,
    ) -> Self {
        Self::new_with_filter(
            dbname,
            env,
            comparator,
            table_capacity,
            block_cache_size,
            None,
        )
    }

    /// Phase B: construct a TableCache that hands a filter
    /// policy to every newly-opened Table. Backed by a
    /// freshly-allocated default `ShardedLRUCache` of size
    /// `block_cache_size` for block storage.
    pub fn new_with_filter(
        dbname: &str,
        env: E,
        comparator: C,
        table_capacity: usize,
        block_cache_size: usize,
        filter_policy: Option<std::sync::Arc<dyn FilterPolicy + Send + Sync>>,
    ) -> Self {
        let block_cache: BlockCache = std::sync::Arc::new(ShardedLRUCache::new(block_cache_size));
        Self::new_full(
            dbname,
            env,
            comparator,
            table_capacity,
            block_cache,
            filter_policy,
            None,
        )
    }

    /// Phase C: construct a TableCache backed by an externally
    /// supplied block cache. Used when callers want to share
    /// one cache across multiple `DBImpl` instances or plug
    /// in a custom implementation via an explicitly set
    /// `Options::block_cache`.
    pub fn new_with_block_cache(
        dbname: &str,
        env: E,
        comparator: C,
        table_capacity: usize,
        block_cache: BlockCache,
        filter_policy: Option<std::sync::Arc<dyn FilterPolicy + Send + Sync>>,
    ) -> Self {
        Self::new_full(
            dbname,
            env,
            comparator,
            table_capacity,
            block_cache,
            filter_policy,
            None,
        )
    }

    /// Phase E: full constructor accepting all the optional
    /// per-Table state - block cache, filter policy, and
    /// compressor.
    pub fn new_full(
        dbname: &str,
        env: E,
        comparator: C,
        table_capacity: usize,
        block_cache: BlockCache,
        filter_policy: Option<std::sync::Arc<dyn FilterPolicy + Send + Sync>>,
        compressor: Option<std::sync::Arc<dyn Compressor>>,
    ) -> Self {
        Self {
            dbname: dbname.to_string(),
            env,
            comparator,
            cache: std::sync::Arc::new(ShardedLRUCache::new(table_capacity)),
            block_cache,
            filter_policy,
            compressor,
        }
    }

    /// Get a value for `key` from the SST identified by
    /// `file_number`. Always verifies CRCs; for explicit
    /// per-call CRC policy use `get_verify`.
    pub fn get(&self, file_number: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_verify(file_number, key, true)
    }

    /// Like `get`, with an explicit `verify_checksums` flag.
    pub fn get_verify(
        &self,
        file_number: u64,
        key: &[u8],
        verify: bool,
    ) -> Result<Option<Vec<u8>>> {
        self.get_full(file_number, key, verify, true)
    }

    /// Phase F: explicit `verify_checksums` + `fill_cache`.
    pub fn get_full(
        &self,
        file_number: u64,
        key: &[u8],
        verify: bool,
        fill_cache: bool,
    ) -> Result<Option<Vec<u8>>> {
        let handle = self.find_table(file_number)?;
        handle.value().get_full(key, verify, fill_cache)
    }

    /// Internal-key-aware point lookup. `internal_key` is a
    /// full (user_key + tag) byte string. Returns the tristate
    /// `LookupResult` so the caller can distinguish a tombstone
    /// (stop searching lower levels) from a true miss.
    ///
    /// Always verifies CRCs; use `internal_get_verify` for
    /// explicit per-call CRC policy.
    pub fn internal_get(
        &self,
        file_number: u64,
        internal_key: &[u8],
    ) -> Result<crate::version_set::LookupResult> {
        self.internal_get_verify(file_number, internal_key, true)
    }

    /// Like `internal_get`, with an explicit `verify_checksums` flag.
    pub fn internal_get_verify(
        &self,
        file_number: u64,
        internal_key: &[u8],
        verify: bool,
    ) -> Result<crate::version_set::LookupResult> {
        self.internal_get_full(file_number, internal_key, verify, true)
    }

    /// Phase F: explicit `verify_checksums` + `fill_cache`.
    pub fn internal_get_full(
        &self,
        file_number: u64,
        internal_key: &[u8],
        verify: bool,
        fill_cache: bool,
    ) -> Result<crate::version_set::LookupResult> {
        let handle = self.find_table(file_number)?;
        handle
            .value()
            .internal_get_full(internal_key, verify, fill_cache)
    }

    /// Approximate byte offset of `key` within the SST
    /// identified by `file_number`.
    pub fn approximate_offset_of(&self, file_number: u64, key: &[u8]) -> Result<u64> {
        let handle = self.find_table(file_number)?;
        Ok(handle.value().approximate_offset_of(key))
    }

    /// Return a lazy iterator over one cached table.
    pub fn new_iterator_verify(&self, file_number: u64, verify: bool) -> Result<TableIterator<C>> {
        let handle = self.find_table(file_number)?;
        handle.value().clone().new_iterator_verify(verify)
    }

    pub fn evict(&self, file_number: u64) {
        let cache_key = file_number.to_le_bytes();
        self.cache.erase(&cache_key);
    }

    fn find_table(&self, file_number: u64) -> Result<CacheHandle<Table<C, E::RandomAccess>>> {
        let cache_key = file_number.to_le_bytes();
        if let Some(h) = self.cache.lookup(&cache_key) {
            return Ok(h);
        }
        let ldb_path = table_file_name(&self.dbname, file_number);
        let sst_path = sst_table_file_name(&self.dbname, file_number);
        let path = if self.env.file_exists(Path::new(&ldb_path)) {
            ldb_path
        } else {
            sst_path
        };
        let path = Path::new(&path);
        let file_size = self.env.get_file_size(path)?;
        let file = self.env.new_random_access_file(path)?;
        let table = Table::open_random_with_options(
            file,
            file_size,
            self.comparator.clone(),
            Some(self.block_cache.clone()),
            self.filter_policy.clone(),
            self.compressor.clone(),
        )?;
        Ok(self.cache.insert(&cache_key, table, 1))
    }
}
