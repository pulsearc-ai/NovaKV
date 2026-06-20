//! Open-Table cache.
//!
//! [`TableCache`] memoizes opened [`crate::table::Table`] handles
//! keyed by file number. The first lookup for a given file number
//! opens the SST and reads its footer + index; subsequent lookups
//! hit the cache. Entries evict via LRU under
//! [`crate::db_impl::Options::max_open_files`].
//!
//! Per-block data caching is layered on top: every Table opened
//! through this cache shares the same
//! [`crate::cache::Cache`]`<Vec<u8>>` for its data blocks.
//!
//! # Why expose this
//!
//! Internal plumbing under [`crate::db_impl::DBImpl`]. Public
//! surface is useful for embedders building custom DB shells that
//! want to reuse the SST-open caching behavior with their own
//! version-set logic.

mod table_cache;
pub use table_cache::*;
