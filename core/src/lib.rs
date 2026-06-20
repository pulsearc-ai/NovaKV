//! `novakv` — an embedded, ordered key-value store. Crate root.
//!
//! The headline entry point is [`db_impl::DBImpl`] — open a DB, run
//! `put` / `get` / `delete` / `write` / `new_iterator` /
//! `get_snapshot`. Most callers only need [`prelude`].
//!
//! # Layout
//!
//! Each public module is a focused layer of the storage engine. Every
//! module's `//!` doc explains what's there and when to reach for it.
//!
//! | Module | What's in it |
//! |--------|--------------|
//! | [`coding`] | varint + fixed-LE primitives |
//! | [`status`] | `Status`, `Code`, `Result` |
//! | [`hash`] | non-cryptographic 32-bit hash |
//! | [`crc32c`] | CRC32C + masked variant |
//! | [`comparator`] | `Comparator` trait + `BytewiseComparator` |
//! | [`format`](mod@format) | internal-key format, level constants |
//! | [`write_batch`] | `WriteBatch`, `WriteBatchHandler` |
//! | [`block`] | data-block builder + reader |
//! | [`filter`] | `FilterPolicy` + `BloomFilterPolicy` |
//! | [`filter_block`] | per-block filter index |
//! | [`two_level_iter`] | index-of-blocks iterator |
//! | [`merging_iter`] | k-way merge iterator |
//! | [`table`] | SSTable builder + reader + `Compressor` |
//! | [`log`] | WAL writer + reader |
//! | [`skiplist`] | lock-free skiplist |
//! | [`memtable`] | in-memory write buffer |
//! | [`filename`] | DB-directory file naming |
//! | [`env`](mod@env) | filesystem trait + `StdEnv` + `MemEnv` |
//! | [`cache`] | `Cache` trait + `ShardedLRUCache` |
//! | [`table_cache`] | open-Table cache |
//! | [`version_set`] | LSM level metadata + manifest |
//! | [`db_impl`] | `DBImpl`, `Options`, `ReadOptions`, `WriteOptions` |
//! | [`db_iter`] | snapshot-aware iterator |
//! | [`repair`] | reconstruct a damaged DB |
//! | [`destroy`] | delete a DB directory |
//! | [`slice`](mod@slice) | `(ptr, len)` byte view |
//!
//! # Quickstart
//!
//! ```
//! use novakv::prelude::*;
//!
//! let env = MemEnv::new();
//! let db = DBImpl::open("/db", env, BytewiseComparator, Options::default()).unwrap();
//! db.put(b"hello", b"world").unwrap();
//! assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
//! ```
//!
//! See [`db_impl`] for the full API surface and tuning knobs.

// Each engine layer is a directory module whose primary file shares the
// directory name (`block/block.rs`); this layout is intentional.
#![allow(clippy::module_inception)]

pub mod block;
pub mod cache;
pub mod coding;
pub mod comparator;
pub mod crc32c;
pub mod db_impl;
pub mod db_iter;
pub mod destroy;
pub mod env;
pub mod filename;
pub mod filter;
pub mod filter_block;
pub mod format;
pub mod hash;
pub mod log;
pub mod memtable;
pub mod merging_iter;
pub mod repair;
pub mod skiplist;
pub mod slice;
pub mod status;
pub mod table;
pub mod table_cache;
pub mod two_level_iter;
pub mod version_set;
pub mod write_batch;

/// Built-in Snappy block compressor. Available behind the `snappy`
/// Cargo feature.
#[cfg(feature = "snappy")]
pub mod snappy;

pub use db_iter::DbIterator;

pub use comparator::{BytewiseComparator, Comparator};
pub use format::{
    InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey, SequenceNumber, ValueType,
    MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK,
};
pub use status::{Code, Result, Status};
pub use write_batch::{WriteBatch, WriteBatchHandler, WriteBatchRecord};

/// Public-API surface. An embedder typically only needs the items in
/// this prelude — concrete types for opening / using / closing a
/// database, plus the trait that abstracts over `DBImpl`.
///
/// ```
/// use novakv::prelude::*;
/// // brings DBImpl, Options, ReadOptions, WriteOptions, Snapshot,
/// // BytewiseComparator, Comparator, Env, MemEnv, StdEnv, Status,
/// // Result, Code, Cache, CacheHandle, ShardedLRUCache, Compressor,
/// // FilterPolicy, BloomFilterPolicy, Logger, WriteBatch, ...
/// let _ = MemEnv::new();
/// ```
pub mod prelude {
    pub use crate::cache::{Cache, CacheHandle, ShardedLRUCache};
    pub use crate::comparator::{BytewiseComparator, Comparator};
    pub use crate::db_impl::{
        DBImpl, Logger, Options, Range, ReadOptions, Snapshot, WriteOptions, DB,
    };
    pub use crate::db_iter::DBIter;
    pub use crate::destroy::destroy_db;
    pub use crate::env::{Env, MemEnv, StdEnv, SyncMode};
    pub use crate::filter::{BloomFilterPolicy, FilterPolicy};
    pub use crate::repair::{repair_db, repair_db_with_options, RepairReport};
    pub use crate::slice::Slice;
    pub use crate::status::{Code, Result, Status};
    pub use crate::table::Compressor;
    pub use crate::write_batch::{Handler, WriteBatch, WriteBatchHandler};
    pub use crate::DbIterator;

    /// Built-in Snappy compressor — re-exported here when the
    /// `snappy` feature is enabled so users don't need a separate
    /// `use novakv::snappy::SnappyCompressor` import.
    #[cfg(feature = "snappy")]
    pub use crate::snappy::SnappyCompressor;
}
