//! On-disk SSTable: builder, reader, and the [`Compressor`] trait.
//!
//! An SST file's layout (top to bottom):
//!
//! ```text
//! [data block 0]
//! [data block 1]
//! ...
//! [filter block]   ← optional, present when filter_policy was set
//! [metaindex block] ("filter.<policy.name()>" → filter_handle)
//! [index block]    (last_key_of_block → block_handle)
//! [footer]         (metaindex_handle, index_handle, magic)
//! ```
//!
//! Each block is followed by a 5-byte trailer (1-byte compression
//! kind + 4-byte CRC32C). Block-handle = `(offset, size)` varint
//! pair.
//!
//! # Public types
//!
//! - [`Table`] — opened reader; `get`, `internal_get`, iterator
//! - [`TableBuilder`] — in-memory writer (collects bytes in a `Vec`)
//! - [`TableFileBuilder`] — streaming writer to a [`crate::env::WritableFile`]
//! - [`TableIterator`] — owned lazy iterator over a Table
//! - [`Compressor`] — pluggable block-compression trait
//! - [`VecRandomAccessFile`] — convenience wrapper over `Vec<u8>`
//!   implementing [`crate::env::RandomAccessFile`]
//!
//! # Building an SST in memory
//!
//! ```
//! use novakv::table::{TableBuilder, Table};
//! use novakv::prelude::*;
//!
//! let mut b = TableBuilder::with_defaults(BytewiseComparator);
//! b.add(b"alpha", b"1");
//! b.add(b"bravo", b"2");
//! b.finish().unwrap();
//! let bytes: Vec<u8> = b.contents().to_vec();
//!
//! // Read it back.
//! let table = Table::open(bytes, BytewiseComparator).unwrap();
//! assert_eq!(table.get(b"alpha").unwrap(), Some(b"1".to_vec()));
//! assert_eq!(table.get(b"missing").unwrap(), None);
//! ```
//!
//! # Pluggable compression
//!
//! [`Compressor`] is the pluggable block-compression hook:
//!
//! ```
//! use std::sync::Arc;
//! use novakv::prelude::*;
//!
//! /// Identity "compressor" — round-trips bytes unchanged. A real
//! /// impl would call into a snappy/lz4/zstd crate here.
//! #[derive(Debug)]
//! struct Identity;
//! impl Compressor for Identity {
//!     fn kind(&self) -> u8 { 1 }
//!     fn compress(&self, input: &[u8]) -> Option<Vec<u8>> { Some(input.to_vec()) }
//!     fn decompress(&self, input: &[u8]) -> Result<Vec<u8>> { Ok(input.to_vec()) }
//! }
//!
//! let mut opts = Options::default();
//! opts.compressor = Some(Arc::new(Identity));
//! let db = DBImpl::open("/db", MemEnv::new(), BytewiseComparator, opts).unwrap();
//! db.put(b"k", b"v").unwrap();
//! assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
//! ```
//!
//! A built-in [`crate::snappy::SnappyCompressor`] is available
//! behind the `snappy` Cargo feature; it pulls in the pure-Rust
//! `snap` crate. The default build stays dep-light — users who
//! don't need Snappy don't pay for it.
//!
//! # Block-cache + filter integration
//!
//! `Table::open_random_with_options(file, size, cmp, block_cache,
//! filter_policy, compressor)` accepts all three optional knobs.
//! The simpler `Table::open` is a Vec-backed shortcut for tests
//! and tools.
//!
//! # Tuning knobs (Options-driven)
//!
//! - [`crate::db_impl::Options::block_size`] (default 4096) — bytes
//!   per data block before flushing to the file
//! - [`crate::db_impl::Options::block_restart_interval`] (default
//!   16) — restart points per block

mod table;
mod table_footer;
pub use table::*;
pub use table_footer::*;
