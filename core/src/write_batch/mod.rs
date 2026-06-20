//! Atomic batched writes.
//!
//! [`WriteBatch`] is the only way to apply multiple mutations
//! atomically. Even a single `db.put(k, v)` is internally a
//! one-record batch — the public `put` / `delete` are convenience
//! wrappers around `db.write(&batch)`.
//!
//! # Building a batch
//!
//! ```
//! use novakv::prelude::*;
//!
//! let env = MemEnv::new();
//! let db = DBImpl::open("/db", env, BytewiseComparator, Options::default()).unwrap();
//!
//! let mut batch = WriteBatch::new();
//! batch.put(b"a", b"1");
//! batch.put(b"b", b"2");
//! batch.delete(b"old-key");
//! db.write(&batch).unwrap();
//! assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
//! ```
//!
//! All three operations land or none do — the batch is applied to
//! the WAL + memtable as a single atomic unit.
//!
//! # Inspecting a batch
//!
//! Implement [`WriteBatchHandler`] (alias [`Handler`]) to walk a
//! batch's records:
//!
//! ```
//! use novakv::prelude::*;
//!
//! struct CountingHandler { puts: usize, dels: usize }
//! impl WriteBatchHandler for CountingHandler {
//!     fn put(&mut self, _key: &[u8], _value: &[u8]) { self.puts += 1; }
//!     fn delete(&mut self, _key: &[u8]) { self.dels += 1; }
//! }
//!
//! let mut batch = WriteBatch::new();
//! batch.put(b"a", b"1");
//! batch.put(b"b", b"2");
//! batch.delete(b"c");
//!
//! let mut h = CountingHandler { puts: 0, dels: 0 };
//! batch.iterate(&mut h).unwrap();
//! assert_eq!((h.puts, h.dels), (2, 1));
//! ```
//!
//! Or get a typed `Vec<WriteBatchRecord>` via `batch.records()`.
//!
//! # On-disk format
//!
//! Header: 8-byte sequence + 4-byte little-endian count.
//! Records: tag byte (`0 = Deletion`, `1 = Value`) + length-prefixed
//! key (+ length-prefixed value for `Value` records).
//!
//! # Capacity / size
//!
//! `batch.approximate_size()` returns the on-disk byte length; useful
//! for size-capped batching (stop appending once you hit a target).
//! `batch.append(&other)` concatenates without re-encoding.

pub mod write_batch;
mod write_batch_header;
mod write_batch_record;
mod write_batch_wrapper;
pub use write_batch_header::*;
pub use write_batch_record::*;
pub use write_batch_wrapper::*;
