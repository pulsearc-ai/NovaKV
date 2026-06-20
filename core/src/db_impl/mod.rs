//! `DBImpl` — the database handle, plus public option structs.
//!
//! This is the headline module. Open a database, put / get / delete
//! / write batches, run iterators, take snapshots, compact ranges,
//! query properties.
//!
//! # Quickstart
//!
//! ```
//! use novakv::prelude::*;
//!
//! let env = MemEnv::new();
//! let db = DBImpl::open("/db", env, BytewiseComparator, Options::default()).unwrap();
//!
//! db.put(b"hello", b"world").unwrap();
//! assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
//!
//! db.delete(b"hello").unwrap();
//! assert_eq!(db.get(b"hello").unwrap(), None);
//! ```
//!
//! # Concurrency
//!
//! `DBImpl` is `Send + Sync`. Every method takes `&self`; interior
//! mutability is handled via `Arc<(Mutex<DBImplCore>, Condvar)>`.
//! Multiple threads can share an `Arc<DBImpl>` directly — no outer
//! `Mutex` needed.
//!
//! ```
//! use std::sync::Arc;
//! use std::thread;
//! use novakv::prelude::*;
//!
//! let db = Arc::new(
//!     DBImpl::open("/db", MemEnv::new(), BytewiseComparator, Options::default()).unwrap()
//! );
//! let handles: Vec<_> = (0..4).map(|i| {
//!     let db = db.clone();
//!     thread::spawn(move || {
//!         db.put(format!("k{i}").as_bytes(), b"v").unwrap();
//!     })
//! }).collect();
//! for h in handles { h.join().unwrap(); }
//! ```
//!
//! # Batched / atomic writes
//!
//! ```
//! use novakv::prelude::*;
//!
//! let db = DBImpl::open("/db", MemEnv::new(), BytewiseComparator, Options::default()).unwrap();
//! let mut batch = WriteBatch::new();
//! batch.put(b"a", b"1");
//! batch.put(b"b", b"2");
//! batch.delete(b"old");
//! db.write(&batch).unwrap();   // all three or none
//! ```
//!
//! # Iterators
//!
//! ```
//! use novakv::prelude::*;
//!
//! let db = DBImpl::open("/db", MemEnv::new(), BytewiseComparator, Options::default()).unwrap();
//! db.put(b"a", b"1").unwrap();
//! db.put(b"b", b"2").unwrap();
//!
//! let mut iter = db.new_iterator().unwrap();
//! iter.seek_to_first();
//! while iter.valid() {
//!     println!("{:?} => {:?}", iter.key(), iter.value());
//!     iter.next();
//! }
//! iter.status().unwrap();   // surface any read errors
//! ```
//!
//! # Snapshots
//!
//! `get_snapshot` pins a sequence number; reads with that snapshot
//! see the database as of that moment. Drop the `Arc<Snapshot>`
//! (or call `release_snapshot`) when done — compaction can't drop
//! tombstones older than the oldest live snapshot.
//!
//! ```
//! use novakv::prelude::*;
//!
//! let db = DBImpl::open("/db", MemEnv::new(), BytewiseComparator, Options::default()).unwrap();
//! db.put(b"k", b"v1").unwrap();
//! let snap = db.get_snapshot();
//! db.put(b"k", b"v2").unwrap();
//!
//! let mut ropts = ReadOptions::default();
//! ropts.snapshot = Some(snap);
//! assert_eq!(db.get_with_options(&ropts, b"k").unwrap(), Some(b"v1".to_vec()));
//! ```
//!
//! # Options
//!
//! All knobs live on [`Options`]. Common ones:
//!
//! | Field | Purpose |
//! |-------|---------|
//! | `create_if_missing` | open errors if false and DB doesn't exist (default `true` for convenience) |
//! | `error_if_exists` | open errors if true and DB exists |
//! | `paranoid_checks` | force CRC verify on every block read |
//! | `write_buffer_size` | memtable size before flush (4 MB default) |
//! | `max_open_files` | open-Table-cache capacity |
//! | `block_size` | bytes per data block (4096 default) |
//! | `block_restart_interval` | restart points per block (16 default) |
//! | `block_cache_size` | size for the default block cache when `block_cache` is None |
//! | `block_cache` | custom `Arc<dyn Cache<Arc<Block>>>` — share across DBs |
//! | `filter_policy` | `Arc<dyn FilterPolicy>` — Bloom or custom |
//! | `compressor` | `Arc<dyn Compressor>` — Snappy/LZ4/etc. (no built-in) |
//! | `info_log` | optional `Arc<dyn Logger>` for diagnostic messages |
//!
//! # ReadOptions
//!
//! - `snapshot` — pin to a snapshot
//! - `verify_checksums` — force per-call CRC verify (overrides
//!   `paranoid_checks` only if true)
//! - `fill_cache` — set false on big scans to avoid evicting hot
//!   blocks
//!
//! # WriteOptions
//!
//! - `sync` — call `fsync` on the WAL after the write, for
//!   durability against power loss
//!
//! # Properties
//!
//! `db.get_property("novakv.stats")` returns the per-level
//! compaction stats table.
//! `db.get_property("novakv.num-files-at-level<N>")` returns the
//! file count at level N. `db.get_property("novakv.sstables")`
//! returns a per-file listing.
//!
//! # Background work
//!
//! Flush + compaction run on a single background thread spawned at
//! `DBImpl::open`. The thread joins on `Drop`. Errors in the bg
//! worker are stashed in `bg_error` and surfaced on the next
//! `get` / `put` / `new_iterator`.
//!
//! # Logger
//!
//! [`Logger`] is a `fn log(&self, message: &str)` trait. Currently
//! accepted via `Options::info_log` but no internal call sites emit
//! messages yet — the field is forward-compat scaffolding.

mod db_impl;
pub use db_impl::*;
