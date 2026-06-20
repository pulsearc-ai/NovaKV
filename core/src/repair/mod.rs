//! DB repair utility.
//!
//! [`repair_db`] walks a damaged DB directory and reconstructs a
//! valid manifest from whatever SSTables and WAL fragments it can
//! salvage. Use when a DB won't open due to a missing or corrupt
//! manifest.
//!
//! ```
//! use novakv::prelude::*;
//!
//! let env = MemEnv::new();
//! // Create then close a DB so the directory exists.
//! {
//!     let _db = DBImpl::open(
//!         "/db", env.clone_handle(), BytewiseComparator, Options::default(),
//!     ).unwrap();
//! }
//! // Repair walks the directory and reconstructs the manifest.
//! let _report: RepairReport = repair_db("/db", env, BytewiseComparator).unwrap();
//! ```
//!
//! [`RepairReport`] tells you how many tables were recovered vs
//! moved to `lost/` (unreadable). The DB can then be reopened
//! normally.
//!
//! # Honoring options
//!
//! [`repair_db`] uses `Options::default()` — fine if the original
//! DB used no filter policy or compressor. If the damaged DB was
//! built with Bloom filters or Snappy compression, use
//! [`repair_db_with_options`] so the rebuilt SSTs preserve those
//! features:
//!
//! ```ignore
//! use std::sync::Arc;
//! use novakv::prelude::*;
//!
//! let env = MemEnv::new();
//! let mut opts = Options::default();
//! opts.filter_policy = Some(Arc::new(BloomFilterPolicy::new(10)));
//! let report = repair_db_with_options("/db", env, BytewiseComparator, opts).unwrap();
//! ```
//!
//! # Caveats
//!
//! - Repair is best-effort, not transactional. Some entries from
//!   the live memtable may be lost if the WAL tail is corrupt.

mod repair;
pub use repair::*;
