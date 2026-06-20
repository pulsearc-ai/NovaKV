//! Probabilistic key-presence filter.
//!
//! A [`FilterPolicy`] hashes a set of keys into a compact byte
//! string that can later answer "is this key *definitely absent*?"
//! False positives are allowed, false negatives are not. The engine
//! consults filters in [`crate::table::Table::get`] /
//! `internal_get` to short-circuit point lookups for absent keys —
//! turning a block read into a CPU-only filter probe.
//!
//! # Built-in: Bloom filter
//!
//! [`BloomFilterPolicy`] is the default. Its wire name
//! (`"novakv.BuiltinBloomFilter2"`) is stored in every SST's
//! metaindex; readers match on it.
//!
//! ```
//! use std::sync::Arc;
//! use novakv::prelude::*;
//!
//! let env = MemEnv::new();
//! let mut opts = Options::default();
//! opts.filter_policy = Some(Arc::new(BloomFilterPolicy::new(10)));
//! let db = DBImpl::open("/db", env, BytewiseComparator, opts).unwrap();
//! db.put(b"k", b"v").unwrap();
//! assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
//! ```
//!
//! Bits-per-key knob: `10` is the default (≈1% false positive
//! rate). Higher = larger filters but fewer false probes.
//!
//! # Custom filter policy
//!
//! ```
//! use novakv::prelude::*;
//!
//! #[derive(Debug)]
//! struct AlwaysMatch;
//!
//! impl FilterPolicy for AlwaysMatch {
//!     fn name(&self) -> &'static str { "myapp.AlwaysMatch" }
//!     fn create_filter(&self, _keys: &[&[u8]], _dst: &mut Vec<u8>) {}
//!     fn key_may_match(&self, _key: &[u8], _filter: &[u8]) -> bool { true }
//! }
//!
//! let p = AlwaysMatch;
//! assert_eq!(p.name(), "myapp.AlwaysMatch");
//! assert!(p.key_may_match(b"any", b""));
//! ```
//!
//! The trait is object-safe; `Arc<dyn FilterPolicy + Send + Sync>`
//! is the install path on `Options`.
//!
//! # Internal-key wrapping
//!
//! At Open time the engine wraps the user policy in
//! `InternalFilterPolicy`, which strips the 8-byte tag from each
//! key before delegating. This is invisible to callers — your
//! `name()` and `key_may_match()` see clean user keys.

mod filter;
pub use filter::*;
