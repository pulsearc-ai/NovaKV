//! Non-cryptographic 32-bit hash.
//!
//! Used by [`crate::filter`]'s Bloom filter (via `bloom_seed =
//! 0xbc9f1d34`) and by the sharded LRU cache for shard selection.
//! Wire-format-relevant: changing the constants would invalidate
//! every existing filter on disk.
//!
//! # Example
//!
//! ```
//! use novakv::hash::hash;
//! let _h = hash(b"some-key", 0);
//! ```
//!
//! Don't use this for security — it's a fast Murmur-style mixer, not
//! a cryptographic primitive.

mod hash;
pub use hash::*;
