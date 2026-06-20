//! Internal-key format and LSM-level constants.
//!
//! "Internal key" = `user_key || (sequence << 8 | type)` with the
//! 8-byte tag in little-endian. This is the format every SSTable +
//! WAL stores; the engine converts to/from internal keys on the
//! boundary of public APIs.
//!
//! # Types
//!
//! - [`SequenceNumber`] — `u64` with 56 usable bits (top byte is
//!   reserved). [`MAX_SEQUENCE_NUMBER`] = `(1 << 56) - 1`.
//! - [`ValueType`] — `Value` (1) or `Deletion` (0). Tombstones use
//!   `Deletion`. [`VALUE_TYPE_FOR_SEEK`] is the variant used when
//!   building seek keys (always `Value`).
//! - [`ParsedInternalKey`] — `(user_key, sequence, type)` after
//!   decoding.
//! - [`InternalKey`] — owned encoded internal key.
//! - [`InternalKeyComparator`] — wraps a user [`crate::comparator::Comparator`]
//!   and orders by `(user_key ASC, sequence DESC, type DESC)` so
//!   newest-version-first sweeps work.
//! - [`LookupKey`] — encoded lookup key (memtable hashes against this).
//!
//! # Level-set constants
//!
//! - `NUM_LEVELS = 7`
//! - `L0_COMPACTION_TRIGGER = 4` (size-triggered)
//! - `L0_SLOWDOWN_WRITES_TRIGGER = 8` (back-pressure)
//! - `L0_STOP_WRITES_TRIGGER = 12` (hard stop)
//! - `MAX_MEM_COMPACT_LEVEL = 2` (highest level a flush can land at
//!   directly)
//!
//! These are baked into the compaction picker and write throttle.
//! Don't change them without rebuilding the compaction tests.
//!
//! # Why expose this module
//!
//! Most users won't touch internal keys directly. The module is
//! public for embedders building custom iterators, repair tools, or
//! debug introspection that need to decode tag bytes.

mod dbformat;
pub mod internal_key;
pub use dbformat::*;
