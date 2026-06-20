//! In-memory write buffer.
//!
//! The memtable wraps a [`crate::skiplist::SkipList`] keyed on
//! internal keys and ordered by
//! [`crate::format::InternalKeyComparator`]. Every write flows
//! through the WAL → memtable; reads consult mem + imm + SSTs in
//! that order.
//!
//! When `approximate_memory_usage()` exceeds
//! [`crate::db_impl::Options::write_buffer_size`] (default 4 MB),
//! the engine swaps `mem → imm`, allocates a new WAL, and the
//! background worker flushes `imm` to a fresh L0 SST.
//!
//! Public mostly for embedders building custom write paths.

mod memtable;
pub use memtable::*;
