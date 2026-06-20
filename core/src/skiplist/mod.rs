//! Lock-free reads, single-writer skiplist.
//!
//! Backing data structure for the memtable. Reads can race writes
//! safely (atomic pointer chases on next-pointers); writes assume
//! the caller serializes them via the outer `DBImpl` mutex.
//!
//! Public for embedders building custom in-memory indexes; most
//! users only see it transitively via [`crate::memtable::MemTable`].

mod skiplist;
pub use skiplist::*;
