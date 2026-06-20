//! K-way merge over multiple sorted iterators.
//!
//! [`MergingIterator`] takes a `Vec<Box<dyn DbIterator>>` and
//! presents the merged stream in [`crate::comparator::Comparator`]
//! order. The engine uses it on the read path
//! (memtable + imm + L0 SSTs + L1+ tables) and during compaction.
//!
//! Public for callers building ad-hoc merge views over heterogeneous
//! iterator sources.

mod merging_iter;
pub use merging_iter::*;
