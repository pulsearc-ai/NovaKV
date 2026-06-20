//! Prefix-compressed key-value block.
//!
//! A "block" is the fundamental on-disk unit of a SSTable: a
//! contiguous run of sorted entries with shared key prefixes
//! compressed away, plus a trailing array of *restart points* that
//! anchors random seeks within the block.
//!
//! # Public types
//!
//! - [`BlockBuilder`] — encodes entries; used by [`crate::table::TableBuilder`]
//! - [`Block`] — owns the parsed bytes
//! - [`BlockIter`] — iterator over a `Block`'s entries (impl
//!   [`crate::DbIterator`])
//!
//! # Why surface this
//!
//! Most users only touch tables via [`crate::db_impl::DBImpl`] —
//! they never construct a block directly. The module is public for:
//!
//! - Custom storage engines reusing the block format
//! - Diagnostic tools that need to read raw blocks
//! - Tests that want to assert specific on-disk layouts
//!
//! # Example: round-trip a block in memory
//!
//! ```
//! use novakv::block::{BlockBuilder, Block, BlockIter};
//! use novakv::prelude::*;
//! use novakv::DbIterator;
//!
//! let mut b = BlockBuilder::new(BytewiseComparator, 16);
//! b.add(b"alpha", b"1");
//! b.add(b"bravo", b"2");
//! b.add(b"charlie", b"3");
//! let bytes = b.finish().to_vec();
//!
//! let block = Block::new(bytes).unwrap();
//! let mut it = BlockIter::new(&block, BytewiseComparator);
//! it.seek_to_first();
//! let mut keys = Vec::new();
//! while it.valid() {
//!     keys.push(it.key().to_vec());
//!     it.next();
//! }
//! assert_eq!(keys, vec![b"alpha".to_vec(), b"bravo".to_vec(), b"charlie".to_vec()]);
//! ```
//!
//! # Restart interval tuning
//!
//! `BlockBuilder::new(comparator, restart_interval)` — every Nth
//! entry is a restart point (full key, not prefix-compressed).
//! Smaller = faster seeks, larger blocks. Default: 16.
//! Exposed via [`crate::db_impl::Options::block_restart_interval`].

mod block;
mod block_entry;
mod block_handle;
mod block_restart_trailer;
mod block_trailer;
pub use block::*;
pub use block_entry::*;
pub use block_handle::*;
pub use block_restart_trailer::*;
pub use block_trailer::*;
