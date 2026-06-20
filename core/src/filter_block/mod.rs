//! Per-data-block filter index.
//!
//! [`FilterBlockBuilder`] writes one filter blob per `2^base_lg`
//! bytes of data block; [`FilterBlockReader`] maps a data block's
//! offset back to the right filter blob. This module is internal
//! plumbing under [`crate::table::Table`] — most users never touch
//! it directly.
//!
//! Public surface for advanced use:
//!
//! - Building tools that produce SSTs outside the engine
//! - Diagnostic readers that want to walk filters out-of-band
//!
//! See [`crate::filter`] for the [`crate::filter::FilterPolicy`]
//! trait that drives the actual hashing.
//!
//! # Wire-format
//!
//! `filter_data | offsets[N] u32le | array_offset u32le | base_lg u8`
//!
//! `base_lg = 11` (the default) → one filter per 2 KB.

mod filter_block;
pub use filter_block::*;
