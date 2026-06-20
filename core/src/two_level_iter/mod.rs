//! Two-level iterator: index iterator over data-block iterators.
//!
//! Used internally by [`crate::table::Table`] to lazy-load data
//! blocks: the outer iterator walks the index block, the inner
//! materializes one data block at a time. Public for embedders that
//! want to compose the same pattern over their own block sources.

mod two_level_iter;
pub use two_level_iter::*;
