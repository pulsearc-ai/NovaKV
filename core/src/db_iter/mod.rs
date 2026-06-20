//! Snapshot-aware DB iterator.
//!
//! [`DBIter`] wraps an internal-key
//! [`crate::merging_iter::MergingIterator`] and exposes the user-key
//! view: it skips tombstones, hides shadowed older versions of the
//! same user key, and stops at the iterator's pinned sequence
//! number.
//!
//! Most users get one of these via
//! [`crate::db_impl::DBImpl::new_iterator`] /
//! `new_iterator_with_options`. Direct construction is rare.
//!
//! # Iterator contract
//!
//! Implements [`crate::DbIterator`]:
//!
//! - `seek_to_first` / `seek_to_last` / `seek(target)` to position
//! - `valid()` returns `true` while positioned on a live entry
//! - `key()` / `value()` are the current user-visible pair
//! - `next()` / `prev()` advance
//! - `status()` surfaces deferred errors (e.g. block corruption
//!   discovered during a `next`)
//!
//! Always check `iter.status().is_ok()` after iteration completes —
//! the iterator can't return errors per-step, so corruption surfaces
//! at the end.

mod db_iter;
mod db_iterator;
pub use db_iter::*;
pub use db_iterator::*;
