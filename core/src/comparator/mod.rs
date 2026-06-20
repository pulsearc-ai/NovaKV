//! Total order over user keys.
//!
//! [`Comparator`] is a trait that defines how the engine orders
//! keys. The default [`BytewiseComparator`] produces lexicographic
//! order over raw bytes.
//!
//! # Implementing a custom comparator
//!
//! ```
//! use novakv::comparator::Comparator;
//! use std::cmp::Ordering;
//!
//! #[derive(Debug, Clone)]
//! struct ReverseBytewise;
//!
//! impl Comparator for ReverseBytewise {
//!     fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
//!         b.cmp(a) // reverse lexicographic
//!     }
//!     fn name(&self) -> &'static str {
//!         "myapp.ReverseBytewise"
//!     }
//!     fn find_shortest_separator(&self, _start: &mut Vec<u8>, _limit: &[u8]) {}
//!     fn find_short_successor(&self, _key: &mut Vec<u8>) {}
//! }
//!
//! let cmp = ReverseBytewise;
//! assert_eq!(cmp.compare(b"a", b"b"), Ordering::Greater);
//! ```
//!
//! ## Constraints
//!
//! - **Total order**: `compare` must define a total order; equal
//!   keys must hash the same way under the configured filter policy.
//! - **`name()` is wire-format**: stored in the manifest at
//!   `DB::Open` time. Reopening an existing DB with a different
//!   comparator name returns `Status::invalid_argument`.
//! - **Separator hints are best-effort**: `find_shortest_separator`
//!   and `find_short_successor` shrink index entries; returning
//!   `start` / `key` unchanged is always safe.
//!
//! ## Internal-key comparator
//!
//! Internally the engine wraps the user comparator in
//! [`crate::format::InternalKeyComparator`], which orders by
//! `(user_key, sequence DESC, type)` — same user_key newest-first.
//! Users never construct that directly; it lives behind `DB::Open`.

mod comparator;
pub use comparator::*;
