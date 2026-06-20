//! LSM level metadata + manifest log.
//!
//! [`VersionSet`] tracks every SSTable across all levels, the
//! current sequence number, and the live WAL number. Each mutation
//! (a flush, a compaction) builds a [`VersionEdit`], appends it to
//! the manifest log, and atomically swaps the current
//! [`Version`] via `Arc`.
//!
//! # Concurrency model
//!
//! - One writer at a time (synchronized externally by `DBImpl`).
//! - Many readers: each `current()` call returns an `Arc<Version>`
//!   that survives subsequent edits. Snapshots use this — drop the
//!   `Arc` (or `release_version`) when done.
//!
//! # Compaction picker
//!
//! - **Seek-triggered** — a single file consumed too many seeks
//!   without yielding values; compact it down a level.
//!   ([`Version::take_file_to_compact`])
//! - **Size-triggered** — a level's `files * size / max_bytes` ≥
//!   1.0; compact the file with the largest internal-key range
//!   that beats the level's `compact_pointer`.
//!
//! See `pick_compaction` for the priority + picking logic, and
//! `pick_manual_compaction(level, begin, end)` for the bounded
//! variant `DBImpl::compact_range` drives.
//!
//! # Why expose this
//!
//! Internal plumbing under [`crate::db_impl::DBImpl`]. Public for
//! repair tools, inspection scripts, and custom DB shells.

mod file_meta_data;
mod version_edit;
mod version_set;
pub use file_meta_data::*;
pub use version_edit::*;
pub use version_set::*;
