//! Write-ahead log.
//!
//! The WAL is a sequence of 32 KB blocks; each block holds one or
//! more records framed with a 7-byte header
//! (`crc32c | len_le16 | kind`). Records that don't fit in a block
//! are split across `First`/`Middle`/`Last` fragments with the same
//! payload spread across them.
//!
//! Two consumers internally:
//! - [`crate::db_impl::DBImpl`] writes every batch to the WAL
//!   before applying to the memtable.
//! - [`crate::version_set::VersionSet`] writes manifest records
//!   using the same framing.
//!
//! # Public types
//!
//! - [`LogWriter`] — appends records via
//!   `add_record_to(&mut WritableFile, &[u8])`
//! - [`LogSequentialReader`] — yields one logical record per call,
//!   reassembling fragmented records and validating CRCs
//!
//! # Why expose this
//!
//! Useful for repair tools, log inspection, and embedders building
//! their own crash-consistent stores on top of a [`crate::env::Env`].

mod log;
mod log_record_header;
pub use log::*;
pub use log_record_header::*;
