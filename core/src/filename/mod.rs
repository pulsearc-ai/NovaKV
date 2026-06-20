//! DB-directory file-naming helpers.
//!
//! Every file inside a DB directory follows a strict naming scheme
//! so recovery can re-derive its purpose from the name alone:
//!
//! | Pattern | Type |
//! |---------|------|
//! | `<n>.log` | WAL segment |
//! | `<n>.ldb` (or legacy `<n>.sst`) | SSTable |
//! | `MANIFEST-<n>` | version-edit log |
//! | `CURRENT` | one-line pointer to active manifest |
//! | `LOCK` | DB-level POSIX lock file |
//! | `<n>.dbtmp` | temp file used by atomic CURRENT install |
//! | `LOG`, `LOG.old` | rotated diagnostic log (info_log) |
//!
//! # Public helpers
//!
//! - [`log_file_name`], [`table_file_name`], [`descriptor_file_name`],
//!   [`current_file_name`], [`lock_file_name`], [`temp_file_name`],
//!   [`info_log_file_name`], [`old_info_log_file_name`]
//! - [`parse_file_name`] — inverse: directory entry → `(number, FileType)`
//! - [`set_current_file`] — atomic CURRENT install via temp + rename
//!
//! # Why expose this
//!
//! Repair, destroy, and external diagnostic tooling all need to
//! enumerate or classify files in a DB directory. The helpers keep
//! the patterns in one place.

mod filename;
pub use filename::*;
