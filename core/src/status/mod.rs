//! Engine-wide error type.
//!
//! [`Status`] carries a [`Code`] and an optional human-readable
//! message. [`Result`] is an alias for `std::result::Result<T, Status>`.
//! Every fallible path in the crate returns `Result<T>`.
//!
//! # Codes
//!
//! - [`Code::Ok`] — success (rarely materialized; OK paths return
//!   `Ok(value)` directly)
//! - [`Code::NotFound`] — key absent, file missing, etc.
//! - [`Code::Corruption`] — checksum mismatch, malformed on-disk
//!   record, comparator-name mismatch on Open
//! - [`Code::NotSupported`] — feature flagged off (e.g. compressed
//!   block read with no compressor configured)
//! - [`Code::InvalidArgument`] — caller misuse
//! - [`Code::IoError`] — env/filesystem failure
//!
//! # Building errors
//!
//! Convenience constructors exist for each variant:
//!
//! ```
//! use novakv::status::Status;
//! let e = Status::not_found("key not present");
//! assert!(e.is_not_found());
//! ```
//!
//! Predicate methods (`is_not_found`, `is_corruption`, `is_io_error`,
//! `is_invalid_argument`, `is_not_supported`) test the code.

mod status;
pub use status::*;
