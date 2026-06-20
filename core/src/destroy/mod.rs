//! Delete an entire DB directory.
//!
//! [`destroy_db`] acquires the DB LOCK, deletes every recognized
//! file inside the directory, then removes the directory itself.
//! Idempotent on a missing target (returns `Ok`).
//!
//! ```
//! use novakv::prelude::*;
//!
//! let env = MemEnv::new();
//! {
//!     let _db = DBImpl::open("/db", env.clone_handle(), BytewiseComparator, Options::default()).unwrap();
//! }   // close
//! destroy_db("/db", env).unwrap();
//! ```
//!
//! # Caveats
//!
//! - The DB must be closed first; the LOCK acquire fails if
//!   another process holds it.
//! - Unrecognized files in the directory (e.g. user-dropped
//!   junk) are left in place. Only recognized database files are
//!   removed.

mod destroy;
pub use destroy::*;
