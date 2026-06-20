//! Filesystem abstraction.
//!
//! [`Env`] is the trait every filesystem-touching code path takes
//! generically. Two impls ship:
//!
//! - [`StdEnv`] — backed by `std::fs`. Production path.
//! - [`MemEnv`] — fully in-memory. Used by every test in this crate
//!   so they don't need a temp dir.
//!
//! # Trait shape
//!
//! ```text
//! pub trait Env: Send + Sync {
//!     type Writable:      WritableFile;
//!     type RandomAccess:  RandomAccessFile;
//!     type Sequential:    SequentialFile;
//!     type Lock:          /* opaque */;
//!
//!     fn new_writable_file(&self, path: &Path) -> Result<Self::Writable>;
//!     fn new_random_access_file(&self, path: &Path) -> Result<Self::RandomAccess>;
//!     fn new_sequential_file(&self, path: &Path) -> Result<Self::Sequential>;
//!     fn lock_file(&self, path: &Path) -> Result<Self::Lock>;
//!     fn read_file(&self, path: &Path) -> Result<Vec<u8>>;
//!     fn write_file(&self, path: &Path, data: &[u8]) -> Result<()>;
//!     fn append_file(&self, path: &Path, data: &[u8]) -> Result<()>;
//!     fn file_exists(&self, path: &Path) -> bool;
//!     fn delete_file(&self, path: &Path) -> Result<()>;
//!     fn rename_file(&self, src: &Path, dst: &Path) -> Result<()>;
//!     fn get_file_size(&self, path: &Path) -> Result<u64>;
//!     fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;
//!     fn create_dir(&self, path: &Path) -> Result<()>;
//!     fn sync_file(&self, path: &Path) -> Result<()>;
//!     fn delete_dir(&self, path: &Path) -> Result<()>;
//! }
//! ```
//!
//! # Custom Env
//!
//! Implementations are free to back files however they like —
//! S3 blobs, encrypted volumes, fault-injection wrappers, etc.
//! The trait is `Send + Sync`; every method takes `&self`.
//!
//! Most fault-injection tests in this crate use this pattern — see
//! `tests/generated_bg_error_propagation.rs` for a working
//! `FaultEnv` that forwards everything to an inner `MemEnv` while
//! letting tests inject failures on specific paths.
//!
//! # File-handle traits
//!
//! - [`WritableFile`] — `append`, `flush`, `sync`, `close`. Buffered
//!   writes; `sync` forces durability.
//! - [`RandomAccessFile`] — `read_at(offset, n)`. Used by Table.
//! - [`SequentialFile`] — `read(n)`, `skip(n)`. Used by log/manifest.

mod env;
pub use env::*;
