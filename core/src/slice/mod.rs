//! Lightweight `(ptr, len)` borrowed-bytes view.
//!
//! In C-style APIs this is a `(const char*, size_t)` pair. In Rust
//! the natural equivalent is `&[u8]`, and most public APIs take
//! `&[u8]` (or `impl AsRef<[u8]>`) directly.
//!
//! [`Slice`] is provided for embedders writing translation layers
//! between C-style APIs and this crate, where you want a
//! structurally compatible struct rather than `&[u8]`.

mod slice;
pub use slice::*;
