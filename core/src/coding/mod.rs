//! Fixed-width and varint primitives for on-disk byte layouts.
//!
//! Every on-disk format in this crate (log records, table blocks,
//! manifest entries, WriteBatch records) bottoms out in these
//! helpers, so endianness and varint width are fixed by the format.
//!
//! # What's here
//!
//! | Function | Purpose |
//! |----------|---------|
//! | [`put_fixed32`] / [`put_fixed64`] | Append little-endian `u32` / `u64` |
//! | [`decode_fixed32`] / [`decode_fixed64`] | Read little-endian `u32` / `u64` |
//! | [`put_varint32`] / [`put_varint64`] | Append 7-bit-grouped varint |
//! | [`get_varint32`] / [`get_varint64`] | Consume a varint from a `&mut &[u8]` cursor |
//! | [`varint_length`] | Bytes a varint will occupy |
//! | [`put_length_prefixed_slice`] / [`get_length_prefixed_slice`] | Varint length + bytes |
//!
//! # Cursor convention
//!
//! Decoders take `input: &mut &[u8]`. On success the slice is advanced
//! past the consumed bytes; on failure the slice is left untouched and
//! a `Status::corruption(...)` is returned. Callers can chain decoders
//! by calling them in sequence on the same cursor.
//!
//! # Example
//!
//! ```
//! use novakv::coding::{put_varint64, get_varint64, put_length_prefixed_slice};
//!
//! let mut buf = Vec::new();
//! put_varint64(&mut buf, 0x1234_5678);
//! put_length_prefixed_slice(&mut buf, b"hello");
//!
//! let mut cursor: &[u8] = &buf;
//! let n = get_varint64(&mut cursor).unwrap();
//! assert_eq!(n, 0x1234_5678);
//! ```

mod coding;
pub use coding::*;
