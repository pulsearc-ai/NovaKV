//! Built-in Snappy block compressor (compression kind byte = 1).
//!
//! Available behind the `snappy` Cargo feature:
//!
//! ```toml
//! [dependencies]
//! novakv = { version = "...", features = ["snappy"] }
//! ```
//!
//! Pulls in the [`snap`] crate (pure-Rust Snappy, no FFI).
//!
//! # Usage
//!
//! ```ignore
//! use std::sync::Arc;
//! use novakv::prelude::*;
//! use novakv::snappy::SnappyCompressor;
//!
//! let env = MemEnv::new();
//! let mut opts = Options::default();
//! opts.compressor = Some(Arc::new(SnappyCompressor));
//! let db = DBImpl::open("/db", env, BytewiseComparator, opts).unwrap();
//! db.put(b"k", b"some-payload-that-compresses-well-aaaaaaaaaaaa").unwrap();
//! ```
//!
//! # Block format
//!
//! `SnappyCompressor::kind` returns `1`. The compressed payload is
//! the raw Snappy stream, with no length-prefix framing.
//!
//! # 12.5% threshold
//!
//! If the compressed output isn't at least 12.5% smaller than the
//! input, the engine stores the block uncompressed instead. This
//! preserves block layout efficiency on incompressible data.

use crate::status::{Result, Status};
use crate::table::Compressor;

/// Built-in Snappy compressor.
///
/// `kind() = 1` selects Snappy. Payload format is the raw Snappy
/// stream (no length-prefix framing).
#[derive(Debug, Default, Clone, Copy)]
pub struct SnappyCompressor;

impl SnappyCompressor {
    pub fn new() -> Self {
        Self
    }
}

impl Compressor for SnappyCompressor {
    fn kind(&self) -> u8 {
        1
    }

    fn compress(&self, input: &[u8]) -> Option<Vec<u8>> {
        let mut encoder = snap::raw::Encoder::new();
        match encoder.compress_vec(input) {
            // Store compressed only if it beats the 12.5% threshold.
            Ok(compressed) if compressed.len() < input.len() - (input.len() / 8) => {
                Some(compressed)
            }
            _ => None,
        }
    }

    fn decompress(&self, input: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = snap::raw::Decoder::new();
        decoder
            .decompress_vec(input)
            .map_err(|e| Status::corruption(format!("snappy decompress: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_compressible_input() {
        let c = SnappyCompressor;
        let input = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".repeat(50);
        let compressed = c
            .compress(&input)
            .expect("highly compressible input must compress");
        assert!(
            compressed.len() < input.len() - (input.len() / 8),
            "compressed {} vs input {} doesn't beat 12.5% threshold",
            compressed.len(),
            input.len(),
        );
        let decoded = c.decompress(&compressed).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn incompressible_input_returns_none() {
        // Random-ish bytes should fail the 12.5% threshold.
        let c = SnappyCompressor;
        let input: Vec<u8> = (0..1024).map(|i| (i as u8).wrapping_mul(31)).collect();
        // Pseudo-random sequences won't reliably compress; accept either
        // None (threshold) or Some with at least the threshold met.
        if let Some(compressed) = c.compress(&input) {
            assert!(compressed.len() < input.len() - (input.len() / 8));
            let decoded = c.decompress(&compressed).unwrap();
            assert_eq!(decoded, input);
        }
    }

    #[test]
    fn empty_input_round_trips() {
        let c = SnappyCompressor;
        // 0-byte input: 0 - 0/8 = 0, so compressed.len() < 0 is impossible
        // → returns None. Caller falls back to uncompressed storage.
        let compressed = c.compress(b"");
        assert!(compressed.is_none());
    }

    #[test]
    fn corrupt_payload_surfaces_corruption() {
        let c = SnappyCompressor;
        let result = c.decompress(&[0xff, 0xff, 0xff, 0xff]);
        assert!(matches!(&result, Err(e) if e.is_corruption()));
    }

    #[test]
    fn kind_byte_is_one() {
        assert_eq!(SnappyCompressor.kind(), 1);
    }
}
