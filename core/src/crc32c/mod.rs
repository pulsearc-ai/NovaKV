//! CRC32C with the Castagnoli polynomial.
//!
//! Used to checksum log records and table blocks on disk. The
//! [`mask`] / [`unmask`] pair implements the "masked CRC" trick that
//! prevents the literal CRC value from ever appearing in the data
//! stream — both are part of the log/table format.
//!
//! # API
//!
//! - [`value(data)`](value) — full CRC32C of `data`
//! - [`extend(crc, more)`](extend) — incremental update for streaming
//!   (e.g. CRC over `[kind_byte] || block_data`)
//! - [`mask(crc)`](mask) / [`unmask(masked)`](unmask) — wire-format mask used by
//!   log record headers and block trailers
//!
//! # Wire-format dependence
//!
//! Both the polynomial and the mask formula are part of the on-disk
//! format; **do not change them**.

mod crc32c;
pub use crc32c::*;
