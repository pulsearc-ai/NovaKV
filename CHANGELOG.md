# Changelog

All notable changes to NovaKV are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-06-19

Initial release. NovaKV began as a Rust port of
[LevelDB](https://github.com/google/leveldb) and retains its on-disk format.

### Added

- Ordered key-value storage: `put` / `get` / `delete`, plus forward/reverse
  ordered iteration and range scans.
- Atomic `WriteBatch` writes applied all-or-nothing.
- Point-in-time `Snapshot` reads via `get_snapshot`.
- Crash safety: write-ahead log (WAL) recovery and `repair_db` for damaged
  directories.
- WAL reuse on reopen: the current WAL can be replayed into memory and appended
  to, instead of flushing replayed entries into an SST and starting a fresh
  WAL/manifest. Backed by appendable-file support and `LogWriter::resume`, with
  a lower-allocation replay path via `read_record_into`.
- Pluggable `Env`: real-filesystem `StdEnv` and in-memory `MemEnv`.
- Bloom filter policy enabled by default, plus a 32 MiB sharded LRU block cache.
- Optional Snappy block compression behind the `snappy` feature (pure-Rust
  `snap` crate); the default build pulls in no third-party crates.

[Unreleased]: https://github.com/pulsearc-ai/novakv/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/pulsearc-ai/novakv/releases/tag/v1.0.0
