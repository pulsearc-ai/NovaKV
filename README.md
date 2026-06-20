# NovaKV

[![crates.io](https://img.shields.io/crates/v/novakv.svg)](https://crates.io/crates/novakv)
[![docs.rs](https://img.shields.io/docsrs/novakv)](https://docs.rs/novakv)
[![license](https://img.shields.io/crates/l/novakv.svg)](#license)

An embedded, ordered key-value store for Rust — by [PulseArc](https://github.com/pulsearc-ai).

NovaKV is a pure-Rust, single-process storage engine built on a
log-structured merge-tree (LSM), in the lineage of Google's
[LevelDB](https://github.com/google/leveldb). Keys are stored sorted, so
range scans and ordered iteration are first-class.

## Features

- **Ordered key-value storage** — `put` / `get` / `delete` plus ordered
  forward/reverse iteration and range scans.
- **Atomic batches** — group writes into a `WriteBatch` applied all-or-nothing.
- **Snapshots** — consistent point-in-time reads via `get_snapshot`.
- **Crash safety** — a write-ahead log (WAL) recovers in-flight writes, and
  `repair_db` reconstructs a damaged directory.
- **Pluggable environment** — run against the real filesystem (`StdEnv`) or a
  fully in-memory `MemEnv` (great for tests).
- **Bloom filters & block cache** — skip SSTs that can't contain a key, and
  keep hot blocks in a sharded LRU cache.
- **Optional Snappy compression** — behind the `snappy` feature.
- **Dependency-light** — the default build pulls in no third-party crates.

## Install

```sh
cargo add novakv
```

## Quickstart

```rust
use novakv::prelude::*;

let env = MemEnv::new();
let db = DBImpl::open("/db", env, BytewiseComparator, Options::default()).unwrap();

db.put(b"hello", b"world").unwrap();
assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
```

Use `StdEnv` instead of `MemEnv` to persist to a real directory.

## Feature flags

| Feature  | Default | Description |
|----------|---------|-------------|
| `snappy` | off     | Built-in Snappy block compressor (`SnappyCompressor`), via the pure-Rust [`snap`](https://crates.io/crates/snap) crate. |

```toml
novakv = { version = "1.0", features = ["snappy"] }
```

## Architecture

Each engine layer is a focused module; see the [crate docs](https://docs.rs/novakv)
for the full API surface and tuning knobs.

| Module                  | What's in it                                  |
|-------------------------|-----------------------------------------------|
| `db_impl`               | `DBImpl`, `Options`, `ReadOptions`, `WriteOptions` |
| `write_batch`           | atomic multi-write batches                    |
| `db_iter`               | snapshot-aware iterator                        |
| `memtable` / `skiplist` | in-memory write buffer                         |
| `table` / `block`       | SSTable builder + reader                       |
| `filter` / `filter_block` | Bloom filters                                |
| `log`                   | write-ahead log                                |
| `version_set`           | LSM level metadata + manifest                  |
| `cache` / `table_cache` | sharded LRU + open-table cache                 |
| `env`                   | filesystem trait + `StdEnv` + `MemEnv`         |
| `repair` / `destroy`    | recover / delete a DB directory                |

## Performance

NovaKV ships a dependency-free latency harness. Reproduce these numbers with:

```sh
cargo bench -p novakv --bench db_latency                    # all suites
cargo bench -p novakv --bench db_latency --features snappy  # adds compression
```

p50 wall-clock against the real filesystem (`StdEnv`), single process, default
workload (100,000 entries, 100-byte values, 32 MiB block cache, Bloom filter
on). Writes are non-syncing unless noted. These are a per-machine yardstick,
not an absolute spec — measured on an Apple M5 Max (macOS 26.5, rustc 1.96):

| Operation                              | p50 latency   | Throughput       |
|----------------------------------------|---------------|------------------|
| Write — put, sequential keys           | 1.3 µs        | ~670K ops/s      |
| Write — put, random keys               | 1.3 µs        | ~650K ops/s      |
| Write — put, fsync per write           | 23 µs         | ~42K ops/s       |
| Write — batch, 1,000 puts/commit       | 216 µs/commit | ~4.2M ops/s      |
| Delete — tombstone                     | 1.2 µs        | ~810K ops/s      |
| Point read — hit, warm cache           | 584 ns        | ~1.6M ops/s      |
| Point read — miss, warm cache (Bloom)  | 333 ns        | ~2.9M ops/s      |
| Point read — hit, cold cache           | 1.3 µs        | ~750K ops/s      |
| Full scan — warm cache                 | 5.6 ms        | ~18.0M entries/s |
| Full scan — cold cache                 | 7.8 ms        | ~12.8M entries/s |
| Range scan — seek + 1,000 `next`       | 54 µs         | —                |
| Reopen — clean (empty WAL)             | 117 µs        | —                |
| Reopen — dirty (10,000-record WAL)     | 2.0 ms        | —                |

**Footprint** — the same dataset rests in ~11.1 MiB on disk (0.97× the logical
key+value bytes, thanks to in-block key prefix compression), and a full
compaction runs at ~530 MiB/s. With the `snappy` feature the (synthetic) payload
compresses to ~2.5 MiB, ~4.4× smaller; real-world ratios depend on your data.

## License

NovaKV is distributed under the terms of **either** the MIT license **or** the
Apache License (Version 2.0) at your option, **and** the BSD-3-Clause license
covering portions derived from LevelDB:

```
(MIT OR Apache-2.0) AND BSD-3-Clause
```

- [LICENSE-MIT](LICENSE-MIT) — © PulseArc
- [LICENSE-APACHE](LICENSE-APACHE) — © PulseArc
- [LICENSE-BSD-3-CLAUSE](LICENSE-BSD-3-CLAUSE) — © The LevelDB Authors

NovaKV began as a Rust port of [LevelDB](https://github.com/google/leveldb) and
retains its on-disk-format design; that original work is BSD-3-Clause licensed,
and that license is preserved here.
