//! Latency baseline harness for novakv.
//!
//! A dependency-free benchmark (`harness = false`) that establishes a
//! *baseline* for the two latency-critical paths we want to optimize:
//! **reopen** (opening an existing DB from disk) and **read/scan**
//! (point lookups and range scans). Every scenario maps to a concrete
//! optimization lever so before/after numbers are directly comparable.
//!
//! Run (release-optimized by default under the `bench` profile):
//!
//! ```text
//! cargo bench -p novakv --bench db_latency
//! ```
//!
//! Tunables via env vars:
//!
//! ```text
//! BENCH_N=200000      # dataset size (entries)            default 100_000
//! BENCH_VALUE=128     # value size in bytes               default 100
//! BENCH_TRIALS=40     # reopen / range-scan trials        default 30
//! BENCH_DIRTY=10000   # entries left in WAL for dirty reopen  default 10_000
//! BENCH_BLOCK_CACHE=32 # block cache size in MiB          default 32 (the crate default)
//! BENCH_KEEP=1        # keep the temp DB dir for inspection   default 0 (delete)
//! ```
//!
//! Select suites (default: all):
//!
//! ```text
//! cargo bench -p novakv --bench db_latency -- reopen get scan write space
//! ```
//!
//! The `space` suite reports Snappy compression only when the crate is
//! built with `--features snappy`; otherwise it prints the uncompressed
//! footprint alone.
//!
//! The numbers are wall-clock on real disk (`StdEnv`). They are a
//! relative yardstick for this machine, not an absolute spec.

use novakv::prelude::*;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

type Bench = DBImpl<BytewiseComparator, StdEnv>;

// ---------------------------------------------------------------------------
// Tiny deterministic RNG (xorshift64*) - avoids pulling in `rand`.
// ---------------------------------------------------------------------------
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
struct Config {
    n: u64,
    value_size: usize,
    trials: usize,
    dirty: u64,
    block_cache: usize,
    keep: bool,
    root: PathBuf,
}

impl Config {
    fn from_env() -> Self {
        let env_usize = |k: &str, d: usize| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let env_u64 = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let block_cache_mib = env_usize("BENCH_BLOCK_CACHE", 32);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let root =
            std::env::temp_dir().join(format!("novakv_bench_{stamp}_{}", std::process::id()));
        Config {
            n: env_u64("BENCH_N", 100_000),
            value_size: env_usize("BENCH_VALUE", 100),
            trials: env_usize("BENCH_TRIALS", 30),
            dirty: env_u64("BENCH_DIRTY", 10_000),
            block_cache: block_cache_mib * 1024 * 1024,
            keep: env_usize("BENCH_KEEP", 0) != 0,
            root,
        }
    }
}

// ---------------------------------------------------------------------------
// Key / value generation. Present keys live in [0, n); absent keys use a
// disjoint prefix so they are guaranteed to miss every SST.
// ---------------------------------------------------------------------------
fn present_key(i: u64) -> Vec<u8> {
    format!("key:{i:016}").into_bytes()
}
/// An absent key that sorts *between* present key `i` and `i+1` (the `x`
/// suffix), so it lies inside the populated range. This matters: a key
/// outside [min,max] is rejected by `Version::get`'s range pruning before
/// any SST is consulted, which would never exercise the Bloom filter.
/// Use only `i` in `0..n-1` so the result stays within range.
fn absent_key(i: u64) -> Vec<u8> {
    let mut k = present_key(i);
    k.push(b'x');
    k
}
fn value_of(i: u64, size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size.max(8)];
    v[..8].copy_from_slice(&i.to_le_bytes());
    // Fill the tail with a non-constant pattern so a future compressor
    // can't trivialize the block.
    for (j, b) in v.iter_mut().enumerate().skip(8) {
        *b = (i as u8).wrapping_add(j as u8);
    }
    v
}

// ---------------------------------------------------------------------------
// Options builders. `filter` toggles the Bloom filter; this is the single
// biggest read lever, so the `get` suite measures both.
// ---------------------------------------------------------------------------
fn make_options(filter: bool, block_cache: usize) -> Options {
    let mut o = Options {
        create_if_missing: true,
        block_cache_size: block_cache,
        ..Options::default()
    };
    // `Options::default()` ships a Bloom filter, so the `false` arm must
    // clear it explicitly - otherwise the `[no-bloom]` rows would silently
    // run *with* a filter and the comparison would be meaningless.
    if filter {
        o.filter_policy = Some(Arc::new(BloomFilterPolicy::new(10)));
    } else {
        o.filter_policy = None;
    }
    o
}

fn open_db(path: &Path, filter: bool, block_cache: usize) -> Bench {
    DBImpl::open(
        path.to_str().unwrap(),
        StdEnv::default(),
        BytewiseComparator,
        make_options(filter, block_cache),
    )
    .expect("open failed")
}

/// Build a fresh DB at `path`: insert `n` entries, force everything into
/// SSTs via a full compaction, then close. Returns nothing - the DB lives
/// on disk for the scenarios to reopen.
fn build_db(path: &Path, cfg: &Config, filter: bool) {
    let _ = destroy_db(path.to_str().unwrap(), StdEnv::default());
    let db = open_db(path, filter, cfg.block_cache);
    for i in 0..cfg.n {
        db.put(present_key(i), value_of(i, cfg.value_size))
            .expect("put failed");
    }
    db.compact_range(None, None).expect("compact failed");
    drop(db);
    // One clean reopen cycle so the WAL is empty for "clean reopen"
    // measurements (recovery sees no records to replay).
    let db = open_db(path, filter, cfg.block_cache);
    drop(db);
}

// ---------------------------------------------------------------------------
// Timing + reporting
// ---------------------------------------------------------------------------

/// Run `f` `trials` times, returning each call's wall-clock duration.
fn time_each<F: FnMut(usize)>(trials: usize, mut f: F) -> Vec<Duration> {
    let mut out = Vec::with_capacity(trials);
    for t in 0..trials {
        let start = Instant::now();
        f(t);
        out.push(start.elapsed());
    }
    out
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn fmt_dur(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns >= 1_000_000_000 {
        format!("{:.2} s", ns as f64 / 1e9)
    } else if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.2} us", ns as f64 / 1e3)
    } else {
        format!("{ns} ns")
    }
}

/// Report a latency distribution plus throughput. `ops` is the total
/// number of logical operations across all samples (for ops/sec).
fn report(label: &str, mut samples: Vec<Duration>, ops: u64) {
    samples.sort_unstable();
    let total: Duration = samples.iter().sum();
    let mean = if samples.is_empty() {
        Duration::ZERO
    } else {
        total / samples.len() as u32
    };
    let p50 = percentile(&samples, 0.50);
    let p90 = percentile(&samples, 0.90);
    let p99 = percentile(&samples, 0.99);
    let max = samples.last().copied().unwrap_or(Duration::ZERO);
    let ops_per_sec = if total.as_secs_f64() > 0.0 {
        ops as f64 / total.as_secs_f64()
    } else {
        0.0
    };
    println!(
        "  {label:<34} p50 {:>9}  p90 {:>9}  p99 {:>9}  max {:>9}  mean {:>9}  {:>12.0} ops/s",
        fmt_dur(p50),
        fmt_dur(p90),
        fmt_dur(p99),
        fmt_dur(max),
        fmt_dur(mean),
        ops_per_sec,
    );
}

fn section(title: &str) {
    println!("\n{title}");
    println!("{}", "-".repeat(title.len()));
}

/// Copy a DB directory's files into `dst` (flat - DB dirs have no
/// subdirectories). Used to give each dirty-reopen trial a pristine base.
fn copy_dir(src: &Path, dst: &Path) {
    let _ = std::fs::remove_dir_all(dst);
    std::fs::create_dir_all(dst).expect("create copy dir");
    for entry in std::fs::read_dir(src).expect("read src dir").flatten() {
        if entry.metadata().map(|m| m.is_file()).unwrap_or(false) {
            std::fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy file");
        }
    }
}

/// Recursively sum the on-disk byte size and count files in `dir`.
fn dir_stats(dir: &Path) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut files = 0usize;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if let Ok(meta) = e.metadata() {
                if meta.is_file() {
                    bytes += meta.len();
                    files += 1;
                }
            }
        }
    }
    (bytes, files)
}

// ---------------------------------------------------------------------------
// Suites
// ---------------------------------------------------------------------------

/// Reopen latency: cost of `DBImpl::open` on an existing DB.
/// - clean: WAL empty (data already in SSTs). Isolates the unconditional
///   new-manifest + new-log work every open performs today, plus
///   manifest recovery.
/// - dirty: `BENCH_DIRTY` entries sit in the WAL, so open must replay
///   them and flush a fresh SST. Gap vs clean == WAL-replay cost,
///   the target of the `reuse_logs` optimization.
fn suite_reopen(cfg: &Config) {
    section("REOPEN  (DBImpl::open on an existing DB)");
    let path = cfg.root.join("reopen");
    build_db(&path, cfg, false);
    let (bytes, files) = dir_stats(&path);
    println!(
        "  dataset: {} entries, {} value bytes, {:.1} MiB on disk across {} files",
        cfg.n,
        cfg.value_size,
        bytes as f64 / (1024.0 * 1024.0),
        files,
    );

    // Clean reopen: WAL stays empty between iterations.
    let clean = time_each(cfg.trials, |_| {
        let db = open_db(&path, false, cfg.block_cache);
        black_box(&db);
        drop(db);
    });
    report("reopen (clean / empty WAL)", clean, cfg.trials as u64);

    // Dirty reopen: measure ONLY the open that must replay `dirty` WAL
    // records and flush them to a fresh SST. Each trial runs against a
    // pristine copy of the compacted base, so the flushed SST does not
    // accumulate and skew later opens, and the re-dirtying writes stay
    // outside the timed region (we are measuring open, not put).
    let dirty_dir = cfg.root.join("reopen_dirty");
    let mut rng = Rng::new(0xD17C);
    let mut dirty = Vec::with_capacity(cfg.trials);
    for _ in 0..cfg.trials {
        copy_dir(&path, &dirty_dir);
        {
            let db = open_db(&dirty_dir, false, cfg.block_cache);
            for _ in 0..cfg.dirty {
                let i = rng.below(cfg.n);
                db.put(present_key(i), value_of(i, cfg.value_size)).unwrap();
            }
            drop(db); // leaves `dirty` records in the WAL
        }
        let start = Instant::now();
        let db = open_db(&dirty_dir, false, cfg.block_cache); // replays + flushes
        dirty.push(start.elapsed());
        drop(db);
    }
    let _ = std::fs::remove_dir_all(&dirty_dir);
    report(
        &format!("reopen (dirty / {} in WAL)", cfg.dirty),
        dirty,
        cfg.trials as u64,
    );
}

/// Point-lookup latency. Hits and misses, warm and cold, Bloom on and off.
/// The headline comparison is `get miss, COLD` with vs without Bloom: that
/// gap is the data-block read the filter avoids on a negative lookup.
fn suite_get(cfg: &Config) {
    section("GET  (point lookups)");
    let probes = cfg.n.min(50_000);
    let cold_probes = cfg.n.min(3_000);
    let miss_span = cfg.n.saturating_sub(1).max(1); // absent_key needs i < n-1

    for &filter in &[false, true] {
        let tag = if filter { "bloom" } else { "no-bloom" };
        let path = cfg.root.join(format!("get_{tag}"));
        build_db(&path, cfg, filter);

        // ---- COLD hit: fresh open, first touch. Includes table-open
        // (footer + index + filter) + first block read, amortized.
        {
            let db = open_db(&path, filter, cfg.block_cache);
            let mut rng = Rng::new(0xC01D);
            let mut sink = 0u64;
            let samples = time_each(cold_probes as usize, |_| {
                let i = rng.below(cfg.n);
                if let Some(v) = db.get(present_key(i)).unwrap() {
                    sink ^= v.len() as u64;
                }
            });
            black_box(sink);
            report(
                &format!("get hit,  COLD cache [{tag}]"),
                samples,
                cold_probes,
            );
            drop(db);
        }
        // ---- COLD miss: separate fresh open so the block cache is empty.
        // Without Bloom this pays a data-block read per probe; with Bloom
        // it short-circuits before the read.
        {
            let db = open_db(&path, filter, cfg.block_cache);
            let mut rng = Rng::new(0xC0DE);
            let mut sink = 0u64;
            let samples = time_each(cold_probes as usize, |_| {
                let i = rng.below(miss_span);
                if db.get(absent_key(i)).unwrap().is_some() {
                    sink += 1;
                }
            });
            black_box(sink);
            report(
                &format!("get miss, COLD cache [{tag}]"),
                samples,
                cold_probes,
            );
            drop(db);
        }

        // ---- WARM: pre-touch the working set, then measure.
        let db = open_db(&path, filter, cfg.block_cache);
        {
            let mut rng = Rng::new(0x5EED);
            for _ in 0..probes {
                let i = rng.below(cfg.n);
                black_box(db.get(present_key(i)).unwrap());
            }
        }
        {
            let mut rng = Rng::new(0xB0B0);
            let mut sink = 0u64;
            let samples = time_each(probes as usize, |_| {
                let i = rng.below(cfg.n);
                if let Some(v) = db.get(present_key(i)).unwrap() {
                    sink ^= v[0] as u64;
                }
            });
            black_box(sink);
            report(&format!("get hit,  warm cache [{tag}]"), samples, probes);
        }
        {
            let mut rng = Rng::new(0xA117);
            let mut sink = 0u64;
            let samples = time_each(probes as usize, |_| {
                let i = rng.below(miss_span);
                if db.get(absent_key(i)).unwrap().is_some() {
                    sink += 1;
                }
            });
            black_box(sink);
            report(&format!("get miss, warm cache [{tag}]"), samples, probes);
        }
        drop(db);
    }
    println!(
        "  (headline: compare the two `get miss, COLD` rows - that gap is the Bloom-filter win.)"
    );
}

/// Scan latency: full forward scans (cold then warm) and bounded range
/// scans. Cold full-scan ~= "scan right after reopen", the realistic case.
fn suite_scan(cfg: &Config) {
    section("SCAN  (iterator forward / range)");
    let path = cfg.root.join("scan");
    build_db(&path, cfg, false);

    // Cold full scan: fresh open, empty caches.
    {
        let db = open_db(&path, false, cfg.block_cache);
        let mut count = 0u64;
        let mut sink = 0u64;
        let start = Instant::now();
        let mut it = db.new_iterator().unwrap();
        it.seek_to_first();
        while it.valid() {
            sink ^= it.value().len() as u64;
            count += 1;
            it.next();
        }
        it.status().unwrap();
        let elapsed = start.elapsed();
        black_box(sink);
        let rate = count as f64 / elapsed.as_secs_f64();
        println!(
            "  full scan, COLD cache             {:>9}  ({} entries, {:>12.0} entries/s)",
            fmt_dur(elapsed),
            count,
            rate,
        );
        drop(db);
    }

    // Warm full scan: second pass over a primed cache.
    {
        let db = open_db(&path, false, cfg.block_cache);
        // prime
        {
            let mut it = db.new_iterator().unwrap();
            it.seek_to_first();
            while it.valid() {
                black_box(it.value().len());
                it.next();
            }
        }
        let mut count = 0u64;
        let mut sink = 0u64;
        let start = Instant::now();
        let mut it = db.new_iterator().unwrap();
        it.seek_to_first();
        while it.valid() {
            sink ^= it.value().len() as u64;
            count += 1;
            it.next();
        }
        it.status().unwrap();
        let elapsed = start.elapsed();
        black_box(sink);
        let rate = count as f64 / elapsed.as_secs_f64();
        println!(
            "  full scan, warm cache             {:>9}  ({} entries, {:>12.0} entries/s)",
            fmt_dur(elapsed),
            count,
            rate,
        );

        // Bounded range scans: seek to a random key, walk `span` entries.
        let span: u64 = 1_000;
        let mut rng = Rng::new(0x5CA4);
        let mut sink2 = 0u64;
        let samples = time_each(cfg.trials, |_| {
            let start_i = rng.below(cfg.n.saturating_sub(span).max(1));
            let mut it = db.new_iterator().unwrap();
            it.seek(&present_key(start_i));
            let mut walked = 0u64;
            while it.valid() && walked < span {
                sink2 ^= it.value().len() as u64;
                walked += 1;
                it.next();
            }
        });
        black_box(sink2);
        report(
            &format!("range scan (seek + {span} next)"),
            samples,
            cfg.trials as u64 * span,
        );
        drop(db);
    }
}

/// Write latency + throughput. Covers what a write-heavy workload tunes:
/// sequential vs random key order, the fsync-per-write durability cost
/// (`WriteOptions::sync`), grouped `WriteBatch` commits, and tombstone
/// (`delete`) cost. Each scenario runs against its own freshly-created DB
/// so background flush/compaction stalls surface in the tail (p99 / max)
/// rather than leaking across scenarios. Bloom filter on (the crate default).
fn suite_write(cfg: &Config) {
    section("WRITE  (put / batch / delete)");
    let bc = cfg.block_cache;
    let val = value_of(0xA5, cfg.value_size); // reused payload; content is irrelevant here
    let wipe = |p: &Path| {
        let _ = destroy_db(p.to_str().unwrap(), StdEnv::default());
    };

    // Sequential, no sync: ascending keys => already-ordered L0 SSTs, the
    // compaction-friendly case.
    {
        let path = cfg.root.join("write_seq");
        wipe(&path);
        let db = open_db(&path, true, bc);
        let samples = time_each(cfg.n as usize, |i| {
            db.put(present_key(i as u64), &val).expect("put failed");
        });
        report("put seq,  no sync", samples, cfg.n);
        drop(db);
        wipe(&path);
    }

    // Random, no sync: scattered keys => overlapping L0 SSTs and more
    // compaction work; the realistic worst case for write throughput.
    {
        let path = cfg.root.join("write_rand");
        wipe(&path);
        let db = open_db(&path, true, bc);
        let mut rng = Rng::new(0x301F);
        let samples = time_each(cfg.n as usize, |_| {
            let i = rng.below(cfg.n);
            db.put(present_key(i), &val).expect("put failed");
        });
        report("put rand, no sync", samples, cfg.n);
        drop(db);
        wipe(&path);
    }

    // Sequential, fsync per write: each put flushes + fsyncs the WAL
    // (sync_mode = Data => fdatasync). Far slower, so bound the op count.
    {
        let ops = cfg.n.min(5_000);
        let path = cfg.root.join("write_sync");
        wipe(&path);
        let db = open_db(&path, true, bc);
        let wo = WriteOptions { sync: true };
        let samples = time_each(ops as usize, |i| {
            db.put_with_options(&wo, present_key(i as u64), &val)
                .expect("put failed");
        });
        report("put seq,  fsync each", samples, ops);
        drop(db);
        wipe(&path);
    }

    // WriteBatch: many puts per commit, amortizing the per-write queue +
    // WAL framing over the whole batch.
    {
        let batch_size: u64 = 1_000;
        let batches = (cfg.n / batch_size).max(1);
        let path = cfg.root.join("write_batch");
        wipe(&path);
        let db = open_db(&path, true, bc);
        let samples = time_each(batches as usize, |b| {
            let base = b as u64 * batch_size;
            let mut wb = WriteBatch::new();
            for j in 0..batch_size {
                wb.put(present_key(base + j), &val);
            }
            db.write(&wb).expect("batch write failed");
        });
        report(
            &format!("batch put ({batch_size}/commit)"),
            samples,
            batches * batch_size,
        );
        drop(db);
        wipe(&path);
    }

    // Delete / tombstone: pre-fill untimed, then time deleting every key.
    // A delete is just a write of a deletion marker.
    {
        let path = cfg.root.join("write_delete");
        wipe(&path);
        let db = open_db(&path, true, bc);
        for i in 0..cfg.n {
            db.put(present_key(i), &val).expect("put failed");
        }
        let samples = time_each(cfg.n as usize, |i| {
            db.delete(present_key(i as u64)).expect("delete failed");
        });
        report("delete (tombstone)", samples, cfg.n);
        drop(db);
        wipe(&path);
    }
}

/// On-disk footprint, compaction speed, and (with the `snappy` feature)
/// compression. Builds the dataset, forces a full compaction, and reports
/// the resting size against the logical key+value bytes - the gap is space
/// amplification (block index + Bloom filter + restart arrays). With
/// `--features snappy` it also builds a compressed copy and reports the ratio.
fn suite_space(cfg: &Config) {
    section("SPACE  (footprint / compaction / compression)");
    let key_len = present_key(0).len() as u64;
    let logical = cfg.n * (key_len + cfg.value_size as u64);
    let mib = 1024.0 * 1024.0;
    println!(
        "  logical dataset: {:.1} MiB ({} entries, {} key + {} value bytes each)",
        logical as f64 / mib,
        cfg.n,
        key_len,
        cfg.value_size,
    );

    let (bytes, files, compact) = build_and_measure(cfg, "space", None);
    let compact_mib_s = (bytes as f64 / mib) / compact.as_secs_f64().max(1e-9);
    println!(
        "  uncompressed:    {:>6.1} MiB / {} files   space amp {:.2}x   compaction {} ({:.0} MiB/s)",
        bytes as f64 / mib,
        files,
        bytes as f64 / logical as f64,
        fmt_dur(compact),
        compact_mib_s,
    );

    #[cfg(feature = "snappy")]
    {
        let comp: Option<Arc<dyn Compressor>> = Some(Arc::new(SnappyCompressor::new()));
        let (cbytes, cfiles, ccompact) = build_and_measure(cfg, "space_snappy", comp);
        println!(
            "  snappy:          {:>6.1} MiB / {} files   ratio {:.2}x smaller    compaction {}",
            cbytes as f64 / mib,
            cfiles,
            bytes as f64 / cbytes.max(1) as f64,
            fmt_dur(ccompact),
        );
    }
    #[cfg(not(feature = "snappy"))]
    println!("  snappy:          (rebuild with `--features snappy` to measure compression)");
}

/// Build `cfg.n` sequential entries under `name`, force a full (timed)
/// compaction, and return (on-disk bytes, file count, compaction time).
fn build_and_measure(
    cfg: &Config,
    name: &str,
    compressor: Option<Arc<dyn Compressor>>,
) -> (u64, usize, Duration) {
    let path = cfg.root.join(name);
    let _ = destroy_db(path.to_str().unwrap(), StdEnv::default());
    let mut opts = make_options(true, cfg.block_cache);
    opts.compressor = compressor;
    let db = DBImpl::open(
        path.to_str().unwrap(),
        StdEnv::default(),
        BytewiseComparator,
        opts,
    )
    .expect("open failed");
    for i in 0..cfg.n {
        db.put(present_key(i), value_of(i, cfg.value_size))
            .expect("put failed");
    }
    let start = Instant::now();
    db.compact_range(None, None).expect("compact failed");
    let compact = start.elapsed();
    let (bytes, files) = dir_stats(&path);
    drop(db);
    let _ = destroy_db(path.to_str().unwrap(), StdEnv::default());
    (bytes, files, compact)
}

// ---------------------------------------------------------------------------
fn main() {
    let cfg = Config::from_env();
    let args: Vec<String> = std::env::args().skip(1).collect();
    // cargo may pass its own flags after `--`; keep only known suite names.
    let wanted: Vec<&str> = args
        .iter()
        .map(|s| s.as_str())
        .filter(|s| matches!(*s, "reopen" | "get" | "scan" | "write" | "space"))
        .collect();
    let run = |name: &str| wanted.is_empty() || wanted.contains(&name);

    std::fs::create_dir_all(&cfg.root).expect("create temp root");

    println!("novakv latency baseline");
    println!("========================");
    println!(
        "config: N={} value={}B trials={} dirty={} block_cache={} MiB",
        cfg.n,
        cfg.value_size,
        cfg.trials,
        cfg.dirty,
        cfg.block_cache / (1024 * 1024),
    );
    println!("tmp:    {}", cfg.root.display());

    if run("reopen") {
        suite_reopen(&cfg);
    }
    if run("get") {
        suite_get(&cfg);
    }
    if run("scan") {
        suite_scan(&cfg);
    }
    if run("write") {
        suite_write(&cfg);
    }
    if run("space") {
        suite_space(&cfg);
    }

    if cfg.keep {
        println!("\n(kept temp dir for inspection: {})", cfg.root.display());
    } else {
        let _ = std::fs::remove_dir_all(&cfg.root);
    }
    println!();
}
