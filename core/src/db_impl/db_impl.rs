use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use crate::cache::Cache;
use crate::comparator::Comparator;
use crate::db_iter::DBIter;
use crate::db_iter::DbIterator;
use crate::env::{Env, WritableFile};
use crate::filename::{
    current_file_name, descriptor_file_name, lock_file_name, log_file_name, parse_file_name,
    sst_table_file_name, table_file_name, FileType,
};
use crate::filter::{BloomFilterPolicy, FilterPolicy};
use crate::format::{
    pack_sequence_and_type, parse_internal_key, InternalKeyComparator, LookupKey, SequenceNumber,
    ValueType, L0_SLOWDOWN_WRITES_TRIGGER, L0_STOP_WRITES_TRIGGER, MAX_SEQUENCE_NUMBER, NUM_LEVELS,
    VALUE_TYPE_FOR_SEEK,
};
use crate::log::{LogSequentialReader, LogWriter};
use crate::memtable::SharedMemTable;
use crate::merging_iter::MergingIterator;
use crate::status::{Result, Status};
use crate::table::{Compressor, TableFileBuilder, TableIterator};
use crate::table_cache::TableCache;
use crate::two_level_iter::TwoLevelIterator;
use crate::version_set::{Version, VersionSet};
use crate::write_batch::{pool_take, PooledWriteBatch, WriteBatchHandler};

pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 4194304;
/// Default per-DB block cache (32 MiB). Holds decompressed data blocks
/// so warm reads/scans skip both I/O and decompression; sized to keep a
/// useful working set resident without an outsized footprint.
pub const DEFAULT_BLOCK_CACHE_SIZE: usize = 33554432;
pub const DEFAULT_MAX_OPEN_FILES: usize = 1000;
const MAX_BATCH_GROUP_SIZE: usize = 1 << 20;
const MAX_BATCH_GROUP_OVERHEAD: usize = 128 << 10;

/// The on-disk format major version - bumped on incompatible
/// format changes.
pub const MAJOR_VERSION: u32 = 1;
/// The on-disk format minor version.
/// Bumped on backward-compatible format additions.
pub const MINOR_VERSION: u32 = 18;

/// Database configuration - minimum useful surface for v1.
/// Only fields the codegen actually consumes; expand later.
#[derive(Clone)]
pub struct Options {
    /// Memtable size threshold before flush is triggered.
    pub write_buffer_size: usize,
    /// TableCache capacity (in bytes / entry count).
    pub block_cache_size: usize,
    /// Max open SST files.
    pub max_open_files: usize,
    /// When `true`, opening a non-existent DB creates a fresh one;
    /// when `false`, the open returns `NotFound`. The classic
    /// default is `false`; this crate defaults to `true` for
    /// convenience - set to `false` for the strict
    /// behavior.
    pub create_if_missing: bool,
    /// If true, refuse to open an existing DB.
    pub error_if_exists: bool,
    /// If true, force checksum verification on every block read
    /// (the paranoid-checks policy). Phase 49
    /// note: v1 always verifies CRCs unconditionally - accepting
    /// the field for forward-compat with future opt-out plumbing.
    pub paranoid_checks: bool,
    /// Optional filter policy. When `Some`,
    /// every SST built by flush + compaction emits a filter block
    /// keyed `"filter.<policy.name()>"` in the metaindex; every
    /// SST opened consults the filter to short-circuit point
    /// lookups for absent keys. For a default,
    /// pass `Arc::new(BloomFilterPolicy::new(10))` for a
    /// 10-bit Bloom.
    pub filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
    /// Optional block cache. When `Some`,
    /// the supplied cache is used for per-block data - useful
    /// for sharing one cache across multiple `DBImpl` instances
    /// or plugging in a custom (non-LRU) cache implementation.
    /// When `None`, a default `ShardedLRUCache` of size
    /// `block_cache_size` is created internally.
    pub block_cache: Option<Arc<dyn Cache<Arc<crate::block::Block>> + Send + Sync>>,
    /// Optional block compressor. When `Some`,
    /// every block written by flush + compaction is offered to
    /// the compressor (which may decline per-block, falling
    /// back to uncompressed storage); reads consult the
    /// compressor whenever the block trailer kind matches.
    /// When `None`, blocks are stored uncompressed.
    /// The crate ships no built-in
    /// compressor - users wire up Snappy/LZ4/etc. themselves to
    /// keep the runtime dependency list empty.
    pub compressor: Option<Arc<dyn Compressor>>,
    /// Phase F: target uncompressed data block size, in bytes.
    /// Default 4096.
    pub block_size: usize,
    /// Phase F: how many keys between restart points inside a
    /// data block.
    /// Default 16. Larger = smaller blocks, slower seeks.
    pub block_restart_interval: usize,
    /// Phase F: optional diagnostic logger.
    /// Currently accepted but only
    /// surfaced via the public field - internal code paths
    /// don't yet emit log messages. Provided for forward-compat
    /// so users can install a logger now.
    pub info_log: Option<Arc<dyn Logger>>,
    /// Durability policy for filesystem syncs - memtable flush,
    /// compaction output, the manifest, and `WriteOptions::sync`
    /// writes. `Data` (`fdatasync`, the default) is used by
    /// the posix env; `Full` adds power-loss durability at
    /// a large cost on macOS. In-memory envs ignore it.
    pub sync_mode: crate::env::SyncMode,
}

/// Diagnostic logging interface.
/// A user-supplied sink for diagnostic messages. The single
/// `log` method takes a pre-formatted string - a
/// printf-style variadic API doesn't translate cleanly to Rust,
/// and `format!` at the call site is more idiomatic anyway.
pub trait Logger: std::fmt::Debug + Send + Sync {
    fn log(&self, message: &str);
}

impl Default for Options {
    fn default() -> Self {
        Self {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            block_cache_size: DEFAULT_BLOCK_CACHE_SIZE,
            max_open_files: DEFAULT_MAX_OPEN_FILES,
            create_if_missing: true,
            error_if_exists: false,
            paranoid_checks: false,
            // 10-bit Bloom by default (~1% false-positive rate): SSTs
            // emit a filter block and point lookups short-circuit absent
            // keys before reading any data block. Backward-compatible -
            // tables built without a filter are still read fine. Pass
            // `filter_policy: None` to opt out.
            filter_policy: Some(Arc::new(BloomFilterPolicy::new(10))),
            block_cache: None,
            compressor: None,
            block_size: crate::table::DEFAULT_BLOCK_SIZE,
            block_restart_interval: crate::table::DEFAULT_BLOCK_RESTART_INTERVAL,
            info_log: None,
            sync_mode: crate::env::SyncMode::Data,
        }
    }
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("write_buffer_size", &self.write_buffer_size)
            .field("block_cache_size", &self.block_cache_size)
            .field("max_open_files", &self.max_open_files)
            .field("create_if_missing", &self.create_if_missing)
            .field("error_if_exists", &self.error_if_exists)
            .field("paranoid_checks", &self.paranoid_checks)
            .field(
                "filter_policy",
                &self.filter_policy.as_ref().map(|p| p.name()),
            )
            .field(
                "block_cache",
                &self.block_cache.as_ref().map(|c| c.total_charge()),
            )
            .field("compressor", &self.compressor.as_ref().map(|c| c.kind()))
            .field("block_size", &self.block_size)
            .field("block_restart_interval", &self.block_restart_interval)
            .field("info_log", &self.info_log.is_some())
            .field("sync_mode", &self.sync_mode)
            .finish()
    }
}

/// A point-in-time read view of the database. Opaque to
/// the user - they only hold the `Arc` and pass it through
/// `ReadOptions::snapshot`. Internally it just pins a
/// sequence number, plus the `Arc::strong_count > 0` keeps
/// the snapshot alive for compaction's tombstone-drop check.
#[derive(Debug)]
pub struct Snapshot {
    sequence: SequenceNumber,
}

impl Snapshot {
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }
}

/// Options that control read behavior. All three fields
/// are honored: `snapshot`, `verify_checksums`, `fill_cache`.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// If `Some`, reads observe state at the snapshot's
    /// sequence number. If `None`, reads see the latest state.
    pub snapshot: Option<Arc<Snapshot>>,
    /// If true, verify CRCs on every block read for this
    /// operation.
    /// Default: `false`.
    pub verify_checksums: bool,
    /// Phase F: when `false`, skip inserting freshly-read
    /// blocks into the block cache. Useful for big scans
    /// that would otherwise evict more useful entries.
    /// Default: `true`.
    pub fill_cache: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            snapshot: None,
            verify_checksums: false,
            fill_cache: true,
        }
    }
}

/// Options that control write behavior. When `sync` is true,
/// writes call `WritableFile::sync` after updating the WAL.
#[derive(Debug, Default, Clone, Copy)]
pub struct WriteOptions {
    /// If true, the write must be flushed to stable storage
    /// before returning OK.
    pub sync: bool,
}

/// A key range. Used by
/// `DB::GetApproximateSizes`. `start` is inclusive, `limit`
/// is exclusive.
#[derive(Debug, Clone)]
pub struct Range {
    pub start: Vec<u8>,
    pub limit: Vec<u8>,
}

/// Per-level compaction statistics.
/// One entry per level. Updated whenever a flush or compaction
/// writes that level. Surfaced via `get_property("novakv.stats")`.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct CompactionStats {
    pub micros: i64,
    pub bytes_read: i64,
    pub bytes_written: i64,
}

impl CompactionStats {
    fn add(&mut self, c: CompactionStats) {
        self.micros += c.micros;
        self.bytes_read += c.bytes_read;
        self.bytes_written += c.bytes_written;
    }
}

#[derive(Debug)]
struct PendingWriter {
    id: u64,
    /// `None` once `build_batch_group` has folded this writer's
    /// batch into the group - the batch is moved out rather than
    /// cloned, and is never read again before the writer is
    /// popped by `finish_writer_group`.
    batch: Option<PooledWriteBatch>,
    sync: bool,
    /// Set only for followers that may be grouped away by the
    /// leader. A leader writer returns its status directly and
    /// leaves this `None`, avoiding an allocation per write.
    result: Option<Arc<std::sync::Mutex<Option<Result<()>>>>>,
}

/// Private inner state of `DBImpl`. The public `DBImpl`
/// wraps this in `Arc<(Mutex<...>, Condvar)>` and adds a
/// background worker thread (Phase 47).
///
/// All algorithmic logic (write, get, flush, compact, etc.)
/// lives on `DBImplCore`'s methods and assumes the caller
/// holds the outer mutex.
pub(super) struct DBImplCore<C: Comparator + Clone + 'static, E: Env + Clone + 'static> {
    dbname: String,
    env: E,
    comparator: C,
    icmp: InternalKeyComparator<C>,
    options: Options,
    mem: SharedMemTable<C>,
    imm: Option<SharedMemTable<C>>,
    log_writer: Option<LogWriter>,
    log_file: Option<E::Writable>,
    _db_lock: E::Lock,
    log_file_number: u64,
    version_set: VersionSet<C, E>,
    table_cache: TableCache<InternalKeyComparator<C>, E>,
    /// Live snapshots, tracked as `Weak` so user-side `Arc`
    /// drops free them automatically. Compaction reads this
    /// to decide which old versions are still observable.
    snapshots: Vec<Weak<Snapshot>>,
    /// Number of public iterators that may still reference files
    /// from older versions. Obsolete table files are retained
    /// until this drops to zero, tracked via Version refs.
    active_iterators: usize,
    obsolete_table_file_numbers: Vec<u64>,
    /// Phase 47 background-thread state: tracks any background
    /// error, whether compaction is scheduled, and shutdown.
    pub(super) shutting_down: bool,
    pub(super) bg_work_scheduled: bool,
    pub(super) bg_work_running: bool,
    pub(super) manual_compaction_running: bool,
    pub(super) bg_error: Option<Status>,
    /// Unparkable handle to the background worker, stored once
    /// `DBImpl::open` has spawned it. The worker parks instead
    /// of waiting on the writer condvar, so writer-path
    /// notifications never wake it; threads that schedule
    /// flush/compaction work `unpark()` it explicitly.
    pub(super) bg_thread_handle: Option<std::thread::Thread>,
    writers: VecDeque<PendingWriter>,
    next_writer_id: u64,
    /// Per-level compaction statistics, one entry per level;
    /// accumulated by flush + compaction.
    pub(super) stats: Vec<CompactionStats>,
    /// Phase N+: running byte-count of the active memtable.
    /// Updated by accumulating `MemTable::add` returns inside
    /// `write_with_options`; read by `make_room_for_write` /
    /// `force_flush`; reset to 0 on memtable swap. Plain
    /// `usize` because every access happens under the outer
    /// mutex - no `Arc<UnsafeCell>` aliasing concerns since
    /// the field lives on `DBImplCore`, not `MemTable`.
    pub(super) mem_memory_usage: usize,
}

/// Inserts WriteBatch records into a memtable, assigning
/// monotonic sequence numbers starting from `sequence`.
struct MemTableInserter<'a, C: Comparator + Clone> {
    sequence: u64,
    mem: &'a SharedMemTable<C>,
    /// Phase N+: running total of encoded entry sizes. The
    /// caller (`write_with_options`) folds this into
    /// `DBImplCore::mem_memory_usage` after iterate finishes.
    bytes_added: usize,
}

impl<'a, C: Comparator + Clone> WriteBatchHandler for MemTableInserter<'a, C> {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.bytes_added += self.mem.add(self.sequence, ValueType::Value, key, value);
        self.sequence += 1;
    }
    fn delete(&mut self, key: &[u8]) {
        self.bytes_added += self.mem.add(self.sequence, ValueType::Deletion, key, b"");
        self.sequence += 1;
    }
}

// Same insertions, driven directly off a decoded WAL record (no
// intermediate `WriteBatch`) - used by `recover_log_file` to replay the
// log without per-record allocation.
impl<'a, C: Comparator + Clone> crate::write_batch::WriteBatchRecordVisitor
    for MemTableInserter<'a, C>
{
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.bytes_added += self.mem.add(self.sequence, ValueType::Value, key, value);
        self.sequence += 1;
    }
    fn delete(&mut self, key: &[u8]) {
        self.bytes_added += self.mem.add(self.sequence, ValueType::Deletion, key, b"");
        self.sequence += 1;
    }
}

impl<C: Comparator + Clone + 'static, E: Env + Clone + 'static> DBImplCore<C, E> {
    /// Open or create a database at `dbname`.
    ///
    /// On a fresh directory: writes an initial CURRENT + MANIFEST,
    /// allocates a log file. On an existing directory: reads CURRENT,
    /// recovers the manifest, replays log records into the memtable.
    pub fn open(dbname: &str, mut env: E, comparator: C, options: Options) -> Result<Self> {
        // Stamp the durability policy onto the env before any
        // file is created, so every writable file it produces -
        // and every clone handed to VersionSet / TableCache -
        // inherits `Options::sync_mode`.
        env.set_sync_mode(options.sync_mode);
        env.create_dir(Path::new(dbname))?;
        let lock_path = lock_file_name(dbname);
        let db_lock = env.lock_file(Path::new(&lock_path))?;

        let mut version_set = VersionSet::new(dbname, env.clone(), comparator.clone());
        let icmp = version_set.icmp().clone();
        // Phase C: honor `Options::block_cache` if provided,
        // otherwise build a default `ShardedLRUCache` of size
        // `block_cache_size` when no cache was supplied.
        // Phase E: pass compressor through to TableCache so
        // every Table opened via cache miss can decompress
        // matching blocks.
        let block_cache_for_cache = options.block_cache.clone().unwrap_or_else(|| {
            std::sync::Arc::new(crate::cache::ShardedLRUCache::new(options.block_cache_size))
                as crate::table_cache::BlockCache
        });
        let table_cache = TableCache::new_full(
            dbname,
            env.clone(),
            icmp.clone(),
            options.max_open_files,
            block_cache_for_cache,
            options.filter_policy.clone(),
            options.compressor.clone(),
        );
        let mut mem = SharedMemTable::new(comparator.clone());

        let current_path = current_file_name(dbname);
        let exists = env.file_exists(Path::new(&current_path));
        if exists && options.error_if_exists {
            return Err(Status::invalid_argument(
                "DB exists but error_if_exists is set",
            ));
        }
        if !exists && !options.create_if_missing {
            return Err(Status::not_found(
                "DB missing and create_if_missing is false",
            ));
        }

        let mut last_log_to_replay: Vec<u64> = Vec::new();
        // reuse_logs state: when set, recovery kept the memtable live and
        // the existing WAL is reopened for append instead of being
        // flushed to an SST. `mem_usage_seed` carries the replayed byte
        // count so the flush trigger accounts for the resumed memtable.
        let mut reuse_log = false;
        let mut mem_usage_seed = 0usize;
        if exists {
            version_set.recover()?;
            // Phase F: comparator-name verification.
            // Refuse to open with a different comparator than
            // was used when the DB was created.
            if let Some(stored) = version_set.recovered_comparator_name() {
                if stored != comparator.name().as_bytes() {
                    return Err(Status::invalid_argument(
                        "comparator does not match value in the existing DB",
                    ));
                }
            }
            // Find log files with number >= the manifest's log_number
            // and replay them into the memtable.
            let entries = env.list_dir(Path::new(dbname))?;
            let min_log = version_set.log_number();
            let prev_log = version_set.prev_log_number();
            for entry in entries {
                let name = entry.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if let Some((number, FileType::LogFile)) = parse_file_name(name) {
                    if number >= min_log || number == prev_log {
                        last_log_to_replay.push(number);
                    }
                }
            }
            last_log_to_replay.sort();

            let mut max_sequence = version_set.last_sequence();
            let mut replayed_bytes = 0usize;
            for log_number in &last_log_to_replay {
                let (seq, bytes) =
                    recover_log_file(&env, dbname, *log_number, &mem, options.paranoid_checks)?;
                replayed_bytes += bytes;
                if seq > max_sequence {
                    max_sequence = seq;
                }
            }
            version_set.set_last_sequence(max_sequence);

            // reuse_logs decision: if recovery consumed exactly the
            // current log and what it replayed still fits the write
            // buffer, keep the memtable live and reopen that same WAL for
            // append (below) - skipping both the SST flush and the
            // fresh-manifest write. Falls back to flushing when there are
            // multiple logs (a prev-log was rolling) or the replayed data
            // is too large to keep resident.
            reuse_log = last_log_to_replay.len() == 1
                && last_log_to_replay[0] == version_set.log_number()
                && replayed_bytes < options.write_buffer_size;

            if reuse_log {
                mem_usage_seed = replayed_bytes;
            } else {
                let recovered_entries = mem.collect_entries();
                if !recovered_entries.is_empty() {
                    let file_number = version_set.new_file_number();
                    let path = table_file_name(dbname, file_number);
                    let file = env.new_writable_file(Path::new(&path))?;
                    let mut builder = TableFileBuilder::with_options(
                        icmp.clone(),
                        options.block_size,
                        options.block_restart_interval,
                        file,
                        options.filter_policy.clone(),
                        options.compressor.clone(),
                    );
                    let smallest = recovered_entries.first().map(|(k, _)| k.clone()).unwrap();
                    let largest = recovered_entries.last().map(|(k, _)| k.clone()).unwrap();
                    for (ik, value) in &recovered_entries {
                        builder.add(ik, value)?;
                    }
                    builder.finish()?;
                    let file_size = builder.file_size();
                    builder.sync()?;
                    builder.close()?;

                    let output_level = version_set.current().pick_level_for_memtable_output(
                        &smallest[..smallest.len() - 8],
                        &largest[..largest.len() - 8],
                    );
                    let mut edit = crate::version_set::VersionEdit::default();
                    edit.new_files.push(crate::version_set::NewFile {
                        level: output_level as u32,
                        meta: crate::version_set::FileMetaData {
                            number: file_number,
                            file_size,
                            smallest,
                            largest,
                        },
                    });
                    version_set.log_and_apply(&mut edit)?;
                    mem = SharedMemTable::new(comparator.clone());
                }
            }
        }

        // Choose the active WAL. reuse_logs: reopen the recovered log for
        // append in place (no new file number, no manifest write). Else
        // allocate a fresh log and record it in a manifest - this also
        // mints the initial manifest for a freshly-created DB.
        let (log_file_number, log_writer, log_file) = if reuse_log {
            let n = version_set.log_number();
            let log_file_path = log_file_name(dbname, n);
            let tail_len = env.get_file_size(Path::new(&log_file_path)).unwrap_or(0);
            let writer = LogWriter::resume(tail_len);
            let file = env.new_appendable_file(Path::new(&log_file_path))?;
            (n, writer, file)
        } else {
            let n = version_set.new_file_number();
            let mut edit = crate::version_set::VersionEdit {
                log_number: Some(n),
                comparator: Some(comparator.name().as_bytes().to_vec()),
                ..Default::default()
            };
            version_set.log_and_apply(&mut edit)?;
            let log_file_path = log_file_name(dbname, n);
            let file = env.new_writable_file(Path::new(&log_file_path))?;
            (n, LogWriter::new(), file)
        };

        // Phase J: announce the successful open.
        if let Some(l) = options.info_log.as_ref() {
            l.log(&format!(
                "DB::Open: {} (last_sequence={}, log_number={})",
                dbname,
                version_set.last_sequence(),
                log_file_number,
            ));
        }

        let db = Self {
            dbname: dbname.to_string(),
            env,
            comparator,
            icmp,
            options,
            mem,
            imm: None,
            log_writer: Some(log_writer),
            log_file: Some(log_file),
            _db_lock: db_lock,
            log_file_number,
            version_set,
            table_cache,
            snapshots: Vec::new(),
            active_iterators: 0,
            obsolete_table_file_numbers: Vec::new(),
            shutting_down: false,
            bg_work_scheduled: false,
            bg_work_running: false,
            manual_compaction_running: false,
            bg_error: None,
            bg_thread_handle: None,
            writers: VecDeque::new(),
            next_writer_id: 1,
            stats: vec![CompactionStats::default(); NUM_LEVELS],
            mem_memory_usage: mem_usage_seed,
        };
        // Recovery replayed the old WAL logs into an SST and wrote
        // a fresh MANIFEST. Delete the now-obsolete log and MANIFEST
        // files so repeated opens do not leak them on disk.
        db.delete_obsolete_logs();
        db.delete_obsolete_manifests();
        Ok(db)
    }

    fn open_log_file(&self, log_file_number: u64) -> Result<E::Writable> {
        let log_path = log_file_name(&self.dbname, log_file_number);
        self.env.new_writable_file(Path::new(&log_path))
    }

    fn switch_active_log(&mut self, new_log: u64) -> Result<()> {
        let new_log_file = self.open_log_file(new_log)?;
        if let Some(log_file) = self.log_file.as_mut() {
            log_file.close()?;
        }
        self.log_writer = Some(LogWriter::new());
        self.log_file_number = new_log;
        self.log_file = Some(new_log_file);
        Ok(())
    }

    /// Groups queued writers behind the front writer until the
    /// size limit or sync boundary is reached.
    fn build_batch_group(&mut self) -> (PooledWriteBatch, usize, bool) {
        let first = self.writers.front_mut().expect("writer queue is empty");
        let first_sync = first.sync;
        // The grouped writers' batches are never read again once
        // the group is built (finish_writer_group only touches
        // `result`), so move the front batch out rather than
        // cloning it.
        let mut result = first
            .batch
            .take()
            .expect("front writer batch already consumed");
        let mut size = result.approximate_size();
        let max_size = if size <= MAX_BATCH_GROUP_OVERHEAD {
            size + MAX_BATCH_GROUP_OVERHEAD
        } else {
            MAX_BATCH_GROUP_SIZE
        };
        let mut count = 1usize;
        for writer in self.writers.iter().skip(1) {
            if writer.sync && !first_sync {
                break;
            }
            let batch = writer
                .batch
                .as_ref()
                .expect("queued writer batch already consumed");
            let new_size = size.saturating_add(batch.approximate_size());
            if new_size > max_size {
                break;
            }
            result.append(batch);
            size = new_size;
            count += 1;
        }
        (result, count, first_sync)
    }

    fn finish_writer_group(&mut self, count: usize, status: Result<()>) {
        for _ in 0..count {
            let writer = self
                .writers
                .pop_front()
                .expect("writer group exceeds queue");
            // A leader left `result` unset; only followers need it.
            if let Some(result) = &writer.result {
                *result.lock().unwrap() = Some(status.clone());
            }
        }
    }

    /// Looks up a key. Searches the active memtable, then
    /// the immutable memtable (if a flush is pending), then the
    /// per-level SSTables via `VersionSet::Version::get`. If
    /// `options.snapshot` is `Some`, lookups observe state at
    /// that snapshot's sequence number; otherwise they see the
    /// latest writes.
    pub fn get_with_options(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot = match &options.snapshot {
            Some(s) => s.sequence(),
            None => self.version_set.last_sequence(),
        };
        let lookup = LookupKey::new(key, snapshot);

        if let Some(result) = self.mem.get(&lookup) {
            return match result {
                Ok(v) => Ok(Some(v)),
                Err(e) if e.is_not_found() => Ok(None),
                Err(e) => Err(e),
            };
        }
        if let Some(imm) = &self.imm {
            if let Some(result) = imm.get(&lookup) {
                return match result {
                    Ok(v) => Ok(Some(v)),
                    Err(e) if e.is_not_found() => Ok(None),
                    Err(e) => Err(e),
                };
            }
        }
        // Fall through to SSTables. Closure returns the
        // tristate `LookupResult` so a tombstone in a higher
        // level shadows lower-level Values.
        let v = self.version_set.current();
        let table_cache = &self.table_cache;
        // Phase 52: paranoid_checks forces CRC verify; otherwise
        // honor the per-call ReadOptions::verify_checksums.
        let verify = self.options.paranoid_checks || options.verify_checksums;
        // Phase F: ReadOptions::fill_cache plumbs through to the
        // block cache insertion in `read_block_cached_full`.
        let fill_cache = options.fill_cache;
        v.get(key, lookup.internal_key(), |file_number, internal_key| {
            table_cache.internal_get_full(file_number, internal_key, verify, fill_cache)
        })
    }

    pub fn options(&self) -> &Options {
        &self.options
    }
    pub fn dbname(&self) -> &str {
        &self.dbname
    }
    pub fn last_sequence(&self) -> u64 {
        self.version_set.last_sequence()
    }
    pub fn approximate_memtable_usage(&self) -> usize {
        self.mem_memory_usage
    }

    fn delete_obsolete_logs(&self) {
        for entry in self
            .env
            .list_dir(Path::new(&self.dbname))
            .unwrap_or_default()
        {
            let Some(name) = entry.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some((number, FileType::LogFile)) = parse_file_name(name) else {
                continue;
            };
            if number < self.log_file_number {
                let path = log_file_name(&self.dbname, number);
                let _ = self.env.delete_file(Path::new(&path));
            }
        }
    }

    /// Deletes every MANIFEST file except the live one (the one
    /// named by CURRENT). Recovery in `open` writes a fresh
    /// MANIFEST, leaving the previously-current one obsolete.
    fn delete_obsolete_manifests(&self) {
        let live = self.version_set.manifest_file_number();
        for entry in self
            .env
            .list_dir(Path::new(&self.dbname))
            .unwrap_or_default()
        {
            let Some(name) = entry.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some((number, FileType::DescriptorFile)) = parse_file_name(name) else {
                continue;
            };
            if number != live {
                let path = descriptor_file_name(&self.dbname, number);
                let _ = self.env.delete_file(Path::new(&path));
            }
        }
    }

    fn delete_table_file_numbers(&self, numbers: &[u64]) {
        for number in numbers {
            self.table_cache.evict(*number);
            let ldb = table_file_name(&self.dbname, *number);
            let _ = self.env.delete_file(Path::new(&ldb));
            let sst = sst_table_file_name(&self.dbname, *number);
            let _ = self.env.delete_file(Path::new(&sst));
        }
    }

    fn retire_table_file_numbers(&mut self, mut numbers: Vec<u64>) {
        if numbers.is_empty() {
            return;
        }
        numbers.sort_unstable();
        numbers.dedup();
        if self.active_iterators == 0 {
            self.delete_table_file_numbers(&numbers);
        } else {
            for number in &numbers {
                self.table_cache.evict(*number);
            }
            self.obsolete_table_file_numbers.extend(numbers);
        }
    }

    fn register_iterator(&mut self) {
        self.active_iterators += 1;
    }

    fn release_iterator(&mut self) {
        if self.active_iterators > 0 {
            self.active_iterators -= 1;
        }
        if self.active_iterators == 0 && !self.obsolete_table_file_numbers.is_empty() {
            let mut numbers = std::mem::take(&mut self.obsolete_table_file_numbers);
            numbers.sort_unstable();
            numbers.dedup();
            self.delete_table_file_numbers(&numbers);
        }
    }

    /// Records a read sample. When a read overlaps
    /// 2+ files at the same level, decrements the first match's
    /// seek allowance. Once a file's allowance hits zero, it's
    /// marked for seek-triggered compaction.
    ///
    /// Returns true iff this call set a new file_to_compact (so
    /// the wrapper can signal the background thread).
    pub fn record_read_sample(&self, internal_key: &[u8]) -> bool {
        self.version_set.current().record_read_sample(internal_key)
    }

    fn has_pending_seek_compaction(&self) -> bool {
        self.version_set.current().has_file_to_compact()
    }

    /// Creates a snapshot. Returns a handle pinned to
    /// the current `last_sequence`. The snapshot remains live
    /// while any `Arc` clone is held; compaction respects it
    /// and won't drop entries the snapshot can still observe.
    /// Drop the `Arc` (or call `release_snapshot`) to release.
    pub fn get_snapshot(&mut self) -> Arc<Snapshot> {
        let snap = Arc::new(Snapshot {
            sequence: self.version_set.last_sequence(),
        });
        // Garbage-collect Weaks whose strong_count has dropped to 0.
        self.snapshots.retain(|w| w.strong_count() > 0);
        self.snapshots.push(Arc::downgrade(&snap));
        snap
    }

    /// Releases a snapshot. Takes ownership of the
    /// caller's `Arc` and drops it. If no other clones exist,
    /// the snapshot is freed and the next compaction is allowed
    /// to drop entries that were previously held alive for it.
    pub fn release_snapshot(&mut self, snapshot: Arc<Snapshot>) {
        drop(snapshot);
        self.snapshots.retain(|w| w.strong_count() > 0);
    }

    /// Smallest sequence currently pinned by any live snapshot.
    /// Used by compaction's tombstone-drop and version-dedup
    /// checks. If no snapshots are live, returns `last_sequence`
    /// (everything <= last_sequence is safe to drop).
    fn smallest_snapshot_sequence(&self) -> SequenceNumber {
        self.snapshots
            .iter()
            .filter_map(|w| w.upgrade())
            .map(|s| s.sequence)
            .min()
            .unwrap_or_else(|| self.version_set.last_sequence())
    }

    /// Creates an iterator. Returns a DBIter that
    /// merges the active memtable, the immutable memtable
    /// (if a flush is pending), and every SST in the current
    /// version. If `options.snapshot` is `Some`, the iterator
    /// observes state at that snapshot's sequence; otherwise
    /// it captures the current `last_sequence`.
    ///
    /// Table children are lazy: iterator construction opens the
    /// table/index, and data blocks are read as iteration moves.
    pub fn new_iterator_with_options(
        &self,
        options: &ReadOptions,
    ) -> Result<DBIter<C, MergingIterator<InternalKeyComparator<C>>>> {
        let snapshot = match &options.snapshot {
            Some(s) => s.sequence(),
            None => self.version_set.last_sequence(),
        };
        let mut children: Vec<Box<dyn crate::db_iter::DbIterator>> = Vec::new();

        // (1) Active memtable.
        children.push(Box::new(self.mem.new_iterator()));

        // (2) Immutable memtable, if a flush is pending.
        if let Some(imm) = &self.imm {
            children.push(Box::new(imm.new_iterator()));
        }

        // (3) SST iterators. L0 files overlap so each one is
        //     its own child; L1+ files within a level are
        //     non-overlapping and can be concatenated into a
        //     single sorted stream per level.
        // Phase 52: paranoid_checks forces verify; otherwise
        // honor ReadOptions::verify_checksums.
        let verify = self.options.paranoid_checks || options.verify_checksums;
        let v = self.version_set.current();
        for level in 0..NUM_LEVELS {
            let files = v.level_files(level);
            if files.is_empty() {
                continue;
            }
            if level == 0 {
                for f in files {
                    let iter = self.table_cache.new_iterator_verify(f.number, verify)?;
                    children.push(Box::new(iter));
                }
            } else {
                let iter =
                    LevelIterator::new(files, self.icmp.clone(), self.table_cache.clone(), verify);
                children.push(Box::new(iter));
            }
        }

        let merging = MergingIterator::new(self.icmp.clone(), children);
        Ok(DBIter::new(self.comparator.clone(), merging, snapshot))
    }

    /// TEST-style raw internal iterator. Unlike
    /// `new_iterator`, this preserves hidden values and
    /// deletion markers.
    fn new_internal_iterator_for_test(
        &self,
        verify: bool,
    ) -> Result<MergingIterator<InternalKeyComparator<C>>> {
        let mut children: Vec<Box<dyn crate::db_iter::DbIterator>> = Vec::new();
        children.push(Box::new(self.mem.new_iterator()));
        if let Some(imm) = &self.imm {
            children.push(Box::new(imm.new_iterator()));
        }
        let v = self.version_set.current();
        for level in 0..NUM_LEVELS {
            let files = v.level_files(level);
            if files.is_empty() {
                continue;
            }
            if level == 0 {
                for f in files {
                    let iter = self.table_cache.new_iterator_verify(f.number, verify)?;
                    children.push(Box::new(iter));
                }
            } else {
                let iter =
                    LevelIterator::new(files, self.icmp.clone(), self.table_cache.clone(), verify);
                children.push(Box::new(iter));
            }
        }
        Ok(MergingIterator::new(self.icmp.clone(), children))
    }

    /// TEST-style `AllEntriesFor`: returns every raw entry for
    /// `user_key` in internal-key order. `Some(value)` is a
    /// value entry; `None` is a deletion marker.
    fn test_all_entries_for(&self, user_key: &[u8]) -> Result<Vec<Option<Vec<u8>>>> {
        let verify = self.options.paranoid_checks;
        let mut iter = self.new_internal_iterator_for_test(verify)?;
        let target =
            crate::format::InternalKey::new(user_key, MAX_SEQUENCE_NUMBER, ValueType::Value);
        iter.seek(target.encode());
        let mut entries = Vec::new();
        while iter.valid() {
            let parsed = parse_internal_key(iter.key()).ok_or_else(|| {
                Status::corruption("corrupted internal key in TEST_AllEntriesFor")
            })?;
            if self.comparator.compare(&parsed.user_key, user_key).is_ne() {
                break;
            }
            match parsed.value_type {
                ValueType::Value => entries.push(Some(iter.value().to_vec())),
                ValueType::Deletion => entries.push(None),
            }
            iter.next();
        }
        iter.status()?;
        Ok(entries)
    }

    /// Looks up a named property. Recognized properties:
    ///   * `novakv.num-files-at-level<N>` - file count at level N
    ///   * `novakv.stats` - per-level summary table
    ///   * `novakv.sstables` - per-file listing
    ///
    /// Returns `None` for unknown properties.
    pub fn get_property(&self, property: &str) -> Option<String> {
        const NUM_FILES_PREFIX: &str = "novakv.num-files-at-level";
        let v = self.version_set.current();
        if let Some(suffix) = property.strip_prefix(NUM_FILES_PREFIX) {
            let level: usize = suffix.parse().ok()?;
            if level >= NUM_LEVELS {
                return None;
            }
            return Some(v.num_files(level).to_string());
        }
        match property {
            "novakv.stats" => {
                // Build the per-level summary table. Skip levels
                // with zero files and zero recorded micros.
                let mut out = String::new();
                out.push_str("                               Compactions\n");
                out.push_str("Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n");
                out.push_str("--------------------------------------------------\n");
                for level in 0..NUM_LEVELS {
                    let files = v.num_files(level);
                    let s = self.stats[level];
                    if s.micros == 0 && files == 0 {
                        continue;
                    }
                    let level_files = v.level_files(level);
                    let total_bytes: u64 = level_files.iter().map(|f| f.file_size).sum();
                    out.push_str(&format!(
                        "{:>3} {:>8} {:>8.0} {:>9.0} {:>8.0} {:>9.0}\n",
                        level,
                        files,
                        (total_bytes as f64) / 1048576.0,
                        (s.micros as f64) / 1e6,
                        (s.bytes_read as f64) / 1048576.0,
                        (s.bytes_written as f64) / 1048576.0,
                    ));
                }
                Some(out)
            }
            "novakv.sstables" => {
                let mut out = String::new();
                for level in 0..NUM_LEVELS {
                    let files = v.level_files(level);
                    if files.is_empty() {
                        continue;
                    }
                    out.push_str(&format!("--- level {} ---\n", level));
                    for f in files {
                        out.push_str(&format!(
                            "  {}: {} bytes [{:?}..{:?}]\n",
                            f.number,
                            f.file_size,
                            &f.smallest[..f.smallest.len().min(8)],
                            &f.largest[..f.largest.len().min(8)],
                        ));
                    }
                }
                Some(out)
            }
            _ => None,
        }
    }

    /// Estimates on-disk sizes for key ranges. Converts each user
    /// bound to the corresponding max-sequence internal key,
    /// asks the LSM for both approximate offsets, and returns
    /// `limit - start` when the offsets are ordered.
    pub fn get_approximate_sizes(&self, ranges: &[Range]) -> Vec<u64> {
        let v = self.version_set.current();
        // The internal-key tag for a seek is constant. Reuse one
        // scratch buffer for the encoded bounds instead of building
        // an `InternalKey` (and its backing `Vec`) per range bound.
        let tag = pack_sequence_and_type(MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK).to_le_bytes();
        let mut scratch = Vec::new();
        let mut sizes = Vec::with_capacity(ranges.len());
        for r in ranges {
            scratch.clear();
            scratch.extend_from_slice(&r.start);
            scratch.extend_from_slice(&tag);
            let start = self.approximate_offset_of(v.as_ref(), &scratch);
            scratch.clear();
            scratch.extend_from_slice(&r.limit);
            scratch.extend_from_slice(&tag);
            let limit = self.approximate_offset_of(v.as_ref(), &scratch);
            sizes.push(limit.saturating_sub(start));
        }
        sizes
    }

    /// Estimates the file offset of an internal key in the LSM.
    fn approximate_offset_of(&self, v: &Version<C>, internal_key: &[u8]) -> u64 {
        let mut result = 0u64;
        for level in 0..NUM_LEVELS {
            let files = v.level_files(level);
            for f in files {
                if v.comparator().compare(&f.largest, internal_key).is_le() {
                    result = result.saturating_add(f.file_size);
                } else if v.comparator().compare(&f.smallest, internal_key).is_gt() {
                    if level > 0 {
                        break;
                    }
                } else if let Ok(offset) = self
                    .table_cache
                    .approximate_offset_of(f.number, internal_key)
                {
                    result = result.saturating_add(offset);
                }
            }
        }
        result
    }
}

pub(super) fn bg_step_async<
    C: Comparator + Clone + Send + Sync + 'static,
    E: Env + Clone + Send + Sync + 'static,
>(
    inner: &Arc<(std::sync::Mutex<DBImplCore<C, E>>, std::sync::Condvar)>,
) -> Result<()> {
    // Phase 1: flush pending imm if any.
    let has_imm = { inner.0.lock().unwrap().imm.is_some() };
    if has_imm {
        compact_memtable_async(inner)?;
    }
    // Phase 2: drain pending compactions.
    loop {
        let next = {
            let mut g = inner.0.lock().unwrap();
            g.version_set.pick_compaction()
        };
        let Some(c) = next else { break };
        do_compaction_work_async(inner, c)?;
    }
    Ok(())
}

/// Lock-releasing memtable flush. Releases the DB mutex
/// around the SST build + write. The lock is held only to take
/// the imm + allocate a file number, and again at the end
/// to apply the version edit.
fn compact_memtable_async<
    C: Comparator + Clone + Send + Sync + 'static,
    E: Env + Clone + Send + Sync + 'static,
>(
    inner: &Arc<(std::sync::Mutex<DBImplCore<C, E>>, std::sync::Condvar)>,
) -> Result<()> {
    // Time the L0 table write: start before
    // we touch I/O, accumulate per-level stats at the end.
    let start = Instant::now();
    // Step 1 (under lock): collect imm entries, allocate file number.
    // Note: we do NOT `take` imm here. The imm is kept
    // alive throughout the flush so iterators created during
    // the flush can still see its entries. We only clear imm
    // at the end (step 3), after the new SST is registered in
    // the manifest - so the data is always visible from one
    // source or the other.
    let snapshot = {
        let mut g = inner.0.lock().unwrap();
        let entries = match &g.imm {
            Some(m) => m.collect_entries(),
            None => return Ok(()),
        };
        if entries.is_empty() {
            g.imm = None;
            inner.1.notify_all();
            return Ok(());
        }
        let smallest = entries.first().map(|(k, _)| k.clone()).unwrap();
        let largest = entries.last().map(|(k, _)| k.clone()).unwrap();
        let output_level = g.version_set.current().pick_level_for_memtable_output(
            &smallest[..smallest.len() - 8],
            &largest[..largest.len() - 8],
        );
        let file_number = g.version_set.new_file_number();
        let icmp = g.icmp.clone();
        let dbname = g.dbname.clone();
        let env = g.env.clone();
        let log_number = g.log_file_number;
        let filter_policy = g.options.filter_policy.clone();
        let compressor = g.options.compressor.clone();
        let block_size = g.options.block_size;
        let block_restart_interval = g.options.block_restart_interval;
        let info_log = g.options.info_log.clone();
        (
            entries,
            file_number,
            output_level,
            icmp,
            dbname,
            env,
            log_number,
            filter_policy,
            compressor,
            block_size,
            block_restart_interval,
            info_log,
        )
    };
    let (
        entries,
        file_number,
        output_level,
        icmp,
        dbname,
        env,
        log_number,
        filter_policy,
        compressor,
        block_size,
        block_restart_interval,
        info_log,
    ) = snapshot;

    // Phase J: announce the flush to the user-supplied logger.
    if let Some(l) = info_log.as_ref() {
        l.log(&format!(
            "Level-0 table #{}: started ({} entries)",
            file_number,
            entries.len()
        ));
    }

    // Step 2 (no lock): build SST + stream it to env.
    let path = table_file_name(&dbname, file_number);
    let file = env.new_writable_file(Path::new(&path))?;
    let mut builder = TableFileBuilder::with_options(
        icmp,
        block_size,
        block_restart_interval,
        file,
        filter_policy,
        compressor,
    );
    let smallest = entries.first().map(|(k, _)| k.clone()).unwrap();
    let largest = entries.last().map(|(k, _)| k.clone()).unwrap();
    for (ik, value) in &entries {
        builder.add(ik, value)?;
    }
    builder.finish()?;
    let file_size = builder.file_size();
    builder.sync()?;
    builder.close()?;

    // Step 3 (under lock): apply version edit, then clear imm.
    // Order matters: the new SST must be registered in the
    // manifest BEFORE we drop the imm reference, so iterators
    // never see a gap (the imm reference is dropped only
    // after the version edit is applied).
    {
        let mut g = inner.0.lock().unwrap();
        let mut edit = crate::version_set::VersionEdit::default();
        edit.new_files.push(crate::version_set::NewFile {
            level: output_level as u32,
            meta: crate::version_set::FileMetaData {
                number: file_number,
                file_size,
                smallest,
                largest,
            },
        });
        edit.log_number = Some(log_number);
        g.version_set.log_and_apply(&mut edit)?;
        g.delete_obsolete_logs();
        g.stats[output_level].add(CompactionStats {
            micros: start.elapsed().as_micros() as i64,
            bytes_read: 0,
            bytes_written: file_size as i64,
        });
        if let Some(l) = info_log.as_ref() {
            l.log(&format!(
                "Level-0 table #{}: {} bytes OK ({} micros)",
                file_number,
                file_size,
                start.elapsed().as_micros(),
            ));
        }
        g.imm = None;
        inner.1.notify_all();
    }
    Ok(())
}

/// Lock-releasing compaction.
/// Releases the lock during table reads,
/// merge-sort, and SST writes. Re-acquires only to allocate
/// file numbers and to apply the final version edit.
fn do_compaction_work_async<
    C: Comparator + Clone + Send + Sync + 'static,
    E: Env + Clone + Send + Sync + 'static,
>(
    inner: &Arc<(std::sync::Mutex<DBImplCore<C, E>>, std::sync::Condvar)>,
    compaction: crate::version_set::Compaction<C>,
) -> Result<()> {
    // Time the compaction work. Start before
    // any I/O. Trivial moves don't accumulate stats - they
    // go through a separate path.
    let start = Instant::now();
    // Trivial-move case: no I/O, just metadata under lock.
    if compaction.is_trivial_move() {
        let mut g = inner.0.lock().unwrap();
        let f = compaction.input(0, 0).clone();
        let level = compaction.level();
        let mut edit = crate::version_set::VersionEdit::default();
        edit.deleted_files.push(crate::version_set::DeletedFile {
            level: level as u32,
            number: f.number,
        });
        edit.new_files.push(crate::version_set::NewFile {
            level: (level + 1) as u32,
            meta: f,
        });
        g.version_set.log_and_apply(&mut edit)?;
        inner.1.notify_all();
        return Ok(());
    }

    // Step 1 (under lock): snapshot inputs + comparator + smallest_snapshot.
    let setup = {
        let g = inner.0.lock().unwrap();
        let icmp = g.icmp.clone();
        let dbname = g.dbname.clone();
        let env = g.env.clone();
        let smallest_snapshot = g.smallest_snapshot_sequence();
        // Phase 53: compaction reads honor paranoid_checks.
        let verify = g.options.paranoid_checks;
        // Phase B: SST outputs honor filter_policy.
        let filter_policy = g.options.filter_policy.clone();
        // Phase E: SST inputs may be compressed; outputs honor compressor.
        let compressor = g.options.compressor.clone();
        // Phase F: SST output block-shape tuning.
        let block_size = g.options.block_size;
        let block_restart_interval = g.options.block_restart_interval;
        // Phase J: optional logger.
        let info_log = g.options.info_log.clone();
        let level = compaction.level();
        let mut input_paths: Vec<String> = Vec::new();
        for which in 0..2 {
            for f in compaction.inputs(which) {
                input_paths.push(table_file_name(&dbname, f.number));
            }
        }
        (
            icmp,
            dbname,
            env,
            smallest_snapshot,
            verify,
            filter_policy,
            compressor,
            block_size,
            block_restart_interval,
            info_log,
            level,
            input_paths,
        )
    };
    let (
        icmp,
        dbname,
        env,
        smallest_snapshot,
        verify,
        filter_policy,
        compressor,
        block_size,
        block_restart_interval,
        info_log,
        level,
        input_paths,
    ) = setup;
    let _ = level; // silence unused (kept for symmetry with edit step)

    // Phase J: announce the compaction.
    if let Some(l) = info_log.as_ref() {
        let n0 = compaction.inputs(0).len();
        let n1 = compaction.inputs(1).len();
        l.log(&format!(
            "Compacting {}@{} + {}@{} files",
            n0,
            level,
            n1,
            level + 1,
        ));
    }

    // Step 2 (no lock): read all input files, sort, merge.
    let mut all: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for path in &input_paths {
        let path = Path::new(path);
        let file_size = env.get_file_size(path)?;
        let file = env.new_random_access_file(path)?;
        let table = crate::table::Table::open_random_with_options(
            file,
            file_size,
            icmp.clone(),
            None,
            None,
            compressor.clone(),
        )?;
        let mut iter = table.new_iterator_verify(verify)?;
        iter.seek_to_first();
        while iter.valid() {
            all.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next();
        }
        iter.status()?;
    }
    all.sort_by(|a, b| icmp.compare(&a.0, &b.0));

    // Step 3 (no lock except brief file-number allocations):
    // walk merged stream, dedupe, build output SSTs.
    let mut outputs: Vec<crate::version_set::FileMetaData> = Vec::new();
    let mut builder: Option<TableFileBuilder<InternalKeyComparator<C>, E::Writable>> = None;
    let mut current_file_number: u64 = 0;
    let mut current_smallest: Vec<u8> = Vec::new();
    let mut current_largest: Vec<u8> = Vec::new();
    let mut last_user_key: Option<Vec<u8>> = None;
    let mut last_sequence_for_key: SequenceNumber = u64::MAX;

    for (ik, value) in &all {
        let user_key = ik[..ik.len() - 8].to_vec();
        let tag = crate::coding::decode_fixed64(&ik[ik.len() - 8..]);
        let value_type = (tag & 0xff) as u8;
        let sequence = tag >> 8;
        let same_user_key = last_user_key.as_ref() == Some(&user_key);
        if !same_user_key {
            last_sequence_for_key = u64::MAX;
        }
        let should_drop = last_sequence_for_key <= smallest_snapshot
            || value_type == ValueType::Deletion as u8
                && sequence <= smallest_snapshot
                && compaction.is_base_level_for_key(&user_key);
        last_user_key = Some(user_key.clone());
        last_sequence_for_key = sequence;
        if should_drop {
            continue;
        }

        if builder.is_none() {
            let n = {
                let mut g = inner.0.lock().unwrap();
                g.version_set.new_file_number()
            };
            let path = table_file_name(&dbname, n);
            let file = env.new_writable_file(Path::new(&path))?;
            let b = TableFileBuilder::with_options(
                icmp.clone(),
                block_size,
                block_restart_interval,
                file,
                filter_policy.clone(),
                compressor.clone(),
            );
            builder = Some(b);
            current_file_number = n;
            current_smallest = ik.clone();
        }
        current_largest = ik.clone();
        builder.as_mut().unwrap().add(ik, value)?;
        last_user_key = Some(user_key);

        if builder.as_ref().unwrap().file_size() >= compaction.max_output_file_size() {
            let b = builder.take().unwrap();
            let meta = finish_compaction_output_async(
                b,
                current_file_number,
                current_smallest.clone(),
                current_largest.clone(),
            )?;
            outputs.push(meta);
        }
    }

    if let Some(b) = builder.take() {
        let meta = finish_compaction_output_async(
            b,
            current_file_number,
            current_smallest,
            current_largest,
        )?;
        outputs.push(meta);
    }

    // Step 4 (under lock): build VersionEdit + log_and_apply.
    let mut bytes_read: i64 = 0;
    for which in 0..2 {
        for f in compaction.inputs(which) {
            bytes_read += f.file_size as i64;
        }
    }
    let mut bytes_written: i64 = 0;
    for meta in &outputs {
        bytes_written += meta.file_size as i64;
    }
    let output_count = outputs.len();
    {
        let mut g = inner.0.lock().unwrap();
        let mut edit = crate::version_set::VersionEdit::default();
        for cp in &compaction.edit().compact_pointers {
            edit.compact_pointers.push(cp.clone());
        }
        for which in 0..2 {
            for f in compaction.inputs(which) {
                edit.deleted_files.push(crate::version_set::DeletedFile {
                    level: (compaction.level() + which) as u32,
                    number: f.number,
                });
            }
        }
        for meta in outputs {
            edit.new_files.push(crate::version_set::NewFile {
                level: (compaction.level() + 1) as u32,
                meta,
            });
        }
        g.version_set.log_and_apply(&mut edit)?;
        let mut obsolete_numbers = Vec::new();
        for which in 0..2 {
            for f in compaction.inputs(which) {
                obsolete_numbers.push(f.number);
            }
        }
        g.retire_table_file_numbers(obsolete_numbers);
        g.stats[compaction.level() + 1].add(CompactionStats {
            micros: start.elapsed().as_micros() as i64,
            bytes_read,
            bytes_written,
        });
        inner.1.notify_all();
    }
    if let Some(l) = info_log.as_ref() {
        l.log(&format!(
            "Compacted level {} -> {}: {} output files, {} bytes read, {} bytes written ({} micros)",
            level, level + 1, output_count, bytes_read, bytes_written, start.elapsed().as_micros(),
        ));
    }
    Ok(())
}

/// Helper for compaction output. Lock-free - operates only
/// on the local builder + env.
fn finish_compaction_output_async<
    C: Comparator + Clone + Send + Sync + 'static,
    W: WritableFile,
>(
    mut builder: TableFileBuilder<InternalKeyComparator<C>, W>,
    file_number: u64,
    smallest: Vec<u8>,
    largest: Vec<u8>,
) -> Result<crate::version_set::FileMetaData> {
    builder.finish()?;
    let file_size = builder.file_size();
    builder.sync()?;
    builder.close()?;
    Ok(crate::version_set::FileMetaData {
        number: file_number,
        file_size,
        smallest,
        largest,
    })
}

/// The public database trait.
///
/// Methods that have a 1:1 inherent counterpart on `DBImpl`
/// are exposed here so callers can write code generic over
/// the concrete DB type. The associated type `Iter` is the
/// concrete iterator returned by `new_iterator*`.
pub trait DB {
    /// Iterator type returned by `new_iterator*`.
    /// For `DBImpl<C, E>` this is `DBIter<C, MergingIterator<...>>`.
    type Iterator: crate::db_iter::DbIterator;

    /// Inserts a key/value pair. Phase 71: takes `&self` since
    /// interior mutability via `Arc<Mutex<DBImplCore>>` serializes.
    fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;
    /// Removes a key.
    fn delete(&self, options: &WriteOptions, key: &[u8]) -> Result<()>;
    /// Applies a batch atomically.
    fn write(&self, options: &WriteOptions, batch: &crate::write_batch::WriteBatch) -> Result<()>;
    /// Looks up a key.
    fn get(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Creates an iterator.
    fn new_iterator(&self, options: &ReadOptions) -> Result<Self::Iterator>;
    /// Creates a snapshot.
    fn get_snapshot(&self) -> Arc<Snapshot>;
    /// Releases a snapshot.
    fn release_snapshot(&self, snapshot: Arc<Snapshot>);
    /// Looks up a named property.
    fn get_property(&self, property: &str) -> Option<String>;
    /// Estimates on-disk sizes for key ranges.
    fn get_approximate_sizes(&self, ranges: &[Range]) -> Vec<u64>;
    /// Compacts a key range.
    fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()>;
}

impl<C: Comparator + Clone + Send + Sync + 'static, E: Env + Clone + Send + Sync + 'static> DB
    for DBImpl<C, E>
{
    type Iterator = crate::db_iter::DBIter<C, MergingIterator<InternalKeyComparator<C>>>;

    fn put(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with_options(options, key, value)
    }
    fn delete(&self, options: &WriteOptions, key: &[u8]) -> Result<()> {
        self.delete_with_options(options, key)
    }
    fn write(&self, options: &WriteOptions, batch: &crate::write_batch::WriteBatch) -> Result<()> {
        self.write_with_options(options, batch)
    }
    fn get(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_with_options(options, key)
    }
    fn new_iterator(&self, options: &ReadOptions) -> Result<Self::Iterator> {
        self.new_iterator_with_options(options)
    }
    fn get_snapshot(&self) -> Arc<Snapshot> {
        DBImpl::get_snapshot(self)
    }
    fn release_snapshot(&self, snapshot: Arc<Snapshot>) {
        DBImpl::release_snapshot(self, snapshot)
    }
    fn get_property(&self, property: &str) -> Option<String> {
        DBImpl::get_property(self, property)
    }
    fn get_approximate_sizes(&self, ranges: &[Range]) -> Vec<u64> {
        DBImpl::get_approximate_sizes(self, ranges)
    }
    fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        DBImpl::compact_range(self, begin, end)
    }
}

/// Public DB orchestrator.
/// Wraps the algorithmic core in `Arc<Mutex>` and
/// drives flush + compaction from a background thread.
pub struct DBImpl<
    C: Comparator + Clone + Send + Sync + 'static,
    E: Env + Clone + Send + Sync + 'static,
> {
    inner: Arc<(std::sync::Mutex<DBImplCore<C, E>>, std::sync::Condvar)>,
    /// Only the root handle owns the background worker. Cloned handles share the
    /// same DB core but leave shutdown to the root handle.
    bg_thread: Option<std::thread::JoinHandle<()>>,
}

impl<C: Comparator + Clone + Send + Sync + 'static, E: Env + Clone + Send + Sync + 'static> Clone
    for DBImpl<C, E>
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            bg_thread: None,
        }
    }
}

impl<C: Comparator + Clone + Send + Sync + 'static, E: Env + Clone + Send + Sync + 'static>
    DBImpl<C, E>
{
    /// Open or create a database at `dbname`. Constructs the
    /// `DBImplCore`, wraps it, and spawns the background worker.
    pub fn open(dbname: &str, env: E, comparator: C, options: Options) -> Result<Self> {
        let core = DBImplCore::open(dbname, env, comparator, options)?;
        let inner = Arc::new((std::sync::Mutex::new(core), std::sync::Condvar::new()));
        let bg_thread = {
            let inner = inner.clone();
            std::thread::spawn(move || bg_loop(inner))
        };
        // Hand the worker's unparkable handle to the core so
        // writers can wake it directly (see `bg_loop`).
        inner.0.lock().unwrap().bg_thread_handle = Some(bg_thread.thread().clone());
        Ok(Self {
            inner,
            bg_thread: Some(bg_thread),
        })
    }

    /// Applies a batch. Drives the auto-flush check
    /// using the wrapper's Condvar so writes block on the
    /// background worker rather than running flush inline.
    pub fn write_with_options(
        &self,
        options: &WriteOptions,
        batch: &crate::write_batch::WriteBatch,
    ) -> Result<()> {
        // Copy the caller's batch into a pooled buffer - the same
        // single copy a clone would do, into a recycled buffer.
        let mut pooled = pool_take();
        pooled.set_contents(batch.contents())?;
        self.write_owned(options, pooled)
    }

    /// Same as `write_with_options`, but takes ownership of a
    /// pooled batch so the `put`/`delete` fast path enqueues it
    /// into the writer group by move instead of cloning.
    fn write_owned(&self, options: &WriteOptions, batch: PooledWriteBatch) -> Result<()> {
        let (mu, cv) = &*self.inner;
        let mut g = mu.lock().unwrap();
        let writer_id = g.next_writer_id;
        g.next_writer_id = g.next_writer_id.wrapping_add(1);
        g.writers.push_back(PendingWriter {
            id: writer_id,
            batch: Some(batch),
            sync: options.sync,
            result: None,
        });
        // No notify here: enqueueing at the back of the writer
        // queue never changes which writer is at the front, so it
        // wakes no waiting writer usefully - it only churns the
        // background thread (which shares this condvar) into
        // locking, finding no work, and re-parking. Writers that
        // must wait are woken by `finish_writer_group`'s notify
        // when the front group completes.
        //
        // A writer that is already at the front is the leader: it
        // runs the group and returns its status directly, so it
        // never needs a result slot. Only a follower - which may
        // be grouped away and popped by the leader - installs an
        // `Arc<Mutex<_>>` so `finish_writer_group` can hand back
        // its status. This keeps the common single-writer path
        // allocation-free here.
        if g.writers.front().map(|w| w.id) != Some(writer_id) {
            let result = Arc::new(std::sync::Mutex::new(None));
            g.writers
                .back_mut()
                .expect("writer was just enqueued")
                .result = Some(result.clone());
            loop {
                if g.writers.front().map(|w| w.id) == Some(writer_id) {
                    break;
                }
                if let Some(done) = result.lock().unwrap().clone() {
                    return done;
                }
                g = cv.wait(g).unwrap();
            }
        }
        let mut allow_delay = true;
        // Wait-loop that ensures the memtable has room.
        loop {
            if let Some(e) = &g.bg_error {
                let status = Err(e.clone());
                let count = g.writers.len();
                g.finish_writer_group(count, status.clone());
                cv.notify_all();
                return status;
            }
            if allow_delay && g.version_set.current().num_files(0) >= L0_SLOWDOWN_WRITES_TRIGGER {
                allow_delay = false;
                drop(g);
                std::thread::sleep(Duration::from_micros(1000));
                g = mu.lock().unwrap();
                continue;
            }
            if g.mem_memory_usage <= g.options.write_buffer_size {
                break;
            }
            if g.imm.is_some() {
                // Previous flush hasn't finished - wait for bg.
                g = cv.wait(g).unwrap();
                continue;
            }
            if g.version_set.current().num_files(0) >= L0_STOP_WRITES_TRIGGER {
                // L0 backpressure - wait for compaction to drain.
                g = cv.wait(g).unwrap();
                continue;
            }
            // Switch active memtable -> immutable + new log.
            let new_log = g.version_set.new_file_number();
            if let Err(e) = g.switch_active_log(new_log) {
                let status = Err(e);
                let count = g.writers.len();
                g.finish_writer_group(count, status.clone());
                cv.notify_all();
                return status;
            }
            let new_mem = SharedMemTable::new(g.comparator.clone());
            let old_mem = std::mem::replace(&mut g.mem, new_mem);
            g.imm = Some(old_mem);
            // Phase N+: fresh memtable starts at 0 bytes.
            g.mem_memory_usage = 0;
            g.bg_work_scheduled = true;
            if let Some(t) = &g.bg_thread_handle {
                t.unpark();
            }
            cv.notify_all();
            break;
        }
        let (updates, writer_count, sync) = g.build_batch_group();
        let last_sequence = g.version_set.last_sequence();
        let count = updates.count() as u64;
        let sequence = last_sequence + 1;
        let sequence_end = last_sequence + count;
        let sequence_bytes = sequence.to_le_bytes();
        let mut log_writer = g.log_writer.take().expect("log writer is missing");
        let mut log_file = g.log_file.take().expect("log file is missing");
        let mem = g.mem.clone();

        // Release the mutex around WAL append,
        // optional WAL sync, and memtable insertion. The
        // writer queue keeps this as the only writer while
        // the memtable supports concurrent readers.
        drop(g);
        let mut status =
            log_writer.add_record_pair_to(&mut log_file, &sequence_bytes, &updates.contents()[8..]);
        let mut sync_error = false;
        if status.is_ok() && sync {
            status = log_file.sync();
            sync_error = status.is_err();
        }
        let mut bytes_added = 0usize;
        if status.is_ok() {
            mem.reserve_entries(count as usize);
            let mut inserter = MemTableInserter {
                sequence,
                mem: &mem,
                bytes_added: 0,
            };
            status = updates.iterate(&mut inserter);
            bytes_added = inserter.bytes_added;
        }
        g = mu.lock().unwrap();
        g.mem_memory_usage += bytes_added;
        g.log_writer = Some(log_writer);
        g.log_file = Some(log_file);
        if sync_error {
            if let Err(e) = &status {
                g.bg_error = Some(e.clone());
            }
        }
        if status.is_ok() {
            g.version_set.set_last_sequence(sequence_end);
        }
        g.finish_writer_group(writer_count, status.clone());
        cv.notify_all();
        status
    }

    /// Phase 69: takes `impl AsRef<[u8]>` so callers can pass
    /// `&[u8]`, `&Vec<u8>`, `Slice`, or `&str` (anything that
    /// converts to a byte slice).
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.put_with_options(&WriteOptions::default(), key, value)
    }
    pub fn put_with_options<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        options: &WriteOptions,
        key: K,
        value: V,
    ) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        // Draw a recycled buffer from the thread-local pool,
        // fill it, and move it into the writer queue.
        let mut batch = pool_take();
        batch.put(key, value);
        self.write_owned(options, batch)
    }
    /// Phase 69: takes `impl AsRef<[u8]>` for the key.
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.delete_with_options(&WriteOptions::default(), key)
    }
    pub fn delete_with_options<K: AsRef<[u8]>>(
        &self,
        options: &WriteOptions,
        key: K,
    ) -> Result<()> {
        let key = key.as_ref();
        let mut batch = pool_take();
        batch.delete(key);
        self.write_owned(options, batch)
    }
    pub fn write(&self, batch: &crate::write_batch::WriteBatch) -> Result<()> {
        self.write_with_options(&WriteOptions::default(), batch)
    }

    /// Phase 69: takes `impl AsRef<[u8]>` for the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.get_with_options(&ReadOptions::default(), key)
    }
    pub fn get_with_options<K: AsRef<[u8]>>(
        &self,
        options: &ReadOptions,
        key: K,
    ) -> Result<Option<Vec<u8>>> {
        let (mu, cv) = &*self.inner;
        let mut g = mu.lock().unwrap();
        // Phase 66: surface any background-thread error to readers.
        if let Some(e) = &g.bg_error {
            return Err(e.clone());
        }
        let result = g.get_with_options(options, key.as_ref());
        if g.has_pending_seek_compaction() {
            g.bg_work_scheduled = true;
            if let Some(t) = &g.bg_thread_handle {
                t.unpark();
            }
            cv.notify_all();
        }
        result
    }

    pub fn new_iterator(&self) -> Result<DBIter<C, MergingIterator<InternalKeyComparator<C>>>> {
        self.new_iterator_with_options(&ReadOptions::default())
    }
    pub fn new_iterator_with_options(
        &self,
        options: &ReadOptions,
    ) -> Result<DBIter<C, MergingIterator<InternalKeyComparator<C>>>> {
        let (mu, _cv) = &*self.inner;
        let mut iter = {
            let mut g = mu.lock().unwrap();
            // Phase 66: surface bg error to iterator construction.
            if let Some(e) = &g.bg_error {
                return Err(e.clone());
            }
            let iter = g.new_iterator_with_options(options)?;
            g.register_iterator();
            iter
        };
        // Phase 58: hook the read sampler. The closure captures
        // an Arc handle so the iterator survives the wrapper drop
        // (calls become no-ops once the bg thread has exited).
        let inner = self.inner.clone();
        iter.set_sampler(Box::new(move |internal_key: &[u8]| {
            let (mu, cv) = &*inner;
            let mut g = mu.lock().unwrap();
            if g.record_read_sample(internal_key) {
                g.bg_work_scheduled = true;
                if let Some(t) = &g.bg_thread_handle {
                    t.unpark();
                }
                cv.notify_all();
            }
        }));
        let inner = self.inner.clone();
        iter.set_drop_hook(Box::new(move || {
            let (mu, _cv) = &*inner;
            let mut g = mu.lock().unwrap();
            g.release_iterator();
        }));
        Ok(iter)
    }

    pub fn options(&self) -> Options {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        g.options().clone()
    }
    pub fn dbname(&self) -> String {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        g.dbname().to_string()
    }
    pub fn last_sequence(&self) -> u64 {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        g.last_sequence()
    }
    pub fn approximate_memtable_usage(&self) -> usize {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        g.approximate_memtable_usage()
    }

    /// Records a read sample. If this call
    /// causes a file's seek allowance to hit zero, signals the
    /// background worker to schedule a seek-triggered compaction.
    pub fn record_read_sample(&self, internal_key: &[u8]) {
        let (mu, cv) = &*self.inner;
        let mut g = mu.lock().unwrap();
        if g.record_read_sample(internal_key) {
            g.bg_work_scheduled = true;
            if let Some(t) = &g.bg_thread_handle {
                t.unpark();
            }
            cv.notify_all();
        }
    }

    pub fn get_snapshot(&self) -> Arc<Snapshot> {
        let (mu, _cv) = &*self.inner;
        let mut g = mu.lock().unwrap();
        g.get_snapshot()
    }
    pub fn release_snapshot(&self, snapshot: Arc<Snapshot>) {
        let (mu, _cv) = &*self.inner;
        let mut g = mu.lock().unwrap();
        g.release_snapshot(snapshot)
    }

    /// Force-flush the active memtable. Schedules the work on
    /// the background thread and waits for completion.
    pub fn force_flush(&self) -> Result<()> {
        let (mu, cv) = &*self.inner;
        let mut g = mu.lock().unwrap();
        // Do not rotate the active memtable/log while a writer
        // group is between WAL append and memtable insertion.
        while !g.writers.is_empty() {
            if let Some(e) = &g.bg_error {
                return Err(e.clone());
            }
            g = cv.wait(g).unwrap();
        }
        if g.mem_memory_usage == 0 && g.imm.is_none() {
            return Ok(());
        }
        // Wait for any in-progress flush to finish first.
        while g.imm.is_some() {
            if let Some(e) = &g.bg_error {
                return Err(e.clone());
            }
            g = cv.wait(g).unwrap();
        }
        // Switch mem->imm + signal bg.
        let new_log = g.version_set.new_file_number();
        g.switch_active_log(new_log)?;
        let new_mem = SharedMemTable::new(g.comparator.clone());
        let old_mem = std::mem::replace(&mut g.mem, new_mem);
        // Phase N+: fresh memtable starts at 0 bytes.
        g.mem_memory_usage = 0;
        g.imm = Some(old_mem);
        g.bg_work_scheduled = true;
        if let Some(t) = &g.bg_thread_handle {
            t.unpark();
        }
        cv.notify_all();
        // Wait for the bg thread to drain imm.
        while g.imm.is_some() {
            if let Some(e) = &g.bg_error {
                return Err(e.clone());
            }
            g = cv.wait(g).unwrap();
        }
        Ok(())
    }

    /// Schedule a flush + compaction cycle. Returns once the
    /// background thread has drained any pending work.
    pub fn maybe_compact(&self) -> Result<()> {
        let (mu, cv) = &*self.inner;
        let mut g = mu.lock().unwrap();
        g.bg_work_scheduled = true;
        if let Some(t) = &g.bg_thread_handle {
            t.unpark();
        }
        cv.notify_all();
        // Wait until bg thread reports no pending work.
        while g.bg_work_scheduled || g.bg_work_running || g.imm.is_some() {
            if let Some(e) = &g.bg_error {
                return Err(e.clone());
            }
            g = cv.wait(g).unwrap();
        }
        Ok(())
    }

    pub fn get_property(&self, property: &str) -> Option<String> {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        g.get_property(property)
    }
    pub fn get_approximate_sizes(&self, ranges: &[Range]) -> Vec<u64> {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        g.get_approximate_sizes(ranges)
    }
    /// Compacts a key range. Flushes the memtable, then
    /// compacts every level overlapping `[begin, end]` into the
    /// next level. Manual compactions reserve the bg worker slot
    /// but release the mutex around table reads/writes.
    pub fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        self.force_flush()?;

        // Compute max level whose files overlap [begin, end].
        let max_level = {
            let (mu, _cv) = &*self.inner;
            let g = mu.lock().unwrap();
            let v = g.version_set.current();
            let mut max_level = 0usize;
            for level in 1..NUM_LEVELS {
                if v.overlap_in_level(level, begin, end) {
                    max_level = level;
                }
            }
            max_level
        };

        // For each level, run manual compactions until no more
        // files at that level overlap the range.
        for level in 0..=max_level {
            if level + 1 >= NUM_LEVELS {
                break;
            }
            loop {
                let c = {
                    let (mu, cv) = &*self.inner;
                    let mut g = mu.lock().unwrap();
                    // Wait for bg/manual work to be idle before selecting inputs.
                    while g.bg_work_scheduled
                        || g.bg_work_running
                        || g.imm.is_some()
                        || g.manual_compaction_running
                    {
                        if let Some(e) = &g.bg_error {
                            return Err(e.clone());
                        }
                        g = cv.wait(g).unwrap();
                    }
                    let c = g.version_set.pick_manual_compaction(level, begin, end);
                    if c.is_some() {
                        g.manual_compaction_running = true;
                        cv.notify_all();
                    }
                    c
                };
                let Some(c) = c else { break };
                let result = do_compaction_work_async(&self.inner, c);
                {
                    let (mu, cv) = &*self.inner;
                    let mut g = mu.lock().unwrap();
                    g.manual_compaction_running = false;
                    if let Some(t) = &g.bg_thread_handle {
                        t.unpark();
                    }
                    cv.notify_all();
                }
                result?;
            }
        }
        Ok(())
    }

    /// Test hook: force a memtable flush.
    pub fn test_compact_memtable(&self) -> Result<()> {
        self.force_flush()
    }

    /// Test hook: count files at a level.
    pub fn test_num_level_files(&self, level: usize) -> usize {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        if level >= NUM_LEVELS {
            return 0;
        }
        g.version_set.current().num_files(level)
    }

    /// Test hook: single-level
    /// manual compaction over `[begin, end]`.
    pub fn test_compact_range(
        &self,
        level: usize,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<()> {
        if level + 1 >= NUM_LEVELS {
            return Ok(());
        }
        self.force_flush()?;
        loop {
            let c = {
                let (mu, cv) = &*self.inner;
                let mut g = mu.lock().unwrap();
                while g.bg_work_scheduled
                    || g.bg_work_running
                    || g.imm.is_some()
                    || g.manual_compaction_running
                {
                    if let Some(e) = &g.bg_error {
                        return Err(e.clone());
                    }
                    g = cv.wait(g).unwrap();
                }
                let c = g.version_set.pick_manual_compaction(level, begin, end);
                if c.is_some() {
                    g.manual_compaction_running = true;
                    cv.notify_all();
                }
                c
            };
            let Some(c) = c else { break };
            let result = do_compaction_work_async(&self.inner, c);
            {
                let (mu, cv) = &*self.inner;
                let mut g = mu.lock().unwrap();
                g.manual_compaction_running = false;
                if let Some(t) = &g.bg_thread_handle {
                    t.unpark();
                }
                cv.notify_all();
            }
            result?;
        }
        Ok(())
    }

    /// Test hook: largest next-level overlapping byte count.
    pub fn test_max_next_level_overlapping_bytes(&self) -> u64 {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        let v = g.version_set.current();
        let mut result = 0u64;
        for level in 1..(NUM_LEVELS - 1) {
            for f in v.level_files(level) {
                let smallest_uk = &f.smallest[..f.smallest.len() - 8];
                let largest_uk = &f.largest[..f.largest.len() - 8];
                let overlaps =
                    v.get_overlapping_inputs(level + 1, Some(smallest_uk), Some(largest_uk));
                let sum: u64 = overlaps.iter().map(|f| f.file_size).sum();
                if sum > result {
                    result = sum;
                }
            }
        }
        result
    }

    /// TEST-style hook for write-queue parity tests.
    pub fn test_pending_writer_count(&self) -> usize {
        let (mu, _cv) = &*self.inner;
        let g = mu.lock().unwrap();
        g.writers.len()
    }

    /// Test hook: collects every stored value for a user key,
    /// including hidden values and deletion markers.
    pub fn test_all_entries_for(&self, user_key: &[u8]) -> Result<Vec<Option<Vec<u8>>>> {
        let (mu, cv) = &*self.inner;
        let mut g = mu.lock().unwrap();
        while g.bg_work_scheduled
            || g.bg_work_running
            || g.imm.is_some()
            || g.manual_compaction_running
        {
            if let Some(e) = &g.bg_error {
                return Err(e.clone());
            }
            g = cv.wait(g).unwrap();
        }
        if let Some(e) = &g.bg_error {
            return Err(e.clone());
        }
        g.test_all_entries_for(user_key)
    }
}

impl<C: Comparator + Clone + Send + Sync + 'static, E: Env + Clone + Send + Sync + 'static> Drop
    for DBImpl<C, E>
{
    fn drop(&mut self) {
        let Some(handle) = self.bg_thread.take() else {
            return;
        };

        // Signal the background thread to exit.
        {
            let (mu, cv) = &*self.inner;
            let mut g = mu.lock().unwrap();
            g.shutting_down = true;
            if let Some(t) = &g.bg_thread_handle {
                t.unpark();
            }
            cv.notify_all();
        }
        let _ = handle.join();
    }
}

/// Background worker thread. Sleeps on the Condvar until either
/// shutdown is signaled or `bg_work_scheduled` is set, then
/// drains imm + pending compactions while releasing the mutex
/// around table I/O.
fn bg_loop<
    C: Comparator + Clone + Send + Sync + 'static,
    E: Env + Clone + Send + Sync + 'static,
>(
    inner: Arc<(std::sync::Mutex<DBImplCore<C, E>>, std::sync::Condvar)>,
) {
    let (mu, cv) = &*inner;
    loop {
        // Wait for work or shutdown - release the guard before
        // calling bg_step_async, which manages its own locking.
        {
            let mut g = mu.lock().unwrap();
            while !g.shutting_down
                && (g.manual_compaction_running || (!g.bg_work_scheduled && g.imm.is_none()))
            {
                // Park rather than wait on the writer condvar: the
                // write path notifies that condvar on every commit,
                // which would wake this thread to no purpose. Threads
                // that actually schedule work `unpark()` us instead.
                // `park` may return spuriously - the `while` re-checks.
                drop(g);
                std::thread::park();
                g = mu.lock().unwrap();
            }
            if g.shutting_down {
                return;
            }
            // Phase 66: once bg_error is sticky, stop attempting
            // work. Just clear the scheduled flag, notify any
            // waiters (so they pick up the error), and re-wait.
            if g.bg_error.is_some() {
                g.bg_work_scheduled = false;
                cv.notify_all();
                continue;
            }
            g.bg_work_scheduled = false;
            g.bg_work_running = true;
        }
        // Run one cycle. bg_step_async releases the lock during
        // table I/O so user reads can proceed concurrently.
        let result = bg_step_async(&inner);
        // Mark done + notify any waiters (force_flush, write).
        {
            let mut g = mu.lock().unwrap();
            g.bg_work_running = false;
            if let Err(e) = result {
                // Phase J: surface bg errors via the user logger
                // before we go quiescent. Errors are sticky after
                // this point - operations will pick them up via
                // the bg_error guard checks.
                if let Some(l) = g.options.info_log.as_ref() {
                    l.log(&format!("Background work failed: {:?}", e));
                }
                g.bg_error = Some(e);
            }
            cv.notify_all();
        }
    }
}

struct LevelFileNumIter<C: Comparator + Clone> {
    files: Vec<crate::version_set::FileMetaData>,
    comparator: InternalKeyComparator<C>,
    pos: isize,
    value: [u8; 8],
}

impl<C: Comparator + Clone> LevelFileNumIter<C> {
    fn new(
        files: &[crate::version_set::FileMetaData],
        comparator: InternalKeyComparator<C>,
    ) -> Self {
        Self {
            files: files.to_vec(),
            comparator,
            pos: -1,
            value: [0; 8],
        }
    }
    fn refresh_value(&mut self) {
        if self.valid() {
            self.value = self.files[self.pos as usize].number.to_le_bytes();
        }
    }
}

impl<C: Comparator + Clone> crate::db_iter::DbIterator for LevelFileNumIter<C> {
    fn valid(&self) -> bool {
        self.pos >= 0 && (self.pos as usize) < self.files.len()
    }
    fn seek_to_first(&mut self) {
        self.pos = if self.files.is_empty() { -1 } else { 0 };
        self.refresh_value();
    }
    fn seek_to_last(&mut self) {
        self.pos = self.files.len() as isize - 1;
        self.refresh_value();
    }
    fn seek(&mut self, target: &[u8]) {
        let mut left = 0usize;
        let mut right = self.files.len();
        while left < right {
            let mid = left + (right - left) / 2;
            if self
                .comparator
                .compare(&self.files[mid].largest, target)
                .is_lt()
            {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        self.pos = if left >= self.files.len() {
            -1
        } else {
            left as isize
        };
        self.refresh_value();
    }
    fn next(&mut self) {
        assert!(self.valid());
        self.pos += 1;
        if self.pos as usize >= self.files.len() {
            self.pos = -1;
        }
        self.refresh_value();
    }
    fn prev(&mut self) {
        assert!(self.valid());
        self.pos -= 1;
        self.refresh_value();
    }
    fn key(&self) -> &[u8] {
        assert!(self.valid());
        &self.files[self.pos as usize].largest
    }
    fn value(&self) -> &[u8] {
        assert!(self.valid());
        &self.value
    }
    fn status(&self) -> Result<()> {
        Ok(())
    }
}

type LevelTableFunction<C> =
    Box<dyn FnMut(&[u8]) -> Result<TableIterator<InternalKeyComparator<C>>>>;

struct LevelIterator<C: Comparator + Clone + 'static> {
    inner: TwoLevelIterator<
        LevelFileNumIter<C>,
        TableIterator<InternalKeyComparator<C>>,
        LevelTableFunction<C>,
    >,
}

impl<C: Comparator + Clone + 'static> LevelIterator<C> {
    fn new<E>(
        files: &[crate::version_set::FileMetaData],
        icmp: InternalKeyComparator<C>,
        table_cache: TableCache<InternalKeyComparator<C>, E>,
        verify: bool,
    ) -> Self
    where
        E: Env + Clone + 'static,
    {
        let index_iter = LevelFileNumIter::new(files, icmp);
        let block_function: LevelTableFunction<C> = Box::new(move |file_number_bytes| {
            if file_number_bytes.len() != 8 {
                return Err(Status::corruption("Level iterator: bad file handle"));
            }
            let file_number = u64::from_le_bytes(file_number_bytes.try_into().unwrap());
            table_cache.new_iterator_verify(file_number, verify)
        });
        Self {
            inner: TwoLevelIterator::new(index_iter, block_function),
        }
    }
}

impl<C: Comparator + Clone + 'static> crate::db_iter::DbIterator for LevelIterator<C> {
    fn valid(&self) -> bool {
        self.inner.valid()
    }
    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
    }
    fn seek_to_last(&mut self) {
        self.inner.seek_to_last();
    }
    fn seek(&mut self, target: &[u8]) {
        self.inner.seek(target);
    }
    fn next(&mut self) {
        self.inner.next();
    }
    fn prev(&mut self) {
        self.inner.prev();
    }
    fn key(&self) -> &[u8] {
        self.inner.key()
    }
    fn value(&self) -> &[u8] {
        self.inner.value()
    }
    fn status(&self) -> Result<()> {
        self.inner.status()
    }
}

/// Replay log records into `mem`. Phase 68: when `paranoid`
/// is true, any corrupt record / batch returns the error.
/// When false, the replay stops at the first corruption and
/// returns the records that were successfully applied - the
/// error-tolerance behavior is driven by `paranoid_checks`.
/// Returns `(max_sequence, bytes_replayed)`.
/// `bytes_replayed` is the encoded-size sum of every record
/// that landed in the memtable - the caller folds it into
/// `DBImplCore::mem_memory_usage` so `make_room_for_write`
/// sees the post-recovery state correctly.
fn recover_log_file<C: Comparator + Clone, E: Env>(
    env: &E,
    dbname: &str,
    log_number: u64,
    mem: &SharedMemTable<C>,
    paranoid: bool,
) -> Result<(u64, usize)> {
    let path = log_file_name(dbname, log_number);
    let file = env.new_sequential_file(Path::new(&path))?;
    let mut reader = LogSequentialReader::new(file);
    let mut max_sequence = 0u64;
    let mut bytes_replayed = 0usize;
    // One reused buffer for every record; the record is parsed in place
    // and its entries inserted directly into the memtable, so replay does
    // not allocate a `WriteBatch` (or copy into it) per record. Only the
    // skiplist node per entry remains - that is inherent.
    let mut record_buf: Vec<u8> = Vec::new();
    loop {
        match reader.read_record_into(&mut record_buf) {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
                if paranoid {
                    return Err(e);
                }
                break; // tolerate a torn tail record from an unclean shutdown
            }
        }
        // Record layout: [u64 sequence][u32 count][entries...] - mirrors
        // WriteBatch's on-disk encoding.
        if record_buf.len() < 12 {
            if paranoid {
                return Err(Status::corruption(
                    "recover: malformed WAL record (too small)",
                ));
            }
            break;
        }
        let seq = crate::coding::decode_fixed64(&record_buf[..8]);
        let count = crate::coding::decode_fixed32(&record_buf[8..12]);
        let mut inserter = MemTableInserter {
            sequence: seq,
            mem,
            bytes_added: 0,
        };
        let mut input: &[u8] = &record_buf[12..];
        let mut visited = 0u32;
        let mut failed = false;
        while !input.is_empty() {
            if let Err(e) = crate::write_batch::WriteBatchRecord::visit(&mut input, &mut inserter) {
                if paranoid {
                    return Err(e);
                }
                failed = true;
                break;
            }
            visited += 1;
        }
        if failed {
            break;
        }
        if visited != count {
            if paranoid {
                return Err(Status::corruption("recover: WAL record has wrong count"));
            }
            break;
        }
        bytes_replayed += inserter.bytes_added;
        if count > 0 {
            let end = seq + count as u64 - 1;
            if end > max_sequence {
                max_sequence = end;
            }
        }
    }
    Ok((max_sequence, bytes_replayed))
}
