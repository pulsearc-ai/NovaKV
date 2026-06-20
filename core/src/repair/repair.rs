use std::path::Path;

use crate::comparator::Comparator;
use crate::db_impl::Options;
use crate::db_iter::DbIterator;
use crate::env::Env;
use crate::filename::{log_file_name, parse_file_name, table_file_name, FileType};
use crate::format::{parse_internal_key, InternalKeyComparator, ValueType};
use crate::log::LogSequentialReader;
use crate::memtable::MemTable;
use crate::status::{Result, Status};
use crate::table::{Table, TableFileBuilder};
use crate::version_set::VersionSet;
use crate::write_batch::WriteBatchHandler;

/// Outcome of `repair_db`. Summarizes what the repair
/// run recovered.
#[derive(Debug, Clone, Default)]
pub struct RepairReport {
    /// Number of SST files referenced by the new manifest.
    pub recovered_files: usize,
    /// Sum of file_size across recovered tables.
    pub recovered_bytes: u64,
    /// File numbers archived to `dbname/lost/` because they
    /// failed to open or scan.
    pub archived_table_numbers: Vec<u64>,
}

/// Reconstructs a damaged database directory.
/// Rebuilds CURRENT + MANIFEST from any surviving
/// `.log` and `.ldb` files in `dbname`. Logs are replayed
/// into fresh SSTs, every table is scanned for
/// (smallest, largest, max_sequence), corrupt files are
/// archived to `dbname/lost/`, and the resulting manifest
/// places every recovered table at level 0 - compaction
/// will redistribute them on the next DB open.
///
/// This convenience form uses `Options::default()`. If the
/// damaged DB was built with a non-default `filter_policy`
/// or `compressor`, use [`repair_db_with_options`] instead so
/// the rebuilt SSTs preserve those features.
pub fn repair_db<C, E>(dbname: &str, env: E, comparator: C) -> Result<RepairReport>
where
    C: Comparator + Clone + 'static,
    E: Env + Clone + 'static,
{
    repair_db_with_options(dbname, env, comparator, Options::default())
}

/// Phase K: repair with explicit `Options`. The supplied
/// `filter_policy` and `compressor` are used both for
/// reading any compressed/filtered tables found during
/// `scan_table` AND for rebuilding the new SSTs from log
/// replay - so repaired SSTs preserve the same on-disk
/// shape as the originals. `block_size` and
/// `block_restart_interval` also flow through.
pub fn repair_db_with_options<C, E>(
    dbname: &str,
    env: E,
    comparator: C,
    options: Options,
) -> Result<RepairReport>
where
    C: Comparator + Clone + 'static,
    E: Env + Clone + 'static,
{
    let mut r = Repairer::new(dbname, env, comparator, options);
    r.run()
}

/// Per-table metadata derived during ScanTable.
struct TableInfo {
    meta: crate::version_set::FileMetaData,
    max_sequence: u64,
}

struct Repairer<C: Comparator + Clone, E: Env> {
    dbname: String,
    env: E,
    comparator: C,
    icmp: InternalKeyComparator<C>,
    options: Options,
    next_file_number: u64,
    manifests: Vec<String>,
    logs: Vec<u64>,
    table_numbers: Vec<u64>,
    tables: Vec<TableInfo>,
    archived: Vec<u64>,
}

impl<C: Comparator + Clone + 'static, E: Env + Clone + 'static> Repairer<C, E> {
    fn new(dbname: &str, env: E, comparator: C, options: Options) -> Self {
        let icmp = InternalKeyComparator::new(comparator.clone());
        Self {
            dbname: dbname.to_string(),
            env,
            comparator,
            icmp,
            options,
            next_file_number: 1,
            manifests: Vec::new(),
            logs: Vec::new(),
            table_numbers: Vec::new(),
            tables: Vec::new(),
            archived: Vec::new(),
        }
    }

    fn run(&mut self) -> Result<RepairReport> {
        self.find_files()?;
        self.convert_log_files_to_tables();
        self.extract_metadata();
        self.write_descriptor()?;
        let bytes = self.tables.iter().map(|t| t.meta.file_size).sum();
        Ok(RepairReport {
            recovered_files: self.tables.len(),
            recovered_bytes: bytes,
            archived_table_numbers: std::mem::take(&mut self.archived),
        })
    }

    /// Lists `dbname` and classifies
    /// every entry into manifests / logs / tables. Tracks
    /// the largest file_number seen so the new manifest
    /// won't reuse one.
    fn find_files(&mut self) -> Result<()> {
        let entries = self.env.list_dir(Path::new(&self.dbname))?;
        if entries.is_empty() {
            return Err(Status::io_error(format!(
                "repair found no files in {}",
                self.dbname
            )));
        }
        for entry in entries {
            let name = entry
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            if let Some((number, ftype)) = parse_file_name(&name) {
                if ftype == FileType::DescriptorFile {
                    self.manifests.push(name);
                } else {
                    if number + 1 > self.next_file_number {
                        self.next_file_number = number + 1;
                    }
                    match ftype {
                        FileType::LogFile => self.logs.push(number),
                        FileType::TableFile => self.table_numbers.push(number),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// For each log:
    /// replay its records into a fresh memtable, write the
    /// memtable as a new SST, then archive the original log.
    /// Errors are non-fatal - repair always tries to recover
    /// what it can.
    fn convert_log_files_to_tables(&mut self) {
        let logs = self.logs.clone();
        for log_num in logs {
            // Best-effort: ignore errors.
            let _ = self.convert_log_to_table(log_num);
            let _ = self.archive_file(&log_file_name(&self.dbname, log_num));
        }
    }

    fn convert_log_to_table(&mut self, log_num: u64) -> Result<()> {
        let path = log_file_name(&self.dbname, log_num);
        let file = self.env.new_sequential_file(Path::new(&path))?;
        let mut reader = LogSequentialReader::new(file);
        let mut mem = MemTable::new(self.comparator.clone());
        let mut max_sequence = 0u64;
        // Loop reading records until EOF or unrecoverable error.
        loop {
            let record = match reader.read_record() {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(_) => break, // bad records stop the file
            };
            if record.len() < 12 {
                continue;
            }
            let mut batch = crate::write_batch::WriteBatch::new();
            if batch.set_contents(&record).is_err() {
                continue;
            }
            let seq = batch.sequence();
            let count = batch.count() as u64;
            let mut inserter = LogToMemInserter {
                sequence: seq,
                mem: &mut mem,
            };
            if batch.iterate(&mut inserter).is_err() {
                continue;
            }
            if count > 0 {
                let end = seq + count - 1;
                if end > max_sequence {
                    max_sequence = end;
                }
            }
        }

        // Write the recovered memtable as a fresh SST.
        let entries = mem.collect_entries();
        if entries.is_empty() {
            return Ok(());
        }
        let file_number = self.next_file_number;
        self.next_file_number += 1;

        let path = table_file_name(&self.dbname, file_number);
        let file = self.env.new_writable_file(Path::new(&path))?;
        // Phase K: rebuilt SSTs honor the supplied filter +
        // compressor + block-shape options.
        let mut builder = TableFileBuilder::with_options(
            self.icmp.clone(),
            self.options.block_size,
            self.options.block_restart_interval,
            file,
            self.options.filter_policy.clone(),
            self.options.compressor.clone(),
        );
        for (k, v) in &entries {
            builder.add(k, v)?;
        }
        builder.finish()?;
        builder.sync()?;
        builder.close()?;
        // ScanTable will re-derive metadata; just register.
        self.table_numbers.push(file_number);
        Ok(())
    }

    /// Scan every table to
    /// derive (smallest, largest, max_sequence). Corrupt or
    /// unreadable tables are archived.
    fn extract_metadata(&mut self) {
        // Snapshot the list because scan_table mutates self.archived.
        let nums = self.table_numbers.clone();
        for n in nums {
            if let Err(_e) = self.scan_table(n) {
                self.archived.push(n);
                let _ = self.archive_file(&table_file_name(&self.dbname, n));
            }
        }
    }

    fn scan_table(&mut self, number: u64) -> Result<()> {
        let path = table_file_name(&self.dbname, number);
        let path_ref = Path::new(&path);
        let file_size = self.env.get_file_size(path_ref)?;
        let file = self.env.new_random_access_file(path_ref)?;
        // Phase K: pass the user's compressor so we can decode
        // SSTs that were written with non-zero kind bytes. We
        // skip the filter policy here - the SST's existing
        // filter block (if any) isn't queried during scan; only
        // the data blocks are walked end-to-end.
        let table = Table::open_random_with_options(
            file,
            file_size,
            self.icmp.clone(),
            None,
            None,
            self.options.compressor.clone(),
        )?;
        let mut iter = table.new_iterator()?;
        iter.seek_to_first();
        if !iter.valid() {
            self.archived.push(number);
            let _ = self.archive_file(&path);
            iter.status()?;
            return Ok(());
        }
        let smallest = iter.key().to_vec();
        let mut largest = smallest.clone();
        let mut max_sequence = 0u64;
        while iter.valid() {
            let key = iter.key();
            largest = key.to_vec();
            if let Some(p) = parse_internal_key(key) {
                if p.sequence > max_sequence {
                    max_sequence = p.sequence;
                }
            }
            iter.next();
        }
        iter.status()?;
        self.tables.push(TableInfo {
            meta: crate::version_set::FileMetaData {
                number,
                file_size,
                smallest,
                largest,
            },
            max_sequence,
        });
        Ok(())
    }

    /// Archives any pre-existing
    /// manifests, then constructs a fresh `VersionSet` and
    /// applies a single `VersionEdit` placing every recovered
    /// table at level 0. Sets log_number=0 (no live WAL),
    /// last_sequence to the max observed in any table, and
    /// next_file_number past everything we've allocated.
    fn write_descriptor(&mut self) -> Result<()> {
        // Archive any pre-existing manifests so the new VersionSet
        // doesn't see them. (CURRENT will be overwritten by
        // log_and_apply.)
        for mfile in self.manifests.clone() {
            let mfp = format!("{}/{}", self.dbname, mfile);
            let _ = self.archive_file(&mfp);
        }

        let mut vs = VersionSet::new(&self.dbname, self.env.clone(), self.comparator.clone());
        // Skip past every file number we've seen - VersionSet
        // will allocate a manifest number from this counter.
        vs.set_next_file_number(self.next_file_number);
        let max_seq = self
            .tables
            .iter()
            .map(|t| t.max_sequence)
            .max()
            .unwrap_or(0);
        vs.set_last_sequence(max_seq);

        let mut edit = crate::version_set::VersionEdit {
            comparator: Some(self.comparator.name().as_bytes().to_vec()),
            log_number: Some(0),
            ..Default::default()
        };
        for t in &self.tables {
            edit.new_files.push(crate::version_set::NewFile {
                level: 0,
                meta: t.meta.clone(),
            });
        }
        vs.log_and_apply(&mut edit)?;
        Ok(())
    }

    /// Move `src` to `dbname/lost/`,
    /// creating the directory if needed. Errors are swallowed
    /// (best-effort).
    fn archive_file(&self, src: &str) -> Result<()> {
        let lost_dir = format!("{}/lost", self.dbname);
        let _ = self.env.create_dir(Path::new(&lost_dir));
        let basename = match src.rsplit_once('/') {
            Some((_, b)) => b,
            None => src,
        };
        let dst = format!("{}/{}", lost_dir, basename);
        self.env.rename_file(Path::new(src), Path::new(&dst))
    }
}

/// Per-record handler that inserts (key, value) pairs into
/// a memtable with monotonically-increasing sequence numbers.
/// Used during log replay.
struct LogToMemInserter<'a, C: Comparator + Clone> {
    sequence: u64,
    mem: &'a mut MemTable<C>,
}

impl<'a, C: Comparator + Clone> WriteBatchHandler for LogToMemInserter<'a, C> {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.mem.add(self.sequence, ValueType::Value, key, value);
        self.sequence += 1;
    }
    fn delete(&mut self, key: &[u8]) {
        self.mem.add(self.sequence, ValueType::Deletion, key, b"");
        self.sequence += 1;
    }
}
