use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use crate::comparator::Comparator;
use crate::env::{Env, WritableFile};
use crate::filename::{
    current_file_name, descriptor_file_name, parse_file_name, set_current_file, FileType,
};
use crate::format::{InternalKeyComparator, MAX_MEM_COMPACT_LEVEL, NUM_LEVELS};
use crate::log::{LogSequentialReader, LogWriter};
use crate::status::{Result, Status};

#[derive(Debug, Clone)]
pub enum LookupResult {
    Found(Vec<u8>),
    NotFound,
    Deleted,
}

pub const TARGET_FILE_SIZE: u64 = 2097152;
pub const MAX_GRANDPARENT_OVERLAP_BYTES: u64 = 20971520;
pub const MAX_BYTES_FOR_LEVEL_BASE: u64 = 10485760;
pub const MAX_BYTES_FOR_LEVEL_MULTIPLIER: u32 = 10;

/// Capacity (in bytes) for level `level` (0-indexed). L0 is
/// scored by file count, not bytes; this is L1+ only.
pub fn max_bytes_for_level(level: usize) -> f64 {
    let mut result = MAX_BYTES_FOR_LEVEL_BASE as f64;
    let mult = MAX_BYTES_FOR_LEVEL_MULTIPLIER as f64;
    let mut lvl = level;
    while lvl > 1 {
        result *= mult;
        lvl -= 1;
    }
    result
}

/// Sum of file_size across the slice.
pub fn total_file_size(files: &[crate::version_set::FileMetaData]) -> u64 {
    files.iter().map(|f| f.file_size).sum()
}

/// Binary search for the file in a sorted, non-overlapping
/// level (L1+) whose largest_internal_key >= `key`. Returns
/// `files.len()` if no such file exists.
pub fn find_file<C: Comparator + Clone>(
    icmp: &InternalKeyComparator<C>,
    files: &[crate::version_set::FileMetaData],
    key: &[u8],
) -> usize {
    let mut left = 0usize;
    let mut right = files.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if icmp.compare(&files[mid].largest, key).is_lt() {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

/// Immutable snapshot of the LSM file set. Cheap to clone
/// (Arc share); iterators hold an `Arc<Version>` to keep
/// the file set alive while VersionSet mutates the current.
#[derive(Debug, Clone)]
pub struct Version<C: Comparator + Clone> {
    files: Vec<Vec<crate::version_set::FileMetaData>>,
    comparator: InternalKeyComparator<C>,
    compaction_score: f64,
    compaction_level: i32,
    /// Phase 57: per-file runtime seek allowance. Decremented by
    /// `record_read_sample` when a read overlaps multiple files.
    /// Side-map keyed by `file_number` so `FileMetaData` stays
    /// purely on-disk (the seek allowance is runtime-only).
    seek_stats: Arc<std::sync::Mutex<std::collections::HashMap<u64, i64>>>,
    /// (level, file_number) of file due for seek-triggered compaction.
    file_to_compact: Arc<std::sync::Mutex<Option<(usize, u64)>>>,
}

impl<C: Comparator + Clone> Version<C> {
    pub fn new(comparator: InternalKeyComparator<C>) -> Self {
        Self {
            files: vec![Vec::new(); NUM_LEVELS],
            comparator,
            compaction_score: -1.0,
            compaction_level: -1,
            seek_stats: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            file_to_compact: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn compaction_score(&self) -> f64 {
        self.compaction_score
    }
    pub fn compaction_level(&self) -> i32 {
        self.compaction_level
    }

    /// Compute compaction score (size-triggered) for every level.
    /// L0 is scored by file count;
    /// L1+ by total bytes / max_bytes_for_level.
    pub fn finalize(&mut self) {
        use crate::format::L0_COMPACTION_TRIGGER;
        let mut best_level = -1i32;
        let mut best_score = -1.0f64;
        for level in 0..(NUM_LEVELS - 1) {
            let score = if level == 0 {
                self.files[0].len() as f64 / L0_COMPACTION_TRIGGER as f64
            } else {
                total_file_size(&self.files[level]) as f64 / max_bytes_for_level(level)
            };
            if score > best_score {
                best_score = score;
                best_level = level as i32;
            }
        }
        self.compaction_score = best_score;
        self.compaction_level = best_level;
    }

    /// Find files at `level` whose user-key range overlaps
    /// with `[user_begin, user_end]`. For L0 the input set is
    /// closed under range expansion (newly-added files may
    /// extend the range, triggering a re-scan).
    pub fn get_overlapping_inputs(
        &self,
        level: usize,
        user_begin: Option<&[u8]>,
        user_end: Option<&[u8]>,
    ) -> Vec<crate::version_set::FileMetaData> {
        let ucmp = self.comparator.user_comparator().clone();
        let files = &self.files[level];
        let mut result: Vec<crate::version_set::FileMetaData> = Vec::new();
        let mut user_begin: Option<Vec<u8>> = user_begin.map(<[u8]>::to_vec);
        let mut user_end: Option<Vec<u8>> = user_end.map(<[u8]>::to_vec);
        let mut i = 0usize;
        while i < files.len() {
            let f = &files[i];
            i += 1;
            let f_start = &f.smallest[..f.smallest.len() - 8];
            let f_limit = &f.largest[..f.largest.len() - 8];
            if let Some(begin) = &user_begin {
                if ucmp.compare(f_limit, begin).is_lt() {
                    continue;
                }
            }
            if let Some(end) = &user_end {
                if ucmp.compare(f_start, end).is_gt() {
                    continue;
                }
            }
            result.push(f.clone());
            if level == 0 {
                let mut restart = false;
                if let Some(begin) = &user_begin {
                    if ucmp.compare(f_start, begin).is_lt() {
                        user_begin = Some(f_start.to_vec());
                        restart = true;
                    }
                } else {
                    user_begin = Some(f_start.to_vec());
                }
                if let Some(end) = &user_end {
                    if ucmp.compare(f_limit, end).is_gt() {
                        user_end = Some(f_limit.to_vec());
                        restart = true;
                    }
                } else {
                    user_end = Some(f_limit.to_vec());
                }
                if restart {
                    result.clear();
                    i = 0;
                }
            }
        }
        result
    }

    pub fn level_files(&self, level: usize) -> &[crate::version_set::FileMetaData] {
        &self.files[level]
    }

    /// Returns true iff
    /// any file at `level` overlaps the user-key range
    /// `[begin, end]` (both bounds are user_keys; `None` means
    /// open-ended).
    pub fn overlap_in_level(&self, level: usize, begin: Option<&[u8]>, end: Option<&[u8]>) -> bool {
        !self.get_overlapping_inputs(level, begin, end).is_empty()
    }

    /// Chooses how deep a freshly flushed memtable can land
    /// without overlapping existing files or too much
    /// grandparent data.
    pub fn pick_level_for_memtable_output(
        &self,
        smallest_user_key: &[u8],
        largest_user_key: &[u8],
    ) -> usize {
        let mut level = 0usize;
        if !self.overlap_in_level(0, Some(smallest_user_key), Some(largest_user_key)) {
            while level < MAX_MEM_COMPACT_LEVEL {
                if self.overlap_in_level(level + 1, Some(smallest_user_key), Some(largest_user_key))
                {
                    break;
                }
                if level + 2 < NUM_LEVELS {
                    let overlaps = self.get_overlapping_inputs(
                        level + 2,
                        Some(smallest_user_key),
                        Some(largest_user_key),
                    );
                    if total_file_size(&overlaps) > MAX_GRANDPARENT_OVERLAP_BYTES {
                        break;
                    }
                }
                level += 1;
            }
        }
        level
    }

    pub fn num_files(&self, level: usize) -> usize {
        self.files[level].len()
    }

    pub fn comparator(&self) -> &InternalKeyComparator<C> {
        &self.comparator
    }

    /// Walks files whose
    /// user-key range contains the sampled key. If 2+ files
    /// match, decrements the first match's seek allowance.
    /// Returns true iff this call set `file_to_compact`.
    pub fn record_read_sample(&self, internal_key: &[u8]) -> bool {
        if internal_key.len() < 8 {
            return false;
        }
        let user_key = &internal_key[..internal_key.len() - 8];
        let ucmp = self.comparator.user_comparator().clone();
        let mut first_match: Option<(usize, u64)> = None;
        let mut match_count = 0usize;
        for f in &self.files[0] {
            let smallest_uk = &f.smallest[..f.smallest.len() - 8];
            let largest_uk = &f.largest[..f.largest.len() - 8];
            if ucmp.compare(user_key, smallest_uk).is_ge()
                && ucmp.compare(user_key, largest_uk).is_le()
            {
                if first_match.is_none() {
                    first_match = Some((0, f.number));
                }
                match_count += 1;
                if match_count >= 2 {
                    break;
                }
            }
        }
        if match_count < 2 {
            for level in 1..NUM_LEVELS {
                let files = &self.files[level];
                if files.is_empty() {
                    continue;
                }
                let idx = find_file(&self.comparator, files, internal_key);
                if idx >= files.len() {
                    continue;
                }
                let f = &files[idx];
                let smallest_uk = &f.smallest[..f.smallest.len() - 8];
                if ucmp.compare(user_key, smallest_uk).is_lt() {
                    continue;
                }
                if first_match.is_none() {
                    first_match = Some((level, f.number));
                }
                match_count += 1;
                if match_count >= 2 {
                    break;
                }
            }
        }
        if match_count < 2 {
            return false;
        }
        let (level, num) = first_match.unwrap();
        self.decrement_seek_allowance(level, num)
    }

    /// Take the pending seek-triggered compaction, if any.
    pub fn take_file_to_compact(&self) -> Option<(usize, u64)> {
        self.file_to_compact.lock().unwrap().take()
    }

    /// True if a read sample or point-read seek charge has
    /// marked a file for seek-triggered compaction.
    pub fn has_file_to_compact(&self) -> bool {
        self.file_to_compact.lock().unwrap().is_some()
    }

    /// Install initial seek allowance for a newly-added file.
    /// The allowance is `file_size / READ_BYTES_PERIOD`,
    /// with a minimum of 100.
    pub fn init_seek_allowance(&self, file_number: u64, file_size: u64) {
        use crate::format::READ_BYTES_PERIOD;
        let allowed = (file_size as i64 / READ_BYTES_PERIOD as i64).max(100);
        self.seek_stats.lock().unwrap().insert(file_number, allowed);
    }

    fn decrement_seek_allowance(&self, level: usize, file_number: u64) -> bool {
        let mut stats = self.seek_stats.lock().unwrap();
        let entry = stats.entry(file_number).or_insert(0);
        *entry -= 1;
        if *entry <= 0 {
            let mut ftc = self.file_to_compact.lock().unwrap();
            if ftc.is_none() {
                *ftc = Some((level, file_number));
                return true;
            }
        }
        false
    }

    /// Get a value for `user_key` by consulting each candidate
    /// file via `lookup`. L0 files are checked newest-first;
    /// L1+ uses binary search. Returns `Ok(None)` if the key
    /// isn't present in any file.
    pub fn get<F>(
        &self,
        user_key: &[u8],
        internal_key: &[u8],
        mut lookup: F,
    ) -> Result<Option<Vec<u8>>>
    where
        F: FnMut(u64, &[u8]) -> Result<LookupResult>,
    {
        let ucmp = self.comparator.user_comparator().clone();
        let mut last_file_read: Option<(usize, u64)> = None;
        let mut charged_seek_file = false;

        // L0: scan files whose user-key range covers `user_key`, newest
        // first. `files[0]` is kept sorted ascending by file number (see
        // `save_to`), so iterating in reverse is newest-first without the
        // per-lookup Vec allocation + sort the old code paid.
        for f in self.files[0].iter().rev() {
            let smallest_uk = &f.smallest[..f.smallest.len() - 8];
            let largest_uk = &f.largest[..f.largest.len() - 8];
            if ucmp.compare(user_key, smallest_uk).is_lt()
                || ucmp.compare(user_key, largest_uk).is_gt()
            {
                continue;
            }
            if !charged_seek_file {
                if let Some((level, file_number)) = last_file_read {
                    self.decrement_seek_allowance(level, file_number);
                    charged_seek_file = true;
                }
            }
            last_file_read = Some((0, f.number));
            match lookup(f.number, internal_key)? {
                LookupResult::Found(v) => return Ok(Some(v)),
                LookupResult::Deleted => return Ok(None),
                LookupResult::NotFound => {}
            }
        }

        // L1+: binary search for the unique file (if any) whose
        // range contains `internal_key`.
        for level in 1..NUM_LEVELS {
            let files = &self.files[level];
            if files.is_empty() {
                continue;
            }
            let idx = find_file(&self.comparator, files, internal_key);
            if idx >= files.len() {
                continue;
            }
            let f = &files[idx];
            let smallest_uk = &f.smallest[..f.smallest.len() - 8];
            if ucmp.compare(user_key, smallest_uk).is_lt() {
                continue;
            }
            if !charged_seek_file {
                if let Some((prev_level, file_number)) = last_file_read {
                    self.decrement_seek_allowance(prev_level, file_number);
                    charged_seek_file = true;
                }
            }
            last_file_read = Some((level, f.number));
            match lookup(f.number, internal_key)? {
                LookupResult::Found(v) => return Ok(Some(v)),
                LookupResult::Deleted => return Ok(None),
                LookupResult::NotFound => {}
            }
        }
        Ok(None)
    }
}

/// Applies a sequence of `VersionEdit`s to a base `Version`,
/// producing a new Version.
pub struct VersionBuilder<'a, C: Comparator + Clone> {
    base: &'a Version<C>,
    deleted_files: Vec<BTreeSet<u64>>,
    added_files: Vec<Vec<crate::version_set::FileMetaData>>,
}

impl<'a, C: Comparator + Clone> VersionBuilder<'a, C> {
    pub fn new(base: &'a Version<C>) -> Self {
        Self {
            base,
            deleted_files: vec![BTreeSet::new(); NUM_LEVELS],
            added_files: vec![Vec::new(); NUM_LEVELS],
        }
    }

    pub fn apply(&mut self, edit: &crate::version_set::VersionEdit) {
        for d in &edit.deleted_files {
            let crate::version_set::DeletedFile { level, number } = d;
            self.deleted_files[*level as usize].insert(*number);
        }
        for nf in &edit.new_files {
            let crate::version_set::NewFile { level, meta } = nf;
            // Re-add: ensure not in deleted set.
            self.deleted_files[*level as usize].remove(&meta.number);
            self.added_files[*level as usize].push(meta.clone());
        }
    }

    pub fn save_to(self, comparator: InternalKeyComparator<C>) -> Version<C> {
        let mut v = Version::new(comparator);
        // Phase 57: migrate seek allowance for surviving files
        // (carry over from base) and initialize for newly-added
        // files (default = file_size / READ_BYTES_PERIOD, min 100).
        {
            let base_stats = self.base.seek_stats.lock().unwrap();
            let mut new_stats = v.seek_stats.lock().unwrap();
            for level in 0..NUM_LEVELS {
                for f in &self.base.files[level] {
                    if self.deleted_files[level].contains(&f.number) {
                        continue;
                    }
                    if let Some(&allowed) = base_stats.get(&f.number) {
                        new_stats.insert(f.number, allowed);
                    }
                }
            }
        }
        for level in 0..NUM_LEVELS {
            let mut files: Vec<crate::version_set::FileMetaData> = self.base.files[level]
                .iter()
                .filter(|f| !self.deleted_files[level].contains(&f.number))
                .cloned()
                .collect();
            for f in &self.added_files[level] {
                if !self.deleted_files[level].contains(&f.number) {
                    files.push(f.clone());
                    v.init_seek_allowance(f.number, f.file_size);
                }
            }
            if level == 0 {
                // L0: order by file_number ascending (older = lower number).
                files.sort_by_key(|a| a.number);
            } else {
                // L1+: order by smallest internal_key.
                let icmp = v.comparator.clone();
                files.sort_by(|a, b| icmp.compare(&a.smallest, &b.smallest));
            }
            v.files[level] = files;
        }
        v.finalize();
        v
    }
}

/// Mutable manager for the LSM version state. Owns the
/// current `Arc<Version>` snapshot, the manifest log writer,
/// and bookkeeping (last_sequence, file numbers, etc.).
/// The compaction picker lives here too.
pub struct VersionSet<C: Comparator + Clone, E: Env> {
    dbname: String,
    env: E,
    icmp: InternalKeyComparator<C>,
    current: Arc<Version<C>>,
    next_file_number: u64,
    manifest_file_number: u64,
    last_sequence: u64,
    log_number: u64,
    prev_log_number: u64,
    manifest_log: Option<LogWriter>,
    manifest_file: Option<E::Writable>,
    compact_pointer: Vec<Vec<u8>>,
    /// Phase F: comparator name read from the recovered manifest.
    /// `None` until `recover()` consumes a `VersionEdit` that
    /// carries one. `DBImpl::open` checks this against
    /// `comparator.name()` and refuses mismatches.
    recovered_comparator_name: Option<Vec<u8>>,
}

impl<C: Comparator + Clone, E: Env> VersionSet<C, E> {
    pub fn new(dbname: &str, env: E, comparator: C) -> Self {
        let icmp = InternalKeyComparator::new(comparator);
        let current = Arc::new(Version::new(icmp.clone()));
        Self {
            dbname: dbname.to_string(),
            env,
            icmp,
            current,
            next_file_number: 2,
            manifest_file_number: 0,
            last_sequence: 0,
            log_number: 0,
            prev_log_number: 0,
            manifest_log: None,
            manifest_file: None,
            compact_pointer: vec![Vec::new(); NUM_LEVELS],
            recovered_comparator_name: None,
        }
    }

    /// Phase F: the comparator name carried by the recovered
    /// manifest, if any. `DBImpl::open` checks this matches
    /// the user-supplied comparator's `name()`.
    pub fn recovered_comparator_name(&self) -> Option<&[u8]> {
        self.recovered_comparator_name.as_deref()
    }

    /// Returns a pinned
    /// snapshot of the current Version. Holding the returned
    /// `Arc` keeps the underlying file set alive across
    /// subsequent edits
    /// (which install a new `Arc<Version>` via log_and_apply).
    /// Drop the `Arc` (or call `release_version`) to release.
    pub fn current(&self) -> Arc<Version<C>> {
        self.current.clone()
    }

    /// Explicit alias for pinning the current Version
    /// (`Arc::clone` already does the same thing) -
    /// equivalent to `current().clone()`.
    pub fn ref_current(&self) -> Arc<Version<C>> {
        self.current.clone()
    }

    /// Drops the caller's Arc
    /// reference. Once strong_count reaches 0, the Version's
    /// file set is freed. The compiler enforces this via Arc
    /// drop semantics; this explicit method is provided
    /// for code that pairs every pin with a matching release.
    pub fn release_version(&self, version: Arc<Version<C>>) {
        drop(version);
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }
    pub fn set_last_sequence(&mut self, s: u64) {
        self.last_sequence = s;
    }
    pub fn log_number(&self) -> u64 {
        self.log_number
    }
    pub fn prev_log_number(&self) -> u64 {
        self.prev_log_number
    }
    pub fn manifest_file_number(&self) -> u64 {
        self.manifest_file_number
    }
    pub fn next_file_number(&self) -> u64 {
        self.next_file_number
    }
    pub fn new_file_number(&mut self) -> u64 {
        let n = self.next_file_number;
        self.next_file_number += 1;
        n
    }
    pub fn reuse_file_number(&mut self, n: u64) {
        if self.next_file_number == n + 1 {
            self.next_file_number = n;
        }
    }
    /// Override the next-file-number counter. Used by repair
    /// to skip past any file numbers it has
    /// already observed before writing the new manifest.
    pub fn set_next_file_number(&mut self, n: u64) {
        if n > self.next_file_number {
            self.next_file_number = n;
        }
    }
    pub fn env(&self) -> &E {
        &self.env
    }
    pub fn dbname(&self) -> &str {
        &self.dbname
    }
    pub fn icmp(&self) -> &InternalKeyComparator<C> {
        &self.icmp
    }
    pub fn compact_pointer(&self, level: usize) -> &[u8] {
        &self.compact_pointer[level]
    }

    /// Apply `edit` to the current version, persist it to the
    /// manifest log, and atomically swap in the new version.
    /// If no manifest exists yet, opens one + writes a snapshot.
    pub fn log_and_apply(&mut self, edit: &mut crate::version_set::VersionEdit) -> Result<()> {
        // Stash current sequence/log numbers into the edit if
        // the caller hasn't done so explicitly.
        if edit.log_number.is_none() {
            edit.log_number = Some(self.log_number);
        }
        if edit.prev_log_number.is_none() {
            edit.prev_log_number = Some(self.prev_log_number);
        }
        edit.next_file_number = Some(self.next_file_number);
        edit.last_sequence = Some(self.last_sequence);

        // Build new version.
        let mut builder = VersionBuilder::new(&self.current);
        builder.apply(edit);
        for cp in &edit.compact_pointers {
            let crate::version_set::CompactPointer { level, key } = cp;
            self.compact_pointer[*level as usize] = key.clone();
        }
        let new_version = builder.save_to(self.icmp.clone());

        // Initialize manifest if needed.
        let new_manifest = self.manifest_log.is_none();
        if new_manifest {
            self.manifest_file_number = self.new_file_number();
            let manifest_path = descriptor_file_name(&self.dbname, self.manifest_file_number);
            let manifest_file = self.env.new_writable_file(Path::new(&manifest_path))?;
            self.manifest_log = Some(LogWriter::new());
            self.manifest_file = Some(manifest_file);
            let snapshot = self.snapshot_edit();
            if let Err(error) = self.append_to_manifest(&snapshot) {
                self.abandon_manifest();
                return Err(error);
            }
        }
        if let Err(error) = self.append_to_manifest(edit) {
            if new_manifest {
                self.abandon_manifest();
            }
            return Err(error);
        }
        if let Err(error) = self.sync_manifest() {
            if new_manifest {
                self.abandon_manifest();
            }
            return Err(error);
        }
        if new_manifest {
            if let Err(error) = self.set_current_file() {
                self.abandon_manifest();
                return Err(error);
            }
        }

        // Pull edit-tracked fields back into the VersionSet.
        if let Some(n) = edit.log_number {
            self.log_number = n;
        }
        if let Some(n) = edit.prev_log_number {
            self.prev_log_number = n;
        }
        self.current = Arc::new(new_version);
        Ok(())
    }

    /// Snapshot the current state as a single VersionEdit
    /// (used when starting a fresh manifest).
    fn snapshot_edit(&self) -> crate::version_set::VersionEdit {
        let mut e = crate::version_set::VersionEdit {
            comparator: Some(self.icmp.user_comparator().name().as_bytes().to_vec()),
            log_number: Some(self.log_number),
            prev_log_number: Some(self.prev_log_number),
            next_file_number: Some(self.next_file_number),
            last_sequence: Some(self.last_sequence),
            ..Default::default()
        };
        for (level, key) in self.compact_pointer.iter().enumerate() {
            if !key.is_empty() {
                e.compact_pointers.push(crate::version_set::CompactPointer {
                    level: level as u32,
                    key: key.clone(),
                });
            }
        }
        for level in 0..NUM_LEVELS {
            for f in &self.current.files[level] {
                e.new_files.push(crate::version_set::NewFile {
                    level: level as u32,
                    meta: f.clone(),
                });
            }
        }
        e
    }

    fn append_to_manifest(&mut self, edit: &crate::version_set::VersionEdit) -> Result<()> {
        let mut buf = Vec::new();
        edit.encode(&mut buf);
        let manifest = self.manifest_log.as_mut().expect("manifest_log");
        let file = self.manifest_file.as_mut().expect("manifest_file");
        manifest.add_record_to(file, &buf)
    }

    fn sync_manifest(&mut self) -> Result<()> {
        self.manifest_file.as_mut().expect("manifest_file").sync()
    }

    fn abandon_manifest(&mut self) {
        if let Some(file) = self.manifest_file.as_mut() {
            let _ = file.close();
        }
        self.manifest_file = None;
        self.manifest_log = None;
    }

    fn set_current_file(&self) -> Result<()> {
        set_current_file(&self.env, &self.dbname, self.manifest_file_number)
    }

    /// Recover from an existing database directory by reading
    /// CURRENT, opening the named MANIFEST, and replaying every
    /// VersionEdit through a VersionBuilder.
    pub fn recover(&mut self) -> Result<()> {
        let current_path = current_file_name(&self.dbname);
        let current_bytes = self.env.read_file(Path::new(&current_path))?;
        let current_str = std::str::from_utf8(&current_bytes)
            .map_err(|_| Status::corruption("CURRENT contains non-UTF-8 bytes"))?;
        let manifest_name = current_str.trim_end_matches('\n');
        // Record the number of the manifest we are loading: it IS the
        // live descriptor. Without this, `manifest_file_number` stays 0
        // and a recovery that reuses the WAL (skipping `log_and_apply`)
        // would treat the live MANIFEST as obsolete and delete it. The
        // non-reuse path overwrites this when `log_and_apply` mints a
        // fresh manifest.
        if let Some((num, FileType::DescriptorFile)) = parse_file_name(manifest_name) {
            self.manifest_file_number = num;
        }
        let manifest_path = format!("{}/{}", self.dbname, manifest_name);
        let manifest_file = self.env.new_sequential_file(Path::new(&manifest_path))?;
        let mut reader = LogSequentialReader::new(manifest_file);
        let base = Arc::new(Version::new(self.icmp.clone()));
        let mut builder = VersionBuilder::new(&base);
        let mut have_log_number = false;
        let mut have_prev_log_number = false;
        let mut have_next_file = false;
        let mut have_last_sequence = false;
        while let Some(record) = reader.read_record()? {
            let mut input: &[u8] = &record;
            let edit = crate::version_set::VersionEdit::decode_from(&mut input)?;
            builder.apply(&edit);
            for cp in &edit.compact_pointers {
                let crate::version_set::CompactPointer { level, key } = cp;
                self.compact_pointer[*level as usize] = key.clone();
            }
            if let Some(n) = edit.log_number {
                self.log_number = n;
                have_log_number = true;
            }
            if let Some(n) = edit.prev_log_number {
                self.prev_log_number = n;
                have_prev_log_number = true;
            }
            if let Some(n) = edit.next_file_number {
                self.next_file_number = n;
                have_next_file = true;
            }
            if let Some(n) = edit.last_sequence {
                self.last_sequence = n;
                have_last_sequence = true;
            }
            if let Some(name) = &edit.comparator {
                self.recovered_comparator_name = Some(name.clone());
            }
        }
        if !(have_next_file && have_last_sequence) {
            return Err(Status::corruption("manifest: missing required fields"));
        }
        let _ = (have_log_number, have_prev_log_number);
        let new_version = builder.save_to(self.icmp.clone());
        self.current = Arc::new(new_version);
        // Manifest log is replaced when the next log_and_apply runs (sets new manifest_file_number).
        self.manifest_log = None;
        self.manifest_file = None;
        Ok(())
    }

    /// Pick the next compaction, in priority order:
    /// seek-triggered files first (compaction caused by
    /// `record_read_sample` consuming a file's seek allowance),
    /// then size-triggered (compaction_score >= 1.0).
    /// Mutates `compact_pointer[level]` to remember progress.
    pub fn pick_compaction(&mut self) -> Option<Compaction<C>> {
        // Phase 57: seek-triggered takes priority.
        if let Some((level, file_number)) = self.current.take_file_to_compact() {
            if level + 1 < NUM_LEVELS {
                if let Some(f) = self.current.files[level]
                    .iter()
                    .find(|f| f.number == file_number)
                    .cloned()
                {
                    let mut input0 = vec![f];
                    if level == 0 {
                        let (smallest, largest) = get_user_range(&input0);
                        input0 =
                            self.current
                                .get_overlapping_inputs(0, Some(&smallest), Some(&largest));
                    }
                    let mut c = Compaction {
                        level,
                        max_output_file_size: TARGET_FILE_SIZE,
                        inputs: [input0, Vec::new()],
                        grandparents: Vec::new(),
                        input_version: self.current.clone(),
                        edit: crate::version_set::VersionEdit::default(),
                    };
                    self.setup_other_inputs(&mut c);
                    return Some(c);
                }
            }
        }

        // Size-triggered fallback.
        if self.current.compaction_score < 1.0 {
            return None;
        }
        let level = self.current.compaction_level as usize;
        assert!(level + 1 < NUM_LEVELS, "compaction at last level");

        // Pick the first file whose largest internal_key > compact_pointer[level].
        let mut input0: Vec<crate::version_set::FileMetaData> = Vec::new();
        let pointer = &self.compact_pointer[level];
        for f in &self.current.files[level] {
            if pointer.is_empty() || self.icmp.compare(&f.largest, pointer).is_gt() {
                input0.push(f.clone());
                break;
            }
        }
        if input0.is_empty() {
            // Wrap-around to the first file.
            input0.push(self.current.files[level][0].clone());
        }

        // For L0, expand inputs to cover all files overlapping the picked one's range.
        if level == 0 {
            let (smallest, largest) = get_user_range(&input0);
            input0 = self
                .current
                .get_overlapping_inputs(0, Some(&smallest), Some(&largest));
            debug_assert!(!input0.is_empty());
        }

        let mut compaction = Compaction {
            level,
            max_output_file_size: TARGET_FILE_SIZE,
            inputs: [input0, Vec::new()],
            grandparents: Vec::new(),
            input_version: self.current.clone(),
            edit: crate::version_set::VersionEdit::default(),
        };
        self.setup_other_inputs(&mut compaction);
        Some(compaction)
    }

    /// Builds
    /// a Compaction over files at `level` whose user-key range
    /// overlaps `[begin, end]`. Returns `None` if no such files.
    ///
    /// For L0, expands the input set to all overlapping files
    /// (since L0 files may overlap each other). For L1+, the
    /// non-overlapping invariant means we can take exactly the
    /// files that span the range.
    pub fn pick_manual_compaction(
        &mut self,
        level: usize,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Option<Compaction<C>> {
        if level + 1 >= NUM_LEVELS {
            return None;
        }
        let inputs = self.current.get_overlapping_inputs(level, begin, end);
        if inputs.is_empty() {
            return None;
        }
        // Cap the number of inputs to keep manual compactions
        // bounded. For v1 we don't cap (test sizes are small);
        // future optimization can chunk by total size.
        let mut compaction = Compaction {
            level,
            max_output_file_size: TARGET_FILE_SIZE,
            inputs: [inputs, Vec::new()],
            grandparents: Vec::new(),
            input_version: self.current.clone(),
            edit: crate::version_set::VersionEdit::default(),
        };
        self.setup_other_inputs(&mut compaction);
        Some(compaction)
    }

    /// Expand inputs[1] to overlap inputs[0]'s range; then try to
    /// expand inputs[0] without growing inputs[1]. Compute grandparents.
    /// `compact_pointer` stores the **largest internal key** (with
    /// the 8-byte tag) so that the next round-robin compare via
    /// InternalKeyComparator stays on internal-key inputs.
    fn setup_other_inputs(&mut self, c: &mut Compaction<C>) {
        let level = c.level;
        let (mut smallest, mut largest) = get_user_range(&c.inputs[0]);
        c.inputs[1] =
            self.current
                .get_overlapping_inputs(level + 1, Some(&smallest), Some(&largest));
        let (mut all_start, mut all_limit) = get_user_range_pair(&c.inputs[0], &c.inputs[1]);

        // Try to grow inputs[0] without growing inputs[1].
        if !c.inputs[1].is_empty() {
            let expanded0 =
                self.current
                    .get_overlapping_inputs(level, Some(&all_start), Some(&all_limit));
            let inputs1_size = total_file_size(&c.inputs[1]);
            let expanded0_size = total_file_size(&expanded0);
            let expanded_limit = 25 * TARGET_FILE_SIZE;
            if expanded0.len() > c.inputs[0].len() && inputs1_size + expanded0_size < expanded_limit
            {
                let (new_start, new_limit) = get_user_range(&expanded0);
                let expanded1 = self.current.get_overlapping_inputs(
                    level + 1,
                    Some(&new_start),
                    Some(&new_limit),
                );
                if expanded1.len() == c.inputs[1].len() {
                    smallest = new_start;
                    largest = new_limit;
                    c.inputs[0] = expanded0;
                    c.inputs[1] = expanded1;
                    let pair = get_user_range_pair(&c.inputs[0], &c.inputs[1]);
                    all_start = pair.0;
                    all_limit = pair.1;
                }
            }
        }

        // Grandparents: files at level+2 overlapping the combined range.
        if level + 2 < NUM_LEVELS {
            c.grandparents =
                self.current
                    .get_overlapping_inputs(level + 2, Some(&all_start), Some(&all_limit));
        }

        // compact_pointer stores the largest *internal* key
        // among inputs[0] (so subsequent picks compare apples-to-apples).
        let largest_internal = largest_internal_key(&c.inputs[0], &self.icmp).clone();
        self.compact_pointer[level] = largest_internal.clone();
        c.edit
            .compact_pointers
            .push(crate::version_set::CompactPointer {
                level: level as u32,
                key: largest_internal,
            });
        let _ = smallest;
        let _ = largest;
    }
}

/// Compute the user-key range (smallest, largest) of a set of files.
fn get_user_range(files: &[crate::version_set::FileMetaData]) -> (Vec<u8>, Vec<u8>) {
    assert!(!files.is_empty());
    let mut smallest = files[0].smallest[..files[0].smallest.len() - 8].to_vec();
    let mut largest = files[0].largest[..files[0].largest.len() - 8].to_vec();
    for f in &files[1..] {
        let s = &f.smallest[..f.smallest.len() - 8];
        let l = &f.largest[..f.largest.len() - 8];
        if s < smallest.as_slice() {
            smallest = s.to_vec();
        }
        if l > largest.as_slice() {
            largest = l.to_vec();
        }
    }
    (smallest, largest)
}

fn get_user_range_pair(
    a: &[crate::version_set::FileMetaData],
    b: &[crate::version_set::FileMetaData],
) -> (Vec<u8>, Vec<u8>) {
    if a.is_empty() {
        return get_user_range(b);
    }
    if b.is_empty() {
        return get_user_range(a);
    }
    let (a_lo, a_hi) = get_user_range(a);
    let (b_lo, b_hi) = get_user_range(b);
    let lo = if a_lo <= b_lo { a_lo } else { b_lo };
    let hi = if a_hi >= b_hi { a_hi } else { b_hi };
    (lo, hi)
}

/// Largest internal key among `files`, by InternalKeyComparator.
fn largest_internal_key<'a, C: Comparator + Clone>(
    files: &'a [crate::version_set::FileMetaData],
    icmp: &InternalKeyComparator<C>,
) -> &'a Vec<u8> {
    assert!(!files.is_empty());
    let mut best = &files[0].largest;
    for f in &files[1..] {
        if icmp.compare(&f.largest, best).is_gt() {
            best = &f.largest;
        }
    }
    best
}

/// A planned compaction. Holds the input files at `level` and
/// `level+1`, the grandparents at `level+2` (for overlap-bounding),
/// and the `VersionEdit` that records the result. The
/// `input_version` Arc keeps the source files alive across the
/// compaction even if VersionSet's current pointer moves.
pub struct Compaction<C: Comparator + Clone> {
    level: usize,
    max_output_file_size: u64,
    inputs: [Vec<crate::version_set::FileMetaData>; 2],
    grandparents: Vec<crate::version_set::FileMetaData>,
    input_version: Arc<Version<C>>,
    edit: crate::version_set::VersionEdit,
}

impl<C: Comparator + Clone> Compaction<C> {
    pub fn level(&self) -> usize {
        self.level
    }
    pub fn max_output_file_size(&self) -> u64 {
        self.max_output_file_size
    }
    pub fn num_input_files(&self, which: usize) -> usize {
        self.inputs[which].len()
    }
    pub fn input(&self, which: usize, i: usize) -> &crate::version_set::FileMetaData {
        &self.inputs[which][i]
    }
    pub fn inputs(&self, which: usize) -> &[crate::version_set::FileMetaData] {
        &self.inputs[which]
    }
    pub fn grandparents(&self) -> &[crate::version_set::FileMetaData] {
        &self.grandparents
    }
    pub fn input_version(&self) -> &Arc<Version<C>> {
        &self.input_version
    }
    pub fn edit(&self) -> &crate::version_set::VersionEdit {
        &self.edit
    }
    pub fn edit_mut(&mut self) -> &mut crate::version_set::VersionEdit {
        &mut self.edit
    }

    /// Trivial move: `input[0]` is one file, `input[1]` is empty,
    /// grandparent overlap is small. Output is just a level-bump.
    pub fn is_trivial_move(&self) -> bool {
        self.num_input_files(0) == 1
            && self.num_input_files(1) == 0
            && total_file_size(&self.grandparents) <= MAX_GRANDPARENT_OVERLAP_BYTES
    }

    /// True iff `user_key` does not exist in any level deeper than
    /// `level + 1`. Used by compaction to decide whether a deletion
    /// tombstone can be dropped: if no deeper data exists, the
    /// tombstone has no purpose.
    pub fn is_base_level_for_key(&self, user_key: &[u8]) -> bool {
        let v = &self.input_version;
        let ucmp = v.comparator().user_comparator().clone();
        for level in (self.level + 2)..NUM_LEVELS {
            for f in v.level_files(level) {
                let smallest_uk = &f.smallest[..f.smallest.len() - 8];
                let largest_uk = &f.largest[..f.largest.len() - 8];
                if ucmp.compare(user_key, smallest_uk).is_ge()
                    && ucmp.compare(user_key, largest_uk).is_le()
                {
                    return false;
                }
            }
        }
        true
    }
}
