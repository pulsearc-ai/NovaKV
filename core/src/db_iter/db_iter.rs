use crate::comparator::Comparator;
use crate::format::{
    append_internal_key, parse_internal_key, ParsedInternalKey, SequenceNumber, ValueType,
    READ_BYTES_PERIOD, VALUE_TYPE_FOR_SEEK,
};
use crate::status::{Result, Status};

/// Iteration direction.
/// Forward: the inner iter is positioned exactly at the
/// entry that yields key()/value().  Reverse: the inner
/// iter is positioned just before all entries whose user_key
/// matches the saved key.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Reverse,
}

type ReadSampler = Box<dyn FnMut(&[u8]) + Send>;

/// Snapshot-aware database iterator.
///
/// Wraps an internal iterator (memtable + immutable +
/// per-level SSTs merged via `MergingIterator`) that yields
/// (internal_key, value) pairs in InternalKeyComparator order
/// (user_key ASC, sequence DESC). DBIter folds same-user_key
/// duplicates by sequence and hides:
///   * entries with `sequence > snapshot`,
///   * any entry shadowed by a more-recent same-user_key entry,
///   * Deletion tombstones (and entries they shadow).
///
/// Generic over the user-key comparator `C` (a `BytewiseComparator`
/// in v1) and the inner iterator type `I`.
pub struct DBIter<C: Comparator, I: crate::db_iter::DbIterator> {
    user_comparator: C,
    iter: I,
    sequence: SequenceNumber,
    direction: Direction,
    valid: bool,
    /// Reverse-mode current key; also used as scratch for Seek.
    saved_key: Vec<u8>,
    /// Reverse-mode current value (Forward reads from `iter`).
    saved_value: Vec<u8>,
    status: Result<()>,
    /// Phase 58: optional read sampler. Called periodically
    /// with the inner iter's current internal key to drive
    /// seek-triggered compaction by recording read samples
    /// as keys are parsed.
    sampler: Option<ReadSampler>,
    /// Bytes-budget counter; decrements on each parsed entry.
    /// When it underflows, fire the sampler and reset.
    bytes_counter: i64,
    /// Sample-period in bytes. Defaults to `READ_BYTES_PERIOD`
    /// (1 MiB).
    read_bytes_period: i64,
    /// Cleanup hook installed by DBImpl to release iterator-held
    /// version/file liveness when the public iterator is dropped.
    drop_hook: Option<Box<dyn FnMut() + Send>>,
}

impl<C: Comparator, I: crate::db_iter::DbIterator> DBIter<C, I> {
    pub fn new(user_comparator: C, iter: I, sequence: SequenceNumber) -> Self {
        Self {
            user_comparator,
            iter,
            sequence,
            direction: Direction::Forward,
            valid: false,
            saved_key: Vec::new(),
            saved_value: Vec::new(),
            status: Ok(()),
            sampler: None,
            bytes_counter: READ_BYTES_PERIOD as i64,
            read_bytes_period: READ_BYTES_PERIOD as i64,
            drop_hook: None,
        }
    }

    /// Install a read-sampler closure. The closure fires
    /// periodically with the inner iter's current internal
    /// key, driving seek-triggered compaction.
    pub fn set_sampler(&mut self, sampler: ReadSampler) {
        self.sampler = Some(sampler);
    }

    pub fn set_drop_hook(&mut self, drop_hook: Box<dyn FnMut() + Send>) {
        self.drop_hook = Some(drop_hook);
    }

    /// Override the byte interval between samples. Useful for
    /// tests that want sampling at a higher rate than the
    /// production default of `READ_BYTES_PERIOD` (1 MiB).
    pub fn set_read_bytes_period(&mut self, period: i64) {
        self.read_bytes_period = period.max(1);
        self.bytes_counter = self.read_bytes_period;
    }

    /// Charge the bytes counter for the inner iter's current
    /// (key, value) pair and fire the sampler if the counter
    /// has gone negative.
    fn maybe_sample(&mut self) {
        let n = (self.iter.key().len() + self.iter.value().len()) as i64;
        self.bytes_counter -= n;
        while self.bytes_counter < 0 {
            self.bytes_counter += self.read_bytes_period;
            if let Some(s) = self.sampler.as_mut() {
                s(self.iter.key());
            }
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    /// User key of the current entry. Forward mode: extracted
    /// from the inner iter. Reverse mode: read from `saved_key`.
    pub fn key(&self) -> &[u8] {
        assert!(self.valid, "DBIter::key on invalid iterator");
        match self.direction {
            Direction::Forward => {
                let k = self.iter.key();
                &k[..k.len() - 8]
            }
            Direction::Reverse => &self.saved_key,
        }
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.valid, "DBIter::value on invalid iterator");
        match self.direction {
            Direction::Forward => self.iter.value(),
            Direction::Reverse => &self.saved_value,
        }
    }

    pub fn status(&self) -> Result<()> {
        match &self.status {
            Ok(()) => self.iter.status(),
            Err(e) => Err(e.clone()),
        }
    }

    pub fn seek_to_first(&mut self) {
        self.direction = Direction::Forward;
        self.saved_value.clear();
        self.iter.seek_to_first();
        if self.iter.valid() {
            self.find_next_user_entry(false);
        } else {
            self.valid = false;
        }
    }

    pub fn seek_to_last(&mut self) {
        self.direction = Direction::Reverse;
        self.saved_value.clear();
        self.iter.seek_to_last();
        self.find_prev_user_entry();
    }

    pub fn seek(&mut self, target: &[u8]) {
        self.direction = Direction::Forward;
        self.saved_value.clear();
        self.saved_key.clear();
        // Build an internal key with the snapshot sequence so
        // the inner iter lands on the newest visible entry.
        append_internal_key(
            &mut self.saved_key,
            &ParsedInternalKey::new(target, self.sequence, VALUE_TYPE_FOR_SEEK),
        );
        let saved_internal = self.saved_key.clone();
        self.iter.seek(&saved_internal);
        if self.iter.valid() {
            self.find_next_user_entry(false);
        } else {
            self.valid = false;
        }
    }

    pub fn next(&mut self) {
        assert!(self.valid, "DBIter::next on invalid iterator");
        if self.direction == Direction::Reverse {
            self.direction = Direction::Forward;
            // The inner iter is positioned just before the entries
            // for this->key(); advance into them and reuse the
            // forward skipping logic to skip the current key.
            if !self.iter.valid() {
                self.iter.seek_to_first();
            } else {
                self.iter.next();
            }
            if !self.iter.valid() {
                self.valid = false;
                self.saved_key.clear();
                return;
            }
            // saved_key already holds the key to skip past.
        } else {
            // Save the current user_key into saved_key so
            // find_next_user_entry skips it.
            let cur = self.iter.key();
            let user_key = &cur[..cur.len() - 8];
            self.saved_key.clear();
            self.saved_key.extend_from_slice(user_key);
        }
        self.find_next_user_entry(true);
    }

    pub fn prev(&mut self) {
        assert!(self.valid, "DBIter::prev on invalid iterator");
        if self.direction == Direction::Forward {
            // Switch direction. Inner iter is at the current
            // entry; scan backwards until the user_key changes.
            let cur_user = {
                let k = self.iter.key();
                k[..k.len() - 8].to_vec()
            };
            self.saved_key = cur_user;
            loop {
                self.iter.prev();
                if !self.iter.valid() {
                    self.valid = false;
                    self.saved_key.clear();
                    self.saved_value.clear();
                    return;
                }
                let k = self.iter.key();
                let uk = &k[..k.len() - 8];
                if self.user_comparator.compare(uk, &self.saved_key).is_lt() {
                    break;
                }
            }
            self.direction = Direction::Reverse;
        }
        self.find_prev_user_entry();
    }

    /// Forward sweep over inner-iter entries until one is
    /// visible (sequence <= snapshot) and not shadowed by a
    /// previously-seen user_key.
    ///
    /// `skipping = true` means: skip any entry whose user_key
    /// equals `saved_key`. Used by `next` (skip the key we
    /// just yielded) and by deletion-tombstone shadowing.
    fn find_next_user_entry(&mut self, mut skipping: bool) {
        assert!(self.iter.valid());
        assert!(self.direction == Direction::Forward);
        loop {
            self.maybe_sample();
            let parsed = match parse_internal_key(self.iter.key()) {
                Some(p) => p,
                None => {
                    self.status = Err(Status::corruption("corrupted internal key in DBIter"));
                    self.valid = false;
                    return;
                }
            };
            if parsed.sequence <= self.sequence {
                match parsed.value_type {
                    ValueType::Deletion => {
                        // Hide upcoming entries for this user_key.
                        self.saved_key.clear();
                        self.saved_key.extend_from_slice(&parsed.user_key);
                        skipping = true;
                    }
                    ValueType::Value => {
                        let shadowed = skipping
                            && self
                                .user_comparator
                                .compare(&parsed.user_key, &self.saved_key)
                                .is_le();
                        if !shadowed {
                            self.valid = true;
                            self.saved_key.clear();
                            return;
                        }
                    }
                }
            }
            self.iter.next();
            if !self.iter.valid() {
                break;
            }
        }
        self.saved_key.clear();
        self.valid = false;
    }

    /// Reverse sweep. Finds the visible entry for the user_key
    /// immediately less than the current `saved_key`.
    /// State machine: walk backwards;
    /// for each entry with sequence <= snapshot, if it's a
    /// Value, remember it; if Deletion, clear the remembered
    /// value. Stop when the user_key changes and we have a
    /// non-deletion remembered.
    fn find_prev_user_entry(&mut self) {
        assert!(self.direction == Direction::Reverse);
        let mut value_type = ValueType::Deletion;
        if self.iter.valid() {
            loop {
                self.maybe_sample();
                let parsed = match parse_internal_key(self.iter.key()) {
                    Some(p) => p,
                    None => {
                        self.status = Err(Status::corruption("corrupted internal key in DBIter"));
                        self.valid = false;
                        return;
                    }
                };
                if parsed.sequence <= self.sequence {
                    if value_type != ValueType::Deletion
                        && self
                            .user_comparator
                            .compare(&parsed.user_key, &self.saved_key)
                            .is_lt()
                    {
                        // Found a non-deletion for an earlier user_key -
                        // we are done; the remembered (saved_key,
                        // saved_value) is the visible entry.
                        break;
                    }
                    value_type = parsed.value_type;
                    if value_type == ValueType::Deletion {
                        self.saved_key.clear();
                        self.saved_value.clear();
                    } else {
                        let raw_value = self.iter.value();
                        self.saved_key.clear();
                        self.saved_key.extend_from_slice(&parsed.user_key);
                        self.saved_value.clear();
                        self.saved_value.extend_from_slice(raw_value);
                    }
                }
                self.iter.prev();
                if !self.iter.valid() {
                    break;
                }
            }
        }

        if value_type == ValueType::Deletion {
            self.valid = false;
            self.saved_key.clear();
            self.saved_value.clear();
            self.direction = Direction::Forward;
        } else {
            self.valid = true;
        }
    }
}

impl<C: Comparator, I: crate::db_iter::DbIterator> crate::db_iter::DbIterator for DBIter<C, I> {
    fn valid(&self) -> bool {
        DBIter::valid(self)
    }
    fn seek_to_first(&mut self) {
        DBIter::seek_to_first(self)
    }
    fn seek_to_last(&mut self) {
        DBIter::seek_to_last(self)
    }
    fn seek(&mut self, target: &[u8]) {
        DBIter::seek(self, target)
    }
    fn next(&mut self) {
        DBIter::next(self)
    }
    fn prev(&mut self) {
        DBIter::prev(self)
    }
    fn key(&self) -> &[u8] {
        DBIter::key(self)
    }
    fn value(&self) -> &[u8] {
        DBIter::value(self)
    }
    fn status(&self) -> Result<()> {
        DBIter::status(self)
    }
}

impl<C: Comparator, I: crate::db_iter::DbIterator> Drop for DBIter<C, I> {
    fn drop(&mut self) {
        if let Some(mut drop_hook) = self.drop_hook.take() {
            drop_hook();
        }
    }
}
