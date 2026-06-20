use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::comparator::Comparator;
use crate::format::{InternalKeyComparator, LookupKey, ValueType};
use crate::skiplist::{SkipList, SkipListCursor};
use crate::status::{Result, Status};

fn get_memtable_length_prefixed_slice(data: &[u8]) -> Option<(&[u8], usize)> {
    let first = *data.first()?;
    if first < 0x80 {
        let len = first as usize;
        let start = 1;
        let end = start + len;
        return (data.len() >= end).then_some((&data[start..end], end));
    }
    let mut result = u32::from(first & 0x7f);
    for index in 1..5 {
        let byte = *data.get(index)?;
        if byte & 0x80 != 0 {
            result |= u32::from(byte & 0x7f) << (7 * index);
        } else {
            result |= u32::from(byte) << (7 * index);
            let start = index + 1;
            let end = start + result as usize;
            return (data.len() >= end).then_some((&data[start..end], end));
        }
    }
    None
}

/// Decode the varint32 length prefix of a memtable entry and
/// return the internal-key slice it guards, skipping every
/// bounds check. This runs on every skiplist comparison, so
/// the checked `get_memtable_length_prefixed_slice` (kept for
/// cold callers) is too expensive here.
///
/// # Safety
///
/// `entry` must be a well-formed memtable key: a varint32
/// length prefix `n` (1-5 bytes) followed by at least `n`
/// readable bytes, and that prefixed internal key must itself
/// be at least 8 bytes (user key + 8-byte tag). Every memtable
/// key satisfies this by construction: node keys are built by
/// `MemTable::add` and search keys by `LookupKey::new` /
/// `MemTableIterator::seek`, and the skiplist only ever feeds
/// `KeyComparator` those keys. Passing any other slice - in
/// particular one shorter than the prefix claims - is
/// undefined behavior.
#[inline]
unsafe fn memtable_internal_key_unchecked(entry: &[u8]) -> &[u8] {
    // SAFETY: a well-formed entry is non-empty (the length
    // prefix is at least one byte), so index 0 is readable.
    let first = unsafe { *entry.get_unchecked(0) };
    if first < 0x80 {
        // 1-byte length prefix - internal keys below 128 bytes,
        // the overwhelmingly common case.
        // SAFETY: the entry holds at least `first` bytes after
        // the 1-byte prefix, per the function contract.
        return unsafe { entry.get_unchecked(1..1 + first as usize) };
    }
    // Cold path: multi-byte varint32 (internal keys >= 128 bytes).
    let mut len = (first & 0x7f) as usize;
    let mut shift = 7u32;
    let mut idx = 1usize;
    loop {
        // SAFETY: a well-formed varint32 terminates within 5
        // bytes, every one of which is part of `entry`.
        let byte = unsafe { *entry.get_unchecked(idx) };
        idx += 1;
        if byte & 0x80 == 0 {
            len |= (byte as usize) << shift;
            break;
        }
        len |= ((byte & 0x7f) as usize) << shift;
        shift += 7;
    }
    // SAFETY: the entry holds at least `len` bytes after the
    // `idx`-byte prefix, per the function contract.
    unsafe { entry.get_unchecked(idx..idx + len) }
}

/// Comparator that decodes the leading length-prefix of a
/// memtable entry and compares the contained internal_keys
/// via `InternalKeyComparator`.
#[derive(Debug, Clone)]
pub struct KeyComparator<C: Comparator + Clone> {
    inner: InternalKeyComparator<C>,
}

impl<C: Comparator + Clone> Comparator for KeyComparator<C> {
    fn name(&self) -> &'static str {
        "novakv.MemTableKeyComparator"
    }
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        // SAFETY: the memtable skiplist only ever passes its
        // comparator well-formed memtable keys - `a` is a node
        // key built by `MemTable::add`, `b` a search key built
        // by `LookupKey::new` / `MemTableIterator::seek` - so
        // the unchecked decode is sound (see
        // `memtable_internal_key_unchecked`).
        let a_ik = unsafe { memtable_internal_key_unchecked(a) };
        let b_ik = unsafe { memtable_internal_key_unchecked(b) };
        // Internal-key order follows `InternalKeyComparator`:
        // user key ascending, then the trailing 8-byte tag
        // descending (newer sequence numbers sort first).
        let a_split = a_ik.len() - 8;
        let b_split = b_ik.len() - 8;
        // SAFETY: an internal key is >= 8 bytes, so `a_split`
        // and `b_split` are valid split points and each tail
        // slice is exactly the 8-byte tag.
        let (a_user, a_tag) =
            unsafe { (a_ik.get_unchecked(..a_split), a_ik.get_unchecked(a_split..)) };
        let (b_user, b_tag) =
            unsafe { (b_ik.get_unchecked(..b_split), b_ik.get_unchecked(b_split..)) };
        match self.inner.user_comparator().compare(a_user, b_user) {
            Ordering::Equal => {
                // SAFETY: `a_tag`/`b_tag` are exactly 8 bytes.
                let a_num = u64::from_le_bytes(unsafe { a_tag.try_into().unwrap_unchecked() });
                let b_num = u64::from_le_bytes(unsafe { b_tag.try_into().unwrap_unchecked() });
                b_num.cmp(&a_num)
            }
            other => other,
        }
    }
    fn find_shortest_separator(&self, _: &mut Vec<u8>, _: &[u8]) {}
    fn find_short_successor(&self, _: &mut Vec<u8>) {}
}

/// In-memory write buffer. Stores entries as
/// `varint32(internal_key_len) + internal_key + varint32(value_len) + value`
/// in a SkipList ordered by `KeyComparator`.
pub struct MemTable<C: Comparator + Clone> {
    comparator: InternalKeyComparator<C>,
    list: SkipList<KeyComparator<C>>,
}

impl<C: Comparator + Clone> MemTable<C> {
    pub fn new(comparator: C) -> Self {
        let internal = InternalKeyComparator::new(comparator);
        let key_cmp = KeyComparator {
            inner: internal.clone(),
        };
        Self {
            comparator: internal,
            list: SkipList::new(key_cmp),
        }
    }

    pub fn reserve_entries(&mut self, additional: usize) {
        self.list.reserve(additional);
    }

    /// Insert a single record. Returns the encoded entry size
    /// in bytes - the caller (usually `DBImplCore::write_with_options`
    /// via `MemTableInserter`) accumulates these to maintain a
    /// non-atomic running total on `DBImplCore`. We don't track
    /// the total here because `MemTable` is shared via
    /// `Arc<UnsafeCell<...>>` and a plain `usize` field would
    /// hit Rust's `&mut`/`&` aliasing rules under the
    /// `with_mut`/`with_ref` pattern.
    pub fn add(&mut self, sequence: u64, value_type: ValueType, key: &[u8], value: &[u8]) -> usize {
        let internal_key_size = key.len() + 8;
        let val_size = value.len();
        let encoded_size = crate::coding::varint_length(internal_key_size as u64)
            + internal_key_size
            + crate::coding::varint_length(val_size as u64)
            + val_size;
        let tag = (sequence << 8) | value_type as u64;
        self.list.insert_with(encoded_size, |entry| {
            let mut offset = 0usize;
            offset +=
                crate::coding::encode_varint32(&mut entry[offset..], internal_key_size as u32);
            entry[offset..offset + key.len()].copy_from_slice(key);
            offset += key.len();
            crate::coding::encode_fixed64(&mut entry[offset..offset + 8], tag);
            offset += 8;
            offset += crate::coding::encode_varint32(&mut entry[offset..], val_size as u32);
            entry[offset..offset + val_size].copy_from_slice(value);
            offset += val_size;
            debug_assert_eq!(offset, encoded_size);
        });
        encoded_size
    }

    /// Look up `lookup_key` in this memtable. Returns:
    /// - `None` - key not present in this memtable; caller should look elsewhere.
    /// - `Some(Err(NotFound))` - found a deletion tombstone.
    /// - `Some(Ok(value))` - found the value.
    pub fn get(&self, lookup_key: &LookupKey) -> Option<Result<Vec<u8>>> {
        let mem_key = lookup_key.memtable_key();
        let mut iter = self.list.iter();
        iter.seek(mem_key);
        if !iter.valid() {
            return None;
        }
        let entry = iter.key();
        let (internal_key, value_offset) = get_memtable_length_prefixed_slice(entry)?;
        if internal_key.len() < 8 {
            return Some(Err(Status::corruption("memtable: short internal key")));
        }
        let entry_user_key = &internal_key[..internal_key.len() - 8];
        if self
            .comparator
            .user_comparator()
            .compare(entry_user_key, lookup_key.user_key())
            .is_ne()
        {
            return None;
        }
        let tag = crate::coding::decode_fixed64(&internal_key[internal_key.len() - 8..]);
        let value_type = (tag & 0xff) as u8;
        if value_type == ValueType::Deletion as u8 {
            return Some(Err(Status::not_found("memtable tombstone")));
        }
        let value = match get_memtable_length_prefixed_slice(&entry[value_offset..]) {
            Some((value, _)) => value,
            None => return Some(Err(Status::corruption("memtable: missing value"))),
        };
        Some(Ok(value.to_vec()))
    }

    /// Read every (internal_key, value) pair in sorted order.
    /// Used by memtable flush; allocates O(memtable size).
    pub fn collect_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut iter = self.list.iter();
        iter.seek_to_first();
        while iter.valid() {
            let entry = iter.key();
            let (internal_key, value_offset) =
                get_memtable_length_prefixed_slice(entry).expect("memtable entry: internal_key");
            let (value, _) = get_memtable_length_prefixed_slice(&entry[value_offset..])
                .expect("memtable entry: value");
            out.push((internal_key.to_vec(), value.to_vec()));
            iter.next();
        }
        out
    }
}

/// Ref-counted memtable handle. Pins the memtable's lifetime
/// so DB iterators can keep walking a memtable
/// after it has been replaced by a newer active memtable.
pub struct SharedMemTable<C: Comparator + Clone> {
    inner: Arc<UnsafeCell<MemTable<C>>>,
}

impl<C: Comparator + Clone> Clone for SharedMemTable<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// SAFETY: DBImpl serializes writers with its mutex. The backing
// SkipList publishes links with atomics, upholding the
// single-writer / multi-reader memtable contract.
unsafe impl<C: Comparator + Clone + Send + Sync> Send for SharedMemTable<C> {}
unsafe impl<C: Comparator + Clone + Send + Sync> Sync for SharedMemTable<C> {}

impl<C: Comparator + Clone> SharedMemTable<C> {
    pub fn new(comparator: C) -> Self {
        Self {
            inner: Arc::new(UnsafeCell::new(MemTable::new(comparator))),
        }
    }

    fn with_ref<R>(&self, f: impl FnOnce(&MemTable<C>) -> R) -> R {
        unsafe { f(&*self.inner.get()) }
    }

    fn with_mut<R>(&self, f: impl FnOnce(&mut MemTable<C>) -> R) -> R {
        unsafe { f(&mut *self.inner.get()) }
    }

    pub fn reserve_entries(&self, additional: usize) {
        self.with_mut(|mem| mem.reserve_entries(additional));
    }

    /// Insert a record. Returns the encoded size for the
    /// caller to accumulate into `DBImplCore::mem_memory_usage`.
    pub fn add(&self, sequence: u64, value_type: ValueType, key: &[u8], value: &[u8]) -> usize {
        self.with_mut(|mem| mem.add(sequence, value_type, key, value))
    }

    pub fn get(&self, lookup_key: &LookupKey) -> Option<Result<Vec<u8>>> {
        self.with_ref(|mem| mem.get(lookup_key))
    }

    pub fn collect_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.with_ref(|mem| mem.collect_entries())
    }

    pub fn new_iterator(&self) -> MemTableIterator<C> {
        MemTableIterator::new(self.clone())
    }
}

/// Streaming iterator over memtable entries. key()/value()
/// return slices into the arena-backed entry.
pub struct MemTableIterator<C: Comparator + Clone> {
    table: SharedMemTable<C>,
    iter: SkipListCursor<KeyComparator<C>>,
    tmp: Vec<u8>,
}

impl<C: Comparator + Clone> MemTableIterator<C> {
    fn new(table: SharedMemTable<C>) -> Self {
        let iter = table.with_ref(|mem| mem.list.cursor());
        Self {
            table,
            iter,
            tmp: Vec::new(),
        }
    }

    fn entry(&self) -> &[u8] {
        let _keep_alive = &self.table;
        self.iter.key()
    }
}

impl<C: Comparator + Clone> crate::db_iter::DbIterator for MemTableIterator<C> {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }
    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }

    fn seek(&mut self, target: &[u8]) {
        self.tmp.clear();
        crate::coding::put_varint32(&mut self.tmp, target.len() as u32);
        self.tmp.extend_from_slice(target);
        self.iter.seek(&self.tmp);
    }

    fn next(&mut self) {
        self.iter.next();
    }
    fn prev(&mut self) {
        self.iter.prev();
    }

    fn key(&self) -> &[u8] {
        let (internal_key, _) =
            get_memtable_length_prefixed_slice(self.entry()).expect("memtable iterator key");
        internal_key
    }

    fn value(&self) -> &[u8] {
        let entry = self.entry();
        let (_, value_offset) =
            get_memtable_length_prefixed_slice(entry).expect("memtable iterator internal key");
        let (value, _) = get_memtable_length_prefixed_slice(&entry[value_offset..])
            .expect("memtable iterator value");
        value
    }

    fn status(&self) -> Result<()> {
        Ok(())
    }
}
