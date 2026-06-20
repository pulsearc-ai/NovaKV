use crate::coding::{decode_fixed32, decode_fixed64};
use crate::status::{Result, Status};

const HEADER_LEN: usize = 12;

pub trait WriteBatchHandler {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
}

/// Alias for `WriteBatchHandler` so callers can write
/// `impl Handler for X` directly.
pub trait Handler: WriteBatchHandler {}
impl<T: WriteBatchHandler> Handler for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteBatch {
    rep: Vec<u8>,
}

impl WriteBatch {
    pub fn new() -> Self {
        let mut batch = Self { rep: Vec::new() };
        batch.clear();
        batch
    }

    /// Creates an empty batch whose buffer is pre-sized for
    /// `extra` bytes of record payload beyond the 12-byte
    /// header, so the first record write does not reallocate.
    pub fn with_capacity(extra: usize) -> Self {
        let mut rep = Vec::with_capacity(HEADER_LEN + extra);
        rep.resize(HEADER_LEN, 0);
        Self { rep }
    }

    pub fn clear(&mut self) {
        self.rep.clear();
        self.rep.resize(HEADER_LEN, 0);
    }

    pub fn count(&self) -> u32 {
        decode_fixed32(&self.rep[8..12])
    }

    fn set_count_inline(&mut self, count: u32) {
        self.rep[8..12].copy_from_slice(&count.to_le_bytes());
    }

    pub fn set_count(&mut self, count: u32) {
        self.set_count_inline(count);
    }

    pub fn sequence(&self) -> u64 {
        decode_fixed64(&self.rep[..8])
    }

    pub fn set_sequence(&mut self, sequence: u64) {
        self.rep[..8].copy_from_slice(&sequence.to_le_bytes());
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        let new_count = self.count().wrapping_add(1);
        self.set_count_inline(new_count);
        crate::write_batch::WriteBatchRecord::encode_put(
            &mut self.rep,
            key.as_ref(),
            value.as_ref(),
        );
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) {
        let new_count = self.count().wrapping_add(1);
        self.set_count_inline(new_count);
        crate::write_batch::WriteBatchRecord::encode_delete(&mut self.rep, key.as_ref());
    }

    pub fn contents(&self) -> &[u8] {
        &self.rep
    }

    pub fn contents_with_sequence(&self, sequence: u64) -> Vec<u8> {
        let mut rep = self.rep.clone();
        rep[..8].copy_from_slice(&sequence.to_le_bytes());
        rep
    }

    /// The byte length of the
    /// on-disk encoding (header + records). Useful for batching
    /// decisions: callers can stop appending once `approximate_size`
    /// reaches a target.
    pub fn approximate_size(&self) -> usize {
        self.rep.len()
    }

    pub fn set_contents(&mut self, contents: impl AsRef<[u8]>) -> Result<()> {
        let contents = contents.as_ref();
        if contents.len() < HEADER_LEN {
            return Err(Status::corruption("malformed WriteBatch (too small)"));
        }
        self.rep.clear();
        self.rep.extend_from_slice(contents);
        Ok(())
    }

    pub fn append(&mut self, source: &WriteBatch) {
        let new_count = self.count().wrapping_add(source.count());
        self.set_count_inline(new_count);
        self.rep.extend_from_slice(&source.rep[HEADER_LEN..]);
    }

    pub fn iterate<H: WriteBatchHandler>(&self, handler: &mut H) -> Result<()> {
        if self.rep.len() < HEADER_LEN {
            return Err(Status::corruption("malformed WriteBatch (too small)"));
        }
        let mut input: &[u8] = &self.rep[HEADER_LEN..];
        let mut adapter = HandlerAdapter {
            inner: handler,
            count: 0,
        };
        while !input.is_empty() {
            crate::write_batch::WriteBatchRecord::visit(&mut input, &mut adapter)?;
            adapter.count += 1;
        }
        if adapter.count != self.count() {
            return Err(Status::corruption("WriteBatch has wrong count"));
        }
        Ok(())
    }

    pub fn records(&self) -> Result<Vec<crate::write_batch::WriteBatchRecord>> {
        let mut collector = RecordCollector::default();
        self.iterate(&mut collector)?;
        Ok(collector.records)
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

struct HandlerAdapter<'a, H: WriteBatchHandler + ?Sized> {
    inner: &'a mut H,
    count: u32,
}

impl<H: WriteBatchHandler + ?Sized> crate::write_batch::WriteBatchRecordVisitor
    for HandlerAdapter<'_, H>
{
    fn delete(&mut self, key: &[u8]) {
        self.inner.delete(key);
    }
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.inner.put(key, value);
    }
}

#[derive(Default)]
struct RecordCollector {
    records: Vec<crate::write_batch::WriteBatchRecord>,
}

impl WriteBatchHandler for RecordCollector {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.records
            .push(crate::write_batch::WriteBatchRecord::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            });
    }
    fn delete(&mut self, key: &[u8]) {
        self.records
            .push(crate::write_batch::WriteBatchRecord::Delete { key: key.to_vec() });
    }
}

// ---- thread-local WriteBatch buffer pool ----

const POOL_MAX_SLOTS: usize = 8;
const POOL_MAX_CAPACITY: usize = 4096;

thread_local! {
    /// Per-thread cache of recycled `WriteBatch` buffers.
    /// Bounded to `POOL_MAX_SLOTS` entries of at most
    /// `POOL_MAX_CAPACITY` bytes each.
    static BATCH_POOL: std::cell::RefCell<Vec<WriteBatch>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// A `WriteBatch` drawn from the thread-local pool. Derefs to
/// `WriteBatch`, and returns its buffer to the pool on drop.
#[derive(Debug)]
pub struct PooledWriteBatch {
    // `Option` so `Drop` can move the buffer out by value.
    inner: Option<WriteBatch>,
}

impl std::ops::Deref for PooledWriteBatch {
    type Target = WriteBatch;
    fn deref(&self) -> &WriteBatch {
        self.inner
            .as_ref()
            .expect("PooledWriteBatch inner already taken")
    }
}

impl std::ops::DerefMut for PooledWriteBatch {
    fn deref_mut(&mut self) -> &mut WriteBatch {
        self.inner
            .as_mut()
            .expect("PooledWriteBatch inner already taken")
    }
}

impl Drop for PooledWriteBatch {
    fn drop(&mut self) {
        if let Some(batch) = self.inner.take() {
            recycle(batch);
        }
    }
}

/// Takes a cleared `WriteBatch` from the thread-local pool,
/// allocating a fresh one only when the pool is empty.
pub fn pool_take() -> PooledWriteBatch {
    let pooled = BATCH_POOL
        .try_with(|pool| pool.borrow_mut().pop())
        .ok()
        .flatten();
    let mut batch = pooled.unwrap_or_default();
    batch.clear();
    PooledWriteBatch { inner: Some(batch) }
}

/// Returns a buffer to the thread-local pool, or frees it when
/// the pool is full or the buffer exceeds `POOL_MAX_CAPACITY`.
/// `try_with` keeps a recycle during thread-local destruction
/// (panic unwind at thread exit) from panicking - it falls
/// back to a normal free.
fn recycle(batch: WriteBatch) {
    if batch.rep.capacity() > POOL_MAX_CAPACITY {
        return;
    }
    let _ = BATCH_POOL.try_with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < POOL_MAX_SLOTS {
            pool.push(batch);
        }
    });
}
