use crate::block::{Block, BlockBuilder, BlockIter};
use crate::cache::Cache;
use crate::comparator::Comparator;
use crate::db_iter::DbIterator;
use crate::env::{RandomAccessFile, WritableFile};
use crate::filter::{FilterPolicy, InternalFilterPolicy};
use crate::filter_block::{FilterBlockBuilder, FilterBlockReader};
use crate::status::{Result, Status};
use crate::two_level_iter::TwoLevelIterator;
use std::sync::Arc;

pub const DEFAULT_BLOCK_SIZE: usize = 4096;
pub const DEFAULT_BLOCK_RESTART_INTERVAL: usize = 16;
/// Size of the 5-byte trailer following every block:
/// 1-byte compression type +
/// 4-byte CRC32C of (data || compression_type).
pub const BLOCK_TRAILER_SIZE: usize = 5;

/// Pluggable block compression. Each implementation
/// claims one block-trailer kind byte (e.g. `BlockTrailer::KIND_SNAPPY = 1`)
/// and provides matching compress/decompress.
///
/// `Options::compressor` is the install point - if `Some`,
/// `TableFileBuilder` consults it for every data/index/meta
/// block, falling back to uncompressed when the compressed
/// output is < 12.5% smaller. On the read side,
/// `read_block_from_file` consults it whenever the trailer
/// kind is non-zero.
pub trait Compressor: std::fmt::Debug + Send + Sync {
    /// The trailer kind byte this compressor produces /
    /// recognizes. Must be > 0 (zero is reserved for
    /// uncompressed). Standard values:
    /// 1 = Snappy.
    fn kind(&self) -> u8;

    /// Compress `input`. Return `Some` to install the
    /// compressed bytes; return `None` to fall back to
    /// uncompressed storage (e.g. when the algorithm
    /// detected the input is incompressible, or the
    /// 12.5% threshold wasn't met).
    fn compress(&self, input: &[u8]) -> Option<Vec<u8>>;

    /// Decompress `input`. Returns `Err(Status::corruption)`
    /// for malformed input.
    fn decompress(&self, input: &[u8]) -> Result<Vec<u8>>;
}

/// Builds an SSTable file: writes a sequence of
/// prefix-compressed data blocks with restart points,
/// followed by an empty meta-index block, an index block,
/// and a 48-byte footer (no filter block in v1).
pub struct TableBuilder<C: Comparator + Clone> {
    comparator: C,
    block_size: usize,
    sink: Vec<u8>,
    data_block: BlockBuilder<C>,
    index_block: BlockBuilder<C>,
    last_key: Vec<u8>,
    num_entries: u64,
    closed: bool,
    pending_index_entry: bool,
    pending_handle: crate::block::BlockHandle,
}

impl<C: Comparator + Clone> TableBuilder<C> {
    pub fn new(comparator: C, block_size: usize, restart_interval: usize) -> Self {
        assert!(restart_interval >= 1);
        Self {
            comparator: comparator.clone(),
            block_size,
            sink: Vec::new(),
            data_block: BlockBuilder::new(comparator.clone(), restart_interval),
            // Index uses restart_interval = 1 so every entry is a
            // restart point - that lets seek() do a clean binary search.
            index_block: BlockBuilder::new(comparator, 1),
            last_key: Vec::new(),
            num_entries: 0,
            closed: false,
            pending_index_entry: false,
            pending_handle: crate::block::BlockHandle::default(),
        }
    }

    pub fn with_defaults(comparator: C) -> Self {
        Self::new(
            comparator,
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_RESTART_INTERVAL,
        )
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }
    pub fn file_size(&self) -> u64 {
        self.sink.len() as u64
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.closed, "TableBuilder: add after finish");
        if self.num_entries > 0 {
            assert!(
                self.comparator.compare(key, &self.last_key).is_gt(),
                "TableBuilder: keys must be added in increasing order"
            );
        }
        if self.pending_index_entry {
            assert!(self.data_block.empty());
            self.comparator
                .find_shortest_separator(&mut self.last_key, key);
            let mut handle_bytes = Vec::new();
            self.pending_handle.encode(&mut handle_bytes);
            let last_key = self.last_key.clone();
            self.index_block.add(&last_key, &handle_bytes);
            self.pending_index_entry = false;
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.num_entries += 1;
        self.data_block.add(key, value);
        if self.data_block.current_size_estimate() >= self.block_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        assert!(!self.closed);
        if self.data_block.empty() {
            return;
        }
        assert!(!self.pending_index_entry);
        let block_bytes = self.data_block.finish().to_vec();
        let handle = Self::write_block(&mut self.sink, &block_bytes);
        self.data_block.reset();
        self.pending_handle = handle;
        self.pending_index_entry = true;
    }

    fn write_block(sink: &mut Vec<u8>, block_data: &[u8]) -> crate::block::BlockHandle {
        let kind = crate::block::BlockTrailer::KIND_NO_COMPRESSION;
        let crc = crate::crc32c::value(block_data);
        let crc = crate::crc32c::extend(crc, &[kind]);
        let offset = sink.len() as u64;
        sink.extend_from_slice(block_data);
        let trailer = crate::block::BlockTrailer { kind, crc };
        trailer.encode(sink);
        crate::block::BlockHandle {
            offset,
            size: block_data.len() as u64,
        }
    }

    pub fn finish(&mut self) -> Result<()> {
        assert!(!self.closed);
        self.flush();
        // Empty metaindex block (no filter in v1).
        let mut metaindex = BlockBuilder::new(self.comparator.clone(), 1);
        let metaindex_bytes = metaindex.finish().to_vec();
        let metaindex_handle = Self::write_block(&mut self.sink, &metaindex_bytes);

        // Final pending index entry.
        if self.pending_index_entry {
            self.comparator.find_short_successor(&mut self.last_key);
            let mut handle_bytes = Vec::new();
            self.pending_handle.encode(&mut handle_bytes);
            let last_key = self.last_key.clone();
            self.index_block.add(&last_key, &handle_bytes);
            self.pending_index_entry = false;
        }
        let index_bytes = self.index_block.finish().to_vec();
        let index_handle = Self::write_block(&mut self.sink, &index_bytes);

        // Footer.
        let footer = crate::table::TableFooter {
            metaindex: metaindex_handle,
            index: index_handle,
        };
        footer.encode(&mut self.sink);
        self.closed = true;
        Ok(())
    }

    pub fn contents(&self) -> &[u8] {
        assert!(self.closed, "TableBuilder: contents called before finish");
        &self.sink
    }
}

/// WritableFile-backed table builder. Uses the same block,
/// index, trailer, and footer encoding as `TableBuilder`, but
/// streams completed sections to the file instead of retaining
/// the entire SST in memory.
/// Type alias for the wrapped policy stored inside a streaming
/// builder, wrapped so filter keys use internal-key encoding.
type InternalFilter = InternalFilterPolicy<Arc<dyn FilterPolicy + Send + Sync>>;

pub struct TableFileBuilder<C: Comparator + Clone, W: WritableFile> {
    comparator: C,
    block_size: usize,
    file: W,
    offset: u64,
    data_block: BlockBuilder<C>,
    index_block: BlockBuilder<C>,
    last_key: Vec<u8>,
    num_entries: u64,
    closed: bool,
    pending_index_entry: bool,
    pending_handle: crate::block::BlockHandle,
    /// Phase B: optional filter block. When `Some`, every key
    /// passed to `add()` is also fed to the filter; on `finish()`,
    /// the filter block is written and a metaindex entry
    /// `"filter.<policy.name()>"` is emitted.
    filter_block: Option<FilterBlockBuilder<InternalFilter>>,
    /// User-policy name, captured at construction so `finish()`
    /// can build the metaindex key without re-borrowing the policy.
    filter_policy_name: Option<&'static str>,
    /// Phase E: optional block compressor. When `Some`, every
    /// block written through `write_block` is run through
    /// `compressor.compress`. If the compressor returns `None`
    /// (e.g. ratio < 12.5%), the block is stored uncompressed.
    compressor: Option<Arc<dyn Compressor>>,
}

impl<C: Comparator + Clone, W: WritableFile> TableFileBuilder<C, W> {
    pub fn new(comparator: C, block_size: usize, restart_interval: usize, file: W) -> Self {
        Self::with_filter(comparator, block_size, restart_interval, file, None)
    }

    pub fn with_defaults(comparator: C, file: W) -> Self {
        Self::new(
            comparator,
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_RESTART_INTERVAL,
            file,
        )
    }

    /// Construct a builder that emits a filter block for every
    /// data block, using the supplied filter policy.
    pub fn with_filter(
        comparator: C,
        block_size: usize,
        restart_interval: usize,
        file: W,
        filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
    ) -> Self {
        Self::with_options(
            comparator,
            block_size,
            restart_interval,
            file,
            filter_policy,
            None,
        )
    }

    /// Phase E: full constructor accepting both a filter policy
    /// and a block compressor. Either or both may be `None`.
    pub fn with_options(
        comparator: C,
        block_size: usize,
        restart_interval: usize,
        file: W,
        filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
        compressor: Option<Arc<dyn Compressor>>,
    ) -> Self {
        assert!(restart_interval >= 1);
        let (filter_block, filter_policy_name) = match filter_policy {
            Some(policy) => {
                let name = policy.name();
                let mut fb = FilterBlockBuilder::new(InternalFilterPolicy::new(policy));
                // Start the first filter block at offset 0 right
                // after construction.
                fb.start_block(0);
                (Some(fb), Some(name))
            }
            None => (None, None),
        };
        Self {
            comparator: comparator.clone(),
            block_size,
            file,
            offset: 0,
            data_block: BlockBuilder::new(comparator.clone(), restart_interval),
            index_block: BlockBuilder::new(comparator, 1),
            last_key: Vec::new(),
            num_entries: 0,
            closed: false,
            pending_index_entry: false,
            pending_handle: crate::block::BlockHandle::default(),
            filter_block,
            filter_policy_name,
            compressor,
        }
    }

    /// Build with a filter, using the default block size and
    /// restart interval.
    pub fn with_defaults_and_filter(
        comparator: C,
        file: W,
        filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
    ) -> Self {
        Self::with_filter(
            comparator,
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_RESTART_INTERVAL,
            file,
            filter_policy,
        )
    }

    /// Phase E: build with both filter + compressor at default
    /// block size and restart interval.
    pub fn with_defaults_filter_and_compressor(
        comparator: C,
        file: W,
        filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
        compressor: Option<Arc<dyn Compressor>>,
    ) -> Self {
        Self::with_options(
            comparator,
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_RESTART_INTERVAL,
            file,
            filter_policy,
            compressor,
        )
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }
    pub fn file_size(&self) -> u64 {
        self.offset
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.closed, "TableFileBuilder: add after finish");
        if self.num_entries > 0 {
            assert!(
                self.comparator.compare(key, &self.last_key).is_gt(),
                "TableFileBuilder: keys must be added in increasing order"
            );
        }
        if self.pending_index_entry {
            assert!(self.data_block.empty());
            self.comparator
                .find_shortest_separator(&mut self.last_key, key);
            let mut handle_bytes = Vec::new();
            self.pending_handle.encode(&mut handle_bytes);
            let last_key = self.last_key.clone();
            self.index_block.add(&last_key, &handle_bytes);
            self.pending_index_entry = false;
        }
        // Feed every key to the filter before recording it
        // in the data block.
        if let Some(fb) = self.filter_block.as_mut() {
            fb.add_key(key);
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.num_entries += 1;
        self.data_block.add(key, value);
        if self.data_block.current_size_estimate() >= self.block_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        assert!(!self.closed);
        if self.data_block.empty() {
            return Ok(());
        }
        assert!(!self.pending_index_entry);
        let block_bytes = self.data_block.finish().to_vec();
        let handle = self.write_block(&block_bytes)?;
        self.data_block.reset();
        self.pending_handle = handle;
        self.pending_index_entry = true;
        // Tell the filter block where the next data block
        // starts.
        if let Some(fb) = self.filter_block.as_mut() {
            fb.start_block(self.offset);
        }
        Ok(())
    }

    fn write_block(&mut self, block_data: &[u8]) -> Result<crate::block::BlockHandle> {
        // Phase E: try compressor first; fall back to uncompressed
        // if it returns None (the 12.5%-threshold logic).
        let (kind, body): (u8, std::borrow::Cow<[u8]>) = match self.compressor.as_ref() {
            Some(c) => match c.compress(block_data) {
                Some(bytes) => (c.kind(), std::borrow::Cow::Owned(bytes)),
                None => (
                    crate::block::BlockTrailer::KIND_NO_COMPRESSION,
                    std::borrow::Cow::Borrowed(block_data),
                ),
            },
            None => (
                crate::block::BlockTrailer::KIND_NO_COMPRESSION,
                std::borrow::Cow::Borrowed(block_data),
            ),
        };
        let body_ref: &[u8] = body.as_ref();
        let crc = crate::crc32c::value(body_ref);
        let crc = crate::crc32c::extend(crc, &[kind]);
        let offset = self.offset;
        let mut encoded = Vec::with_capacity(body_ref.len() + BLOCK_TRAILER_SIZE);
        encoded.extend_from_slice(body_ref);
        let trailer = crate::block::BlockTrailer { kind, crc };
        trailer.encode(&mut encoded);
        self.file.append(&encoded)?;
        self.offset += encoded.len() as u64;
        // BlockHandle.size is the on-disk body size - what the
        // reader will request via read_at, before the trailer.
        Ok(crate::block::BlockHandle {
            offset,
            size: body_ref.len() as u64,
        })
    }

    pub fn finish(&mut self) -> Result<()> {
        assert!(!self.closed);
        self.flush()?;

        // Write the filter block (if any) before the
        // metaindex.
        let filter_block_handle: Option<crate::block::BlockHandle> =
            match self.filter_block.as_mut() {
                Some(fb) => {
                    let bytes = fb.finish().to_vec();
                    Some(self.write_block(&bytes)?)
                }
                None => None,
            };

        // Build the metaindex block. When a filter is
        // present, add a
        // `"filter.<name>"` -> filter_block_handle entry.
        let mut metaindex = BlockBuilder::new(self.comparator.clone(), 1);
        if let (Some(handle), Some(name)) = (filter_block_handle, self.filter_policy_name) {
            let key = format!("filter.{name}");
            let mut handle_bytes = Vec::new();
            handle.encode(&mut handle_bytes);
            metaindex.add(key.as_bytes(), &handle_bytes);
        }
        let metaindex_bytes = metaindex.finish().to_vec();
        let metaindex_handle = self.write_block(&metaindex_bytes)?;

        if self.pending_index_entry {
            self.comparator.find_short_successor(&mut self.last_key);
            let mut handle_bytes = Vec::new();
            self.pending_handle.encode(&mut handle_bytes);
            let last_key = self.last_key.clone();
            self.index_block.add(&last_key, &handle_bytes);
            self.pending_index_entry = false;
        }
        let index_bytes = self.index_block.finish().to_vec();
        let index_handle = self.write_block(&index_bytes)?;

        let footer = crate::table::TableFooter {
            metaindex: metaindex_handle,
            index: index_handle,
        };
        let mut footer_bytes = Vec::new();
        footer.encode(&mut footer_bytes);
        self.file.append(&footer_bytes)?;
        self.offset += footer_bytes.len() as u64;
        self.file.flush()?;
        self.closed = true;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        assert!(self.closed, "TableFileBuilder: sync before finish");
        self.file.sync()
    }

    pub fn close(&mut self) -> Result<()> {
        self.file.close()
    }
}

/// Read a block from the table file at `handle`. If `verify`
/// is true, validate the trailing CRC. If false, skip the CRC
/// computation (saves CPU on every block read at the cost of
/// accepting silent corruption).
///
/// Phase E: when the trailer kind byte is non-zero, the caller-
/// supplied `compressor` is consulted to decompress the body.
/// If no compressor is registered (or its `kind()` doesn't
/// match), corruption is returned - unknown algorithms are
/// refused.
fn read_block_from_file<F: RandomAccessFile>(
    file: &F,
    handle: &crate::block::BlockHandle,
    verify: bool,
    compressor: Option<&Arc<dyn Compressor>>,
) -> Result<Vec<u8>> {
    let n = handle.size as usize;
    if n as u64 != handle.size {
        return Err(Status::corruption("Table: block size overflow"));
    }
    let total = n
        .checked_add(BLOCK_TRAILER_SIZE)
        .ok_or_else(|| Status::corruption("Table: block read size overflow"))?;
    let bytes = file.read_at(handle.offset, total)?;
    let block_data = &bytes[..n];
    let mut trailer_input: &[u8] = &bytes[n..total];
    let trailer = crate::block::BlockTrailer::decode_from(&mut trailer_input)?;
    if verify {
        let expected = crate::crc32c::value(block_data);
        let expected = crate::crc32c::extend(expected, &[trailer.kind]);
        if expected != trailer.crc {
            return Err(Status::corruption("Table: block checksum mismatch"));
        }
    }
    if trailer.kind == crate::block::BlockTrailer::KIND_NO_COMPRESSION {
        return Ok(block_data.to_vec());
    }
    match compressor {
        Some(c) if c.kind() == trailer.kind => c.decompress(block_data),
        _ => Err(Status::corruption("Table: unsupported compression")),
    }
}

/// Read the metaindex and load the filter block. Returns
/// `None` on any failure
/// (corrupt metaindex, missing entry, short filter block) -
/// the filter is purely an optimization, never required.
fn read_filter_block<C: Comparator + Clone, F: RandomAccessFile>(
    file: &F,
    metaindex_handle: &crate::block::BlockHandle,
    comparator: C,
    policy: Arc<dyn FilterPolicy + Send + Sync>,
    compressor: Option<&Arc<dyn Compressor>>,
) -> Option<TableFilter> {
    // Read the metaindex block. CRC always verified - this
    // is a one-time, small read.
    let meta_bytes = read_block_from_file(file, metaindex_handle, true, compressor).ok()?;
    let meta = Block::new(meta_bytes).ok()?;
    let mut iter = BlockIter::new(&meta, comparator);
    // Build the lookup key: "filter.<user_policy.name()>".
    let key = format!("filter.{}", policy.name());
    iter.seek(key.as_bytes());
    if !iter.valid() || iter.key() != key.as_bytes() {
        return None;
    }
    // Decode the BlockHandle pointing at the filter block.
    let mut handle_value = iter.value();
    let filter_handle = crate::block::BlockHandle::decode_from(&mut handle_value).ok()?;
    let filter_bytes = read_block_from_file(file, &filter_handle, true, compressor).ok()?;
    Some(TableFilter {
        data: Arc::new(filter_bytes),
        policy: InternalFilterPolicy::new(policy),
    })
}

#[derive(Debug, Clone)]
pub struct VecRandomAccessFile {
    bytes: Arc<Vec<u8>>,
}

impl VecRandomAccessFile {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Arc::new(bytes),
        }
    }
    pub fn bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

impl RandomAccessFile for VecRandomAccessFile {
    fn read_at(&self, offset: u64, n: usize) -> Result<Vec<u8>> {
        let start = offset as usize;
        if start as u64 != offset {
            return Err(Status::io_error("VecRandomAccessFile: offset overflow"));
        }
        let end = start
            .checked_add(n)
            .ok_or_else(|| Status::io_error("VecRandomAccessFile: length overflow"))?;
        if end > self.bytes.len() {
            return Err(Status::io_error("VecRandomAccessFile: short read"));
        }
        Ok(self.bytes[start..end].to_vec())
    }
}

/// An SSTable opened for reading, supporting point lookups.
/// Phase B: when constructed via `open_random_with_options` and
/// the options carry a `filter_policy`, the metaindex is parsed
/// for `"filter.<name>"` and the matching filter block is
/// loaded into `filter`. Point lookups consult the filter
/// before reading the data block.
/// Clone is required so TableCache can hand out iterators while
/// retaining the cached table entry.
#[derive(Clone)]
pub struct Table<C: Comparator + Clone, F: RandomAccessFile = VecRandomAccessFile> {
    file: F,
    comparator: C,
    index_block: Block,
    metaindex_offset: u64,
    /// Phase 72: per-block LRU cache shared across the
    /// owning TableCache. `None` means "no caching". Phase C:
    /// now a `dyn Cache<Arc<Block>>` so users can plug in custom
    /// implementations via `Options::block_cache`.
    block_cache:
        Option<std::sync::Arc<dyn crate::cache::Cache<std::sync::Arc<Block>> + Send + Sync>>,
    /// Unique id allocated by the block cache; combined with
    /// the block offset to form the cache key.
    cache_id: u64,
    /// Phase E: optional compressor used to decompress data
    /// blocks whose trailer kind matches `compressor.kind()`.
    /// `None` means "only uncompressed blocks accepted".
    compressor: Option<Arc<dyn Compressor>>,
    /// Phase B: filter block bytes + the user filter policy
    /// wrapped in `InternalFilterPolicy`. `None` when no filter
    /// was configured or none was found in the metaindex.
    filter: Option<TableFilter>,
}

/// Wrapper holding a filter block's raw bytes alongside the
/// `InternalFilterPolicy`-wrapped user policy, since
/// `FilterBlockReader` borrows from the bytes.
#[derive(Clone)]
struct TableFilter {
    data: Arc<Vec<u8>>,
    policy: InternalFilterPolicy<Arc<dyn FilterPolicy + Send + Sync>>,
}

impl TableFilter {
    fn key_may_match(&self, block_offset: u64, key: &[u8]) -> bool {
        match FilterBlockReader::new(self.policy.clone(), &self.data) {
            Some(reader) => reader.key_may_match(block_offset, key),
            // Corrupt or too-short filter block: be safe, don't skip.
            None => true,
        }
    }
}

impl<C: Comparator + Clone> Table<C, VecRandomAccessFile> {
    /// Open a table from `file`. The footer + index block are
    /// always read with CRC verification (cheap, one-time).
    /// Per-block verify policy for data reads is set per-call
    /// via `get` / `internal_get` / `collect_entries`.
    pub fn open(file: Vec<u8>, comparator: C) -> Result<Self> {
        let file_size = file.len() as u64;
        Self::open_random(VecRandomAccessFile::new(file), file_size, comparator)
    }

    pub fn file(&self) -> &[u8] {
        self.file.bytes()
    }
}

impl<C: Comparator + Clone, F: RandomAccessFile> Table<C, F> {
    pub fn open_random(file: F, file_size: u64, comparator: C) -> Result<Self> {
        Self::open_random_with_cache(file, file_size, comparator, None)
    }

    /// Phase 72: open with an optional block cache shared
    /// across all Tables in the same TableCache. The cache_id
    /// is allocated from the cache so per-Table block keys
    /// don't collide. Phase C: now polymorphic over any
    /// `dyn Cache<Arc<Block>>`, not just `ShardedLRUCache`.
    pub fn open_random_with_cache(
        file: F,
        file_size: u64,
        comparator: C,
        block_cache: Option<Arc<dyn Cache<Arc<Block>> + Send + Sync>>,
    ) -> Result<Self> {
        Self::open_random_with_options(file, file_size, comparator, block_cache, None, None)
    }

    /// Phase B: open a table, optionally consulting a filter
    /// policy. When `filter_policy` is `Some` and the SST's
    /// metaindex contains a matching `"filter.<name>"`
    /// entry, the filter block is loaded eagerly so point
    /// lookups can short-circuit. Phase E adds optional
    /// `compressor` for decompressing blocks whose trailer
    /// kind is non-zero.
    pub fn open_random_with_options(
        file: F,
        file_size: u64,
        comparator: C,
        block_cache: Option<Arc<dyn Cache<Arc<Block>> + Send + Sync>>,
        filter_policy: Option<Arc<dyn FilterPolicy + Send + Sync>>,
        compressor: Option<Arc<dyn Compressor>>,
    ) -> Result<Self> {
        if file_size < crate::table::TableFooter::ENCODED_LENGTH as u64 {
            return Err(Status::corruption("Table: file too small for footer"));
        }
        let footer_start = file_size - crate::table::TableFooter::ENCODED_LENGTH as u64;
        let footer_bytes = file.read_at(footer_start, crate::table::TableFooter::ENCODED_LENGTH)?;
        let mut footer_input: &[u8] = &footer_bytes;
        let footer = crate::table::TableFooter::decode_from(&mut footer_input)?;

        // Index block: always verify (small, one-time cost).
        let index_bytes = read_block_from_file(&file, &footer.index, true, compressor.as_ref())?;
        let index_block = Block::new(index_bytes)?;

        // Read the metaindex. Errors during metaindex /
        // filter loading are swallowed - meta info is not
        // needed for operation, so we just lose the
        // optimization.
        let filter = match filter_policy {
            Some(policy) => read_filter_block(
                &file,
                &footer.metaindex,
                comparator.clone(),
                policy,
                compressor.as_ref(),
            ),
            None => None,
        };

        let cache_id = block_cache.as_ref().map(|c| c.new_id()).unwrap_or(0);
        Ok(Self {
            file,
            comparator,
            index_block,
            metaindex_offset: footer.metaindex.offset,
            block_cache,
            cache_id,
            filter,
            compressor,
        })
    }

    /// Read a data block, consulting the block cache if present.
    /// The block-cache lookup is keyed by `(cache_id, offset)`.
    /// Cache hits skip the file read
    /// entirely; misses read and (when `fill_cache`) insert.
    fn read_block_cached(
        &self,
        handle: &crate::block::BlockHandle,
        verify: bool,
    ) -> Result<Arc<Block>> {
        self.read_block_cached_full(handle, verify, true)
    }

    /// Phase F: explicit `fill_cache` knob. When `false`, a
    /// cache miss reads from disk but does NOT insert into the
    /// cache - useful for one-shot scans that would otherwise
    /// evict hotter blocks.
    ///
    /// The cache stores the parsed `Block` behind an `Arc`, so a
    /// cache hit is a refcount bump - no block copy, no re-parse.
    pub(crate) fn read_block_cached_full(
        &self,
        handle: &crate::block::BlockHandle,
        verify: bool,
        fill_cache: bool,
    ) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            let mut key = self.cache_id.to_le_bytes().to_vec();
            key.extend_from_slice(&handle.offset.to_le_bytes());
            if let Some(h) = cache.lookup(&key) {
                return Ok(h.value().clone());
            }
            let bytes = read_block_from_file(&self.file, handle, verify, self.compressor.as_ref())?;
            let block = Arc::new(Block::new(bytes)?);
            if fill_cache {
                let charge = block.size();
                cache.insert(&key, block.clone(), charge);
            }
            return Ok(block);
        }
        let bytes = read_block_from_file(&self.file, handle, verify, self.compressor.as_ref())?;
        Ok(Arc::new(Block::new(bytes)?))
    }

    /// Lazy table iterator. Reads only the index eagerly; data
    /// blocks are loaded one at a time as the iterator moves.
    pub fn new_iterator(self) -> Result<TableIterator<C>>
    where
        C: 'static,
        F: 'static,
    {
        self.new_iterator_verify(true)
    }

    /// Creates an iterator over the table with explicit per-block CRC policy.
    pub fn new_iterator_verify(self, verify: bool) -> Result<TableIterator<C>>
    where
        C: 'static,
        F: 'static,
    {
        TableIterator::new(self, verify)
    }

    /// Estimates the file offset of `key`. Uses the index
    /// block to find the data block that would contain `key`
    /// and returns that block's file offset. If `key` is past
    /// the last index entry, returns the metaindex block offset
    /// near the end of the SST.
    pub fn approximate_offset_of(&self, key: &[u8]) -> u64 {
        let mut index_iter = BlockIter::new(&self.index_block, self.comparator.clone());
        index_iter.seek(key);
        if index_iter.valid() {
            let mut input = index_iter.value();
            match crate::block::BlockHandle::decode_from(&mut input) {
                Ok(handle) => handle.offset,
                Err(_) => self.metaindex_offset,
            }
        } else {
            self.metaindex_offset
        }
    }

    /// Read every entry in the table into memory in sorted
    /// (InternalKeyComparator) order. Used by compaction; the
    /// allocation is O(table size) so prefer streaming for
    /// other use cases.
    ///
    /// Convenience wrapper that always verifies CRCs.
    pub fn collect_entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.collect_entries_verify(true)
    }

    /// Like `collect_entries`, with explicit per-block CRC
    /// verification policy. `verify=false` skips CRC checks for
    /// performance; `verify=true` matches the default.
    pub fn collect_entries_verify(&self, verify: bool) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut entries = Vec::new();
        let mut index_iter = BlockIter::new(&self.index_block, self.comparator.clone());
        index_iter.seek_to_first();
        while index_iter.valid() {
            let mut handle_bytes = index_iter.value();
            let handle = crate::block::BlockHandle::decode_from(&mut handle_bytes)?;
            let block = self.read_block_cached(&handle, verify)?;
            let mut block_iter = BlockIter::new(&block, self.comparator.clone());
            block_iter.seek_to_first();
            while block_iter.valid() {
                entries.push((block_iter.key().to_vec(), block_iter.value().to_vec()));
                block_iter.next();
            }
            block_iter.status()?;
            index_iter.next();
        }
        index_iter.status()?;
        Ok(entries)
    }

    /// Point lookup. Returns `Ok(None)` if the key is not
    /// present in the table; `Ok(Some(value))` if it is.
    /// Errors propagate corruption from the underlying blocks.
    ///
    /// Convenience wrapper that always verifies CRCs.
    pub fn get(&self, target: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_verify(target, true)
    }

    /// Like `get`, with explicit per-block CRC verification.
    pub fn get_verify(&self, target: &[u8], verify: bool) -> Result<Option<Vec<u8>>> {
        self.get_full(target, verify, true)
    }

    /// Phase F: full read path with explicit `verify_checksums`
    /// + `fill_cache` knobs.
    pub fn get_full(
        &self,
        target: &[u8],
        verify: bool,
        fill_cache: bool,
    ) -> Result<Option<Vec<u8>>> {
        let mut index_iter = BlockIter::new(&self.index_block, self.comparator.clone());
        index_iter.seek(target);
        if !index_iter.valid() {
            return Ok(None);
        }
        let mut handle_bytes = index_iter.value();
        let handle = crate::block::BlockHandle::decode_from(&mut handle_bytes)?;
        if let Some(filter) = &self.filter {
            let mut tagged = Vec::with_capacity(target.len() + 8);
            tagged.extend_from_slice(target);
            tagged.extend_from_slice(&[0u8; 8]);
            if !filter.key_may_match(handle.offset, &tagged) {
                return Ok(None);
            }
        }
        let data_block = self.read_block_cached_full(&handle, verify, fill_cache)?;
        let mut data_iter = BlockIter::new(&data_block, self.comparator.clone());
        data_iter.seek(target);
        if !data_iter.valid() {
            return Ok(None);
        }
        if self.comparator.compare(data_iter.key(), target).is_eq() {
            Ok(Some(data_iter.value().to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Internal-key-aware lookup. `target` is a full internal
    /// key (user_key + 8-byte tag). Tables built by compaction
    /// store entries in `InternalKeyComparator` order: same
    /// user_key sorted by sequence DESC. The first entry at-or-
    /// after `target` whose user_key matches is the visible one.
    ///
    /// Returns `Found(value)` for a Value record, `Deleted` for
    /// a Deletion tombstone, `NotFound` if no entry in this
    /// table covers `target`'s user_key. The `Comparator` `C`
    /// MUST be `InternalKeyComparator<_>` for the index/block
    /// seeks to land in the right place.
    ///
    /// Convenience wrapper that always verifies CRCs.
    pub fn internal_get(&self, target: &[u8]) -> Result<crate::version_set::LookupResult> {
        self.internal_get_verify(target, true)
    }

    /// Like `internal_get`, with explicit per-block CRC verification.
    pub fn internal_get_verify(
        &self,
        target: &[u8],
        verify: bool,
    ) -> Result<crate::version_set::LookupResult> {
        self.internal_get_full(target, verify, true)
    }

    /// Phase F: full read path with explicit `verify_checksums`
    /// + `fill_cache` knobs.
    pub fn internal_get_full(
        &self,
        target: &[u8],
        verify: bool,
        fill_cache: bool,
    ) -> Result<crate::version_set::LookupResult> {
        use crate::version_set::LookupResult;
        assert!(target.len() >= 8, "internal key requires an 8-byte tag");
        let target_user_key = &target[..target.len() - 8];
        let mut index_iter = BlockIter::new(&self.index_block, self.comparator.clone());
        index_iter.seek(target);
        if !index_iter.valid() {
            return Ok(LookupResult::NotFound);
        }
        let mut handle_bytes = index_iter.value();
        let handle = crate::block::BlockHandle::decode_from(&mut handle_bytes)?;
        if let Some(filter) = &self.filter {
            if !filter.key_may_match(handle.offset, target) {
                return Ok(LookupResult::NotFound);
            }
        }
        let data_block = self.read_block_cached_full(&handle, verify, fill_cache)?;
        let mut data_iter = BlockIter::new(&data_block, self.comparator.clone());
        data_iter.seek(target);
        if !data_iter.valid() {
            return Ok(LookupResult::NotFound);
        }
        let entry_key = data_iter.key();
        if entry_key.len() < 8 {
            return Err(Status::corruption("Table: malformed internal key"));
        }
        let entry_user_key = &entry_key[..entry_key.len() - 8];
        if entry_user_key != target_user_key {
            return Ok(LookupResult::NotFound);
        }
        let tag = u64::from_le_bytes(entry_key[entry_key.len() - 8..].try_into().unwrap());
        // value_type byte = tag & 0xff. 0 = Deletion, 1 = Value.
        match (tag & 0xff) as u8 {
            0 => Ok(LookupResult::Deleted),
            1 => Ok(LookupResult::Found(data_iter.value().to_vec())),
            _ => Err(Status::corruption("Table: unknown ValueType")),
        }
    }
}

type TableBlockFunction<C> = Box<dyn FnMut(&[u8]) -> Result<BlockIter<'static, C>>>;

pub struct TableIterator<C: Comparator + Clone + 'static> {
    inner: TwoLevelIterator<BlockIter<'static, C>, BlockIter<'static, C>, TableBlockFunction<C>>,
}

impl<C: Comparator + Clone + 'static> TableIterator<C> {
    fn new<F>(table: Table<C, F>, verify: bool) -> Result<Self>
    where
        F: RandomAccessFile + 'static,
    {
        let index_iter = BlockIter::from_owned(table.index_block.clone(), table.comparator.clone());
        let comparator = table.comparator.clone();
        let block_function: TableBlockFunction<C> = Box::new(move |handle_bytes| {
            let mut input = handle_bytes;
            let handle = crate::block::BlockHandle::decode_from(&mut input)?;
            let block = table.read_block_cached(&handle, verify)?;
            Ok(BlockIter::from_shared(block, comparator.clone()))
        });
        Ok(Self {
            inner: TwoLevelIterator::new(index_iter, block_function),
        })
    }
}

impl<C: Comparator + Clone + 'static> crate::db_iter::DbIterator for TableIterator<C> {
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
