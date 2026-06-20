use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use crate::status::{Result, Status};

/// Environment abstraction over the filesystem - enough for
/// log writer/reader, table builder/reader,
/// and manifest read/write.
/// Durability policy for `WritableFile::sync`.
///
/// `Full` forces a hardware-level flush (macOS `F_FULLFSYNC`) -
/// durable across power loss. `Data` issues `fdatasync` -
/// durable across an OS crash but not necessarily a power loss,
/// and on macOS is many times faster. `Data` is the default
/// posix sync behavior, using `fdatasync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Full hardware barrier (`F_FULLFSYNC` on macOS) - durable
    /// across power loss.
    Full,
    /// `fdatasync` - durable across an OS crash. The default.
    #[default]
    Data,
}

/// Writable file handle used by DB logs and table/manifest writers.
pub trait WritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn close(&mut self) -> Result<()>;
}

/// Random read handle used by SST/table readers.
pub trait RandomAccessFile {
    fn read_at(&self, offset: u64, n: usize) -> Result<Vec<u8>>;
}

/// Sequential read handle used by WAL and MANIFEST recovery.
pub trait SequentialFile {
    fn read(&mut self, n: usize) -> Result<Vec<u8>>;
    fn skip(&mut self, mut n: u64) -> Result<()> {
        const CHUNK: usize = 8192;
        while n > 0 {
            let take = (n as usize).min(CHUNK);
            let got = self.read(take)?;
            if got.is_empty() {
                break;
            }
            n = n.saturating_sub(got.len() as u64);
        }
        Ok(())
    }
}

/// RAII lock handle returned by `Env::lock_file`.
/// Dropping the handle releases the lock.
pub trait FileLock: Send + 'static {}

pub trait Env {
    type Writable: WritableFile + Send + 'static;
    type RandomAccess: RandomAccessFile + Clone + Send + Sync + 'static;
    type Sequential: SequentialFile + Send + 'static;
    type Lock: FileLock;
    fn new_writable_file(&self, path: &Path) -> Result<Self::Writable>;
    /// Open `path` for appending without truncating, creating it if
    /// absent. Subsequent writes extend the file from its current end -
    /// used by `reuse_logs` recovery to resume an existing WAL. The
    /// default preserves existing contents through the trait's own
    /// methods (read, rewrite, leave the cursor at the end), so custom
    /// envs stay correct without overriding; `StdEnv`/`MemEnv` override
    /// with a real append for efficiency.
    fn new_appendable_file(&self, path: &Path) -> Result<Self::Writable> {
        let existing = if self.file_exists(path) {
            self.read_file(path)?
        } else {
            Vec::new()
        };
        let mut file = self.new_writable_file(path)?;
        if !existing.is_empty() {
            file.append(&existing)?;
        }
        Ok(file)
    }
    /// Set the durability policy stamped onto every writable
    /// file this env subsequently creates. Default no-op:
    /// in-memory envs have nothing to sync.
    fn set_sync_mode(&mut self, _mode: SyncMode) {}
    fn new_random_access_file(&self, path: &Path) -> Result<Self::RandomAccess>;
    fn new_sequential_file(&self, path: &Path) -> Result<Self::Sequential>;
    fn lock_file(&self, path: &Path) -> Result<Self::Lock>;
    fn read_file(&self, path: &Path) -> Result<Vec<u8>>;
    fn write_file(&self, path: &Path, data: &[u8]) -> Result<()>;
    fn append_file(&self, path: &Path, data: &[u8]) -> Result<()>;
    fn file_exists(&self, path: &Path) -> bool;
    fn delete_file(&self, path: &Path) -> Result<()>;
    fn rename_file(&self, src: &Path, dst: &Path) -> Result<()>;
    fn get_file_size(&self, path: &Path) -> Result<u64>;
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;
    fn create_dir(&self, path: &Path) -> Result<()>;
    /// Force `path`'s data to stable storage - invoked when
    /// `WriteOptions::sync`
    /// is set. Default is a no-op (most write_file impls already
    /// flush to disk on close).
    fn sync_file(&self, _path: &Path) -> Result<()> {
        Ok(())
    }
    /// Recursively delete `path` and everything under it.
    /// Behaves like `std::fs::remove_dir_all`. Used by `destroy_db`.
    /// Default impl walks `list_dir` + `delete_file` - works
    /// for envs that flatten dirs into key prefixes.
    fn delete_dir(&self, path: &Path) -> Result<()> {
        let entries = self.list_dir(path)?;
        for entry in entries {
            if entry == *path {
                continue;
            }
            // Best-effort: try as dir first, then file.
            if self.delete_dir(&entry).is_err() {
                let _ = self.delete_file(&entry);
            }
        }
        Ok(())
    }
}

fn io_err(prefix: &str, e: std::io::Error) -> Status {
    Status::io_error(format!("{prefix}: {e}"))
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| io_err(&format!("mkdir {}", parent.display()), e))?;
        }
    }
    Ok(())
}

/// std::fs-backed Env. Uses real disk I/O.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdEnv {
    /// Durability policy applied to every writable file this env
    /// creates. `DBImpl::open` stamps it from `Options::sync_mode`;
    /// `StdEnv::default()` is `SyncMode::Data`.
    sync_mode: SyncMode,
}

#[cfg(unix)]
extern "C" {
    fn fdatasync(fd: i32) -> i32;
}

#[derive(Debug)]
pub struct StdWritableFile {
    file: std::io::BufWriter<fs::File>,
    sync_mode: SyncMode,
    closed: bool,
}

impl StdWritableFile {
    fn ensure_open(&self, op: &str) -> Result<()> {
        if self.closed {
            Err(Status::io_error(format!("{op}: writable file is closed")))
        } else {
            Ok(())
        }
    }
}

impl WritableFile for StdWritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.ensure_open("append")?;
        self.file
            .write_all(data)
            .map_err(|e| io_err("writable append", e))
    }
    fn flush(&mut self) -> Result<()> {
        self.ensure_open("flush")?;
        self.file.flush().map_err(|e| io_err("writable flush", e))
    }
    fn sync(&mut self) -> Result<()> {
        self.ensure_open("sync")?;
        self.file
            .flush()
            .map_err(|e| io_err("writable sync flush", e))?;
        match self.sync_mode {
            // Full hardware barrier - power-loss durable. On
            // macOS `sync_all` issues `fcntl(F_FULLFSYNC)`.
            SyncMode::Full => self
                .file
                .get_ref()
                .sync_all()
                .map_err(|e| io_err("writable sync", e)),
            SyncMode::Data => {
                #[cfg(unix)]
                {
                    use std::os::fd::AsRawFd;
                    let fd = self.file.get_ref().as_raw_fd();
                    // SAFETY: `fd` is a live, open descriptor owned
                    // by `self.file` for the duration of this call;
                    // `fdatasync` only flushes that descriptor.
                    if unsafe { fdatasync(fd) } != 0 {
                        return Err(io_err(
                            "writable fdatasync",
                            std::io::Error::last_os_error(),
                        ));
                    }
                    Ok(())
                }
                #[cfg(not(unix))]
                {
                    self.file
                        .get_ref()
                        .sync_all()
                        .map_err(|e| io_err("writable sync", e))
                }
            }
        }
    }
    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.file.flush().map_err(|e| io_err("writable close", e))?;
        self.closed = true;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StdRandomAccessFile {
    path: PathBuf,
    file: Arc<fs::File>,
}

impl RandomAccessFile for StdRandomAccessFile {
    fn read_at(&self, offset: u64, n: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; n];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(&mut buf, offset).map_err(|e| {
                io_err(
                    &format!("random read {} @{}+{}", self.path.display(), offset, n),
                    e,
                )
            })?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let mut filled = 0usize;
            while filled < n {
                let got = self
                    .file
                    .seek_read(&mut buf[filled..], offset + filled as u64)
                    .map_err(|e| {
                        io_err(
                            &format!("random read {} @{}+{}", self.path.display(), offset, n),
                            e,
                        )
                    })?;
                if got == 0 {
                    return Err(Status::io_error(format!(
                        "random read {} @{}+{}: unexpected end of file",
                        self.path.display(),
                        offset,
                        n
                    )));
                }
                filled += got;
            }
        }
        Ok(buf)
    }
}

#[derive(Debug)]
pub struct StdSequentialFile {
    path: PathBuf,
    file: fs::File,
}

impl SequentialFile for StdSequentialFile {
    fn read(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(n);
        Read::by_ref(&mut self.file)
            .take(n as u64)
            .read_to_end(&mut buf)
            .map_err(|e| io_err(&format!("sequential read {}", self.path.display()), e))?;
        Ok(buf)
    }
    fn skip(&mut self, n: u64) -> Result<()> {
        let offset = i64::try_from(n).map_err(|_| {
            Status::io_error(format!("sequential skip overflow: {}", self.path.display()))
        })?;
        self.file
            .seek(SeekFrom::Current(offset))
            .map_err(|e| io_err(&format!("sequential skip {}", self.path.display()), e))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct StdFileLock {
    path: PathBuf,
    _file: fs::File,
}

impl FileLock for StdFileLock {}

static STD_LOCKS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();

fn std_lock_table() -> &'static Mutex<BTreeSet<PathBuf>> {
    STD_LOCKS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(windows)]
fn open_std_lock_file(path: &Path) -> std::io::Result<fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(path)
}

#[cfg(unix)]
fn open_std_lock_file(path: &Path) -> std::io::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    lock_std_file(&file)?;
    Ok(file)
}

#[cfg(unix)]
fn lock_std_file(file: &fs::File) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;

    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }

    let fd = file.as_raw_fd();
    if unsafe { flock(fd, LOCK_EX | LOCK_NB) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(all(not(windows), not(unix)))]
fn open_std_lock_file(path: &Path) -> std::io::Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

impl Drop for StdFileLock {
    fn drop(&mut self) {
        if let Ok(mut locks) = std_lock_table().lock() {
            locks.remove(&self.path);
        }
    }
}

impl Env for StdEnv {
    type Writable = StdWritableFile;
    type RandomAccess = StdRandomAccessFile;
    type Sequential = StdSequentialFile;
    type Lock = StdFileLock;
    fn set_sync_mode(&mut self, mode: SyncMode) {
        self.sync_mode = mode;
    }
    fn new_writable_file(&self, path: &Path) -> Result<Self::Writable> {
        ensure_parent_dir(path)?;
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| io_err(&format!("open writable {}", path.display()), e))?;
        Ok(StdWritableFile {
            file: std::io::BufWriter::with_capacity(64 * 1024, file),
            sync_mode: self.sync_mode,
            closed: false,
        })
    }
    fn new_appendable_file(&self, path: &Path) -> Result<Self::Writable> {
        ensure_parent_dir(path)?;
        // `append(true)` sets O_APPEND: every write lands at EOF, no
        // truncation - so an existing WAL is resumed, not clobbered.
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| io_err(&format!("open appendable {}", path.display()), e))?;
        Ok(StdWritableFile {
            file: std::io::BufWriter::with_capacity(64 * 1024, file),
            sync_mode: self.sync_mode,
            closed: false,
        })
    }
    fn new_random_access_file(&self, path: &Path) -> Result<Self::RandomAccess> {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| io_err(&format!("open random {}", path.display()), e))?;
        Ok(StdRandomAccessFile {
            path: path.to_path_buf(),
            file: Arc::new(file),
        })
    }
    fn new_sequential_file(&self, path: &Path) -> Result<Self::Sequential> {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| io_err(&format!("open sequential {}", path.display()), e))?;
        Ok(StdSequentialFile {
            path: path.to_path_buf(),
            file,
        })
    }
    fn lock_file(&self, path: &Path) -> Result<Self::Lock> {
        ensure_parent_dir(path)?;
        let path_buf = path.to_path_buf();
        {
            let mut locks = std_lock_table().lock().unwrap();
            if !locks.insert(path_buf.clone()) {
                return Err(Status::io_error(format!(
                    "lock {}: already held by process",
                    path.display()
                )));
            }
        }
        match open_std_lock_file(path) {
            Ok(file) => Ok(StdFileLock {
                path: path_buf,
                _file: file,
            }),
            Err(error) => {
                std_lock_table().lock().unwrap().remove(&path_buf);
                Err(io_err(&format!("lock {}", path.display()), error))
            }
        }
    }
    fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        fs::read(path).map_err(|e| io_err(&format!("read {}", path.display()), e))
    }
    fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        ensure_parent_dir(path)?;
        fs::write(path, data).map_err(|e| io_err(&format!("write {}", path.display()), e))
    }
    fn append_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        ensure_parent_dir(path)?;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| io_err(&format!("open {}", path.display()), e))?;
        f.write_all(data)
            .map_err(|e| io_err(&format!("append {}", path.display()), e))
    }
    fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn delete_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path).map_err(|e| io_err(&format!("delete {}", path.display()), e))
    }
    fn rename_file(&self, src: &Path, dst: &Path) -> Result<()> {
        ensure_parent_dir(dst)?;
        fs::rename(src, dst)
            .map_err(|e| io_err(&format!("rename {} -> {}", src.display(), dst.display()), e))
    }
    fn get_file_size(&self, path: &Path) -> Result<u64> {
        let meta =
            fs::metadata(path).map_err(|e| io_err(&format!("stat {}", path.display()), e))?;
        Ok(meta.len())
    }
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let entries =
            fs::read_dir(path).map_err(|e| io_err(&format!("read_dir {}", path.display()), e))?;
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| io_err(&format!("iterate {}", path.display()), e))?;
            out.push(entry.path());
        }
        Ok(out)
    }
    fn create_dir(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path).map_err(|e| io_err(&format!("mkdir {}", path.display()), e))
    }
    fn sync_file(&self, path: &Path) -> Result<()> {
        // Open the file and call sync_all() to fsync its
        // data to stable storage.
        let f = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| io_err(&format!("sync open {}", path.display()), e))?;
        f.sync_all()
            .map_err(|e| io_err(&format!("sync {}", path.display()), e))
    }
    fn delete_dir(&self, path: &Path) -> Result<()> {
        // No-op if the dir doesn't exist (idempotent on
        // missing target).
        if !path.exists() {
            return Ok(());
        }
        fs::remove_dir_all(path).map_err(|e| io_err(&format!("rmdir -r {}", path.display()), e))
    }
}

/// In-memory Env. Used for tests that don't want real disk I/O.
/// Cloning a `MemEnv` shares the underlying file map via Arc;
/// each clone sees writes from every other clone.
#[derive(Debug, Default, Clone)]
pub struct MemEnv {
    files: SharedMemFiles,
    locks: Arc<Mutex<BTreeSet<PathBuf>>>,
}

type MemFile = Arc<Mutex<Vec<u8>>>;
type SharedMemFiles = Arc<Mutex<BTreeMap<PathBuf, MemFile>>>;

impl MemEnv {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(BTreeMap::new())),
            locks: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    /// Returns a clone that shares the same underlying file map
    /// (alias for `Clone::clone`; explicit name for clarity in
    /// recovery tests that need two `Env` instances over the same
    /// in-memory state).
    pub fn clone_handle(&self) -> Self {
        self.clone()
    }
}

#[derive(Debug)]
pub struct MemWritableFile {
    // Direct handle to this file's byte buffer. Holding the
    // buffer Arc means `append` never re-keys the file map by
    // path: no PathBuf clone, no BTreeMap walk, no global lock.
    buf: Arc<Mutex<Vec<u8>>>,
    closed: bool,
}

impl MemWritableFile {
    fn ensure_open(&self, op: &str) -> Result<()> {
        if self.closed {
            Err(Status::io_error(format!("{op}: writable file is closed")))
        } else {
            Ok(())
        }
    }
}

impl WritableFile for MemWritableFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.ensure_open("append")?;
        self.buf.lock().unwrap().extend_from_slice(data);
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        self.ensure_open("flush")
    }
    fn sync(&mut self) -> Result<()> {
        self.ensure_open("sync")
    }
    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MemRandomAccessFile {
    path: PathBuf,
    data: Arc<Vec<u8>>,
}

impl RandomAccessFile for MemRandomAccessFile {
    fn read_at(&self, offset: u64, n: usize) -> Result<Vec<u8>> {
        let start = offset as usize;
        if start as u64 != offset {
            return Err(Status::io_error(format!(
                "random read offset overflow: {}",
                self.path.display()
            )));
        }
        let end = start.checked_add(n).ok_or_else(|| {
            Status::io_error(format!(
                "random read length overflow: {}",
                self.path.display()
            ))
        })?;
        if end > self.data.len() {
            return Err(Status::io_error(format!(
                "short random read {} @{}+{}",
                self.path.display(),
                offset,
                n
            )));
        }
        Ok(self.data[start..end].to_vec())
    }
}

#[derive(Debug)]
pub struct MemSequentialFile {
    path: PathBuf,
    data: Arc<Vec<u8>>,
    offset: usize,
}

impl SequentialFile for MemSequentialFile {
    fn read(&mut self, n: usize) -> Result<Vec<u8>> {
        let end = self.offset.saturating_add(n).min(self.data.len());
        let out = self.data[self.offset..end].to_vec();
        self.offset = end;
        Ok(out)
    }
    fn skip(&mut self, n: u64) -> Result<()> {
        let n = usize::try_from(n).map_err(|_| {
            Status::io_error(format!("sequential skip overflow: {}", self.path.display()))
        })?;
        self.offset = self.offset.saturating_add(n).min(self.data.len());
        Ok(())
    }
}

#[derive(Debug)]
pub struct MemFileLock {
    locks: Arc<Mutex<BTreeSet<PathBuf>>>,
    path: PathBuf,
}

impl FileLock for MemFileLock {}

impl Drop for MemFileLock {
    fn drop(&mut self) {
        if let Ok(mut locks) = self.locks.lock() {
            locks.remove(&self.path);
        }
    }
}

impl Env for MemEnv {
    type Writable = MemWritableFile;
    type RandomAccess = MemRandomAccessFile;
    type Sequential = MemSequentialFile;
    type Lock = MemFileLock;
    fn new_writable_file(&self, path: &Path) -> Result<Self::Writable> {
        // Reuse the existing buffer Arc when the path is already
        // known (truncating it) so any handle opened earlier stays
        // connected to the same entry, matching the prior map-keyed
        // behavior. Otherwise insert a fresh empty buffer.
        let mut files = self.files.lock().unwrap();
        let buf = files
            .entry(path.to_path_buf())
            .and_modify(|b| b.lock().unwrap().clear())
            .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
            .clone();
        Ok(MemWritableFile { buf, closed: false })
    }
    fn new_appendable_file(&self, path: &Path) -> Result<Self::Writable> {
        // Reuse the existing buffer WITHOUT clearing so appends extend
        // the current contents (resume an existing WAL); create empty
        // if the path is new.
        let mut files = self.files.lock().unwrap();
        let buf = files
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
            .clone();
        Ok(MemWritableFile { buf, closed: false })
    }
    fn new_random_access_file(&self, path: &Path) -> Result<Self::RandomAccess> {
        let files = self.files.lock().unwrap();
        let data = files
            .get(path)
            .map(|b| b.lock().unwrap().clone())
            .ok_or_else(|| Status::io_error(format!("not found: {}", path.display())))?;
        Ok(MemRandomAccessFile {
            path: path.to_path_buf(),
            data: Arc::new(data),
        })
    }
    fn new_sequential_file(&self, path: &Path) -> Result<Self::Sequential> {
        let files = self.files.lock().unwrap();
        let data = files
            .get(path)
            .map(|b| b.lock().unwrap().clone())
            .ok_or_else(|| Status::io_error(format!("not found: {}", path.display())))?;
        Ok(MemSequentialFile {
            path: path.to_path_buf(),
            data: Arc::new(data),
            offset: 0,
        })
    }
    fn lock_file(&self, path: &Path) -> Result<Self::Lock> {
        let path_buf = path.to_path_buf();
        {
            let mut locks = self.locks.lock().unwrap();
            if !locks.insert(path_buf.clone()) {
                return Err(Status::io_error(format!(
                    "lock {}: already held by process",
                    path.display()
                )));
            }
        }
        self.files
            .lock()
            .unwrap()
            .entry(path_buf.clone())
            .or_default();
        Ok(MemFileLock {
            locks: self.locks.clone(),
            path: path_buf,
        })
    }
    fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        let files = self.files.lock().unwrap();
        files
            .get(path)
            .map(|b| b.lock().unwrap().clone())
            .ok_or_else(|| Status::io_error(format!("not found: {}", path.display())))
    }
    fn write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), Arc::new(Mutex::new(data.to_vec())));
        Ok(())
    }
    fn append_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        files
            .entry(path.to_path_buf())
            .or_default()
            .lock()
            .unwrap()
            .extend_from_slice(data);
        Ok(())
    }
    fn file_exists(&self, path: &Path) -> bool {
        self.files.lock().unwrap().contains_key(path)
    }
    fn delete_file(&self, path: &Path) -> Result<()> {
        if self.files.lock().unwrap().remove(path).is_none() {
            return Err(Status::io_error(format!("not found: {}", path.display())));
        }
        Ok(())
    }
    fn rename_file(&self, src: &Path, dst: &Path) -> Result<()> {
        let mut files = self.files.lock().unwrap();
        let data = files
            .remove(src)
            .ok_or_else(|| Status::io_error(format!("rename: src missing: {}", src.display())))?;
        files.insert(dst.to_path_buf(), data);
        Ok(())
    }
    fn get_file_size(&self, path: &Path) -> Result<u64> {
        self.files
            .lock()
            .unwrap()
            .get(path)
            .map(|b| b.lock().unwrap().len() as u64)
            .ok_or_else(|| Status::io_error(format!("not found: {}", path.display())))
    }
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        // Like `std::fs::read_dir`: return only direct
        // children of `path`. This matters for any caller
        // that uses subdirectories (e.g., repair archives
        // into `dbname/lost/`) and would otherwise see
        // archived files leak back into the DB listing.
        let files = self.files.lock().unwrap();
        let prefix = path.to_path_buf();
        let mut out = BTreeSet::new();
        for p in files.keys() {
            if !p.starts_with(&prefix) || p == &prefix {
                continue;
            }
            let rel = match p.strip_prefix(&prefix) {
                Ok(r) => r,
                Err(_) => continue,
            };
            // Take only the first component - that is the
            // direct child of `prefix`. Files deeper in the
            // tree contribute their containing dir as a path.
            if let Some(first) = rel.components().next() {
                out.insert(prefix.join(first.as_os_str()));
            }
        }
        Ok(out.into_iter().collect())
    }
    fn create_dir(&self, _path: &Path) -> Result<()> {
        // MemEnv has no directory hierarchy; no-op.
        Ok(())
    }
    fn delete_dir(&self, path: &Path) -> Result<()> {
        // Remove every entry whose path starts with this prefix.
        // Like `fs::remove_dir_all` semantics: idempotent on
        // a missing dir (no error if no files match).
        let mut files = self.files.lock().unwrap();
        files.retain(|p, _| !p.starts_with(path));
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("novakv-{name}-{}-{nanos}.lock", std::process::id()))
    }

    #[test]
    fn std_lock_file_rejects_a_second_unix_lock_holder() {
        let path = unique_temp_path("std-lock");
        let first = open_std_lock_file(&path).expect("first lock should succeed");

        let second = open_std_lock_file(&path);
        assert!(
            second.is_err(),
            "second exclusive nonblocking flock should fail while first handle is held"
        );

        drop(first);
        let third = open_std_lock_file(&path).expect("lock should be released when handle drops");
        drop(third);
        let _ = fs::remove_file(path);
    }
}
