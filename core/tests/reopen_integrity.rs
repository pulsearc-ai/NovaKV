//! Reopen / recovery integrity tests.
//!
//! These guard the `reuse_logs` reopen path (resume an existing WAL on
//! open instead of flushing it to an SST + rewriting the manifest). The
//! invariant under test is simple and absolute: **no acknowledged write
//! may be lost or altered across a close + reopen**, on every path —
//! clean (empty WAL), dirty (WAL reused), repeated reopen (WAL appended
//! in place), and the large-WAL fallback (flush instead of reuse).

use novakv::prelude::*;
use std::path::PathBuf;

fn tmp_dir(tag: &str) -> PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "novakv_reopen_{tag}_{stamp}_{}",
        std::process::id()
    ))
}

fn key_of(i: u64) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}
fn val_of(i: u64) -> Vec<u8> {
    format!("value-number-{i}").into_bytes()
}

fn open(dir: &str, opts: Options) -> DBImpl<BytewiseComparator, StdEnv> {
    DBImpl::open(dir, StdEnv::default(), BytewiseComparator, opts).expect("open")
}

/// Assert every key in `0..n` reads back its expected value.
fn assert_all_present(db: &DBImpl<BytewiseComparator, StdEnv>, n: u64) {
    for i in 0..n {
        assert_eq!(
            db.get(key_of(i)).expect("get"),
            Some(val_of(i)),
            "key {i} missing or wrong after reopen"
        );
    }
}

struct TempDb(PathBuf);
impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = destroy_db(self.0.to_str().unwrap(), StdEnv::default());
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn clean_reopen_preserves_data() {
    let dir = tmp_dir("clean");
    let _guard = TempDb(dir.clone());
    let path = dir.to_str().unwrap();
    let n = 500;

    let db = open(path, Options::default());
    for i in 0..n {
        db.put(key_of(i), val_of(i)).unwrap();
    }
    db.compact_range(None, None).unwrap(); // flush to SSTs -> empty WAL
    drop(db);

    // Reopen several times: the live manifest must survive each cycle
    // (the reuse path skips log_and_apply, so a mis-set manifest number
    // would delete it and the next open would fail).
    for _ in 0..5 {
        let db = open(path, Options::default());
        assert_all_present(&db, n);
        drop(db);
    }
}

#[test]
fn dirty_reopen_replays_wal_via_reuse() {
    let dir = tmp_dir("dirty");
    let _guard = TempDb(dir.clone());
    let path = dir.to_str().unwrap();

    let db = open(path, Options::default());
    for i in 0..300 {
        db.put(key_of(i), val_of(i)).unwrap();
    }
    db.compact_range(None, None).unwrap(); // 0..300 now in SSTs
                                           // These stay in the WAL (no compaction) -> reopen must replay them.
    for i in 300..500 {
        db.put(key_of(i), val_of(i)).unwrap();
    }
    drop(db);

    let db = open(path, Options::default());
    assert_all_present(&db, 500); // both the SST data and the replayed WAL
    drop(db);
}

#[test]
fn repeated_reopen_appends_same_wal() {
    // Each cycle writes a fresh slice and reopens without compacting, so
    // the reused WAL is appended in place and replayed in full every
    // time. Exercises LogWriter::resume across many offsets/blocks.
    let dir = tmp_dir("repeat");
    let _guard = TempDb(dir.clone());
    let path = dir.to_str().unwrap();

    let mut written = 0u64;
    for cycle in 0..6 {
        let db = open(path, Options::default());
        assert_all_present(&db, written); // prior cycles intact
        for i in written..written + 250 {
            db.put(key_of(i), val_of(i)).unwrap();
        }
        written += 250;
        let _ = cycle;
        drop(db);
    }

    let db = open(path, Options::default());
    assert_all_present(&db, written);
    drop(db);
}

#[test]
fn large_wal_forces_flush_fallback() {
    // A small write buffer makes the replayed WAL exceed write_buffer_size
    // at reopen, taking the fallback (flush to SST) path instead of reuse.
    // Data must be intact either way.
    let dir = tmp_dir("fallback");
    let _guard = TempDb(dir.clone());
    let path = dir.to_str().unwrap();
    let opts = Options {
        write_buffer_size: 16 * 1024,
        ..Options::default()
    };

    let db = open(path, opts.clone());
    for i in 0..2000 {
        db.put(key_of(i), val_of(i)).unwrap();
    }
    drop(db);

    let db = open(path, opts);
    assert_all_present(&db, 2000);
    drop(db);
}

#[test]
fn tombstone_survives_reopen() {
    let dir = tmp_dir("delete");
    let _guard = TempDb(dir.clone());
    let path = dir.to_str().unwrap();

    let db = open(path, Options::default());
    db.put(key_of(1), val_of(1)).unwrap();
    db.put(key_of(2), val_of(2)).unwrap();
    db.delete(key_of(1)).unwrap();
    drop(db);

    let db = open(path, Options::default());
    assert_eq!(db.get(key_of(1)).unwrap(), None, "deleted key resurfaced");
    assert_eq!(db.get(key_of(2)).unwrap(), Some(val_of(2)));
    drop(db);
}

#[test]
fn multi_entry_batch_replays() {
    // A single WriteBatch with several entries becomes one WAL record
    // with count > 1; replay must visit every entry.
    let dir = tmp_dir("multibatch");
    let _guard = TempDb(dir.clone());
    let path = dir.to_str().unwrap();

    let db = open(path, Options::default());
    let mut batch = WriteBatch::new();
    for i in 0..50 {
        batch.put(key_of(i), val_of(i));
    }
    batch.delete(key_of(10));
    db.write(&batch).unwrap(); // one record, count == 51
    drop(db); // stays in WAL

    let db = open(path, Options::default());
    for i in 0..50 {
        if i == 10 {
            assert_eq!(db.get(key_of(i)).unwrap(), None);
        } else {
            assert_eq!(db.get(key_of(i)).unwrap(), Some(val_of(i)));
        }
    }
    drop(db);
}

#[test]
fn large_value_fragmented_wal_replays() {
    // A value larger than the 32 KiB log block forces the record to be
    // split into FIRST/MIDDLE/LAST fragments; replay must reassemble it.
    let dir = tmp_dir("fragment");
    let _guard = TempDb(dir.clone());
    let path = dir.to_str().unwrap();
    let big: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();

    let db = open(path, Options::default());
    db.put(b"big", &big).unwrap();
    db.put(b"small", b"ok").unwrap();
    drop(db); // both in WAL

    let db = open(path, Options::default());
    assert_eq!(db.get(b"big").unwrap(), Some(big));
    assert_eq!(db.get(b"small").unwrap(), Some(b"ok".to_vec()));
    drop(db);
}

#[test]
fn overwrite_latest_wins_across_reopen() {
    let dir = tmp_dir("overwrite");
    let _guard = TempDb(dir.clone());
    let path = dir.to_str().unwrap();

    let db = open(path, Options::default());
    db.put(key_of(7), b"first").unwrap();
    db.compact_range(None, None).unwrap(); // first -> SST
    db.put(key_of(7), b"second").unwrap(); // newer, in WAL
    drop(db);

    let db = open(path, Options::default());
    assert_eq!(db.get(key_of(7)).unwrap(), Some(b"second".to_vec()));
    drop(db);
}
