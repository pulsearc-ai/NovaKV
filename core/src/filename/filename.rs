use std::path::Path;

use crate::env::{Env, WritableFile};
use crate::status::Result;

/// The kind of file in a database directory. Wire-format: these
/// suffixes/names are what `parse_file_name` decodes from
/// directory listings on database open and recovery.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FileType {
    LogFile,        // <number>.log
    DbLockFile,     // LOCK
    TableFile,      // <number>.ldb (or legacy <number>.sst)
    DescriptorFile, // MANIFEST-<number>
    CurrentFile,    // CURRENT
    TempFile,       // <number>.dbtmp
    InfoLogFile,    // LOG (rotated to LOG.old)
}

fn make_file_name(dbname: &str, number: u64, suffix: &str) -> String {
    format!("{dbname}/{number:06}.{suffix}")
}

pub fn log_file_name(dbname: &str, number: u64) -> String {
    make_file_name(dbname, number, "log")
}

pub fn table_file_name(dbname: &str, number: u64) -> String {
    make_file_name(dbname, number, "ldb")
}

pub fn sst_table_file_name(dbname: &str, number: u64) -> String {
    make_file_name(dbname, number, "sst")
}

pub fn descriptor_file_name(dbname: &str, number: u64) -> String {
    format!("{dbname}/MANIFEST-{number:06}")
}

pub fn current_file_name(dbname: &str) -> String {
    format!("{dbname}/CURRENT")
}

pub fn lock_file_name(dbname: &str) -> String {
    format!("{dbname}/LOCK")
}

pub fn temp_file_name(dbname: &str, number: u64) -> String {
    make_file_name(dbname, number, "dbtmp")
}

pub fn info_log_file_name(dbname: &str) -> String {
    format!("{dbname}/LOG")
}

pub fn old_info_log_file_name(dbname: &str) -> String {
    format!("{dbname}/LOG.old")
}

/// Atomically updates the CURRENT file to point at a manifest.
/// Writes `<number>.dbtmp`, syncs it, then renames it over CURRENT.
pub fn set_current_file<E: Env>(env: &E, dbname: &str, descriptor_number: u64) -> Result<()> {
    let manifest = descriptor_file_name(dbname, descriptor_number);
    let prefix = format!("{dbname}/");
    let manifest_name = manifest.strip_prefix(&prefix).unwrap_or(&manifest);
    let contents = format!("{manifest_name}\n");
    let tmp = temp_file_name(dbname, descriptor_number);
    let tmp_path = Path::new(&tmp);
    let write_result = (|| -> Result<()> {
        let mut file = env.new_writable_file(tmp_path)?;
        file.append(contents.as_bytes())?;
        file.sync()?;
        file.close()?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = env.delete_file(tmp_path);
        return Err(error);
    }
    let current = current_file_name(dbname);
    match env.rename_file(tmp_path, Path::new(&current)) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = env.delete_file(tmp_path);
            Err(error)
        }
    }
}

/// Parse a file name into `(number, FileType)` if it matches
/// one of the known on-disk patterns. Returns `None` for
/// names that don't fit any pattern (e.g. random user files).
pub fn parse_file_name(filename: &str) -> Option<(u64, FileType)> {
    // Strip leading directory if present.
    let basename = match filename.rfind('/') {
        Some(idx) => &filename[idx + 1..],
        None => filename,
    };
    if basename == "CURRENT" {
        return Some((0, FileType::CurrentFile));
    }
    if basename == "LOCK" {
        return Some((0, FileType::DbLockFile));
    }
    if basename == "LOG" || basename == "LOG.old" {
        return Some((0, FileType::InfoLogFile));
    }
    if let Some(rest) = basename.strip_prefix("MANIFEST-") {
        let number: u64 = rest.parse().ok()?;
        return Some((number, FileType::DescriptorFile));
    }
    // <number>.<suffix>
    let dot = basename.find('.')?;
    let number: u64 = basename[..dot].parse().ok()?;
    let suffix = &basename[dot + 1..];
    let file_type = match suffix {
        "log" => FileType::LogFile,
        "ldb" | "sst" => FileType::TableFile,
        "dbtmp" => FileType::TempFile,
        _ => return None,
    };
    Some((number, file_type))
}
