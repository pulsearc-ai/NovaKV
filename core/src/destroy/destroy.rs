use std::path::Path;

use crate::env::Env;
use crate::filename::{lock_file_name, parse_file_name, FileType};
use crate::status::Result;

/// Deletes a database directory and its contents.
/// Acquires the DB LOCK before deleting database files.
/// Idempotent on a missing target (returns Ok).
pub fn destroy_db<E: Env>(dbname: &str, env: E) -> Result<()> {
    let db_path = Path::new(dbname);
    let Ok(entries) = env.list_dir(db_path) else {
        return env.delete_dir(db_path);
    };
    if entries.is_empty() {
        return Ok(());
    }
    let lock_path = lock_file_name(dbname);
    let lock_path_ref = Path::new(&lock_path);
    let lock = env.lock_file(lock_path_ref)?;
    let mut result: Result<()> = Ok(());
    for entry in entries {
        let name = entry.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if matches!(parse_file_name(name), Some((_, FileType::DbLockFile))) {
            continue;
        }
        let delete_result = match env.delete_file(&entry) {
            Ok(()) => Ok(()),
            Err(_) => env.delete_dir(&entry),
        };
        if result.is_ok() {
            if let Err(error) = delete_result {
                result = Err(error);
            }
        }
    }
    drop(lock);
    let _ = env.delete_file(lock_path_ref);
    let _ = env.delete_dir(db_path);
    result
}
