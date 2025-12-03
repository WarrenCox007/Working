//! Scans filesystem for items, computes metadata and hashes, and stores in the DB.

use anyhow::Context;
use globset::{Glob, GlobSet, GlobSetBuilder};
use sqlx::SqlitePool;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task;
use walkdir::WalkDir;

#[derive(Debug, Clone, Default)]
pub enum HashMode {
    #[default]
    None,
    Fast, // read first N bytes + size/mtime
    Full, // full-file blake3
}

impl From<&str> for HashMode {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "fast" => HashMode::Fast,
            "full" => HashMode::Full,
            _ => HashMode::None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScannedItem {
    pub path: PathBuf,
    pub is_dir: bool,
    pub size: i64,
    pub mtime: i64,
    pub hash: Option<String>,
}

pub async fn scan(
    roots: &[PathBuf],
    excludes: &[String],
    hash_mode: &HashMode,
    pool: &SqlitePool,
) -> anyhow::Result<u64> {
    let (tx, mut rx) = mpsc::channel(100);
    let exclude_set = build_globset(excludes)?;
    let hash_mode = hash_mode.clone();
    let roots = roots.to_vec();

    // Walker task
    let walker_handle = task::spawn_blocking(move || {
        for root in roots {
            for entry in WalkDir::new(root)
                .follow_links(true)
                .into_iter()
                .filter_entry(|e| should_descend(e.path(), false, &exclude_set))
            {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let path = entry.path();
                if path.is_dir() || is_excluded(path, &exclude_set) || is_hidden(path) {
                    continue;
                }

                let meta = match fs::metadata(path) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                let mtime = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or_default();

                let hash = match hash_mode {
                    HashMode::None => None,
                    HashMode::Fast => fast_hash(path).ok(),
                    HashMode::Full => full_hash(path).ok(),
                };

                let item = ScannedItem {
                    path: path.to_path_buf(),
                    is_dir: meta.is_dir(),
                    size: meta.len() as i64,
                    mtime,
                    hash,
                };

                if tx.blocking_send(item).is_err() {
                    // Receiver dropped, stop walking.
                    break;
                }
            }
        }
    });

    let mut count = 0u64;
    while let Some(item) = rx.recv().await {
        upsert_file_in_db(pool, &item)
            .await
            .with_context(|| format!("Failed to upsert file in DB: {:?}", item.path))?;
        count += 1;
    }

    walker_handle.await?;
    Ok(count)
}

async fn upsert_file_in_db(pool: &SqlitePool, item: &ScannedItem) -> anyhow::Result<()> {
    let path_str = item.path.to_string_lossy().to_string();
    let ext = item
        .path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string());

    let ctime = item.mtime; // Placeholder

    let res = sqlx::query!(
        r#"
        INSERT INTO files (path, size, mtime, ctime, hash, ext, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))
        ON CONFLICT(path) DO UPDATE SET
            size = excluded.size,
            mtime = excluded.mtime,
            hash = excluded.hash,
            last_seen = strftime('%s','now'),
            status = 'seen'
        WHERE
            files.size != excluded.size OR
            files.mtime != excluded.mtime OR
            COALESCE(files.hash, '') != COALESCE(excluded.hash, '');
        "#,
        path_str,
        item.size,
        item.mtime,
        ctime,
        item.hash,
        ext
    )
    .execute(pool)
    .await?;

    if res.rows_affected() > 0 {
        sqlx::query!(
            "INSERT OR REPLACE INTO dirty (path, reason) VALUES (?, 'rescan')",
            path_str
        )
        .execute(pool)
        .await?;
    }

    Ok(())
}

fn build_globset(patterns: &[String]) -> anyhow::Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pat in patterns {
        let glob = Glob::new(pat)?;
        builder.add(glob);
    }
    Ok(builder.build()?)
}

fn should_descend(path: &Path, include_hidden: bool, excludes: &GlobSet) -> bool {
    if is_excluded(path, excludes) {
        return false;
    }
    if !include_hidden && is_hidden(path) {
        return false;
    }
    true
}

fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

fn is_excluded(path: &Path, excludes: &GlobSet) -> bool {
    excludes.is_match(path)
}

fn fast_hash(path: &Path) -> anyhow::Result<String> {
    use std::io::Read;
    const BYTES: usize = 64 * 1024;
    let mut file = fs::File::open(path)?;
    let mut buf = vec![0u8; BYTES];
    let n = file.read(&mut buf)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&buf[..n]);
    Ok(hasher.finalize().to_hex().to_string())
}

fn full_hash(path: &Path) -> anyhow::Result<String> {
    use std::io::Read;
    let mut file = fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
