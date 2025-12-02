use globset::{Glob, GlobSet, GlobSetBuilder};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::task;
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub enum HashMode {
    None,
    Fast, // read first N bytes + size/mtime
    Full, // full-file blake3
}

#[derive(Debug, Clone)]
pub struct ScanConfig {
    pub roots: Vec<PathBuf>,
    pub include_hidden: bool,
    pub follow_symlinks: bool,
    pub excludes: Vec<String>,
    pub hash_mode: HashMode,
}

#[derive(Debug, Clone)]
pub struct ScannedItem {
    pub path: PathBuf,
    pub is_dir: bool,
    pub size: u64,
    pub mtime: i64,
    pub hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ScanResult {
    pub discovered: Vec<ScannedItem>,
}

pub async fn scan(config: ScanConfig) -> anyhow::Result<ScanResult> {
    let exclude_set = build_globset(&config.excludes)?;
    let include_hidden = config.include_hidden;
    let follow_symlinks = config.follow_symlinks;
    let hash_mode = config.hash_mode.clone();

    let roots = config.roots.clone();
    let discovered = task::spawn_blocking(move || {
        let mut items = Vec::new();
        for root in roots {
            for entry in WalkDir::new(root)
                .follow_links(follow_symlinks)
                .into_iter()
                .filter_entry(|e| should_descend(e.path(), include_hidden, &exclude_set))
            {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                let path = entry.path().to_path_buf();
                if is_excluded(&path, &exclude_set) || (!include_hidden && is_hidden(&path)) {
                    continue;
                }

                let meta = match fs::metadata(&path) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                let is_dir = meta.is_dir();
                let size = meta.len();
                let mtime = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or_default();

                let hash = if is_dir {
                    None
                } else {
                    match hash_mode {
                        HashMode::None => None,
                        HashMode::Fast => fast_hash(&path).ok(),
                        HashMode::Full => full_hash(&path).ok(),
                    }
                };

                items.push(ScannedItem {
                    path,
                    is_dir,
                    size,
                    mtime,
                    hash,
                });
            }
        }
        items
    })
    .await?;

    Ok(ScanResult { discovered })
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
