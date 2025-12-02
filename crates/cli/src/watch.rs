use anyhow::Result;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use organizer_core::config::AppConfig;
use organizer_core::pipeline;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};
use storage;

pub async fn watch_paths(cfg: AppConfig, paths: Vec<String>, debounce_ms: u64) -> Result<()> {
    let mut watch_list: Vec<PathBuf> = if paths.is_empty() {
        cfg.scan.include.iter().map(|p| PathBuf::from(p)).collect()
    } else {
        paths.into_iter().map(PathBuf::from).collect()
    };
    if watch_list.is_empty() {
        watch_list.push(PathBuf::from("."));
    }

    let (tx, rx) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(
        tx,
        notify::Config::default().with_poll_interval(Duration::from_millis(750)),
    )?;
    for p in &watch_list {
        let mode = if p.is_dir() {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        watcher.watch(p, mode)?;
    }

    println!("Watching {} path(s)...", watch_list.len());
    let debounce = Duration::from_millis(debounce_ms.max(200));
    let mut pending: HashSet<PathBuf> = HashSet::new();
    let mut last_flush = Instant::now();

    loop {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(event) => {
                if let Ok(ev) = event {
                    for path in ev.paths {
                        if path.is_file() {
                            pending.insert(path);
                        }
                    }
                }
            }
            Err(_) => {}
        }

        if !pending.is_empty() && last_flush.elapsed() >= debounce {
            let batch: Vec<PathBuf> = pending.drain().collect();
            last_flush = Instant::now();
            for path in &batch {
                if let Err(e) = pipeline::process_file(&cfg, path).await {
                    eprintln!("process error for {:?}: {}", path, e);
                } else {
                    let _ = mark_dirty(&cfg.database.path, path).await;
                }
            }
            if cfg!(feature = "keyword-index") {
                let _ = crate::refresh_keyword_index_if_dirty(&cfg, None).await;
            }
            println!(
                "Processed batch of {} file(s); next refresh after {:?}",
                batch.len(),
                debounce
            );
        }
    }
}

async fn mark_dirty(db_path: &str, path: &PathBuf) -> Result<()> {
    let pool = storage::connect(db_path).await?;
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO dirty(path, reason, updated_at) VALUES (?1,'watch', strftime('%s','now'))",
    )
    .bind(path.to_string_lossy())
    .execute(&pool)
    .await?;
    Ok(())
}
