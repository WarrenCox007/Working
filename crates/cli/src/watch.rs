use crate::keyword_index;
use anyhow::Result;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use organizer_core::config::AppConfig;
use organizer_core::pipeline;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::time::Duration;
use storage;

pub async fn watch_paths(cfg: AppConfig, paths: Vec<String>) -> Result<()> {
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
        notify::Config::default().with_poll_interval(Duration::from_secs(2)),
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
    loop {
        match rx.recv() {
            Ok(event) => {
                if let Ok(ev) = event {
                    for path in ev.paths {
                        if path.is_file() {
                            if let Err(e) = pipeline::process_file(&cfg, &path).await {
                                eprintln!("process error for {:?}: {}", path, e);
                            } else {
                                // mark dirty for keyword index refresh
                                let _ = mark_dirty(&cfg.database.path, &path).await;
                                let _ = keyword_index::enabled::build_index(
                                    &crate::keyword_index_dir(&cfg.database.path),
                                    &[],
                                );
                            }
                        }
                    }
                }
            }
            Err(e) => eprintln!("watch error: {:?}", e),
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
