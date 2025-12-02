use crate::keyword_index;
use anyhow::Result;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use organizer_core::config::AppConfig;
use organizer_core::pipeline;
use organizer_core::vectorstore::AsQdrant;
use sqlx::Row;
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
    let mut processed_total: usize = 0;

    loop {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(event) => {
                if let Ok(ev) = event {
                    for path in ev.paths {
                        pending.insert(path);
                    }
                }
            }
            Err(_) => {}
        }

        if !pending.is_empty() && last_flush.elapsed() >= debounce {
            let batch: Vec<PathBuf> = pending.drain().collect();
            last_flush = Instant::now();
            let mut removed: Vec<String> = Vec::new();
            let mut removed_hashes: Vec<String> = Vec::new();
            let mut removed_file_hashes: Vec<String> = Vec::new();
            let mut removed_point_ids: Vec<String> = Vec::new();
            processed_total += batch.len();
            for path in &batch {
                if path.is_file() {
                    if let Err(e) = pipeline::process_file(&cfg, path).await {
                        eprintln!("process error for {:?}: {}", path, e);
                    } else {
                        let _ = mark_dirty(&cfg.database.path, path).await;
                    }
                } else {
                    // Handle deletions: collect hashes, remove from DB and mark dirty for keyword index cleanup.
                    if let Ok((chunk_hashes, file_hash)) =
                        collect_hashes(&cfg.database.path, path).await
                    {
                        removed_hashes.extend(chunk_hashes.clone());
                        removed_point_ids.extend(chunk_hashes);
                        if let Some(fh) = file_hash {
                            removed_file_hashes.push(fh);
                        }
                    }
                    if let Err(e) = purge_file(&cfg.database.path, path).await {
                        eprintln!("purge error for {:?}: {}", path, e);
                    } else {
                        let _ = mark_dirty(&cfg.database.path, path).await;
                        removed.push(path.to_string_lossy().into_owned());
                    }
                }
            }
            if !removed.is_empty() {
                if cfg!(feature = "keyword-index") {
                    let _ = keyword_index::enabled::delete_docs(
                        &crate::keyword_index_dir(&cfg.database.path),
                        &removed,
                    );
                }
                // Best-effort vector delete for removed files
                if let Some(qdrant) =
                    organizer_core::pipeline::build_vector_store(&cfg).downcast_qdrant()
                {
                    let mut must = vec![serde_json::json!({
                        "key": "path",
                        "match": { "any": removed }
                    })];
                    if !removed_hashes.is_empty() {
                        must.push(serde_json::json!({
                            "key": "hash",
                            "match": { "any": removed_hashes }
                        }));
                    }
                    if !removed_file_hashes.is_empty() {
                        must.push(serde_json::json!({
                            "key": "file_hash",
                            "match": { "any": removed_file_hashes }
                        }));
                    }
                    let _ = qdrant
                        .delete_by_filter(serde_json::json!({ "must": must }))
                        .await;
                    if !removed_point_ids.is_empty() {
                        let _ = qdrant.delete_by_ids(&removed_point_ids).await;
                    }
                }
            }
            if cfg!(feature = "keyword-index") {
                let _ = crate::refresh_keyword_index_if_dirty(&cfg, None).await;
            }
            println!(
                "Processed batch of {} file(s) (total processed: {}); next refresh after {:?}",
                batch.len(),
                processed_total,
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

async fn purge_file(db_path: &str, path: &PathBuf) -> Result<()> {
    let pool = storage::connect(db_path).await?;
    // Mark actions for this file as errored, then delete (covers cases where FK cascade is not enforced).
    let err_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM actions WHERE file_id=(SELECT id FROM files WHERE path = ?1)",
    )
    .bind(path.to_string_lossy())
    .fetch_one(&pool)
    .await
    .unwrap_or(0);
    if err_count > 0 {
        eprintln!("purging {} actions for deleted path {:?}", err_count, path);
    }
    let _ = sqlx::query(
        "UPDATE actions SET status='error' WHERE file_id=(SELECT id FROM files WHERE path = ?1)",
    )
    .bind(path.to_string_lossy())
    .execute(&pool)
    .await;
    let _ = sqlx::query("DELETE FROM actions WHERE file_id=(SELECT id FROM files WHERE path = ?1)")
        .bind(path.to_string_lossy())
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM files WHERE path = ?1")
        .bind(path.to_string_lossy())
        .execute(&pool)
        .await?;
    let _ =
        sqlx::query("INSERT INTO audit(action_id, event, detail) VALUES(NULL,'file_purged',?1)")
            .bind(path.to_string_lossy())
            .execute(&pool)
            .await;
    Ok(())
}

async fn collect_hashes(db_path: &str, path: &PathBuf) -> Result<(Vec<String>, Option<String>)> {
    let pool = storage::connect(db_path).await?;
    let rows = sqlx::query(
        "SELECT c.hash, f.hash as file_hash FROM chunks c JOIN files f ON f.id = c.file_id WHERE f.path = ?1 AND c.hash IS NOT NULL",
    )
    .bind(path.to_string_lossy())
    .fetch_all(&pool)
    .await?;
    let mut hashes = Vec::new();
    let mut file_hash: Option<String> = None;
    for row in rows {
        if let Ok(h) = row.try_get::<String, _>(0) {
            hashes.push(h);
        }
        if file_hash.is_none() {
            if let Ok(fh) = row.try_get::<String, _>(1) {
                if !fh.is_empty() {
                    file_hash = Some(fh);
                }
            }
        }
    }
    Ok((hashes, file_hash))
}
