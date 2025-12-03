use crate::keyword_index;
use anyhow::Result;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use organizer_core::classifier;
use organizer_core::config::{AppConfig, SafetyConfig};
use organizer_core::embeddings;
use organizer_core::extractor;
use organizer_core::pipeline;
use organizer_core::scanner::{self, HashMode};
use organizer_core::vectorstore::AsQdrant;
use sqlx::{QueryBuilder, Row};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};
use storage;

pub async fn watch_paths(
    cfg: AppConfig,
    paths: Vec<String>,
    debounce_ms: u64,
    quiet: bool,
) -> Result<()> {
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

    if !quiet {
        println!("Watching {} path(s)...", watch_list.len());
    }
    let debounce = Duration::from_millis(debounce_ms.max(200));
    let mut pending: HashSet<PathBuf> = HashSet::new();
    let mut last_flush = Instant::now();
    let mut processed_total: usize = 0;
    let safety: SafetyConfig = cfg.safety.clone();
    let mut attempted_vectors_last: usize = 0;

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
                    if let Err(e) = process_file(&cfg, path).await {
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
                        &keyword_index_dir(&cfg.database.path),
                        &removed,
                    );
                }
                // Best-effort vector delete for removed files
                if let Some(qdrant) =
                    organizer_core::pipeline::build_vector_store(&cfg).downcast_qdrant()
                {
                    if safety.immediate_vector_delete {
                        // record attempted counts
                        attempted_vectors_last = removed.len()
                            + removed_hashes.len()
                            + removed_file_hashes.len()
                            + removed_point_ids.len();
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
            }
            if cfg!(feature = "keyword-index") {
                let _ = refresh_keyword_index_if_dirty(&cfg, None).await;
            }
            if !quiet {
                println!(
                    "Processed batch of {} file(s) (total processed: {}); next refresh after {:?}",
                    batch.len(),
                    processed_total,
                    debounce
                );
            }
            if !removed.is_empty() {
                let attempted_vectors = attempted_vectors_last as i64;
                let attempted_docs = removed.len() as i64;
                let _ = log_audit(
                    &cfg.database.path,
                    &removed,
                    &removed_hashes,
                    &removed_point_ids,
                    attempted_docs,
                    attempted_vectors,
                )
                .await;
            }
        }
    }
}

async fn process_file(cfg: &AppConfig, path: &PathBuf) -> Result<()> {
    // Re-scan just this path, then run extractor/embed/classify.
    let pool = storage::connect(&cfg.database.path).await?;
    // Capture existing hash to detect change.
    let hash_before: Option<String> = sqlx::query_scalar(
        "SELECT COALESCE(full_hash, fast_hash, hash) FROM files WHERE path = ?",
    )
        .bind(path.to_string_lossy())
        .fetch_optional(&pool)
        .await?
        .flatten();

    let hash_mode = HashMode::from(cfg.scan.hash_mode.as_deref().unwrap_or(""));
    let excludes = cfg.scan.exclude.clone();
    let roots = vec![path.clone()];
    scanner::scan(&roots, &excludes, &hash_mode, &pool).await?;

    extractor::run_extractor(&pool, &cfg.parsers).await?;

    // Restrict downstream work to this file for speed.
    let file_row = sqlx::query("SELECT id FROM files WHERE path = ?")
        .bind(path.to_string_lossy())
        .fetch_optional(&pool)
        .await?;
    let file_ids: Vec<i64> = file_row.into_iter().filter_map(|r| r.try_get(0).ok()).collect();
    if file_ids.is_empty() {
        return Ok(());
    }

    // If hash unchanged, skip embed/classify.
    let hash_after: Option<String> = sqlx::query_scalar(
        "SELECT COALESCE(full_hash, fast_hash, hash) FROM files WHERE id = ?",
    )
        .bind(file_ids[0])
        .fetch_optional(&pool)
        .await?
        .flatten();
    if hash_before.is_some() && hash_before == hash_after {
        return Ok(());
    }

    let registry = pipeline::build_registry(cfg);
    let vector_store = pipeline::build_vector_store(cfg);
    if let Some(qdrant) = vector_store.downcast_qdrant() {
        // Embed any new chunks and classify with kNN.
        let _ = embeddings::run_embedder_for_files(
            &pool,
            &registry,
            &qdrant,
            cfg.embeddings.batch_size,
            Some(&file_ids),
        )
        .await?;
        let files = files_for_ids(&pool, &file_ids).await?;
        if !files.is_empty() {
            let _ =
                classifier::run_classifier_for_files(&pool, &registry, &qdrant, files).await?;
        }
    } else {
        let files = files_for_ids(&pool, &file_ids).await?;
        if !files.is_empty() {
            let _ = classifier::run_classifier_no_knn_for_files(&pool, &registry, files).await?;
        }
    }
    Ok(())
}

async fn files_for_ids(pool: &sqlx::SqlitePool, ids: &[i64]) -> Result<Vec<storage::models::File>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut qb = QueryBuilder::new("SELECT * FROM files WHERE id IN (");
    let mut separated = qb.separated(", ");
    for id in ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");
    let rows = qb.build_query_as::<storage::models::File>().fetch_all(pool).await?;
    Ok(rows)
}

// Copied from main.rs until shared helpers are extracted.
fn keyword_index_dir(db_path: &str) -> PathBuf {
    let stripped = db_path.strip_prefix("sqlite://").unwrap_or(db_path);
    let db = PathBuf::from(stripped);
    let base = if stripped == ":memory:" {
        std::env::temp_dir()
    } else {
        db.parent().unwrap_or_else(|| Path::new(".")).to_path_buf()
    };
    let dir = base.join(".organizer_keyword_index");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

async fn refresh_keyword_index_if_dirty(cfg: &AppConfig, tags: Option<&[String]>) -> Result<()> {
    if !cfg!(feature = "keyword-index") {
        return Ok(());
    }
    let pool = storage::connect(&cfg.database.path).await?;
    let rows = sqlx::query("SELECT path FROM dirty")
        .fetch_all(&pool)
        .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let paths: Vec<String> = rows.into_iter().filter_map(|r| r.try_get(0).ok()).collect();
    let dir = keyword_index_dir(&cfg.database.path);
    let (docs, missing) = keyword_index_docs_for_paths(&cfg.database.path, &paths, tags).await?;
    if dir.join("meta.json").exists() {
        let _ = keyword_index::enabled::upsert_docs(&dir, &docs);
        if !missing.is_empty() {
            let _ = keyword_index::enabled::delete_docs(&dir, &missing);
        }
    } else {
        let _ = keyword_index::enabled::build_index(&dir, &docs);
    }
    let mut qb = sqlx::QueryBuilder::new("DELETE FROM dirty WHERE path IN (");
    let mut separated = qb.separated(", ");
    for p in &paths {
        separated.push_bind(p);
    }
    separated.push_unseparated(")");
    let _ = qb.build().execute(&pool).await;
    Ok(())
}

pub async fn keyword_index_docs_for_paths(
    db_path: &str,
    paths: &[String],
    _tags: Option<&[String]>,
) -> Result<(Vec<(String, String)>, Vec<String>)> {
    if paths.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let pool = storage::connect(db_path).await?;
    let mut qb = QueryBuilder::new("SELECT path, mime, id FROM files WHERE path IN (");
    let mut separated = qb.separated(", ");
    for p in paths {
        separated.push_bind(p);
    }
    separated.push_unseparated(")");
    let rows = qb.build().fetch_all(&pool).await?;
    let mut docs = Vec::new();
    let mut found = HashSet::new();
    let mut file_ids = Vec::new();
    for row in rows {
        let path: String = row.get(0);
        let mime: Option<String> = row.try_get(1).ok();
        let file_id: i64 = row.try_get(2).unwrap_or_default();
        file_ids.push((file_id, path.clone(), mime.clone()));
        let doc_text = format!("{} {}", path, mime.clone().unwrap_or_default());
        docs.push((path.clone(), doc_text));
        found.insert(path);
    }
    // Append first chunk preview text for richer keyword search
    if !file_ids.is_empty() {
        let mut qb_chunks =
            QueryBuilder::new("SELECT file_id, text_preview FROM chunks WHERE file_id IN (");
        let mut sep = qb_chunks.separated(", ");
        for (id, _, _) in &file_ids {
            sep.push_bind(id);
        }
        sep.push_unseparated(")");
        let chunk_rows = qb_chunks.build().fetch_all(&pool).await?;
        let mut chunk_map: std::collections::HashMap<i64, String> = std::collections::HashMap::new();
        for row in chunk_rows {
            let fid: i64 = row.try_get(0).unwrap_or_default();
            if let Ok(Some(text)) = row.try_get::<Option<String>, _>(1) {
                chunk_map.entry(fid).or_insert(text);
            }
        }
        for (fid, path, mime) in file_ids {
            if let Some(text) = chunk_map.get(&fid) {
                let doc_text = format!("{} {} {}", path, mime.clone().unwrap_or_default(), text);
                docs.push((path.clone(), doc_text));
            }
        }
    }
    let missing: Vec<String> = paths
        .iter()
        .filter(|p| !found.contains(*p))
        .cloned()
        .collect();
    Ok((docs, missing))
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

async fn log_audit(
    db_path: &str,
    paths: &[String],
    hashes: &[String],
    point_ids: &[String],
    attempted_docs: i64,
    attempted_vectors: i64,
) -> Result<()> {
    let pool = storage::connect(db_path).await?;
    let detail = serde_json::json!({
        "paths": paths,
        "hashes": hashes,
        "vector_ids": point_ids,
        "attempted_docs": attempted_docs,
        "attempted_vectors": attempted_vectors,
    })
    .to_string();
    let _ =
        sqlx::query("INSERT INTO audit(action_id, event, detail) VALUES(NULL,'watch_purge',?1)")
            .bind(detail)
            .execute(&pool)
            .await?;
    Ok(())
}
