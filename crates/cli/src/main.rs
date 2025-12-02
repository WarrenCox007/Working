use anyhow::Result;
use clap::{Parser, Subcommand};
use organizer_core::config;
use organizer_core::config::AppConfig;
use organizer_core::embeddings;
use organizer_core::pipeline;
use organizer_core::pipeline::PipelineMode;
use organizer_core::search;
use organizer_core::vectorstore::AsQdrant;
use sqlx::{QueryBuilder, Row};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use storage;
mod apply;
mod fs_apply;
mod keyword_index;
mod paths;
mod undo;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let cfg = config::load(cli.config.as_deref())?;

    match cli.command {
        Commands::Scan { json } => run_pipeline(cfg, PipelineMode::Scan, json).await,
        Commands::Classify { json } => run_pipeline(cfg, PipelineMode::Classify, json).await,
        Commands::Suggest { json, list, tags } => {
            if list {
                run_actions(cfg, "planned", None, None, false, false, &tags, &[], json).await
            } else {
                run_pipeline(cfg, PipelineMode::Suggest, json).await
            }
        }
        Commands::Apply {
            dry_run,
            ids,
            json,
            fields,
            summary,
            verbose,
            allow_paths,
            deny_paths,
            trash_dir,
            conflict,
        } => {
            run_apply(
                cfg,
                dry_run,
                ids.as_deref(),
                json,
                summary,
                verbose,
                allow_paths,
                deny_paths,
                trash_dir,
                conflict,
                Some(fields),
            )
            .await
        }
        Commands::Search {
            query,
            topk,
            path_prefix,
            mime,
            hybrid,
            after,
            before,
            keyword_index,
            keyword_index_refresh,
            tags,
            fields,
        } => {
            run_search(
                cfg,
                query,
                topk,
                path_prefix,
                mime,
                hybrid,
                after,
                before,
                keyword_index,
                keyword_index_refresh,
                tags,
                fields,
            )
            .await
        }
        Commands::Actions {
            status,
            rule,
            kind,
            has_backup,
            duplicates_only,
            tags,
            fields,
            json,
        } => {
            run_actions(
                cfg,
                &status,
                rule.as_deref(),
                kind.as_deref(),
                has_backup,
                duplicates_only,
                &tags,
                &fields,
                json,
            )
            .await
        }
        Commands::Undo { ids, backup_path } => {
            run_undo(cfg, ids.as_deref(), backup_path.as_deref()).await
        }
    }
}

#[derive(Parser)]
#[command(name = "ai-organizer")]
#[command(about = "AI-powered file organizer", long_about = None)]
struct Cli {
    /// Path to config TOML
    #[arg(short, long)]
    config: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan and index files
    Scan {
        /// Output JSON summary
        #[arg(long)]
        json: bool,
    },
    /// Run classification pipeline
    Classify {
        /// Output JSON summary
        #[arg(long)]
        json: bool,
    },
    /// Generate suggestions
    Suggest {
        /// Output JSON summary
        #[arg(long)]
        json: bool,
        /// List existing planned actions instead of running pipeline
        #[arg(long, default_value_t = false)]
        list: bool,
        /// Filter listed actions by tag (comma-separated)
        #[arg(long, value_delimiter = ',', num_args = 1.., default_values_t = Vec::<String>::new())]
        tags: Vec<String>,
    },
    /// Apply planned actions (move/tag/rename) from the database
    Apply {
        /// Do not actually perform changes, only print what would happen
        #[arg(long, default_value_t = true)]
        dry_run: bool,
        /// Comma-separated action IDs to apply; if omitted, apply all planned
        #[arg(long)]
        ids: Option<String>,
        /// Output JSON
        #[arg(long)]
        json: bool,
        /// Restrict output fields (comma-separated), e.g. id,path,kind,status,backup
        #[arg(long, value_delimiter = ',', num_args = 1.., default_values_t = Vec::<String>::new())]
        fields: Vec<String>,
        /// Show brief summary instead of full rows (non-JSON)
        #[arg(long, default_value_t = false)]
        summary: bool,
        /// Verbose per-action output (non-JSON)
        #[arg(long, default_value_t = false)]
        verbose: bool,
        /// Override allow paths (comma-separated)
        #[arg(long)]
        allow_paths: Option<String>,
        /// Override deny paths (comma-separated)
        #[arg(long)]
        deny_paths: Option<String>,
        /// Override trash directory for backups
        #[arg(long)]
        trash_dir: Option<String>,
        /// Conflict policy: rename|skip|overwrite
        #[arg(long, default_value = "rename")]
        conflict: String,
    },
    /// Semantic search against vector store
    Search {
        /// Query text to embed and search
        query: String,
        /// Number of results
        #[arg(short, long, default_value_t = 5)]
        topk: u64,
        /// Optional path prefix filter
        #[arg(long)]
        path_prefix: Option<String>,
        /// Optional MIME filter
        #[arg(long)]
        mime: Option<String>,
        /// Also merge keyword/path search results (hybrid)
        #[arg(long, default_value_t = false)]
        hybrid: bool,
        /// Only include files modified after this RFC3339 timestamp
        #[arg(long)]
        after: Option<String>,
        /// Only include files modified before this RFC3339 timestamp
        #[arg(long)]
        before: Option<String>,
        /// Use Tantivy keyword index if available (requires feature)
        #[arg(long, default_value_t = false)]
        keyword_index: bool,
        /// Force rebuilding the keyword index (otherwise reuse if present)
        #[arg(long, default_value_t = false)]
        keyword_index_refresh: bool,
        /// Filter by tag names (comma-separated)
        #[arg(long, value_delimiter = ',', num_args = 1.., default_values_t = Vec::<String>::new())]
        tags: Vec<String>,
        /// Restrict output fields (comma-separated), e.g. path,score,tags,payload
        #[arg(long, value_delimiter = ',', num_args = 1.., default_values_t = Vec::<String>::new())]
        fields: Vec<String>,
    },
    /// List actions from the database
    Actions {
        /// Status filter (planned|executed|error)
        #[arg(long, default_value = "planned")]
        status: String,
        /// Filter by rule name
        #[arg(long)]
        rule: Option<String>,
        /// Filter by kind (move|rename|tag)
        #[arg(long)]
        kind: Option<String>,
        /// Filter by backup presence
        #[arg(long, default_value_t = false)]
        has_backup: bool,
        /// Show only duplicate-related actions
        #[arg(long, default_value_t = false)]
        duplicates_only: bool,
        /// Filter by tag names (comma-separated)
        #[arg(long, value_delimiter = ',', num_args = 1.., default_values_t = Vec::<String>::new())]
        tags: Vec<String>,
        /// Restrict output fields (comma-separated), e.g. id,path,kind,status,tags,backup_path
        #[arg(long, value_delimiter = ',', num_args = 1.., default_values_t = Vec::<String>::new())]
        fields: Vec<String>,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Undo executed actions (placeholder)
    Undo {
        /// Comma-separated action IDs; if omitted, reset all executed
        #[arg(long)]
        ids: Option<String>,
        /// Provide backup path to restore from if not recorded
        #[arg(long)]
        backup_path: Option<String>,
    },
}

async fn run_pipeline(cfg: AppConfig, mode: PipelineMode, json: bool) -> Result<()> {
    let is_suggest = matches!(mode, PipelineMode::Suggest);
    let mode_label = match mode {
        PipelineMode::Scan => "scan",
        PipelineMode::Classify => "classify",
        PipelineMode::Suggest => "suggest",
        PipelineMode::All => "all",
    };
    let summary = pipeline::run_with_mode_summary(cfg.clone(), mode).await?;
    if json {
        let mut summary_json = serde_json::json!({
            "status": "ok",
            "mode": mode_label,
            "discovered": summary.discovered,
            "processed_files": summary.processed_files,
            "embedded_chunks": summary.embedded_chunks,
        });
        if is_suggest {
            if let Ok(actions) = fetch_actions(&cfg.database.path).await {
                summary_json
                    .as_object_mut()
                    .unwrap()
                    .insert("actions".into(), serde_json::to_value(actions)?);
            }
        }
        println!("{}", serde_json::to_string_pretty(&summary_json)?);
    } else {
        println!(
            "{}: discovered {}, processed {}, embedded chunks {}",
            mode_label, summary.discovered, summary.processed_files, summary.embedded_chunks
        );
        if is_suggest {
            if let Ok(actions) = fetch_actions(&cfg.database.path).await {
                println!("planned actions: {}", actions.len());
            }
        }
    }
    Ok(())
}

async fn run_search(
    cfg: AppConfig,
    query: String,
    topk: u64,
    path_prefix: Option<String>,
    mime: Option<String>,
    hybrid: bool,
    after: Option<String>,
    before: Option<String>,
    keyword_index: bool,
    keyword_index_refresh: bool,
    tags: Vec<String>,
    fields: Vec<String>,
) -> Result<()> {
    // Build provider registry for embeddings and vector store.
    let registry = pipeline::build_registry(&cfg);
    let vector_store = pipeline::build_vector_store(&cfg);
    let tag_filter = if tags.is_empty() { None } else { Some(tags) };
    let fields = if fields.is_empty() {
        vec![
            "path".to_string(),
            "score".to_string(),
            "tags".to_string(),
            "snippet".to_string(),
            "payload".to_string(),
        ]
    } else {
        fields
    };
    let tagged_paths: Option<HashSet<String>> = if let Some(t) = &tag_filter {
        Some(fetch_paths_with_tags(&cfg.database.path, t).await?)
    } else {
        None
    };
    let use_keyword_index = keyword_index && cfg!(feature = "keyword-index");
    if let Some(qdrant) = vector_store.downcast_qdrant() {
        let embed = embeddings::embed(
            embeddings::EmbeddingRequest {
                texts: vec![query.clone()],
                provider: Some(cfg.embeddings.provider.clone()),
            },
            &registry,
        )
        .await?;
        let vector = embed.vectors.into_iter().next().unwrap_or_default();
        let filter = build_qdrant_filter(
            path_prefix.clone(),
            mime.clone(),
            after.as_deref(),
            before.as_deref(),
        );
        let results = search::vector_search(&qdrant, vector, topk, filter).await?;
        let mut results_json: Vec<serde_json::Value> = results
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id": r.id,
                    "score": r.score,
                    "payload": r.payload,
                })
            })
            .collect();
        if let Some(allowed) = &tagged_paths {
            results_json.retain(|r| {
                extract_path(r)
                    .map(|p| allowed.contains(&p))
                    .unwrap_or(false)
            });
        }
        if hybrid {
            let mut seen = std::collections::HashSet::new();
            for r in &results_json {
                if let Some(p) = r
                    .get("payload")
                    .and_then(|p| p.get("path"))
                    .and_then(|v| v.as_str())
                {
                    seen.insert(p.to_string());
                }
            }
            let fallback = if use_keyword_index {
                refresh_keyword_index_if_dirty(&cfg, tag_filter.as_deref()).await?;
                keyword_index_search(
                    &cfg,
                    &query,
                    topk,
                    path_prefix.clone(),
                    mime.clone(),
                    after.as_deref(),
                    before.as_deref(),
                    tag_filter.as_deref(),
                    keyword_index_refresh,
                )
                .await?
            } else {
                keyword_search(
                    &cfg.database.path,
                    &query,
                    path_prefix.clone(),
                    mime.clone(),
                    after.as_deref(),
                    before.as_deref(),
                    tag_filter.as_deref(),
                )
                .await?
            };
            for f in fallback {
                if let Some(p) = f.get("path").and_then(|v| v.as_str()) {
                    if seen.insert(p.to_string()) {
                        results_json.push(f);
                    }
                }
            }
        }
        attach_tags(&cfg.database.path, &mut results_json).await?;
        attach_snippets(&cfg.database.path, &mut results_json).await?;
        let filtered = filter_fields(results_json, &fields);
        let out = serde_json::to_string_pretty(&filtered)?;
        println!("{}", out);
        return Ok(());
    }

    // Fallback: simple DB LIKE search if vector store is unavailable.
    eprintln!("Vector store not configured; using fallback path search.");
    let mut results = if use_keyword_index {
        refresh_keyword_index_if_dirty(&cfg, tag_filter.as_deref()).await?;
        keyword_index_search(
            &cfg,
            &query,
            topk,
            path_prefix.clone(),
            mime.clone(),
            after.as_deref(),
            before.as_deref(),
            tag_filter.as_deref(),
            keyword_index_refresh,
        )
        .await?
    } else {
        keyword_search(
            &cfg.database.path,
            &query,
            path_prefix.clone(),
            mime.clone(),
            after.as_deref(),
            before.as_deref(),
            tag_filter.as_deref(),
        )
        .await?
    };
    if let Some(allowed) = &tagged_paths {
        results.retain(|r| {
            extract_path(r)
                .map(|p| allowed.contains(&p))
                .unwrap_or(false)
        });
    }
    attach_tags(&cfg.database.path, &mut results).await?;
    attach_snippets(&cfg.database.path, &mut results).await?;
    let filtered = filter_fields(results, &fields);
    println!("{}", serde_json::to_string_pretty(&filtered)?);
    Ok(())
}

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

fn extract_path(val: &serde_json::Value) -> Option<String> {
    if let Some(p) = val
        .get("payload")
        .and_then(|p| p.get("path"))
        .and_then(|v| v.as_str())
    {
        return Some(p.to_string());
    }
    val.get("path")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

async fn fetch_paths_with_tags(db_path: &str, tags: &[String]) -> Result<HashSet<String>> {
    if tags.is_empty() {
        return Ok(HashSet::new());
    }
    let pool = storage::connect(db_path).await?;
    let mut query =
        QueryBuilder::new("SELECT DISTINCT files.path FROM files JOIN file_tags ft ON ft.file_id = files.id JOIN tags t ON t.id = ft.tag_id WHERE t.name IN (");
    let mut first = true;
    for tag in tags {
        if !first {
            query.push(", ");
        }
        first = false;
        query.push_bind(tag);
    }
    query.push(")");
    let rows = query.build().fetch_all(&pool).await?;
    let mut paths = HashSet::new();
    for row in rows {
        let path: String = row.get(0);
        paths.insert(path);
    }
    Ok(paths)
}

async fn fetch_tags_for_paths(
    db_path: &str,
    paths: &[String],
) -> Result<HashMap<String, Vec<String>>> {
    if paths.is_empty() {
        return Ok(HashMap::new());
    }
    let pool = storage::connect(db_path).await?;
    let mut qb = QueryBuilder::new(
        "SELECT files.path, GROUP_CONCAT(t.name, ',') as tags FROM files LEFT JOIN file_tags ft ON ft.file_id = files.id LEFT JOIN tags t ON t.id = ft.tag_id WHERE files.path IN (",
    );
    let mut separated = qb.separated(", ");
    for p in paths {
        separated.push_bind(p);
    }
    separated.push_unseparated(")");
    qb.push(" GROUP BY files.path");
    let rows = qb.build().fetch_all(&pool).await?;
    let mut map = HashMap::new();
    for row in rows {
        let path: String = row.get(0);
        let tags_col: Option<String> = row.try_get(1).ok();
        let tags_vec: Vec<String> = tags_col
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string())
            .collect();
        map.insert(path, tags_vec);
    }
    Ok(map)
}

async fn attach_tags(db_path: &str, results: &mut Vec<serde_json::Value>) -> Result<()> {
    let paths: Vec<String> = results.iter().filter_map(|r| extract_path(r)).collect();
    if paths.is_empty() {
        return Ok(());
    }
    let tag_map = fetch_tags_for_paths(db_path, &paths).await?;
    for r in results.iter_mut() {
        if let Some(path) = extract_path(r) {
            if let Some(tags) = tag_map.get(&path) {
                if tags.is_empty() {
                    continue;
                }
                let tags_val = serde_json::Value::Array(
                    tags.iter()
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect(),
                );
                if let Some(payload) = r.get_mut("payload") {
                    if payload.is_object() {
                        payload
                            .as_object_mut()
                            .unwrap()
                            .insert("tags".into(), tags_val.clone());
                    }
                }
                if r.get("payload").is_none() || !r.get("payload").unwrap().is_object() {
                    if let Some(obj) = r.as_object_mut() {
                        obj.insert("tags".into(), tags_val);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn fetch_snippets_for_paths(
    db_path: &str,
    paths: &[String],
) -> Result<HashMap<String, String>> {
    if paths.is_empty() {
        return Ok(HashMap::new());
    }
    let pool = storage::connect(db_path).await?;
    let mut qb = QueryBuilder::new("SELECT files.path, chunks.text_preview FROM files JOIN chunks ON chunks.file_id = files.id WHERE files.path IN (");
    let mut separated = qb.separated(", ");
    for p in paths {
        separated.push_bind(p);
    }
    separated.push_unseparated(") ORDER BY chunks.start");
    let rows = qb.build().fetch_all(&pool).await?;
    let mut map = HashMap::new();
    for row in rows {
        let path: String = row.get(0);
        let snippet: Option<String> = row.try_get(1).ok();
        if let Some(s) = snippet {
            map.entry(path).or_insert(s);
        }
    }
    Ok(map)
}

async fn attach_snippets(db_path: &str, results: &mut Vec<serde_json::Value>) -> Result<()> {
    let paths: Vec<String> = results.iter().filter_map(|r| extract_path(r)).collect();
    if paths.is_empty() {
        return Ok(());
    }
    let snippets = fetch_snippets_for_paths(db_path, &paths).await?;
    for r in results.iter_mut() {
        if let Some(path) = extract_path(r) {
            if let Some(snippet) = snippets.get(&path) {
                let snippet_val = serde_json::Value::String(snippet.clone());
                if let Some(payload) = r.get_mut("payload") {
                    if payload.is_object() {
                        payload
                            .as_object_mut()
                            .unwrap()
                            .insert("snippet".into(), snippet_val.clone());
                    }
                }
                if r.get("payload").is_none() || !r.get("payload").unwrap().is_object() {
                    if let Some(obj) = r.as_object_mut() {
                        obj.insert("snippet".into(), snippet_val);
                    }
                }
            }
        }
    }
    Ok(())
}

fn filter_fields(mut results: Vec<serde_json::Value>, fields: &[String]) -> Vec<serde_json::Value> {
    if fields.is_empty() {
        return results;
    }
    let want: HashSet<String> = fields.iter().map(|s| s.to_lowercase()).collect();
    for r in results.iter_mut() {
        if let Some(obj) = r.as_object_mut() {
            let mut keep = serde_json::Map::new();
            for (k, v) in obj.iter() {
                let key_lower = k.to_lowercase();
                if want.contains(&key_lower) {
                    keep.insert(k.clone(), v.clone());
                }
            }
            *obj = keep;
        }
    }
    results
}

async fn keyword_search(
    db_path: &str,
    query: &str,
    path_prefix: Option<String>,
    mime: Option<String>,
    after: Option<&str>,
    before: Option<&str>,
    tags: Option<&[String]>,
) -> Result<Vec<serde_json::Value>> {
    let tags = tags.unwrap_or(&[]);
    let pool = storage::connect(db_path).await?;
    let mut qb = QueryBuilder::new("SELECT DISTINCT files.path, files.mime FROM files");
    if !tags.is_empty() {
        qb.push(" JOIN file_tags ft ON ft.file_id = files.id JOIN tags t ON t.id = ft.tag_id");
    }
    qb.push(" WHERE files.path LIKE ");
    qb.push_bind(format!("%{}%", query));
    if let Some(pref) = path_prefix {
        qb.push(" AND files.path LIKE ");
        qb.push_bind(format!("{}%", pref));
    }
    if let Some(m) = mime {
        qb.push(" AND files.mime = ");
        qb.push_bind(m);
    }
    if let Some(a) = after.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()) {
        qb.push(" AND files.mtime >= ");
        qb.push_bind(a.timestamp());
    }
    if let Some(b) = before.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()) {
        qb.push(" AND files.mtime <= ");
        qb.push_bind(b.timestamp());
    }
    if !tags.is_empty() {
        qb.push(" AND t.name IN (");
        let mut first = true;
        for tag in tags {
            if !first {
                qb.push(", ");
            }
            first = false;
            qb.push_bind(tag);
        }
        qb.push(")");
    }
    let rows = qb.build().fetch_all(&pool).await?;
    let mut results = Vec::new();
    for row in rows {
        let path: String = row.get(0);
        let mime: Option<String> = row.try_get(1).ok();
        results.push(serde_json::json!({ "path": path, "mime": mime }));
    }
    Ok(results)
}

async fn enrich_paths(
    db_path: &str,
    paths: &[String],
    path_prefix: Option<String>,
    mime: Option<String>,
    after: Option<&str>,
    before: Option<&str>,
) -> Result<Vec<serde_json::Value>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let pool = storage::connect(db_path).await?;
    let mut qb = QueryBuilder::new("SELECT path, mime, mtime FROM files WHERE path IN (");
    let mut separated = qb.separated(", ");
    for p in paths {
        separated.push_bind(p);
    }
    separated.push_unseparated(")");
    if let Some(pref) = path_prefix {
        qb.push(" AND path LIKE ");
        qb.push_bind(format!("{}%", pref));
    }
    if let Some(m) = mime {
        qb.push(" AND mime = ");
        qb.push_bind(m);
    }
    if let Some(a) = after.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()) {
        qb.push(" AND mtime >= ");
        qb.push_bind(a.timestamp());
    }
    if let Some(b) = before.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()) {
        qb.push(" AND mtime <= ");
        qb.push_bind(b.timestamp());
    }
    let rows = qb.build().fetch_all(&pool).await?;
    let mut results = Vec::new();
    for row in rows {
        let path: String = row.get(0);
        let mime_val: Option<String> = row.try_get(1).ok();
        results.push(serde_json::json!({ "path": path, "mime": mime_val }));
    }
    Ok(results)
}

async fn enrich_paths_for_index(
    db_path: &str,
    paths: &[String],
    tags: Option<&[String]>,
) -> Result<(Vec<(String, String)>, Vec<String>)> {
    if paths.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let pool = storage::connect(db_path).await?;
    let mut qb = QueryBuilder::new("SELECT path, mime FROM files WHERE path IN (");
    let mut separated = qb.separated(", ");
    for p in paths {
        separated.push_bind(p);
    }
    separated.push_unseparated(")");
    if let Some(tag_list) = tags {
        if !tag_list.is_empty() {
            qb.push(" AND EXISTS (SELECT 1 FROM file_tags ft JOIN tags t ON t.id = ft.tag_id WHERE ft.file_id = files.id AND t.name IN (");
            let mut sep = qb.separated(", ");
            for t in tag_list {
                sep.push_bind(t);
            }
            sep.push_unseparated("))");
        }
    }
    let rows = qb.build().fetch_all(&pool).await?;
    let mut docs = Vec::new();
    let mut found = std::collections::HashSet::new();
    for row in rows {
        let path: String = row.get(0);
        let mime: Option<String> = row.try_get(1).ok();
        let mime_text = mime.as_deref().unwrap_or("");
        docs.push((path.clone(), format!("{} {}", path, mime_text)));
        found.insert(path);
    }
    let missing: Vec<String> = paths
        .iter()
        .filter(|p| !found.contains(*p))
        .cloned()
        .collect();
    Ok((docs, missing))
}

async fn keyword_index_search(
    cfg: &AppConfig,
    query: &str,
    topk: u64,
    path_prefix: Option<String>,
    mime: Option<String>,
    after: Option<&str>,
    before: Option<&str>,
    tags: Option<&[String]>,
    refresh: bool,
) -> Result<Vec<serde_json::Value>> {
    if !cfg!(feature = "keyword-index") {
        return Ok(Vec::new());
    }
    let dir = keyword_index_dir(&cfg.database.path);
    let meta_exists = dir.join("meta.json").exists();
    if refresh || !meta_exists {
        let docs = keyword_search(&cfg.database.path, "", None, None, None, None, tags)
            .await?
            .into_iter()
            .filter_map(|v| {
                let path = v.get("path")?.as_str()?;
                // Use path + mime text as a minimal keyword corpus.
                let mime_text = v.get("mime").and_then(|m| m.as_str()).unwrap_or("");
                Some((path.to_string(), format!("{} {}", path, mime_text)))
            })
            .collect::<Vec<_>>();
        let _ = keyword_index::enabled::build_index(&dir, &docs);
    }
    let hits = keyword_index::enabled::search(&dir, query, topk as usize).unwrap_or_default();
    enrich_paths(&cfg.database.path, &hits, path_prefix, mime, after, before).await
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
    let (docs, missing) = enrich_paths_for_index(&cfg.database.path, &paths, tags).await?;
    if dir.join("meta.json").exists() {
        let _ = keyword_index::enabled::upsert_docs(&dir, &docs);
        if !missing.is_empty() {
            let _ = keyword_index::enabled::delete_docs(&dir, &missing);
        }
    } else {
        let _ = keyword_index::enabled::build_index(&dir, &docs);
    }
    let mut qb = QueryBuilder::new("DELETE FROM dirty WHERE path IN (");
    let mut separated = qb.separated(", ");
    for p in &paths {
        separated.push_bind(p);
    }
    separated.push_unseparated(")");
    let _ = qb.build().execute(&pool).await;
    Ok(())
}

fn build_qdrant_filter(
    path_prefix: Option<String>,
    mime: Option<String>,
    after: Option<&str>,
    before: Option<&str>,
) -> Option<serde_json::Value> {
    let mut must = Vec::new();
    if let Some(p) = path_prefix {
        must.push(serde_json::json!({
            "key": "path",
            "match": { "value": p }
        }));
    }
    if let Some(m) = mime {
        must.push(serde_json::json!({
            "key": "mime",
            "match": { "value": m }
        }));
    }
    if let Some(a) = after.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()) {
        must.push(serde_json::json!({
            "key": "mtime",
            "range": { "gte": a.timestamp() }
        }));
    }
    if let Some(b) = before.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()) {
        must.push(serde_json::json!({
            "key": "mtime",
            "range": { "lte": b.timestamp() }
        }));
    }
    if must.is_empty() {
        None
    } else {
        Some(serde_json::json!({ "must": must }))
    }
}

async fn run_apply(
    cfg: AppConfig,
    dry_run: bool,
    ids: Option<&str>,
    json: bool,
    summary: bool,
    verbose: bool,
    allow_override: Option<String>,
    deny_override: Option<String>,
    trash_override: Option<String>,
    conflict: String,
    fields: Option<Vec<String>>,
) -> Result<()> {
    let mut safety = cfg.safety.clone();
    if let Some(allow) = allow_override {
        safety.allow_paths = allow
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(deny) = deny_override {
        safety.deny_paths = deny
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(trash) = trash_override {
        safety.trash_dir = Some(trash);
    }

    let actions =
        apply::apply_actions(&cfg.database.path, dry_run, ids, &safety, &conflict).await?;
    let mut vals: Vec<serde_json::Value> = actions
        .iter()
        .filter_map(|a| serde_json::to_value(a).ok())
        .collect();
    let default_fields = vec![
        "id".to_string(),
        "path".to_string(),
        "kind".to_string(),
        "status".to_string(),
        "backup".to_string(),
        "error".to_string(),
    ];
    let filtered_fields = fields.clone().unwrap_or_else(|| default_fields.clone());
    vals = filter_fields(vals, &filtered_fields);
    if json {
        println!("{}", serde_json::to_string_pretty(&vals)?);
    } else if summary {
        let processed = vals.len();
        let executed = vals
            .iter()
            .filter(|v| v.get("status").and_then(|s| s.as_str()) == Some("executed"))
            .count();
        let failed = vals
            .iter()
            .filter(|v| v.get("status").and_then(|s| s.as_str()) == Some("error"))
            .count();
        println!(
            "apply summary: processed={}, executed={}, failed={}, dry_run={}",
            processed, executed, failed, dry_run
        );
    } else {
        println!("processed {} actions", vals.len());
        if verbose || fields.is_some() {
            for v in &vals {
                println!("{}", serde_json::to_string(v)?);
            }
        }
    }
    Ok(())
}

async fn run_actions(
    cfg: AppConfig,
    status: &str,
    rule: Option<&str>,
    kind: Option<&str>,
    has_backup: bool,
    duplicates_only: bool,
    tags: &[String],
    fields: &[String],
    json: bool,
) -> Result<()> {
    let pool = storage::connect(&cfg.database.path).await?;
    let mut query = QueryBuilder::new("SELECT actions.id, files.path, actions.kind, actions.payload_json, actions.status, actions.backup_path, GROUP_CONCAT(t.name, ',') as tags FROM actions JOIN files ON files.id = actions.file_id LEFT JOIN file_tags ft ON ft.file_id = files.id LEFT JOIN tags t ON t.id = ft.tag_id");
    query.push(" WHERE actions.status = ");
    query.push_bind(status);
    if let Some(r) = rule {
        query.push(" AND actions.payload_json LIKE ");
        query.push_bind(format!("%\"rule\":\"{}\"%", r));
    }
    if let Some(k) = kind {
        query.push(" AND actions.kind = ");
        query.push_bind(k);
    }
    if has_backup {
        query.push(" AND actions.backup_path IS NOT NULL AND actions.backup_path != ''");
    }
    if duplicates_only {
        query.push(" AND actions.payload_json LIKE '%\"duplicate_of\"%'");
    }
    if !tags.is_empty() {
        query.push(" AND t.name IN (");
        let mut separated = query.separated(", ");
        for tag in tags {
            separated.push_bind(tag);
        }
        separated.push_unseparated("");
        query.push(")");
    }
    query.push(" GROUP BY actions.id, files.path, actions.kind, actions.payload_json, actions.status, actions.backup_path ORDER BY actions.id");
    let rows = query.build().fetch_all(&pool).await?;
    let mut vals = Vec::new();
    for row in rows {
        let id: i64 = row.get(0);
        let path: String = row.get(1);
        let kind: String = row.get(2);
        let payload: String = row.get(3);
        let status: String = row.get(4);
        let backup: Option<String> = row.try_get(5).ok();
        let tags_col: Option<String> = row.try_get(6).ok();
        let duplicate_of = serde_json::from_str::<serde_json::Value>(&payload)
            .ok()
            .and_then(|v| {
                v.get("duplicate_of")
                    .and_then(|d| d.as_str())
                    .map(|s| s.to_string())
            });
        let tags_vec: Vec<String> = tags_col
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string())
            .collect();
        let rule = serde_json::from_str::<serde_json::Value>(&payload)
            .ok()
            .and_then(|v| {
                v.get("rule")
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string())
            });
        vals.push(serde_json::json!({
            "id": id,
            "path": path,
            "kind": kind,
            "payload": payload,
            "status": status,
            "rule": rule,
            "backup_path": backup,
            "duplicate_of": duplicate_of,
            "tags": tags_vec,
        }));
    }
    let filtered_fields = if fields.is_empty() {
        vec![
            "id".to_string(),
            "path".to_string(),
            "kind".to_string(),
            "status".to_string(),
            "rule".to_string(),
            "backup_path".to_string(),
            "duplicate_of".to_string(),
            "tags".to_string(),
            "snippet".to_string(),
        ]
    } else {
        fields.iter().cloned().collect()
    };
    if json {
        let mut enriched = vals;
        attach_snippets(&cfg.database.path, &mut enriched).await?;
        let filtered = filter_fields(enriched, &filtered_fields);
        println!("{}", serde_json::to_string_pretty(&filtered)?);
    } else {
        let mut enriched = vals;
        attach_snippets(&cfg.database.path, &mut enriched).await?;
        let filtered = filter_fields(enriched, &filtered_fields);
        for v in &filtered {
            println!("{}", serde_json::to_string(v)?);
        }
    }
    Ok(())
}

async fn fetch_actions(db_path: &str) -> Result<Vec<serde_json::Value>> {
    let pool = storage::connect(db_path).await?;
    let rows = sqlx::query("SELECT actions.id, files.path, actions.kind, actions.payload_json, actions.status, actions.backup_path, GROUP_CONCAT(t.name, ',') as tags FROM actions JOIN files ON files.id = actions.file_id LEFT JOIN file_tags ft ON ft.file_id = files.id LEFT JOIN tags t ON t.id = ft.tag_id GROUP BY actions.id, files.path, actions.kind, actions.payload_json, actions.status, actions.backup_path ORDER BY actions.id")
        .fetch_all(&pool)
        .await?;
    let mut vals = Vec::new();
    for row in rows {
        let id: i64 = row.get(0);
        let path: String = row.get(1);
        let kind: String = row.get(2);
        let payload: String = row.get(3);
        let status: String = row.get(4);
        let backup: Option<String> = row.try_get(5).ok();
        let tags_col: Option<String> = row.try_get(6).ok();
        let tags_vec: Vec<String> = tags_col
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string())
            .collect();
        let rule = serde_json::from_str::<serde_json::Value>(&payload)
            .ok()
            .and_then(|v| {
                v.get("rule")
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string())
            });
        vals.push(serde_json::json!({
            "id": id,
            "path": path,
            "kind": kind,
            "payload": payload,
            "status": status,
            "rule": rule,
            "backup_path": backup,
            "tags": tags_vec,
        }));
    }
    Ok(vals)
}

async fn run_undo(cfg: AppConfig, ids: Option<&str>, backup_override: Option<&str>) -> Result<()> {
    undo::undo_actions(&cfg.database.path, ids, backup_override).await?;
    Ok(())
}
