//! Storage layer: SQLite schemas and helpers.
//!
//! Holds DB pool setup and migration runner.

use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;

pub mod models {
    use serde::{Deserialize, Serialize};
    use sqlx::FromRow;

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct File {
        pub id: i64,
        pub path: String,
        pub size: i64,
        pub mtime: i64,
        pub ctime: i64,
        pub hash: Option<String>,
        pub fast_hash: Option<String>,
        pub full_hash: Option<String>,
        pub mime: Option<String>,
        pub ext: Option<String>,
        pub status: String,
        pub first_seen: i64,
        pub last_seen: i64,
    }

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct Metadata {
        pub id: i64,
        pub file_id: i64,
        pub key: String,
        pub value: Option<String>,
        pub source: Option<String>,
        pub updated_at: i64,
    }

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct Chunk {
        pub id: i64,
        pub file_id: i64,
        pub hash: String,
        pub start: i64,
        pub end: i64,
        pub text_preview: Option<String>,
    }

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct Tag {
        pub id: i64,
        pub name: String,
    }

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct FileTag {
        pub file_id: i64,
        pub tag_id: i64,
        pub confidence: f64,
        pub source: Option<String>,
        pub updated_at: i64,
    }

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct Rule {
        pub id: i64,
        pub name: String,
        pub condition_json: String,
        pub action_json: String,
        pub priority: i64,
        pub enabled: i64,
    }

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct Action {
        pub id: i64,
        pub file_id: i64,
        pub kind: String,
        pub payload_json: String,
        pub status: String,
        pub created_at: i64,
        pub executed_at: Option<i64>,
        pub undo_token: Option<String>,
        pub backup_path: Option<String>,
    }

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct Audit {
        pub id: i64,
        pub action_id: Option<i64>,
        pub event: String,
        pub detail: Option<String>,
        pub created_at: i64,
    }

    #[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
    pub struct Dirty {
        pub path: String,
        pub reason: Option<String>,
        pub updated_at: i64,
    }
}

pub async fn connect(database_url: &str) -> anyhow::Result<SqlitePool> {
    let mut url = database_url.to_string();
    if !database_url.starts_with("sqlite:") {
        let path = std::path::PathBuf::from(database_url);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let norm = path.to_string_lossy().replace('\\', "/");
        if path.is_absolute() {
            url = format!("sqlite:///{}", norm.trim_start_matches('/'));
        } else {
            url = format!("sqlite://{}", norm);
        }
    }
    let mut opts = SqlitePoolOptions::new();
    if url.contains("memory") {
        opts = opts.max_connections(1);
    } else {
        opts = opts.max_connections(5);
    }
    let pool = opts.connect(&url).await?;
    Ok(pool)
}

pub async fn migrate(pool: &SqlitePool) -> anyhow::Result<()> {
    // Applies SQLx migrations located in crates/storage/migrations.
    // Safe to run multiple times (idempotent).
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}
