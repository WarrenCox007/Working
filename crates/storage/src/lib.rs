//! Storage layer: SQLite schemas and helpers.
//!
//! Holds DB pool setup and migration runner.

use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;

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
