use sqlx::{Row, SqlitePool};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct IndexRecord {
    pub path: PathBuf,
    pub size: u64,
    pub mtime: i64,
    pub hash: Option<String>,
    pub mime: Option<String>,
    pub ext: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ChunkRecord {
    pub file_id: String,
    pub start: i64,
    pub end: i64,
    pub text_preview: Option<String>,
    pub hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ActionRecordDb {
    pub file_path: String,
    pub kind: String,
    pub payload: serde_json::Value,
    pub rule: Option<String>,
    pub backup: Option<String>,
}

pub struct Indexer {
    pool: SqlitePool,
}

impl Indexer {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn upsert(&self, record: IndexRecord) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO files (path, size, mtime, ctime, hash, mime, ext, status, first_seen, last_seen)
            VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?6, 'seen', strftime('%s','now'), strftime('%s','now'))
            ON CONFLICT(path) DO UPDATE SET
                size=excluded.size,
                mtime=excluded.mtime,
                hash=excluded.hash,
                mime=excluded.mime,
                ext=excluded.ext,
                last_seen=strftime('%s','now')
            "#,
        )
        .bind(record.path.to_string_lossy())
        .bind(record.size as i64)
        .bind(record.mtime)
        .bind(record.hash)
        .bind(record.mime)
        .bind(record.ext)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_chunk(&self, file_path: &str, chunk: ChunkRecord) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO chunks (file_id, hash, start, end, text_preview)
            VALUES ((SELECT id FROM files WHERE path = ?1), ?2, ?3, ?4, ?5)
            "#,
        )
        .bind(file_path)
        .bind(chunk.hash)
        .bind(chunk.start)
        .bind(chunk.end)
        .bind(chunk.text_preview)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn detect_duplicate_for_hash(
        &self,
        current_path: &str,
        hash: &str,
    ) -> anyhow::Result<Option<String>> {
        let row = sqlx::query(
            r#"
            SELECT path FROM files WHERE hash = ?1 AND path != ?2 LIMIT 1
            "#,
        )
        .bind(hash)
        .bind(current_path)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    pub async fn insert_action(
        &self,
        action: crate::suggester::ActionRecord,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO actions (file_id, kind, payload_json, status, undo_token, backup_path)
            VALUES ((SELECT id FROM files WHERE path = ?1), ?2, ?3, 'planned', ?4, NULL)
            "#,
        )
        .bind(action.file_path)
        .bind(action.kind)
        .bind(action.payload.to_string())
        .bind(action.rule.unwrap_or_default())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_action_status(&self, action_id: i64, status: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE actions
            SET status = ?2, executed_at = CASE WHEN ?2='executed' THEN strftime('%s','now') ELSE executed_at END
            WHERE id = ?1
            "#,
        )
        .bind(action_id)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_metadata(
        &self,
        file_path: &str,
        meta: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        for (k, v) in meta {
            sqlx::query(
                r#"
                INSERT INTO metadata (file_id, key, value, source)
                VALUES ((SELECT id FROM files WHERE path = ?1), ?2, ?3, 'extract')
                ON CONFLICT(file_id, key) DO UPDATE SET value=excluded.value, updated_at=strftime('%s','now')
                "#,
            )
            .bind(file_path)
            .bind(k)
            .bind(v)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }
}
