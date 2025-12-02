use anyhow::Result;
use sqlx::Row;
use std::fs;
use std::path::PathBuf;
use storage;

pub async fn undo_actions(
    db_path: &str,
    ids: Option<&str>,
    backup_override: Option<&str>,
) -> Result<()> {
    let pool = storage::connect(db_path).await?;
    let rows = if let Some(id_list) = ids {
        let placeholders: Vec<String> = id_list.split(',').map(|_| "?".into()).collect();
        let sql = format!(
            "SELECT actions.id, files.path, actions.kind, actions.payload_json, actions.status, actions.executed_at, actions.backup_path FROM actions JOIN files ON files.id = actions.file_id WHERE actions.status = 'executed' AND actions.id IN ({})",
            placeholders.join(",")
        );
        let mut query = sqlx::query(&sql);
        for id in id_list.split(',') {
            query = query.bind(id.trim());
        }
        query.fetch_all(&pool).await?
    } else {
        sqlx::query("SELECT actions.id, files.path, actions.kind, actions.payload_json, actions.status, actions.executed_at, actions.backup_path FROM actions JOIN files ON files.id = actions.file_id WHERE actions.status = 'executed'")
            .fetch_all(&pool)
            .await?
    };

    for row in rows {
        let id: i64 = row.get(0);
        let path: String = row.get(1);
        let payload: String = row.get(3);
        let backup_col: Option<String> = row.try_get(6).ok();
        let mut restored = false;
        if let Some(backup) = backup_override.map(|s| s.to_string()).or_else(|| {
            backup_col
                .or_else(|| extract_backup(&payload).map(|p| p.to_string_lossy().into_owned()))
        }) {
            let dest = PathBuf::from(&path);
            if dest.exists() {
                continue; // skip to avoid overwrite
            }
            if let Some(parent) = dest.parent() {
                let _ = fs::create_dir_all(parent);
            }
            if fs::copy(&backup, &dest).is_ok() {
                restored = true;
            }
        }
        if restored {
            sqlx::query("UPDATE actions SET status='planned', executed_at=NULL WHERE id = ?1")
                .bind(id)
                .execute(&pool)
                .await?;
            let _ = sqlx::query("INSERT OR REPLACE INTO dirty(path, reason, updated_at) VALUES (?1,'undo', strftime('%s','now'))")
                .bind(path)
                .execute(&pool)
                .await;
        }
    }

    Ok(())
}

fn extract_backup(payload: &str) -> Option<PathBuf> {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|v| v.get("backup").and_then(|b| b.as_str()).map(PathBuf::from))
}
