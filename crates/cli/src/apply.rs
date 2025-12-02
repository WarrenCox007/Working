use crate::fs_apply;
use crate::paths;
use anyhow::Result;
use organizer_core::config::SafetyConfig;
use serde::Serialize;
use serde_json::Value;
use sqlx::Row;
use std::path::PathBuf;
use storage;

#[derive(Debug, Serialize)]
pub struct ActionView {
    pub id: i64,
    pub path: String,
    pub kind: String,
    pub payload: String,
    pub status: String,
    pub rule: Option<String>,
    pub error: Option<String>,
    pub backup: Option<String>,
}

fn extract_dest(payload: &str) -> Option<String> {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|v| v.get("to").and_then(|t| t.as_str()).map(|s| s.to_string()))
}

pub async fn apply_actions(
    db_path: &str,
    dry_run: bool,
    ids: Option<&str>,
    safety: &SafetyConfig,
    conflict: &str,
) -> Result<Vec<ActionView>> {
    let pool = storage::connect(db_path).await?;
    let rows = if let Some(id_list) = ids {
        let placeholders: Vec<String> = id_list.split(',').map(|_| "?".into()).collect();
        let sql = format!(
            "SELECT actions.id, files.path, actions.kind, actions.payload_json, actions.status, actions.backup_path FROM actions JOIN files ON files.id = actions.file_id WHERE actions.status = 'planned' AND actions.id IN ({})",
            placeholders.join(",")
        );
        let mut query = sqlx::query(&sql);
        for id in id_list.split(',') {
            query = query.bind(id.trim());
        }
        query.fetch_all(&pool).await?
    } else {
        sqlx::query("SELECT actions.id, files.path, actions.kind, actions.payload_json, actions.status, actions.backup_path FROM actions JOIN files ON files.id = actions.file_id WHERE actions.status = 'planned'")
            .fetch_all(&pool)
            .await?
    };

    let mut views = Vec::new();

    let mut success = 0usize;
    let mut failed = 0usize;

    for row in rows {
        let id: i64 = row.get(0);
        let path: String = row.get(1);
        let kind: String = row.get(2);
        let payload: String = row.get(3);
        let mut status: String = row.get(4);
        let mut error = None;
        let mut backup_path: Option<String> = None;
        let rule = extract_rule(&payload);
        let mut dirty_paths: Vec<String> = vec![path.clone()];
        if let Some(dest) = extract_dest(&payload) {
            dirty_paths.push(dest);
        }

        if !dry_run {
            // Allow/deny enforcement
            if !paths::is_allowed(
                std::path::Path::new(&path),
                &safety.allow_paths,
                &safety.deny_paths,
            ) {
                error = Some("path denied".to_string());
                sqlx::query("UPDATE actions SET status='error' WHERE id = ?1")
                    .bind(id)
                    .execute(&pool)
                    .await?;
                views.push(ActionView {
                    id,
                    path,
                    kind,
                    payload,
                    status,
                    rule,
                    error,
                    backup: backup_path.clone(),
                });
                continue;
            }
            let action = fs_apply::parse_action(&path, &kind, &payload);
            match kind.as_str() {
                "tag" => {
                    // Persist tag into tags/file_tags tables.
                    if let Some(tag) = extract_tag(&payload) {
                        sqlx::query("INSERT OR IGNORE INTO tags(name) VALUES (?1)")
                            .bind(&tag)
                            .execute(&pool)
                            .await?;
                        sqlx::query("INSERT OR IGNORE INTO file_tags(file_id, tag_id, confidence, source) VALUES ((SELECT id FROM files WHERE path = ?1),(SELECT id FROM tags WHERE name = ?2),1.0,'apply')")
                            .bind(&path)
                            .bind(&tag)
                            .execute(&pool)
                            .await?;
                        sqlx::query("UPDATE actions SET status='executed', executed_at=strftime('%s','now') WHERE id = ?1")
                                .bind(id)
                                .execute(&pool)
                                .await?;
                        status = "executed".to_string();
                    } else {
                        error = Some("invalid tag payload".to_string());
                        sqlx::query("UPDATE actions SET status='error' WHERE id = ?1")
                            .bind(id)
                            .execute(&pool)
                            .await?;
                    }
                }
                _ => {
                    let trash_dir = safety.trash_dir.as_ref().map(|p| PathBuf::from(p));
                    match fs_apply::apply_action(
                        action,
                        trash_dir.as_deref(),
                        safety.copy_then_delete,
                        conflict,
                    ) {
                        Ok(bp) => {
                            if let Some(td) = &trash_dir {
                                backup_path = bp
                                    .map(|p| p.to_string_lossy().into_owned())
                                    .or_else(|| Some(td.to_string_lossy().into_owned()));
                            } else {
                                backup_path = bp.map(|p| p.to_string_lossy().into_owned());
                            }
                            sqlx::query("UPDATE actions SET status='executed', executed_at=strftime('%s','now'), backup_path=?2 WHERE id = ?1")
                                .bind(id)
                                .bind(backup_path.clone())
                                .execute(&pool)
                                .await?;
                            status = "executed".to_string();
                            success += 1;
                        }
                        Err(e) => {
                            error = Some(e.to_string());
                            sqlx::query("UPDATE actions SET status='error' WHERE id = ?1")
                                .bind(id)
                                .execute(&pool)
                                .await?;
                            failed += 1;
                        }
                    }
                }
            }
            // mark dirty for downstream indexing
            for d in dirty_paths {
                let _ = sqlx::query("INSERT OR REPLACE INTO dirty(path, reason, updated_at) VALUES (?1,'apply', strftime('%s','now'))")
                    .bind(d)
                    .execute(&pool)
                    .await;
            }
        }

        views.push(ActionView {
            id,
            path,
            kind,
            payload,
            status,
            rule,
            error,
            backup: backup_path,
        });
    }

    if !dry_run {
        println!("apply summary: success={}, failed={}", success, failed);
    } else {
        println!("dry-run: {} actions listed", views.len());
    }

    Ok(views)
}

fn extract_tag(payload: &str) -> Option<String> {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|v| v.get("tag").and_then(|t| t.as_str()).map(|s| s.to_string()))
}

fn extract_rule(payload: &str) -> Option<String> {
    serde_json::from_str::<Value>(payload).ok().and_then(|v| {
        v.get("rule")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string())
    })
}
