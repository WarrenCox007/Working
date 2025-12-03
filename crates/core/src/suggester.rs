use storage::models::{File, Rule as DbRule};
use crate::rules::{self, Rule, RuleContext};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::SqlitePool;
use std::path::PathBuf;

#[derive(Debug, FromRow)]
struct FileWithTags {
    id: i64,
    path: String,
    mime: Option<String>,
    ext: Option<String>,
    tags: Option<String>,
}

pub async fn run_suggester(pool: &SqlitePool) -> anyhow::Result<()> {
    // 1. Load enabled rules from the database
    let db_rules = sqlx::query_as::<_, DbRule>("SELECT * FROM rules WHERE enabled = 1")
        .fetch_all(pool)
        .await?;

    let rules: Vec<Rule> = db_rules
        .into_iter()
        .filter_map(|r| {
            let condition = serde_json::from_str(&r.condition_json).ok()?;
            let actions = serde_json::from_str(&r.action_json).ok()?;
            Some(Rule {
                name: r.name,
                priority: r.priority as i32,
                enabled: r.enabled > 0,
                condition,
                actions,
            })
        })
        .collect();

    // 2. Fetch files that don't have a planned 'move' or 'rename' action
    let files_to_process = sqlx::query_as::<_, FileWithTags>(
        r#"
        SELECT f.id, f.path, f.mime, f.ext, GROUP_CONCAT(t.name) as tags
        FROM files f
        LEFT JOIN file_tags ft ON f.id = ft.file_id
        LEFT JOIN tags t ON ft.tag_id = t.id
        WHERE f.id NOT IN (
            SELECT file_id FROM actions WHERE status = 'planned' AND (kind = 'move' OR kind = 'rename')
        )
        GROUP BY f.id
        "#,
    )
    .fetch_all(pool)
    .await?;

    for file in files_to_process {
        let path = PathBuf::from(&file.path);
        let tags: Vec<String> = file
            .tags
            .map(|s| s.split(',').map(String::from).collect())
            .unwrap_or_default();

        let ctx = RuleContext {
            path: &path,
            mime: file.mime.as_deref(),
            ext: file.ext.as_deref(),
            tags: &tags,
        };

        // 3. Evaluate rules and get suggestions
        for matched_rule in rules::evaluate(&rules, &ctx) {
            for action in &matched_rule.actions {
                let (kind, payload) = match action {
                    rules::Action::Move { to } => ("move", serde_json::json!({ "to": to })),
                    rules::Action::Tag { tag } => ("tag", serde_json::json!({ "tag": tag })),
                    rules::Action::Rename { template } => {
                        ("rename", serde_json::json!({ "to": template }))
                    }
                };

                // 4. Store action in DB
                sqlx::query!(
                    r#"
                    INSERT INTO actions (file_id, kind, payload_json, status)
                    VALUES (?, ?, ?, 'planned')
                    "#,
                    file.id,
                    kind,
                    payload.to_string()
                )
                .execute(pool)
                .await?;
            }
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SuggestedAction {
    Move { from: PathBuf, to: PathBuf },
    Tag { path: PathBuf, tag: String },
    Rename { from: PathBuf, to: PathBuf },
    Dedupe { path: PathBuf, duplicate_of: String },
    MergeDuplicate { path: PathBuf, duplicate_of: String },
    Noop,
}

#[derive(Debug, Serialize)]
pub struct ActionRecord {
    pub file_path: String,
    pub kind: String,
    pub payload: serde_json::Value,
    pub rule: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ApplyOutcome {
    Executed,
    Skipped(String),
}
