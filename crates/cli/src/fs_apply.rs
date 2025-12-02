use anyhow::Result;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub enum ActionKind {
    Move {
        from: PathBuf,
        to: PathBuf,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
    },
    Tag {
        _path: PathBuf,
        _tag: String,
    },
    Dedupe {
        _path: PathBuf,
        _duplicate_of: String,
    },
    MergeDuplicate {
        from: PathBuf,
        target: PathBuf,
        strategy: String,
    },
    Unsupported,
}

pub fn parse_action(path: &str, kind: &str, payload: &str) -> ActionKind {
    let from = PathBuf::from(path);
    let parsed: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
    match kind {
        "move" => {
            let to = parsed
                .get("to")
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| from.clone());
            ActionKind::Move { from, to }
        }
        "rename" => {
            let to = parsed
                .get("to")
                .and_then(|v| v.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| from.clone());
            ActionKind::Rename { from, to }
        }
        "tag" => {
            let tag = parsed
                .get("tag")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            ActionKind::Tag {
                _path: from,
                _tag: tag,
            }
        }
        "dedupe" => {
            let dupe = parsed
                .get("duplicate_of")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            ActionKind::Dedupe {
                _path: from,
                _duplicate_of: dupe,
            }
        }
        "merge_duplicate" => {
            let dupe = parsed
                .get("duplicate_of")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let strategy = parsed
                .get("strategy")
                .and_then(|v| v.as_str())
                .unwrap_or("trash")
                .to_string();
            ActionKind::MergeDuplicate {
                from: from.clone(),
                target: PathBuf::from(dupe),
                strategy,
            }
        }
        _ => ActionKind::Unsupported,
    }
}

pub fn apply_action(
    action: ActionKind,
    trash_dir: Option<&Path>,
    copy_then_delete: bool,
    conflict_policy: &str,
) -> Result<Option<PathBuf>> {
    match action {
        ActionKind::Move { from, to } | ActionKind::Rename { from, to } => {
            let target = if to.exists() {
                match conflict_policy {
                    "skip" => return Ok(None),
                    "overwrite" => to,
                    _ => resolve_conflict(&to)?,
                }
            } else {
                to
            };
            let backup = apply_move(from, target, trash_dir, copy_then_delete)?;
            Ok(backup)
        }
        ActionKind::Tag { .. } => Ok(None),
        ActionKind::Dedupe { .. } => Ok(None),
        ActionKind::MergeDuplicate {
            from,
            target,
            strategy,
        } => match strategy.as_str() {
            "replace" => apply_action(
                ActionKind::Move { from, to: target },
                trash_dir,
                copy_then_delete,
                "overwrite",
            ),
            _ => {
                if let Some(trash) = trash_dir {
                    let backup = backup_to_trash(&from, trash)?;
                    let _ = fs::remove_file(&from);
                    Ok(Some(backup))
                } else {
                    let _ = fs::remove_file(&from);
                    Ok(None)
                }
            }
        },
        ActionKind::Unsupported => Ok(None),
    }
}

fn backup_to_trash(src: &Path, trash_dir: &Path) -> Result<PathBuf> {
    fs::create_dir_all(trash_dir)?;
    let file_name = src
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_else(|| "backup".into());
    let mut candidate = trash_dir.join(&file_name);
    if candidate.exists() {
        let stem = candidate
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("backup")
            .to_string();
        let ext = candidate
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_string();
        let mut counter = 1;
        loop {
            let name = if ext.is_empty() {
                format!("{}_{}", stem, counter)
            } else {
                format!("{}_{}.{}", stem, counter, ext)
            };
            candidate = trash_dir.join(name);
            if !candidate.exists() {
                break;
            }
            counter += 1;
        }
    }
    let _ = fs::copy(src, &candidate);
    Ok(candidate)
}

fn resolve_conflict(dest: &Path) -> Result<PathBuf> {
    let mut candidate = dest.to_path_buf();
    let stem = candidate
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file")
        .to_string();
    let ext = candidate
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_string();
    let mut counter = 1;
    loop {
        let name = if ext.is_empty() {
            format!("{}_{}", stem, counter)
        } else {
            format!("{}_{}.{}", stem, counter, ext)
        };
        candidate = candidate
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(name);
        if !candidate.exists() {
            return Ok(candidate);
        }
        counter += 1;
    }
}

fn apply_move(
    from: PathBuf,
    to: PathBuf,
    trash_dir: Option<&Path>,
    copy_then_delete: bool,
) -> Result<Option<PathBuf>> {
    let mut backup_path = None;
    if let Some(trash) = trash_dir {
        backup_path = Some(backup_to_trash(&from, trash)?);
    }
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }
    if copy_then_delete {
        fs::copy(&from, &to)?;
        fs::remove_file(&from)?;
    } else {
        fs::rename(&from, &to)?;
    }
    Ok(backup_path)
}
