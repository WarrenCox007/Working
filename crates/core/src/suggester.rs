use crate::rules::{self, Rule, RuleContext};
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum SuggestedAction {
    Move { from: PathBuf, to: PathBuf },
    Tag { path: PathBuf, tag: String },
    Rename { from: PathBuf, to: PathBuf },
    Dedupe { path: PathBuf, duplicate_of: String },
    Noop,
}

pub fn suggest(_path: &PathBuf) -> Vec<SuggestedAction> {
    vec![SuggestedAction::Noop]
}

pub fn suggest_with_rules(
    path: &PathBuf,
    mime: Option<&str>,
    ext: Option<&str>,
    tags: &[String],
    ruleset: &[Rule],
) -> Vec<(SuggestedAction, Option<String>)> {
    let ctx = RuleContext {
        path,
        mime,
        ext,
        tags,
    };
    let mut actions = Vec::new();
    for rule in rules::evaluate(ruleset, &ctx) {
        for action in &rule.actions {
            match action {
                rules::Action::Move { to } => actions.push((
                    SuggestedAction::Move {
                        from: path.clone(),
                        to: PathBuf::from(to),
                    },
                    Some(rule.name.clone()),
                )),
                rules::Action::Tag { tag } => actions.push((
                    SuggestedAction::Tag {
                        path: path.clone(),
                        tag: tag.clone(),
                    },
                    Some(rule.name.clone()),
                )),
                rules::Action::Rename { template } => actions.push((
                    SuggestedAction::Rename {
                        from: path.clone(),
                        to: PathBuf::from(template),
                    },
                    Some(rule.name.clone()),
                )),
            }
        }
    }
    if actions.is_empty() {
        actions.push((SuggestedAction::Noop, None));
    }
    actions
}

#[derive(Debug, Serialize)]
pub struct ActionRecord {
    pub file_path: String,
    pub kind: String,
    pub payload: serde_json::Value,
    pub rule: Option<String>,
}

impl From<SuggestedAction> for ActionRecord {
    fn from(value: SuggestedAction) -> Self {
        match value {
            SuggestedAction::Move { from, to } => ActionRecord {
                file_path: from.to_string_lossy().into_owned(),
                kind: "move".into(),
                payload: serde_json::json!({ "to": to }),
                rule: None,
            },
            SuggestedAction::Tag { path, tag } => ActionRecord {
                file_path: path.to_string_lossy().into_owned(),
                kind: "tag".into(),
                payload: serde_json::json!({ "tag": tag }),
                rule: None,
            },
            SuggestedAction::Rename { from, to } => ActionRecord {
                file_path: from.to_string_lossy().into_owned(),
                kind: "rename".into(),
                payload: serde_json::json!({ "to": to }),
                rule: None,
            },
            SuggestedAction::Dedupe { path, duplicate_of } => ActionRecord {
                file_path: path.to_string_lossy().into_owned(),
                kind: "dedupe".into(),
                payload: serde_json::json!({ "duplicate_of": duplicate_of }),
                rule: None,
            },
            SuggestedAction::Noop => ActionRecord {
                file_path: String::new(),
                kind: "noop".into(),
                payload: serde_json::json!({}),
                rule: None,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ApplyOutcome {
    Executed,
    Skipped(String),
}
