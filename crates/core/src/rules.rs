use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    PathPrefix { prefix: String },
    Mime { mime: String },
    Extension { ext: String },
    Tag { tag: String },
    And { all: Vec<Condition> },
    Or { any: Vec<Condition> },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    Move { to: String },
    Tag { tag: String },
    Rename { template: String },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Rule {
    pub name: String,
    pub priority: i32,
    pub enabled: bool,
    pub condition: Condition,
    pub actions: Vec<Action>,
}

#[derive(Debug, Clone)]
pub struct RuleContext<'a> {
    pub path: &'a Path,
    pub mime: Option<&'a str>,
    pub ext: Option<&'a str>,
    pub tags: &'a [String],
}

pub fn matches(condition: &Condition, ctx: &RuleContext<'_>) -> bool {
    match condition {
        Condition::PathPrefix { prefix } => ctx
            .path
            .to_str()
            .map(|p| p.starts_with(prefix))
            .unwrap_or(false),
        Condition::Mime { mime } => ctx.mime.map(|m| m == mime).unwrap_or(false),
        Condition::Extension { ext } => ctx.ext.map(|e| e == ext).unwrap_or(false),
        Condition::Tag { tag } => ctx.tags.iter().any(|t| t == tag),
        Condition::And { all } => all.iter().all(|c| matches(c, ctx)),
        Condition::Or { any } => any.iter().any(|c| matches(c, ctx)),
    }
}

pub fn evaluate<'a>(rules: &'a [Rule], ctx: &RuleContext<'a>) -> Vec<&'a Rule> {
    let mut matched: Vec<&Rule> = rules
        .iter()
        .filter(|r| r.enabled && matches(&r.condition, ctx))
        .collect();
    matched.sort_by_key(|r| r.priority);
    matched
}

pub fn apply_actions(rule: &Rule, _ctx: &RuleContext<'_>) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for action in &rule.actions {
        match action {
            Action::Move { to } => {
                result.insert("move".to_string(), to.clone());
            }
            Action::Tag { tag } => {
                result.insert("tag".to_string(), tag.clone());
            }
            Action::Rename { template } => {
                result.insert("rename".to_string(), template.clone());
            }
        }
    }
    result
}

pub fn load_rules_from_dir(dir: &Path) -> anyhow::Result<Vec<Rule>> {
    let mut rules = Vec::new();
    if !dir.exists() {
        return Ok(rules);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            let content = fs::read_to_string(entry.path())?;
            if entry.path().extension().and_then(|e| e.to_str()) == Some("toml") {
                let rule: Rule = toml::from_str(&content)?;
                rules.push(rule);
            }
        }
    }
    Ok(rules)
}
