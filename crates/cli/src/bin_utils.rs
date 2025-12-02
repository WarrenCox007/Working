use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AllowDeny {
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub deny: Vec<String>,
}
