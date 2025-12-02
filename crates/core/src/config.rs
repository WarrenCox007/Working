use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub scan: ScanPaths,
    pub embeddings: EmbeddingConfig,
    pub vectors: VectorConfig,
    pub classification: ClassificationConfig,
    pub safety: SafetyConfig,
    pub rules: RuleConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPaths {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub hash_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    pub provider: String,
    pub model: String,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorConfig {
    pub provider: String,
    pub url: Option<String>,
    pub collection: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationConfig {
    pub thresholds: Thresholds,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thresholds {
    pub accept: f32,
    pub review: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyConfig {
    pub dry_run: bool,
    pub allow_delete: bool,
    #[serde(default)]
    pub allow_paths: Vec<String>,
    #[serde(default)]
    pub deny_paths: Vec<String>,
    #[serde(default)]
    pub trash_dir: Option<String>,
    #[serde(default)]
    pub copy_then_delete: bool,
    #[serde(default)]
    pub immediate_vector_delete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleConfig {
    pub path: Option<String>,
}

pub fn load(path: Option<&str>) -> anyhow::Result<AppConfig> {
    let mut settings = config::Config::builder();
    if let Some(p) = path {
        settings = settings.add_source(config::File::with_name(p));
    } else {
        settings = settings.add_source(config::File::with_name("config/default").required(false));
    }
    let cfg = settings.build()?;
    Ok(cfg.try_deserialize()?)
}
