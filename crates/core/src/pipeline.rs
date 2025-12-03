use crate::config::AppConfig;
use crate::embeddings::EmbeddingResult;
use crate::{
    classifier, embeddings, extractor, indexer, scanner, suggester,
    vectorstore::{self, VectorStore},
};
use anyhow::Context;
use providers::lmstudio::{LmStudioConfig, LmStudioProvider};
use providers::noop::NoopProvider;
use providers::openai::{OpenAiConfig, OpenAiProvider};
use providers::qdrant::QdrantClient;
use providers::ProviderRegistry;
use std::sync::Arc;
use storage::{connect, migrate};
use tracing::{debug, info, warn};

pub enum PipelineMode {
    Scan,
    Classify,
    Suggest,
    All,
}

pub struct PipelineSummary {
    pub discovered: usize,
    pub processed_files: usize,
    pub embedded_chunks: usize,
}

pub async fn run_with_mode(config: AppConfig, mode: PipelineMode) -> anyhow::Result<()> {
    let _ = run_with_mode_summary(config, mode).await?;
    Ok(())
}

pub async fn run_with_mode_summary(
    config: AppConfig,
    mode: PipelineMode,
) -> anyhow::Result<PipelineSummary> {
    // Setup DB
    let pool = connect(&config.database.path).await.context("db connect")?;
    migrate(&pool).await.context("db migrate")?;

    // Setup providers
    let registry = build_registry(&config);
    let vector_store = build_vector_store(&config);
    let qdrant_client = vector_store.downcast_qdrant();

    let mut summary = PipelineSummary::default();

    if matches!(mode, PipelineMode::Scan | PipelineMode::All) {
        info!("Starting scan phase...");
        let roots: Vec<std::path::PathBuf> =
            config.scan.include.iter().map(std::path::PathBuf::from).collect();
        let hash_mode = scanner::HashMode::from(config.scan.hash_mode.as_deref().unwrap_or(""));
        summary.discovered =
            scanner::scan(&roots, &config.scan.exclude, &hash_mode, &pool).await? as usize;
        info!("Scan complete. Discovered {} files.", summary.discovered);
    }

    if matches!(
        mode,
        PipelineMode::Classify | PipelineMode::Suggest | PipelineMode::All
    ) {
        info!("Starting extraction phase...");
        extractor::run_extractor(&pool).await?;
        info!("Extraction complete.");

        if let Some(qdrant) = &qdrant_client {
            info!("Starting embedding phase...");
            embeddings::run_embedder(&pool, &registry, qdrant, config.embeddings.batch_size)
                .await?;
            info!("Embedding complete.");
        }

        info!("Starting classification phase...");
        if let Some(qdrant) = &qdrant_client {
            classifier::run_classifier(&pool, &registry, qdrant).await?;
        } else {
            warn!("Vector DB not configured, skipping kNN classification.");
            classifier::run_classifier_no_knn(&pool, &registry).await?;
        }
        info!("Classification complete.");
    }

    if matches!(mode, PipelineMode::Suggest | PipelineMode::All) {
        info!("Starting suggestion phase...");
        if let Some(rules_path) = &config.rules.path {
            if let Ok(rules) =
                crate::rules::load_rules_from_dir(&std::path::PathBuf::from(rules_path))
            {
                for rule in rules {
                    let condition_json = serde_json::to_string(&rule.condition).unwrap();
                    let action_json = serde_json::to_string(&rule.actions).unwrap();
                    sqlx::query(
                        "INSERT INTO rules (name, priority, enabled, condition_json, action_json) 
                         VALUES (?, ?, ?, ?, ?)
                         ON CONFLICT(name) DO UPDATE SET
                           priority=excluded.priority,
                           enabled=excluded.enabled,
                           condition_json=excluded.condition_json,
                           action_json=excluded.action_json",
                    )
                    .bind(rule.name)
                    .bind(rule.priority)
                    .bind(rule.enabled)
                    .bind(condition_json)
                    .bind(action_json)
                    .execute(&pool)
                    .await?;
                }
            }
        }
        suggester::run_suggester(&pool).await?;
        info!("Suggestion complete.");
    }

    Ok(summary)
}



pub fn build_registry(config: &crate::config::AppConfig) -> ProviderRegistry {
    let mut reg = ProviderRegistry::new().with_embedding("noop", Arc::new(NoopProvider));

    if let (Some(key), Some(base)) = (
        std::env::var_os("OPENAI_API_KEY"),
        std::env::var_os("OPENAI_BASE_URL"),
    ) {
        let provider = OpenAiProvider::new(OpenAiConfig {
            api_key: key.to_string_lossy().into_owned(),
            base_url: base.to_string_lossy().into_owned(),
            embedding_model: config.embeddings.model.clone(),
            chat_model: "gpt-4o-mini".to_string(),
        });
        reg = reg
            .with_embedding("openai", Arc::new(provider.clone()))
            .with_llm("openai", Arc::new(provider));
    }

    if let Some(base) = std::env::var_os("LMSTUDIO_BASE_URL") {
        let provider = LmStudioProvider::new(LmStudioConfig {
            base_url: base.to_string_lossy().into_owned(),
            embedding_model: config.embeddings.model.clone(),
            chat_model: "lmstudio-chat".to_string(),
        });
        reg = reg
            .with_embedding("lmstudio", Arc::new(provider.clone()))
            .with_llm("lmstudio", Arc::new(provider));
    }

    reg.set_preferred_embedding(&config.embeddings.provider)
}

pub fn build_vector_store(config: &crate::config::AppConfig) -> Box<dyn VectorStore> {
    match config.vectors.provider.as_str() {
        "qdrant" => {
            if let Some(url) = &config.vectors.url {
                let client = QdrantClient::new(providers::qdrant::QdrantConfig {
                    url: url.clone(),
                    collection: config.vectors.collection.clone(),
                    api_key: std::env::var("QDRANT_API_KEY").ok(),
                });
                return Box::new(vectorstore::QdrantStore::new(client));
            }
            Box::new(vectorstore::NoopVectorStore)
        }
        _ => Box::new(vectorstore::NoopVectorStore),
    }
}


