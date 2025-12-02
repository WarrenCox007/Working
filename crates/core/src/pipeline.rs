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
    let indexer = indexer::Indexer::new(pool.clone());
    let vector_store = build_vector_store(&config);

    // Setup providers
    let registry = build_registry(&config);

    // Scan
    let scan_result = scanner::scan(scanner::ScanConfig {
        roots: config.scan.include.iter().map(|p| p.into()).collect(),
        include_hidden: false,
        follow_symlinks: false,
        excludes: config.scan.exclude.clone(),
        hash_mode: parse_hash_mode(config.scan.hash_mode.as_deref()),
    })
    .await
    .context("scan")?;
    info!("discovered {} entries", scan_result.discovered.len());
    let mut processed_files = 0usize;
    let mut embedded_chunks = 0usize;

    for item in &scan_result.discovered {
        if item.is_dir {
            continue;
        }
        let extracted = extractor::extract(item.path.clone())
            .await
            .context("extract")?;
        if matches!(
            mode,
            PipelineMode::Classify | PipelineMode::All | PipelineMode::Suggest
        ) {
            let embed_res = embeddings::embed(
                embeddings::EmbeddingRequest {
                    texts: extracted.snippets.clone(),
                    provider: Some(config.embeddings.provider.clone()),
                },
                &registry,
            )
            .await;
            if let Err(e) = embed_res {
                warn!("embedding skipped for {:?}: {}", item.path, e);
            }

            let classify_res = classifier::classify(
                classifier::ClassificationInput {
                    text: extracted.snippets.join("\n"),
                    metadata: serde_json::json!({"mime": extracted.mime, "size": extracted.size}),
                    provider: None,
                },
                &registry,
            )
            .await;
            if let Err(e) = classify_res {
                warn!("classification skipped for {:?}: {}", item.path, e);
            }
        }

        if matches!(mode, PipelineMode::Suggest | PipelineMode::All) {
            let rules_path = config
                .rules
                .path
                .as_ref()
                .map(|p| std::path::PathBuf::from(p))
                .unwrap_or_else(|| std::path::PathBuf::from("rules"));
            let mut rules = Vec::new();
            if let Ok(rs) = crate::rules::load_rules_from_dir(&rules_path) {
                rules = rs;
            }
            if rules.is_empty() {
                let suggestions = suggester::suggest(&item.path);
                for s in suggestions {
                    let action_record: crate::suggester::ActionRecord = s.into();
                    if let Err(e) = indexer.insert_action(action_record).await {
                        warn!("failed to persist action for {:?}: {}", item.path, e);
                    }
                }
            } else {
                let suggestions = suggester::suggest_with_rules(
                    &item.path,
                    extracted.mime.as_deref(),
                    item.path.extension().and_then(|e| e.to_str()),
                    &Vec::new(),
                    &rules,
                );
                for (s, rule_name) in suggestions {
                    let mut action_record: crate::suggester::ActionRecord = s.into();
                    action_record.rule = rule_name;
                    // embed rule name into payload for visibility
                    if let Some(r) = &action_record.rule {
                        let mut payload = action_record.payload;
                        payload["rule"] = serde_json::Value::String(r.clone());
                        action_record.payload = payload;
                    }
                    if let Err(e) = indexer.insert_action(action_record).await {
                        warn!("failed to persist action for {:?}: {}", item.path, e);
                    }
                }
            }
        }
        indexer
            .upsert(indexer::IndexRecord {
                path: item.path.clone(),
                size: item.size,
                mtime: item.mtime,
                hash: item.hash.clone(),
                mime: extracted.mime.clone(),
                ext: item
                    .path
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string()),
            })
            .await
            .context("index")?;
        let do_embeddings = matches!(
            mode,
            PipelineMode::Classify | PipelineMode::All | PipelineMode::Suggest
        );
        for chunk in extracted.chunks {
            let chunk_hash = blake3::hash(chunk.text.as_bytes()).to_hex().to_string();
            indexer
                .insert_chunk(
                    &item.path.to_string_lossy(),
                    indexer::ChunkRecord {
                        file_id: item.path.to_string_lossy().into_owned(),
                        start: chunk.start as i64,
                        end: chunk.end as i64,
                        text_preview: Some(chunk.text.clone()),
                        hash: Some(chunk_hash.clone()),
                    },
                )
                .await
                .context("insert chunk")?;
            if do_embeddings {
                if let Ok(EmbeddingResult { vectors }) = embeddings::embed(
                    embeddings::EmbeddingRequest {
                        texts: vec![chunk.text.clone()],
                        provider: Some(config.embeddings.provider.clone()),
                    },
                    &registry,
                )
                .await
                {
                    if let Some(vec) = vectors.into_iter().next() {
                        let mut meta = std::collections::HashMap::new();
                        meta.insert("path".to_string(), item.path.to_string_lossy().into_owned());
                        meta.insert(
                            "mime".to_string(),
                            extracted.mime.clone().unwrap_or_default(),
                        );
                        meta.insert("hash".to_string(), chunk_hash.clone());
                        meta.insert("mtime".to_string(), item.mtime.to_string());
                        if let Err(e) = vector_store
                            .upsert(vec![vectorstore::VectorRecord {
                                id: chunk_hash.clone(),
                                vector: vec,
                                metadata: meta,
                            }])
                            .await
                        {
                            warn!("vector upsert failed for {:?}: {}", item.path, e);
                        } else {
                            embedded_chunks += 1;
                        }
                    }
                }
            }
        }

        if let Some(exif) = extracted.exif.as_ref() {
            if let Err(e) = indexer
                .insert_metadata(&item.path.to_string_lossy(), exif)
                .await
            {
                warn!("metadata insert failed for {:?}: {}", item.path, e);
            }
        }
        debug!("processed file");
        processed_files += 1;
    }

    info!("processed files: {}", processed_files);
    if matches!(
        mode,
        PipelineMode::Classify | PipelineMode::All | PipelineMode::Suggest
    ) {
        info!("embedded chunks: {}", embedded_chunks);
    }

    Ok(PipelineSummary {
        discovered: scan_result.discovered.len(),
        processed_files,
        embedded_chunks,
    })
}

fn parse_hash_mode(maybe: Option<&str>) -> scanner::HashMode {
    match maybe.unwrap_or("none").to_lowercase().as_str() {
        "fast" => scanner::HashMode::Fast,
        "full" => scanner::HashMode::Full,
        _ => scanner::HashMode::None,
    }
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
