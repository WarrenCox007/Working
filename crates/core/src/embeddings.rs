use storage::models::Chunk;
use providers::qdrant::{QdrantClient, QdrantPoint};
use providers::ProviderRegistry;
use sqlx::SqlitePool;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct EmbeddingRequest {
    pub texts: Vec<String>,
    pub provider: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EmbeddingResult {
    pub vectors: Vec<Vec<f32>>,
}

pub async fn embed(
    req: EmbeddingRequest,
    registry: &ProviderRegistry,
) -> anyhow::Result<EmbeddingResult> {
    let provider = registry.embedding(req.provider.as_deref())?;
    let resp = provider.embed(&req.texts).await?;
    Ok(EmbeddingResult {
        vectors: resp.vectors,
    })
}

pub async fn run_embedder(
    pool: &SqlitePool,
    registry: &ProviderRegistry,
    vector_db: &QdrantClient,
    batch_size: usize,
) -> anyhow::Result<()> {
    let chunks = sqlx::query_as::<_, Chunk>("SELECT * FROM chunks")
        .fetch_all(pool)
        .await?;

    for batch in chunks.chunks(batch_size) {
        let texts: Vec<String> = batch.iter().map(|c| c.text_preview.clone().unwrap_or_default()).collect();

        let req = EmbeddingRequest {
            texts,
            provider: None, // Use preferred provider
        };

        let embeddings = embed(req, registry).await?;

        let points = batch
            .iter()
            .zip(embeddings.vectors.into_iter())
            .map(|(chunk, vector)| {
                let mut payload = HashMap::new();
                payload.insert("file_id".to_string(), serde_json::json!(chunk.file_id));
                payload.insert("chunk_id".to_string(), serde_json::json!(chunk.id));
                QdrantPoint {
                    id: chunk.hash.clone(),
                    vector,
                    payload,
                }
            })
            .collect();

        vector_db.upsert(points).await?;
    }

    Ok(())
}
