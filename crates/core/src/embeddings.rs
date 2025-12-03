use providers::qdrant::{QdrantClient, QdrantPoint};
use providers::ProviderRegistry;
use sqlx::SqlitePool;
use std::collections::{HashMap, HashSet};
use storage::models::Chunk;
use sqlx::Row;

#[derive(Debug, Clone)]
struct ChunkWithFile {
    pub chunk: Chunk,
    pub path: String,
    pub mime: Option<String>,
    pub ext: Option<String>,
    pub mtime: Option<i64>,
}

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

/// Embed all chunks.
pub async fn run_embedder(
    pool: &SqlitePool,
    registry: &ProviderRegistry,
    vector_db: &QdrantClient,
    batch_size: usize,
) -> anyhow::Result<usize> {
    run_embedder_for_files(pool, registry, vector_db, batch_size, None).await
}

/// Embed chunks for a specific set of file IDs (or all if None).
pub async fn run_embedder_for_files(
    pool: &SqlitePool,
    registry: &ProviderRegistry,
    vector_db: &QdrantClient,
    batch_size: usize,
    file_ids: Option<&[i64]>,
) -> anyhow::Result<usize> {
    let mut query = String::from(
        "SELECT c.id, c.file_id, c.hash, c.start, c.end, c.text_preview, f.path, f.mime, f.ext, f.mtime \
         FROM chunks c JOIN files f ON f.id = c.file_id",
    );
    let mut has_filter = false;
    if let Some(ids) = file_ids {
        if !ids.is_empty() {
            let placeholders = std::iter::repeat("?")
                .take(ids.len())
                .collect::<Vec<_>>()
                .join(",");
            query.push_str(&format!(" WHERE c.file_id IN ({})", placeholders));
            has_filter = true;
        }
    }
    if !has_filter {
        // no additional filter needed
    }
    let mut q = sqlx::query(&query);
    if let Some(ids) = file_ids {
        for id in ids {
            q = q.bind(id);
        }
    }
    let rows = q.fetch_all(pool).await?;
    let mut chunks = Vec::new();
    for row in rows {
        chunks.push(ChunkWithFile {
            chunk: Chunk {
                id: row.try_get("id")?,
                file_id: row.try_get("file_id")?,
                hash: row.try_get("hash")?,
                start: row.try_get::<i64, _>("start")?,
                end: row.try_get::<i64, _>("end")?,
                text_preview: row.try_get("text_preview")?,
            },
            path: row.try_get("path")?,
            mime: row.try_get("mime")?,
            ext: row.try_get("ext")?,
            mtime: row.try_get("mtime").ok(),
        });
    }

    // Skip chunks already in the vector store to avoid re-embedding unchanged content.
    let mut present: HashSet<String> = HashSet::new();
    if !chunks.is_empty() {
        let chunk_ids: Vec<String> = chunks.iter().map(|c| c.chunk.hash.clone()).collect();
        for batch_ids in chunk_ids.chunks(256) {
            if let Ok(resp) = vector_db.retrieve(batch_ids.to_vec()).await {
                for p in resp.result {
                    present.insert(p.id);
                }
            }
        }
    }

    let chunks: Vec<ChunkWithFile> = if present.is_empty() {
        chunks
    } else {
        chunks
            .into_iter()
            .filter(|c| !present.contains(&c.chunk.hash))
            .collect()
    };

    let mut embedded = 0usize;
    for batch in chunks.chunks(batch_size) {
        let texts: Vec<String> = batch
            .iter()
            .map(|c| c.chunk.text_preview.clone().unwrap_or_default())
            .collect();

        let req = EmbeddingRequest {
            texts,
            provider: None, // Use preferred provider
        };

        let embeddings = embed(req, registry).await?;

        let points = batch
            .iter()
            .zip(embeddings.vectors.into_iter())
            .map(|(chunk_with_file, vector)| {
                let chunk = &chunk_with_file.chunk;
                let mut payload = HashMap::new();
                payload.insert("file_id".to_string(), serde_json::json!(chunk.file_id));
                payload.insert("chunk_id".to_string(), serde_json::json!(chunk.id));
                payload.insert("path".to_string(), serde_json::json!(chunk_with_file.path.clone()));
                if let Some(m) = &chunk_with_file.mime {
                    payload.insert("mime".to_string(), serde_json::json!(m));
                }
                if let Some(e) = &chunk_with_file.ext {
                    payload.insert("ext".to_string(), serde_json::json!(e));
                }
                if let Some(mt) = chunk_with_file.mtime {
                    payload.insert("mtime".to_string(), serde_json::json!(mt));
                }
                let prefixes = path_prefixes(&chunk_with_file.path);
                payload.insert("path_prefixes".to_string(), serde_json::json!(prefixes));
                QdrantPoint {
                    id: chunk.hash.clone(),
                    vector,
                    payload,
                }
            })
            .collect();

        vector_db.upsert(points).await?;
        embedded += batch.len();
    }

    Ok(embedded)
}

fn path_prefixes(path: &str) -> Vec<String> {
    let mut prefixes = Vec::new();
    let normalized = path.replace('\\', "/").to_lowercase();
    let mut accum = String::new();
    for part in normalized.split('/') {
        if part.is_empty() {
            continue;
        }
        accum.push('/');
        accum.push_str(part);
        prefixes.push(accum.clone());
    }
    prefixes
}
