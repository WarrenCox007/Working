use anyhow::Result;
use providers::qdrant::{QdrantClient, SearchResult};
use providers::ProviderRegistry;
use sqlx::SqlitePool;
use std::collections::HashMap;
use storage::models::{Chunk, File};

#[derive(Debug, Clone)]
pub struct ClassificationInput {
    pub text: String,
    pub metadata: serde_json::Value,
    pub provider: Option<String>,
    pub knn_candidates: Vec<(String, f32)>,
}

#[derive(Debug, Clone)]
pub struct ClassificationOutcome {
    pub label: String,
    pub confidence: f32,
}

pub async fn run_classifier(
    pool: &SqlitePool,
    registry: &ProviderRegistry,
    vector_db: &QdrantClient,
) -> Result<()> {
    let files_to_classify = sqlx::query_as::<_, File>(
        "SELECT * FROM files WHERE id NOT IN (SELECT DISTINCT file_id FROM file_tags)",
    )
    .fetch_all(pool)
    .await?;

    for file in files_to_classify {
        let chunks =
            sqlx::query_as::<_, Chunk>("SELECT * FROM chunks WHERE file_id = ? ORDER BY start")
                .bind(file.id)
                .fetch_all(pool)
                .await?;

        let chunk_hashes: Vec<String> = chunks.iter().map(|c| c.hash.clone()).collect();
        let chunk_vectors = vector_db.retrieve(chunk_hashes).await?;

        let mut knn_candidates = Vec::new();
        if !chunk_vectors.result.is_empty() {
            let vectors: Vec<Vec<f32>> = chunk_vectors
                .result
                .into_iter()
                .map(|p| p.vector)
                .collect();
            knn_candidates = classify_knn(file.id, vectors, pool, vector_db, 5).await?;
        }

        let full_text = chunks
            .into_iter()
            .map(|c| c.text_preview.unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");

        let metadata = serde_json::json!({
            "path": file.path,
            "mime": file.mime,
            "ext": file.ext,
        });

        let input = ClassificationInput {
            text: full_text,
            metadata,
            provider: None, // Use preferred
            knn_candidates,
        };

        let outcome = classify(input, registry).await?;

        if outcome.confidence > 0.5 {
            // Threshold to accept classification
            let mut tx = pool.begin().await?;

            sqlx::query!("INSERT OR IGNORE INTO tags (name) VALUES (?)", outcome.label)
                .execute(&mut *tx)
                .await?;

            let tag = sqlx::query!("SELECT id FROM tags WHERE name = ?", outcome.label)
                .fetch_one(&mut *tx)
                .await?;

            sqlx::query!(
                "INSERT OR IGNORE INTO file_tags (file_id, tag_id, confidence, source) VALUES (?, ?, ?, 'classifier')",
                file.id,
                tag.id,
                outcome.confidence
            )
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
        }
    }

    Ok(())
}

/// A version of the classifier runner that does not perform kNN.
pub async fn run_classifier_no_knn(
    pool: &SqlitePool,
    registry: &ProviderRegistry,
) -> anyhow::Result<()> {
    let files_to_classify = sqlx::query_as::<_, File>(
        "SELECT * FROM files WHERE id NOT IN (SELECT DISTINCT file_id FROM file_tags)",
    )
    .fetch_all(pool)
    .await?;

    for file in files_to_classify {
        let chunks =
            sqlx::query_as::<_, Chunk>("SELECT * FROM chunks WHERE file_id = ? ORDER BY start")
                .bind(file.id)
                .fetch_all(pool)
                .await?;

        let full_text = chunks
            .into_iter()
            .map(|c| c.text_preview.unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");

        let metadata = serde_json::json!({
            "path": file.path,
            "mime": file.mime,
            "ext": file.ext,
        });

        let input = ClassificationInput {
            text: full_text,
            metadata,
            provider: None, // Use preferred
            knn_candidates: Vec::new(),
        };

        let outcome = classify(input, registry).await?;

        if outcome.confidence > 0.5 {
            // Threshold to accept classification
            let mut tx = pool.begin().await?;

            sqlx::query!("INSERT OR IGNORE INTO tags (name) VALUES (?)", outcome.label)
                .execute(&mut *tx)
                .await?;

            let tag = sqlx::query!("SELECT id FROM tags WHERE name = ?", outcome.label)
                .fetch_one(&mut *tx)
                .await?;

            sqlx::query!(
                "INSERT OR IGNORE INTO file_tags (file_id, tag_id, confidence, source) VALUES (?, ?, ?, 'classifier')",
                file.id,
                tag.id,
                outcome.confidence
            )
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
        }
    }

    Ok(())
}

async fn classify_knn(
    file_id: i64,
    vectors: Vec<Vec<f32>>,
    pool: &SqlitePool,
    vector_db: &QdrantClient,
    k: usize,
) -> Result<Vec<(String, f32)>> {
    let mut neighbor_tags = HashMap::new();

    for vector in vectors {
        // Exclude self from search results
        let filter = Some(serde_json::json!({
            "must_not": [
                {
                    "key": "file_id",
                    "match": {
                        "value": file_id
                    }
                }
            ]
        }));
        let search_result = vector_db.search(vector, k as u64, filter).await?;
        let neighbor_file_ids: Vec<i64> = search_result
            .result
            .iter()
            .filter_map(|r| r.payload.as_ref())
            .filter_map(|p| p.get("file_id"))
            .filter_map(|v| v.as_i64())
            .collect();

        if neighbor_file_ids.is_empty() {
            continue;
        }

        let sql = format!(
            "SELECT t.name, ft.confidence FROM tags t JOIN file_tags ft ON t.id = ft.tag_id WHERE ft.file_id IN ({})",
            neighbor_file_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",")
        );

        let mut query = sqlx::query_as::<_, (String, f32)>(&sql);
        for id in neighbor_file_ids {
            query = query.bind(id);
        }

        let tags = query.fetch_all(pool).await?;
        for (tag, confidence) in tags {
            *neighbor_tags.entry(tag).or_insert(0.0) += confidence;
        }
    }

    let mut sorted_tags: Vec<(String, f32)> = neighbor_tags.into_iter().collect();
    sorted_tags.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    Ok(sorted_tags)
}

pub async fn classify(
    input: ClassificationInput,
    registry: &ProviderRegistry,
) -> anyhow::Result<ClassificationOutcome> {
    // Fast path: heuristics.
    if let Some(label) = heuristic_label(&input.metadata) {
        return Ok(ClassificationOutcome {
            label,
            confidence: 0.9,
        });
    }

    // kNN path
    if let Some((label, confidence)) = input.knn_candidates.first() {
        if *confidence > 0.7 {
            // Some threshold for accepting kNN result
            return Ok(ClassificationOutcome {
                label: label.clone(),
                confidence: *confidence,
            });
        }
    }

    // LLM fallback
    let provider = registry.llm(input.provider.as_deref());
    if let Ok(llm) = provider {
        let prompt = format!(
            "Classify the file with metadata {:?}. Text:\n{}",
            input.metadata, input.text
        );
        if let Ok(resp) = llm.classify(&prompt).await {
            return Ok(ClassificationOutcome {
                label: resp.label,
                confidence: resp.confidence,
            });
        }
    }

    Ok(ClassificationOutcome {
        label: "unknown".to_string(),
        confidence: 0.0,
    })
}

fn heuristic_label(meta: &serde_json::Value) -> Option<String> {
    let mime = meta.get("mime").and_then(|m| m.as_str()).unwrap_or("");
    let ext = meta
        .get("ext")
        .and_then(|e| e.as_str())
        .unwrap_or("")
        .to_lowercase();
    let path = meta
        .get("path")
        .and_then(|p| p.as_str())
        .unwrap_or("")
        .to_lowercase();

    let label = if mime.contains("pdf") || ext == "pdf" {
        "document/pdf"
    } else if mime.contains("msword")
        || mime.contains("officedocument")
        || matches!(
            ext.as_str(),
            "doc" | "docx" | "ppt" | "pptx" | "xls" | "xlsx"
        )
    {
        "document/office"
    } else if mime.starts_with("image/")
        || matches!(ext.as_str(), "jpg" | "jpeg" | "png" | "gif" | "heic")
    {
        "image"
    } else if mime.starts_with("text/") || matches!(ext.as_str(), "txt" | "md" | "rtf") {
        "text"
    } else if matches!(ext.as_str(), "zip" | "rar" | "7z" | "tar" | "gz") {
        "archive"
    } else if path.contains("download") && matches!(ext.as_str(), "pdf" | "docx" | "zip") {
        "inbox/download"
    } else {
        ""
    };

    if label.is_empty() {
        None
    } else {
        Some(label.to_string())
    }
}
