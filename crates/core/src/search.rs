use providers::qdrant::{QdrantClient, SearchResult};

pub async fn vector_search(
    client: &QdrantClient,
    vector: Vec<f32>,
    limit: u64,
    filter: Option<serde_json::Value>,
) -> anyhow::Result<Vec<SearchResult>> {
    let resp = client.search(vector, limit, filter).await?;
    Ok(resp.result)
}
