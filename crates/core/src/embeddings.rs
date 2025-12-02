use providers::ProviderRegistry;

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
