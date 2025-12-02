use crate::{ClassifyResponse, EmbedResponse, EmbeddingProvider, LlmProvider, ProviderError};

#[derive(Debug, Default)]
pub struct NoopProvider;

#[async_trait::async_trait]
impl EmbeddingProvider for NoopProvider {
    async fn embed(&self, texts: &[String]) -> Result<EmbedResponse, ProviderError> {
        Ok(EmbedResponse {
            vectors: vec![vec![]; texts.len()],
        })
    }
}

#[async_trait::async_trait]
impl LlmProvider for NoopProvider {
    async fn classify(&self, _prompt: &str) -> Result<ClassifyResponse, ProviderError> {
        Err(ProviderError::NotImplemented)
    }
}
