//! Provider abstractions for LLMs and embeddings.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

pub mod lmstudio;
pub mod noop;
pub mod openai;
pub mod qdrant;

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("not implemented")]
    NotImplemented,
    #[error("request failed: {0}")]
    RequestFailed(String),
    #[error("unknown provider: {0}")]
    UnknownProvider(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbedResponse {
    pub vectors: Vec<Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifyResponse {
    pub label: String,
    pub confidence: f32,
    pub rationale: Option<String>,
}

#[async_trait::async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, texts: &[String]) -> Result<EmbedResponse, ProviderError>;
}

#[async_trait::async_trait]
pub trait LlmProvider: Send + Sync {
    async fn classify(&self, prompt: &str) -> Result<ClassifyResponse, ProviderError>;
}

#[derive(Default, Clone)]
pub struct ProviderRegistry {
    embeddings: HashMap<String, Arc<dyn EmbeddingProvider>>,
    llms: HashMap<String, Arc<dyn LlmProvider>>,
    pub preferred_embedding: Option<String>,
    pub preferred_llm: Option<String>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_embedding(mut self, name: &str, provider: Arc<dyn EmbeddingProvider>) -> Self {
        self.embeddings.insert(name.to_string(), provider);
        self
    }

    pub fn with_llm(mut self, name: &str, provider: Arc<dyn LlmProvider>) -> Self {
        self.llms.insert(name.to_string(), provider);
        self
    }

    pub fn set_preferred_embedding(mut self, name: &str) -> Self {
        self.preferred_embedding = Some(name.to_string());
        self
    }

    pub fn set_preferred_llm(mut self, name: &str) -> Self {
        self.preferred_llm = Some(name.to_string());
        self
    }

    pub fn embedding(
        &self,
        name: Option<&str>,
    ) -> Result<Arc<dyn EmbeddingProvider>, ProviderError> {
        let key = name
            .map(str::to_string)
            .or_else(|| self.preferred_embedding.clone())
            .ok_or_else(|| {
                ProviderError::UnknownProvider("no embedding provider configured".into())
            })?;
        self.embeddings
            .get(&key)
            .cloned()
            .ok_or_else(|| ProviderError::UnknownProvider(key))
    }

    pub fn llm(&self, name: Option<&str>) -> Result<Arc<dyn LlmProvider>, ProviderError> {
        let key = name
            .map(str::to_string)
            .or_else(|| self.preferred_llm.clone())
            .ok_or_else(|| ProviderError::UnknownProvider("no llm provider configured".into()))?;
        self.llms
            .get(&key)
            .cloned()
            .ok_or_else(|| ProviderError::UnknownProvider(key))
    }
}

// TODO: Add concrete providers (LM Studio, GPT4All, OpenAI) behind feature flags.
