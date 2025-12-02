use crate::{ClassifyResponse, EmbedResponse, EmbeddingProvider, LlmProvider, ProviderError};
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Clone)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub base_url: String,
    pub embedding_model: String,
    pub chat_model: String,
}

#[derive(Clone)]
pub struct OpenAiProvider {
    client: Client,
    cfg: Arc<OpenAiConfig>,
}

impl OpenAiProvider {
    pub fn new(cfg: OpenAiConfig) -> Self {
        Self {
            client: Client::new(),
            cfg: Arc::new(cfg),
        }
    }
}

#[derive(Deserialize)]
struct EmbeddingApiResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

#[async_trait::async_trait]
impl EmbeddingProvider for OpenAiProvider {
    async fn embed(&self, texts: &[String]) -> Result<EmbedResponse, ProviderError> {
        #[derive(serde::Serialize)]
        struct EmbedRequest<'a> {
            model: &'a str,
            input: &'a [String],
        }

        let body = EmbedRequest {
            model: &self.cfg.embedding_model,
            input: texts,
        };

        let resp = self
            .client
            .post(format!("{}/v1/embeddings", self.cfg.base_url))
            .bearer_auth(&self.cfg.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::RequestFailed(e.to_string()))?;

        let parsed: EmbeddingApiResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::RequestFailed(e.to_string()))?;

        Ok(EmbedResponse {
            vectors: parsed.data.into_iter().map(|d| d.embedding).collect(),
        })
    }
}

#[async_trait::async_trait]
impl LlmProvider for OpenAiProvider {
    async fn classify(&self, prompt: &str) -> Result<ClassifyResponse, ProviderError> {
        #[derive(serde::Serialize)]
        struct ChatMessage<'a> {
            role: &'static str,
            content: &'a str,
        }
        #[derive(serde::Serialize)]
        struct ChatRequest<'a> {
            model: &'a str,
            messages: Vec<ChatMessage<'a>>,
        }
        #[derive(Deserialize)]
        struct Choice {
            message: ChatMessageResp,
        }
        #[derive(Deserialize)]
        struct ChatMessageResp {
            content: String,
        }
        #[derive(Deserialize)]
        struct ChatApiResponse {
            choices: Vec<Choice>,
        }

        let body = ChatRequest {
            model: &self.cfg.chat_model,
            messages: vec![ChatMessage {
                role: "user",
                content: prompt,
            }],
        };

        let resp = self
            .client
            .post(format!("{}/v1/chat/completions", self.cfg.base_url))
            .bearer_auth(&self.cfg.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::RequestFailed(e.to_string()))?;

        let parsed: ChatApiResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::RequestFailed(e.to_string()))?;

        let content = parsed
            .choices
            .get(0)
            .map(|c| c.message.content.clone())
            .unwrap_or_default();

        Ok(ClassifyResponse {
            label: content,
            confidence: 0.0,
            rationale: None,
        })
    }
}
