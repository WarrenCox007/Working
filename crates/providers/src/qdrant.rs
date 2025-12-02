use crate::ProviderError;
use bytes::Bytes;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone)]
pub struct QdrantConfig {
    pub url: String,
    pub collection: String,
    pub api_key: Option<String>,
}

#[derive(Clone)]
pub struct QdrantClient {
    client: Client,
    cfg: QdrantConfig,
}

impl QdrantClient {
    pub fn new(cfg: QdrantConfig) -> Self {
        Self {
            client: Client::new(),
            cfg,
        }
    }

    pub async fn search(
        &self,
        vector: Vec<f32>,
        limit: u64,
        filter: Option<serde_json::Value>,
    ) -> Result<QdrantSearchResponse, ProviderError> {
        #[derive(Serialize)]
        struct SearchRequest {
            vector: Vec<f32>,
            limit: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            filter: Option<serde_json::Value>,
        }
        let url = format!(
            "{}/collections/{}/points/search",
            self.cfg.url, self.cfg.collection
        );
        let body = SearchRequest {
            vector,
            limit,
            filter,
        };
        let mut builder = self.client.post(url).json(&body);
        if let Some(key) = &self.cfg.api_key {
            builder = builder.header("api-key", key);
        }
        let resp = builder
            .send()
            .await
            .map_err(|e| ProviderError::RequestFailed(e.to_string()))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.bytes().await.unwrap_or(Bytes::from_static(b""));
            return Err(ProviderError::RequestFailed(format!(
                "status {} body {:?}",
                status, body
            )));
        }
        let parsed: QdrantSearchResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::RequestFailed(e.to_string()))?;
        Ok(parsed)
    }

    pub async fn upsert(&self, points: Vec<QdrantPoint>) -> Result<(), ProviderError> {
        let url = format!(
            "{}/collections/{}/points",
            self.cfg.url, self.cfg.collection
        );
        let req = QdrantUpsert { points };
        let mut builder = self.client.put(url).json(&req);
        if let Some(key) = &self.cfg.api_key {
            builder = builder.header("api-key", key);
        }
        let resp = builder
            .send()
            .await
            .map_err(|e| ProviderError::RequestFailed(e.to_string()))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.bytes().await.unwrap_or(Bytes::from_static(b""));
            return Err(ProviderError::RequestFailed(format!(
                "status {} body {:?}",
                status, body
            )));
        }
        Ok(())
    }

    pub async fn delete_by_filter(&self, filter: serde_json::Value) -> Result<(), ProviderError> {
        #[derive(Serialize)]
        struct DeletePoints {
            filter: serde_json::Value,
        }
        let url = format!(
            "{}/collections/{}/points/delete",
            self.cfg.url, self.cfg.collection
        );
        let body = DeletePoints { filter };
        let mut builder = self.client.post(url).json(&body);
        if let Some(key) = &self.cfg.api_key {
            builder = builder.header("api-key", key);
        }
        let resp = builder
            .send()
            .await
            .map_err(|e| ProviderError::RequestFailed(e.to_string()))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.bytes().await.unwrap_or(Bytes::from_static(b""));
            return Err(ProviderError::RequestFailed(format!(
                "status {} body {:?}",
                status, body
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct QdrantUpsert {
    pub points: Vec<QdrantPoint>,
}

#[derive(Debug, Serialize)]
pub struct QdrantPoint {
    pub id: String,
    pub vector: Vec<f32>,
    pub payload: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct QdrantResponse {
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct QdrantSearchResponse {
    pub result: Vec<SearchResult>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SearchResult {
    pub id: serde_json::Value,
    pub score: f32,
    pub payload: Option<serde_json::Value>,
}
