use providers::qdrant::{QdrantClient, QdrantPoint};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct VectorRecord {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: HashMap<String, String>,
}

#[async_trait::async_trait]
pub trait VectorStore: Send + Sync {
    async fn upsert(&self, records: Vec<VectorRecord>) -> anyhow::Result<()>;
    fn as_any(&self) -> &dyn std::any::Any;
}

/// In-memory no-op vector store placeholder.
pub struct NoopVectorStore;

#[async_trait::async_trait]
impl VectorStore for NoopVectorStore {
    async fn upsert(&self, _records: Vec<VectorRecord>) -> anyhow::Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct QdrantStore {
    client: QdrantClient,
}

impl QdrantStore {
    pub fn new(client: QdrantClient) -> Self {
        Self { client }
    }

    pub fn client(&self) -> QdrantClient {
        self.client.clone()
    }
}

#[async_trait::async_trait]
impl VectorStore for QdrantStore {
    async fn upsert(&self, records: Vec<VectorRecord>) -> anyhow::Result<()> {
        let points: Vec<QdrantPoint> = records
            .into_iter()
            .map(|r| QdrantPoint {
                id: r.id,
                vector: r.vector,
                payload: r
                    .metadata
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::String(v)))
                    .collect(),
            })
            .collect();
        self.client.upsert(points).await?;
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub trait AsQdrant {
    fn downcast_qdrant(&self) -> Option<QdrantClient>;
}

impl AsQdrant for Box<dyn VectorStore> {
    fn downcast_qdrant(&self) -> Option<QdrantClient> {
        if let Some(store) = self.as_any().downcast_ref::<QdrantStore>() {
            Some(store.client())
        } else {
            None
        }
    }
}
