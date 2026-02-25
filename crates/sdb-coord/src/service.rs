use async_trait::async_trait;
use sdb_bson::Document;
use sdb_common::Result;

/// Coordinator service trait — top-level request handling.
#[async_trait]
pub trait CoordinatorService: Send + Sync {
    async fn query(&self, collection: &str, condition: &Document) -> Result<Vec<Document>>;
    async fn insert(&self, collection: &str, docs: Vec<Document>) -> Result<()>;
    async fn update(&self, collection: &str, condition: &Document, modifier: &Document) -> Result<()>;
    async fn delete(&self, collection: &str, condition: &Document) -> Result<()>;
}
