use async_trait::async_trait;
use sdb_common::Result;

/// Catalog service trait — handles catalog-level operations.
#[async_trait]
pub trait CatalogService: Send + Sync {
    async fn create_collection_space(&self, name: &str) -> Result<()>;
    async fn drop_collection_space(&self, name: &str) -> Result<()>;
    async fn create_collection(&self, cs_name: &str, cl_name: &str) -> Result<()>;
    async fn drop_collection(&self, cs_name: &str, cl_name: &str) -> Result<()>;
    async fn get_catalog_info(&self, collection: &str) -> Result<sdb_bson::Document>;
}
