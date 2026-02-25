use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use sdb_bson::{Document, Value};
use sdb_common::Result;

use crate::catalog::CatalogManager;

/// Catalog service trait — handles catalog-level operations.
#[async_trait]
pub trait CatalogService: Send + Sync {
    async fn create_collection_space(&self, name: &str) -> Result<()>;
    async fn drop_collection_space(&self, name: &str) -> Result<()>;
    async fn create_collection(&self, cs_name: &str, cl_name: &str) -> Result<()>;
    async fn drop_collection(&self, cs_name: &str, cl_name: &str) -> Result<()>;
    async fn get_catalog_info(&self, collection: &str) -> Result<Document>;
}

/// Concrete implementation wrapping a shared CatalogManager.
pub struct CatalogServiceImpl {
    manager: Arc<RwLock<CatalogManager>>,
}

impl CatalogServiceImpl {
    pub fn new(manager: Arc<RwLock<CatalogManager>>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl CatalogService for CatalogServiceImpl {
    async fn create_collection_space(&self, name: &str) -> Result<()> {
        let mut mgr = self.manager.write().unwrap();
        mgr.create_collection_space(name)?;
        Ok(())
    }

    async fn drop_collection_space(&self, name: &str) -> Result<()> {
        let mut mgr = self.manager.write().unwrap();
        mgr.drop_collection_space(name)
    }

    async fn create_collection(&self, cs_name: &str, cl_name: &str) -> Result<()> {
        let mut mgr = self.manager.write().unwrap();
        mgr.create_collection(cs_name, cl_name)
    }

    async fn drop_collection(&self, cs_name: &str, cl_name: &str) -> Result<()> {
        let mut mgr = self.manager.write().unwrap();
        mgr.drop_collection(cs_name, cl_name)
    }

    async fn get_catalog_info(&self, collection: &str) -> Result<Document> {
        let mgr = self.manager.read().unwrap();
        // Parse "cs.cl" format.
        let (cs, cl) = collection
            .split_once('.')
            .ok_or(sdb_common::SdbError::InvalidArg)?;
        let meta = mgr.get_collection(cs, cl)?;
        let mut doc = Document::new();
        doc.insert("name", Value::String(meta.name.clone()));
        doc.insert("cs_id", Value::Int32(meta.cs_id as i32));
        doc.insert("cl_id", Value::Int32(meta.cl_id as i32));
        doc.insert("replicated", Value::Boolean(meta.replicated));
        Ok(doc)
    }
}
