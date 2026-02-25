use crate::cursor::ClientCursor;
use sdb_bson::Document;
use sdb_common::Result;

/// Handle for a collection on the remote server.
pub struct Collection {
    pub full_name: String,
}

impl Collection {
    pub(crate) fn new(full_name: String) -> Self {
        Self { full_name }
    }

    pub async fn insert(&self, _doc: Document) -> Result<()> {
        // Stub
        Ok(())
    }

    pub async fn query(&self, _condition: Option<Document>) -> Result<ClientCursor> {
        // Stub
        Ok(ClientCursor::empty())
    }

    pub async fn update(&self, _condition: Document, _modifier: Document) -> Result<()> {
        // Stub
        Ok(())
    }

    pub async fn delete(&self, _condition: Document) -> Result<()> {
        // Stub
        Ok(())
    }

    pub async fn count(&self, _condition: Option<Document>) -> Result<u64> {
        // Stub
        Ok(0)
    }
}
