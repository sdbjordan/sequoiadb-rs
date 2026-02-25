use sdb_bson::Document;
use sdb_common::{RecordId, Result};

/// Core storage engine trait for CRUD operations on records.
pub trait StorageEngine: Send + Sync {
    fn insert(&self, doc: &Document) -> Result<RecordId>;
    fn find(&self, rid: RecordId) -> Result<Document>;
    fn delete(&self, rid: RecordId) -> Result<()>;
    fn update(&self, rid: RecordId, doc: &Document) -> Result<()>;
}
