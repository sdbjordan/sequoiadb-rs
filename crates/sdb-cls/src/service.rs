use async_trait::async_trait;
use sdb_common::{Lsn, Result};

/// Replication service trait.
#[async_trait]
pub trait ReplicationService: Send + Sync {
    async fn sync(&self) -> Result<Lsn>;
    async fn get_status(&self) -> Result<sdb_bson::Document>;
}
