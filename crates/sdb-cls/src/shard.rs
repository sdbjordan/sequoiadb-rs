use sdb_bson::Document;
use sdb_common::{GroupId, Result};

/// Manages sharding — determines which group owns a given shard key range.
pub struct ShardManager {
    pub shard_key: Option<Document>,
}

impl ShardManager {
    pub fn new() -> Self {
        Self { shard_key: None }
    }

    /// Determine the target group for a given document based on shard key.
    pub fn route(&self, _doc: &Document) -> Result<GroupId> {
        // Stub: always route to group 1
        Ok(1)
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}
