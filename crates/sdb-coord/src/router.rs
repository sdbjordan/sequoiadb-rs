use sdb_bson::Document;
use sdb_common::{GroupId, Result};

/// Coordinator router — routes requests to the correct data groups.
pub struct CoordRouter;

impl CoordRouter {
    pub fn new() -> Self {
        Self
    }

    /// Route a query to the appropriate group(s).
    pub fn route_query(&self, _collection: &str, _condition: &Document) -> Result<Vec<GroupId>> {
        // Stub: broadcast to all groups
        Ok(vec![1])
    }

    /// Route an insert to the appropriate group.
    pub fn route_insert(&self, _collection: &str, _doc: &Document) -> Result<GroupId> {
        // Stub
        Ok(1)
    }
}

impl Default for CoordRouter {
    fn default() -> Self {
        Self::new()
    }
}
