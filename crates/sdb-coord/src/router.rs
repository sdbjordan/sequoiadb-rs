use sdb_bson::Document;
use sdb_cls::shard::ChunkInfo;
use sdb_cls::ShardManager;
use sdb_common::{GroupId, Result, SdbError};

/// Coordinator router — routes requests to the correct data groups.
/// Uses a ShardManager to determine routing based on shard key.
pub struct CoordRouter {
    shard_managers: std::collections::HashMap<String, ShardManager>,
    default_group: GroupId,
}

impl CoordRouter {
    pub fn new() -> Self {
        Self {
            shard_managers: std::collections::HashMap::new(),
            default_group: 1,
        }
    }

    /// Register a shard manager for a collection.
    pub fn register_shard(&mut self, collection: &str, manager: ShardManager) {
        self.shard_managers.insert(collection.to_string(), manager);
    }

    /// Route a query to the appropriate group(s).
    pub fn route_query(&self, collection: &str, condition: Option<&Document>) -> Result<Vec<GroupId>> {
        if let Some(sm) = self.shard_managers.get(collection) {
            Ok(sm.route_query(condition))
        } else {
            Ok(vec![self.default_group])
        }
    }

    /// Route an insert to the appropriate group.
    pub fn route_insert(&self, collection: &str, doc: &Document) -> Result<GroupId> {
        if let Some(sm) = self.shard_managers.get(collection) {
            sm.route(doc)
        } else {
            Ok(self.default_group)
        }
    }

    /// Route an update — may need to go to multiple groups if condition doesn't include shard key.
    pub fn route_update(&self, collection: &str, condition: Option<&Document>) -> Result<Vec<GroupId>> {
        self.route_query(collection, condition)
    }

    /// Route a delete — same logic as update routing.
    pub fn route_delete(&self, collection: &str, condition: Option<&Document>) -> Result<Vec<GroupId>> {
        self.route_query(collection, condition)
    }

    /// Query shard configuration for a collection.
    /// Returns (shard_key, num_groups) if sharded, None otherwise.
    pub fn shard_info(&self, collection: &str) -> Option<(String, u32)> {
        let sm = self.shard_managers.get(collection)?;
        let key = sm.shard_key.as_ref()?.clone();
        Some((key, sm.num_groups))
    }

    /// Get chunk info for a collection.
    pub fn chunk_info(&self, collection: &str) -> Vec<ChunkInfo> {
        match self.shard_managers.get(collection) {
            Some(sm) => sm.chunk_info().to_vec(),
            None => vec![],
        }
    }

    /// Record an insert to a group for chunk tracking.
    pub fn record_insert(&mut self, collection: &str, group_id: GroupId) {
        if let Some(sm) = self.shard_managers.get_mut(collection) {
            sm.record_insert(group_id);
        }
    }

    /// Record deletes from a group for chunk tracking.
    pub fn record_delete(&mut self, collection: &str, group_id: GroupId, count: u64) {
        if let Some(sm) = self.shard_managers.get_mut(collection) {
            sm.record_delete(group_id, count);
        }
    }

    /// Split a chunk: move docs from source to target group.
    pub fn split_chunk(&mut self, collection: &str, source: GroupId, target: GroupId, docs_to_move: u64) -> Result<u32> {
        let sm = self.shard_managers.get_mut(collection).ok_or(SdbError::CollectionNotFound)?;
        sm.split_chunk(source, target, docs_to_move)
    }

    /// Set migrating flag on a chunk.
    pub fn set_migrating(&mut self, collection: &str, group_id: GroupId, migrating: bool) {
        if let Some(sm) = self.shard_managers.get_mut(collection) {
            sm.set_migrating(group_id, migrating);
        }
    }

    /// Find imbalance for a collection. Returns (source, target, docs_to_move).
    pub fn find_imbalance(&self, collection: &str) -> Option<(GroupId, GroupId, u64)> {
        self.shard_managers.get(collection)?.find_imbalance()
    }

    /// Get shard type for a collection: "hash", "range", or None.
    pub fn shard_type(&self, collection: &str) -> Option<&'static str> {
        let sm = self.shard_managers.get(collection)?;
        if sm.shard_key.is_none() {
            return None;
        }
        if sm.is_range_sharded() {
            Some("range")
        } else {
            Some("hash")
        }
    }

    /// Get a mutable reference to the shard manager for a collection.
    pub fn shard_manager_mut(&mut self, collection: &str) -> Option<&mut ShardManager> {
        self.shard_managers.get_mut(collection)
    }

    /// Get all sharded collection names.
    pub fn sharded_collections(&self) -> Vec<String> {
        self.shard_managers.keys().cloned().collect()
    }
}

impl Default for CoordRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::Value;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    #[test]
    fn no_shard_routes_to_default() {
        let router = CoordRouter::new();
        let groups = router.route_query("cs.cl", None).unwrap();
        assert_eq!(groups, vec![1]);
    }

    #[test]
    fn insert_routes_by_shard() {
        let mut router = CoordRouter::new();
        let mut sm = ShardManager::new();
        sm.set_hash_sharding("x", 3);
        router.register_shard("cs.cl", sm);

        let g = router.route_insert("cs.cl", &doc(&[("x", Value::Int32(42))])).unwrap();
        assert!(g >= 1 && g <= 3);
    }

    #[test]
    fn query_with_shard_key_routes_to_one() {
        let mut router = CoordRouter::new();
        let mut sm = ShardManager::new();
        sm.set_hash_sharding("x", 4);
        router.register_shard("cs.cl", sm);

        let groups = router.route_query("cs.cl", Some(&doc(&[("x", Value::Int32(10))]))).unwrap();
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn query_without_shard_key_broadcasts() {
        let mut router = CoordRouter::new();
        let mut sm = ShardManager::new();
        sm.set_hash_sharding("x", 3);
        router.register_shard("cs.cl", sm);

        let groups = router.route_query("cs.cl", Some(&doc(&[("y", Value::Int32(10))]))).unwrap();
        assert_eq!(groups.len(), 3);
    }

    #[test]
    fn unregistered_collection_defaults() {
        let router = CoordRouter::new();
        let g = router.route_insert("unknown.cl", &doc(&[("x", Value::Int32(1))])).unwrap();
        assert_eq!(g, 1);
    }
}
