use sdb_bson::Document;
use sdb_cls::ShardManager;
use sdb_common::{GroupId, Result};

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
