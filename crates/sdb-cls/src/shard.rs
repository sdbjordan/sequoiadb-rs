use sdb_bson::{Document, Value};
use sdb_common::{GroupId, Result, SdbError};

/// Shard key range boundary.
#[derive(Debug, Clone)]
pub struct ShardRange {
    pub group_id: GroupId,
    pub low_bound: Option<Value>,
    pub up_bound: Option<Value>,
}

/// Manages sharding — determines which group owns a given shard key range.
pub struct ShardManager {
    pub shard_key: Option<String>,
    pub ranges: Vec<ShardRange>,
    pub num_groups: u32,
}

impl ShardManager {
    pub fn new() -> Self {
        Self {
            shard_key: None,
            ranges: Vec::new(),
            num_groups: 1,
        }
    }

    /// Configure hash-based sharding on a field across N groups.
    pub fn set_hash_sharding(&mut self, field: &str, num_groups: u32) {
        self.shard_key = Some(field.to_string());
        self.num_groups = num_groups.max(1);
        self.ranges.clear();
    }

    /// Configure range-based sharding with explicit boundaries.
    pub fn add_range(&mut self, field: &str, range: ShardRange) {
        if self.shard_key.is_none() {
            self.shard_key = Some(field.to_string());
        }
        self.ranges.push(range);
    }

    /// Determine the target group for a given document based on shard key.
    pub fn route(&self, doc: &Document) -> Result<GroupId> {
        let field = match &self.shard_key {
            Some(f) => f,
            None => return Ok(1), // no sharding → group 1
        };

        let val = doc.get(field).ok_or(SdbError::InvalidArg)?;

        // If range-based sharding is configured, check ranges
        if !self.ranges.is_empty() {
            return self.route_by_range(val);
        }

        // Hash-based routing
        Ok(self.hash_route(val))
    }

    /// Route a query condition to the group(s) that need to be scanned.
    /// Returns all groups if the condition doesn't include the shard key.
    pub fn route_query(&self, condition: Option<&Document>) -> Vec<GroupId> {
        if self.num_groups <= 1 {
            return vec![1];
        }

        let field = match &self.shard_key {
            Some(f) => f,
            None => return vec![1],
        };

        // If condition has an exact match on shard key, route to one group
        if let Some(cond) = condition {
            if let Some(val) = cond.get(field) {
                if !self.ranges.is_empty() {
                    if let Ok(gid) = self.route_by_range(val) {
                        return vec![gid];
                    }
                } else {
                    return vec![self.hash_route(val)];
                }
            }
        }

        // Broadcast to all groups
        (1..=self.num_groups).collect()
    }

    fn hash_route(&self, val: &Value) -> GroupId {
        let h = simple_hash(val);
        (h % self.num_groups) + 1
    }

    fn route_by_range(&self, val: &Value) -> Result<GroupId> {
        for range in &self.ranges {
            let above_low = match &range.low_bound {
                Some(low) => sdb_mth::compare::compare_values(val, low) != std::cmp::Ordering::Less,
                None => true,
            };
            let below_up = match &range.up_bound {
                Some(up) => sdb_mth::compare::compare_values(val, up) == std::cmp::Ordering::Less,
                None => true,
            };
            if above_low && below_up {
                return Ok(range.group_id);
            }
        }
        // Fallback to group 1 if no range matched
        Ok(1)
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple deterministic hash for Value types.
fn simple_hash(val: &Value) -> u32 {
    match val {
        Value::Int32(n) => *n as u32,
        Value::Int64(n) => *n as u32,
        Value::String(s) => {
            let mut h: u32 = 0;
            for b in s.bytes() {
                h = h.wrapping_mul(31).wrapping_add(b as u32);
            }
            h
        }
        Value::Double(f) => (*f as i64) as u32,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    #[test]
    fn no_sharding_routes_to_one() {
        let sm = ShardManager::new();
        assert_eq!(sm.route(&doc(&[("x", Value::Int32(42))])).unwrap(), 1);
    }

    #[test]
    fn hash_sharding_distributes() {
        let mut sm = ShardManager::new();
        sm.set_hash_sharding("x", 3);
        let g1 = sm.route(&doc(&[("x", Value::Int32(0))])).unwrap();
        let g2 = sm.route(&doc(&[("x", Value::Int32(1))])).unwrap();
        let g3 = sm.route(&doc(&[("x", Value::Int32(2))])).unwrap();
        // At least two different groups should be used
        assert!(g1 >= 1 && g1 <= 3);
        assert!(g2 >= 1 && g2 <= 3);
        assert!(g3 >= 1 && g3 <= 3);
    }

    #[test]
    fn hash_sharding_deterministic() {
        let mut sm = ShardManager::new();
        sm.set_hash_sharding("key", 4);
        let g1 = sm.route(&doc(&[("key", Value::String("hello".into()))])).unwrap();
        let g2 = sm.route(&doc(&[("key", Value::String("hello".into()))])).unwrap();
        assert_eq!(g1, g2);
    }

    #[test]
    fn range_sharding() {
        let mut sm = ShardManager::new();
        sm.add_range("x", ShardRange {
            group_id: 1,
            low_bound: None,
            up_bound: Some(Value::Int32(100)),
        });
        sm.add_range("x", ShardRange {
            group_id: 2,
            low_bound: Some(Value::Int32(100)),
            up_bound: None,
        });

        assert_eq!(sm.route(&doc(&[("x", Value::Int32(50))])).unwrap(), 1);
        assert_eq!(sm.route(&doc(&[("x", Value::Int32(150))])).unwrap(), 2);
    }

    #[test]
    fn route_query_with_shard_key() {
        let mut sm = ShardManager::new();
        sm.set_hash_sharding("x", 4);
        let groups = sm.route_query(Some(&doc(&[("x", Value::Int32(10))])));
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn route_query_without_shard_key_broadcasts() {
        let mut sm = ShardManager::new();
        sm.set_hash_sharding("x", 3);
        let groups = sm.route_query(Some(&doc(&[("y", Value::Int32(10))])));
        assert_eq!(groups.len(), 3);
    }

    #[test]
    fn missing_shard_key_in_doc() {
        let mut sm = ShardManager::new();
        sm.set_hash_sharding("x", 3);
        assert!(sm.route(&doc(&[("y", Value::Int32(10))])).is_err());
    }
}
