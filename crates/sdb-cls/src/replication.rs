use sdb_common::{Lsn, NodeAddress, Result, SdbError};
use std::collections::HashMap;

/// Replica sync status for a single peer.
#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub node: NodeAddress,
    pub synced_lsn: Lsn,
    pub is_alive: bool,
}

/// Replication agent — handles log shipping to replicas.
pub struct ReplicationAgent {
    pub local_node: NodeAddress,
    pub local_lsn: Lsn,
    pub replicas: HashMap<u16, ReplicaStatus>,
}

impl ReplicationAgent {
    pub fn new(local_node: NodeAddress) -> Self {
        Self {
            local_node,
            local_lsn: 0,
            replicas: HashMap::new(),
        }
    }

    /// Add a replica peer.
    pub fn add_replica(&mut self, node: NodeAddress) {
        self.replicas.insert(node.node_id, ReplicaStatus {
            node,
            synced_lsn: 0,
            is_alive: true,
        });
    }

    /// Remove a replica peer.
    pub fn remove_replica(&mut self, node_id: u16) -> Result<()> {
        self.replicas.remove(&node_id).ok_or(SdbError::NodeNotFound)?;
        Ok(())
    }

    /// Record that the local node has advanced to a new LSN.
    pub fn advance_local_lsn(&mut self, lsn: Lsn) {
        self.local_lsn = lsn;
    }

    /// Simulate sending logs to a replica. Returns the new synced LSN.
    /// In a real system this would ship WAL records over the network.
    pub async fn sync_to(&mut self, target_id: u16, up_to_lsn: Lsn) -> Result<Lsn> {
        let replica = self.replicas.get_mut(&target_id).ok_or(SdbError::NodeNotFound)?;
        if !replica.is_alive {
            return Err(SdbError::NetworkError);
        }
        // Simulate: instantly sync up to the requested LSN (capped at local)
        let new_lsn = up_to_lsn.min(self.local_lsn);
        replica.synced_lsn = new_lsn;
        Ok(new_lsn)
    }

    /// Sync all replicas to the current local LSN.
    pub async fn sync_all(&mut self) -> Result<()> {
        let target_lsn = self.local_lsn;
        let node_ids: Vec<u16> = self.replicas.keys().copied().collect();
        for nid in node_ids {
            let _ = self.sync_to(nid, target_lsn).await;
        }
        Ok(())
    }

    /// Mark a replica as alive or dead.
    pub fn set_replica_alive(&mut self, node_id: u16, alive: bool) -> Result<()> {
        let replica = self.replicas.get_mut(&node_id).ok_or(SdbError::NodeNotFound)?;
        replica.is_alive = alive;
        Ok(())
    }

    /// Get the minimum synced LSN across all alive replicas.
    pub fn min_synced_lsn(&self) -> Lsn {
        self.replicas.values()
            .filter(|r| r.is_alive)
            .map(|r| r.synced_lsn)
            .min()
            .unwrap_or(0)
    }

    /// Get the number of replicas that have synced up to at least the given LSN.
    pub fn count_synced(&self, lsn: Lsn) -> usize {
        self.replicas.values()
            .filter(|r| r.is_alive && r.synced_lsn >= lsn)
            .count()
    }

    /// Check if a write is durable (majority of replicas have synced).
    pub fn is_majority_synced(&self, lsn: Lsn) -> bool {
        let total = self.replicas.len() + 1; // include self
        let synced = self.count_synced(lsn) + 1; // self is always synced
        synced > total / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(node: u16) -> NodeAddress {
        NodeAddress { group_id: 1, node_id: node }
    }

    #[tokio::test]
    async fn add_and_sync_replica() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.advance_local_lsn(100);

        let lsn = agent.sync_to(2, 100).await.unwrap();
        assert_eq!(lsn, 100);
        assert_eq!(agent.replicas[&2].synced_lsn, 100);
    }

    #[tokio::test]
    async fn sync_capped_at_local_lsn() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.advance_local_lsn(50);

        let lsn = agent.sync_to(2, 100).await.unwrap();
        assert_eq!(lsn, 50); // capped
    }

    #[tokio::test]
    async fn sync_dead_replica_fails() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.set_replica_alive(2, false).unwrap();
        agent.advance_local_lsn(100);

        assert!(agent.sync_to(2, 100).await.is_err());
    }

    #[tokio::test]
    async fn sync_all() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.add_replica(addr(3));
        agent.advance_local_lsn(200);

        agent.sync_all().await.unwrap();
        assert_eq!(agent.replicas[&2].synced_lsn, 200);
        assert_eq!(agent.replicas[&3].synced_lsn, 200);
    }

    #[test]
    fn min_synced_lsn() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.add_replica(addr(3));
        agent.replicas.get_mut(&2).unwrap().synced_lsn = 100;
        agent.replicas.get_mut(&3).unwrap().synced_lsn = 50;
        assert_eq!(agent.min_synced_lsn(), 50);
    }

    #[test]
    fn majority_synced() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        agent.add_replica(addr(3));
        agent.advance_local_lsn(100);
        // self(100) + node2(0) + node3(0) — only self synced
        assert!(!agent.is_majority_synced(100));

        agent.replicas.get_mut(&2).unwrap().synced_lsn = 100;
        // self(100) + node2(100) + node3(0) — 2/3 = majority
        assert!(agent.is_majority_synced(100));
    }

    #[test]
    fn remove_replica() {
        let mut agent = ReplicationAgent::new(addr(1));
        agent.add_replica(addr(2));
        assert!(agent.remove_replica(2).is_ok());
        assert!(agent.remove_replica(2).is_err());
    }
}
