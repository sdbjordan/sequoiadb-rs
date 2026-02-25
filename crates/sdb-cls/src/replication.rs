use sdb_common::{Lsn, NodeAddress, Result};

/// Replication agent — handles log shipping to replicas.
pub struct ReplicationAgent {
    pub local_node: NodeAddress,
    pub replicas: Vec<NodeAddress>,
    pub last_synced_lsn: Lsn,
}

impl ReplicationAgent {
    pub fn new(local_node: NodeAddress) -> Self {
        Self {
            local_node,
            replicas: Vec::new(),
            last_synced_lsn: 0,
        }
    }

    pub fn add_replica(&mut self, node: NodeAddress) {
        self.replicas.push(node);
    }

    pub async fn sync_to(&self, _target: &NodeAddress, _from_lsn: Lsn) -> Result<Lsn> {
        // Stub
        Ok(self.last_synced_lsn)
    }
}
