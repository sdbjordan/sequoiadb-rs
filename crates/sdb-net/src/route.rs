use sdb_common::{GroupId, NodeAddress, NodeId, Result};
use std::collections::HashMap;
use std::net::SocketAddr;

/// Node routing table — maps node addresses to socket addresses.
pub struct NetRoute {
    routes: HashMap<NodeAddress, SocketAddr>,
}

impl NetRoute {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }

    pub fn add(&mut self, node: NodeAddress, addr: SocketAddr) {
        self.routes.insert(node, addr);
    }

    pub fn remove(&mut self, node: &NodeAddress) {
        self.routes.remove(node);
    }

    pub fn lookup(&self, node: &NodeAddress) -> Result<SocketAddr> {
        self.routes
            .get(node)
            .copied()
            .ok_or(sdb_common::SdbError::NodeNotFound)
    }

    pub fn get_group_nodes(&self, group_id: GroupId) -> Vec<(NodeId, SocketAddr)> {
        self.routes
            .iter()
            .filter(|(addr, _)| addr.group_id == group_id)
            .map(|(addr, sock)| (addr.node_id, *sock))
            .collect()
    }
}

impl Default for NetRoute {
    fn default() -> Self {
        Self::new()
    }
}
