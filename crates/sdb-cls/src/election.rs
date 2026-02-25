use sdb_common::{NodeAddress, NodeId, Result};

/// Election state for a replica group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionState {
    Primary,
    Secondary,
    Candidate,
    Unknown,
}

/// Manages primary election within a replica group.
pub struct ElectionManager {
    pub local_node: NodeAddress,
    pub state: ElectionState,
    pub primary: Option<NodeId>,
    pub term: u64,
}

impl ElectionManager {
    pub fn new(local_node: NodeAddress) -> Self {
        Self {
            local_node,
            state: ElectionState::Unknown,
            primary: None,
            term: 0,
        }
    }

    pub async fn start_election(&mut self) -> Result<()> {
        // Stub
        self.state = ElectionState::Candidate;
        Ok(())
    }

    pub fn is_primary(&self) -> bool {
        self.state == ElectionState::Primary
    }
}
