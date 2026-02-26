use sdb_common::{NodeAddress, NodeId, Result, SdbError};

/// Election state for a replica group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionState {
    Primary,
    Secondary,
    Candidate,
    Unknown,
}

/// Manages primary election within a replica group.
/// V1: single-node simulation. In a real system this would use Raft or Bully.
pub struct ElectionManager {
    pub local_node: NodeAddress,
    pub state: ElectionState,
    pub primary: Option<NodeId>,
    pub term: u64,
    pub peers: Vec<NodeAddress>,
    pub votes_received: u32,
}

impl ElectionManager {
    pub fn new(local_node: NodeAddress) -> Self {
        Self {
            local_node,
            state: ElectionState::Unknown,
            primary: None,
            term: 0,
            peers: Vec::new(),
            votes_received: 0,
        }
    }

    /// Add a peer node to the replica group.
    pub fn add_peer(&mut self, node: NodeAddress) {
        self.peers.push(node);
    }

    /// Start an election. The local node becomes a candidate.
    /// In v1, if we have a majority (or no peers), we immediately win.
    pub async fn start_election(&mut self) -> Result<()> {
        self.term += 1;
        self.state = ElectionState::Candidate;
        self.votes_received = 1; // vote for self

        let total = self.peers.len() as u32 + 1; // self + peers
        let majority = total / 2 + 1;

        if self.votes_received >= majority {
            self.promote_to_primary();
        }

        Ok(())
    }

    /// Record a vote from a peer. If majority reached, become primary.
    pub fn receive_vote(&mut self, _from: NodeId) -> Result<()> {
        if self.state != ElectionState::Candidate {
            return Err(SdbError::InvalidArg);
        }
        self.votes_received += 1;
        let total = self.peers.len() as u32 + 1;
        let majority = total / 2 + 1;
        if self.votes_received >= majority {
            self.promote_to_primary();
        }
        Ok(())
    }

    /// Step down from primary to secondary.
    pub fn step_down(&mut self) {
        self.state = ElectionState::Secondary;
        self.primary = None;
    }

    /// Accept another node as primary (e.g., after receiving a higher term).
    pub fn accept_primary(&mut self, primary_id: NodeId, term: u64) {
        if term >= self.term {
            self.term = term;
            self.state = ElectionState::Secondary;
            self.primary = Some(primary_id);
        }
    }

    pub fn is_primary(&self) -> bool {
        self.state == ElectionState::Primary
    }

    pub fn current_term(&self) -> u64 {
        self.term
    }

    fn promote_to_primary(&mut self) {
        self.state = ElectionState::Primary;
        self.primary = Some(self.local_node.node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(group: u32, node: u16) -> NodeAddress {
        NodeAddress { group_id: group, node_id: node }
    }

    #[tokio::test]
    async fn single_node_election() {
        let mut em = ElectionManager::new(addr(1, 1));
        assert_eq!(em.state, ElectionState::Unknown);

        em.start_election().await.unwrap();
        // Single node = majority of 1, should be primary
        assert!(em.is_primary());
        assert_eq!(em.term, 1);
    }

    #[tokio::test]
    async fn three_node_needs_majority() {
        let mut em = ElectionManager::new(addr(1, 1));
        em.add_peer(addr(1, 2));
        em.add_peer(addr(1, 3));

        em.start_election().await.unwrap();
        // 1 vote (self) out of 3 — not enough
        assert_eq!(em.state, ElectionState::Candidate);

        // Receive one more vote → 2/3 = majority
        em.receive_vote(2).unwrap();
        assert!(em.is_primary());
    }

    #[test]
    fn step_down() {
        let mut em = ElectionManager::new(addr(1, 1));
        em.state = ElectionState::Primary;
        em.primary = Some(1);
        em.step_down();
        assert_eq!(em.state, ElectionState::Secondary);
        assert!(em.primary.is_none());
    }

    #[test]
    fn accept_primary() {
        let mut em = ElectionManager::new(addr(1, 2));
        em.accept_primary(1, 5);
        assert_eq!(em.state, ElectionState::Secondary);
        assert_eq!(em.primary, Some(1));
        assert_eq!(em.term, 5);
    }

    #[test]
    fn ignore_lower_term() {
        let mut em = ElectionManager::new(addr(1, 2));
        em.term = 10;
        em.state = ElectionState::Primary;
        em.accept_primary(3, 5); // lower term, ignored
        assert_eq!(em.state, ElectionState::Primary);
    }

    #[test]
    fn vote_not_candidate() {
        let mut em = ElectionManager::new(addr(1, 1));
        em.state = ElectionState::Secondary;
        assert!(em.receive_vote(2).is_err());
    }
}
