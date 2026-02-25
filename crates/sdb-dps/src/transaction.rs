use sdb_common::Result;
use std::collections::HashMap;

/// Transaction states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed,
    Aborted,
}

/// Manages active transactions.
pub struct TransactionManager {
    active_txns: HashMap<u64, TxnState>,
    next_txn_id: u64,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_txns: HashMap::new(),
            next_txn_id: 1,
        }
    }

    pub fn begin(&mut self) -> Result<u64> {
        let id = self.next_txn_id;
        self.next_txn_id += 1;
        self.active_txns.insert(id, TxnState::Active);
        Ok(id)
    }

    pub fn commit(&mut self, txn_id: u64) -> Result<()> {
        self.active_txns
            .get_mut(&txn_id)
            .map(|s| *s = TxnState::Committed)
            .ok_or(sdb_common::SdbError::TransactionError)
    }

    pub fn abort(&mut self, txn_id: u64) -> Result<()> {
        self.active_txns
            .get_mut(&txn_id)
            .map(|s| *s = TxnState::Aborted)
            .ok_or(sdb_common::SdbError::TransactionError)
    }

    pub fn is_active(&self, txn_id: u64) -> bool {
        self.active_txns.get(&txn_id) == Some(&TxnState::Active)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
