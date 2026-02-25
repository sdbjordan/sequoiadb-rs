/// Snapshot types for monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotType {
    Contexts,
    Sessions,
    Collections,
    CollectionSpaces,
    Database,
    System,
    Transactions,
    Health,
}

/// A point-in-time snapshot of system state.
pub struct Snapshot {
    pub snapshot_type: SnapshotType,
    pub timestamp: u64,
}

impl Snapshot {
    pub fn new(snapshot_type: SnapshotType) -> Self {
        Self {
            snapshot_type,
            timestamp: 0, // Stub
        }
    }
}
