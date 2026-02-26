use std::time::{SystemTime, UNIX_EPOCH};

use sdb_bson::{Document, Value};

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
    pub details: Vec<Document>,
}

impl Snapshot {
    /// Create a new snapshot with the current timestamp.
    pub fn new(snapshot_type: SnapshotType) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            snapshot_type,
            timestamp,
            details: Vec::new(),
        }
    }

    /// Create a snapshot with pre-existing detail documents.
    pub fn with_details(snapshot_type: SnapshotType, details: Vec<Document>) -> Self {
        let mut snap = Self::new(snapshot_type);
        snap.details = details;
        snap
    }

    /// Convert the snapshot into a summary BSON document.
    pub fn to_document(&self) -> Document {
        let type_str = match self.snapshot_type {
            SnapshotType::Contexts => "Contexts",
            SnapshotType::Sessions => "Sessions",
            SnapshotType::Collections => "Collections",
            SnapshotType::CollectionSpaces => "CollectionSpaces",
            SnapshotType::Database => "Database",
            SnapshotType::System => "System",
            SnapshotType::Transactions => "Transactions",
            SnapshotType::Health => "Health",
        };
        let mut doc = Document::new();
        doc.insert("Type", Value::String(type_str.into()));
        doc.insert("Timestamp", Value::Int64(self.timestamp as i64));
        doc.insert("Count", Value::Int32(self.details.len() as i32));
        doc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_has_real_timestamp() {
        let snap = Snapshot::new(SnapshotType::Database);
        assert!(snap.timestamp > 0);
    }

    #[test]
    fn snapshot_types_distinct() {
        assert_ne!(SnapshotType::Contexts, SnapshotType::Sessions);
        assert_eq!(SnapshotType::Health, SnapshotType::Health);
    }

    #[test]
    fn snapshot_to_document() {
        let snap = Snapshot::new(SnapshotType::Sessions);
        let doc = snap.to_document();
        assert_eq!(doc.get("Type"), Some(&Value::String("Sessions".into())));
        assert_eq!(doc.get("Count"), Some(&Value::Int32(0)));
    }

    #[test]
    fn snapshot_with_details() {
        let mut detail = Document::new();
        detail.insert("id", Value::Int64(42));
        let snap = Snapshot::with_details(SnapshotType::Sessions, vec![detail]);
        assert_eq!(snap.details.len(), 1);
        assert_eq!(snap.to_document().get("Count"), Some(&Value::Int32(1)));
    }
}
