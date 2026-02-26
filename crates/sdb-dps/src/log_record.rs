use sdb_common::Lsn;

/// Operation type for a WAL log record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogOp {
    Insert,
    Update,
    Delete,
    TxnBegin,
    TxnCommit,
    TxnAbort,
    CollectionCreate,
    CollectionDrop,
    IndexCreate,
    IndexDrop,
}

impl LogOp {
    /// Human-readable operation name for oplog output.
    pub fn as_str(&self) -> &'static str {
        match self {
            LogOp::Insert => "insert",
            LogOp::Update => "update",
            LogOp::Delete => "delete",
            LogOp::TxnBegin => "txnBegin",
            LogOp::TxnCommit => "txnCommit",
            LogOp::TxnAbort => "txnAbort",
            LogOp::CollectionCreate => "createCL",
            LogOp::CollectionDrop => "dropCL",
            LogOp::IndexCreate => "createIndex",
            LogOp::IndexDrop => "dropIndex",
        }
    }
}

/// A single log record in the write-ahead log.
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub txn_id: u64,
    pub op: LogOp,
    pub data: Vec<u8>,
}
