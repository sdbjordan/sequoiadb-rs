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

/// A single log record in the write-ahead log.
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub txn_id: u64,
    pub op: LogOp,
    pub data: Vec<u8>,
}
