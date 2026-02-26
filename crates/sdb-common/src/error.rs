use thiserror::Error;

/// Unified error type mirroring SequoiaDB's ossErr.h error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SdbError {
    #[error("OK")]
    Ok,
    #[error("System error")]
    Sys,
    #[error("Out of memory")]
    Oom,
    #[error("Invalid argument")]
    InvalidArg,
    #[error("Permission denied")]
    PermissionDenied,
    #[error("Record not found")]
    NotFound,
    #[error("Collection already exists")]
    CollectionAlreadyExists,
    #[error("Collection space already exists")]
    CollectionSpaceAlreadyExists,
    #[error("Collection not found")]
    CollectionNotFound,
    #[error("Collection space not found")]
    CollectionSpaceNotFound,
    #[error("Duplicate key")]
    DuplicateKey,
    #[error("Network error")]
    NetworkError,
    #[error("Network close")]
    NetworkClose,
    #[error("Timeout")]
    Timeout,
    #[error("Query not found")]
    QueryNotFound,
    #[error("Invalid BSON")]
    InvalidBson,
    #[error("EOF")]
    Eof,
    #[error("Transaction error")]
    TransactionError,
    #[error("Replication error")]
    ReplicationError,
    #[error("Catalog error")]
    CatalogError,
    #[error("Authentication failed")]
    AuthFailed,
    #[error("Not primary")]
    NotPrimary,
    #[error("Node not found")]
    NodeNotFound,
    #[error("Invalid configuration")]
    InvalidConfig,
    #[error("IO error")]
    IoError,
    #[error("Corrupted record")]
    CorruptedRecord,
    #[error("Index not found")]
    IndexNotFound,
    #[error("Index already exists")]
    IndexAlreadyExists,
    #[error("Task not found")]
    TaskNotFound,
    #[error("Chunk is migrating")]
    ChunkMigrating,
    #[error("Not secondary")]
    NotSecondary,
}

pub type Result<T> = std::result::Result<T, SdbError>;
