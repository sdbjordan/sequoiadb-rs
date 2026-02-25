use thiserror::Error;

#[derive(Debug, Error)]
pub enum BsonError {
    #[error("Invalid BSON data")]
    InvalidData,
    #[error("Unexpected type: expected {expected:?}, got {got:?}")]
    UnexpectedType {
        expected: crate::BsonType,
        got: crate::BsonType,
    },
    #[error("Buffer too short: need {need} bytes, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("Invalid UTF-8 string")]
    InvalidUtf8,
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    #[error("Document too large: {0} bytes")]
    DocumentTooLarge(usize),
}

pub type BsonResult<T> = std::result::Result<T, BsonError>;
