use sdb_bson::element::Value;

/// An index key extracted from a document.
#[derive(Debug, Clone)]
pub struct IndexKey {
    pub fields: Vec<Value>,
}

/// Range of keys for index scan.
#[derive(Debug, Clone)]
pub struct KeyRange {
    pub start: Option<IndexKey>,
    pub end: Option<IndexKey>,
    pub start_inclusive: bool,
    pub end_inclusive: bool,
}
