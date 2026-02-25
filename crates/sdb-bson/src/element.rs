use crate::types::{BsonType, ObjectId};

/// A single BSON element (key-value pair within a document).
#[derive(Debug, Clone, PartialEq)]
pub struct Element {
    pub key: String,
    pub value: Value,
}

/// BSON value variants.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Double(f64),
    String(String),
    Document(crate::Document),
    Array(Vec<Value>),
    Binary(Vec<u8>),
    ObjectId(ObjectId),
    Boolean(bool),
    Date(i64),
    Null,
    Regex { pattern: String, options: String },
    Int32(i32),
    Timestamp(u64),
    Int64(i64),
    Decimal(Vec<u8>),
    MinKey,
    MaxKey,
}

impl Value {
    /// Returns the BSON type tag for this value.
    pub fn bson_type(&self) -> BsonType {
        match self {
            Value::Double(_) => BsonType::Double,
            Value::String(_) => BsonType::String,
            Value::Document(_) => BsonType::Object,
            Value::Array(_) => BsonType::Array,
            Value::Binary(_) => BsonType::Binary,
            Value::ObjectId(_) => BsonType::ObjectId,
            Value::Boolean(_) => BsonType::Boolean,
            Value::Date(_) => BsonType::Date,
            Value::Null => BsonType::Null,
            Value::Regex { .. } => BsonType::Regex,
            Value::Int32(_) => BsonType::Int32,
            Value::Timestamp(_) => BsonType::Timestamp,
            Value::Int64(_) => BsonType::Int64,
            Value::Decimal(_) => BsonType::Decimal,
            Value::MinKey => BsonType::MinKey,
            Value::MaxKey => BsonType::MaxKey,
        }
    }
}
