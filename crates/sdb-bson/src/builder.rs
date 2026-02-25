use crate::document::Document;
use crate::element::Value;
use crate::types::ObjectId;

/// Fluent builder for constructing BSON documents.
pub struct DocumentBuilder {
    doc: Document,
}

impl DocumentBuilder {
    pub fn new() -> Self {
        Self {
            doc: Document::new(),
        }
    }

    pub fn append_double(mut self, key: impl Into<String>, val: f64) -> Self {
        self.doc.insert(key, Value::Double(val));
        self
    }

    pub fn append_string(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.doc.insert(key, Value::String(val.into()));
        self
    }

    pub fn append_document(mut self, key: impl Into<String>, val: Document) -> Self {
        self.doc.insert(key, Value::Document(val));
        self
    }

    pub fn append_array(mut self, key: impl Into<String>, val: Vec<Value>) -> Self {
        self.doc.insert(key, Value::Array(val));
        self
    }

    pub fn append_binary(mut self, key: impl Into<String>, val: Vec<u8>) -> Self {
        self.doc.insert(key, Value::Binary(val));
        self
    }

    pub fn append_object_id(mut self, key: impl Into<String>, val: ObjectId) -> Self {
        self.doc.insert(key, Value::ObjectId(val));
        self
    }

    pub fn append_bool(mut self, key: impl Into<String>, val: bool) -> Self {
        self.doc.insert(key, Value::Boolean(val));
        self
    }

    pub fn append_date(mut self, key: impl Into<String>, val: i64) -> Self {
        self.doc.insert(key, Value::Date(val));
        self
    }

    pub fn append_null(mut self, key: impl Into<String>) -> Self {
        self.doc.insert(key, Value::Null);
        self
    }

    pub fn append_i32(mut self, key: impl Into<String>, val: i32) -> Self {
        self.doc.insert(key, Value::Int32(val));
        self
    }

    pub fn append_i64(mut self, key: impl Into<String>, val: i64) -> Self {
        self.doc.insert(key, Value::Int64(val));
        self
    }

    pub fn append_timestamp(mut self, key: impl Into<String>, val: u64) -> Self {
        self.doc.insert(key, Value::Timestamp(val));
        self
    }

    pub fn build(self) -> Document {
        self.doc
    }
}

impl Default for DocumentBuilder {
    fn default() -> Self {
        Self::new()
    }
}
