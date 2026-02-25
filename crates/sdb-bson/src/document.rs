use crate::decode::Decode;
use crate::element::{Element, Value};
use crate::encode::Encode;
use crate::error::BsonResult;

/// A BSON document — an ordered list of key-value elements.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Document {
    elements: Vec<Element>,
}

impl Document {
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.elements
            .iter()
            .find(|e| e.key == key)
            .map(|e| &e.value)
    }

    /// Insert a key-value pair. Appends to the end (preserves insertion order).
    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        self.elements.push(Element {
            key: key.into(),
            value,
        });
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns true if the document has no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Iterate over elements.
    pub fn iter(&self) -> impl Iterator<Item = &Element> {
        self.elements.iter()
    }

    /// Encode this document to BSON bytes.
    pub fn to_bytes(&self) -> BsonResult<Vec<u8>> {
        let mut buf = Vec::new();
        self.encode(&mut buf)?;
        Ok(buf)
    }

    /// Decode a document from BSON bytes.
    pub fn from_bytes(buf: &[u8]) -> BsonResult<Self> {
        Self::decode(buf)
    }
}
