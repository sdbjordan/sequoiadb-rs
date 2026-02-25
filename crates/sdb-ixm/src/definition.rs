use sdb_bson::Document;
use sdb_bson::element::Value;

use crate::key::IndexKey;

/// Definition of an index on a collection.
#[derive(Debug, Clone)]
pub struct IndexDefinition {
    pub name: String,
    pub key_pattern: Document,
    pub unique: bool,
    pub enforced: bool,
    pub not_null: bool,
}

impl IndexDefinition {
    /// Extract sort directions from key_pattern.
    /// Each field value should be 1 (asc) or -1 (desc).
    /// Returns vec of i8 directions.
    pub fn directions(&self) -> Vec<i8> {
        self.key_pattern
            .iter()
            .map(|elem| match &elem.value {
                Value::Int32(v) if *v < 0 => -1i8,
                Value::Int64(v) if *v < 0 => -1i8,
                Value::Double(v) if *v < 0.0 => -1i8,
                _ => 1i8,
            })
            .collect()
    }

    /// Extract an IndexKey from a document based on this index's key_pattern.
    /// Missing fields become Null.
    pub fn extract_key(&self, doc: &Document) -> IndexKey {
        let fields: Vec<Value> = self
            .key_pattern
            .iter()
            .map(|elem| {
                doc.get(&elem.key)
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect();
        IndexKey::new(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_def(fields: &[(&str, i32)]) -> IndexDefinition {
        let mut kp = Document::new();
        for (name, dir) in fields {
            kp.insert(*name, Value::Int32(*dir));
        }
        IndexDefinition {
            name: "test_idx".to_string(),
            key_pattern: kp,
            unique: false,
            enforced: false,
            not_null: false,
        }
    }

    #[test]
    fn test_directions() {
        let def = make_def(&[("a", 1), ("b", -1), ("c", 1)]);
        assert_eq!(def.directions(), vec![1, -1, 1]);
    }

    #[test]
    fn test_extract_key() {
        let def = make_def(&[("name", 1), ("age", 1)]);
        let mut doc = Document::new();
        doc.insert("name", Value::String("alice".into()));
        doc.insert("age", Value::Int32(30));
        doc.insert("extra", Value::Boolean(true));

        let key = def.extract_key(&doc);
        assert_eq!(key.fields.len(), 2);
    }

    #[test]
    fn test_extract_key_missing_field() {
        let def = make_def(&[("name", 1), ("age", 1)]);
        let mut doc = Document::new();
        doc.insert("name", Value::String("bob".into()));
        // age is missing → Null

        let key = def.extract_key(&doc);
        assert_eq!(key.fields.len(), 2);
        assert_eq!(key.fields[1], Value::Null);
    }
}
