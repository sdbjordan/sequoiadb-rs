use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use sdb_bson::{Document, Value};
use sdb_bson::types::ObjectId;
use serde_json::{json, Map, Value as JsonValue};

/// Convert an sdb_bson::Value to a serde_json::Value.
pub fn value_to_json(val: &Value) -> JsonValue {
    match val {
        Value::Double(v) => json!(*v),
        Value::String(v) => JsonValue::String(v.clone()),
        Value::Document(doc) => doc_to_json(doc),
        Value::Array(arr) => JsonValue::Array(arr.iter().map(value_to_json).collect()),
        Value::Boolean(v) => JsonValue::Bool(*v),
        Value::Int32(v) => json!(*v),
        Value::Int64(v) => json!(*v),
        Value::Null => JsonValue::Null,
        Value::ObjectId(oid) => {
            let hex: String = oid.bytes.iter().map(|b| format!("{b:02x}")).collect();
            json!({ "$oid": hex })
        }
        Value::Date(ms) => json!({ "$date": *ms }),
        Value::Binary(data) => json!({ "$binary": BASE64.encode(data) }),
        Value::Regex { pattern, options } => {
            json!({ "$regex": pattern, "$options": options })
        }
        Value::Timestamp(ts) => json!({ "$timestamp": *ts }),
        Value::Decimal(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            json!({ "$decimal": hex })
        }
        Value::MinKey => json!({ "$minKey": 1 }),
        Value::MaxKey => json!({ "$maxKey": 1 }),
    }
}

/// Convert an sdb_bson::Document to a serde_json::Value (Object).
pub fn doc_to_json(doc: &Document) -> JsonValue {
    let mut map = Map::new();
    for elem in doc.iter() {
        map.insert(elem.key.clone(), value_to_json(&elem.value));
    }
    JsonValue::Object(map)
}

/// Convert a serde_json::Value to an sdb_bson::Value.
pub fn json_to_value(val: &JsonValue) -> Value {
    match val {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                    Value::Int32(i as i32)
                } else {
                    Value::Int64(i)
                }
            } else {
                Value::Double(n.as_f64().unwrap_or(0.0))
            }
        }
        JsonValue::String(s) => Value::String(s.clone()),
        JsonValue::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        JsonValue::Object(map) => {
            // Check for extended JSON types
            if let Some(oid) = map.get("$oid").and_then(|v| v.as_str()) {
                return parse_oid(oid);
            }
            if let Some(ms) = map.get("$date").and_then(|v| v.as_i64()) {
                return Value::Date(ms);
            }
            if let Some(b64) = map.get("$binary").and_then(|v| v.as_str()) {
                if let Ok(bytes) = BASE64.decode(b64) {
                    return Value::Binary(bytes);
                }
            }
            if let Some(pattern) = map.get("$regex").and_then(|v| v.as_str()) {
                let options = map
                    .get("$options")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                return Value::Regex {
                    pattern: pattern.to_string(),
                    options,
                };
            }
            if let Some(ts) = map.get("$timestamp").and_then(|v| v.as_u64()) {
                return Value::Timestamp(ts);
            }
            if let Some(hex) = map.get("$decimal").and_then(|v| v.as_str()) {
                if let Some(bytes) = hex_decode(hex) {
                    return Value::Decimal(bytes);
                }
            }
            if map.get("$minKey").is_some() {
                return Value::MinKey;
            }
            if map.get("$maxKey").is_some() {
                return Value::MaxKey;
            }
            // Regular document
            Value::Document(json_map_to_doc(map))
        }
    }
}

/// Convert a serde_json::Value (must be Object) to an sdb_bson::Document.
pub fn json_to_doc(val: &JsonValue) -> Result<Document, String> {
    match val {
        JsonValue::Object(map) => Ok(json_map_to_doc(map)),
        _ => Err("expected JSON object".to_string()),
    }
}

fn json_map_to_doc(map: &Map<String, JsonValue>) -> Document {
    let mut doc = Document::new();
    for (k, v) in map {
        doc.insert(k.clone(), json_to_value(v));
    }
    doc
}

fn parse_oid(hex: &str) -> Value {
    let bytes = hex_decode(hex).unwrap_or_else(|| vec![0u8; 12]);
    let mut oid_bytes = [0u8; 12];
    let len = bytes.len().min(12);
    oid_bytes[..len].copy_from_slice(&bytes[..len]);
    Value::ObjectId(ObjectId { bytes: oid_bytes })
}

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_primitives() {
        let mut doc = Document::new();
        doc.insert("str", Value::String("hello".into()));
        doc.insert("i32", Value::Int32(42));
        doc.insert("i64", Value::Int64(i64::MAX));
        doc.insert("f64", Value::Double(3.14));
        doc.insert("bool", Value::Boolean(true));
        doc.insert("null", Value::Null);

        let json = doc_to_json(&doc);
        let back = json_to_doc(&json).unwrap();

        assert_eq!(back.get("str"), Some(&Value::String("hello".into())));
        assert_eq!(back.get("i32"), Some(&Value::Int32(42)));
        assert_eq!(back.get("i64"), Some(&Value::Int64(i64::MAX)));
        assert_eq!(back.get("f64"), Some(&Value::Double(3.14)));
        assert_eq!(back.get("bool"), Some(&Value::Boolean(true)));
        assert_eq!(back.get("null"), Some(&Value::Null));
    }

    #[test]
    fn test_roundtrip_object_id() {
        let oid = ObjectId {
            bytes: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        };
        let val = Value::ObjectId(oid);
        let json = value_to_json(&val);
        let back = json_to_value(&json);
        assert_eq!(back, val);
    }

    #[test]
    fn test_roundtrip_date() {
        let val = Value::Date(1700000000000);
        let json = value_to_json(&val);
        let back = json_to_value(&json);
        assert_eq!(back, val);
    }

    #[test]
    fn test_roundtrip_binary() {
        let val = Value::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let json = value_to_json(&val);
        let back = json_to_value(&json);
        assert_eq!(back, val);
    }

    #[test]
    fn test_roundtrip_regex() {
        let val = Value::Regex {
            pattern: "^abc".into(),
            options: "i".into(),
        };
        let json = value_to_json(&val);
        let back = json_to_value(&json);
        assert_eq!(back, val);
    }

    #[test]
    fn test_roundtrip_nested_doc() {
        let mut inner = Document::new();
        inner.insert("x", Value::Int32(1));

        let mut doc = Document::new();
        doc.insert("nested", Value::Document(inner));
        doc.insert("arr", Value::Array(vec![Value::Int32(1), Value::String("two".into())]));

        let json = doc_to_json(&doc);
        let back = json_to_doc(&json).unwrap();

        // Check nested doc
        if let Some(Value::Document(d)) = back.get("nested") {
            assert_eq!(d.get("x"), Some(&Value::Int32(1)));
        } else {
            panic!("expected nested document");
        }

        // Check array
        if let Some(Value::Array(a)) = back.get("arr") {
            assert_eq!(a.len(), 2);
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_roundtrip_special_types() {
        let val = Value::Timestamp(123456789);
        assert_eq!(json_to_value(&value_to_json(&val)), val);

        let val = Value::MinKey;
        assert_eq!(json_to_value(&value_to_json(&val)), val);

        let val = Value::MaxKey;
        assert_eq!(json_to_value(&value_to_json(&val)), val);
    }

    #[test]
    fn test_json_number_to_int32_or_int64() {
        // Small number → Int32
        let j: JsonValue = serde_json::from_str("42").unwrap();
        assert_eq!(json_to_value(&j), Value::Int32(42));

        // Large number → Int64
        let j: JsonValue = serde_json::from_str("3000000000").unwrap();
        assert_eq!(json_to_value(&j), Value::Int64(3000000000));

        // Float → Double
        let j: JsonValue = serde_json::from_str("3.14").unwrap();
        assert_eq!(json_to_value(&j), Value::Double(3.14));
    }
}
