use std::cmp::Ordering;

use sdb_bson::{Document, Value};

/// Resolve a dot-notation path like "a.b.c" against a document.
/// Returns None if any intermediate key is missing or not a document.
pub fn resolve_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Value> {
    let mut parts = path.splitn(2, '.');
    let key = parts.next()?;
    let val = doc.get(key)?;
    match parts.next() {
        None => Some(val),
        Some(rest) => match val {
            Value::Document(inner) => resolve_path(inner, rest),
            _ => None,
        },
    }
}

/// Convert a numeric Value to f64 for cross-type comparison.
fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int32(n) => Some(*n as f64),
        Value::Int64(n) => Some(*n as f64),
        Value::Double(n) => Some(*n),
        _ => None,
    }
}

/// BSON type ordering for cross-type comparison.
/// MinKey < Null < Numbers < String < Object < Array < Binary < ObjectId
/// < Boolean < Date < Timestamp < Regex < MaxKey
fn type_order(v: &Value) -> u8 {
    match v {
        Value::MinKey => 0,
        Value::Null => 1,
        Value::Int32(_) | Value::Int64(_) | Value::Double(_) => 2,
        Value::String(_) => 3,
        Value::Document(_) => 4,
        Value::Array(_) => 5,
        Value::Binary(_) => 6,
        Value::ObjectId(_) => 7,
        Value::Boolean(_) => 8,
        Value::Date(_) => 9,
        Value::Timestamp(_) => 10,
        Value::Regex { .. } => 11,
        Value::Decimal(_) => 12,
        Value::MaxKey => 255,
    }
}

/// Check if two values are in the same comparable numeric group.
fn both_numeric(a: &Value, b: &Value) -> bool {
    matches!(
        (a, b),
        (
            Value::Int32(_) | Value::Int64(_) | Value::Double(_),
            Value::Int32(_) | Value::Int64(_) | Value::Double(_)
        )
    )
}

/// Compare two BSON values, returning an Ordering.
/// - Same-type values compared naturally.
/// - Numeric types (i32/i64/f64) promoted to f64 for cross-type comparison.
/// - Different type groups ordered by BSON type order.
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    // Numeric cross-type promotion
    if both_numeric(a, b) {
        let fa = to_f64(a).unwrap();
        let fb = to_f64(b).unwrap();
        return fa.partial_cmp(&fb).unwrap_or(Ordering::Equal);
    }

    let ta = type_order(a);
    let tb = type_order(b);
    if ta != tb {
        return ta.cmp(&tb);
    }

    // Same type group — compare within type
    match (a, b) {
        (Value::MinKey, Value::MinKey) => Ordering::Equal,
        (Value::MaxKey, Value::MaxKey) => Ordering::Equal,
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::ObjectId(a), Value::ObjectId(b)) => a.bytes.cmp(&b.bytes),
        (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

/// Equality check: same-type PartialEq, with numeric cross-type promotion.
pub fn values_equal(a: &Value, b: &Value) -> bool {
    if both_numeric(a, b) {
        let fa = to_f64(a).unwrap();
        let fb = to_f64(b).unwrap();
        return fa == fb;
    }
    a == b
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::DocumentBuilder;

    #[test]
    fn resolve_simple_path() {
        let doc = DocumentBuilder::new()
            .append_i32("age", 25)
            .append_string("name", "alice")
            .build();
        assert_eq!(resolve_path(&doc, "age"), Some(&Value::Int32(25)));
        assert_eq!(
            resolve_path(&doc, "name"),
            Some(&Value::String("alice".into()))
        );
        assert_eq!(resolve_path(&doc, "missing"), None);
    }

    #[test]
    fn resolve_nested_path() {
        let inner = DocumentBuilder::new().append_i32("c", 42).build();
        let mid = DocumentBuilder::new().append_document("b", inner).build();
        let doc = DocumentBuilder::new().append_document("a", mid).build();
        assert_eq!(resolve_path(&doc, "a.b.c"), Some(&Value::Int32(42)));
        assert_eq!(resolve_path(&doc, "a.b.missing"), None);
        assert_eq!(resolve_path(&doc, "a.missing.c"), None);
    }

    #[test]
    fn compare_same_type() {
        assert_eq!(
            compare_values(&Value::Int32(1), &Value::Int32(2)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Int32(5), &Value::Int32(5)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::String("a".into()), &Value::String("b".into())),
            Ordering::Less
        );
    }

    #[test]
    fn compare_numeric_cross_type() {
        assert_eq!(
            compare_values(&Value::Int32(1), &Value::Int64(2)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Int32(5), &Value::Double(5.0)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Double(1.5), &Value::Int32(2)),
            Ordering::Less
        );
    }

    #[test]
    fn compare_different_type_groups() {
        // Null < Number
        assert_eq!(
            compare_values(&Value::Null, &Value::Int32(1)),
            Ordering::Less
        );
        // Number < String
        assert_eq!(
            compare_values(&Value::Int32(999), &Value::String("a".into())),
            Ordering::Less
        );
        // MinKey < everything < MaxKey
        assert_eq!(compare_values(&Value::MinKey, &Value::Null), Ordering::Less);
        assert_eq!(
            compare_values(&Value::Int32(0), &Value::MaxKey),
            Ordering::Less
        );
    }

    #[test]
    fn values_equal_cross_numeric() {
        assert!(values_equal(&Value::Int32(10), &Value::Int64(10)));
        assert!(values_equal(&Value::Int32(3), &Value::Double(3.0)));
        assert!(!values_equal(&Value::Int32(3), &Value::Double(3.1)));
    }

    #[test]
    fn values_equal_different_types() {
        assert!(!values_equal(&Value::Int32(1), &Value::String("1".into())));
        assert!(!values_equal(&Value::Null, &Value::Boolean(false)));
    }
}
