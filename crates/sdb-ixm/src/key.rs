use std::cmp::Ordering;

use sdb_bson::element::Value;
use sdb_common::{Result, SdbError};

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

// --- Type tag constants for key encoding ---
const TAG_NULL: u8 = 0;
const TAG_MIN_KEY: u8 = 1;
const TAG_MAX_KEY: u8 = 2;
const TAG_BOOLEAN: u8 = 3;
const TAG_INT32: u8 = 4;
const TAG_INT64: u8 = 5;
const TAG_DOUBLE: u8 = 6;
const TAG_STRING: u8 = 7;

/// BSON type ordering for comparison (MongoDB-compatible):
/// MinKey < Null < Number < String < Boolean < MaxKey
fn bson_type_order(v: &Value) -> u8 {
    match v {
        Value::MinKey => 0,
        Value::Null => 1,
        Value::Int32(_) | Value::Int64(_) | Value::Double(_) => 2,
        Value::String(_) => 3,
        Value::Boolean(_) => 5,
        Value::MaxKey => 255,
        // Types we don't index in v1 get a middle rank
        _ => 128,
    }
}

/// Compare two BSON values following BSON comparison rules.
/// MinKey < Null < Number < String < Boolean < MaxKey
/// Numbers cross-compare by promoting to f64.
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    let ord_a = bson_type_order(a);
    let ord_b = bson_type_order(b);

    if ord_a != ord_b {
        return ord_a.cmp(&ord_b);
    }

    // Same type group — compare within group
    match (a, b) {
        (Value::MinKey, Value::MinKey) => Ordering::Equal,
        (Value::MaxKey, Value::MaxKey) => Ordering::Equal,
        (Value::Null, Value::Null) => Ordering::Equal,

        // Number group: promote to f64
        (na, nb) if ord_a == 2 => {
            let fa = value_to_f64(na);
            let fb = value_to_f64(nb);
            fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
        }

        // String
        (Value::String(sa), Value::String(sb)) => sa.cmp(sb),

        // Boolean: false < true
        (Value::Boolean(ba), Value::Boolean(bb)) => ba.cmp(bb),

        _ => Ordering::Equal,
    }
}

fn value_to_f64(v: &Value) -> f64 {
    match v {
        Value::Int32(i) => *i as f64,
        Value::Int64(i) => *i as f64,
        Value::Double(d) => *d,
        _ => 0.0,
    }
}

impl IndexKey {
    pub fn new(fields: Vec<Value>) -> Self {
        Self { fields }
    }

    /// Compare this key with another, respecting per-field sort directions.
    /// directions[i] = 1 for ascending, -1 for descending.
    pub fn cmp_with_directions(&self, other: &IndexKey, directions: &[i8]) -> Ordering {
        let len = self.fields.len().min(other.fields.len());
        for i in 0..len {
            let ord = compare_values(&self.fields[i], &other.fields[i]);
            if ord != Ordering::Equal {
                let dir = directions.get(i).copied().unwrap_or(1);
                return if dir < 0 { ord.reverse() } else { ord };
            }
        }
        // Shorter key < longer key
        self.fields.len().cmp(&other.fields.len())
    }

    /// Compare with default ascending direction on all fields.
    pub fn cmp_ascending(&self, other: &IndexKey) -> Ordering {
        let len = self.fields.len().min(other.fields.len());
        for i in 0..len {
            let ord = compare_values(&self.fields[i], &other.fields[i]);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        self.fields.len().cmp(&other.fields.len())
    }

    /// Encode this key to binary format.
    /// Format: [field_count: u16] then per field: [type_tag: u8][type-specific data]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.fields.len() as u16).to_le_bytes());
        for field in &self.fields {
            encode_value(&mut buf, field);
        }
        buf
    }

    /// Decode a key from binary format.
    pub fn decode(data: &[u8]) -> Result<IndexKey> {
        if data.len() < 2 {
            return Err(SdbError::InvalidBson);
        }
        let field_count = u16::from_le_bytes([data[0], data[1]]) as usize;
        let mut pos = 2;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let (value, consumed) = decode_value(&data[pos..])?;
            fields.push(value);
            pos += consumed;
        }
        Ok(IndexKey { fields })
    }
}

fn encode_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buf.push(TAG_NULL),
        Value::MinKey => buf.push(TAG_MIN_KEY),
        Value::MaxKey => buf.push(TAG_MAX_KEY),
        Value::Boolean(b) => {
            buf.push(TAG_BOOLEAN);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Int32(i) => {
            buf.push(TAG_INT32);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Int64(i) => {
            buf.push(TAG_INT64);
            buf.extend_from_slice(&i.to_le_bytes());
        }
        Value::Double(d) => {
            buf.push(TAG_DOUBLE);
            buf.extend_from_slice(&d.to_le_bytes());
        }
        Value::String(s) => {
            buf.push(TAG_STRING);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        // Unsupported types encode as Null
        _ => buf.push(TAG_NULL),
    }
}

fn decode_value(data: &[u8]) -> Result<(Value, usize)> {
    if data.is_empty() {
        return Err(SdbError::InvalidBson);
    }
    let tag = data[0];
    match tag {
        TAG_NULL => Ok((Value::Null, 1)),
        TAG_MIN_KEY => Ok((Value::MinKey, 1)),
        TAG_MAX_KEY => Ok((Value::MaxKey, 1)),
        TAG_BOOLEAN => {
            if data.len() < 2 {
                return Err(SdbError::InvalidBson);
            }
            Ok((Value::Boolean(data[1] != 0), 2))
        }
        TAG_INT32 => {
            if data.len() < 5 {
                return Err(SdbError::InvalidBson);
            }
            let v = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
            Ok((Value::Int32(v), 5))
        }
        TAG_INT64 => {
            if data.len() < 9 {
                return Err(SdbError::InvalidBson);
            }
            let v = i64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            Ok((Value::Int64(v), 9))
        }
        TAG_DOUBLE => {
            if data.len() < 9 {
                return Err(SdbError::InvalidBson);
            }
            let v = f64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            Ok((Value::Double(v), 9))
        }
        TAG_STRING => {
            if data.len() < 5 {
                return Err(SdbError::InvalidBson);
            }
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Err(SdbError::InvalidBson);
            }
            let s = std::str::from_utf8(&data[5..5 + len]).map_err(|_| SdbError::InvalidBson)?;
            Ok((Value::String(s.to_string()), 5 + len))
        }
        _ => Err(SdbError::InvalidBson),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let key = IndexKey::new(vec![
            Value::Int32(42),
            Value::String("hello".to_string()),
            Value::Null,
            Value::Boolean(true),
            Value::Double(3.125),
            Value::Int64(i64::MAX),
        ]);
        let encoded = key.encode();
        let decoded = IndexKey::decode(&encoded).unwrap();
        assert_eq!(decoded.fields.len(), key.fields.len());
        for (a, b) in key.fields.iter().zip(decoded.fields.iter()) {
            assert_eq!(compare_values(a, b), Ordering::Equal);
        }
    }

    #[test]
    fn test_encode_decode_min_max_key() {
        let key = IndexKey::new(vec![Value::MinKey, Value::MaxKey]);
        let encoded = key.encode();
        let decoded = IndexKey::decode(&encoded).unwrap();
        assert_eq!(decoded.fields.len(), 2);
    }

    #[test]
    fn test_compare_values_type_order() {
        // MinKey < Null < Number < String < Boolean < MaxKey
        assert_eq!(compare_values(&Value::MinKey, &Value::Null), Ordering::Less);
        assert_eq!(
            compare_values(&Value::Null, &Value::Int32(0)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Int32(0), &Value::String("a".into())),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::String("z".into()), &Value::Boolean(false)),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Boolean(true), &Value::MaxKey),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_values_numbers_cross_type() {
        // i32 vs i64
        assert_eq!(
            compare_values(&Value::Int32(10), &Value::Int64(10)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Int32(5), &Value::Int64(10)),
            Ordering::Less
        );
        // i32 vs f64
        assert_eq!(
            compare_values(&Value::Int32(3), &Value::Double(3.0)),
            Ordering::Equal
        );
        assert_eq!(
            compare_values(&Value::Int32(3), &Value::Double(3.5)),
            Ordering::Less
        );
    }

    #[test]
    fn test_compare_values_strings() {
        assert_eq!(
            compare_values(&Value::String("abc".into()), &Value::String("abd".into())),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::String("abc".into()), &Value::String("abc".into())),
            Ordering::Equal
        );
    }

    #[test]
    fn test_cmp_with_directions() {
        let a = IndexKey::new(vec![Value::Int32(1), Value::Int32(10)]);
        let b = IndexKey::new(vec![Value::Int32(1), Value::Int32(20)]);
        // Ascending on both
        assert_eq!(a.cmp_with_directions(&b, &[1, 1]), Ordering::Less);
        // Descending on second field
        assert_eq!(a.cmp_with_directions(&b, &[1, -1]), Ordering::Greater);
    }

    #[test]
    fn test_empty_key() {
        let key = IndexKey::new(vec![]);
        let encoded = key.encode();
        let decoded = IndexKey::decode(&encoded).unwrap();
        assert!(decoded.fields.is_empty());
    }
}
