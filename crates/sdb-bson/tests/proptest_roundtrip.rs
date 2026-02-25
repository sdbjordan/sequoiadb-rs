use proptest::array::uniform12;
use proptest::prelude::*;
use sdb_bson::element::Value;
use sdb_bson::types::ObjectId;
use sdb_bson::{Document, DocumentBuilder};

// ── Arbitrary Value generation ──

/// Generate a BSON-safe string (no interior null bytes, reasonable length).
fn arb_bson_string() -> impl Strategy<Value = String> {
    "[^\x00]{0,64}"
}

/// Generate a BSON-safe key (non-empty, no interior null bytes, no dots).
fn arb_bson_key() -> impl Strategy<Value = String> {
    "[a-zA-Z_][a-zA-Z0-9_]{0,15}"
}

/// Generate a leaf Value (non-recursive).
fn arb_leaf_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<f64>()
            .prop_filter("finite float", |f| f.is_finite())
            .prop_map(Value::Double),
        arb_bson_string().prop_map(Value::String),
        any::<i32>().prop_map(Value::Int32),
        any::<i64>().prop_map(Value::Int64),
        any::<bool>().prop_map(Value::Boolean),
        any::<i64>().prop_map(Value::Date),
        any::<u64>().prop_map(Value::Timestamp),
        Just(Value::Null),
        Just(Value::MinKey),
        Just(Value::MaxKey),
        uniform12(any::<u8>()).prop_map(|bytes| Value::ObjectId(ObjectId { bytes })),
        prop::collection::vec(any::<u8>(), 0..64).prop_map(Value::Binary),
        (
            arb_bson_string(),
            "[a-z]{0,4}".prop_filter("no null", |s| !s.contains('\0'))
        )
            .prop_map(|(pattern, options)| Value::Regex { pattern, options }),
    ]
}

/// Generate a Value with bounded nesting depth.
fn arb_value(depth: u32) -> BoxedStrategy<Value> {
    if depth == 0 {
        arb_leaf_value().boxed()
    } else {
        prop_oneof![
            8 => arb_leaf_value(),
            1 => arb_document(depth - 1).prop_map(Value::Document),
            1 => prop::collection::vec(arb_value(depth - 1), 0..4).prop_map(Value::Array),
        ]
        .boxed()
    }
}

/// Generate a Document with bounded depth and field count.
fn arb_document(depth: u32) -> impl Strategy<Value = Document> {
    prop::collection::vec((arb_bson_key(), arb_value(depth)), 0..6).prop_map(|pairs| {
        let mut doc = Document::new();
        let mut seen = std::collections::HashSet::new();
        for (key, val) in pairs {
            // Deduplicate keys — Document allows duplicates but roundtrip
            // comparison would fail on ordering ambiguity
            if seen.insert(key.clone()) {
                doc.insert(key, val);
            }
        }
        doc
    })
}

/// Generate a top-level test document.
fn arb_test_document() -> impl Strategy<Value = Document> {
    arb_document(2)
}

// ── Property tests ──

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Core invariant: encode then decode must produce the original document.
    #[test]
    fn roundtrip_identity(doc in arb_test_document()) {
        let bytes = doc.to_bytes().expect("encode should succeed");
        let decoded = Document::from_bytes(&bytes).expect("decode should succeed");
        prop_assert_eq!(&doc, &decoded);
    }

    /// Encoded bytes must start with a valid i32 length that matches actual size.
    #[test]
    fn encoded_length_header_consistent(doc in arb_test_document()) {
        let bytes = doc.to_bytes().expect("encode should succeed");
        prop_assert!(bytes.len() >= 5); // minimum: 4-byte len + EOO
        let header_len = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        prop_assert_eq!(header_len, bytes.len());
    }

    /// Encoded bytes must end with 0x00 (EOO marker).
    #[test]
    fn encoded_ends_with_eoo(doc in arb_test_document()) {
        let bytes = doc.to_bytes().expect("encode should succeed");
        prop_assert_eq!(*bytes.last().unwrap(), 0x00);
    }

    /// Double-encoding should produce identical bytes.
    #[test]
    fn double_roundtrip_stable(doc in arb_test_document()) {
        let bytes1 = doc.to_bytes().expect("encode 1");
        let mid = Document::from_bytes(&bytes1).expect("decode 1");
        let bytes2 = mid.to_bytes().expect("encode 2");
        prop_assert_eq!(&bytes1, &bytes2);
    }

    /// Every leaf value should survive a single-field roundtrip.
    #[test]
    fn single_field_roundtrip(value in arb_value(0)) {
        let doc = DocumentBuilder::new().build(); // start empty
        let mut doc = doc;
        doc.insert("v", value.clone());
        let bytes = doc.to_bytes().expect("encode");
        let decoded = Document::from_bytes(&bytes).expect("decode");
        prop_assert_eq!(decoded.get("v"), Some(&value));
    }
}
