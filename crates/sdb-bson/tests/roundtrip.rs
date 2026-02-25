use sdb_bson::*;
use sdb_bson::element::Value;
use sdb_bson::types::ObjectId;

fn roundtrip(doc: &Document) -> Document {
    let bytes = doc.to_bytes().unwrap();
    Document::from_bytes(&bytes).unwrap()
}

#[test]
fn empty_document() {
    let doc = Document::new();
    let bytes = doc.to_bytes().unwrap();
    assert_eq!(bytes, vec![5, 0, 0, 0, 0]);
    let decoded = Document::from_bytes(&bytes).unwrap();
    assert!(decoded.is_empty());
}

#[test]
fn single_double() {
    let doc = DocumentBuilder::new()
        .append_double("pi", 3.14159)
        .build();
    let rt = roundtrip(&doc);
    match rt.get("pi").unwrap() {
        Value::Double(v) => assert!((*v - 3.14159).abs() < 1e-10),
        _ => panic!("expected Double"),
    }
}

#[test]
fn single_string() {
    let doc = DocumentBuilder::new()
        .append_string("name", "hello世界")
        .build();
    let rt = roundtrip(&doc);
    match rt.get("name").unwrap() {
        Value::String(s) => assert_eq!(s, "hello世界"),
        _ => panic!("expected String"),
    }
}

#[test]
fn single_i32() {
    let doc = DocumentBuilder::new().append_i32("x", -42).build();
    let rt = roundtrip(&doc);
    match rt.get("x").unwrap() {
        Value::Int32(v) => assert_eq!(*v, -42),
        _ => panic!("expected Int32"),
    }
}

#[test]
fn single_i64() {
    let doc = DocumentBuilder::new()
        .append_i64("big", i64::MAX)
        .build();
    let rt = roundtrip(&doc);
    match rt.get("big").unwrap() {
        Value::Int64(v) => assert_eq!(*v, i64::MAX),
        _ => panic!("expected Int64"),
    }
}

#[test]
fn single_bool() {
    let doc = DocumentBuilder::new()
        .append_bool("t", true)
        .append_bool("f", false)
        .build();
    let rt = roundtrip(&doc);
    assert!(matches!(rt.get("t").unwrap(), Value::Boolean(true)));
    assert!(matches!(rt.get("f").unwrap(), Value::Boolean(false)));
}

#[test]
fn single_null() {
    let doc = DocumentBuilder::new().append_null("n").build();
    let rt = roundtrip(&doc);
    assert!(matches!(rt.get("n").unwrap(), Value::Null));
}

#[test]
fn single_date() {
    let ms = 1700000000000i64;
    let doc = DocumentBuilder::new().append_date("d", ms).build();
    let rt = roundtrip(&doc);
    match rt.get("d").unwrap() {
        Value::Date(v) => assert_eq!(*v, ms),
        _ => panic!("expected Date"),
    }
}

#[test]
fn single_timestamp() {
    let ts = 0x0000_0001_0000_0002u64;
    let doc = DocumentBuilder::new().append_timestamp("ts", ts).build();
    let rt = roundtrip(&doc);
    match rt.get("ts").unwrap() {
        Value::Timestamp(v) => assert_eq!(*v, ts),
        _ => panic!("expected Timestamp"),
    }
}

#[test]
fn single_object_id() {
    let oid = ObjectId {
        bytes: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    };
    let doc = DocumentBuilder::new()
        .append_object_id("_id", oid)
        .build();
    let rt = roundtrip(&doc);
    match rt.get("_id").unwrap() {
        Value::ObjectId(o) => assert_eq!(o.bytes, oid.bytes),
        _ => panic!("expected ObjectId"),
    }
}

#[test]
fn single_binary() {
    let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let doc = DocumentBuilder::new()
        .append_binary("bin", data.clone())
        .build();
    let rt = roundtrip(&doc);
    match rt.get("bin").unwrap() {
        Value::Binary(b) => assert_eq!(b, &data),
        _ => panic!("expected Binary"),
    }
}

#[test]
fn single_regex() {
    let doc = DocumentBuilder::new()
        .append_regex("re", "^abc$", "im")
        .build();
    let rt = roundtrip(&doc);
    match rt.get("re").unwrap() {
        Value::Regex { pattern, options } => {
            assert_eq!(pattern, "^abc$");
            assert_eq!(options, "im");
        }
        _ => panic!("expected Regex"),
    }
}

#[test]
fn single_min_max_key() {
    let doc = DocumentBuilder::new()
        .append_min_key("lo")
        .append_max_key("hi")
        .build();
    let rt = roundtrip(&doc);
    assert!(matches!(rt.get("lo").unwrap(), Value::MinKey));
    assert!(matches!(rt.get("hi").unwrap(), Value::MaxKey));
}

#[test]
fn nested_document() {
    let inner = DocumentBuilder::new()
        .append_i32("a", 1)
        .append_string("b", "two")
        .build();
    let doc = DocumentBuilder::new()
        .append_document("nested", inner)
        .build();
    let rt = roundtrip(&doc);
    match rt.get("nested").unwrap() {
        Value::Document(d) => {
            assert_eq!(d.len(), 2);
            match d.get("a").unwrap() {
                Value::Int32(v) => assert_eq!(*v, 1),
                _ => panic!("expected Int32"),
            }
        }
        _ => panic!("expected Document"),
    }
}

#[test]
fn array_value() {
    let arr = vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)];
    let doc = DocumentBuilder::new().append_array("arr", arr).build();
    let rt = roundtrip(&doc);
    match rt.get("arr").unwrap() {
        Value::Array(a) => {
            assert_eq!(a.len(), 3);
            assert!(matches!(a[0], Value::Int32(1)));
            assert!(matches!(a[1], Value::Int32(2)));
            assert!(matches!(a[2], Value::Int32(3)));
        }
        _ => panic!("expected Array"),
    }
}

#[test]
fn mixed_multi_field() {
    let doc = DocumentBuilder::new()
        .append_i32("int", 42)
        .append_string("str", "hello")
        .append_double("dbl", 1.5)
        .append_bool("flag", true)
        .append_null("nil")
        .append_i64("big", 9999999999)
        .build();
    let rt = roundtrip(&doc);
    assert_eq!(rt.len(), 6);
    assert!(matches!(rt.get("int").unwrap(), Value::Int32(42)));
    assert!(matches!(rt.get("nil").unwrap(), Value::Null));
    assert!(matches!(rt.get("flag").unwrap(), Value::Boolean(true)));
}

#[test]
fn decimal_passthrough() {
    // Fake 12-byte decimal payload: 4-byte size prefix (12) + 8 bytes data
    let mut raw_bytes = vec![];
    raw_bytes.extend_from_slice(&12i32.to_le_bytes());
    raw_bytes.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    let doc = DocumentBuilder::new()
        .append_decimal("dec", raw_bytes.clone())
        .build();
    let rt = roundtrip(&doc);
    match rt.get("dec").unwrap() {
        Value::Decimal(d) => assert_eq!(d, &raw_bytes),
        _ => panic!("expected Decimal"),
    }
}

#[test]
fn known_bytes_empty_doc() {
    let bytes = vec![5u8, 0, 0, 0, 0];
    let doc = Document::from_bytes(&bytes).unwrap();
    assert!(doc.is_empty());
    assert_eq!(doc.to_bytes().unwrap(), bytes);
}

#[test]
fn known_bytes_int32() {
    // { "x": 1 }
    let bytes = vec![
        12, 0, 0, 0, // doc length
        16,           // type Int32
        b'x', 0,     // key
        1, 0, 0, 0,  // value
        0,            // EOO
    ];
    let doc = Document::from_bytes(&bytes).unwrap();
    assert_eq!(doc.len(), 1);
    match doc.get("x").unwrap() {
        Value::Int32(v) => assert_eq!(*v, 1),
        _ => panic!("expected Int32"),
    }
    assert_eq!(doc.to_bytes().unwrap(), bytes);
}

// ── Error cases ──

#[test]
fn error_too_short() {
    assert!(Document::from_bytes(&[1, 0, 0]).is_err());
}

#[test]
fn error_bad_length() {
    let bytes = vec![100, 0, 0, 0, 0];
    assert!(Document::from_bytes(&bytes).is_err());
}

#[test]
fn error_unknown_type() {
    let bytes = vec![
        8, 0, 0, 0,
        99,
        b'x', 0,
        0,
    ];
    assert!(Document::from_bytes(&bytes).is_err());
}
