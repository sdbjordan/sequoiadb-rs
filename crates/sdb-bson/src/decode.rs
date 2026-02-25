use crate::element::Value;
use crate::error::{BsonError, BsonResult};
use crate::raw;
use crate::types::{BsonType, ObjectId};

/// Trait for types that can be decoded from BSON binary format.
pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> BsonResult<Self>;
}

impl Decode for crate::Document {
    fn decode(buf: &[u8]) -> BsonResult<Self> {
        let mut offset = 0;
        decode_document(buf, &mut offset)
    }
}

fn decode_document(buf: &[u8], offset: &mut usize) -> BsonResult<crate::Document> {
    let doc_len = raw::read_i32_le(buf, offset)? as usize;
    if doc_len < 5 {
        return Err(BsonError::InvalidData);
    }
    let doc_end = *offset - 4 + doc_len; // offset was already advanced past the 4-byte len
    raw::ensure_remaining(buf, *offset, doc_len - 4)?; // remaining bytes in doc

    let mut doc = crate::Document::new();

    loop {
        if *offset >= doc_end {
            return Err(BsonError::InvalidData);
        }
        let tag = raw::read_u8(buf, offset)?;
        if tag == 0x00 {
            // EOO
            break;
        }
        let bson_type = BsonType::from_u8(tag).ok_or(BsonError::UnknownType(tag))?;
        let key = raw::read_cstring(buf, offset)?;
        let value = decode_value(buf, offset, bson_type)?;
        doc.insert(key, value);
    }

    if *offset != doc_end {
        return Err(BsonError::InvalidData);
    }

    Ok(doc)
}

fn decode_value(buf: &[u8], offset: &mut usize, bson_type: BsonType) -> BsonResult<Value> {
    match bson_type {
        BsonType::Double => {
            let v = raw::read_f64_le(buf, offset)?;
            Ok(Value::Double(v))
        }
        BsonType::String => {
            let s = raw::read_string(buf, offset)?;
            Ok(Value::String(s))
        }
        BsonType::Object => {
            let doc = decode_document(buf, offset)?;
            Ok(Value::Document(doc))
        }
        BsonType::Array => {
            // Array is a document with numeric keys
            let arr_doc = decode_document(buf, offset)?;
            let values: Vec<Value> = arr_doc.iter().map(|e| e.value.clone()).collect();
            Ok(Value::Array(values))
        }
        BsonType::Binary => {
            let len = raw::read_i32_le(buf, offset)? as usize;
            let _subtype = raw::read_u8(buf, offset)?;
            let data = raw::read_bytes(buf, offset, len)?;
            Ok(Value::Binary(data))
        }
        BsonType::ObjectId => {
            let bytes = raw::read_bytes(buf, offset, 12)?;
            let mut oid = [0u8; 12];
            oid.copy_from_slice(&bytes);
            Ok(Value::ObjectId(ObjectId { bytes: oid }))
        }
        BsonType::Boolean => {
            let v = raw::read_u8(buf, offset)?;
            Ok(Value::Boolean(v != 0))
        }
        BsonType::Date => {
            let ms = raw::read_i64_le(buf, offset)?;
            Ok(Value::Date(ms))
        }
        BsonType::Null | BsonType::Undefined => Ok(Value::Null),
        BsonType::Regex => {
            let pattern = raw::read_cstring(buf, offset)?;
            let options = raw::read_cstring(buf, offset)?;
            Ok(Value::Regex { pattern, options })
        }
        BsonType::Int32 => {
            let v = raw::read_i32_le(buf, offset)?;
            Ok(Value::Int32(v))
        }
        BsonType::Timestamp => {
            let v = raw::read_u64_le(buf, offset)?;
            Ok(Value::Timestamp(v))
        }
        BsonType::Int64 => {
            let v = raw::read_i64_le(buf, offset)?;
            Ok(Value::Int64(v))
        }
        BsonType::Decimal => {
            // SequoiaDB Decimal: read the size prefix (i32) then total bytes
            // We passthrough the entire payload including the size prefix
            let size_start = *offset;
            let size = raw::read_i32_le(buf, offset)? as usize;
            if size < 4 {
                return Err(BsonError::InvalidData);
            }
            let remaining = size - 4; // we already read 4 bytes for the size
            let rest = raw::read_bytes(buf, offset, remaining)?;
            let mut data = Vec::with_capacity(size);
            data.extend_from_slice(&buf[size_start..size_start + 4]);
            data.extend_from_slice(&rest);
            Ok(Value::Decimal(data))
        }
        BsonType::MinKey => Ok(Value::MinKey),
        BsonType::MaxKey => Ok(Value::MaxKey),
        BsonType::Ref | BsonType::Code | BsonType::Symbol | BsonType::CodeWithScope => {
            // These types are rarely used; skip by reading as string-like
            match bson_type {
                BsonType::Code | BsonType::Symbol => {
                    let s = raw::read_string(buf, offset)?;
                    Ok(Value::String(s))
                }
                BsonType::Ref => {
                    // DBRef: string namespace + 12-byte ObjectId
                    let _ns = raw::read_string(buf, offset)?;
                    let _oid = raw::read_bytes(buf, offset, 12)?;
                    Ok(Value::Null)
                }
                BsonType::CodeWithScope => {
                    // [i32 total_len] [string code] [document scope]
                    let total_len = raw::read_i32_le(buf, offset)? as usize;
                    if total_len < 4 {
                        return Err(BsonError::InvalidData);
                    }
                    // skip remaining bytes
                    let remaining = total_len - 4;
                    let _ = raw::read_bytes(buf, offset, remaining)?;
                    Ok(Value::Null)
                }
                _ => unreachable!(),
            }
        }
        BsonType::Eoo => Err(BsonError::InvalidData),
    }
}
