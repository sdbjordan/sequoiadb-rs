use crate::element::Value;
use crate::error::BsonResult;
use crate::raw;

/// Trait for types that can be encoded to BSON binary format.
pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>) -> BsonResult<()>;
}

impl Encode for crate::Document {
    fn encode(&self, buf: &mut Vec<u8>) -> BsonResult<()> {
        let start = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // placeholder for length

        for elem in self.iter() {
            encode_element(buf, &elem.key, &elem.value)?;
        }

        buf.push(0x00); // EOO
        let len = (buf.len() - start) as i32;
        buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
        Ok(())
    }
}

fn encode_element(buf: &mut Vec<u8>, key: &str, value: &Value) -> BsonResult<()> {
    raw::write_u8(buf, value.bson_type() as u8);
    raw::write_cstring(buf, key);
    encode_value(buf, value)
}

fn encode_value(buf: &mut Vec<u8>, value: &Value) -> BsonResult<()> {
    match value {
        Value::Double(v) => {
            raw::write_f64_le(buf, *v);
        }
        Value::String(s) => {
            raw::write_string(buf, s);
        }
        Value::Document(doc) => {
            doc.encode(buf)?;
        }
        Value::Array(arr) => {
            // Array is encoded as a document with "0", "1", "2"... keys
            let start = buf.len();
            buf.extend_from_slice(&[0u8; 4]); // placeholder
            for (i, val) in arr.iter().enumerate() {
                let key = i.to_string();
                encode_element(buf, &key, val)?;
            }
            buf.push(0x00); // EOO
            let len = (buf.len() - start) as i32;
            buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
        }
        Value::Binary(data) => {
            // [i32 len] [u8 subtype] [bytes]
            raw::write_i32_le(buf, data.len() as i32);
            raw::write_u8(buf, 0x00); // subtype generic
            buf.extend_from_slice(data);
        }
        Value::ObjectId(oid) => {
            buf.extend_from_slice(&oid.bytes);
        }
        Value::Boolean(v) => {
            raw::write_u8(buf, if *v { 1 } else { 0 });
        }
        Value::Date(ms) => {
            raw::write_i64_le(buf, *ms);
        }
        Value::Null => {
            // no payload
        }
        Value::Regex { pattern, options } => {
            raw::write_cstring(buf, pattern);
            raw::write_cstring(buf, options);
        }
        Value::Int32(v) => {
            raw::write_i32_le(buf, *v);
        }
        Value::Timestamp(v) => {
            raw::write_u64_le(buf, *v);
        }
        Value::Int64(v) => {
            raw::write_i64_le(buf, *v);
        }
        Value::Decimal(data) => {
            // passthrough raw bytes
            buf.extend_from_slice(data);
        }
        Value::MinKey | Value::MaxKey => {
            // no payload
        }
    }
    Ok(())
}
