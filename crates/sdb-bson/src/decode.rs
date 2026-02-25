use crate::error::BsonResult;

/// Trait for types that can be decoded from BSON binary format.
pub trait Decode: Sized {
    fn decode(buf: &[u8]) -> BsonResult<Self>;
}

impl Decode for crate::Document {
    fn decode(buf: &[u8]) -> BsonResult<Self> {
        if buf.len() < 5 {
            return Err(crate::error::BsonError::BufferTooShort {
                need: 5,
                have: buf.len(),
            });
        }
        // Stub: return empty document
        // TODO: parse elements from buffer
        Ok(crate::Document::new())
    }
}
