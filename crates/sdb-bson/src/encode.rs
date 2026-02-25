use crate::error::BsonResult;

/// Trait for types that can be encoded to BSON binary format.
pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>) -> BsonResult<()>;
}

impl Encode for crate::Document {
    fn encode(&self, buf: &mut Vec<u8>) -> BsonResult<()> {
        // Stub: write a minimal empty document (5 bytes: length + null terminator)
        let start = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // placeholder for length
        // TODO: encode each element
        buf.push(0); // null terminator
        let len = (buf.len() - start) as u32;
        buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
        Ok(())
    }
}
