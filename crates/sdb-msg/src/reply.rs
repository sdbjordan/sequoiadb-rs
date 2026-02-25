use crate::codec;
use crate::header::MsgHeader;
use sdb_bson::{Decode, Document, Encode};
use sdb_common::SdbError;

/// Reply message (C++ MsgOpReply).
///
/// After header: contextID(8) + flags(4) + startFrom(4) + numReturned(4) +
///   returnMask(4) + dataLen(4) + BSON docs
#[derive(Debug, Clone)]
pub struct MsgOpReply {
    pub header: MsgHeader,
    pub context_id: i64,
    pub flags: i32,
    pub start_from: i32,
    pub num_returned: i32,
    pub return_mask: i32,
    pub data_len: i32,
    pub docs: Vec<Document>,
}

impl MsgOpReply {
    /// Size of the fixed reply fields (after header).
    const FIXED_SIZE: usize = 8 + 4 + 4 + 4 + 4 + 4; // 28 bytes

    /// Encode the reply to wire bytes (including header).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.header.encode(&mut buf);

        codec::write_i64(&mut buf, self.context_id);
        codec::write_i32(&mut buf, self.flags);
        codec::write_i32(&mut buf, self.start_from);
        codec::write_i32(&mut buf, self.num_returned);
        codec::write_i32(&mut buf, self.return_mask);

        // Encode docs to compute data_len
        let mut doc_buf = Vec::new();
        for doc in &self.docs {
            doc.encode(&mut doc_buf).expect("BSON encode failed");
        }
        codec::write_i32(&mut buf, doc_buf.len() as i32);
        buf.extend_from_slice(&doc_buf);

        // Patch msg_len
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf
    }

    /// Decode from payload bytes (after header has been read).
    pub fn decode(header: &MsgHeader, payload: &[u8]) -> Result<Self, SdbError> {
        if payload.len() < Self::FIXED_SIZE {
            return Err(SdbError::InvalidArg);
        }
        let mut off = 0;
        let context_id = codec::read_i64(payload, &mut off)?;
        let flags = codec::read_i32(payload, &mut off)?;
        let start_from = codec::read_i32(payload, &mut off)?;
        let num_returned = codec::read_i32(payload, &mut off)?;
        let return_mask = codec::read_i32(payload, &mut off)?;
        let data_len = codec::read_i32(payload, &mut off)?;

        let mut docs = Vec::new();
        let data_end = off + data_len as usize;
        while off < data_end && off < payload.len() {
            let doc = Document::decode(&payload[off..]).map_err(|_| SdbError::InvalidBson)?;
            let doc_bytes = doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
            off += doc_bytes.len();
            docs.push(doc);
        }

        Ok(Self {
            header: header.clone(),
            context_id,
            flags,
            start_from,
            num_returned,
            return_mask,
            data_len,
            docs,
        })
    }

    /// Create a success reply for a request.
    pub fn ok(request_opcode: i32, request_id: u64, docs: Vec<Document>) -> Self {
        let reply_opcode = request_opcode | crate::opcode::REPLY_MASK;
        let num_returned = docs.len() as i32;
        Self {
            header: MsgHeader::new_request(reply_opcode, request_id),
            context_id: -1,
            flags: 0,
            start_from: 0,
            num_returned,
            return_mask: 0,
            data_len: 0, // will be computed during encode
            docs,
        }
    }

    /// Create an error reply for a request.
    pub fn error(request_opcode: i32, request_id: u64, err: &SdbError) -> Self {
        let reply_opcode = request_opcode | crate::opcode::REPLY_MASK;
        let error_code = sdb_error_to_i32(err);
        Self {
            header: MsgHeader::new_request(reply_opcode, request_id),
            context_id: -1,
            flags: error_code,
            start_from: 0,
            num_returned: 0,
            return_mask: 0,
            data_len: 0,
            docs: Vec::new(),
        }
    }
}

/// Map SdbError to a negative integer error code (C++ convention).
fn sdb_error_to_i32(err: &SdbError) -> i32 {
    match err {
        SdbError::Ok => 0,
        SdbError::Sys => -1,
        SdbError::Oom => -2,
        SdbError::InvalidArg => -6,
        SdbError::PermissionDenied => -7,
        SdbError::NotFound => -29,
        SdbError::CollectionAlreadyExists => -22,
        SdbError::CollectionSpaceAlreadyExists => -33,
        SdbError::CollectionNotFound => -23,
        SdbError::CollectionSpaceNotFound => -34,
        SdbError::DuplicateKey => -38,
        SdbError::NetworkError => -15,
        SdbError::NetworkClose => -16,
        SdbError::Timeout => -17,
        SdbError::QueryNotFound => -31,
        SdbError::InvalidBson => -6,
        SdbError::Eof => -9,
        SdbError::IndexNotFound => -47,
        SdbError::IndexAlreadyExists => -46,
        _ => -1, // generic system error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opcode::OpCode;
    use sdb_bson::Value;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    #[test]
    fn reply_empty_roundtrip() {
        let reply = MsgOpReply::ok(OpCode::QueryReq as i32, 1, vec![]);
        let bytes = reply.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpReply::decode(&header, payload).unwrap();
        assert_eq!(decoded.flags, 0);
        assert_eq!(decoded.num_returned, 0);
        assert!(decoded.docs.is_empty());
    }

    #[test]
    fn reply_with_docs_roundtrip() {
        let docs = vec![
            doc(&[("a", Value::Int32(1))]),
            doc(&[("b", Value::String("hello".into()))]),
        ];
        let reply = MsgOpReply::ok(OpCode::QueryReq as i32, 1, docs);
        let bytes = reply.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpReply::decode(&header, payload).unwrap();
        assert_eq!(decoded.num_returned, 2);
        assert_eq!(decoded.docs.len(), 2);
        assert_eq!(decoded.docs[0].get("a"), Some(&Value::Int32(1)));
        assert_eq!(
            decoded.docs[1].get("b"),
            Some(&Value::String("hello".into()))
        );
    }

    #[test]
    fn error_reply() {
        let reply = MsgOpReply::error(
            OpCode::InsertReq as i32,
            42,
            &SdbError::DuplicateKey,
        );
        let bytes = reply.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpReply::decode(&header, payload).unwrap();
        assert!(decoded.flags < 0); // error code is negative
        assert!(decoded.docs.is_empty());
    }
}
