use crate::codec;
use crate::header::MsgHeader;
use crate::opcode::OpCode;
use sdb_bson::{Decode, Document, Encode};
use sdb_common::SdbError;

// ── MsgOpQuery ──────────────────────────────────────────────────────────

/// Query request (also used for DDL commands via "$command" name prefix).
///
/// C++ wire layout after header:
///   version(4) + w(2) + padding(2) + flags(4) + nameLength(4) +
///   numToSkip(8) + numToReturn(8) + name(aligned) + condition + selector + orderBy + hint
#[derive(Debug, Clone)]
pub struct MsgOpQuery {
    pub header: MsgHeader,
    pub version: i32,
    pub w: i16,
    pub flags: i32,
    pub name_length: i32,
    pub num_to_skip: i64,
    pub num_to_return: i64,
    pub name: String,
    pub condition: Option<Document>,
    pub selector: Option<Document>,
    pub order_by: Option<Document>,
    pub hint: Option<Document>,
}

impl MsgOpQuery {
    /// Encode to wire bytes (includes header).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Reserve space for header (will patch msg_len at end)
        self.header.encode(&mut buf);

        codec::write_i32(&mut buf, self.version);
        codec::write_i16(&mut buf, self.w);
        codec::write_i16(&mut buf, 0); // padding
        codec::write_i32(&mut buf, self.flags);
        codec::write_i32(&mut buf, self.name_length);
        codec::write_i64(&mut buf, self.num_to_skip);
        codec::write_i64(&mut buf, self.num_to_return);
        codec::write_cstring(&mut buf, &self.name);
        codec::pad_align4(&mut buf);

        encode_optional_doc(&mut buf, &self.condition);
        encode_optional_doc(&mut buf, &self.selector);
        encode_optional_doc(&mut buf, &self.order_by);
        encode_optional_doc(&mut buf, &self.hint);

        // Patch msg_len
        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf
    }

    /// Decode from payload bytes (after header has been read).
    /// `header` is the already-decoded header; `payload` starts right after the header.
    pub fn decode(header: &MsgHeader, payload: &[u8]) -> Result<Self, SdbError> {
        let mut off = 0;
        let version = codec::read_i32(payload, &mut off)?;
        let w = codec::read_i16(payload, &mut off)?;
        let _padding = codec::read_i16(payload, &mut off)?;
        let flags = codec::read_i32(payload, &mut off)?;
        let name_length = codec::read_i32(payload, &mut off)?;
        let num_to_skip = codec::read_i64(payload, &mut off)?;
        let num_to_return = codec::read_i64(payload, &mut off)?;

        let name = codec::read_cstring(payload, &mut off)?;
        codec::skip_align4(payload, &mut off);

        let condition = decode_optional_doc(payload, &mut off)?;
        let selector = decode_optional_doc(payload, &mut off)?;
        let order_by = decode_optional_doc(payload, &mut off)?;
        let hint = decode_optional_doc(payload, &mut off)?;

        Ok(Self {
            header: header.clone(),
            version,
            w,
            flags,
            name_length,
            num_to_skip,
            num_to_return,
            name,
            condition,
            selector,
            order_by,
            hint,
        })
    }

    /// Create a new query message.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: u64,
        collection: &str,
        condition: Option<Document>,
        selector: Option<Document>,
        order_by: Option<Document>,
        hint: Option<Document>,
        skip: i64,
        limit: i64,
        flags: i32,
    ) -> Self {
        let name_length = collection.len() as i32 + 1; // includes null terminator
        Self {
            header: MsgHeader::new_request(OpCode::QueryReq as i32, request_id),
            version: 1,
            w: 1,
            flags,
            name_length,
            num_to_skip: skip,
            num_to_return: limit,
            name: collection.to_string(),
            condition,
            selector,
            order_by,
            hint,
        }
    }
}

// ── MsgOpInsert ─────────────────────────────────────────────────────────

/// Insert request (opcode 2002).
///
/// C++ wire layout after header:
///   version(4) + w(2) + padding(2) + flags(4) + nameLength(4) + name(aligned) + BSON docs
#[derive(Debug, Clone)]
pub struct MsgOpInsert {
    pub header: MsgHeader,
    pub version: i32,
    pub w: i16,
    pub flags: i32,
    pub name_length: i32,
    pub name: String,
    pub docs: Vec<Document>,
}

impl MsgOpInsert {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.header.encode(&mut buf);

        codec::write_i32(&mut buf, self.version);
        codec::write_i16(&mut buf, self.w);
        codec::write_i16(&mut buf, 0); // padding
        codec::write_i32(&mut buf, self.flags);
        codec::write_i32(&mut buf, self.name_length);
        codec::write_cstring(&mut buf, &self.name);
        codec::pad_align4(&mut buf);

        for doc in &self.docs {
            doc.encode(&mut buf).expect("BSON encode failed");
        }

        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf
    }

    pub fn decode(header: &MsgHeader, payload: &[u8]) -> Result<Self, SdbError> {
        let mut off = 0;
        let version = codec::read_i32(payload, &mut off)?;
        let w = codec::read_i16(payload, &mut off)?;
        let _padding = codec::read_i16(payload, &mut off)?;
        let flags = codec::read_i32(payload, &mut off)?;
        let name_length = codec::read_i32(payload, &mut off)?;
        let name = codec::read_cstring(payload, &mut off)?;
        codec::skip_align4(payload, &mut off);

        let mut docs = Vec::new();
        while off < payload.len() {
            let doc = Document::decode(&payload[off..]).map_err(|_| SdbError::InvalidBson)?;
            let doc_bytes = doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
            off += doc_bytes.len();
            docs.push(doc);
        }

        Ok(Self {
            header: header.clone(),
            version,
            w,
            flags,
            name_length,
            name,
            docs,
        })
    }

    pub fn new(request_id: u64, collection: &str, docs: Vec<Document>, flags: i32) -> Self {
        let name_length = collection.len() as i32 + 1;
        Self {
            header: MsgHeader::new_request(OpCode::InsertReq as i32, request_id),
            version: 1,
            w: 1,
            flags,
            name_length,
            name: collection.to_string(),
            docs,
        }
    }
}

// ── MsgOpUpdate ─────────────────────────────────────────────────────────

/// Update request (opcode 2001).
///
/// After header: version(4) + w(2) + padding(2) + flags(4) + nameLength(4) +
///   name(aligned) + condition BSON + modifier BSON + hint BSON
#[derive(Debug, Clone)]
pub struct MsgOpUpdate {
    pub header: MsgHeader,
    pub version: i32,
    pub w: i16,
    pub flags: i32,
    pub name_length: i32,
    pub name: String,
    pub condition: Document,
    pub modifier: Document,
    pub hint: Option<Document>,
}

impl MsgOpUpdate {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.header.encode(&mut buf);

        codec::write_i32(&mut buf, self.version);
        codec::write_i16(&mut buf, self.w);
        codec::write_i16(&mut buf, 0);
        codec::write_i32(&mut buf, self.flags);
        codec::write_i32(&mut buf, self.name_length);
        codec::write_cstring(&mut buf, &self.name);
        codec::pad_align4(&mut buf);

        self.condition.encode(&mut buf).expect("BSON encode failed");
        self.modifier.encode(&mut buf).expect("BSON encode failed");
        if let Some(ref hint) = self.hint {
            hint.encode(&mut buf).expect("BSON encode failed");
        }

        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf
    }

    pub fn decode(header: &MsgHeader, payload: &[u8]) -> Result<Self, SdbError> {
        let mut off = 0;
        let version = codec::read_i32(payload, &mut off)?;
        let w = codec::read_i16(payload, &mut off)?;
        let _padding = codec::read_i16(payload, &mut off)?;
        let flags = codec::read_i32(payload, &mut off)?;
        let name_length = codec::read_i32(payload, &mut off)?;
        let name = codec::read_cstring(payload, &mut off)?;
        codec::skip_align4(payload, &mut off);

        let condition = Document::decode(&payload[off..]).map_err(|_| SdbError::InvalidBson)?;
        off += condition.to_bytes().map_err(|_| SdbError::InvalidBson)?.len();

        let modifier = Document::decode(&payload[off..]).map_err(|_| SdbError::InvalidBson)?;
        off += modifier.to_bytes().map_err(|_| SdbError::InvalidBson)?.len();

        let hint = if off < payload.len() {
            let h = Document::decode(&payload[off..]).map_err(|_| SdbError::InvalidBson)?;
            Some(h)
        } else {
            None
        };

        Ok(Self {
            header: header.clone(),
            version,
            w,
            flags,
            name_length,
            name,
            condition,
            modifier,
            hint,
        })
    }

    pub fn new(
        request_id: u64,
        collection: &str,
        condition: Document,
        modifier: Document,
        hint: Option<Document>,
        flags: i32,
    ) -> Self {
        let name_length = collection.len() as i32 + 1;
        Self {
            header: MsgHeader::new_request(OpCode::UpdateReq as i32, request_id),
            version: 1,
            w: 1,
            flags,
            name_length,
            name: collection.to_string(),
            condition,
            modifier,
            hint,
        }
    }
}

// ── MsgOpDelete ─────────────────────────────────────────────────────────

/// Delete request (opcode 2006).
///
/// After header: version(4) + w(2) + padding(2) + flags(4) + nameLength(4) +
///   name(aligned) + condition BSON + hint BSON
#[derive(Debug, Clone)]
pub struct MsgOpDelete {
    pub header: MsgHeader,
    pub version: i32,
    pub w: i16,
    pub flags: i32,
    pub name_length: i32,
    pub name: String,
    pub condition: Document,
    pub hint: Option<Document>,
}

impl MsgOpDelete {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.header.encode(&mut buf);

        codec::write_i32(&mut buf, self.version);
        codec::write_i16(&mut buf, self.w);
        codec::write_i16(&mut buf, 0);
        codec::write_i32(&mut buf, self.flags);
        codec::write_i32(&mut buf, self.name_length);
        codec::write_cstring(&mut buf, &self.name);
        codec::pad_align4(&mut buf);

        self.condition.encode(&mut buf).expect("BSON encode failed");
        if let Some(ref hint) = self.hint {
            hint.encode(&mut buf).expect("BSON encode failed");
        }

        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf
    }

    pub fn decode(header: &MsgHeader, payload: &[u8]) -> Result<Self, SdbError> {
        let mut off = 0;
        let version = codec::read_i32(payload, &mut off)?;
        let w = codec::read_i16(payload, &mut off)?;
        let _padding = codec::read_i16(payload, &mut off)?;
        let flags = codec::read_i32(payload, &mut off)?;
        let name_length = codec::read_i32(payload, &mut off)?;
        let name = codec::read_cstring(payload, &mut off)?;
        codec::skip_align4(payload, &mut off);

        let condition = Document::decode(&payload[off..]).map_err(|_| SdbError::InvalidBson)?;
        off += condition.to_bytes().map_err(|_| SdbError::InvalidBson)?.len();

        let hint = if off < payload.len() {
            let h = Document::decode(&payload[off..]).map_err(|_| SdbError::InvalidBson)?;
            Some(h)
        } else {
            None
        };

        Ok(Self {
            header: header.clone(),
            version,
            w,
            flags,
            name_length,
            name,
            condition,
            hint,
        })
    }

    pub fn new(
        request_id: u64,
        collection: &str,
        condition: Document,
        hint: Option<Document>,
        flags: i32,
    ) -> Self {
        let name_length = collection.len() as i32 + 1;
        Self {
            header: MsgHeader::new_request(OpCode::DeleteReq as i32, request_id),
            version: 1,
            w: 1,
            flags,
            name_length,
            name: collection.to_string(),
            condition,
            hint,
        }
    }
}

// ── MsgOpGetMore ────────────────────────────────────────────────────────

/// GetMore request (opcode 2005).
///
/// After header: contextID(8) + numToReturn(4)
#[derive(Debug, Clone)]
pub struct MsgOpGetMore {
    pub header: MsgHeader,
    pub context_id: i64,
    pub num_to_return: i32,
}

impl MsgOpGetMore {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.header.encode(&mut buf);
        codec::write_i64(&mut buf, self.context_id);
        codec::write_i32(&mut buf, self.num_to_return);

        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf
    }

    pub fn decode(header: &MsgHeader, payload: &[u8]) -> Result<Self, SdbError> {
        let mut off = 0;
        let context_id = codec::read_i64(payload, &mut off)?;
        let num_to_return = codec::read_i32(payload, &mut off)?;
        Ok(Self {
            header: header.clone(),
            context_id,
            num_to_return,
        })
    }
}

// ── MsgOpKillContexts ──────────────────────────────────────────────────

/// KillContext request (opcode 2007).
///
/// After header: ZERO(4) + numContexts(4) + contextIDs(variable)
#[derive(Debug, Clone)]
pub struct MsgOpKillContexts {
    pub header: MsgHeader,
    pub context_ids: Vec<i64>,
}

impl MsgOpKillContexts {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.header.encode(&mut buf);
        codec::write_i32(&mut buf, 0); // ZERO
        codec::write_i32(&mut buf, self.context_ids.len() as i32);
        for &id in &self.context_ids {
            codec::write_i64(&mut buf, id);
        }

        let len = buf.len() as i32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        buf
    }

    pub fn decode(header: &MsgHeader, payload: &[u8]) -> Result<Self, SdbError> {
        let mut off = 0;
        let _zero = codec::read_i32(payload, &mut off)?;
        let num_contexts = codec::read_i32(payload, &mut off)?;
        let mut context_ids = Vec::with_capacity(num_contexts as usize);
        for _ in 0..num_contexts {
            context_ids.push(codec::read_i64(payload, &mut off)?);
        }
        Ok(Self {
            header: header.clone(),
            context_ids,
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn encode_optional_doc(buf: &mut Vec<u8>, doc: &Option<Document>) {
    match doc {
        Some(d) => {
            d.encode(buf).expect("BSON encode failed");
        }
        None => {
            // Empty BSON document: 5 bytes (len=5, EOO)
            Document::new().encode(buf).expect("BSON encode failed");
        }
    }
}

fn decode_optional_doc(
    payload: &[u8],
    off: &mut usize,
) -> Result<Option<Document>, SdbError> {
    if *off >= payload.len() {
        return Ok(None);
    }
    let doc = Document::decode(&payload[*off..]).map_err(|_| SdbError::InvalidBson)?;
    let doc_bytes = doc.to_bytes().map_err(|_| SdbError::InvalidBson)?;
    *off += doc_bytes.len();
    if doc.is_empty() {
        Ok(None)
    } else {
        Ok(Some(doc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdb_bson::Value;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    #[test]
    fn query_encode_decode_roundtrip() {
        let q = MsgOpQuery::new(
            1,
            "mycs.mycl",
            Some(doc(&[("x", Value::Int32(1))])),
            None,
            None,
            None,
            10,
            100,
            0,
        );
        let bytes = q.encode();

        let header = MsgHeader::decode(&bytes).unwrap();
        assert_eq!(header.opcode, OpCode::QueryReq as i32);
        assert_eq!(header.msg_len, bytes.len() as i32);

        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpQuery::decode(&header, payload).unwrap();
        assert_eq!(decoded.name, "mycs.mycl");
        assert_eq!(decoded.num_to_skip, 10);
        assert_eq!(decoded.num_to_return, 100);
        assert!(decoded.condition.is_some());
        assert!(decoded.selector.is_none());
    }

    #[test]
    fn query_command_roundtrip() {
        let q = MsgOpQuery::new(
            2,
            "$create collectionspace",
            Some(doc(&[("Name", Value::String("mycs".into()))])),
            None,
            None,
            None,
            0,
            -1,
            0,
        );
        let bytes = q.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpQuery::decode(&header, payload).unwrap();
        assert_eq!(decoded.name, "$create collectionspace");
    }

    #[test]
    fn insert_single_doc_roundtrip() {
        let msg = MsgOpInsert::new(
            1,
            "cs.cl",
            vec![doc(&[("a", Value::Int32(42))])],
            0,
        );
        let bytes = msg.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpInsert::decode(&header, payload).unwrap();
        assert_eq!(decoded.name, "cs.cl");
        assert_eq!(decoded.docs.len(), 1);
        assert_eq!(decoded.docs[0].get("a"), Some(&Value::Int32(42)));
    }

    #[test]
    fn insert_multi_doc_roundtrip() {
        let docs = vec![
            doc(&[("x", Value::Int32(1))]),
            doc(&[("x", Value::Int32(2))]),
            doc(&[("x", Value::Int32(3))]),
        ];
        let msg = MsgOpInsert::new(1, "cs.cl", docs, 0);
        let bytes = msg.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpInsert::decode(&header, payload).unwrap();
        assert_eq!(decoded.docs.len(), 3);
    }

    #[test]
    fn update_roundtrip() {
        let msg = MsgOpUpdate::new(
            1,
            "cs.cl",
            doc(&[("x", Value::Int32(1))]),
            doc(&[("$set", Value::Document(doc(&[("x", Value::Int32(2))])))]),
            None,
            0,
        );
        let bytes = msg.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpUpdate::decode(&header, payload).unwrap();
        assert_eq!(decoded.name, "cs.cl");
        assert_eq!(decoded.condition.get("x"), Some(&Value::Int32(1)));
    }

    #[test]
    fn delete_roundtrip() {
        let msg = MsgOpDelete::new(
            1,
            "cs.cl",
            doc(&[("x", Value::Int32(1))]),
            None,
            0,
        );
        let bytes = msg.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpDelete::decode(&header, payload).unwrap();
        assert_eq!(decoded.name, "cs.cl");
        assert_eq!(decoded.condition.get("x"), Some(&Value::Int32(1)));
    }

    #[test]
    fn getmore_roundtrip() {
        let msg = MsgOpGetMore {
            header: MsgHeader::new_request(OpCode::GetMoreReq as i32, 1),
            context_id: 42,
            num_to_return: 100,
        };
        let bytes = msg.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpGetMore::decode(&header, payload).unwrap();
        assert_eq!(decoded.context_id, 42);
        assert_eq!(decoded.num_to_return, 100);
    }

    #[test]
    fn kill_context_roundtrip() {
        let msg = MsgOpKillContexts {
            header: MsgHeader::new_request(OpCode::KillContextReq as i32, 1),
            context_ids: vec![10, 20, 30],
        };
        let bytes = msg.encode();
        let header = MsgHeader::decode(&bytes).unwrap();
        let payload = &bytes[MsgHeader::SIZE..];
        let decoded = MsgOpKillContexts::decode(&header, payload).unwrap();
        assert_eq!(decoded.context_ids, vec![10, 20, 30]);
    }
}
