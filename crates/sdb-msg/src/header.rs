use crate::codec;
use sdb_common::SdbError;

/// 8-byte route ID (C++ MsgRouteID union).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MsgRouteID {
    pub value: u64,
}

impl MsgRouteID {
    pub fn group_id(self) -> u32 {
        self.value as u32
    }

    pub fn node_id(self) -> u16 {
        (self.value >> 32) as u16
    }

    pub fn service_id(self) -> u16 {
        (self.value >> 48) as u16
    }
}

/// 12-byte global ID (C++ MsgGlobalID).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MsgGlobalID {
    pub query_id: [u8; 8],
    pub query_op_id: u32,
}

/// 52-byte message header matching the C++ MsgHeader structure.
///
/// Wire layout (little-endian):
/// ```text
/// offset  size  field
///   0      4    msg_len
///   4      4    eye
///   8      4    tid
///  12      8    route_id
///  20      8    request_id
///  28      4    opcode
///  32      2    version
///  34      2    flags
///  36     12    global_id (8 bytes query_id + 4 bytes query_op_id)
///  48      4    reserve
///  52           (end)
/// ```
#[derive(Debug, Clone)]
pub struct MsgHeader {
    pub msg_len: i32,
    pub eye: i32,
    pub tid: u32,
    pub route_id: MsgRouteID,
    pub request_id: u64,
    pub opcode: i32,
    pub version: i16,
    pub flags: i16,
    pub global_id: MsgGlobalID,
    pub reserve: [u8; 4],
}

impl MsgHeader {
    /// Header size in bytes.
    pub const SIZE: usize = 52;

    /// Default eye catcher value (C++ MSG_COMM_EYE_DEFAULT = 0x0000EEEE).
    pub const EYE_DEFAULT: i32 = 0x0000_EEEE;

    /// Create a new header for a request message.
    pub fn new_request(opcode: i32, request_id: u64) -> Self {
        Self {
            msg_len: Self::SIZE as i32,
            eye: Self::EYE_DEFAULT,
            tid: 0,
            route_id: MsgRouteID::default(),
            request_id,
            opcode,
            version: 1,
            flags: 0,
            global_id: MsgGlobalID::default(),
            reserve: [0; 4],
        }
    }

    /// Encode the header into `buf`.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        codec::write_i32(buf, self.msg_len);
        codec::write_i32(buf, self.eye);
        codec::write_u32(buf, self.tid);
        codec::write_u64(buf, self.route_id.value);
        codec::write_u64(buf, self.request_id);
        codec::write_i32(buf, self.opcode);
        codec::write_i16(buf, self.version);
        codec::write_i16(buf, self.flags);
        buf.extend_from_slice(&self.global_id.query_id);
        codec::write_u32(buf, self.global_id.query_op_id);
        buf.extend_from_slice(&self.reserve);
    }

    /// Decode a header from `buf` (must be at least 52 bytes).
    pub fn decode(buf: &[u8]) -> Result<Self, SdbError> {
        if buf.len() < Self::SIZE {
            return Err(SdbError::InvalidArg);
        }
        let mut off = 0;
        let msg_len = codec::read_i32(buf, &mut off)?;
        let eye = codec::read_i32(buf, &mut off)?;
        let tid = codec::read_u32(buf, &mut off)?;
        let route_value = codec::read_u64(buf, &mut off)?;
        let request_id = codec::read_u64(buf, &mut off)?;
        let opcode = codec::read_i32(buf, &mut off)?;
        let version = codec::read_i16(buf, &mut off)?;
        let flags = codec::read_i16(buf, &mut off)?;

        let mut query_id = [0u8; 8];
        query_id.copy_from_slice(codec::read_bytes(buf, &mut off, 8)?);
        let query_op_id = codec::read_u32(buf, &mut off)?;

        let mut reserve = [0u8; 4];
        reserve.copy_from_slice(codec::read_bytes(buf, &mut off, 4)?);

        Ok(Self {
            msg_len,
            eye,
            tid,
            route_id: MsgRouteID { value: route_value },
            request_id,
            opcode,
            version,
            flags,
            global_id: MsgGlobalID {
                query_id,
                query_op_id,
            },
            reserve,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_encode_decode_roundtrip() {
        let h = MsgHeader {
            msg_len: 100,
            eye: MsgHeader::EYE_DEFAULT,
            tid: 42,
            route_id: MsgRouteID { value: 0xDEAD_BEEF },
            request_id: 12345,
            opcode: 2004,
            version: 1,
            flags: 0,
            global_id: MsgGlobalID {
                query_id: [1, 2, 3, 4, 5, 6, 7, 8],
                query_op_id: 99,
            },
            reserve: [0; 4],
        };

        let mut buf = Vec::new();
        h.encode(&mut buf);
        assert_eq!(buf.len(), MsgHeader::SIZE);

        let decoded = MsgHeader::decode(&buf).unwrap();
        assert_eq!(decoded.msg_len, 100);
        assert_eq!(decoded.eye, MsgHeader::EYE_DEFAULT);
        assert_eq!(decoded.tid, 42);
        assert_eq!(decoded.route_id.value, 0xDEAD_BEEF);
        assert_eq!(decoded.request_id, 12345);
        assert_eq!(decoded.opcode, 2004);
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.flags, 0);
        assert_eq!(decoded.global_id.query_id, [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(decoded.global_id.query_op_id, 99);
    }

    #[test]
    fn header_size_is_52() {
        assert_eq!(MsgHeader::SIZE, 52);
    }

    #[test]
    fn decode_too_short() {
        let buf = [0u8; 40];
        assert!(MsgHeader::decode(&buf).is_err());
    }

    #[test]
    fn route_id_decompose() {
        // group_id=7, node_id=3, service_id=1
        let value: u64 = 7 | (3u64 << 32) | (1u64 << 48);
        let rid = MsgRouteID { value };
        assert_eq!(rid.group_id(), 7);
        assert_eq!(rid.node_id(), 3);
        assert_eq!(rid.service_id(), 1);
    }
}
