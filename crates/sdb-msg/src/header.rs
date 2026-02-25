use crate::opcode::OpCode;

/// Common message header for all SequoiaDB protocol messages.
/// Corresponds to MsgHeader in the original C++ engine.
#[derive(Debug, Clone)]
pub struct MsgHeader {
    /// Total message length in bytes (including header).
    pub msg_len: u32,
    /// Operation code identifying the message type.
    pub opcode: OpCode,
    /// Transaction ID (0 if not in a transaction).
    pub tid: u32,
    /// Route ID for the sender node.
    pub route_id: u64,
    /// Unique request ID for correlating request/reply.
    pub request_id: u64,
}

impl MsgHeader {
    /// Header size in bytes.
    pub const SIZE: usize = 28;

    pub fn new(opcode: OpCode, request_id: u64) -> Self {
        Self {
            msg_len: Self::SIZE as u32,
            opcode,
            tid: 0,
            route_id: 0,
            request_id,
        }
    }
}
