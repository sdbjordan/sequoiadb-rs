/// Message operation codes matching SequoiaDB C++ protocol.
///
/// DDL operations (CreateCS, DropCS, etc.) are handled via QueryReq + "$command" names,
/// matching the C++ engine behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum OpCode {
    MsgReq = 1000,
    UpdateReq = 2001,
    InsertReq = 2002,
    SqlReq = 2003,
    QueryReq = 2004,
    GetMoreReq = 2005,
    DeleteReq = 2006,
    KillContextReq = 2007,
    Disconnect = 2008,
    TransBeginReq = 2010,
    TransCommitReq = 2011,
    TransRollbackReq = 2012,
    AggregateReq = 2019,
    // Internal
    CatalogReq = 3001,
    CatalogReply = 3002,
    ReplSync = 4001,
    ReplConsistency = 4002,
    ReplVoteReq = 4003,
    ReplVoteReply = 4004,
    ReplHeartbeat = 4005,
    ReplMemberChange = 4006,
    ChunkMigrateData = 4007,
}

/// Reply bit flag — OR'd with request opcode to form reply opcode (C++ convention).
pub const REPLY_MASK: i32 = 0x4000_0000_u32 as i32;

impl OpCode {
    /// Convert from raw i32 wire value to OpCode.
    pub fn from_i32(v: i32) -> Option<Self> {
        // Strip reply bit if present, then match
        let base = v & !REPLY_MASK;
        match base {
            1000 => Some(Self::MsgReq),
            2001 => Some(Self::UpdateReq),
            2002 => Some(Self::InsertReq),
            2003 => Some(Self::SqlReq),
            2004 => Some(Self::QueryReq),
            2005 => Some(Self::GetMoreReq),
            2006 => Some(Self::DeleteReq),
            2007 => Some(Self::KillContextReq),
            2008 => Some(Self::Disconnect),
            2010 => Some(Self::TransBeginReq),
            2011 => Some(Self::TransCommitReq),
            2012 => Some(Self::TransRollbackReq),
            2019 => Some(Self::AggregateReq),
            3001 => Some(Self::CatalogReq),
            3002 => Some(Self::CatalogReply),
            4001 => Some(Self::ReplSync),
            4002 => Some(Self::ReplConsistency),
            4003 => Some(Self::ReplVoteReq),
            4004 => Some(Self::ReplVoteReply),
            4005 => Some(Self::ReplHeartbeat),
            4006 => Some(Self::ReplMemberChange),
            4007 => Some(Self::ChunkMigrateData),
            _ => None,
        }
    }

    /// Return the reply opcode value (request opcode | REPLY_MASK).
    pub fn reply_code(self) -> i32 {
        (self as i32) | REPLY_MASK
    }

    /// Check if a raw i32 opcode value is a reply (has the reply bit set).
    pub fn is_reply(raw: i32) -> bool {
        (raw & REPLY_MASK) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_i32_known() {
        assert_eq!(OpCode::from_i32(2001), Some(OpCode::UpdateReq));
        assert_eq!(OpCode::from_i32(2002), Some(OpCode::InsertReq));
        assert_eq!(OpCode::from_i32(2004), Some(OpCode::QueryReq));
        assert_eq!(OpCode::from_i32(2006), Some(OpCode::DeleteReq));
        assert_eq!(OpCode::from_i32(2005), Some(OpCode::GetMoreReq));
        assert_eq!(OpCode::from_i32(2007), Some(OpCode::KillContextReq));
        assert_eq!(OpCode::from_i32(2008), Some(OpCode::Disconnect));
    }

    #[test]
    fn from_i32_unknown() {
        assert_eq!(OpCode::from_i32(9999), None);
    }

    #[test]
    fn from_i32_strips_reply_bit() {
        let reply_val = 2004 | REPLY_MASK;
        assert_eq!(OpCode::from_i32(reply_val), Some(OpCode::QueryReq));
    }

    #[test]
    fn reply_code_sets_bit() {
        let rc = OpCode::QueryReq.reply_code();
        assert_eq!(rc, 2004 | REPLY_MASK);
        assert!(OpCode::is_reply(rc));
    }

    #[test]
    fn is_reply_false_for_request() {
        assert!(!OpCode::is_reply(2004));
    }
}
