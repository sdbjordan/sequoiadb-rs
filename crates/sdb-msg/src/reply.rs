use crate::header::MsgHeader;
use sdb_bson::Document;

/// Reply message returned from the server.
#[derive(Debug, Clone)]
pub struct MsgReply {
    pub header: MsgHeader,
    pub context_id: i64,
    pub flags: i32,
    pub start_from: i32,
    pub num_returned: i32,
    pub docs: Vec<Document>,
}
