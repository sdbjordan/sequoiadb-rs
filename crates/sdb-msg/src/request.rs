use crate::header::MsgHeader;
use sdb_bson::Document;

/// Insert request message.
#[derive(Debug, Clone)]
pub struct MsgInsert {
    pub header: MsgHeader,
    pub collection: String,
    pub flags: u32,
    pub docs: Vec<Document>,
}

/// Query request message.
#[derive(Debug, Clone)]
pub struct MsgQuery {
    pub header: MsgHeader,
    pub collection: String,
    pub flags: u32,
    pub skip: i64,
    pub limit: i64,
    pub condition: Document,
    pub selector: Option<Document>,
    pub order_by: Option<Document>,
    pub hint: Option<Document>,
}

/// Delete request message.
#[derive(Debug, Clone)]
pub struct MsgDelete {
    pub header: MsgHeader,
    pub collection: String,
    pub flags: u32,
    pub condition: Document,
    pub hint: Option<Document>,
}

/// Update request message.
#[derive(Debug, Clone)]
pub struct MsgUpdate {
    pub header: MsgHeader,
    pub collection: String,
    pub flags: u32,
    pub condition: Document,
    pub modifier: Document,
    pub hint: Option<Document>,
}

/// GetMore request message.
#[derive(Debug, Clone)]
pub struct MsgGetMore {
    pub header: MsgHeader,
    pub context_id: i64,
    pub num_to_return: i32,
}

/// Kill context request message.
#[derive(Debug, Clone)]
pub struct MsgKillContext {
    pub header: MsgHeader,
    pub context_ids: Vec<i64>,
}
