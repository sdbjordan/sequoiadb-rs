use sdb_common::RecordId;

/// A record stored within an extent.
pub struct Record {
    pub rid: RecordId,
    pub size: u32,
    pub data: Vec<u8>,
    pub deleted: bool,
}
