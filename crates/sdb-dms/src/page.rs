use sdb_common::PageId;

/// Default page size: 64KB (SequoiaDB default).
pub const PAGE_SIZE: usize = 65536;

/// A single page in a data file.
pub struct Page {
    pub id: PageId,
    pub data: Box<[u8; PAGE_SIZE]>,
    pub dirty: bool,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        Self {
            id,
            data: Box::new([0u8; PAGE_SIZE]),
            dirty: false,
        }
    }
}
